"""Python port of v4 RegimeAwareStrategy (ValueRevert + regime gate to 0050).

Faithfully reproduces the Scala implementation's selection logic + NAV walk,
using Polars + DuckDB for vectorized in-memory computation. Target:
end-to-end <2 min vs Scala's ~10-15 min.

Parity checks against Scala (commit master):
  * 2018-01-02 → 2026-04-17, capital NT$1M
  * v4 regime_aware, commission 0.0285%, sell_tax 0.3%
  * Expected: CAGR ~27.67%, Sharpe ~0.96, MDD ~-39%, Excess +247pp

Usage:
    uv run python research/v4_strategy.py

Caveats / simplifications (to be tightened once numbers match):
  * Monthly NAV (not daily) — acceptable since benchmark also monthly
  * DRIP approximated as cash dividend add-back (no share reinvestment)
  * Ignores 0050 2025-06-18 1:4 split detection (handled via price ratio)
  * Point-in-time universe uses same SQL as Universe.eligible
"""
from __future__ import annotations

import argparse
import math
from dataclasses import dataclass

import polars as pl

from db import connect


TOPN = 10
REGIME_THRESHOLD = 0.05
COMMISSION = 0.000285
SELL_TAX = 0.003
TWSE_TRADING_DAYS_PER_YEAR = 252


# ---------- Data loaders ---------- #


def load_rebalance_dates(con, start: str, end: str, min_day: int = 1) -> list:
    q = f"""
    SELECT MIN(date) AS d FROM pg.public.daily_quote
    WHERE market='twse' AND company_code='0050'
      AND date >= '{start}'::date AND date <= '{end}'::date
      AND EXTRACT(DAY FROM date) >= {min_day}
    GROUP BY date_trunc('month', date)
    ORDER BY d
    """
    return [r[0] for r in con.sql(q).fetchall()]


def load_trading_days(con, start: str, end: str) -> list:
    q = f"""
    SELECT date FROM pg.public.daily_quote
    WHERE market='twse' AND company_code='0050'
      AND date BETWEEN '{start}'::date AND '{end}'::date
    ORDER BY date
    """
    return [r[0] for r in con.sql(q).fetchall()]


def load_prices_matrix(con, start: str, end: str) -> pl.DataFrame:
    """Wide: one column per stock, one row per trading day. Too wide — skip, use long form."""
    q = f"""
    SELECT date, company_code, closing_price
    FROM pg.public.daily_quote
    WHERE market='twse'
      AND date BETWEEN ('{start}'::date - INTERVAL '130 days') AND '{end}'::date
      AND company_code ~ '^[0-9]{{4}}$'
      AND closing_price > 0
    """
    return con.sql(q).pl()


# ---------- Signal: composite (pbBand inverted, restricted to drop_score<10 universe) ---------- #


def universe_eligible(con, asof, lookback_days: int = 30, min_median_tv: int = 50_000_000, min_trading_days: int = 10) -> set:
    q = f"""
    WITH liq AS (
      SELECT company_code,
             percentile_disc(0.5) WITHIN GROUP (ORDER BY trade_value) AS median_tv,
             COUNT(*) AS days
      FROM pg.public.daily_quote
      WHERE market='twse'
        AND date <= '{asof}'::date
        AND date > '{asof}'::date - INTERVAL '{lookback_days} days'
        AND company_code ~ '^[1-9][0-9]{{3}}$'
      GROUP BY company_code
    )
    SELECT company_code FROM liq
    WHERE days >= {min_trading_days}
      AND median_tv >= {min_median_tv}
      AND company_code NOT IN (SELECT company_code FROM pg.public.etf)
    """
    return {r[0] for r in con.sql(q).fetchall()}


def pub_lag_asof_quarter(asof) -> tuple[int, int]:
    """Latest (year, quarter) whose publication deadline + 7d buffer <= asof."""
    d = asof
    # Check each candidate in recent ~2 years
    candidates = []
    for y in range(d.year - 2, d.year + 1):
        for q, dm, dd in [(1, 5, 22), (2, 8, 21), (3, 11, 21), (4, 4, 7)]:
            dl_y = y + 1 if q == 4 else y
            from datetime import date as _d
            dl = _d(dl_y, dm, dd)
            if dl <= d:
                candidates.append((y, q, dl))
    if not candidates:
        return None
    yr, q, _ = max(candidates, key=lambda t: t[2])
    return (yr, q)


def compute_composite(con, asof) -> dict:
    """Value-revert composite: top-10 by (inverted pbBand) within drop_score<10 universe.
    Returns dict[code -> score]. Empty if regime or insufficient data."""
    universe = universe_eligible(con, asof)
    if not universe:
        return {}
    pq = pub_lag_asof_quarter(asof)
    if pq is None:
        return {}
    yr, q = pq
    codes_sql = ",".join(f"'{c}'" for c in universe)
    # drop_score filter
    drop_q = f"""
    SELECT DISTINCT ON (company_code) company_code
    FROM pg.public.growth_analysis_ttm
    WHERE company_code IN ({codes_sql})
      AND (year < {yr} OR (year = {yr} AND quarter <= {q}))
      AND COALESCE(drop_score, 0) < 10
    ORDER BY company_code, year DESC, quarter DESC
    """
    safe = {r[0] for r in con.sql(drop_q).fetchall()}
    if not safe:
        return {}

    # pbBand = close_now / median_pb_3y6m
    safe_sql = ",".join(f"'{c}'" for c in safe)
    pb_q = f"""
    WITH hist AS (
      SELECT company_code,
             percentile_cont(0.5) WITHIN GROUP (ORDER BY price_book_ratio) AS pb_median
      FROM pg.public.stock_per_pbr_dividend_yield
      WHERE market='twse'
        AND date <= '{asof}'::date
        AND date >= '{asof}'::date - INTERVAL '3 years 6 months'
        AND company_code IN ({safe_sql})
        AND price_book_ratio > 0
      GROUP BY company_code
    ),
    cur AS (
      SELECT DISTINCT ON (company_code) company_code, price_book_ratio AS pb_now
      FROM pg.public.stock_per_pbr_dividend_yield
      WHERE market='twse'
        AND date <= '{asof}'::date
        AND date >= '{asof}'::date - INTERVAL '10 days'
        AND company_code IN ({safe_sql})
        AND price_book_ratio > 0
      ORDER BY company_code, date DESC
    )
    SELECT h.company_code, c.pb_now / h.pb_median AS pb_band
    FROM hist h JOIN cur c USING (company_code)
    WHERE h.pb_median > 0
    """
    rows = con.sql(pb_q).fetchall()
    # Invert: lower pbBand = better
    return {r[0]: -r[1] for r in rows}


def trailing_63d_return(con, asof, code: str = "0050") -> float | None:
    """Split-adjusted 63-trading-day return. Uses price ratio heuristic for split detection."""
    q = f"""
    SELECT date, closing_price
    FROM pg.public.daily_quote
    WHERE market='twse' AND company_code='{code}'
      AND date <= '{asof}'::date
      AND date >= '{asof}'::date - INTERVAL '130 days'
      AND closing_price > 0
    ORDER BY date
    """
    df = con.sql(q).pl()
    if len(df) < 64:
        return None
    prices = df["closing_price"].to_list()
    dates = df["date"].to_list()
    # Split detection: consecutive day price-ratio 2.5-15x with 3-14d gap
    adj = 1.0
    start_idx = len(prices) - 64
    start_date = dates[start_idx]
    for i in range(1, len(prices)):
        prev_close = prices[i - 1]
        close = prices[i]
        if close <= 0:
            continue
        ratio = prev_close / close
        gap = (dates[i] - dates[i - 1]).days
        if 3 <= gap <= 14 and 2.5 <= ratio <= 15:
            if dates[i] > start_date:
                adj *= ratio
    end_price = prices[-1]
    adjusted_start = prices[start_idx] / adj
    return end_price / adjusted_start - 1.0


def compute_regime_composite(con, asof) -> dict:
    """v4: if 0050 63d return >= REGIME_THRESHOLD, switch to 100% 0050; else value-revert."""
    r = trailing_63d_return(con, asof)
    if r is not None and r >= REGIME_THRESHOLD:
        return {"0050": float("inf")}
    return compute_composite(con, asof)


# ---------- Backtester (monthly NAV, equal-weight) ---------- #


@dataclass
class BacktestResult:
    name: str
    dates: list
    navs: list
    start: object
    end: object


def run_backtest(con, rebal_dates, trading_days, initial_capital: float,
                 composite_fn, name: str) -> BacktestResult:
    """Monthly rebalance. Cash + holdings tracked; NAV = cash + sum(shares * today_close).
    Dividend reinvested on ex-right date at same-day close."""
    # Pre-load all daily prices we'll ever need (wide table via pivot for efficiency).
    start = trading_days[0]
    end = trading_days[-1]

    # Daily prices keyed by (date, code)
    px_q = f"""
    SELECT date, company_code, closing_price
    FROM pg.public.daily_quote
    WHERE market='twse'
      AND date BETWEEN '{start}' AND '{end}'
      AND closing_price > 0
    """
    px_df = con.sql(px_q).pl()
    # Convert to dict for O(1) lookup per (date, code)
    px_lookup = {}
    for row in px_df.iter_rows():
        px_lookup[(row[0], row[1])] = row[2]

    # Dividends — cash_dividend only (matches Scala Backtester.loadDividends)
    div_q = f"""
    SELECT date, company_code, cash_dividend AS dps
    FROM pg.public.ex_right_dividend
    WHERE market='twse'
      AND date BETWEEN '{start}' AND '{end}'
      AND cash_dividend > 0
    """
    div_lookup = {(row[0], row[1]): row[2] for row in con.sql(div_q).fetchall()}

    # Stock-split detection — same heuristic as Scala Backtester.loadSplits:
    # day-over-day ratio 2.5-15x across 3-14 day gap, no ex_right entry.
    split_q = f"""
    WITH seq AS (
      SELECT company_code, date, closing_price,
             LAG(date) OVER (PARTITION BY company_code ORDER BY date) AS prev_date,
             LAG(closing_price) OVER (PARTITION BY company_code ORDER BY date) AS prev_close
      FROM pg.public.daily_quote
      WHERE market='twse'
        AND date BETWEEN '{start}' AND '{end}'
    )
    SELECT date, company_code, prev_close / closing_price AS ratio
    FROM seq
    WHERE prev_close IS NOT NULL AND closing_price > 0
      AND (prev_close / closing_price BETWEEN 2.5 AND 15
           OR prev_close / closing_price BETWEEN 0.067 AND 0.4)
      AND (date - prev_date) BETWEEN 3 AND 14
      AND NOT EXISTS (
        SELECT 1 FROM pg.public.ex_right_dividend
        WHERE company_code = seq.company_code AND date = seq.date
      )
    """
    split_lookup = {(row[0], row[1]): row[2] for row in con.sql(split_q).fetchall()}

    # Pre-compute composite/regime for all rebalance dates
    print(f"[{name}] pre-computing composite for {len(rebal_dates)} rebalance dates ...")
    composites = {}
    for d in rebal_dates:
        composites[d] = composite_fn(con, d)

    rebal_set = set(rebal_dates)

    cash = initial_capital
    holdings = {}  # code -> shares
    nav_history = []

    for today in trading_days:
        # 1. DRIP — cash dividend → buy additional shares at today's close
        for code, shares in list(holdings.items()):
            dps = div_lookup.get((today, code))
            if dps and dps > 0:
                cash_div = shares * dps
                px = px_lookup.get((today, code))
                if px and px > 0:
                    holdings[code] = shares + cash_div / px
                else:
                    cash += cash_div

        # 2. Stock splits — multiply share count by factor (NAV continuous)
        for code, shares in list(holdings.items()):
            factor = split_lookup.get((today, code))
            if factor:
                holdings[code] = shares * factor

        # 2. Rebalance
        if today in rebal_set:
            comp = composites.get(today, {})
            picks = sorted(comp.items(), key=lambda x: -x[1])[:TOPN]
            if picks:
                # 0050 regime case: all-in 0050
                if picks[0][0] == "0050" and math.isinf(picks[0][1]):
                    target = {"0050": 1.0}
                else:
                    target = {c: 1.0 / len(picks) for c, _ in picks}

                # Compute current NAV for target sizing
                total_value = cash + sum(
                    shares * px_lookup.get((today, code), 0)
                    for code, shares in holdings.items()
                )

                # Sell everything not in target (simplification vs delta rebalance)
                for code in list(holdings.keys()):
                    if code not in target:
                        px = px_lookup.get((today, code), 0)
                        if px > 0:
                            proceeds = holdings[code] * px * (1 - COMMISSION - SELL_TAX)
                            cash += proceeds
                        del holdings[code]

                # For each target, adjust to target weight
                for code, w in target.items():
                    want_value = total_value * w
                    px = px_lookup.get((today, code), 0)
                    if px <= 0:
                        continue
                    current_value = holdings.get(code, 0) * px
                    delta_value = want_value - current_value
                    if delta_value > 0:  # buy
                        buy_value = delta_value / (1 + COMMISSION)
                        add_shares = buy_value / px
                        holdings[code] = holdings.get(code, 0) + add_shares
                        cash -= buy_value * (1 + COMMISSION)
                    elif delta_value < 0:  # sell partial
                        sell_value = -delta_value
                        sell_shares = sell_value / px
                        holdings[code] = max(0, holdings.get(code, 0) - sell_shares)
                        cash += sell_value * (1 - COMMISSION - SELL_TAX)

        # 3. Record NAV at today's close
        nav = cash + sum(
            shares * px_lookup.get((today, code), 0)
            for code, shares in holdings.items()
        )
        nav_history.append((today, nav))

    return BacktestResult(
        name=name,
        dates=[d for d, _ in nav_history],
        navs=[v for _, v in nav_history],
        start=trading_days[0],
        end=trading_days[-1],
    )


# ---------- Metrics ---------- #


def summarize(r: BacktestResult) -> dict:
    navs = r.navs
    years = max((r.end - r.start).days / 365.25, 1e-9)
    cagr = (navs[-1] / navs[0]) ** (1 / years) - 1

    # Daily log returns
    import numpy as np
    arr = np.array(navs)
    rets = np.diff(arr) / arr[:-1]
    vol = rets.std(ddof=1) * math.sqrt(TWSE_TRADING_DAYS_PER_YEAR)
    rf = 0.01
    sharpe = (cagr - rf) / vol if vol > 0 else 0.0

    # Max drawdown
    peak = arr[0]
    mdd = 0.0
    for v in arr:
        peak = max(peak, v)
        mdd = min(mdd, (v - peak) / peak)

    return {
        "CAGR": cagr,
        "Sharpe": sharpe,
        "MDD": mdd,
        "total_return": navs[-1] / navs[0] - 1,
        "final_nav": navs[-1],
    }


# ---------- Main ---------- #


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2018-01-02")
    p.add_argument("--end", default="2026-04-17")
    p.add_argument("--capital", type=float, default=1_000_000)
    p.add_argument("--min-day", type=int, default=1, help="month-start=1 (original v4), 15=month-mid")
    args = p.parse_args()

    con = connect()

    import time as _t
    t0 = _t.time()

    rebal_dates = load_rebalance_dates(con, args.start, args.end, args.min_day)
    trading_days = load_trading_days(con, args.start, args.end)
    print(f"[setup] {len(rebal_dates)} rebalance dates, {len(trading_days)} trading days")

    # v4 strategy
    v4 = run_backtest(con, rebal_dates, trading_days, args.capital,
                       compute_regime_composite, "regime_aware")
    # 0050 benchmark (hold through, regime stays on all months via forced high threshold)
    bench = run_backtest(con, [trading_days[0]], trading_days, args.capital,
                         lambda c, d: {"0050": float("inf")}, "hold_0050")

    t1 = _t.time()
    print(f"\n[runtime] {t1 - t0:.1f}s")

    for r in (v4, bench):
        s = summarize(r)
        print(f"\n=== {r.name} ===")
        print(f"  CAGR:    {s['CAGR']*100:+.2f}%")
        print(f"  Sharpe:  {s['Sharpe']:.3f}")
        print(f"  MDD:     {s['MDD']*100:+.2f}%")
        print(f"  total:   {s['total_return']*100:+.2f}%")
        print(f"  finalNAV: ${s['final_nav']:,.0f}")

    ex_v4 = summarize(v4)["total_return"]
    ex_bench = summarize(bench)["total_return"]
    print(f"\nExcess vs 0050: {(ex_v4 - ex_bench)*100:+.2f} pp")


if __name__ == "__main__":
    main()
