"""Chase + trailing-stop backtest — the "Plan B" north-star test.

Hypothesis: individual spike stocks show no reliable pre-event signal
(see `spike_dataset.parquet` top-20 inspection), but post-peak return
distribution has a positive-skew fat tail. Capture the tail with
breakout-entry + trailing-stop-exit, letting winners run and cutting
losers fast.

Stack:
  * vectorbt 1.0 — Portfolio.from_signals (native sl_stop + sl_trail)
  * stockstats    — RSI / MACD / Bollinger variants (Phase 2d)
  * empyrical    — CAGR / Sharpe / MDD to match pyfolio
  * pyfolio      — full tear-sheet (Phase 2e)

Cost model (matches user's 國泰/富邦/永豐 e-trading + v4 baseline):
  buy commission 0.0285%, sell commission 0.0285%, sell tax 0.3%.
  Round-trip = 0.357%. Baked symmetric: 0.1785% per side.

Universe: TWSE 4-digit codes, 30-day median trade_value >= NT$50M.

Usage:
    uv run --project research python research/experiments/chase_trailing_stop.py
"""
from __future__ import annotations

import os
import sys
import time

import numpy as np
import pandas as pd
import polars as pl
import vectorbt as vbt
import empyrical as ep

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect  # noqa: E402


# --- Config: longest possible window (daily_quote starts 2004-02-11; 60d warmup
# → chase entries from ~2004-05 onwards). 22-year max-history baseline. ---
START = "2004-04-01"
END = "2026-04-17"
INIT_CAPITAL = 1_000_000

# Real: buy 0.0285%, sell 0.0285% + 0.3% tax. Round-trip = 0.00357.
# vectorbt 'fees' is symmetric per side; split evenly → 0.001785 per side.
COMMISSION = 0.000285
SELL_TAX = 0.003
FEES_SYM = (2 * COMMISSION + SELL_TAX) / 2  # 0.001785

OUT_DIR = "research/experiments/out"
os.makedirs(OUT_DIR, exist_ok=True)


def load_price_panel(con, start: str, end: str):
    """Wide pandas DataFrame: index=date, columns=company_code, values=close/tv."""
    # Chase universe = TWSE only. Empirical: adding TPEx crashes chase CAGR
    # from +34% to +11% over 2018-2026 with MDD going -40% → -73%. TPEx small
    # caps generate too many false breakouts that trailing-stop whipsaws into
    # cumulative losses. TPEx exposure needs a fundamentally different
    # strategy design (intraday / news-driven / regime-gated).
    # See `project_strategy_research_findings.md` "Failed direction: chase+TPEx".
    df = con.sql(f"""
        SELECT date, company_code, closing_price, trade_value
        FROM daily_quote
        WHERE market = 'twse'
          AND date BETWEEN '{start}' AND '{end}'
          AND regexp_matches(company_code, '^[1-9][0-9]{{3}}$')
          AND closing_price > 0
    """).pl()

    prices_pl = df.pivot(on="company_code", index="date", values="closing_price").sort("date")
    tv_pl     = df.pivot(on="company_code", index="date", values="trade_value").sort("date")

    prices = prices_pl.to_pandas().set_index("date")
    tv     = tv_pl.to_pandas().set_index("date")
    prices.index = pd.to_datetime(prices.index)
    tv.index     = pd.to_datetime(tv.index)
    return prices, tv


def build_entries_return_breakout(
    prices: pd.DataFrame,
    tv: pd.DataFrame,
    lookback: int = 20,
    ret_thresh: float = 0.30,
    adv_window: int = 30,
    adv_thresh: float = 50_000_000,
) -> pd.DataFrame:
    """Entries where stock gained >= ret_thresh over `lookback` days AND
    30-day median trade_value >= adv_thresh. Triggers on first-True only
    (so a single breakout isn't re-entered every day of the uptrend)."""
    ret = prices / prices.shift(lookback) - 1
    adv = tv.rolling(adv_window, min_periods=10).median()
    raw = (ret >= ret_thresh) & (adv >= adv_thresh)
    first_true = raw & ~raw.shift(1, fill_value=False)
    return first_true.fillna(False)


def run_chase(
    prices: pd.DataFrame,
    entries: pd.DataFrame,
    trail_stop: float = 0.15,
    slot_dollars: float = 100_000,
    fees: float = FEES_SYM,
    init_cash: float = INIT_CAPITAL,
):
    """Run vectorbt chase + trailing stop.

    slot_dollars = fixed $ per entry. init_cash / slot_dollars = effective
    TOPN. cash_sharing=True so all columns share the pool — additional
    entries are silently skipped once cash runs out (natural TOPN cap).
    """
    pf = vbt.Portfolio.from_signals(
        close=prices,
        entries=entries,
        sl_stop=trail_stop,
        sl_trail=True,
        size=slot_dollars,
        size_type="Value",
        fees=fees,
        init_cash=init_cash,
        cash_sharing=True,
        group_by=True,
        freq="1D",
    )
    return pf


def compute_split_safe_drip_returns(con, code: str, start: str, end: str) -> pd.Series:
    """Split-safe + DRIP daily return for single stock (benchmarks).
    Copies v4.py `compute_daily_returns_sql` logic:
      * If gap 3-14 days AND prev/cur ratio in [2.5,15] or [0.067,0.4] AND
        no ex-right entry that day → 0.0 (skip split day).
      * Else: (close + cash_dividend) / prev_close - 1  (DRIP-adjusted).
    """
    q = f"""
    WITH px AS (
      SELECT company_code, date, closing_price,
             LAG(closing_price) OVER (PARTITION BY company_code ORDER BY date) AS prev_close,
             LAG(date) OVER (PARTITION BY company_code ORDER BY date) AS prev_date
      FROM daily_quote
      WHERE company_code = '{code}'
        AND date BETWEEN DATE '{start}' - INTERVAL '10 days' AND DATE '{end}'
        AND closing_price > 0
    )
    SELECT px.date,
      CASE
        WHEN px.prev_close IS NOT NULL
             AND (px.prev_close / px.closing_price BETWEEN 2.5 AND 15
                  OR px.prev_close / px.closing_price BETWEEN 0.067 AND 0.4)
             AND (px.date - px.prev_date) BETWEEN 3 AND 14
             AND NOT EXISTS (
               SELECT 1 FROM ex_right_dividend e
               WHERE e.company_code = px.company_code AND e.date = px.date)
        THEN 0.0
        WHEN px.prev_close IS NOT NULL AND px.prev_close > 0
        THEN (px.closing_price + COALESCE(d.cash_dividend, 0)) / px.prev_close - 1.0
        ELSE NULL
      END AS ret
    FROM px
    LEFT JOIN ex_right_dividend d ON d.company_code = px.company_code AND d.date = px.date
    WHERE px.date BETWEEN DATE '{start}' AND DATE '{end}'
    """
    df = con.sql(q).pl().to_pandas().set_index("date")
    df.index = pd.to_datetime(df.index)
    return df["ret"].fillna(0.0)


def metrics(returns: pd.Series, label: str, bench_ret: pd.Series | None = None) -> dict:
    """Standard metrics via empyrical. Returns dict for sweep aggregation."""
    if len(returns) == 0 or returns.abs().sum() == 0:
        return {"label": label, "CAGR": 0.0, "Sharpe": 0.0, "MDD": 0.0,
                "Calmar": 0.0, "FinalNAV": INIT_CAPITAL}
    cagr = ep.cagr(returns)
    sharpe = ep.sharpe_ratio(returns)
    sortino = ep.sortino_ratio(returns)
    mdd = ep.max_drawdown(returns)
    calmar = ep.calmar_ratio(returns)
    total_ret = (1 + returns).prod() - 1
    final_nav = INIT_CAPITAL * (1 + total_ret)

    res = {
        "label": label,
        "CAGR": cagr, "Sharpe": sharpe, "Sortino": sortino,
        "MDD": mdd, "Calmar": calmar, "FinalNAV": final_nav,
    }
    if bench_ret is not None:
        aligned = returns.align(bench_ret, join="inner")
        excess = aligned[0] - aligned[1]
        res["ExcessAnnual"] = ep.cagr(excess)
    return res


def print_metrics(m: dict):
    print(f"=== {m['label']} ===")
    print(f"  CAGR:     {m['CAGR']*100:+.2f}%")
    print(f"  Sharpe:   {m['Sharpe']:.3f}")
    if "Sortino" in m:
        print(f"  Sortino:  {m['Sortino']:.3f}")
    print(f"  MDD:      {m['MDD']*100:+.2f}%")
    print(f"  Calmar:   {m['Calmar']:.3f}")
    print(f"  FinalNAV: ${m['FinalNAV']:,.0f}")
    if "ExcessAnnual" in m:
        print(f"  Excess p.a.: {m['ExcessAnnual']*100:+.2f}%")


def extract_returns(pf) -> pd.Series:
    """Daily returns from vectorbt Portfolio (group_by=True)."""
    r = pf.returns()
    if isinstance(r, pd.DataFrame):
        r = r.iloc[:, 0]
    return r.fillna(0.0)


def build_regime_mask(con, start: str, end: str, threshold: float = 0.05) -> pd.Series:
    """Boolean mask indexed by date: True = 0050 63-trading-day SPLIT-SAFE
    DRIP return >= threshold (in-trend). Matches v4.RegimeAwareStrategy.
    """
    ret0050 = compute_split_safe_drip_returns(con, "0050", start, end)
    nav = (1 + ret0050).cumprod()
    # 63-day return ending at each day
    ret63 = nav / nav.shift(63) - 1
    return (ret63 >= threshold).fillna(False)


def apply_regime_mask(entries: pd.DataFrame, regime_mask: pd.Series) -> pd.DataFrame:
    """Zero out entries on days when regime is off."""
    rm = regime_mask.reindex(entries.index, fill_value=False)
    # Broadcast: True when regime on AND entry signal fires
    return entries.mul(rm.astype(int), axis=0).astype(bool)


def run_sweep(prices, tv, regime_mask, bench_0050):
    """Parameter sweep — entry × trail × slot × regime."""
    entry_configs = [
        ("10d+20%", 10, 0.20),
        ("20d+30%", 20, 0.30),
        ("20d+50%", 20, 0.50),
        ("60d+50%", 60, 0.50),
        ("60d+80%", 60, 0.80),
    ]
    trail_stops = [0.10, 0.15, 0.20, 0.25]
    slot_sizes = [50_000, 100_000, 200_000]  # 20, 10, 5 slots
    regime_variants = [("no_regime", None), ("regime_on", regime_mask)]

    results = []
    t0 = time.time()
    for entry_name, lb, th in entry_configs:
        entries_raw = build_entries_return_breakout(prices, tv, lookback=lb, ret_thresh=th)
        n_raw = int(entries_raw.sum().sum())
        for reg_name, reg in regime_variants:
            entries = apply_regime_mask(entries_raw, reg) if reg is not None else entries_raw
            n_eff = int(entries.sum().sum())
            for ts in trail_stops:
                for slot in slot_sizes:
                    pf = run_chase(prices, entries, trail_stop=ts, slot_dollars=slot)
                    ret = extract_returns(pf)
                    label = f"{entry_name} | trail={int(ts*100)}% | slot=${slot//1000}K | {reg_name}"
                    m = metrics(ret, label, bench_ret=bench_0050)
                    m.update(dict(
                        entry=entry_name, trail_pct=ts, slot=slot, regime=reg_name,
                        n_raw_entries=n_raw, n_entries=n_eff,
                    ))
                    results.append(m)
        print(f"  [{entry_name}] done ({time.time()-t0:.1f}s)")

    df = pd.DataFrame(results)
    return df


def main():
    t0 = time.time()
    con = connect()
    print(f"[db] connected ({time.time()-t0:.1f}s)")

    prices, tv = load_price_panel(con, START, END)
    print(f"[data] prices: {prices.shape}  tv: {tv.shape}  ({time.time()-t0:.1f}s)")

    # --- Benchmarks (split-safe + DRIP, matches v4 calc) ---
    bench_0050 = compute_split_safe_drip_returns(con, "0050", START, END)
    m_0050 = metrics(bench_0050, "hold_0050 benchmark (split-safe + DRIP)")
    print()
    print_metrics(m_0050)

    # --- Base case: 20-day +30%, trailing 15%, 10 slots ---
    print("\n[signal] building base-case entries (20d +30%, ADV>=50M) ...")
    entries = build_entries_return_breakout(
        prices, tv, lookback=20, ret_thresh=0.30,
        adv_window=30, adv_thresh=50_000_000
    )
    n_entries = int(entries.sum().sum())
    print(f"[signal] entries: {n_entries:,} (across {(entries.sum() > 0).sum()} stocks)")

    print("[backtest] running base case ...")
    pf = run_chase(prices, entries, trail_stop=0.15, slot_dollars=100_000)
    ret = extract_returns(pf)
    m_base = metrics(ret, "BASE: 20d+30% entry / 15% trail / 10 slots / 2-折 fees",
                     bench_ret=bench_0050)
    print()
    print_metrics(m_base)

    # Save base case returns for downstream pyfolio
    pd.DataFrame({"ret": ret, "nav": INIT_CAPITAL * (1 + ret).cumprod()}).to_parquet(
        os.path.join(OUT_DIR, "chase_base_returns.parquet"))
    print(f"\n[out] saved base case returns to {OUT_DIR}/chase_base_returns.parquet")

    # --- Parameter sweep ---
    print("\n[sweep] building regime mask ...")
    regime_mask = build_regime_mask(con, START, END, threshold=0.05)
    pct_regime_on = regime_mask.mean()
    print(f"[sweep] regime on {pct_regime_on*100:.1f}% of days")

    print("[sweep] running parameter sweep (5 entries × 4 trails × 3 slots × 2 regime = 120 bt) ...")
    sweep_df = run_sweep(prices, tv, regime_mask, bench_0050)
    sweep_path = os.path.join(OUT_DIR, "chase_sweep.parquet")
    sweep_df.to_parquet(sweep_path)
    print(f"[sweep] saved {len(sweep_df)} results to {sweep_path}")

    # Top 10 by CAGR, top 10 by Sharpe, top 10 by Calmar
    print("\n=== TOP 10 by CAGR ===")
    top = sweep_df.sort_values("CAGR", ascending=False).head(10)
    for _, r in top.iterrows():
        print(f"  {r['label']:64} CAGR {r['CAGR']*100:+6.2f}%  Sharpe {r['Sharpe']:5.2f}  MDD {r['MDD']*100:+6.1f}%  excess {r.get('ExcessAnnual', 0)*100:+5.1f}%")

    print("\n=== TOP 10 by Sharpe ===")
    top = sweep_df.sort_values("Sharpe", ascending=False).head(10)
    for _, r in top.iterrows():
        print(f"  {r['label']:64} Sharpe {r['Sharpe']:5.2f}  CAGR {r['CAGR']*100:+6.2f}%  MDD {r['MDD']*100:+6.1f}%  excess {r.get('ExcessAnnual', 0)*100:+5.1f}%")

    print("\n=== TOP 10 by Calmar (CAGR / |MDD|) ===")
    top = sweep_df.sort_values("Calmar", ascending=False).head(10)
    for _, r in top.iterrows():
        print(f"  {r['label']:64} Calmar {r['Calmar']:5.2f}  CAGR {r['CAGR']*100:+6.2f}%  Sharpe {r['Sharpe']:5.2f}  MDD {r['MDD']*100:+6.1f}%")

    print(f"\n[total] {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
