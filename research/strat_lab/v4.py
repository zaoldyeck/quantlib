"""Python port of v4 RegimeAwareStrategy.

Design:
  * Heavy joins in DuckDB SQL (columnar + parallel).
  * Python / Polars only assembles plan + cumprod.
  * Zero per-day Python loops in hot path.
  * Cache DB: research/cache.duckdb (regen via cache_tables.py).

Parity target vs Scala v4: CAGR 27.67%, Sharpe 0.96, MDD -39%, Excess +247pp.
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
from datetime import date

import numpy as np
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect
from prices import fetch_adjusted_panel, fetch_daily_returns

TOPN = 10
REGIME_THRESHOLD = 0.05
COMMISSION = 0.000285
SELL_TAX = 0.003
TDPY = 252


def create_temp_tables(con, rebal_ds: list[date]):
    """Register Python list as DuckDB table (via Polars roundtrip)."""
    df = pl.DataFrame({"rebal_d": rebal_ds})
    con.register("rebal", df)


def compute_picks_sql(con, rebal_ds) -> pl.DataFrame:
    """All top-N picks in ONE DuckDB SQL. Leverages columnar engine + parallel exec."""
    if not rebal_ds:
        return pl.DataFrame(schema={"rebal_d": pl.Date, "company_code": pl.Utf8, "weight": pl.Float64})

    create_temp_tables(con, rebal_ds)

    q = f"""
    WITH
    uni AS (
      -- Universe = TWSE 4-digit equity codes only (TPEx ignored).
      -- Empirical finding: adding TPEx to pbBand picker drops CAGR ~-8pp +
      -- MDD to -53% (see project_strategy_research_findings.md).
      -- pbBand value factor fails on TPEx small-caps (cheap = truly bad).
      -- Keep v4 as TWSE pure-play; use chase / new factors for TPEx universe.
      SELECT r.rebal_d, dq.company_code
      FROM rebal r
      JOIN daily_quote dq
        ON dq.market = 'twse'
       AND regexp_matches(dq.company_code, '^[1-9][0-9]{{3}}$')
       AND dq.date <= r.rebal_d AND dq.date > r.rebal_d - INTERVAL '30 days'
      GROUP BY r.rebal_d, dq.company_code
      HAVING percentile_disc(0.5) WITHIN GROUP (ORDER BY dq.trade_value) >= 50000000
         AND COUNT(*) >= 10
    ),
    uni_clean AS (
      SELECT * FROM uni WHERE company_code NOT IN (SELECT company_code FROM etf)
    ),
    qfor AS (
      SELECT r.rebal_d, MAX(cand.year * 100 + cand.quarter) AS yq
      FROM rebal r
      CROSS JOIN (
        SELECT y::INT AS year, q::INT AS quarter,
               CASE q WHEN 1 THEN make_date(y,5,22) WHEN 2 THEN make_date(y,8,21)
                      WHEN 3 THEN make_date(y,11,21) WHEN 4 THEN make_date(y+1,4,7) END AS deadline
        FROM (VALUES (2014),(2015),(2016),(2017),(2018),(2019),(2020),(2021),(2022),(2023),(2024),(2025),(2026)) t1(y)
        CROSS JOIN (VALUES (1),(2),(3),(4)) t2(q)
      ) cand
      WHERE cand.deadline <= r.rebal_d
      GROUP BY r.rebal_d
    ),
    drop_safe AS (
      -- First-principles distressed filter (replaces growth_analysis_ttm.drop_score < 10).
      -- raw_quarterly.f_score_raw is Piotroski F9 derived from raw IS+BS+CF (no PG VIEW).
      -- f_score_raw >= 4 keeps companies with at least 4 of 9 quality criteria positive,
      -- a stricter analogue to the original drop_score gate.
      SELECT DISTINCT ON (u.rebal_d, u.company_code) u.rebal_d, u.company_code
      FROM uni_clean u
      JOIN qfor qf ON qf.rebal_d = u.rebal_d
      JOIN raw_quarterly rq
        ON rq.company_code = u.company_code
       AND (rq.year * 100 + rq.quarter) <= qf.yq
      WHERE COALESCE(rq.f_score_raw, 0) >= 4
      ORDER BY u.rebal_d, u.company_code, rq.year DESC, rq.quarter DESC
    ),
    pb_med AS (
      SELECT ds.rebal_d, ds.company_code,
             percentile_cont(0.5) WITHIN GROUP (ORDER BY spd.price_book_ratio) AS pbm
      FROM drop_safe ds
      JOIN stock_per_pbr spd
        ON spd.market = 'twse'
       AND spd.company_code = ds.company_code
       AND spd.date <= ds.rebal_d AND spd.date >= ds.rebal_d - INTERVAL '3 years 6 months'
       AND spd.price_book_ratio > 0
      GROUP BY ds.rebal_d, ds.company_code
    ),
    pb_cur AS (
      SELECT DISTINCT ON (ds.rebal_d, ds.company_code) ds.rebal_d, ds.company_code,
             spd.price_book_ratio AS pb_now
      FROM drop_safe ds
      JOIN stock_per_pbr spd
        ON spd.market = 'twse'
       AND spd.company_code = ds.company_code
       AND spd.date <= ds.rebal_d AND spd.date >= ds.rebal_d - INTERVAL '10 days'
       AND spd.price_book_ratio > 0
      ORDER BY ds.rebal_d, ds.company_code, spd.date DESC
    ),
    ranked AS (
      SELECT m.rebal_d, m.company_code,
             ROW_NUMBER() OVER (PARTITION BY m.rebal_d ORDER BY c.pb_now / m.pbm ASC) AS rnk
      FROM pb_med m
      JOIN pb_cur c ON c.rebal_d = m.rebal_d AND c.company_code = m.company_code
      WHERE m.pbm > 0
    )
    SELECT rebal_d, company_code, (1.0 / CAST({TOPN} AS DOUBLE)) AS weight
    FROM ranked WHERE rnk <= {TOPN}
    """
    return con.sql(q).pl()


def regime_signals(con, rebal_ds):
    """0050 63-trading-day total-return (DRIP) per rebal date.

    Uses `prices.fetch_adjusted_panel` so cash-dividend + capital-reduction are
    properly back-adjusted — semantically a "63-day total-return" signal.
    Replaces the old hand-rolled gap-based split detector.
    """
    if not rebal_ds:
        return {}
    min_d = min(rebal_ds)
    panel = (fetch_adjusted_panel(
                con, min_d.isoformat(), max(rebal_ds).isoformat(),
                codes=["0050"], market="twse",
                include_extra_history_days=150,
            )
            .sort("date"))
    dates = panel["date"].to_list()
    prices = panel["close"].to_list()  # adjusted

    result = {}
    for rebal_d in rebal_ds:
        idx = None
        for i, d in enumerate(dates):
            if d > rebal_d:
                break
            idx = i
        if idx is None or idx < 63:
            result[rebal_d] = None
            continue
        result[rebal_d] = prices[idx] / prices[idx - 63] - 1.0
    return result


def backtest(start: str, end: str, min_day: int, capital: float, use_regime: bool = True):
    con = connect()
    t0 = time.time()

    days = [r[0] for r in con.sql(f"""
        SELECT date FROM daily_quote
        WHERE market='twse' AND company_code='0050'
          AND date BETWEEN DATE '{start}' AND DATE '{end}' ORDER BY date
    """).fetchall()]
    rebal = [r[0] for r in con.sql(f"""
        SELECT MIN(date) FROM daily_quote
        WHERE market='twse' AND company_code='0050'
          AND date BETWEEN DATE '{start}' AND DATE '{end}'
          AND EXTRACT(DAY FROM date) >= {min_day}
        GROUP BY date_trunc('month', date) ORDER BY MIN(date)
    """).fetchall()]
    print(f"[cal]    {len(days)} days, {len(rebal)} rebals ({time.time()-t0:.2f}s)")

    # Regime
    if use_regime:
        regime = regime_signals(con, rebal)
        non_trend = [d for d, r in regime.items() if r is None or r < REGIME_THRESHOLD]
        trend_dates = [d for d in rebal if d not in set(non_trend)]
    else:
        non_trend = rebal
        trend_dates = []
    print(f"[regime] {len(trend_dates)}/{len(rebal)} in-trend ({time.time()-t0:.2f}s)")

    # Picks
    picks = compute_picks_sql(con, non_trend)
    print(f"[picks]  {len(picks):,} rows ({time.time()-t0:.2f}s)")

    if trend_dates:
        trend_df = pl.DataFrame({
            "rebal_d": trend_dates,
            "company_code": ["0050"] * len(trend_dates),
            "weight": [1.0] * len(trend_dates),
        })
        picks = pl.concat([picks, trend_df])

    # Daily returns for all held codes
    held_codes = picks["company_code"].unique().to_list()
    rets = fetch_daily_returns(con, start, end, codes=held_codes, market="twse")
    print(f"[rets]   {len(rets):,} daily returns ({time.time()-t0:.2f}s)")

    # Asof join: each day → last rebal STRICTLY BEFORE that day.
    # (Scala Backtester trades at today's close on rebal day; new picks only
    # affect returns from the NEXT trading day. Shift rebal dates +1 day so
    # that on rebal day itself, the anchor points to the PREVIOUS rebal.)
    days_df = pl.DataFrame({"date": days}).sort("date")
    rebal_df = (pl.DataFrame({"active_rebal": rebal})
                .with_columns((pl.col("active_rebal") + pl.duration(days=1)).alias("effective"))
                .sort("effective"))
    days_anchored = days_df.join_asof(
        rebal_df, left_on="date", right_on="effective", strategy="backward"
    )
    contrib = (days_anchored
               .join(picks, left_on="active_rebal", right_on="rebal_d", how="left")
               .join(rets, on=["date", "company_code"], how="left")
               .with_columns((pl.col("weight") * pl.col("ret")).alias("c")))
    port = (contrib
            .group_by("date").agg(pl.col("c").sum().alias("r"))
            .sort("date")
            .with_columns(pl.col("r").fill_null(0.0)))

    # Precise per-rebal turnover: overlap between consecutive pick sets.
    # sold_fraction = (1 - |prev_picks ∩ cur_picks| / TOPN)
    # cost_per_rebal = sold × SELL_TAX + (sold + bought) × COMMISSION
    #                = sold × (SELL_TAX + 2 × COMMISSION)   (equal weight buy=sell)
    picks_by_date = {}
    for row in picks.iter_rows(named=True):
        picks_by_date.setdefault(row["rebal_d"], set()).add(row["company_code"])
    sorted_rebal = sorted(rebal)
    cost_map: dict = {}
    prev_set: set | None = None
    for rd in sorted_rebal:
        cur_set = picks_by_date.get(rd, set())
        if prev_set is None:
            sold_frac = 1.0 if cur_set else 0.0  # first rebal: 100% buy
        else:
            overlap = len(cur_set & prev_set)
            size = max(len(cur_set), 1)
            sold_frac = (size - overlap) / size if cur_set else 0.0
        cost_map[rd] = sold_frac * (SELL_TAX + 2 * COMMISSION)
        prev_set = cur_set

    cost_df = pl.DataFrame({
        "date": list(cost_map.keys()),
        "cost": list(cost_map.values()),
    })
    port = (port.join(cost_df, on="date", how="left")
                .with_columns(pl.col("cost").fill_null(0.0))
                .with_columns((pl.col("r") - pl.col("cost")).alias("net")))

    rets_arr = port["net"].to_numpy()
    navs = capital * np.cumprod(1 + rets_arr)
    print(f"[done]   runtime {time.time()-t0:.2f}s")

    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (navs[-1] / capital) ** (1 / years) - 1
    vol = rets_arr.std(ddof=1) * math.sqrt(TDPY)
    sharpe = (cagr - 0.01) / vol if vol > 0 else 0
    peak, mdd = capital, 0.0
    for v in navs:
        peak = max(peak, v)
        mdd = min(mdd, (v - peak) / peak)

    return {
        "runtime": time.time() - t0,
        "CAGR": cagr, "Sharpe": sharpe, "MDD": mdd,
        "final": float(navs[-1]),
        "total_return": float(navs[-1] / capital - 1),
        # Daily time series (for downstream ensemble / tear-sheet)
        "dates": [d for d in port["date"].to_list()],
        "net_returns": [float(x) for x in rets_arr],
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2018-01-02")
    p.add_argument("--end", default="2026-04-17")
    p.add_argument("--capital", type=float, default=1_000_000)
    p.add_argument("--min-day", type=int, default=1)
    args = p.parse_args()

    print("=== v4 regime_aware (Python) ===")
    v4 = backtest(args.start, args.end, args.min_day, args.capital, use_regime=True)
    print(f"  CAGR:     {v4['CAGR']*100:+.2f}%")
    print(f"  Sharpe:   {v4['Sharpe']:.3f}")
    print(f"  MDD:      {v4['MDD']*100:+.2f}%")
    print(f"  finalNAV: ${v4['final']:,.0f}")

    # 0050 benchmark
    print("\n=== hold_0050 ===")
    con = connect()
    rets0050 = fetch_daily_returns(con, args.start, args.end, codes=["0050"], market="twse").sort("date")
    rets_arr = rets0050["ret"].fill_null(0.0).to_numpy()
    navs = args.capital * np.cumprod(1 + rets_arr)
    days = rets0050["date"].to_list()
    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (navs[-1] / args.capital) ** (1 / years) - 1
    print(f"  CAGR:     {cagr*100:+.2f}%")
    print(f"  finalNAV: ${navs[-1]:,.0f}")
    print(f"\nExcess v4 vs 0050: {(v4['total_return'] - (navs[-1]/args.capital - 1))*100:+.2f} pp")


if __name__ == "__main__":
    main()
