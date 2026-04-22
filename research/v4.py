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
import time
from datetime import date
import numpy as np
import polars as pl

from db import connect

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
      SELECT r.rebal_d, dq.company_code
      FROM rebal r
      JOIN daily_quote dq
        ON regexp_matches(dq.company_code, '^[1-9][0-9]{{3}}$')
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
      SELECT DISTINCT ON (u.rebal_d, u.company_code) u.rebal_d, u.company_code
      FROM uni_clean u
      JOIN qfor qf ON qf.rebal_d = u.rebal_d
      JOIN growth_analysis_ttm gat
        ON gat.company_code = u.company_code
       AND (gat.year * 100 + gat.quarter) <= qf.yq
      WHERE COALESCE(gat.drop_score, 0) < 10
      ORDER BY u.rebal_d, u.company_code, gat.year DESC, gat.quarter DESC
    ),
    pb_med AS (
      SELECT ds.rebal_d, ds.company_code,
             percentile_cont(0.5) WITHIN GROUP (ORDER BY spd.price_book_ratio) AS pbm
      FROM drop_safe ds
      JOIN stock_per_pbr spd
        ON spd.company_code = ds.company_code
       AND spd.date <= ds.rebal_d AND spd.date >= ds.rebal_d - INTERVAL '3 years 6 months'
       AND spd.price_book_ratio > 0
      GROUP BY ds.rebal_d, ds.company_code
    ),
    pb_cur AS (
      SELECT DISTINCT ON (ds.rebal_d, ds.company_code) ds.rebal_d, ds.company_code,
             spd.price_book_ratio AS pb_now
      FROM drop_safe ds
      JOIN stock_per_pbr spd
        ON spd.company_code = ds.company_code
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
    """0050 63-trading-day split-adjusted return per rebal date (Python, lightweight)."""
    if not rebal_ds:
        return {}
    min_d = min(rebal_ds)
    px = con.sql(f"""
        SELECT date, closing_price FROM daily_quote
        WHERE company_code='0050' AND date >= DATE '{min_d}' - INTERVAL '150 days'
          AND closing_price > 0 ORDER BY date
    """).pl()
    dates = px["date"].to_list()
    prices = px["closing_price"].to_list()

    splits = []
    for i in range(1, len(dates)):
        gap = (dates[i] - dates[i - 1]).days
        if prices[i] > 0 and 3 <= gap <= 14:
            ratio = prices[i - 1] / prices[i]
            if 2.5 <= ratio <= 15 or 0.067 <= ratio <= 0.4:
                splits.append((dates[i], ratio))

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
        s_d, e_d = dates[idx - 63], dates[idx]
        s_p, e_p = prices[idx - 63], prices[idx]
        adj = 1.0
        for sd, sf in splits:
            if sd > s_d and sd <= e_d:
                adj *= sf
        result[rebal_d] = e_p / (s_p / adj) - 1.0
    return result


def compute_daily_returns_sql(con, start, end, held_codes):
    """Split-safe DRIP-adjusted daily return in one SQL."""
    codes_sql = ",".join(f"'{c}'" for c in held_codes)
    return con.sql(f"""
    WITH px AS (
      SELECT company_code, date, closing_price,
             LAG(closing_price) OVER (PARTITION BY company_code ORDER BY date) AS prev_close,
             LAG(date) OVER (PARTITION BY company_code ORDER BY date) AS prev_date
      FROM daily_quote
      WHERE company_code IN ({codes_sql})
        AND date BETWEEN DATE '{start}' - INTERVAL '10 days' AND DATE '{end}'
        AND closing_price > 0
    )
    SELECT px.company_code, px.date,
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
    """).pl()


def backtest(start: str, end: str, min_day: int, capital: float, use_regime: bool = True):
    con = connect()
    t0 = time.time()

    days = [r[0] for r in con.sql(f"""
        SELECT date FROM daily_quote WHERE company_code='0050'
          AND date BETWEEN DATE '{start}' AND DATE '{end}' ORDER BY date
    """).fetchall()]
    rebal = [r[0] for r in con.sql(f"""
        SELECT MIN(date) FROM daily_quote WHERE company_code='0050'
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
    rets = compute_daily_returns_sql(con, start, end, held_codes)
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
    rets0050 = compute_daily_returns_sql(con, args.start, args.end, ["0050"]).sort("date")
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
