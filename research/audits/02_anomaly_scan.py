"""Scan daily_quote for dates where closing_price has an impossible jump.

A 'market-wide anomaly' is a date where >= min_stocks stocks show a
day-over-day close ratio outside [0.5, 2.0] without a corresponding
ex_right_dividend or capital_reduction event. These dates indicate that
the crawler captured a partial/test/stale CSV instead of the final
end-of-day data.

Output: research/out/anomaly_dates.parquet — one row per suspicious date
with per-market stats. Review then feed to 03_refetch_anomalies.py.
"""
from __future__ import annotations
import argparse
import os
import sys

import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect


def scan(con, min_stocks: int = 20, market: str = "twse") -> pl.DataFrame:
    """Return dates where >= min_stocks stocks have anomalous close ratios."""
    q = f"""
    WITH with_prev AS (
      SELECT
        company_code, market, date, closing_price,
        LAG(closing_price) OVER (
          PARTITION BY company_code, market ORDER BY date
        ) AS prev_close,
        LAG(date) OVER (
          PARTITION BY company_code, market ORDER BY date
        ) AS prev_date
      FROM pg.public.daily_quote
      WHERE market = '{market}'
        AND company_code ~ '^[1-9][0-9]{{3}}$'
        AND closing_price > 0
    ),
    anomalies AS (
      SELECT w.market, w.date, w.company_code, w.closing_price, w.prev_close,
             w.closing_price / w.prev_close AS ratio
      FROM with_prev w
      WHERE w.prev_close IS NOT NULL AND w.prev_close > 0
        AND (w.date - w.prev_date) <= 7       -- close in time (no long halt)
        AND (w.closing_price / w.prev_close < 0.5
             OR w.closing_price / w.prev_close > 2.0)
        -- Exclude genuine corporate actions:
        AND NOT EXISTS (
          SELECT 1 FROM pg.public.ex_right_dividend e
          WHERE e.market = w.market AND e.company_code = w.company_code
            AND e.date > w.prev_date AND e.date <= w.date
        )
        AND NOT EXISTS (
          SELECT 1 FROM pg.public.capital_reduction c
          WHERE c.market = w.market AND c.company_code = w.company_code
            AND c.date > w.prev_date AND c.date <= w.date
        )
    ),
    ranked AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY date, market ORDER BY ratio) AS rn
      FROM anomalies
    )
    SELECT date, market,
           COUNT(*) AS n_anomalies,
           MIN(ratio) AS min_ratio,
           MAX(ratio) AS max_ratio,
           AVG(ratio) AS avg_ratio,
           ARRAY_AGG(company_code) FILTER (WHERE rn <= 5) AS sample_codes
    FROM ranked
    GROUP BY date, market
    HAVING COUNT(*) >= {min_stocks}
    ORDER BY date
    """
    return con.sql(q).pl()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--min-stocks", type=int, default=20,
                   help="Min anomalous stocks/day to flag as market-wide bug")
    p.add_argument("--markets", nargs="+", default=["twse", "tpex"])
    args = p.parse_args()

    con = connect()

    all_dates: list[pl.DataFrame] = []
    for mkt in args.markets:
        print(f"[02] scanning {mkt} …")
        df = scan(con, args.min_stocks, mkt)
        print(f"[02] {mkt}: {len(df)} anomaly dates (>= {args.min_stocks} stocks)")
        all_dates.append(df)

    combined = pl.concat(all_dates) if all_dates else pl.DataFrame()

    if len(combined) == 0:
        print("[02] no anomalies found — clean data or threshold too high")
        return

    print()
    print("=== All anomaly dates ===")
    with pl.Config(tbl_rows=200, tbl_width_chars=160, fmt_str_lengths=80):
        print(combined.sort("date"))

    out = "research/out/anomaly_dates.parquet"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    combined.write_parquet(out)
    print(f"\n[02] saved {len(combined)} dates → {out}")


if __name__ == "__main__":
    main()
