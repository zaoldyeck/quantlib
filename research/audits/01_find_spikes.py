"""Identify historical 'spike' events on TWSE stocks.

A spike = stock whose price from day T closes up by >= min_gain over the next
window_days trading days. Split / ex-right events are excluded (they cause
fake spikes).

Usage:
    uv run research/audits/01_find_spikes.py --min-gain 0.80 --window 60

Writes `research/out/spikes.parquet` for downstream scripts.
"""
from __future__ import annotations

import argparse
import os
import sys

import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect


def find_spikes(
    con,
    min_gain: float,
    window_days: int,
    start: str,
    end: str,
    markets: tuple[str, ...] = ("twse", "tpex"),
) -> pl.DataFrame:
    """Return one row per spike event: (market, company_code, date, closing_price,
    px_future, gain). `date` is the entry (T=0) date; `px_future` is close N
    trading days later; gain = pct return (split-adjusted via event exclusion).

    Default universe includes both TWSE + TPEx because breakout / spike stocks
    are heavily concentrated in TPEx small-caps.
    """
    markets_sql = ",".join(f"'{m}'" for m in markets)
    q = f"""
    WITH px AS (
      SELECT market, company_code, date, closing_price,
             LEAD(closing_price, {window_days}) OVER (
               PARTITION BY market, company_code ORDER BY date
             ) AS px_future,
             LEAD(date, {window_days}) OVER (
               PARTITION BY market, company_code ORDER BY date
             ) AS date_future
      FROM daily_quote
      WHERE market IN ({markets_sql})
        AND date BETWEEN '{start}' AND '{end}'
        AND regexp_matches(company_code, '^[1-9][0-9]{{3}}$')
        AND closing_price > 0
    ),
    -- Exclude windows containing split / ex-right events (these inflate gain).
    events AS (
      SELECT market, company_code, date FROM ex_right_dividend
      UNION ALL
      SELECT market, company_code, date FROM capital_reduction
    )
    SELECT
      px.market,
      px.company_code,
      px.date,
      px.closing_price,
      px.px_future,
      (px.px_future / px.closing_price - 1) AS gain
    FROM px
    WHERE px.px_future IS NOT NULL
      AND px.closing_price > 0
      AND (px.px_future / px.closing_price - 1) >= {min_gain}
      AND NOT EXISTS (
        SELECT 1 FROM events e
        WHERE e.market = px.market
          AND e.company_code = px.company_code
          AND e.date > px.date AND e.date <= px.date_future
      )
    ORDER BY gain DESC
    """
    return con.sql(q).pl()


def dedupe_overlapping(events: pl.DataFrame, cooldown_days: int = 30) -> pl.DataFrame:
    """Keep only the first event per company within any cooldown-day window.
    A single uptrend produces many overlapping qualifying entries; take only
    the earliest.
    """
    return (
        events.sort(["company_code", "date"])
        .with_columns(
            # Compute diff from previous event's date within each company
            (pl.col("date") - pl.col("date").shift(1).over("company_code")).dt.total_days()
            .fill_null(10**9)
            .alias("days_since_prev")
        )
        .filter(pl.col("days_since_prev") >= cooldown_days)
        .drop("days_since_prev")
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--min-gain", type=float, default=0.80, help="Minimum gain as decimal (0.80 = 80%)")
    p.add_argument("--window", type=int, default=60, help="Trading days for the gain window")
    p.add_argument("--start", default="2015-01-01")
    p.add_argument("--end", default="2025-12-31")
    p.add_argument("--cooldown", type=int, default=30, help="Cooldown days to dedupe overlapping events")
    p.add_argument("--out", default=None, help="Output parquet path (default: research/out/spikes_<params>.parquet)")
    args = p.parse_args()

    con = connect()

    print(f"[01] scanning TWSE {args.start} → {args.end} for {args.window}-day gains >= {args.min_gain*100:.0f}% ...")
    raw = find_spikes(con, args.min_gain, args.window, args.start, args.end)
    events = dedupe_overlapping(raw, args.cooldown)

    n_raw = len(raw)
    n_events = len(events)
    n_companies = events["company_code"].n_unique()
    print(f"[01] raw qualifying rows: {n_raw:,}")
    print(f"[01] after cooldown dedupe ({args.cooldown}d): {n_events:,} events across {n_companies} companies")
    print()

    print("=== Top 20 spikes by gain ===")
    with pl.Config(tbl_rows=20, tbl_width_chars=120):
        print(events.sort("gain", descending=True).head(20))

    print()
    print("=== Events per year ===")
    by_year = (
        events.with_columns(pl.col("date").dt.year().alias("year"))
        .group_by("year").agg(pl.len().alias("n_events"))
        .sort("year")
    )
    with pl.Config(tbl_rows=30):
        print(by_year)

    print()
    print("=== Top 10 companies by event count ===")
    by_company = (
        events.group_by("company_code").agg([
            pl.len().alias("n_events"),
            pl.col("gain").max().alias("max_gain"),
            pl.col("gain").mean().alias("avg_gain"),
        ]).sort("n_events", descending=True)
    )
    with pl.Config(tbl_rows=10):
        print(by_company.head(10))

    out_path = args.out or f"research/out/spikes_g{int(args.min_gain*100)}_w{args.window}.parquet"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    events.write_parquet(out_path)
    print(f"\n[01] saved {n_events:,} events → {out_path}")


if __name__ == "__main__":
    main()
