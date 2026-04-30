"""Event-driven v2 — composite signal (vol_surge × revenue_yoy_positive).

V1 finding: pure vol_surge CAGR only +11.5%, losing 0050 by 8pp. Top-15 case
studies confirmed "every real spike has 2-3 signals aligned, not just one".

V2 adds a fundamental quality gate: entry must have latest PIT-safe monthly
revenue YoY >= threshold. This captures the 3669 / 4426 / 6204 / 5314 /
3715 pattern where revenue acceleration validated the volume-signal.

Also exposes more aggressive parameters: tighter surge, larger N_slots, more
granular YoY gates.

Usage:
    uv run --project research python research/experiments/event_driven_v2.py
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

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))
from db import connect  # noqa: E402

sys.path.insert(0, HERE)
from chase_trailing_stop import (  # noqa: E402
    load_price_panel, compute_split_safe_drip_returns,
    metrics, print_metrics, extract_returns,
    FEES_SYM, INIT_CAPITAL,
)
from event_driven_v1 import build_vol_surge_signal, build_exits_signal, run_event_driven  # noqa: E402

START = "2015-01-02"
END = "2026-04-17"
OUT_DIR = "research/experiments/out"
os.makedirs(OUT_DIR, exist_ok=True)


def load_revenue_pit_lookup(con) -> pl.DataFrame:
    """Build (company_code, pub_date, yoy) rows for PIT-safe latest-revenue
    asof-join. pub_date = next-month 10th + 3 days safety buffer (= day 13).
    Only TWSE."""
    q = """
    SELECT
      company_code,
      make_date(
        CASE WHEN month = 12 THEN year + 1 ELSE year END,
        CASE WHEN month = 12 THEN 1 ELSE month + 1 END,
        13
      ) AS pub_date,
      monthly_revenue_yoy AS yoy
    FROM operating_revenue
    WHERE market = 'twse'
      AND monthly_revenue_yoy IS NOT NULL
    """
    df = con.sql(q).pl().sort("pub_date")
    return df


def apply_revenue_gate(
    entries: pd.DataFrame,
    rev_events: pl.DataFrame,
    min_yoy: float = 20.0,
) -> pd.DataFrame:
    """Zero out entries where latest available monthly revenue YoY < min_yoy.

    Uses Polars join_asof (binary search) instead of per-entry SQL — fast for
    thousands of entries.
    """
    # Collect all entry (code, date) pairs
    entry_rows = []
    for code in entries.columns:
        dates = entries.index[entries[code]]
        for d in dates:
            entry_rows.append((code, d.date() if hasattr(d, 'date') else d))
    if not entry_rows:
        return entries

    entry_df = pl.DataFrame(
        entry_rows, schema=["company_code", "entry_date"], orient="row"
    ).with_columns(pl.col("entry_date").cast(pl.Date))

    # asof join — for each entry, find the latest rev event with pub_date <= entry_date
    joined = entry_df.sort("entry_date").join_asof(
        rev_events, left_on="entry_date", right_on="pub_date",
        by="company_code", strategy="backward",
    )

    # Keep only entries passing the YoY gate
    valid = joined.filter(pl.col("yoy").is_not_null() & (pl.col("yoy") >= min_yoy))

    # Build filtered boolean DataFrame
    filtered = pd.DataFrame(False, index=entries.index, columns=entries.columns)
    for row in valid.iter_rows(named=True):
        code = row["company_code"]
        date = row["entry_date"]
        # date is a Python datetime.date; convert to pandas Timestamp for indexing
        ts = pd.Timestamp(date)
        if ts in filtered.index and code in filtered.columns:
            filtered.at[ts, code] = True
    return filtered


def main():
    t0 = time.time()
    con = connect()

    prices, tv = load_price_panel(con, START, END)
    print(f"[data] prices: {prices.shape}, tv: {tv.shape} ({time.time()-t0:.1f}s)")

    bench_0050 = compute_split_safe_drip_returns(con, "0050", START, END)
    m_0050 = metrics(bench_0050, "hold_0050 benchmark")
    print()
    print_metrics(m_0050)

    # Precompute revenue events
    print("\n[data] loading PIT-safe revenue events ...")
    rev_events = load_revenue_pit_lookup(con)
    print(f"[data] {len(rev_events)} revenue events ({time.time()-t0:.1f}s)")

    # Sweep composite signal params
    print("\n[sweep] vol_surge × revenue_yoy composite ...")
    configs = []
    for surge_thresh in [3.0, 5.0, 10.0]:
        for min_yoy in [0.0, 20.0, 50.0]:  # 0 = no gate
            for trail in [0.10, 0.15, 0.20]:
                for slot_dollars in [50_000, 100_000, 200_000]:
                    # Build vol_surge with "silent" fully open (silent_max=0.30 ≈ no constraint)
                    entries = build_vol_surge_signal(
                        prices, tv,
                        surge_threshold=surge_thresh,
                        silent_price_max=0.30,
                    )
                    if min_yoy > 0:
                        entries = apply_revenue_gate(entries, rev_events, min_yoy=min_yoy)
                    n_entries = int(entries.sum().sum())
                    if n_entries < 30:
                        continue
                    exits = build_exits_signal(entries, prices, time_stop_days=120)
                    pf = run_event_driven(
                        prices, entries, exits,
                        trail_stop=trail, slot_dollars=slot_dollars,
                    )
                    ret = extract_returns(pf)
                    m = metrics(
                        ret,
                        f"surge={surge_thresh} yoy={min_yoy} trail={trail} slot={slot_dollars//1000}K",
                        bench_ret=bench_0050,
                    )
                    m.update({
                        "surge": surge_thresh, "min_yoy": min_yoy,
                        "trail": trail, "slot_dollars": slot_dollars,
                        "n_entries": n_entries,
                    })
                    configs.append(m)
    df = pd.DataFrame(configs)
    print(f"[sweep] {len(df)} configs tested ({time.time()-t0:.1f}s)")

    print("\n=== TOP 15 by CAGR ===")
    for _, r in df.sort_values("CAGR", ascending=False).head(15).iterrows():
        print(f"  surge {r['surge']:4.1f}  yoy {r['min_yoy']:4.0f}%  trail {r['trail']:4.2f}  "
              f"slot ${int(r['slot_dollars'])//1000:>3}K  n={int(r['n_entries']):>5}  "
              f"CAGR {r['CAGR']*100:+6.2f}%  Sharpe {r['Sharpe']:5.2f}  "
              f"MDD {r['MDD']*100:+6.1f}%")

    print("\n=== TOP 10 by Sharpe ===")
    for _, r in df.sort_values("Sharpe", ascending=False).head(10).iterrows():
        print(f"  surge {r['surge']:4.1f}  yoy {r['min_yoy']:4.0f}%  trail {r['trail']:4.2f}  "
              f"slot ${int(r['slot_dollars'])//1000:>3}K  Sharpe {r['Sharpe']:5.2f}  "
              f"CAGR {r['CAGR']*100:+6.2f}%  MDD {r['MDD']*100:+6.1f}%")

    out_path = os.path.join(OUT_DIR, "event_driven_v2_sweep.parquet")
    df.to_parquet(out_path)
    print(f"\n[out] sweep → {out_path}")
    print(f"[total] {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
