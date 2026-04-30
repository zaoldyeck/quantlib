"""Event-driven daily signal engine v1 — foundation for news-driven extension.

Contrast with v4 (monthly rebalance) and chase (price breakout):
  * Daily scan of universe (TWSE).
  * Entry = **condition transition** (false yesterday → true today), not calendar.
  * Exit multi-condition OR: trailing stop 15% + time stop 120d + factor stale.
  * Universe TWSE-only (empirical: TPEx fails v4 AND chase; needs own strategy).

Starting factor (v1): `vol_surge_silent_accumulation` — 5d avg trade_value /
60d avg trade_value > threshold AND price 5d ret < 5%. Captures the 5201 奇偶
/ 3672 康聯訊 pattern where volume expands 5+ weeks before price breaks out.

Subsequent versions will layer revenue_yoy_accel, pbr_deep_value,
sector_laggard — see `project_spike_case_studies.md` case synthesis.

Cost / universe / v4-parity assumptions inherited from chase_trailing_stop.py
(2-折 fees baked symmetric, 50M ADV min, equal-weight 10 slots).

Usage:
    uv run --project research python research/experiments/event_driven_v1.py
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
    load_price_panel, run_chase, compute_split_safe_drip_returns,
    metrics, print_metrics, extract_returns,
    FEES_SYM, INIT_CAPITAL,
)

START = "2015-01-02"
END = "2026-04-17"
OUT_DIR = "research/experiments/out"
os.makedirs(OUT_DIR, exist_ok=True)


def build_vol_surge_signal(
    prices: pd.DataFrame,
    tv: pd.DataFrame,
    short_window: int = 5,
    long_window: int = 60,
    surge_threshold: float = 3.0,
    silent_price_max: float = 0.05,
    adv_window: int = 30,
    adv_thresh: float = 50_000_000,
) -> pd.DataFrame:
    """Silent accumulation signal.

    Trigger conditions (all must hold):
      1. `tv_{short_window}d_avg / tv_{long_window}d_avg >= surge_threshold`
         (volume expanding 3x faster than baseline)
      2. `|price_{short_window}d_return| <= silent_price_max`
         (price still flat — accumulation not yet in price)
      3. 30-day median ADV >= 50M NTD (liquidity gate)

    Returns wide DataFrame of booleans. First-True transition applied (a stock
    doesn't fire the same signal day after day).
    """
    # Volume: short vs long
    tv_short = tv.rolling(short_window, min_periods=max(2, short_window // 2)).mean()
    tv_long = tv.rolling(long_window, min_periods=max(10, long_window // 3)).mean()
    ratio = tv_short / tv_long

    # Price: still flat
    price_ret_short = prices / prices.shift(short_window) - 1

    # Liquidity
    adv = tv.rolling(adv_window, min_periods=10).median()

    raw = (
        (ratio >= surge_threshold)
        & (price_ret_short.abs() <= silent_price_max)
        & (adv >= adv_thresh)
    )
    raw = raw.fillna(False)

    # First-True transition only (avoid retriggering every day)
    first_true = raw & ~raw.shift(1, fill_value=False)
    return first_true.fillna(False)


def build_exits_signal(
    entries: pd.DataFrame,
    prices: pd.DataFrame,
    time_stop_days: int = 120,
) -> pd.DataFrame:
    """Time-stop exit signal — for each entry date, generate exit True at
    entry_date + time_stop_days. Trailing stop is handled natively by vectorbt
    via sl_stop=0.15 sl_trail=True."""
    exits = pd.DataFrame(False, index=entries.index, columns=entries.columns)
    for col in entries.columns:
        entry_dates = entries.index[entries[col]]
        for ed in entry_dates:
            # Find the trading day `time_stop_days` after ed
            try:
                iloc = entries.index.get_loc(ed)
                if iloc + time_stop_days < len(entries.index):
                    exits.iat[iloc + time_stop_days, exits.columns.get_loc(col)] = True
            except KeyError:
                pass
    return exits


def run_event_driven(
    prices: pd.DataFrame,
    entries: pd.DataFrame,
    exits: pd.DataFrame,
    trail_stop: float = 0.15,
    slot_dollars: float = 100_000,
    fees: float = FEES_SYM,
    init_cash: float = INIT_CAPITAL,
):
    """Event-driven backtest via vectorbt.

    - entries: condition first-True (per stock per day)
    - exits: OR of (trailing stop via sl_stop) + (explicit time stop signal)
    """
    pf = vbt.Portfolio.from_signals(
        close=prices,
        entries=entries,
        exits=exits,
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


def main():
    t0 = time.time()
    con = connect()

    # 1. Load universe (TWSE only; empirical — TPEx destroys this strategy class)
    prices, tv = load_price_panel(con, START, END)
    print(f"[data] prices: {prices.shape}, tv: {tv.shape} ({time.time()-t0:.1f}s)")

    # 2. Benchmarks
    bench_0050 = compute_split_safe_drip_returns(con, "0050", START, END)
    m_0050 = metrics(bench_0050, "hold_0050 benchmark")
    print()
    print_metrics(m_0050)

    # 3. Parameter sweep: surge threshold × silent-price constraint × trail
    print("\n[sweep] vol_surge parameter grid ...")
    configs = []
    for surge_thresh in [3.0, 5.0, 10.0]:
        for silent_max in [0.05, 0.10, 0.30]:  # 0.30 ≈ "no constraint"
            for trail in [0.10, 0.15, 0.20]:
                entries = build_vol_surge_signal(
                    prices, tv,
                    surge_threshold=surge_thresh,
                    silent_price_max=silent_max,
                )
                n_entries = int(entries.sum().sum())
                if n_entries < 50:
                    # Too few signals, skip
                    continue
                exits = build_exits_signal(entries, prices, time_stop_days=120)
                pf = run_event_driven(prices, entries, exits,
                                      trail_stop=trail, slot_dollars=100_000)
                ret = extract_returns(pf)
                m = metrics(ret,
                            f"surge={surge_thresh} silent={silent_max} trail={trail}",
                            bench_ret=bench_0050)
                m.update({"surge": surge_thresh, "silent": silent_max,
                          "trail": trail, "n_entries": n_entries})
                configs.append(m)

    df = pd.DataFrame(configs)
    print(f"\n[sweep] {len(df)} configs tested ({time.time()-t0:.1f}s)")

    print("\n=== TOP 10 by CAGR ===")
    for _, r in df.sort_values("CAGR", ascending=False).head(10).iterrows():
        print(f"  surge {r['surge']:4.1f}  silent {r['silent']:4.2f}  trail {r['trail']:4.2f}  "
              f"n={int(r['n_entries']):>5}  CAGR {r['CAGR']*100:+6.2f}%  "
              f"Sharpe {r['Sharpe']:5.2f}  MDD {r['MDD']*100:+6.1f}%")

    print("\n=== TOP 5 by Sharpe ===")
    for _, r in df.sort_values("Sharpe", ascending=False).head(5).iterrows():
        print(f"  surge {r['surge']:4.1f}  silent {r['silent']:4.2f}  trail {r['trail']:4.2f}  "
              f"Sharpe {r['Sharpe']:5.2f}  CAGR {r['CAGR']*100:+6.2f}%  "
              f"MDD {r['MDD']*100:+6.1f}%")

    # Save sweep
    out_path = os.path.join(OUT_DIR, "event_driven_v1_sweep.parquet")
    df.to_parquet(out_path)
    print(f"\n[out] sweep → {out_path}")
    print(f"[total] {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
