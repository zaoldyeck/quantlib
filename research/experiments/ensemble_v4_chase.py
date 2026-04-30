"""Ensemble test: v4 RegimeAware + best chase variant.

Hypothesis: chase (price action) and v4 (value + drop filter) capture different
alpha sources. Their daily returns should be low-correlated. A 50/50 blend
should keep CAGR near mid-point while reducing MDD via diversification.

Steps:
  1. Reuse chase_trailing_stop.py's best config to produce daily returns.
  2. Run v4.backtest(); extract daily returns.
  3. Align dates, compute correlation.
  4. Compose blended daily returns (weighted sum) and report metrics.
  5. Sweep blend weights to find best risk-adjusted mix.
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

# Allow importing sibling db.py + strat_lab/v4.py (v4 was moved 2026-04-30)
HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", "strat_lab"))
from db import connect  # noqa: E402
import v4 as v4_mod  # noqa: E402

# Reuse helpers from chase_trailing_stop.py
sys.path.insert(0, HERE)
from chase_trailing_stop import (  # noqa: E402
    load_price_panel, build_entries_return_breakout, run_chase,
    compute_split_safe_drip_returns, metrics, print_metrics, extract_returns,
    FEES_SYM, INIT_CAPITAL,
)

# Longest-common window: v4 needs 3.5y pbBand history (stock_per_pbr from
# 2005-09) → earliest viable = 2009-01. Chase can go back to 2004 but blend
# is constrained by v4's warmup requirement.
START = "2009-01-02"
END   = "2026-04-17"
OUT_DIR = "research/experiments/out"
os.makedirs(OUT_DIR, exist_ok=True)


def run_best_chase(con, start, end):
    """Recompute best chase config (60d+80% entry, 15% trail, 5 slots / $200K)
    over the given window."""
    prices, tv = load_price_panel(con, start, end)
    entries = build_entries_return_breakout(
        prices, tv, lookback=60, ret_thresh=0.80,
        adv_window=30, adv_thresh=50_000_000,
    )
    pf = run_chase(prices, entries, trail_stop=0.15, slot_dollars=200_000)
    return extract_returns(pf)


def blend_returns(r_a: pd.Series, r_b: pd.Series, w_a: float) -> pd.Series:
    """Linear blend of two daily return series on common dates."""
    aligned = pd.DataFrame({"a": r_a, "b": r_b}).dropna(how="any")
    return w_a * aligned["a"] + (1 - w_a) * aligned["b"]


def main():
    t0 = time.time()
    con = connect()

    # --- 1. Run v4 baseline, get daily returns
    print(f"[1] running v4 baseline backtest ({START} → {END}) ...")
    v4_result = v4_mod.backtest(START, END, min_day=1,
                                 capital=INIT_CAPITAL, use_regime=True)
    v4_dates = pd.to_datetime(v4_result["dates"])
    v4_ret = pd.Series(v4_result["net_returns"], index=v4_dates, name="v4")
    print(f"    v4 CAGR: {v4_result['CAGR']*100:+.2f}% / Sharpe {v4_result['Sharpe']:.2f} / MDD {v4_result['MDD']*100:+.2f}%")
    print(f"    ({time.time()-t0:.1f}s)")

    # --- 2. Run best chase, get daily returns
    print(f"\n[2] running best chase (60d+80%, trail=15%, $200K slots) ...")
    chase_ret = run_best_chase(con, START, END)
    chase_ret.name = "chase"
    m_chase = metrics(chase_ret, "chase 60d+80%/15%/$200K")
    print(f"    chase CAGR: {m_chase['CAGR']*100:+.2f}% / Sharpe {m_chase['Sharpe']:.2f} / MDD {m_chase['MDD']*100:+.2f}%")
    print(f"    ({time.time()-t0:.1f}s)")

    # --- 3. Correlation & stats
    df = pd.concat([v4_ret, chase_ret], axis=1, join="inner")
    corr = df["v4"].corr(df["chase"])
    print(f"\n[3] daily-return correlation: {corr:.3f}  (aligned N={len(df)})")
    if corr > 0.7:
        print("    > 0.7 — strategies are duplicates, ensemble won't help much")
    elif corr < 0.3:
        print("    < 0.3 — strategies are nearly orthogonal, ensemble should shine")
    else:
        print("    0.3–0.7 moderate — ensemble may help")

    # --- 4. Benchmarks + blend sweep
    print("\n[4] blend weight sweep ...")
    bench_0050 = compute_split_safe_drip_returns(con, "0050", START, END)

    results = []
    weights = np.linspace(0.0, 1.0, 11)  # 0, 10, ..., 100% v4
    for w_v4 in weights:
        blend = blend_returns(df["v4"], df["chase"], w_a=w_v4)
        m = metrics(blend, f"blend: {int(w_v4*100)}% v4 + {int((1-w_v4)*100)}% chase",
                    bench_ret=bench_0050)
        m["w_v4"] = w_v4
        results.append(m)

    res_df = pd.DataFrame(results)
    print(f"\n{'w_v4':>5} {'CAGR':>8} {'Sharpe':>7} {'Sortino':>8} {'MDD':>8} {'Calmar':>7} {'Excess':>8}")
    for _, r in res_df.iterrows():
        print(f"{r['w_v4']*100:>4.0f}%  {r['CAGR']*100:+6.2f}%  {r['Sharpe']:6.2f}  {r['Sortino']:7.2f}  {r['MDD']*100:+6.2f}% {r['Calmar']:6.2f}  {r.get('ExcessAnnual', 0)*100:+6.2f}%")

    # Highlight best by each metric
    print("\n=== Best blends ===")
    for by in ["CAGR", "Sharpe", "Calmar"]:
        row = res_df.loc[res_df[by].idxmax()]
        print(f"  best {by:6}: w_v4={row['w_v4']*100:.0f}% → CAGR {row['CAGR']*100:+.2f}%, Sharpe {row['Sharpe']:.2f}, MDD {row['MDD']*100:+.2f}%")

    # Save for downstream pyfolio
    out_path = os.path.join(OUT_DIR, "ensemble_blend.parquet")
    res_df.to_parquet(out_path)
    # Save best blend's daily return
    best_w = res_df.loc[res_df["Sharpe"].idxmax(), "w_v4"]
    best_blend = blend_returns(df["v4"], df["chase"], w_a=best_w)
    pd.DataFrame({"ret": best_blend}).to_parquet(os.path.join(OUT_DIR, "ensemble_best_returns.parquet"))
    print(f"\n[out] sweep → {out_path}")
    print(f"[out] best-Sharpe blend returns → {OUT_DIR}/ensemble_best_returns.parquet")
    print(f"\n[total] {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
