"""Comprehensive hybrid strategy sweep — re-validates "5+5 is optimal" claim with prices.py.

After 2026-04-30 prices.py refactor (DRIP back-adjust), all sub-strategy NAVs
shifted. The previous ship verdict (5+5 NAV 80/20 with C+B) was based on raw
daily_quote NAV — needs full re-validation.

This script:
  1. Generates sub-strategy NAVs for all variants (5 rankers × monthly × TPEx for iter_13;
     max ∈ {1,3,5,7,10} × ATR{on,off} for iter_24)
  2. Sweeps hybrid combinations: slot ratios {1+9, 2+8, ..., 9+1, 0+10, 10+0} × NAV weights
     {50/50, 60/40, 70/30, 80/20, 90/10}
  3. Computes IS metrics (CAGR / Sortino / MDD / Sharpe) for each combination
  4. Outputs: research/strat_lab/results/hybrid_sweep_v6.csv (sortable verdict table)

Hard constraint enforced: total hold count ≤ 10 (slot_a + slot_b ≤ 10).

Run:
    uv run --project research python research/strat_lab/sweep_hybrid.py
"""
from __future__ import annotations

import math
import os
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

HERE = Path(__file__).parent
RESULTS = HERE / "results"
RESULTS.mkdir(exist_ok=True)

START = "2005-01-03"
END = "2026-04-25"
CAPITAL = 1_000_000
TDPY = 252
RF = 0.01

# ─── Sub-strategy variants to generate ─────────────────────────────
ITER13_RANKERS = ["mcap", "roa_recent", "roa_med", "rev_cagr5y", "composite"]
ITER24_MAX_POSITIONS = [1, 3, 5, 7, 10]
ITER24_ATR = [False, True]   # False = fixed -15%, True = ATR-based

# ─── Hybrid sweep grid ─────────────────────────────────────────────
# Each tuple = (slot_a_topn, slot_b_max), constraint: slot_a + slot_b ≤ 10
SLOT_CONFIGS = [
    (1, 9), (2, 8), (3, 7), (4, 6), (5, 5),
    (6, 4), (7, 3), (8, 2), (9, 1),
    (10, 0),  # pure quality
    (0, 10),  # pure catalyst
]
NAV_WEIGHTS = [0.50, 0.60, 0.70, 0.80, 0.85, 0.90]   # weight for slot A


def run_cmd(cmd: list[str]) -> int:
    """Run subprocess, return exit code."""
    try:
        result = subprocess.run(cmd, cwd=HERE.parent.parent,
                                  check=False, timeout=600,
                                  capture_output=True, text=True)
        return result.returncode
    except subprocess.TimeoutExpired:
        return 1


def gen_iter13_nav(ranker: str) -> str:
    """Generate iter_13 monthly TPEx NAV for given ranker. Returns CSV path."""
    suffix = f"monthly_{ranker}_tpex"
    out_csv = RESULTS / f"iter_13_{suffix}_daily.csv"
    if out_csv.exists():
        return str(out_csv)
    print(f"  [gen] iter_13 monthly {ranker} TPEx ...")
    cmd = ["uv", "run", "--project", "research", "python",
           str(HERE / "iter_13.py"),
           "--start", START, "--end", END,
           "--freq", "monthly", "--ranker", ranker,
           "--universe", "twse_tpex",
           "--mode", "mcap",
           "--suffix", suffix]
    rc = run_cmd(cmd)
    if rc != 0 or not out_csv.exists():
        raise RuntimeError(f"iter_13 generation failed for ranker={ranker} rc={rc}")
    return str(out_csv)


def gen_iter24_nav(max_pos: int, atr: bool) -> str:
    """Generate iter_24 NAV. Returns CSV path."""
    if max_pos == 0:
        return None  # no catalyst slot
    suffix = f"max{max_pos}" + ("_atr" if atr else "")
    out_csv = RESULTS / f"iter_24_{suffix}_daily.csv"
    if out_csv.exists():
        return str(out_csv)
    print(f"  [gen] iter_24 max={max_pos} atr={atr} ...")
    cmd = ["uv", "run", "--project", "research", "python",
           str(HERE / "iter_24.py"),
           "--start", START, "--end", END,
           "--max-positions", str(max_pos)]
    if atr:
        cmd.append("--atr-trailing")
    rc = run_cmd(cmd)
    if rc != 0 or not out_csv.exists():
        raise RuntimeError(f"iter_24 generation failed max={max_pos} atr={atr} rc={rc}")
    return str(out_csv)


def compute_metrics(nav_arr: np.ndarray, days: list[date], capital: float = CAPITAL) -> dict:
    """Compute CAGR / Sharpe / Sortino / MDD."""
    rets = np.diff(np.concatenate([[capital], nav_arr])) / np.concatenate([[capital], nav_arr[:-1]])
    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (nav_arr[-1] / capital) ** (1 / years) - 1
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY))
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - RF) / vol if vol > 0 else 0
    sortino = (cagr - RF) / downvol if downvol > 0 else 0
    peak, mdd = capital, 0.0
    for v in nav_arr:
        peak = max(peak, v)
        mdd = min(mdd, (v - peak) / peak)
    return dict(cagr=cagr, sharpe=sharpe, sortino=sortino, mdd=mdd, vol=vol,
                final_nav=float(nav_arr[-1]))


def hybrid_blend(nav_a_csv: str | None, nav_b_csv: str | None,
                  w_a: float, capital: float = CAPITAL) -> pl.DataFrame:
    """Year-end rebalanced 80/20-style blend of two NAV streams.

    Args:
      nav_a_csv: path to slot A NAV CSV (or None for pure-B)
      nav_b_csv: path to slot B NAV CSV (or None for pure-A)
      w_a: weight for slot A (1 - w_a goes to slot B)

    Returns:
      DataFrame with columns (date, nav)
    """
    if nav_a_csv and nav_b_csv:
        n_a = pl.read_csv(nav_a_csv, try_parse_dates=True).sort("date").select(["date", pl.col("nav").alias("nav_a")])
        n_b = pl.read_csv(nav_b_csv, try_parse_dates=True).sort("date").select(["date", pl.col("nav").alias("nav_b")])
        df = n_a.join(n_b, on="date", how="inner").sort("date")
    elif nav_a_csv:
        df = (pl.read_csv(nav_a_csv, try_parse_dates=True).sort("date")
              .select(["date", pl.col("nav").alias("nav_a")])
              .with_columns(pl.lit(capital).alias("nav_b")))
    else:
        df = (pl.read_csv(nav_b_csv, try_parse_dates=True).sort("date")
              .select(["date", pl.col("nav").alias("nav_b")])
              .with_columns(pl.lit(capital).alias("nav_a")))

    # Daily return from each
    df = df.with_columns([
        pl.col("nav_a").pct_change().fill_null(0.0).alias("ret_a"),
        pl.col("nav_b").pct_change().fill_null(0.0).alias("ret_b"),
        pl.col("date").dt.year().alias("year"),
    ])

    w_b = 1.0 - w_a
    nav = capital
    nav_hist = []
    for yr, sub in df.group_by("year", maintain_order=True):
        cap_a = nav * w_a
        cap_b = nav * w_b
        for ra, rb, d in zip(sub["ret_a"].to_list(), sub["ret_b"].to_list(),
                              sub["date"].to_list()):
            cap_a *= (1 + ra)
            cap_b *= (1 + rb)
            nav = cap_a + cap_b
            nav_hist.append((d, nav))

    return pl.DataFrame({"date": [d for d, _ in nav_hist],
                         "nav": [n for _, n in nav_hist]})


def main():
    t0 = time.time()
    print("=" * 78)
    print("Comprehensive hybrid sweep — prices.py adjusted NAV re-validation")
    print(f"  Window: {START} → {END}")
    print(f"  Iter13: {len(ITER13_RANKERS)} rankers × monthly × TPEx")
    print(f"  Iter24: max ∈ {ITER24_MAX_POSITIONS} × ATR ∈ {ITER24_ATR}")
    print(f"  Hybrid: {len(SLOT_CONFIGS)} slot configs × {len(NAV_WEIGHTS)} weights")
    print(f"  Hard cap: total holdings ≤ 10 ✅")
    print("=" * 78)

    # ── Phase 1a: iter_13 sub-strategy NAVs ──
    print("\n[1a] Generating iter_13 sub-strategy NAVs ...")
    iter13_navs = {}
    for ranker in ITER13_RANKERS:
        try:
            iter13_navs[ranker] = gen_iter13_nav(ranker)
        except Exception as e:
            print(f"  ⚠️  {ranker} failed: {e}")
            iter13_navs[ranker] = None
    print(f"  done ({sum(1 for v in iter13_navs.values() if v)}/{len(ITER13_RANKERS)})")

    # ── Phase 1b: iter_24 sub-strategy NAVs ──
    print("\n[1b] Generating iter_24 sub-strategy NAVs ...")
    iter24_navs = {}
    for mp in ITER24_MAX_POSITIONS:
        for atr in ITER24_ATR:
            key = (mp, atr)
            try:
                iter24_navs[key] = gen_iter24_nav(mp, atr)
            except Exception as e:
                print(f"  ⚠️  max={mp} atr={atr} failed: {e}")
                iter24_navs[key] = None
    print(f"  done ({sum(1 for v in iter24_navs.values() if v)}/{len(iter24_navs)})")

    # ── Phase 2: hybrid sweep ──
    print(f"\n[2] Hybrid sweep — using mcap ranker as primary (cross-val later) ...")
    print(f"    {len(SLOT_CONFIGS) * len(NAV_WEIGHTS)} total combinations\n")

    nav_a = iter13_navs["mcap"]   # primary slot A
    rows = []

    for (slot_a, slot_b) in SLOT_CONFIGS:
        # Pick iter_24 variant matching slot_b max-positions
        # ATR=on for slot_b ≥ 5 (memory final ship spec)
        atr = slot_b >= 5
        nav_b_key = (slot_b, atr) if slot_b > 0 else None
        nav_b = iter24_navs.get(nav_b_key) if nav_b_key else None

        if slot_a == 0 and slot_b == 0:
            continue   # invalid

        for w_a in NAV_WEIGHTS:
            if slot_a == 0:
                w_a_eff = 0.0
            elif slot_b == 0:
                w_a_eff = 1.0
            else:
                w_a_eff = w_a

            try:
                hybrid = hybrid_blend(nav_a if slot_a > 0 else None,
                                       nav_b if slot_b > 0 else None,
                                       w_a_eff)
                m = compute_metrics(hybrid["nav"].to_numpy(), hybrid["date"].to_list())
                rows.append({
                    "slot_a": slot_a, "slot_b": slot_b,
                    "w_a": w_a_eff, "w_b": 1 - w_a_eff,
                    "atr_b": atr, "ranker_a": "mcap",
                    "cagr": m["cagr"], "sortino": m["sortino"],
                    "sharpe": m["sharpe"], "mdd": m["mdd"],
                    "final_nav": m["final_nav"],
                    "tag": f"{slot_a}+{slot_b}_w{int(w_a_eff*100)}_{'atr' if atr else 'fix'}",
                })
            except Exception as e:
                print(f"  ⚠️  {slot_a}+{slot_b} w={w_a_eff:.2f}: {e}")

            # if pure-A or pure-B, only run once
            if slot_a == 0 or slot_b == 0:
                break

    df = pl.DataFrame(rows).sort("sortino", descending=True)
    out_csv = RESULTS / "hybrid_sweep_v6.csv"
    df.write_csv(out_csv)

    # ── Phase 3: cross-validation for top hybrid ──
    print(f"\n[3] Cross-validation — top hybrid with non-mcap rankers ...")
    top = df.head(1).to_dicts()[0]
    print(f"   IS top1: {top['tag']} → Sortino {top['sortino']:.3f}, CAGR {top['cagr']*100:.2f}%")

    cv_rows = []
    for ranker in ITER13_RANKERS:
        nav_a_cv = iter13_navs.get(ranker)
        if not nav_a_cv:
            continue
        atr = top["slot_b"] >= 5
        nav_b_cv = iter24_navs.get((top["slot_b"], atr)) if top["slot_b"] > 0 else None
        try:
            hybrid = hybrid_blend(nav_a_cv if top["slot_a"] > 0 else None,
                                    nav_b_cv if top["slot_b"] > 0 else None,
                                    top["w_a"])
            m = compute_metrics(hybrid["nav"].to_numpy(), hybrid["date"].to_list())
            cv_rows.append({
                "ranker": ranker, "slot_a": top["slot_a"], "slot_b": top["slot_b"],
                "w_a": top["w_a"], "atr_b": atr,
                "cagr": m["cagr"], "sortino": m["sortino"],
                "sharpe": m["sharpe"], "mdd": m["mdd"],
            })
        except Exception as e:
            print(f"  ⚠️  CV ranker={ranker}: {e}")

    cv_df = pl.DataFrame(cv_rows).sort("sortino", descending=True)
    cv_csv = RESULTS / "hybrid_cross_validation_v6.csv"
    cv_df.write_csv(cv_csv)

    # ── Print summary ──
    print(f"\n{'=' * 78}")
    print(f"COMPLETE in {time.time()-t0:.1f}s")
    print(f"{'=' * 78}\n")

    print("Top 10 hybrid configurations (IS, sorted by Sortino):")
    print(df.head(10).select(["tag", "slot_a", "slot_b", "w_a",
                                "cagr", "sortino", "sharpe", "mdd"])
          .to_pandas().to_string(index=False))

    print(f"\nCross-validation top hybrid ({top['tag']}) across rankers:")
    print(cv_df.to_pandas().to_string(index=False))

    print(f"\nSaved:")
    print(f"  - {out_csv}")
    print(f"  - {cv_csv}")


if __name__ == "__main__":
    main()
