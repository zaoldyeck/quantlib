"""Phase B: iter_24 entry+exit parameter sweep.

Tests:
  - yoy_entry      ∈ {20, 30, 40}      (entry threshold)
  - breakout_lkb   ∈ {60, 90}          (breakout lookback days)
  - vol_multiplier ∈ {1.5, 2.0}        (volume must exceed N × avg)
  - atr_mult       ∈ {2.0, 3.0, 4.0}   (ATR multiplier for trailing)
  = 3 × 2 × 2 × 3 = 36 configurations

Fixed: max=5 + ATR trailing (these were determined by Phase 0 / memory).

Each backtest ~25s; total ~15 min sequential.
Output: research/strat_lab/results/sweep_iter24_params_v6.csv

Run:
    uv run --project research python research/strat_lab/sweep_iter24_params.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from itertools import product
from pathlib import Path

import math
import numpy as np
import polars as pl

HERE = Path(__file__).parent
RESULTS = HERE / "results"

START = "2005-01-03"
END = "2026-04-25"

# Sweep grid
YOY_ENTRY = [20.0, 30.0, 40.0]
BREAKOUT_LKB = [60, 90]
VOL_MULT = [1.5, 2.0]
ATR_MULT = [2.0, 3.0, 4.0]

TDPY = 252
RF = 0.01


def metrics_from_nav_csv(path: Path) -> dict:
    df = pl.read_csv(path, try_parse_dates=True).sort("date")
    nav = df["nav"].to_numpy()
    days = df["date"].to_list()
    capital = 1_000_000.0
    rets = np.diff(np.concatenate([[capital], nav])) / np.concatenate([[capital], nav[:-1]])
    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (nav[-1] / capital) ** (1 / years) - 1
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY))
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - RF) / vol if vol > 0 else 0.0
    sortino = (cagr - RF) / downvol if downvol > 0 else 0.0
    peak, mdd = capital, 0.0
    for v in nav:
        peak = max(peak, v); mdd = min(mdd, (v - peak) / peak)
    return {"cagr": cagr, "sortino": sortino, "sharpe": sharpe, "mdd": mdd,
            "final_nav": float(nav[-1])}


def main():
    t0 = time.time()
    grid = list(product(YOY_ENTRY, BREAKOUT_LKB, VOL_MULT, ATR_MULT))
    print("=" * 78)
    print(f"iter_24 entry/exit param sweep — {len(grid)} configs (max=5, ATR on)")
    print("=" * 78)

    rows = []
    for i, (yoy, lkb, vmult, amult) in enumerate(grid):
        suffix = f"sweep_y{int(yoy)}_lkb{lkb}_v{vmult:.1f}_atr{amult:.1f}"
        out_csv = RESULTS / f"iter_24_{suffix}_daily.csv"
        cmd = [
            "uv", "run", "--project", "research", "python",
            str(HERE / "iter_24.py"),
            "--start", START, "--end", END,
            "--max-positions", "5", "--atr-trailing",
            "--yoy-entry", str(yoy),
            "--breakout-lookback", str(lkb),
            "--vol-multiplier", str(vmult),
            "--atr-mult", str(amult),
            "--out-suffix", suffix,
        ]
        if out_csv.exists():
            print(f"[{i+1:2d}/{len(grid)}] {suffix} (cached)")
        else:
            print(f"[{i+1:2d}/{len(grid)}] {suffix} ...")
            t1 = time.time()
            r = subprocess.run(cmd, cwd=HERE.parent.parent, check=False,
                                capture_output=True, text=True)
            if r.returncode != 0 or not out_csv.exists():
                print(f"  ⚠️  failed (rc={r.returncode})")
                continue
            print(f"  done {time.time()-t1:.1f}s")
        m = metrics_from_nav_csv(out_csv)
        rows.append({
            "yoy_entry": yoy, "breakout_lkb": lkb,
            "vol_mult": vmult, "atr_mult": amult,
            "tag": suffix, **m,
        })

    df = pl.DataFrame(rows).sort("sortino", descending=True)
    out = RESULTS / "sweep_iter24_params_v6.csv"
    df.write_csv(out)

    print(f"\n{'=' * 78}")
    print(f"DONE in {time.time()-t0:.1f}s")
    print(f"{'=' * 78}\n")
    print(df.head(15).select([
        "yoy_entry", "breakout_lkb", "vol_mult", "atr_mult",
        pl.col("cagr").mul(100).round(2).alias("cagr%"),
        pl.col("sortino").round(3),
        pl.col("sharpe").round(3),
        pl.col("mdd").mul(100).round(2).alias("mdd%"),
    ]).to_pandas().to_string(index=False))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
