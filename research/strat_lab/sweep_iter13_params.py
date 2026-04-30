"""Phase C: iter_13 entry+exit parameter sweep.

Tests:
  - min_roa  ∈ {0.08, 0.10, 0.12, 0.15}   (5y ROA TTM median threshold)
  - min_gm   ∈ {0.25, 0.30, 0.35}          (5y GM TTM median threshold)
  - stop_loss ∈ {0, 0.15}                  (none vs Phase A best)
  = 4 × 3 × 2 = 24 configurations

Fixed: monthly rebal + mcap ranker + TWSE+TPEx (Phase 0 ship spec).

Each backtest ~30s; total ~12 min sequential.
Output: research/strat_lab/results/sweep_iter13_params_v6.csv

Run:
    uv run --project research python research/strat_lab/sweep_iter13_params.py
"""
from __future__ import annotations

import math
import os
import subprocess
import sys
import time
from datetime import date
from itertools import product
from pathlib import Path

import numpy as np
import polars as pl

HERE = Path(__file__).parent
RESULTS = HERE / "results"

START = "2005-01-03"
END = "2026-04-25"

MIN_ROA = [0.08, 0.10, 0.12, 0.15]
MIN_GM = [0.25, 0.30, 0.35]
STOP_LOSS = [0.0, 0.15]   # 0 = no stop; Phase A best was 0.15

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


def run_iter13(roa: float, gm: float, suffix: str) -> Path:
    """Run iter_13 with custom ROA/GM thresholds. Returns NAV CSV path."""
    out_csv = RESULTS / f"iter_13_{suffix}_daily.csv"
    if out_csv.exists():
        return out_csv
    cmd = [
        "uv", "run", "--project", "research", "python",
        str(HERE / "iter_13.py"),
        "--start", START, "--end", END,
        "--freq", "monthly", "--ranker", "mcap",
        "--universe", "twse_tpex", "--mode", "mcap",
        "--min-roa", str(roa), "--min-gm", str(gm),
        "--suffix", suffix,
    ]
    r = subprocess.run(cmd, cwd=HERE.parent.parent, check=False,
                        capture_output=True, text=True, timeout=600)
    if r.returncode != 0 or not out_csv.exists():
        raise RuntimeError(f"iter_13 failed for roa={roa} gm={gm}: rc={r.returncode}\n{r.stderr[:500]}")
    return out_csv


def apply_stop_loss(picks_csv: Path, stop_loss_pct: float, output_csv: Path) -> Path:
    """Replay iter_13 NAV with intra-month stop-loss (calls iter_13_event_exit logic)."""
    if stop_loss_pct == 0:
        # Just use the picks-derived NAV directly (already at default no_stop output)
        return picks_csv.parent / picks_csv.name.replace("_picks", "_daily")

    if output_csv.exists():
        return output_csv

    cmd = [
        "uv", "run", "--project", "research", "python", "-c",
        f"""
import sys; sys.path.insert(0, 'research/strat_lab')
from datetime import date
from iter_13_event_exit import apply_stop_loss as fn
fn('{picks_csv}', {stop_loss_pct}, date(2005,1,3), date(2026,4,25), '{output_csv}')
"""
    ]
    r = subprocess.run(cmd, cwd=HERE.parent.parent, check=False,
                        capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        raise RuntimeError(f"stop_loss failed: rc={r.returncode}\n{r.stderr[:500]}")
    return output_csv


def main():
    t0 = time.time()
    grid = list(product(MIN_ROA, MIN_GM, STOP_LOSS))
    print("=" * 78)
    print(f"iter_13 entry+exit sweep — {len(grid)} configs (monthly mcap dual)")
    print("=" * 78)

    rows = []
    for i, (roa, gm, sl) in enumerate(grid):
        base_suffix = f"sweep_roa{int(roa*100)}_gm{int(gm*100)}"
        sl_lbl = "no_stop" if sl == 0 else f"sl{int(sl*100)}"
        full_suffix = f"{base_suffix}_{sl_lbl}"

        # Step 1: generate base monthly NAV (no stop-loss yet)
        try:
            base_csv = run_iter13(roa, gm, base_suffix)
            picks_csv = base_csv.parent / base_csv.name.replace("_daily", "_picks")
        except Exception as e:
            print(f"  ⚠️  [{i+1:2d}/{len(grid)}] {base_suffix} base failed: {e}")
            continue

        # Step 2: apply stop-loss (or use base NAV if sl=0)
        if sl == 0:
            final_csv = base_csv
        else:
            final_csv = RESULTS / f"iter_13_{full_suffix}_daily.csv"
            try:
                apply_stop_loss(picks_csv, sl, final_csv)
            except Exception as e:
                print(f"  ⚠️  [{i+1:2d}/{len(grid)}] {full_suffix} sl failed: {e}")
                continue

        m = metrics_from_nav_csv(final_csv)
        rows.append({
            "min_roa": roa, "min_gm": gm, "stop_loss": sl,
            "tag": full_suffix, **m,
        })
        print(f"[{i+1:2d}/{len(grid)}] {full_suffix}: "
              f"CAGR {m['cagr']*100:+.2f}% Sortino {m['sortino']:.3f} MDD {m['mdd']*100:.1f}%")

    df = pl.DataFrame(rows).sort("sortino", descending=True)
    out = RESULTS / "sweep_iter13_params_v6.csv"
    df.write_csv(out)

    print(f"\n{'=' * 78}")
    print(f"DONE in {time.time()-t0:.1f}s")
    print(f"{'=' * 78}\n")
    print(df.head(15).select([
        "min_roa", "min_gm", "stop_loss",
        pl.col("cagr").mul(100).round(2).alias("cagr%"),
        pl.col("sortino").round(3),
        pl.col("sharpe").round(3),
        pl.col("mdd").mul(100).round(2).alias("mdd%"),
    ]).to_pandas().to_string(index=False))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
