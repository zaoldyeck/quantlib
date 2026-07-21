"""iter_76 - NAV-level multi-strategy PM allocator research.

This is a research harness over validated daily NAV sleeves.  It does not
promote anything to execution-ready because NAV-level sleeve allocation still
needs target-book reconciliation before it can enforce the 10-stock limit.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from pm_allocator import MomentumAllocatorConfig, Sleeve, run_momentum_allocator  # noqa: E402


RESULTS = Path("research/strat_lab/results")


def load_top_sleeves(summary_path: Path, top_n: int) -> list[Sleeve]:
    summary = (
        pl.read_csv(summary_path, try_parse_dates=True)
        .sort(["robust_growth_score", "oos_log_cagr", "oos_cagr"], descending=[True, True, True])
        .head(top_n)
    )
    sleeves = []
    for row in summary.iter_rows(named=True):
        name = row["name"]
        path = RESULTS / f"{name}_daily.csv"
        if path.exists():
            sleeves.append(Sleeve(name=name, daily_path=path))
    if len(sleeves) < 2:
        raise RuntimeError("need at least two sleeve daily files for allocator research")
    return sleeves


def build_configs() -> list[MomentumAllocatorConfig]:
    configs = []
    for lookback in (21, 63, 126):
        for top_k in (1, 2, 3):
            for vol_penalty in (0.0, 0.5, 1.0):
                configs.append(
                    MomentumAllocatorConfig(
                        lookback_days=lookback,
                        top_k=top_k,
                        min_score=0.0,
                        vol_penalty=vol_penalty,
                        max_sleeve_weight=1.0,
                        cash_when_no_positive=True,
                    )
                )
    return configs


def run(summary_path: Path, top_n: int, n_trials: int | None = None) -> None:
    sleeves = load_top_sleeves(summary_path, top_n)
    configs = build_configs()
    effective_trials = n_trials if n_trials is not None else len(configs)
    rows = []
    best: tuple[dict[str, object], pl.DataFrame, pl.DataFrame] | None = None
    for cfg in configs:
        name = f"iter76_pm_top{top_n}_lb{cfg.lookback_days}_k{cfg.top_k}_vp{cfg.vol_penalty:g}"
        daily, weights, metrics = run_momentum_allocator(sleeves, cfg, name=name, n_trials=effective_trials)
        row = {"name": name, "top_n_sleeves": top_n, **asdict(cfg), **metrics}
        rows.append(row)
        if best is None or float(row["robust_growth_score"]) > float(best[0]["robust_growth_score"]):
            best = (row, daily, weights)

    out = pl.DataFrame(rows).sort(["robust_growth_score", "oos_log_cagr", "oos_cagr"], descending=[True, True, True])
    out_path = RESULTS / "iter_76_pm_allocator_summary.csv"
    out.write_csv(out_path)
    (RESULTS / "iter_76_pm_allocator_sleeves.json").write_text(
        json.dumps([s.name for s in sleeves], ensure_ascii=False, indent=2)
    )
    if best is not None:
        best_name = str(best[0]["name"])
        best[1].write_csv(RESULTS / f"{best_name}_daily.csv")
        best[2].write_csv(RESULTS / f"{best_name}_weights.csv")

    cols = [
        "name",
        "oos_cagr",
        "recent_1y_cagr",
        "oos_sortino",
        "oos_mdd",
        "oos_cdar_95",
        "oos_ulcer_index",
        "oos_k_ratio",
        "robust_growth_score",
        "boot_cagr_lb",
        "dsr",
        "pbo",
    ]
    print(out.select([c for c in cols if c in out.columns]).head(20).to_pandas().to_string(index=False))
    print(f"\nSaved: {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", type=Path, default=RESULTS / "iter_75_dynamic_industry_summary.csv")
    ap.add_argument("--top-n", type=int, default=6)
    ap.add_argument("--n-trials", type=int, default=None)
    args = ap.parse_args()
    run(args.summary, args.top_n, args.n_trials)


if __name__ == "__main__":
    main()
