"""Professional validation for the pure-quant campaign champion.

Validates a realistic-execution NAV series: yearly-fold walk-forward metrics,
PBO (CSCV), year-block bootstrap CI, DSR (campaign trial count), Lo-2002
autocorrelation-robust Sharpe, plus same-window correlation with the Iter95
realistic champion NAV.

Usage:
  uv run --project . python src/quantlib/experiments/quant_event_engine_v1_validate.py \
      --variant newhigh_rot
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from quantlib import paths

REPO_ROOT = paths.REPO
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src" / "quantlib"))
sys.path.insert(0, str(REPO_ROOT / "src" / "quantlib" / "strat_lab"))

from validate_hybrid import (  # noqa: E402
    TDPY,
    bootstrap_ci,
    deflated_sharpe,
    lo_2002_sharpe_test,
    metrics,
    pbo_cscv,
)

RESULTS = paths.OUT_STRAT_LAB
DOCS = REPO_ROOT / "docs" / "strategy_research"
CHAMPION_NAV = (
    RESULTS / "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_daily.csv"
)
N_TRIALS_CAMPAIGN = 24  # quant ledger trials + inherited spike-study combo search


def read_nav(path: Path) -> pl.DataFrame:
    df = pl.read_csv(path, try_parse_dates=True).select(["date", "nav"]).sort("date")
    if df["date"].dtype != pl.Date:
        df = df.with_columns(pl.col("date").cast(pl.Date))
    return df


def rets_dates(df: pl.DataFrame) -> tuple[np.ndarray, list[date]]:
    nav = df["nav"].to_numpy().astype(float)
    return nav[1:] / nav[:-1] - 1.0, df["date"].to_list()[1:]


def yearly_folds(rets: np.ndarray, dates: list[date]) -> list[dict]:
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("y"))
    folds = []
    for key, g in df.group_by("y", maintain_order=True):
        if g.height < 60:
            continue
        m = metrics(g["ret"].to_numpy(), years=g.height / TDPY)
        folds.append({"fold": key[0], "n": g.height, **m})
    return folds


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", default="newhigh_rot")
    parser.add_argument("--series", default="exec", choices=("exec", "paper"))
    args = parser.parse_args()
    suffix = "_exec_daily.csv" if args.series == "exec" else "_daily.csv"
    path = RESULTS / f"quant_event_engine_v1_{args.variant}{suffix}"
    df = read_nav(path)
    rets, dates = rets_dates(df)
    years = (dates[-1] - dates[0]).days / 365.25

    m = metrics(rets, years=years)
    lo = lo_2002_sharpe_test(rets)
    folds = yearly_folds(rets, dates)
    pbo = pbo_cscv(folds)
    boot = bootstrap_ci(rets, dates)
    dsr = deflated_sharpe(m["sharpe"], N_TRIALS_CAMPAIGN, rets)

    print(f"series={args.variant}/{args.series} window={dates[0]}~{dates[-1]}")
    print(
        f"cagr={m['cagr']:.4f} sharpe={m['sharpe']:.3f} sortino={m['sortino']:.3f} mdd={m['mdd']:.4f}"
    )
    print(f"lo_t={lo.get('t_stat', float('nan')):.3f} lo_p={lo.get('p_value', float('nan')):.5f}")
    print(f"dsr={dsr:.4f} (trials={N_TRIALS_CAMPAIGN}) pbo={pbo:.3f} folds={len(folds)}")
    print("bootstrap:", {k: round(v, 4) for k, v in boot.items() if isinstance(v, float)})
    fold_line = {f["fold"]: round(f["cagr"], 3) for f in folds}
    print("fold cagr by year:", fold_line)
    pos_share = float(np.mean([f["cagr"] > 0 for f in folds])) if folds else float("nan")
    print(f"fold_pos_share={pos_share:.2f} fold_cagr_min={min(f['cagr'] for f in folds):.3f}")

    if CHAMPION_NAV.exists():
        b = read_nav(CHAMPION_NAV).rename({"nav": "nav_b"})
        j = df.rename({"nav": "nav_a"}).join(b, on="date", how="inner").sort("date")
        if j.height > 200:
            ra = j["nav_a"].to_numpy()
            rb = j["nav_b"].to_numpy()
            ra = ra[1:] / ra[:-1] - 1.0
            rb = rb[1:] / rb[:-1] - 1.0
            yrs = (j["date"][-1] - j["date"][0]).days / 365.25
            blend = metrics(0.5 * ra + 0.5 * rb, years=yrs)
            print(
                f"vs iter95 same-window: corr={np.corrcoef(ra, rb)[0,1]:.3f} "
                f"iter95_cagr={metrics(rb, years=yrs)['cagr']:.3f} "
                f"blend5050 cagr={blend['cagr']:.3f} mdd={blend['mdd']:.3f} sortino={blend['sortino']:.3f}"
            )

    out = DOCS / f"quant_event_engine_v1_validation_{args.variant}.md"
    out.write_text(
        "\n".join(
            [
                f"# quant_event_engine_v1 validation — {args.variant} ({args.series})",
                "",
                f"- window {dates[0]}~{dates[-1]}, trials={N_TRIALS_CAMPAIGN}",
                f"- cagr {m['cagr']:.4f}, sharpe {m['sharpe']:.3f}, sortino {m['sortino']:.3f}, mdd {m['mdd']:.4f}",
                f"- Lo-2002 t={lo.get('t_stat', float('nan')):.3f} (p={lo.get('p_value', float('nan')):.5f}); DSR={dsr:.4f}; PBO={pbo:.3f}",
                f"- bootstrap: {boot}",
                f"- yearly folds: {fold_line}; pos_share={pos_share:.2f}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(f"report -> {out}")


if __name__ == "__main__":
    main()
