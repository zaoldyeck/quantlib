"""iter_49 - refine risk-off exposure around iter48 leaders."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, validate_daily
from iter_48_portfolio_risk_overlay import load_gates, load_strategy


RESULTS = Path("research/strat_lab/results")
N_TRIALS_EFFECTIVE = 1500


BASES = [
    ("iter45_w61_mkt_mom21", RESULTS / "iter45_q3_risk_breakout_top3_w61_gate_mkt_mom21_daily.csv"),
    ("iter45_w62_mkt_mom21", RESULTS / "iter45_q3_risk_breakout_top3_w62_gate_mkt_mom21_daily.csv"),
    (
        "iter46_w62_mom21_or_q3_ma50",
        RESULTS / "iter46_q3_risk_breakout_top3_w62_gate_mkt_mom21_or_q3_ma50_daily.csv",
    ),
    ("iter44_w57_always_q3", RESULTS / "iter44_q3_risk_breakout_top3_w57_fallback_q3_daily.csv"),
]


GATES = [
    "gate_q3_ma50",
    "gate_mkt_mom21",
    "gate_q3_ma50_and_mkt_ma50",
    "gate_mkt_ma100_and_mom21",
]


def apply_overlay(strategy: pl.DataFrame, gates: pl.DataFrame, gate: str, off_mult: float) -> pl.DataFrame:
    df = strategy.join(gates, on="date", how="inner").sort("date")
    rets = (
        df.with_columns(pl.when(pl.col(gate)).then(1.0).otherwise(off_mult).alias("exposure"))
        .with_columns((pl.col("ret_strategy") * pl.col("exposure")).alias("ret"))
        .select("ret")
        .to_numpy()
        .reshape(-1)
    )
    nav = CAPITAL * np.cumprod(1 + rets)
    return pl.DataFrame({"date": df["date"].to_list(), "nav": nav})


def main() -> None:
    gates = load_gates()
    off_mults = [x / 100 for x in range(60, 101, 5)]
    n_candidates = len(BASES) * len(GATES) * len(off_mults)
    print(
        f"[iter49] bases={len(BASES)} gates={len(GATES)} candidates={n_candidates} "
        f"dsr_trials={N_TRIALS_EFFECTIVE}",
        flush=True,
    )
    rows = []
    for base_name, path in BASES:
        strategy = load_strategy(path)
        print(f"[iter49] base={base_name}", flush=True)
        for gate in GATES:
            for off_mult in off_mults:
                off_tag = str(int(round(off_mult * 100)))
                name = f"iter49_{base_name}_{gate}_off{off_tag}"
                daily = apply_overlay(strategy, gates, gate, off_mult)
                out_path = RESULTS / f"{name}_daily.csv"
                daily.write_csv(out_path)
                row = validate_daily(
                    name,
                    daily,
                    N_TRIALS_EFFECTIVE,
                    {"max_active": 6.0, "trade_days": 0.0, "avg_turnover_trade_day": 0.0},
                )
                row["base"] = base_name
                row["gate"] = gate
                row["off_mult"] = off_mult
                row["promotable"] = (
                    row["dsr"] >= 0.95
                    and row["pbo"] < 0.50
                    and row["boot_cagr_lb"] > 0.10
                    and row["oos_mdd"] > -0.45
                )
                rows.append(row)

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_49_overlay_offgrid_refinement_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "base",
        "gate",
        pl.col("off_mult").mul(100).round(0).cast(pl.Int64).alias("off_pct"),
        pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
        pl.col("sortino").round(3).alias("full_sortino"),
        pl.col("mdd").mul(100).round(2).alias("full_mdd_pct"),
        pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
        pl.col("oos_sortino").round(3),
        pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
        pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
        pl.col("dsr").round(3),
        pl.col("pbo").round(3),
    ]
    print("=" * 120)
    print("iter_49 overlay off-grid refinement")
    print("=" * 120)
    print(summary.select(view_cols).head(30).to_pandas().to_string(index=False))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
