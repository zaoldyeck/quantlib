"""iter_46 - focused refinement around the iter45 mkt_mom21 gate.

The iter45 breakthrough improved return and Sortino but sat close to the DSR
and PBO thresholds. This focused sweep combines prior-day 21-day market
momentum with the best lower-drawdown gates from iter45, using a fixed higher
trial count for DSR so the narrower search does not make validation easier.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import validate_daily
from iter_45_fallback_gate_sweep import annual_blend, load_base


RESULTS = Path("research/strat_lab/results")
N_TRIALS_EFFECTIVE = 1200


def add_combo_gates(base: pl.DataFrame) -> tuple[pl.DataFrame, list[str]]:
    specs = {
        "gate_mkt_mom21_and_q3_ma50": pl.col("gate_mkt_mom21") & pl.col("gate_q3_ma50"),
        "gate_mkt_mom21_or_q3_ma50": pl.col("gate_mkt_mom21") | pl.col("gate_q3_ma50"),
        "gate_mkt_mom21_and_mkt_ma50": pl.col("gate_mkt_mom21") & pl.col("gate_mkt_ma50"),
        "gate_mkt_mom21_or_mkt_ma50": pl.col("gate_mkt_mom21") | pl.col("gate_mkt_ma50"),
        "gate_mkt_mom21_and_q3_ma100": pl.col("gate_mkt_mom21") & pl.col("gate_q3_ma100"),
        "gate_mkt_mom21_or_q3_ma100": pl.col("gate_mkt_mom21") | pl.col("gate_q3_ma100"),
        "gate_mkt_mom21_and_mkt_ma100": pl.col("gate_mkt_mom21") & pl.col("gate_mkt_ma100"),
        "gate_mkt_mom21_or_mkt_ma100": pl.col("gate_mkt_mom21") | pl.col("gate_mkt_ma100"),
        "gate_mkt_mom21_and_mkt_dd10": pl.col("gate_mkt_mom21") & pl.col("gate_mkt_dd10"),
        "gate_mkt_mom21_or_mkt_dd10": pl.col("gate_mkt_mom21") | pl.col("gate_mkt_dd10"),
        "gate_mkt_mom21_and_q3_mom63": pl.col("gate_mkt_mom21") & pl.col("gate_q3_mom63"),
        "gate_mkt_mom21_or_q3_mom63": pl.col("gate_mkt_mom21") | pl.col("gate_q3_mom63"),
        "gate_q3_ma50_and_mkt_ma50": pl.col("gate_q3_ma50") & pl.col("gate_mkt_ma50"),
        "gate_q3_ma50_or_mkt_ma50": pl.col("gate_q3_ma50") | pl.col("gate_mkt_ma50"),
    }
    return base.with_columns([expr.alias(name) for name, expr in specs.items()]), list(specs)


def main() -> None:
    base, gates = add_combo_gates(load_base())
    weights = list(range(50, 69))
    n_candidates = len(gates) * len(weights)
    print(
        f"[iter46] loaded base rows={base.height} gates={len(gates)} "
        f"candidates={n_candidates} dsr_trials={N_TRIALS_EFFECTIVE}",
        flush=True,
    )
    rows = []
    for gate in gates:
        print(f"[iter46] gate={gate}", flush=True)
        for pct in weights:
            name = f"iter46_q3_risk_breakout_top3_w{pct}_{gate}"
            daily = annual_blend(pct / 100, gate, base)
            out_path = RESULTS / f"{name}_daily.csv"
            daily.write_csv(out_path)
            row = validate_daily(
                name,
                daily,
                N_TRIALS_EFFECTIVE,
                {"max_active": 6.0, "trade_days": 0.0, "avg_turnover_trade_day": 0.0},
            )
            row["gate"] = gate
            row["q_weight"] = pct / 100
            row["promotable"] = (
                row["dsr"] >= 0.95
                and row["pbo"] < 0.50
                and row["boot_cagr_lb"] > 0.10
                and row["oos_mdd"] > -0.45
            )
            rows.append(row)

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_46_mom21_combo_refinement_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "gate",
        pl.col("q_weight").mul(100).round(0).cast(pl.Int64).alias("q_weight_pct"),
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
    print("iter_46 mom21 combo refinement")
    print("=" * 120)
    print(summary.select(view_cols).head(30).to_pandas().to_string(index=False))
    print("\nTop promotable by OOS CAGR")
    print(
        summary.filter(pl.col("promotable"))
        .sort(["oos_cagr", "oos_sortino"], descending=[True, True])
        .select(view_cols)
        .head(15)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
