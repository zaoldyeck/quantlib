"""iter_42 - precision refinement around the iter_41 breakthrough.

iter_41 found the useful structure:
  Quality3/5 + breakout_risk_ma200_cash_top3

This script sweeps the quality sleeve weight at 1% granularity so the promoted
candidate is not an artifact of the coarse 5% grid.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

from iter_38_next_open_hybrid_validation import annual_rebalanced_blend
from iter_40_research_campaign import validate_daily


RESULTS = Path("research/strat_lab/results")


def main() -> None:
    event = RESULTS / "iter_40_breakout_risk_ma200_cash_top3_daily.csv"
    configs = [
        ("q3", RESULTS / "latest_q3_daily.csv", 3),
        ("q5", RESULTS / "latest_q5_daily.csv", 5),
    ]
    rows = []
    n_trials = len(configs) * 36
    for qname, qpath, qslots in configs:
        for pct in range(50, 86):
            name = f"iter42_{qname}_risk_breakout_top3_w{pct}"
            daily = annual_rebalanced_blend(qpath, event, pct / 100)
            out_path = RESULTS / f"{name}_daily.csv"
            daily.write_csv(out_path)
            row = validate_daily(name, daily, n_trials, {"max_active": float(qslots + 3), "trade_days": 0.0, "avg_turnover_trade_day": 0.0})
            row["q_sleeve"] = qname
            row["q_weight"] = pct / 100
            row["slots"] = qslots + 3
            row["path"] = str(out_path)
            row["promotable"] = (
                row["dsr"] >= 0.95
                and row["pbo"] < 0.50
                and row["boot_cagr_lb"] > 0.10
                and row["oos_mdd"] > -0.45
            )
            rows.append(row)

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_42_precision_refinement_summary.csv"
    summary.write_csv(out)
    print("=" * 120)
    print("iter_42 precision refinement")
    print("=" * 120)
    print(
        summary.select(
            [
                "name",
                "promotable",
                "q_sleeve",
                "slots",
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
        )
        .head(30)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
