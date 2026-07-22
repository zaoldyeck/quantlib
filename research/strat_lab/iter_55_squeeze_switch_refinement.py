"""iter_55 - focused risk refinement around the high-CAGR squeeze switch.

iter54 showed that the squeeze sleeve can add a lot of CAGR when gated, but it
does not pass the DSR gate. This focused pass tests a small pre-specified set
of nested gates that require both market/quality trend and the squeeze sleeve's
own prior trend before switching.

The switch still holds exactly one complete sleeve per day, so the <=10 holding
limit is preserved.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, validate_daily  # noqa: E402
from iter_54_cross_family_switch import SLEEVES, load_switch_base, slot_count  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
N_TRIALS_EFFECTIVE = 420

DEFENSES = ["iter42_w59_champion", "iter44_w74_q3_trend"]
ATTACKS = ["iter52_squeeze_top5", "iter53_lgbm_weekly_top10", "iter53_lgbm_weekly_top10_half"]


def daily_from_returns(dates: list, rets: np.ndarray) -> pl.DataFrame:
    return pl.DataFrame({"date": dates, "nav": CAPITAL * np.cumprod(1.0 + rets)})


def add_refined_gates(base: pl.DataFrame) -> pl.DataFrame:
    return base.with_columns(
        [
            (pl.col("gate_q3_ma50") & pl.col("gate_iter52_squeeze_top5_ma50")).alias("gate_q3_ma50_and_sq_ma50"),
            (pl.col("gate_q3_ma50") & pl.col("gate_iter52_squeeze_top5_ma100")).alias("gate_q3_ma50_and_sq_ma100"),
            (pl.col("gate_q3_mom63") & pl.col("gate_iter52_squeeze_top5_ma50")).alias("gate_q3_mom63_and_sq_ma50"),
            (pl.col("gate_mkt_mom21") & pl.col("gate_q3_ma50") & pl.col("gate_iter52_squeeze_top5_ma50")).alias(
                "gate_mkt_mom21_q3_ma50_sq_ma50"
            ),
            (pl.col("gate_mkt_mom63") & pl.col("gate_q3_ma50") & pl.col("gate_iter52_squeeze_top5_ma50")).alias(
                "gate_mkt_mom63_q3_ma50_sq_ma50"
            ),
            (pl.col("gate_mkt_ma50") & pl.col("gate_q3_ma50") & pl.col("gate_iter52_squeeze_top5_ma50")).alias(
                "gate_mkt_ma50_q3_ma50_sq_ma50"
            ),
            (
                (pl.col("score_iter52_squeeze_top5_mom21") > pl.col("score_iter42_w59_champion_mom21"))
                & pl.col("gate_q3_ma50")
            ).alias("gate_sq_mom21_beats_iter42_and_q3_ma50"),
            (
                (pl.col("score_iter52_squeeze_top5_mom63") > pl.col("score_iter42_w59_champion_mom63"))
                & pl.col("gate_q3_ma50")
            ).alias("gate_sq_mom63_beats_iter42_and_q3_ma50"),
        ]
    )


def switch_pair(base: pl.DataFrame, defense: str, attack: str, gate: str, off_scale: float = 1.0) -> pl.DataFrame:
    if off_scale >= 0.999:
        ret = (
            base.with_columns(
                pl.when(pl.col(gate)).then(pl.col(f"ret_{attack}")).otherwise(pl.col(f"ret_{defense}")).alias("ret")
            )["ret"]
            .to_numpy()
            .astype(float, copy=False)
        )
    else:
        ret = (
            base.with_columns(
                pl.when(pl.col(gate))
                .then(pl.col(f"ret_{attack}"))
                .otherwise(pl.col(f"ret_{defense}") * off_scale)
                .alias("ret")
            )["ret"]
            .to_numpy()
            .astype(float, copy=False)
        )
    return daily_from_returns(base["date"].to_list(), ret)


def main() -> None:
    base = add_refined_gates(load_switch_base())
    gates = [
        "gate_q3_ma50_and_sq_ma50",
        "gate_q3_ma50_and_sq_ma100",
        "gate_q3_mom63_and_sq_ma50",
        "gate_mkt_mom21_q3_ma50_sq_ma50",
        "gate_mkt_mom63_q3_ma50_sq_ma50",
        "gate_mkt_ma50_q3_ma50_sq_ma50",
        "gate_sq_mom21_beats_iter42_and_q3_ma50",
        "gate_sq_mom63_beats_iter42_and_q3_ma50",
    ]
    off_scales = [1.0, 0.75, 0.50]
    print(
        f"[iter55] rows={base.height} defenses={len(DEFENSES)} attacks={len(ATTACKS)} "
        f"gates={len(gates)} off_scales={len(off_scales)} dsr_trials={N_TRIALS_EFFECTIVE}",
        flush=True,
    )

    rows = []
    for defense in DEFENSES:
        for attack in ATTACKS:
            for gate in gates:
                for off_scale in off_scales:
                    tag = int(off_scale * 100)
                    name = f"iter55_{defense}_{attack}_{gate}_off{tag}"
                    daily = switch_pair(base, defense, attack, gate, off_scale)
                    out_path = RESULTS / f"{name}_daily.csv"
                    daily.write_csv(out_path)
                    row = validate_daily(
                        name,
                        daily,
                        N_TRIALS_EFFECTIVE,
                        {
                            "max_active": max(slot_count(defense), slot_count(attack)),
                            "trade_days": 0.0,
                            "avg_turnover_trade_day": 0.0,
                        },
                    )
                    row["defense"] = defense
                    row["attack"] = attack
                    row["gate"] = gate
                    row["off_scale"] = off_scale
                    row["promotable"] = (
                        row["dsr"] >= 0.95
                        and row["pbo"] < 0.50
                        and row["boot_cagr_lb"] > 0.10
                        and row["oos_mdd"] > -0.45
                        and row["max_active"] <= 10.0
                    )
                    rows.append(row)

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_55_squeeze_switch_refinement_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "defense",
        "attack",
        "gate",
        pl.col("off_scale").round(2),
        pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
        pl.col("sortino").round(3).alias("full_sortino"),
        pl.col("mdd").mul(100).round(2).alias("full_mdd_pct"),
        pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
        pl.col("oos_sortino").round(3),
        pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
        pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
        pl.col("dsr").round(3),
        pl.col("pbo").round(3),
        pl.col("max_active").cast(pl.Int64),
    ]
    print("=" * 120)
    print("iter_55 squeeze switch refinement")
    print("=" * 120)
    print(summary.select(view_cols).head(35).to_pandas().to_string(index=False))
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
