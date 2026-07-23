"""iter_48 - portfolio-level risk overlay.

The fallback gate can lift CAGR, but DSR remains fragile when cumulative search
penalty is raised. This iteration tests a long-only cash throttle on top of the
best daily strategies: when a prior-day gate is risk-off, hold only a fraction
of the strategy exposure and keep the remainder in cash.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, validate_daily
from iter_45_fallback_gate_sweep import load_base


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
N_TRIALS_EFFECTIVE = 1400


BASES = [
    ("iter42_w61_cash", RESULTS / "iter42_q3_risk_breakout_top3_w61_daily.csv"),
    ("iter42_w72_cash", RESULTS / "iter42_q3_risk_breakout_top3_w72_daily.csv"),
    ("iter45_w61_mkt_mom21", RESULTS / "iter45_q3_risk_breakout_top3_w61_gate_mkt_mom21_daily.csv"),
    ("iter45_w62_mkt_mom21", RESULTS / "iter45_q3_risk_breakout_top3_w62_gate_mkt_mom21_daily.csv"),
    (
        "iter46_w62_mom21_or_q3_ma50",
        RESULTS / "iter46_q3_risk_breakout_top3_w62_gate_mkt_mom21_or_q3_ma50_daily.csv",
    ),
    ("iter44_w57_always_q3", RESULTS / "iter44_q3_risk_breakout_top3_w57_fallback_q3_daily.csv"),
]


def load_gates() -> pl.DataFrame:
    base = load_base()
    return base.with_columns(
        [
            (pl.col("gate_mkt_ma150") | pl.col("gate_mkt_mom63")).alias("gate_mkt_ma150_or_mom63"),
            (pl.col("gate_mkt_ma100") & pl.col("gate_mkt_mom21")).alias("gate_mkt_ma100_and_mom21"),
            (pl.col("gate_q3_ma100") | pl.col("gate_q3_mom63")).alias("gate_q3_ma100_or_mom63"),
            (pl.col("gate_q3_ma50") & pl.col("gate_mkt_ma50")).alias("gate_q3_ma50_and_mkt_ma50"),
        ]
    ).select(
        [
            "date",
            "gate_mkt_ma50",
            "gate_mkt_ma100",
            "gate_mkt_ma150",
            "gate_mkt_ma200",
            "gate_mkt_mom21",
            "gate_mkt_mom63",
            "gate_mkt_dd10",
            "gate_mkt_dd15",
            "gate_mkt_ma150_or_mom63",
            "gate_mkt_ma100_and_mom21",
            "gate_q3_ma50",
            "gate_q3_ma100",
            "gate_q3_mom63",
            "gate_q3_ma100_or_mom63",
            "gate_q3_ma50_and_mkt_ma50",
        ]
    )


def load_strategy(path: Path) -> pl.DataFrame:
    return (
        pl.read_csv(path, try_parse_dates=True)
        .sort("date")
        .select(["date", "nav"])
        .with_columns(pl.col("nav").pct_change().fill_null(0.0).alias("ret_strategy"))
        .select(["date", "ret_strategy"])
    )


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
    gate_cols = [c for c in gates.columns if c.startswith("gate_")]
    off_mults = [0.0, 0.25, 0.50, 0.75]
    n_candidates = len(BASES) * len(gate_cols) * len(off_mults)
    print(
        f"[iter48] gates={len(gate_cols)} bases={len(BASES)} candidates={n_candidates} "
        f"dsr_trials={N_TRIALS_EFFECTIVE}",
        flush=True,
    )
    rows = []
    for base_name, path in BASES:
        strategy = load_strategy(path)
        print(f"[iter48] base={base_name}", flush=True)
        for gate in gate_cols:
            for off_mult in off_mults:
                off_tag = str(int(round(off_mult * 100)))
                name = f"iter48_{base_name}_{gate}_off{off_tag}"
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
    out = RESULTS / "iter_48_portfolio_risk_overlay_summary.csv"
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
    print("iter_48 portfolio risk overlay")
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
