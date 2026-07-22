"""iter_45 - lookahead-safe fallback gate sweep.

iter_44 showed that replacing idle event-sleeve cash with Quality3 can raise
CAGR, but the always-on fallback pushes drawdown close to the acceptance limit.
This iteration searches for a gate that uses only prior-day information to turn
the idle fallback on and off.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import polars as pl
from research import paths

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from research.db import connect

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, validate_daily
from research.prices import fetch_adjusted_panel


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
START = "2005-01-03"
END = "2026-05-08"


def load_q3() -> pl.DataFrame:
    return (
        pl.read_csv(RESULTS / "latest_q3_daily.csv", try_parse_dates=True)
        .sort("date")
        .select(["date", "nav"])
        .with_columns(
            [
                pl.col("nav").pct_change().fill_null(0.0).alias("ret_q3"),
                pl.col("nav").rolling_mean(50).alias("q3_ma50"),
                pl.col("nav").rolling_mean(100).alias("q3_ma100"),
                pl.col("nav").rolling_mean(150).alias("q3_ma150"),
                pl.col("nav").rolling_mean(200).alias("q3_ma200"),
            ]
        )
        .with_columns(
            [
                (pl.col("nav") > pl.col("q3_ma50")).fill_null(True).shift(1).fill_null(True).alias("gate_q3_ma50"),
                (pl.col("nav") > pl.col("q3_ma100")).fill_null(True).shift(1).fill_null(True).alias("gate_q3_ma100"),
                (pl.col("nav") > pl.col("q3_ma150")).fill_null(True).shift(1).fill_null(True).alias("gate_q3_ma150"),
                (pl.col("nav") > pl.col("q3_ma200")).fill_null(True).shift(1).fill_null(True).alias("gate_q3_ma200"),
                (pl.col("nav").pct_change(21) > 0).fill_null(True).shift(1).fill_null(True).alias("gate_q3_mom21"),
                (pl.col("nav").pct_change(63) > 0).fill_null(True).shift(1).fill_null(True).alias("gate_q3_mom63"),
                (pl.col("nav").pct_change(126) > 0).fill_null(True).shift(1).fill_null(True).alias("gate_q3_mom126"),
            ]
        )
        .select(
            [
                "date",
                "ret_q3",
                "gate_q3_ma50",
                "gate_q3_ma100",
                "gate_q3_ma150",
                "gate_q3_ma200",
                "gate_q3_mom21",
                "gate_q3_mom63",
                "gate_q3_mom126",
            ]
        )
    )


def load_event() -> pl.DataFrame:
    return (
        pl.read_csv(RESULTS / "iter_40_breakout_risk_ma200_cash_top3_daily.csv", try_parse_dates=True)
        .sort("date")
        .select(["date", "nav", "active"])
        .with_columns(pl.col("nav").pct_change().fill_null(0.0).alias("ret_event"))
        .with_columns((pl.col("active").shift(1).fill_null(0) > 0).alias("had_position_prev"))
        .with_columns(((pl.col("active") > 0) | pl.col("had_position_prev")).alias("event_invested"))
        .select(["date", "ret_event", "event_invested"])
    )


def load_market_gates() -> pl.DataFrame:
    con = connect()
    try:
        px = (
            fetch_adjusted_panel(
                con,
                START,
                END,
                codes=["0050"],
                market="twse",
                include_extra_history_days=420,
            )
            .sort("date")
            .with_columns(
                [
                    pl.col("close").rolling_mean(50).alias("ma50"),
                    pl.col("close").rolling_mean(100).alias("ma100"),
                    pl.col("close").rolling_mean(150).alias("ma150"),
                    pl.col("close").rolling_mean(200).alias("ma200"),
                    pl.col("close").rolling_mean(250).alias("ma250"),
                    pl.col("close").rolling_max(252).alias("hi252"),
                ]
            )
            .with_columns(
                [
                    (pl.col("close") > pl.col("ma50")).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_ma50"),
                    (pl.col("close") > pl.col("ma100")).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_ma100"),
                    (pl.col("close") > pl.col("ma150")).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_ma150"),
                    (pl.col("close") > pl.col("ma200")).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_ma200"),
                    (pl.col("close") > pl.col("ma250")).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_ma250"),
                    (pl.col("close").pct_change(21) > 0).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_mom21"),
                    (pl.col("close").pct_change(63) > 0).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_mom63"),
                    (pl.col("close").pct_change(126) > 0).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_mom126"),
                    (pl.col("close").pct_change(252) > 0).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_mom252"),
                    (pl.col("close") / pl.col("hi252") > 0.90).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_dd10"),
                    (pl.col("close") / pl.col("hi252") > 0.85).fill_null(True).shift(1).fill_null(True).alias("gate_mkt_dd15"),
                ]
            )
            .filter((pl.col("date") >= pl.date(2005, 1, 3)) & (pl.col("date") <= pl.date(2026, 5, 8)))
            .select(
                [
                    "date",
                    "gate_mkt_ma50",
                    "gate_mkt_ma100",
                    "gate_mkt_ma150",
                    "gate_mkt_ma200",
                    "gate_mkt_ma250",
                    "gate_mkt_mom21",
                    "gate_mkt_mom63",
                    "gate_mkt_mom126",
                    "gate_mkt_mom252",
                    "gate_mkt_dd10",
                    "gate_mkt_dd15",
                ]
            )
        )
    finally:
        con.close()
    return px


def load_base() -> pl.DataFrame:
    base = load_q3().join(load_event(), on="date", how="inner").join(load_market_gates(), on="date", how="left")
    gate_cols = [c for c in base.columns if c.startswith("gate_")]
    base = base.with_columns([pl.col(c).fill_null(True) for c in gate_cols])
    return (
        base.with_columns(
            [
                pl.lit(False).alias("gate_cash"),
                pl.lit(True).alias("gate_always_q3"),
                (pl.col("gate_mkt_ma150") | pl.col("gate_mkt_mom63")).alias("gate_mkt_ma150_or_mom63"),
                (pl.col("gate_mkt_ma200") | pl.col("gate_mkt_mom63")).alias("gate_mkt_ma200_or_mom63"),
                (pl.col("gate_mkt_ma100") & pl.col("gate_mkt_mom21")).alias("gate_mkt_ma100_and_mom21"),
                (pl.col("gate_q3_ma100") | pl.col("gate_q3_mom63")).alias("gate_q3_ma100_or_mom63"),
                (pl.col("gate_q3_ma100") & pl.col("gate_mkt_ma150")).alias("gate_q3_ma100_and_mkt_ma150"),
                (pl.col("gate_q3_mom63") & pl.col("gate_mkt_mom63")).alias("gate_q3_mom63_and_mkt_mom63"),
                pl.col("date").dt.year().alias("year"),
            ]
        )
        .sort("date")
    )


def annual_blend(q_weight: float, gate_col: str, base: pl.DataFrame) -> pl.DataFrame:
    df = base.with_columns(
        pl.when(pl.col("event_invested"))
        .then(pl.col("ret_event"))
        .otherwise(pl.when(pl.col(gate_col)).then(pl.col("ret_q3")).otherwise(0.0))
        .alias("ret_sat")
    )
    nav = CAPITAL
    rows = []
    sat_weight = 1.0 - q_weight
    for _, sub in df.group_by("year", maintain_order=True):
        cap_q = nav * q_weight
        cap_s = nav * sat_weight
        for d, rq, rs in zip(sub["date"].to_list(), sub["ret_q3"].to_list(), sub["ret_sat"].to_list(), strict=True):
            cap_q *= 1 + rq
            cap_s *= 1 + rs
            nav = cap_q + cap_s
            rows.append({"date": d, "nav": nav})
    return pl.DataFrame(rows)


def main() -> None:
    base = load_base()
    gates = [
        "gate_cash",
        "gate_always_q3",
        "gate_mkt_ma50",
        "gate_mkt_ma100",
        "gate_mkt_ma150",
        "gate_mkt_ma200",
        "gate_mkt_ma250",
        "gate_mkt_mom21",
        "gate_mkt_mom63",
        "gate_mkt_mom126",
        "gate_mkt_mom252",
        "gate_mkt_dd10",
        "gate_mkt_dd15",
        "gate_mkt_ma150_or_mom63",
        "gate_mkt_ma200_or_mom63",
        "gate_mkt_ma100_and_mom21",
        "gate_q3_ma50",
        "gate_q3_ma100",
        "gate_q3_ma150",
        "gate_q3_ma200",
        "gate_q3_mom21",
        "gate_q3_mom63",
        "gate_q3_mom126",
        "gate_q3_ma100_or_mom63",
        "gate_q3_ma100_and_mkt_ma150",
        "gate_q3_mom63_and_mkt_mom63",
    ]
    weights = list(range(45, 76))
    n_trials = len(gates) * len(weights)
    print(f"[iter45] loaded base rows={base.height} gates={len(gates)} candidates={n_trials}", flush=True)
    rows = []
    for gate in gates:
        print(f"[iter45] gate={gate}", flush=True)
        for pct in weights:
            name = f"iter45_q3_risk_breakout_top3_w{pct}_{gate}"
            daily = annual_blend(pct / 100, gate, base)
            out_path = RESULTS / f"{name}_daily.csv"
            daily.write_csv(out_path)
            row = validate_daily(
                name,
                daily,
                n_trials,
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
    out = RESULTS / "iter_45_fallback_gate_sweep_summary.csv"
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
    print("iter_45 fallback gate sweep")
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
