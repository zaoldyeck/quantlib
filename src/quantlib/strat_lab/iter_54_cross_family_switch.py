"""iter_54 - whole-strategy switch across unrelated alpha families.

Blending two stock-picking portfolios can violate the <=10 holdings mandate
because the union of names may exceed 10. This iteration preserves the mandate
by choosing exactly one complete sleeve per day:

  - iter42: validated defensive champion
  - iter44: higher-OOS idle-fallback challenger
  - iter52: high-return but unstable squeeze family
  - iter53: walk-forward ML weekly family

Switches use only prior-day information: market/quality gates or trailing NAV
momentum of the complete sleeves.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, validate_daily  # noqa: E402
from iter_45_fallback_gate_sweep import load_base as load_gate_base  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
N_TRIALS_EFFECTIVE = 900


SLEEVES = [
    ("iter42_w59_champion", RESULTS / "iter42_q3_risk_breakout_top3_w59_daily.csv", 6.0),
    ("iter44_w74_q3_trend", RESULTS / "iter44_q3_risk_breakout_top3_w74_fallback_q3_trend_daily.csv", 6.0),
    ("iter52_squeeze_top5", RESULTS / "iter52_squeeze_top5_monthly_daily.csv", 5.0),
    ("iter53_lgbm_weekly_top10", RESULTS / "iter53_lgbm_h63_w5_weekly_top10_none_daily.csv", 10.0),
    ("iter53_lgbm_weekly_top10_half", RESULTS / "iter53_lgbm_h63_w5_weekly_top10_ma200_half_daily.csv", 10.0),
]

DEFENSE = "iter42_w59_champion"
ATTACKS = [name for name, _, _ in SLEEVES if name != DEFENSE]


def nav_to_features(name: str, path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return (
        pl.read_csv(path, try_parse_dates=True)
        .sort("date")
        .select(["date", "nav"])
        .with_columns(
            [
                pl.col("nav").pct_change().fill_null(0.0).alias(f"ret_{name}"),
                pl.col("nav").pct_change(21).shift(1).fill_null(0.0).alias(f"score_{name}_mom21"),
                pl.col("nav").pct_change(63).shift(1).fill_null(0.0).alias(f"score_{name}_mom63"),
                pl.col("nav").pct_change(126).shift(1).fill_null(0.0).alias(f"score_{name}_mom126"),
                (pl.col("nav") > pl.col("nav").rolling_mean(50)).fill_null(True).shift(1).fill_null(True).alias(
                    f"gate_{name}_ma50"
                ),
                (pl.col("nav") > pl.col("nav").rolling_mean(100)).fill_null(True).shift(1).fill_null(True).alias(
                    f"gate_{name}_ma100"
                ),
            ]
        )
        .drop("nav")
    )


def load_switch_base(sleeve_names: set[str] | None = None) -> pl.DataFrame:
    selected = [s for s in SLEEVES if sleeve_names is None or s[0] in sleeve_names]
    if not selected:
        raise ValueError("load_switch_base requires at least one sleeve")

    df = nav_to_features(*selected[0][:2])
    for name, path, _slots in selected[1:]:
        df = df.join(nav_to_features(name, path), on="date", how="inner")

    gates = load_gate_base().select(
        [
            "date",
            "gate_mkt_ma50",
            "gate_mkt_ma100",
            "gate_mkt_ma150",
            "gate_mkt_ma200",
            "gate_mkt_mom21",
            "gate_mkt_mom63",
            "gate_mkt_mom126",
            "gate_q3_ma50",
            "gate_q3_ma100",
            "gate_q3_mom21",
            "gate_q3_mom63",
        ]
    )
    df = df.join(gates, on="date", how="left")
    gate_cols = [c for c in df.columns if c.startswith("gate_")]
    derived_gates = [
        pl.lit(True).alias("gate_always"),
        (pl.col("gate_mkt_mom21") & pl.col("gate_q3_ma50")).alias("gate_mkt_mom21_and_q3_ma50"),
        (pl.col("gate_mkt_ma50") & pl.col("gate_q3_ma50")).alias("gate_mkt_ma50_and_q3_ma50"),
        (pl.col("gate_mkt_mom63") | pl.col("gate_q3_mom63")).alias("gate_mkt_mom63_or_q3_mom63"),
    ]
    if f"gate_{DEFENSE}_ma50" in df.columns:
        derived_gates.append(
            (pl.col(f"gate_{DEFENSE}_ma50") & pl.col("gate_mkt_mom21")).alias(
                "gate_defense_ma50_and_mkt_mom21"
            )
        )
    return (
        df.with_columns([pl.col(c).fill_null(True) for c in gate_cols])
        .with_columns(derived_gates)
        .sort("date")
    )


def daily_from_returns(dates: list, rets: np.ndarray) -> pl.DataFrame:
    return pl.DataFrame({"date": dates, "nav": CAPITAL * np.cumprod(1.0 + rets)})


def switch_one_attack(base: pl.DataFrame, attack: str, gate: str) -> pl.DataFrame:
    ret = (
        base.with_columns(
            pl.when(pl.col(gate)).then(pl.col(f"ret_{attack}")).otherwise(pl.col(f"ret_{DEFENSE}")).alias("ret")
        )["ret"]
        .to_numpy()
        .astype(float, copy=False)
    )
    return daily_from_returns(base["date"].to_list(), ret)


def switch_best_prior(base: pl.DataFrame, gate: str, lookback: int, include_defense: bool) -> pl.DataFrame:
    names = [name for name, _, _ in SLEEVES] if include_defense else ATTACKS
    score_cols = [f"score_{name}_mom{lookback}" for name in names]
    ret_cols = [f"ret_{name}" for name in names]
    scores = base.select(score_cols).to_numpy()
    rets = base.select(ret_cols).to_numpy()
    best_idx = np.argmax(scores, axis=1)
    selected = rets[np.arange(len(best_idx)), best_idx]
    defense = base[f"ret_{DEFENSE}"].to_numpy()
    gate_arr = base[gate].to_numpy()
    out = np.where(gate_arr, selected, defense)
    return daily_from_returns(base["date"].to_list(), out)


def slot_count(name: str) -> float:
    return dict((n, slots) for n, _p, slots in SLEEVES)[name]


def main() -> None:
    base = load_switch_base()
    gates = [
        "gate_always",
        "gate_mkt_mom21",
        "gate_mkt_mom63",
        "gate_mkt_ma50",
        "gate_mkt_ma100",
        "gate_q3_ma50",
        "gate_q3_mom63",
        "gate_mkt_mom21_and_q3_ma50",
        "gate_mkt_ma50_and_q3_ma50",
        "gate_mkt_mom63_or_q3_mom63",
        "gate_defense_ma50_and_mkt_mom21",
    ]
    print(f"[iter54] rows={base.height} attacks={len(ATTACKS)} gates={len(gates)} dsr_trials={N_TRIALS_EFFECTIVE}", flush=True)

    rows = []
    for gate in gates:
        print(f"[iter54] gate={gate}", flush=True)
        for attack in ATTACKS:
            name = f"iter54_switch_{attack}_when_{gate}"
            daily = switch_one_attack(base, attack, gate)
            out_path = RESULTS / f"{name}_daily.csv"
            daily.write_csv(out_path)
            row = validate_daily(
                name,
                daily,
                N_TRIALS_EFFECTIVE,
                {"max_active": max(slot_count(DEFENSE), slot_count(attack)), "trade_days": 0.0, "avg_turnover_trade_day": 0.0},
            )
            row["selector"] = "single_attack"
            row["attack"] = attack
            row["gate"] = gate
            row["lookback"] = 0
            row["promotable"] = (
                row["dsr"] >= 0.95
                and row["pbo"] < 0.50
                and row["boot_cagr_lb"] > 0.10
                and row["oos_mdd"] > -0.45
                and row["max_active"] <= 10.0
            )
            rows.append(row)

        for lookback in (21, 63, 126):
            for include_defense in (False, True):
                label = "all" if include_defense else "attacks"
                name = f"iter54_switch_best_{label}_mom{lookback}_when_{gate}"
                daily = switch_best_prior(base, gate, lookback, include_defense)
                out_path = RESULTS / f"{name}_daily.csv"
                daily.write_csv(out_path)
                row = validate_daily(
                    name,
                    daily,
                    N_TRIALS_EFFECTIVE,
                    {
                        "max_active": max(slot_count(n) for n in ([DEFENSE] + ATTACKS)),
                        "trade_days": 0.0,
                        "avg_turnover_trade_day": 0.0,
                    },
                )
                row["selector"] = f"best_{label}"
                row["attack"] = f"best_{label}"
                row["gate"] = gate
                row["lookback"] = lookback
                row["promotable"] = (
                    row["dsr"] >= 0.95
                    and row["pbo"] < 0.50
                    and row["boot_cagr_lb"] > 0.10
                    and row["oos_mdd"] > -0.45
                    and row["max_active"] <= 10.0
                )
                rows.append(row)

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_54_cross_family_switch_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "selector",
        "attack",
        "gate",
        "lookback",
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
    print("iter_54 cross-family whole-strategy switch")
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
