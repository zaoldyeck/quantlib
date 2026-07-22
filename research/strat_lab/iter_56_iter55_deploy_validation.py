"""iter_56 - deployment validation for iter55 switch candidates.

Before promoting iter55, validate what a live portfolio manager would care
about:

  - extra whole-sleeve switch friction, because iter55 stitches complete sleeve
    returns and does not otherwise charge for switching from defense to attack;
  - regime/cycle slices and recent performance;
  - yearly consistency and attack exposure concentration.

This is validation, not a new optimization pass.
"""
from __future__ import annotations

import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, RF, TDPY, validate_daily  # noqa: E402
from iter_55_squeeze_switch_refinement import add_refined_gates  # noqa: E402
from iter_54_cross_family_switch import load_switch_base  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")


@dataclass(frozen=True)
class Candidate:
    key: str
    label: str
    defense: str
    attack: str
    gate: str
    off_scale: float = 1.0


CANDIDATES = [
    Candidate(
        key="iter55_mkt_mom63_q3_ma50_sq_ma50",
        label="iter55 mkt_mom63+q3_ma50+squeeze_ma50",
        defense="iter44_w74_q3_trend",
        attack="iter52_squeeze_top5",
        gate="gate_mkt_mom63_q3_ma50_sq_ma50",
    ),
    Candidate(
        key="iter55_sq_mom21_beats_iter42_q3_ma50",
        label="iter55 squeeze mom21 beats iter42 + q3_ma50",
        defense="iter44_w74_q3_trend",
        attack="iter52_squeeze_top5",
        gate="gate_sq_mom21_beats_iter42_and_q3_ma50",
    ),
]
SWITCH_BASE_SLEEVES = {
    "iter42_w59_champion",
    "iter44_w74_q3_trend",
    "iter52_squeeze_top5",
}

FRICTION_SCENARIOS = [
    ("none", 0.0000),
    ("light_10bp", 0.0010),
    ("tax_commission_36bp", 0.00357),
    ("stress_75bp", 0.0075),
]

SLICES = [
    ("GFC_2008_2009", "2008-01-01", "2009-12-31"),
    ("Euro_2011", "2011-01-01", "2011-12-31"),
    ("China_2015", "2015-01-01", "2015-12-31"),
    ("TradeWar_2018", "2018-01-01", "2018-12-31"),
    ("Covid_2020", "2020-01-01", "2020-12-31"),
    ("Bear_2022", "2022-01-01", "2022-12-31"),
    ("Recent_2025_2026", "2025-01-01", "2026-05-08"),
]


def metrics_from_rets(rets: np.ndarray, dates: list) -> dict[str, float]:
    if len(rets) < 2:
        return {"cagr": 0.0, "sortino": 0.0, "sharpe": 0.0, "mdd": 0.0, "final_nav": CAPITAL}
    nav = CAPITAL * np.cumprod(1.0 + rets)
    years = max((dates[-1] - dates[0]).days / 365.25, len(rets) / TDPY, 1e-9)
    cagr = (nav[-1] / CAPITAL) ** (1.0 / years) - 1.0
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY))
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    peak = CAPITAL
    mdd = 0.0
    for v in nav:
        peak = max(peak, float(v))
        mdd = min(mdd, (float(v) - peak) / peak)
    return {
        "cagr": float(cagr),
        "sortino": float((cagr - RF) / downvol) if downvol > 0 else 0.0,
        "sharpe": float((cagr - RF) / vol) if vol > 0 else 0.0,
        "mdd": float(mdd),
        "final_nav": float(nav[-1]),
    }


def build_candidate_returns(base: pl.DataFrame, cand: Candidate, switch_cost: float) -> pl.DataFrame:
    selected_attack = base[cand.gate].to_numpy().astype(bool)
    ret_attack = base[f"ret_{cand.attack}"].to_numpy().astype(float)
    ret_defense = base[f"ret_{cand.defense}"].to_numpy().astype(float) * cand.off_scale
    rets = np.where(selected_attack, ret_attack, ret_defense)

    selected = np.where(selected_attack, cand.attack, cand.defense)
    switched = np.r_[False, selected[1:] != selected[:-1]]
    if switch_cost > 0:
        rets = np.where(switched, (1.0 + rets) * (1.0 - switch_cost) - 1.0, rets)

    nav = CAPITAL * np.cumprod(1.0 + rets)
    return pl.DataFrame(
        {
            "date": base["date"].to_list(),
            "nav": nav,
            "ret": rets,
            "selected": selected.tolist(),
            "switched": switched.tolist(),
        }
    )


def slice_rows(name: str, df: pl.DataFrame) -> list[dict[str, object]]:
    rows = []
    for label, start, end in SLICES:
        sub = df.filter((pl.col("date") >= pl.lit(start).str.to_date()) & (pl.col("date") <= pl.lit(end).str.to_date()))
        if sub.height < 20:
            continue
        m = metrics_from_rets(sub["ret"].to_numpy(), sub["date"].to_list())
        rows.append({"name": name, "slice": label, "days": sub.height, **m})
    return rows


def yearly_rows(name: str, df: pl.DataFrame) -> list[dict[str, object]]:
    rows = []
    for year, sub in df.with_columns(pl.col("date").dt.year().alias("year")).group_by("year", maintain_order=True):
        y = year[0] if isinstance(year, tuple) else year
        if sub.height < 20:
            continue
        m = metrics_from_rets(sub["ret"].to_numpy(), sub["date"].to_list())
        rows.append(
            {
                "name": name,
                "year": int(y),
                "days": sub.height,
                "attack_days": int((sub["selected"] == "iter52_squeeze_top5").sum()),
                "switches": int(sub["switched"].sum()),
                **m,
            }
        )
    return rows


def main() -> None:
    base = add_refined_gates(load_switch_base(SWITCH_BASE_SLEEVES))
    validation_rows = []
    slice_out = []
    yearly_out = []

    for cand in CANDIDATES:
        for scenario, switch_cost in FRICTION_SCENARIOS:
            name = f"{cand.key}_{scenario}"
            df = build_candidate_returns(base, cand, switch_cost)
            daily = df.select(["date", "nav"])
            daily.write_csv(RESULTS / f"iter56_{name}_daily.csv")
            row = validate_daily(
                name,
                daily,
                420,
                {
                    "max_active": 6.0,
                    "trade_days": float(df["switched"].sum()),
                    "avg_turnover_trade_day": switch_cost,
                },
            )
            row["candidate"] = cand.key
            row["label"] = cand.label
            row["friction_scenario"] = scenario
            row["switch_cost"] = switch_cost
            row["attack_day_pct"] = float((df["selected"] == cand.attack).sum()) / max(df.height, 1)
            row["switches"] = int(df["switched"].sum())
            row["promotable"] = (
                row["dsr"] >= 0.95
                and row["pbo"] < 0.50
                and row["boot_cagr_lb"] > 0.10
                and row["oos_mdd"] > -0.45
                and row["max_active"] <= 10.0
            )
            validation_rows.append(row)

            slice_out.extend(slice_rows(name, df))
            yearly_out.extend(yearly_rows(name, df))

    summary = pl.DataFrame(validation_rows).sort(
        ["promotable", "candidate", "switch_cost"], descending=[True, False, False]
    )
    slices = pl.DataFrame(slice_out)
    yearly = pl.DataFrame(yearly_out)
    summary.write_csv(RESULTS / "iter_56_iter55_deploy_validation_summary.csv")
    slices.write_csv(RESULTS / "iter_56_iter55_deploy_validation_slices.csv")
    yearly.write_csv(RESULTS / "iter_56_iter55_deploy_validation_yearly.csv")

    view_cols = [
        "candidate",
        "friction_scenario",
        pl.col("switch_cost").mul(10000).round(1).alias("switch_cost_bp"),
        pl.col("attack_day_pct").mul(100).round(1).alias("attack_day_pct"),
        "switches",
        pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
        pl.col("sortino").round(3).alias("full_sortino"),
        pl.col("mdd").mul(100).round(2).alias("full_mdd_pct"),
        pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
        pl.col("oos_sortino").round(3),
        pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
        pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
        pl.col("dsr").round(3),
        pl.col("pbo").round(3),
        "promotable",
    ]
    print("=" * 120)
    print("iter_56 iter55 deploy validation")
    print("=" * 120)
    print(summary.select(view_cols).to_pandas().to_string(index=False))
    print("\nWorst yearly returns under tax_commission_36bp:")
    print(
        yearly.filter(pl.col("name").str.ends_with("tax_commission_36bp"))
        .sort("cagr")
        .select(
            [
                "name",
                "year",
                pl.col("cagr").mul(100).round(2).alias("cagr_pct"),
                pl.col("sortino").round(3),
                pl.col("mdd").mul(100).round(2).alias("mdd_pct"),
                "attack_days",
                "switches",
            ]
        )
        .head(12)
        .to_pandas()
        .to_string(index=False)
    )
    print("\nSlice returns under tax_commission_36bp:")
    print(
        slices.filter(pl.col("name").str.ends_with("tax_commission_36bp"))
        .select(
            [
                "name",
                "slice",
                pl.col("cagr").mul(100).round(2).alias("cagr_pct"),
                pl.col("sortino").round(3),
                pl.col("mdd").mul(100).round(2).alias("mdd_pct"),
            ]
        )
        .to_pandas()
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
