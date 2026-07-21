"""iter_57 - cost-aware low-turnover whole-sleeve switch.

iter56 invalidated naive iter55 promotion after charging whole-sleeve switch
friction. This iteration rebuilds the switcher from the actual deployment
constraint: switching complete sleeves is expensive, so a live strategy should
switch slowly, require persistence, and charge the cost inside the simulation.

Design:
  - exactly one sleeve held per day, so max holdings is still <= 10;
  - switch cost is charged on every defense/attack state change;
  - state changes are allowed only on weekly/monthly schedule;
  - minimum holding period and enter/exit confirmation reduce churn.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, metrics_from_rets, validate_daily  # noqa: E402
from iter_54_cross_family_switch import load_switch_base, slot_count  # noqa: E402
from iter_55_squeeze_switch_refinement import add_refined_gates  # noqa: E402


RESULTS = Path("research/strat_lab/results")
SWITCH_COST = 0.00357
VALIDATE_TOP_N = 180


@dataclass(frozen=True)
class SwitchSpec:
    name: str
    defense: str
    attack: str
    entry_gate: str
    exit_gate: str
    schedule: str
    min_hold_days: int
    confirm_days: int


DEFENSES = ["iter44_w74_q3_trend", "iter42_w59_champion"]
ATTACKS = ["iter52_squeeze_top5"]
SWITCH_BASE_SLEEVES = set(DEFENSES + ATTACKS)
ENTRY_GATES = [
    "gate_mkt_mom63_q3_ma50_sq_ma50",
    "gate_sq_mom21_beats_iter42_and_q3_ma50",
    "gate_q3_ma50_and_sq_ma50",
    "gate_mkt_ma50_q3_ma50_sq_ma50",
]
EXIT_GATES = [
    "gate_mkt_mom63_q3_ma50_sq_ma50",
    "gate_q3_ma50_and_sq_ma50",
    "gate_mkt_ma50_q3_ma50_sq_ma50",
]
SCHEDULES = ["weekly", "monthly", "quarterly"]
MIN_HOLDS = [5, 10, 20, 40, 60]
CONFIRMS = [1, 2, 3]


def scheduled_dates(base: pl.DataFrame, schedule: str) -> np.ndarray:
    d = base["date"]
    if schedule == "weekly":
        return base.select((pl.col("date").dt.weekday() == 5).alias("x"))["x"].to_numpy()
    if schedule == "monthly":
        key = base.select((pl.col("date").dt.year() * 100 + pl.col("date").dt.month()).alias("m"))
        return key.select((pl.col("m") != pl.col("m").shift(1)).fill_null(True).alias("x"))["x"].to_numpy()
    if schedule == "quarterly":
        key = base.select((pl.col("date").dt.year() * 10 + pl.col("date").dt.quarter()).alias("q"))
        return key.select((pl.col("q") != pl.col("q").shift(1)).fill_null(True).alias("x"))["x"].to_numpy()
    raise ValueError(schedule)


def confirmed(arr: np.ndarray, days: int) -> np.ndarray:
    if days <= 1:
        return arr.astype(bool)
    out = np.zeros_like(arr, dtype=bool)
    count = 0
    for i, v in enumerate(arr.astype(bool)):
        count = count + 1 if v else 0
        out[i] = count >= days
    return out


def simulate_switch(base: pl.DataFrame, spec: SwitchSpec) -> pl.DataFrame:
    dates = base["date"].to_list()
    ret_defense = base[f"ret_{spec.defense}"].to_numpy().astype(float)
    ret_attack = base[f"ret_{spec.attack}"].to_numpy().astype(float)
    entry_gate = confirmed(base[spec.entry_gate].to_numpy().astype(bool), spec.confirm_days)
    exit_gate = confirmed(~base[spec.exit_gate].to_numpy().astype(bool), spec.confirm_days)
    sched = scheduled_dates(base, spec.schedule)

    selected = []
    rets = []
    switched = []
    state = spec.defense
    held = 10_000
    for i in range(len(dates)):
        new_state = state
        if sched[i] and held >= spec.min_hold_days:
            if state == spec.defense and entry_gate[i]:
                new_state = spec.attack
            elif state == spec.attack and exit_gate[i]:
                new_state = spec.defense

        did_switch = new_state != state
        state = new_state
        held = 0 if did_switch else held + 1
        r = ret_attack[i] if state == spec.attack else ret_defense[i]
        if did_switch:
            r = (1.0 + r) * (1.0 - SWITCH_COST) - 1.0
        selected.append(state)
        switched.append(did_switch)
        rets.append(r)

    arr = np.asarray(rets, dtype=float)
    return pl.DataFrame(
        {
            "date": dates,
            "nav": CAPITAL * np.cumprod(1.0 + arr),
            "ret": arr,
            "selected": selected,
            "switched": switched,
        }
    )


def fast_screen_row(spec: SwitchSpec, daily: pl.DataFrame, switches: float, attack_day_pct: float) -> dict[str, float | str]:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    full = metrics_from_rets(rets, dates)
    frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = frame.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_metrics = metrics_from_rets(oos["ret"].to_numpy(), oos["date"].to_list())
    recent = frame.filter(pl.col("date") >= frame["date"][-1] - timedelta(days=365 * 3))
    recent_metrics = metrics_from_rets(recent["ret"].to_numpy(), recent["date"].to_list())
    return {
        "name": spec.name,
        **full,
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_sharpe": oos_metrics["sharpe"],
        "oos_mdd": oos_metrics["mdd"],
        "recent_3y_cagr": recent_metrics["cagr"],
        "recent_3y_sortino": recent_metrics["sortino"],
        "recent_3y_mdd": recent_metrics["mdd"],
        "max_active": max(slot_count(spec.defense), slot_count(spec.attack)),
        "trade_days": switches,
        "avg_turnover_trade_day": SWITCH_COST if switches else 0.0,
        "defense": spec.defense,
        "attack": spec.attack,
        "entry_gate": spec.entry_gate,
        "exit_gate": spec.exit_gate,
        "schedule": spec.schedule,
        "min_hold_days": float(spec.min_hold_days),
        "confirm_days": float(spec.confirm_days),
        "switches": switches,
        "attack_day_pct": attack_day_pct,
    }


def main() -> None:
    base = add_refined_gates(load_switch_base(SWITCH_BASE_SLEEVES))
    specs = [
        SwitchSpec(
            name=f"iter57_{defense}_{attack}_{entry}_exit_{exit_gate}_{schedule}_hold{hold}_confirm{confirm}",
            defense=defense,
            attack=attack,
            entry_gate=entry,
            exit_gate=exit_gate,
            schedule=schedule,
            min_hold_days=hold,
            confirm_days=confirm,
        )
        for defense in DEFENSES
        for attack in ATTACKS
        for entry in ENTRY_GATES
        for exit_gate in EXIT_GATES
        for schedule in SCHEDULES
        for hold in MIN_HOLDS
        for confirm in CONFIRMS
    ]
    n_trials = len(specs)
    print(
        f"[iter57] specs={n_trials} validate_top={VALIDATE_TOP_N} switch_cost={SWITCH_COST:.4%}",
        flush=True,
    )

    screen_rows = []
    by_name = {spec.name: spec for spec in specs}
    for i, spec in enumerate(specs, 1):
        df = simulate_switch(base, spec)
        switches = float(df["switched"].sum())
        attack_day_pct = float((df["selected"] == spec.attack).sum()) / max(df.height, 1)
        row = fast_screen_row(spec, df.select(["date", "nav"]), switches, attack_day_pct)
        row["screen_pass"] = (
            row["oos_sortino"] > 1.20
            and row["oos_mdd"] > -0.45
            and row["recent_3y_cagr"] > 0.25
            and row["trade_days"] <= 90.0
        )
        screen_rows.append(row)
        if i % 120 == 0:
            print(
                f"[iter57 screen] {i:04d}/{n_trials} {spec.name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%} Recent3Y={row['recent_3y_cagr']:+.2%}",
                flush=True,
            )

    screen = pl.DataFrame(screen_rows).sort(
        ["screen_pass", "oos_sortino", "recent_3y_cagr", "oos_cagr"],
        descending=[True, True, True, True],
    )
    screen_path = RESULTS / "iter_57_cost_aware_switch_screen.csv"
    screen.write_csv(screen_path)
    selected_names = screen.head(VALIDATE_TOP_N)["name"].to_list()
    print(f"[iter57] full-validation candidates={len(selected_names)}", flush=True)

    rows = []
    for i, name in enumerate(selected_names, 1):
        spec = by_name[name]
        df = simulate_switch(base, spec)
        daily = df.select(["date", "nav"])
        out_path = RESULTS / f"{spec.name}_daily.csv"
        daily.write_csv(out_path)
        row = validate_daily(
            spec.name,
            daily,
            n_trials,
            {
                "max_active": max(slot_count(spec.defense), slot_count(spec.attack)),
                "trade_days": float(df["switched"].sum()),
                "avg_turnover_trade_day": SWITCH_COST,
            },
        )
        row["defense"] = spec.defense
        row["attack"] = spec.attack
        row["entry_gate"] = spec.entry_gate
        row["exit_gate"] = spec.exit_gate
        row["schedule"] = spec.schedule
        row["min_hold_days"] = spec.min_hold_days
        row["confirm_days"] = spec.confirm_days
        row["switches"] = int(df["switched"].sum())
        row["attack_day_pct"] = float((df["selected"] == spec.attack).sum()) / max(df.height, 1)
        row["path"] = str(out_path)
        row["promotable"] = (
            row["dsr"] >= 0.95
            and row["pbo"] < 0.50
            and row["boot_cagr_lb"] > 0.10
            and row["oos_mdd"] > -0.45
            and row["max_active"] <= 10.0
        )
        rows.append(row)
        if i % 25 == 0 or row["promotable"]:
            print(
                f"[iter57 validate] {i:03d}/{len(selected_names)} {spec.name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%} DSR={row['dsr']:.3f} PBO={row['pbo']:.3f}",
                flush=True,
            )

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_57_cost_aware_switch_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "defense",
        "entry_gate",
        "exit_gate",
        "schedule",
        "min_hold_days",
        "confirm_days",
        "switches",
        pl.col("attack_day_pct").mul(100).round(1).alias("attack_day_pct"),
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
    print("iter_57 cost-aware low-turnover switch")
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
    print(f"\nSaved: {screen_path}")
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
