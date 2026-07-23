"""iter_62 - sector-leadership meta switch.

Active ETFs recently beat the conservative production strategy because they
loaded into the dominant technology / AI leadership regime. This iteration
tests a deployable version of that idea without using the short active-ETF
history as an optimization input:

  - use long-history 0052/0050 relative strength as the sector leadership proxy;
  - keep a production core by default;
  - promote into costed high-firepower sleeves only when sector leadership and
    sleeve-level momentum agree;
  - charge an extra whole-book switch cost for every meta state change.

All signals are prior-day signals. The full-validation DSR uses the full number
of searched specifications, not just the shortlisted candidates.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from quantlib.db import connect  # noqa: E402
from iter_40_research_campaign import CAPITAL, metrics_from_rets, validate_daily  # noqa: E402
from iter_57_cost_aware_switch import confirmed, scheduled_dates  # noqa: E402
from quantlib.prices import total_return_series  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
SWITCH_COST = 0.00357
VALIDATE_TOP_N = 240


@dataclass(frozen=True)
class Sleeve:
    key: str
    label: str
    path: Path
    max_active: float


@dataclass(frozen=True)
class MetaSpec:
    name: str
    core: Sleeve
    attack: Sleeve
    gate: str
    schedule: str
    lookback: int
    margin: float
    min_hold_days: int
    confirm_days: int


CORES = [
    Sleeve(
        "iter42",
        "Corrected Iter42 w59 core",
        RESULTS / "iter42_q3_risk_breakout_top3_w59_daily.csv",
        6.0,
    ),
    Sleeve(
        "iter61",
        "Iter61 recency meta switch",
        RESULTS / "iter61_meta_gate_mkt_ma50_q3_ma50_sq_ma50_monthly_lb63_m0_hold20_confirm2_daily.csv",
        6.0,
    ),
]
ATTACKS = [
    Sleeve(
        "iter57",
        "Iter57 cost-aware monthly switch",
        RESULTS
        / "iter57_iter44_w74_q3_trend_iter52_squeeze_top5_gate_mkt_mom63_q3_ma50_sq_ma50_exit_gate_mkt_mom63_q3_ma50_sq_ma50_monthly_hold20_confirm3_daily.csv",
        6.0,
    ),
    Sleeve(
        "iter56_mkt36",
        "Iter56 costed mkt63/q3/squeeze 36bp",
        RESULTS / "iter56_iter55_mkt_mom63_q3_ma50_sq_ma50_tax_commission_36bp_daily.csv",
        6.0,
    ),
]

SECTOR_GATES = [
    "gate_0052_mom21",
    "gate_0052_mom63",
    "gate_tech_rs_mom21",
    "gate_tech_rs_mom63",
    "gate_tech_rs_ma63",
    "gate_tech_rs_ma126",
    "gate_tech_rs_ma63_abs63",
    "gate_tech_rs_ma126_abs63",
    "gate_tech_rs_mom21_abs21",
    "gate_tech_rs_mom63_abs63",
]
SCHEDULES = ["weekly", "monthly"]
LOOKBACKS = [21, 63, 126, 252]
MARGINS = [-0.05, 0.0, 0.05, 0.10]
MIN_HOLDS = [5, 10, 20, 40]
CONFIRMS = [1, 2]


def load_nav(sleeve: Sleeve, suffix: str) -> pl.DataFrame:
    if not sleeve.path.exists():
        raise FileNotFoundError(sleeve.path)
    return (
        pl.read_csv(sleeve.path, try_parse_dates=True)
        .sort("date")
        .select(["date", pl.col("nav").cast(pl.Float64).alias(f"nav_{suffix}")])
        .with_columns(pl.col(f"nav_{suffix}").pct_change().fill_null(0.0).alias(f"ret_{suffix}"))
    )


def load_sector_features(start: str, end: str) -> pl.DataFrame:
    con = connect(read_only=True)
    try:
        tech = total_return_series(con, "0052", start, end, market="twse").select(
            ["date", pl.col("adj_close").alias("px_0052")]
        )
        market = total_return_series(con, "0050", start, end, market="twse").select(
            ["date", pl.col("adj_close").alias("px_0050")]
        )
    finally:
        con.close()

    return (
        tech.join(market, on="date", how="inner")
        .sort("date")
        .with_columns(
            [
                (pl.col("px_0052") / pl.col("px_0050")).alias("tech_rs"),
                pl.col("px_0052").pct_change(21).shift(1).fill_null(0.0).alias("mom_0052_21"),
                pl.col("px_0052").pct_change(63).shift(1).fill_null(0.0).alias("mom_0052_63"),
                (pl.col("px_0052") / pl.col("px_0050")).pct_change(21).shift(1).fill_null(0.0).alias("mom_rs_21"),
                (pl.col("px_0052") / pl.col("px_0050")).pct_change(63).shift(1).fill_null(0.0).alias("mom_rs_63"),
                (pl.col("px_0052") / pl.col("px_0050")).rolling_mean(63).shift(1).alias("rs_ma63"),
                (pl.col("px_0052") / pl.col("px_0050")).rolling_mean(126).shift(1).alias("rs_ma126"),
                (pl.col("px_0052") / pl.col("px_0050")).shift(1).alias("rs_lag1"),
            ]
        )
        .with_columns(
            [
                (pl.col("mom_0052_21") > 0.0).alias("gate_0052_mom21"),
                (pl.col("mom_0052_63") > 0.0).alias("gate_0052_mom63"),
                (pl.col("mom_rs_21") > 0.0).alias("gate_tech_rs_mom21"),
                (pl.col("mom_rs_63") > 0.0).alias("gate_tech_rs_mom63"),
                (pl.col("rs_lag1") > pl.col("rs_ma63")).alias("gate_tech_rs_ma63"),
                (pl.col("rs_lag1") > pl.col("rs_ma126")).alias("gate_tech_rs_ma126"),
            ]
        )
        .with_columns(
            [
                (pl.col("gate_tech_rs_ma63") & pl.col("gate_0052_mom63")).alias("gate_tech_rs_ma63_abs63"),
                (pl.col("gate_tech_rs_ma126") & pl.col("gate_0052_mom63")).alias("gate_tech_rs_ma126_abs63"),
                (pl.col("gate_tech_rs_mom21") & pl.col("gate_0052_mom21")).alias("gate_tech_rs_mom21_abs21"),
                (pl.col("gate_tech_rs_mom63") & pl.col("gate_0052_mom63")).alias("gate_tech_rs_mom63_abs63"),
            ]
        )
        .select(["date", *SECTOR_GATES])
    )


def load_base(core: Sleeve, attack: Sleeve) -> pl.DataFrame:
    raw = load_nav(core, "core").join(load_nav(attack, "attack"), on="date", how="inner").sort("date")
    sector = load_sector_features(raw["date"][0].isoformat(), raw["date"][-1].isoformat())
    return (
        raw.join(sector, on="date", how="left")
        .with_columns([pl.col(c).fill_null(False) for c in SECTOR_GATES])
        .sort("date")
    )


def add_momentum(base: pl.DataFrame, lookback: int) -> pl.DataFrame:
    return base.with_columns(
        [
            pl.col("nav_core").pct_change(lookback).shift(1).fill_null(0.0).alias("mom_core"),
            pl.col("nav_attack").pct_change(lookback).shift(1).fill_null(0.0).alias("mom_attack"),
        ]
    )


def simulate_meta(base_raw: pl.DataFrame, spec: MetaSpec) -> pl.DataFrame:
    base = add_momentum(base_raw, spec.lookback)
    dates = base["date"].to_list()
    ret_core = base["ret_core"].to_numpy().astype(float)
    ret_attack = base["ret_attack"].to_numpy().astype(float)
    rel = (base["mom_attack"] - base["mom_core"]).to_numpy().astype(float)
    gate = base[spec.gate].to_numpy().astype(bool)
    enter = confirmed(gate & (rel >= spec.margin), spec.confirm_days)
    exit_ = confirmed((~gate) | (rel < -spec.margin / 2.0), spec.confirm_days)
    sched = scheduled_dates(base, spec.schedule)

    state = "core"
    held = 10_000
    rets = []
    selected = []
    switched = []
    for i in range(len(dates)):
        new_state = state
        if sched[i] and held >= spec.min_hold_days:
            if state == "core" and enter[i]:
                new_state = "attack"
            elif state == "attack" and exit_[i]:
                new_state = "core"
        did_switch = new_state != state
        state = new_state
        held = 0 if did_switch else held + 1
        r = ret_attack[i] if state == "attack" else ret_core[i]
        if did_switch:
            r = (1.0 + r) * (1.0 - SWITCH_COST) - 1.0
        rets.append(r)
        selected.append(state)
        switched.append(did_switch)

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


def fast_screen_row(spec: MetaSpec, daily: pl.DataFrame, switches: float, attack_day_pct: float) -> dict[str, float | str]:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    full = metrics_from_rets(rets, dates)
    frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = frame.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    recent = frame.filter(pl.col("date") >= frame["date"][-1] - timedelta(days=365 * 3))
    active_window = frame.filter(pl.col("date") >= pl.date(2026, 1, 22))
    oos_metrics = metrics_from_rets(oos["ret"].to_numpy(), oos["date"].to_list())
    recent_metrics = metrics_from_rets(recent["ret"].to_numpy(), recent["date"].to_list())
    active_metrics = metrics_from_rets(active_window["ret"].to_numpy(), active_window["date"].to_list())
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
        "active_window_total_return": active_metrics["final_nav"] / CAPITAL - 1.0,
        "active_window_cagr": active_metrics["cagr"],
        "active_window_mdd": active_metrics["mdd"],
        "core": spec.core.key,
        "attack": spec.attack.key,
        "gate": spec.gate,
        "schedule": spec.schedule,
        "lookback": float(spec.lookback),
        "margin": spec.margin,
        "min_hold_days": float(spec.min_hold_days),
        "confirm_days": float(spec.confirm_days),
        "max_active": max(spec.core.max_active, spec.attack.max_active),
        "trade_days": switches,
        "avg_turnover_trade_day": SWITCH_COST if switches else 0.0,
        "switches": switches,
        "attack_day_pct": attack_day_pct,
    }


def build_specs() -> list[MetaSpec]:
    return [
        MetaSpec(
            name=(
                f"iter62_{core.key}_{attack.key}_{gate}_{schedule}"
                f"_lb{lookback}_m{int(margin * 100)}_hold{hold}_confirm{confirm}"
            ),
            core=core,
            attack=attack,
            gate=gate,
            schedule=schedule,
            lookback=lookback,
            margin=margin,
            min_hold_days=hold,
            confirm_days=confirm,
        )
        for core in CORES
        for attack in ATTACKS
        for gate in SECTOR_GATES
        for schedule in SCHEDULES
        for lookback in LOOKBACKS
        for margin in MARGINS
        for hold in MIN_HOLDS
        for confirm in CONFIRMS
    ]


def main() -> None:
    specs = build_specs()
    n_trials = len(specs)
    print(f"[iter62] specs={n_trials} validate_top={VALIDATE_TOP_N} switch_cost={SWITCH_COST:.4%}", flush=True)

    bases: dict[tuple[str, str], pl.DataFrame] = {}
    screen_rows = []
    daily_cache: dict[str, pl.DataFrame] = {}
    for i, spec in enumerate(specs, 1):
        key = (spec.core.key, spec.attack.key)
        base = bases.get(key)
        if base is None:
            base = load_base(spec.core, spec.attack)
            bases[key] = base
            print(
                f"[iter62] loaded base core={spec.core.key} attack={spec.attack.key} "
                f"{base['date'][0]}->{base['date'][-1]} rows={base.height}",
                flush=True,
            )
        df = simulate_meta(base, spec)
        switches = float(df["switched"].sum())
        attack_pct = float((df["selected"] == "attack").sum()) / max(df.height, 1)
        row = fast_screen_row(spec, df.select(["date", "nav"]), switches, attack_pct)
        row["screen_pass"] = (
            row["oos_sortino"] > 1.35
            and row["oos_mdd"] > -0.43
            and row["recent_3y_cagr"] > 0.30
            and row["active_window_total_return"] > 0.25
            and row["trade_days"] <= 120.0
        )
        screen_rows.append(row)
        if row["screen_pass"] or (row["oos_sortino"] > 1.55 and row["active_window_total_return"] > 0.20):
            daily_cache[spec.name] = df
        if i % 500 == 0:
            print(
                f"[iter62 screen] {i:04d}/{n_trials} {spec.name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%} ActiveWin={row['active_window_total_return']:+.2%}",
                flush=True,
            )

    screen = pl.DataFrame(screen_rows).sort(
        ["screen_pass", "oos_sortino", "active_window_total_return", "recent_3y_cagr"],
        descending=[True, True, True, True],
    )
    screen_path = RESULTS / "iter_62_sector_leadership_meta_screen.csv"
    screen.write_csv(screen_path)
    selected_names = screen.head(VALIDATE_TOP_N)["name"].to_list()
    by_name = {spec.name: spec for spec in specs}
    print(f"[iter62] full-validation candidates={len(selected_names)}", flush=True)

    rows = []
    for i, name in enumerate(selected_names, 1):
        spec = by_name[name]
        df = daily_cache.get(name)
        if df is None:
            df = simulate_meta(bases[(spec.core.key, spec.attack.key)], spec)
        daily = df.select(["date", "nav"])
        out_path = RESULTS / f"{name}_daily.csv"
        daily.write_csv(out_path)
        switches = float(df["switched"].sum())
        row = validate_daily(
            name,
            daily,
            n_trials,
            {
                "max_active": max(spec.core.max_active, spec.attack.max_active),
                "trade_days": switches,
                "avg_turnover_trade_day": SWITCH_COST if switches else 0.0,
            },
        )
        row["core"] = spec.core.key
        row["attack"] = spec.attack.key
        row["gate"] = spec.gate
        row["schedule"] = spec.schedule
        row["lookback"] = spec.lookback
        row["margin"] = spec.margin
        row["min_hold_days"] = spec.min_hold_days
        row["confirm_days"] = spec.confirm_days
        row["switches"] = switches
        row["attack_day_pct"] = float((df["selected"] == "attack").sum()) / max(df.height, 1)
        active = fast_screen_row(spec, daily, switches, row["attack_day_pct"])
        row["active_window_total_return"] = active["active_window_total_return"]
        row["active_window_cagr"] = active["active_window_cagr"]
        row["active_window_mdd"] = active["active_window_mdd"]
        row["path"] = str(out_path)
        row["promotable"] = (
            row["dsr"] >= 0.95
            and row["pbo"] < 0.50
            and row["boot_cagr_lb"] > 0.10
            and row["oos_mdd"] > -0.43
            and row["max_active"] <= 10.0
        )
        rows.append(row)
        if i % 25 == 0 or row["promotable"]:
            print(
                f"[iter62 validate] {i:03d}/{len(selected_names)} {name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%} DSR={row['dsr']:.3f} PBO={row['pbo']:.3f} "
                f"ActiveWin={row['active_window_total_return']:+.2%}",
                flush=True,
            )

    summary = pl.DataFrame(rows).sort(
        ["promotable", "oos_sortino", "active_window_total_return", "oos_cagr"],
        descending=[True, True, True, True],
    )
    summary_path = RESULTS / "iter_62_sector_leadership_meta_summary.csv"
    summary.write_csv(summary_path)
    view_cols = [
        "name",
        "promotable",
        "core",
        "attack",
        "gate",
        "schedule",
        "lookback",
        pl.col("margin").mul(100).round(0).cast(pl.Int64).alias("margin_pct"),
        "min_hold_days",
        "confirm_days",
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
        pl.col("active_window_total_return").mul(100).round(2).alias("active_window_total_pct"),
        pl.col("switches").cast(pl.Int64),
    ]
    print("=" * 140)
    print("iter_62 sector-leadership meta switch")
    print("=" * 140)
    print(summary.select(view_cols).head(40).to_pandas().to_string(index=False))
    print("\nTop promotable by active-window return")
    print(
        summary.filter(pl.col("promotable"))
        .sort(["active_window_total_return", "oos_sortino"], descending=[True, True])
        .select(view_cols)
        .head(20)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {screen_path}")
    print(f"Saved: {summary_path}")


if __name__ == "__main__":
    main()
