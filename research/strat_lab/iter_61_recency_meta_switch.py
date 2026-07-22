"""iter_61 - recency-weighted meta switch between production core and candidate.

This is the first research pass under the new framework. It does not optimize
single-stock signals. Instead it asks a PM-level question:

  Can the production core stay active by default, while a paper/watchlist
  candidate is promoted only when recent evidence and regime gates agree?

The switch uses only prior-day information, charges an additional whole-book
switch cost, and validates only the top screen candidates with full DSR/PBO.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, metrics_from_rets, validate_daily  # noqa: E402
from iter_54_cross_family_switch import load_switch_base  # noqa: E402
from iter_55_squeeze_switch_refinement import add_refined_gates  # noqa: E402
from iter_57_cost_aware_switch import confirmed, scheduled_dates  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
SWITCH_COST = 0.00357
VALIDATE_TOP_N = 140
SWITCH_BASE_SLEEVES = {
    "iter42_w59_champion",
    "iter44_w74_q3_trend",
    "iter52_squeeze_top5",
}

CORE_PATH = RESULTS / "iter42_q3_risk_breakout_top3_w59_daily.csv"
ITER57_SUMMARY_PATH = RESULTS / "iter_57_cost_aware_switch_summary.csv"
CANDIDATE_FALLBACK_PATH = (
    RESULTS
    / "iter57_iter44_w74_q3_trend_iter52_squeeze_top5_gate_mkt_mom63_q3_ma50_sq_ma50_exit_gate_mkt_mom63_q3_ma50_sq_ma50_monthly_hold20_confirm3_daily.csv"
)


@dataclass(frozen=True)
class MetaSpec:
    name: str
    gate: str
    schedule: str
    lookback: int
    margin: float
    min_hold_days: int
    confirm_days: int


GATES = [
    "gate_mkt_mom63_q3_ma50_sq_ma50",
    "gate_q3_ma50_and_sq_ma50",
    "gate_mkt_ma50_q3_ma50_sq_ma50",
    "gate_sq_mom21_beats_iter42_and_q3_ma50",
]
SCHEDULES = ["monthly", "quarterly"]
LOOKBACKS = [63, 126, 252, 504]
MARGINS = [0.00, 0.05, 0.10]
MIN_HOLDS = [20, 40, 60]
CONFIRMS = [1, 2]


def load_nav(path: Path, key: str) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return (
        pl.read_csv(path, try_parse_dates=True)
        .sort("date")
        .select(["date", pl.col("nav").cast(pl.Float64).alias(f"nav_{key}")])
        .with_columns(pl.col(f"nav_{key}").pct_change().fill_null(0.0).alias(f"ret_{key}"))
    )


def select_candidate_path() -> Path:
    if ITER57_SUMMARY_PATH.exists():
        summary = pl.read_csv(ITER57_SUMMARY_PATH)
        if {"promotable", "path", "oos_sortino", "boot_cagr_lb", "oos_cagr"}.issubset(summary.columns):
            promotable = summary.filter(pl.col("promotable"))
            candidate_pool = promotable if promotable.height else summary
            if candidate_pool.height:
                path = Path(
                    candidate_pool.sort(
                        ["oos_sortino", "boot_cagr_lb", "oos_cagr"],
                        descending=[True, True, True],
                    )["path"][0]
                )
                if path.exists():
                    return path
    return CANDIDATE_FALLBACK_PATH


def load_base() -> tuple[pl.DataFrame, Path]:
    candidate_path = select_candidate_path()
    gates = add_refined_gates(load_switch_base(SWITCH_BASE_SLEEVES)).select(["date", *GATES])
    base = (
        load_nav(CORE_PATH, "core")
        .join(load_nav(candidate_path, "candidate"), on="date", how="inner")
        .join(gates, on="date", how="left")
        .with_columns([pl.col(c).fill_null(False) for c in GATES])
        .sort("date")
    )
    return base, candidate_path


def add_momentum(base: pl.DataFrame, lookback: int) -> pl.DataFrame:
    return base.with_columns(
        [
            pl.col("nav_core").pct_change(lookback).shift(1).fill_null(0.0).alias("mom_core"),
            pl.col("nav_candidate").pct_change(lookback).shift(1).fill_null(0.0).alias("mom_candidate"),
        ]
    )


def simulate_meta(base_raw: pl.DataFrame, spec: MetaSpec) -> pl.DataFrame:
    base = add_momentum(base_raw, spec.lookback)
    dates = base["date"].to_list()
    ret_core = base["ret_core"].to_numpy().astype(float)
    ret_candidate = base["ret_candidate"].to_numpy().astype(float)
    rel = (base["mom_candidate"] - base["mom_core"]).to_numpy().astype(float)
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
                new_state = "candidate"
            elif state == "candidate" and exit_[i]:
                new_state = "core"
        did_switch = new_state != state
        state = new_state
        held = 0 if did_switch else held + 1
        r = ret_candidate[i] if state == "candidate" else ret_core[i]
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


def fast_screen_row(name: str, daily: pl.DataFrame, extra: dict[str, float]) -> dict[str, float | str]:
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
        "name": name,
        **full,
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_sharpe": oos_metrics["sharpe"],
        "oos_mdd": oos_metrics["mdd"],
        "recent_3y_cagr": recent_metrics["cagr"],
        "recent_3y_sortino": recent_metrics["sortino"],
        "recent_3y_mdd": recent_metrics["mdd"],
        **extra,
    }


def build_specs() -> list[MetaSpec]:
    return [
        MetaSpec(
            name=f"iter61_meta_{gate}_{schedule}_lb{lookback}_m{int(margin*100)}_hold{hold}_confirm{confirm}",
            gate=gate,
            schedule=schedule,
            lookback=lookback,
            margin=margin,
            min_hold_days=hold,
            confirm_days=confirm,
        )
        for gate in GATES
        for schedule in SCHEDULES
        for lookback in LOOKBACKS
        for margin in MARGINS
        for hold in MIN_HOLDS
        for confirm in CONFIRMS
    ]


def main() -> None:
    base, candidate_path = load_base()
    specs = build_specs()
    n_trials = len(specs)
    print(
        f"[iter61] candidate={candidate_path.name} specs={n_trials} validate_top={VALIDATE_TOP_N}",
        flush=True,
    )

    screen_rows = []
    daily_cache: dict[str, pl.DataFrame] = {}
    for i, spec in enumerate(specs, 1):
        df = simulate_meta(base, spec)
        switches = float(df["switched"].sum())
        candidate_pct = float((df["selected"] == "candidate").sum()) / max(df.height, 1)
        row = fast_screen_row(
            spec.name,
            df.select(["date", "nav"]),
            {
                "max_active": 6.0,
                "trade_days": switches,
                "avg_turnover_trade_day": SWITCH_COST if switches else 0.0,
                "switches": switches,
                "candidate_day_pct": candidate_pct,
                "gate": spec.gate,
                "schedule": spec.schedule,
                "lookback": float(spec.lookback),
                "margin": spec.margin,
                "min_hold_days": float(spec.min_hold_days),
                "confirm_days": float(spec.confirm_days),
            },
        )
        row["screen_pass"] = row["recent_3y_cagr"] > 0.25 and row["oos_mdd"] > -0.45 and row["oos_sortino"] > 1.2
        screen_rows.append(row)
        # Keep only very plausible dailies in memory; anything else can be rerun.
        if row["screen_pass"] or row["oos_sortino"] > 1.5:
            daily_cache[spec.name] = df
        if i % 120 == 0:
            print(
                f"[iter61 screen] {i:03d}/{n_trials} {spec.name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%} Recent3Y={row['recent_3y_cagr']:+.2%}",
                flush=True,
            )

    screen = pl.DataFrame(screen_rows).sort(
        ["screen_pass", "oos_sortino", "recent_3y_cagr", "oos_cagr"],
        descending=[True, True, True, True],
    )
    screen_path = RESULTS / "iter_61_recency_meta_switch_screen.csv"
    screen.write_csv(screen_path)
    selected_names = set(screen.head(VALIDATE_TOP_N)["name"].to_list())
    by_name = {spec.name: spec for spec in specs}
    print(f"[iter61] full-validation candidates={len(selected_names)}", flush=True)

    rows = []
    for name in selected_names:
        spec = by_name[name]
        df = daily_cache.get(name)
        if df is None:
            df = simulate_meta(base, spec)
        daily = df.select(["date", "nav"])
        out_path = RESULTS / f"{name}_daily.csv"
        daily.write_csv(out_path)
        switches = float(df["switched"].sum())
        row = validate_daily(
            name,
            daily,
            n_trials,
            {
                "max_active": 6.0,
                "trade_days": switches,
                "avg_turnover_trade_day": SWITCH_COST if switches else 0.0,
            },
        )
        row["gate"] = spec.gate
        row["schedule"] = spec.schedule
        row["lookback"] = spec.lookback
        row["margin"] = spec.margin
        row["min_hold_days"] = spec.min_hold_days
        row["confirm_days"] = spec.confirm_days
        row["switches"] = switches
        row["candidate_day_pct"] = float((df["selected"] == "candidate").sum()) / max(df.height, 1)
        row["path"] = str(out_path)
        row["promotable"] = (
            row["dsr"] >= 0.95
            and row["pbo"] < 0.50
            and row["boot_cagr_lb"] > 0.10
            and row["oos_mdd"] > -0.45
            and row["max_active"] <= 10.0
        )
        rows.append(row)
        if len(rows) % 25 == 0 or row["promotable"]:
            print(
                f"[iter61 validate] {len(rows):03d}/{len(selected_names)} {name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%} DSR={row['dsr']:.3f} PBO={row['pbo']:.3f}",
                flush=True,
            )

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_61_recency_meta_switch_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "gate",
        "schedule",
        "lookback",
        pl.col("margin").mul(100).round(0).cast(pl.Int64).alias("margin_pct"),
        "min_hold_days",
        "confirm_days",
        pl.col("candidate_day_pct").mul(100).round(1).alias("candidate_day_pct"),
        pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
        pl.col("sortino").round(3).alias("full_sortino"),
        pl.col("mdd").mul(100).round(2).alias("full_mdd_pct"),
        pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
        pl.col("oos_sortino").round(3),
        pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
        pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
        pl.col("dsr").round(3),
        pl.col("pbo").round(3),
        pl.col("switches").cast(pl.Int64),
    ]
    print("=" * 120)
    print("iter_61 recency-weighted meta switch")
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
