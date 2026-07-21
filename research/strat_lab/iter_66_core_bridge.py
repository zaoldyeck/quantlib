"""iter_66 - stronger production cores with active-ETF bridge.

Iter65 used the active-window Iter63 champion as its only core. That fixed the
active ETF comparison but did not reach cumulative DSR 0.95. This pass tests a
small set of already production-like Iter63 cores with higher OOS Sharpe/DSR,
then applies the same active-ETF bridge logic. The goal is to improve the core's
long-horizon statistical quality, not to add another broad parameter search.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, os.path.dirname(__file__))

from iter_40_research_campaign import CAPITAL, metrics_from_rets, validate_daily  # noqa: E402
from iter_57_cost_aware_switch import confirmed, scheduled_dates  # noqa: E402
from iter_62_sector_leadership_meta import SWITCH_COST, load_sector_features  # noqa: E402
from iter_64_active_etf_beater_confirm import (  # noqa: E402
    ACTIVE_ETFS,
    compare_active_etfs,
    load_active_etfs,
    strict_dsr,
    window_metrics,
)
from iter_65_active_etf_bridge import CUMULATIVE_TRIALS as ITER65_CUMULATIVE_TRIALS  # noqa: E402


RESULTS = Path("research/strat_lab/results")
OUT_PREFIX = "iter_66_core_bridge"
VALIDATE_TOP_N = 48


@dataclass(frozen=True)
class Sleeve:
    key: str
    path: Path
    max_active: float = 6.0


@dataclass(frozen=True)
class Spec:
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
        "core63_active",
        RESULTS
        / (
            "iter63_iter62_iter42_iter57_gate_tech_rs_mom21_abs21_monthly_lb21_m-5_hold40_confirm2"
            "_gate_mkt_mom21_off75_confirm2_hold10_daily.csv"
        ),
    ),
    Sleeve(
        "core63_sharpe",
        RESULTS
        / (
            "iter63_iter62_iter61_iter56_mkt36_gate_0052_mom21_monthly_lb252_m5_hold40_confirm1"
            "_gate_mkt_mom21_off50_confirm2_hold10_daily.csv"
        ),
    ),
    Sleeve(
        "core63_sortino",
        RESULTS
        / (
            "iter63_iter62_iter61_iter56_mkt36_gate_0052_mom21_monthly_lb252_m5_hold40_confirm1"
            "_gate_mkt_mom21_off75_confirm2_hold40_daily.csv"
        ),
    ),
    Sleeve(
        "core63_ma100",
        RESULTS
        / (
            "iter63_iter62_iter61_iter56_mkt36_gate_0052_mom21_monthly_lb252_m5_hold40_confirm1"
            "_gate_mkt_ma100_and_mom21_off75_confirm2_hold40_daily.csv"
        ),
    ),
    Sleeve(
        "core63_dd10_cagr",
        RESULTS
        / (
            "iter63_iter62_iter61_iter56_mkt36_gate_0052_mom21_monthly_lb252_m5_hold40_confirm1"
            "_gate_mkt_dd10_off75_confirm2_hold10_daily.csv"
        ),
    ),
]

ATTACKS = [
    Sleeve(
        "iter64_no_overlay",
        RESULTS / "iter64_iter62_iter42_iter57_gate_tech_rs_mom21_abs21_monthly_lb63_m-5_hold40_confirm2_no_overlay_daily.csv",
    ),
    Sleeve(
        "iter64_dd10_off75",
        RESULTS
        / (
            "iter64_iter62_iter42_iter57_gate_tech_rs_mom21_abs21_monthly_lb63_m-5_hold40_confirm2"
            "_gate_mkt_dd10_off75_confirm2_hold10_daily.csv"
        ),
    ),
]

GATES = ["gate_0052_mom21", "gate_tech_rs_mom21_abs21", "gate_tech_rs_mom63_abs63"]
SCHEDULES = ["weekly", "monthly"]
LOOKBACKS = [21, 63]
MARGINS = [0.0, 0.05]
MIN_HOLDS = [20, 40]
CONFIRMS = [1, 2]
FOCUSED_TRIALS = (
    len(CORES)
    * len(ATTACKS)
    * len(GATES)
    * len(SCHEDULES)
    * len(LOOKBACKS)
    * len(MARGINS)
    * len(MIN_HOLDS)
    * len(CONFIRMS)
)
CUMULATIVE_TRIALS = ITER65_CUMULATIVE_TRIALS + FOCUSED_TRIALS


def load_nav(sleeve: Sleeve, suffix: str) -> pl.DataFrame:
    if not sleeve.path.exists():
        raise FileNotFoundError(sleeve.path)
    return (
        pl.read_csv(sleeve.path, try_parse_dates=True)
        .sort("date")
        .select(["date", pl.col("nav").cast(pl.Float64).alias(f"nav_{suffix}")])
        .with_columns(pl.col(f"nav_{suffix}").pct_change().fill_null(0.0).alias(f"ret_{suffix}"))
    )


def load_base(core: Sleeve, attack: Sleeve) -> pl.DataFrame:
    raw = load_nav(core, "core").join(load_nav(attack, "attack"), on="date", how="inner").sort("date")
    sector = load_sector_features(raw["date"][0].isoformat(), raw["date"][-1].isoformat())
    return (
        raw.join(sector, on="date", how="left")
        .with_columns([pl.col(c).fill_null(False) for c in GATES])
        .sort("date")
    )


def add_momentum(base: pl.DataFrame, lookback: int) -> pl.DataFrame:
    return base.with_columns(
        [
            pl.col("nav_core").pct_change(lookback).shift(1).fill_null(0.0).alias("mom_core"),
            pl.col("nav_attack").pct_change(lookback).shift(1).fill_null(0.0).alias("mom_attack"),
        ]
    )


def simulate(base_raw: pl.DataFrame, spec: Spec) -> pl.DataFrame:
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
            "selected": selected,
            "switched": switched,
        }
    )


def fast_row(daily: pl.DataFrame) -> dict[str, float]:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = frame.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    active = frame.filter(pl.col("date") >= pl.date(2026, 1, 22))
    oos_metrics = metrics_from_rets(oos["ret"].to_numpy(), oos["date"].to_list())
    active_metrics = metrics_from_rets(active["ret"].to_numpy(), active["date"].to_list())
    return {
        **metrics_from_rets(rets, dates),
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_sharpe": oos_metrics["sharpe"],
        "oos_mdd": oos_metrics["mdd"],
        "active_window_total_return": active_metrics["final_nav"] / CAPITAL - 1.0,
    }


def build_specs() -> list[Spec]:
    specs = []
    for core in CORES:
        for attack in ATTACKS:
            for gate in GATES:
                for schedule in SCHEDULES:
                    for lookback in LOOKBACKS:
                        for margin in MARGINS:
                            for hold in MIN_HOLDS:
                                for confirm in CONFIRMS:
                                    name = (
                                        f"iter66_{core.key}_{attack.key}_{gate}_{schedule}"
                                        f"_lb{lookback}_m{int(margin * 100)}_hold{hold}_confirm{confirm}"
                                    )
                                    specs.append(Spec(name, core, attack, gate, schedule, lookback, margin, hold, confirm))
    return specs


def main() -> None:
    specs = build_specs()
    print(
        f"[iter66] specs={len(specs)} focused_trials={FOCUSED_TRIALS} "
        f"cumulative_trials={CUMULATIVE_TRIALS}",
        flush=True,
    )
    bases = {(core.key, attack.key): load_base(core, attack) for core in CORES for attack in ATTACKS}
    start = min(base["date"][0] for base in bases.values())
    end = max(base["date"][-1] for base in bases.values())
    etfs = load_active_etfs(start, end)

    rows = []
    compare_rows = []
    for i, spec in enumerate(specs, 1):
        daily = simulate(bases[(spec.core.key, spec.attack.key)], spec)
        stats = fast_row(daily)
        active_summary, active_rows = compare_active_etfs(spec.name, daily, etfs)
        switches = float(daily["switched"].sum())
        attack_pct = float((daily["selected"] == "attack").sum()) / max(daily.height, 1)
        row = {
            "name": spec.name,
            "core": spec.core.key,
            "attack": spec.attack.key,
            "gate": spec.gate,
            "schedule": spec.schedule,
            "lookback": float(spec.lookback),
            "margin": spec.margin,
            "min_hold_days": float(spec.min_hold_days),
            "confirm_days": float(spec.confirm_days),
            "switches": switches,
            "attack_day_pct": attack_pct,
            "max_active": 6.0,
            **stats,
            **window_metrics(daily, 365),
            **active_summary,
        }
        rows.append(row)
        compare_rows.extend(active_rows)
        if i % 80 == 0:
            print(
                f"[iter66 screen] {i:03d}/{len(specs)} {spec.core.key} wins={row['active_etf_wins']:.0f}/"
                f"{row['active_etf_count']:.0f} OOS={row['oos_cagr']:+.2%} "
                f"Sortino={row['oos_sortino']:.3f} Sharpe={row['oos_sharpe']:.3f} "
                f"1Y={row['recent_1y_cagr']:+.2%}",
                flush=True,
            )

    screen = pl.DataFrame(rows)
    selected_names = set()
    for selected_frame in [
        screen.sort(["active_etf_wins", "oos_sharpe", "oos_sortino"], descending=[True, True, True]).head(VALIDATE_TOP_N),
        screen.sort(["active_etf_wins", "oos_sortino", "oos_cagr"], descending=[True, True, True]).head(VALIDATE_TOP_N),
        screen.sort(["active_etf_wins", "active_etf_min_gap", "oos_sharpe"], descending=[True, True, True]).head(VALIDATE_TOP_N),
        screen.sort(["active_etf_wins", "recent_1y_cagr", "oos_sharpe"], descending=[True, True, True]).head(VALIDATE_TOP_N),
    ]:
        selected_names.update(selected_frame["name"].to_list())
    selected = screen.filter(pl.col("name").is_in(selected_names))
    print(f"[iter66] full-validation candidates={selected.height}", flush=True)

    final_rows = []
    for row0 in selected.iter_rows(named=True):
        spec = next(s for s in specs if s.name == row0["name"])
        daily = simulate(bases[(spec.core.key, spec.attack.key)], spec)
        out_path = RESULTS / f"{spec.name}_daily.csv"
        daily.write_csv(out_path)
        focused = validate_daily(
            spec.name,
            daily,
            FOCUSED_TRIALS,
            {
                "max_active": 6.0,
                "trade_days": float(row0["switches"]),
                "avg_turnover_trade_day": SWITCH_COST if row0["switches"] else 0.0,
            },
        )
        final = {
            **row0,
            **focused,
            "path": str(out_path),
            "focused_dsr": float(focused["dsr"]),
            "cumulative_dsr": strict_dsr(daily, CUMULATIVE_TRIALS),
            "focused_trials": float(FOCUSED_TRIALS),
            "cumulative_trials": float(CUMULATIVE_TRIALS),
        }
        final["beats_all_active_etfs"] = final["active_etf_wins"] == final["active_etf_count"]
        final["focused_promotable"] = (
            final["focused_dsr"] >= 0.95
            and final["pbo"] < 0.50
            and final["boot_cagr_lb"] > 0.10
            and final["oos_mdd"] > -0.45
        )
        final["strict_promotable"] = (
            final["cumulative_dsr"] >= 0.95
            and final["pbo"] < 0.50
            and final["boot_cagr_lb"] > 0.10
            and final["oos_mdd"] > -0.45
        )
        if final["strict_promotable"] and final["beats_all_active_etfs"]:
            final["classification"] = "Production-All-ETF-Beater"
        elif final["focused_promotable"] and final["beats_all_active_etfs"]:
            final["classification"] = "Focused-Pass / Cumulative-Watchlist"
        elif final["beats_all_active_etfs"]:
            final["classification"] = "All-ETF-Beater / Research"
        else:
            final["classification"] = "Rejected"
        final_rows.append(final)

    summary = pl.DataFrame(final_rows).sort(
        ["strict_promotable", "focused_promotable", "beats_all_active_etfs", "active_etf_wins", "cumulative_dsr"],
        descending=[True, True, True, True, True],
    )
    screen_path = RESULTS / f"{OUT_PREFIX}_screen.csv"
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    compare_path = RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv"
    screen.write_csv(screen_path)
    summary.write_csv(summary_path)
    pl.DataFrame(compare_rows).write_csv(compare_path)
    print(f"[iter66] wrote {screen_path}", flush=True)
    print(f"[iter66] wrote {summary_path}", flush=True)
    print(f"[iter66] wrote {compare_path}", flush=True)
    print(summary.head(12).select([
        "classification",
        "name",
        "active_etf_wins",
        "active_etf_min_gap",
        "oos_cagr",
        "oos_sortino",
        "oos_sharpe",
        "oos_mdd",
        "recent_1y_cagr",
        "focused_dsr",
        "cumulative_dsr",
        "pbo",
        "attack_day_pct",
        "switches",
    ]).to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
