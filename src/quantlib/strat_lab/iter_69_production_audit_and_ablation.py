"""iter_69 - production target audit and complexity ablation.

This pass answers two production questions for the current Iter67 champion:

1. Can we reconstruct a daily broker-style target book and prove the live
   holding count stays below the 10-stock mandate?
2. Is the current Core/Attack switch materially better than simpler variants,
   or should production prefer a simpler rule?
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.dirname(__file__))

from iter_40_research_campaign import CAPITAL, build_price_lookup, metrics_from_rets, simulate, validate_daily  # noqa: E402
from iter_54_cross_family_switch import load_switch_base  # noqa: E402
from iter_55_squeeze_switch_refinement import add_refined_gates  # noqa: E402
from iter_57_cost_aware_switch import confirmed, scheduled_dates  # noqa: E402
from iter_61_recency_meta_switch import MetaSpec as Iter61Spec  # noqa: E402
from iter_61_recency_meta_switch import load_base as load_iter61_base  # noqa: E402
from iter_61_recency_meta_switch import simulate_meta as simulate_iter61_meta  # noqa: E402
from iter_62_sector_leadership_meta import MetaSpec as Iter62Spec  # noqa: E402
from iter_62_sector_leadership_meta import Sleeve as Iter62Sleeve  # noqa: E402
from iter_62_sector_leadership_meta import load_base as load_iter62_base  # noqa: E402
from iter_62_sector_leadership_meta import simulate_meta as simulate_iter62_meta  # noqa: E402
from iter_64_active_etf_beater_confirm import compare_active_etfs, load_active_etfs, strict_dsr, window_metrics  # noqa: E402
from iter_66_core_bridge import Sleeve as BridgeSleeve  # noqa: E402
from iter_66_core_bridge import load_base as load_bridge_base  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_67_partial_bridge import CORE as ITER67_CORE  # noqa: E402
from iter_67_partial_bridge import PartialSpec, simulate_partial  # noqa: E402
from iter_68_position_level_bridge import (  # noqa: E402
    Book,
    BookByDate,
    build_iter64_targets,
    build_position_books,
    exposure_by_date,
)


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_69_production_audit"
HARD_CAP_POSITIONS = 5
SWITCH_COST = 0.00357
NAV_RECONCILIATION_TOL = 0.05
WEIGHT_CHANGE_THRESHOLD = 0.30
CORE63_SHARPE_PATH = RESULTS / (
    "iter63_iter62_iter61_iter56_mkt36_gate_0052_mom21_monthly_lb252_m5_hold40_confirm1"
    "_gate_mkt_mom21_off50_confirm2_hold10_daily.csv"
)
ATTACK64_PATH = RESULTS / "iter64_iter62_iter42_iter57_gate_tech_rs_mom21_abs21_monthly_lb63_m-5_hold40_confirm2_no_overlay_daily.csv"
ITER67_NAV_PATH = RESULTS / (
    "iter67_core63_sharpe_iter64_no_overlay_gate_tech_rs_mom21_abs21_monthly_lb42_m-5"
    "_hold60_confirm2_w100_daily.csv"
)
CHAMPION_NAME = "iter69_current_iter67_full_switch"

BookState = tuple[BookByDate, dict[date, str]]


@dataclass(frozen=True)
class AblationSpec:
    name: str
    selector: str
    label: str
    complexity_score: int
    gate: str | None = "gate_tech_rs_mom21_abs21"
    schedule: str = "monthly"
    lookback: int = 42
    margin: float = -0.05
    min_hold_days: int = 60
    confirm_days: int = 2
    use_relative: bool = True
    switch_cost: float = SWITCH_COST


def state_by_date(frame: pl.DataFrame) -> dict[date, str]:
    return dict(zip(frame["date"].to_list(), frame["selected"].to_list(), strict=True))


def scale_books(source: BookByDate, exposure: dict[date, float]) -> BookByDate:
    return {
        d: {code: weight * exposure.get(d, 1.0) for code, weight in book.items() if weight * exposure.get(d, 1.0) > 1e-12}
        for d, book in source.items()
    }


def choose_books(days: list[date], state: dict[date, str], core: BookByDate, attack: BookByDate, attack_label: str = "attack") -> BookByDate:
    return {d: attack.get(d, {}) if state.get(d) == attack_label else core.get(d, {}) for d in days}


def cap_book(book: Book, max_positions: int = HARD_CAP_POSITIONS) -> Book:
    if len(book) <= max_positions:
        return dict(book)
    kept = dict(sorted(book.items(), key=lambda kv: (-kv[1], kv[0]))[:max_positions])
    old_total = min(sum(book.values()), 1.0)
    kept_total = sum(kept.values())
    if kept_total <= 0 or old_total <= 0:
        return kept
    scale = old_total / kept_total
    return {code: weight * scale for code, weight in kept.items()}


def apply_hard_cap(books: BookByDate, max_positions: int = HARD_CAP_POSITIONS) -> BookByDate:
    return {d: cap_book(book, max_positions) for d, book in books.items()}


def l1_distance(a: Book, b: Book) -> float:
    codes = set(a) | set(b)
    return sum(abs(a.get(code, 0.0) - b.get(code, 0.0)) for code in codes)


def compress_target_books(books: BookByDate, threshold: float = WEIGHT_CHANGE_THRESHOLD) -> BookByDate:
    """Keep execution targets only when the actionable broker order changes.

    Daily position books contain drifted mark-to-market weights. Treating every
    row as a fresh rebalance order creates artificial turnover. A broker target
    plan should trade when membership changes, gross exposure changes, the year
    turns, or weights drift enough to justify an order.
    """
    out: BookByDate = {}
    last_target: Book | None = None
    last_members: tuple[str, ...] | None = None
    last_year: int | None = None
    last_gross: float | None = None
    for d in sorted(books):
        target = dict(books[d])
        members = tuple(sorted(target))
        gross = round(sum(target.values()), 6)
        should_trade = False
        if last_target is None:
            should_trade = True
        elif d.year != last_year:
            should_trade = True
        elif members != last_members:
            should_trade = True
        elif last_gross is not None and abs(gross - last_gross) > 1e-4:
            should_trade = True
        elif l1_distance(target, last_target) >= threshold:
            should_trade = True

        if should_trade:
            out[d] = target
            last_target = target
            last_members = members
            last_year = d.year
            last_gross = gross
    return out


def build_iter56_targets(days: list[date], iter44: BookByDate, squeeze: BookByDate) -> BookState:
    base = add_refined_gates(load_switch_base({"iter42_w59_champion", "iter44_w74_q3_trend", "iter52_squeeze_top5"}))
    gate = dict(zip(base["date"].to_list(), base["gate_mkt_mom63_q3_ma50_sq_ma50"].to_list(), strict=True))
    state = {d: "iter52_squeeze_top5" if bool(gate.get(d, False)) else "iter44_w74_q3_trend" for d in days}
    books = {d: squeeze.get(d, {}) if state[d] == "iter52_squeeze_top5" else iter44.get(d, {}) for d in days}
    return books, state


def build_iter61_targets(days: list[date], iter42: BookByDate, iter57: BookByDate) -> BookState:
    spec = Iter61Spec(
        name="iter69_iter61_rebuild",
        gate="gate_mkt_ma50_q3_ma50_sq_ma50",
        schedule="monthly",
        lookback=63,
        margin=0.0,
        min_hold_days=20,
        confirm_days=2,
    )
    state = state_by_date(simulate_iter61_meta(load_iter61_base()[0], spec))
    return choose_books(days, state, iter42, iter57, "candidate"), state


def build_iter62_targets(days: list[date], iter61: BookByDate, iter56: BookByDate) -> BookState:
    core = Iter62Sleeve(
        "iter61",
        "Iter61 recency meta switch",
        RESULTS / "iter61_meta_gate_mkt_ma50_q3_ma50_sq_ma50_monthly_lb63_m0_hold20_confirm2_daily.csv",
        6.0,
    )
    attack = Iter62Sleeve(
        "iter56_mkt36",
        "Iter56 costed mkt63/q3/squeeze 36bp",
        RESULTS / "iter56_iter55_mkt_mom63_q3_ma50_sq_ma50_tax_commission_36bp_daily.csv",
        6.0,
    )
    spec = Iter62Spec(
        name="iter69_iter62_rebuild",
        core=core,
        attack=attack,
        gate="gate_0052_mom21",
        schedule="monthly",
        lookback=252,
        margin=0.05,
        min_hold_days=40,
        confirm_days=1,
    )
    state = state_by_date(simulate_iter62_meta(load_iter62_base(core, attack), spec))
    return choose_books(days, state, iter61, iter56), state


def build_core63_sharpe_targets(days: list[date], iter62: BookByDate) -> BookByDate:
    expo = exposure_by_date(days, "gate_mkt_mom21", 0.50, 2, 10)
    return scale_books(iter62, expo)


def build_iter67_state() -> dict[date, str]:
    base = load_bridge_base(BridgeSleeve("core63_sharpe", CORE63_SHARPE_PATH), BridgeSleeve("iter64_no_overlay", ATTACK64_PATH))
    spec = PartialSpec(
        name=CHAMPION_NAME,
        attack=BridgeSleeve("iter64_no_overlay", ATTACK64_PATH),
        gate="gate_tech_rs_mom21_abs21",
        schedule="monthly",
        lookback=42,
        margin=-0.05,
        min_hold_days=60,
        confirm_days=2,
        attack_weight=1.0,
    )
    return state_by_date(simulate_partial(base, spec))


def write_target_audit(days: list[date], core63: BookByDate, attack64: BookByDate) -> tuple[pl.DataFrame, pl.DataFrame, BookByDate]:
    state = build_iter67_state()
    rows = []
    daily_rows = []
    capped_books: BookByDate = {}
    for d in days:
        selected = "attack64" if state.get(d) == "attack" else "core63"
        raw_book = attack64.get(d, {}) if selected == "attack64" else core63.get(d, {})
        book = cap_book(raw_book)
        capped_books[d] = book
        raw_active = len(raw_book)
        active = len(book)
        total_weight = float(sum(book.values()))
        raw_total_weight = float(sum(raw_book.values()))
        capped_out = max(raw_active - active, 0)
        daily_rows.append(
            {
                "date": d,
                "selected": selected,
                "raw_active_positions": raw_active,
                "active_positions": active,
                "hard_cap_positions": HARD_CAP_POSITIONS,
                "capped_out_positions": capped_out,
                "raw_total_weight": raw_total_weight,
                "total_weight": total_weight,
            }
        )
        if not book:
            rows.append(
                {
                    "date": d,
                    "selected": selected,
                    "company_code": "CASH",
                    "target_weight": 0.0,
                    "raw_active_positions": raw_active,
                    "active_positions": active,
                    "hard_cap_positions": HARD_CAP_POSITIONS,
                    "capped_out_positions": capped_out,
                    "total_weight": total_weight,
                }
            )
            continue
        for code, weight in sorted(book.items()):
            rows.append(
                {
                    "date": d,
                    "selected": selected,
                    "company_code": code,
                    "target_weight": weight,
                    "raw_active_positions": raw_active,
                    "active_positions": active,
                    "hard_cap_positions": HARD_CAP_POSITIONS,
                    "capped_out_positions": capped_out,
                    "total_weight": total_weight,
                }
            )
    targets = pl.DataFrame(rows)
    daily = pl.DataFrame(daily_rows)
    targets.write_csv(RESULTS / f"{OUT_PREFIX}_iter67_targets.csv")
    daily.write_csv(RESULTS / f"{OUT_PREFIX}_iter67_daily_position_counts.csv")

    summary = pl.DataFrame(
        [
            {
                "name": "iter67_full_switch_target_book",
                "start": daily["date"][0].isoformat(),
                "end": daily["date"][-1].isoformat(),
                "days": daily.height,
                "hard_cap_positions": HARD_CAP_POSITIONS,
                "raw_max_active_positions": int(daily["raw_active_positions"].max()),
                "max_active_positions": int(daily["active_positions"].max()),
                "raw_days_over_hard_cap": int((daily["raw_active_positions"] > HARD_CAP_POSITIONS).sum()),
                "days_over_hard_cap_after_cap": int((daily["active_positions"] > HARD_CAP_POSITIONS).sum()),
                "days_over_10": int((daily["active_positions"] > 10).sum()),
                "days_over_6": int((daily["active_positions"] > 6).sum()),
                "max_total_weight": float(daily["total_weight"].max()),
                "min_total_weight": float(daily["total_weight"].min()),
                "attack_days": int((daily["selected"] == "attack64").sum()),
                "core_days": int((daily["selected"] == "core63").sum()),
            }
        ]
    )
    summary.write_csv(RESULTS / f"{OUT_PREFIX}_position_summary.csv")

    if int(summary["days_over_hard_cap_after_cap"][0]) != 0:
        raise AssertionError("Iter67 hard-cap target book violated <=5 holdings")
    if int(summary["days_over_10"][0]) != 0:
        raise AssertionError("Iter67 target book violated <=10 holdings")
    if float(summary["max_total_weight"][0]) > 1.000001:
        raise AssertionError("Iter67 target book exceeded 100% gross long weight")
    return summary, daily, capped_books


def run_hard_cap_backtest(days: list[date], panel: pl.DataFrame, books: BookByDate) -> pl.DataFrame:
    codes = {code for book in books.values() for code in book}
    execution_targets = compress_target_books(books)
    daily, stats = simulate(days, build_price_lookup(panel, codes), execution_targets, {d: 1.0 for d in days}, persist=True)
    daily_path = RESULTS / f"{OUT_PREFIX}_hard_cap5_target_book_daily.csv"
    daily.write_csv(daily_path)
    pl.DataFrame(
        [
            {"date": d, "company_code": code, "target_weight": weight}
            for d, book in execution_targets.items()
            for code, weight in sorted(book.items())
        ]
    ).write_csv(RESULTS / f"{OUT_PREFIX}_hard_cap5_execution_targets.csv")
    n_trials = ITER67_CUMULATIVE_TRIALS + 1
    focused = validate_daily("iter69_hard_cap5_target_book", daily, 1, stats)
    etfs = load_active_etfs(daily["date"][0], daily["date"][-1])
    active_summary, active_rows = compare_active_etfs("iter69_hard_cap5_target_book", daily, etfs)
    pl.DataFrame(active_rows).write_csv(RESULTS / f"{OUT_PREFIX}_hard_cap5_active_etf_comparison.csv")
    source_nav = load_nav(ITER67_NAV_PATH, "source_nav")
    reconciled = (
        source_nav.join(daily.select(["date", pl.col("nav").alias("target_book_nav")]), on="date", how="inner")
        .with_columns((pl.col("target_book_nav") / pl.col("source_nav")).alias("nav_ratio"))
        .select(
            [
                pl.len().alias("reconciliation_rows"),
                pl.col("nav_ratio").min().alias("nav_ratio_min"),
                pl.col("nav_ratio").max().alias("nav_ratio_max"),
                pl.col("nav_ratio").last().alias("nav_ratio_final"),
            ]
        )
        .to_dicts()[0]
    )
    lineage_reconciled = (
        abs(float(reconciled["nav_ratio_final"]) - 1.0) <= NAV_RECONCILIATION_TOL
        and float(reconciled["nav_ratio_min"]) >= 1.0 - NAV_RECONCILIATION_TOL
        and float(reconciled["nav_ratio_max"]) <= 1.0 + NAV_RECONCILIATION_TOL
    )
    row = {
        **focused,
        "cumulative_dsr": strict_dsr(daily, n_trials),
        **window_metrics(daily, 365),
        **active_summary,
        **reconciled,
        "execution_target_days": float(len(execution_targets)),
        "execution_weight_change_threshold": WEIGHT_CHANGE_THRESHOLD,
        "lineage_reconciled": lineage_reconciled,
        "nav_reconciliation_tol": NAV_RECONCILIATION_TOL,
        "path": str(daily_path),
    }
    row["strict_promotable"] = (
        row["lineage_reconciled"]
        and row["cumulative_dsr"] >= 0.95
        and row["pbo"] < 0.50
        and row["boot_cagr_lb"] > 0.10
        and row["oos_mdd"] > -0.45
        and row["max_active"] <= HARD_CAP_POSITIONS
    )
    out = pl.DataFrame([row])
    out.write_csv(RESULTS / f"{OUT_PREFIX}_hard_cap5_backtest_summary.csv")
    return out


def load_nav(path: Path, nav_col: str) -> pl.DataFrame:
    return (
        pl.read_csv(path, try_parse_dates=True)
        .sort("date")
        .select(["date", pl.col("nav").cast(pl.Float64).alias(nav_col)])
        .with_columns(pl.col(nav_col).pct_change().fill_null(0.0).alias(f"ret_{nav_col}"))
    )


def base_for_ablation() -> pl.DataFrame:
    base = load_bridge_base(ITER67_CORE, BridgeSleeve("iter64_no_overlay", ATTACK64_PATH))
    return base.sort("date")


def simulate_ablation(base_raw: pl.DataFrame, spec: AblationSpec) -> pl.DataFrame:
    if spec.selector == "core_only":
        nav = base_raw["nav_core"].to_numpy().astype(float)
        dates = base_raw["date"].to_list()
        return pl.DataFrame({"date": dates, "nav": nav, "selected": ["core"] * len(dates), "switched": [False] * len(dates)})
    if spec.selector == "attack_only":
        nav = base_raw["nav_attack"].to_numpy().astype(float)
        dates = base_raw["date"].to_list()
        return pl.DataFrame({"date": dates, "nav": nav, "selected": ["attack"] * len(dates), "switched": [False] * len(dates)})

    base = base_raw.with_columns(
        [
            pl.col("nav_core").pct_change(spec.lookback).shift(1).fill_null(0.0).alias("mom_core"),
            pl.col("nav_attack").pct_change(spec.lookback).shift(1).fill_null(0.0).alias("mom_attack"),
        ]
    )
    dates = base["date"].to_list()
    ret_core = base["ret_core"].to_numpy().astype(float)
    ret_attack = base["ret_attack"].to_numpy().astype(float)
    rel = (base["mom_attack"] - base["mom_core"]).to_numpy().astype(float)
    gate = np.ones(base.height, dtype=bool) if spec.gate is None else base[spec.gate].to_numpy().astype(bool)
    rel_enter = rel >= spec.margin if spec.use_relative else np.ones(base.height, dtype=bool)
    rel_exit = rel < -spec.margin / 2.0 if spec.use_relative else np.zeros(base.height, dtype=bool)
    enter = confirmed(gate & rel_enter, spec.confirm_days)
    exit_ = confirmed((~gate) | rel_exit, spec.confirm_days)
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
        if did_switch and spec.switch_cost > 0:
            r = (1.0 + r) * (1.0 - spec.switch_cost) - 1.0
        rets.append(r)
        selected.append(state)
        switched.append(did_switch)
    arr = np.asarray(rets, dtype=float)
    return pl.DataFrame({"date": dates, "nav": CAPITAL * np.cumprod(1.0 + arr), "selected": selected, "switched": switched})


def ablation_specs() -> list[AblationSpec]:
    return [
        AblationSpec(CHAMPION_NAME, "switch", "Current Iter67 full-switch", 8),
        AblationSpec("iter69_core63_only", "core_only", "Core63 only: no outer switch", 3),
        AblationSpec("iter69_attack64_only", "attack_only", "Attack64 only: no outer switch", 2),
        AblationSpec(
            "iter69_gate_only_tech21_monthly_hold60",
            "switch",
            "Gate-only: tech leadership, no relative sleeve momentum",
            5,
            use_relative=False,
        ),
        AblationSpec(
            "iter69_relative_only_monthly_lb42_hold60",
            "switch",
            "Relative-momentum only, no tech gate",
            5,
            gate=None,
        ),
        AblationSpec(
            "iter69_current_hold40",
            "switch",
            "Current gate, shorter 40-day hold",
            7,
            min_hold_days=40,
        ),
        AblationSpec(
            "iter69_current_confirm1",
            "switch",
            "Current gate, 1-day confirmation",
            7,
            confirm_days=1,
        ),
        AblationSpec(
            "iter69_current_quarterly",
            "switch",
            "Current gate, quarterly decision schedule",
            7,
            schedule="quarterly",
        ),
        AblationSpec(
            "iter69_current_lb63",
            "switch",
            "Current gate, 63-day relative lookback",
            7,
            lookback=63,
        ),
        AblationSpec(
            "iter69_0052_gate_relative_monthly_lb42",
            "switch",
            "Simpler 0052 absolute momentum gate plus relative momentum",
            6,
            gate="gate_0052_mom21",
        ),
    ]


def fast_metrics(daily: pl.DataFrame) -> dict[str, float]:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = frame.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_metrics = metrics_from_rets(oos["ret"].to_numpy(), oos["date"].to_list())
    return {
        **metrics_from_rets(rets, dates),
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_sharpe": oos_metrics["sharpe"],
        "oos_mdd": oos_metrics["mdd"],
    }


def run_ablation() -> pl.DataFrame:
    specs = ablation_specs()
    cumulative_trials = ITER67_CUMULATIVE_TRIALS + len(specs)
    base = base_for_ablation()
    etfs = load_active_etfs(base["date"][0], base["date"][-1])
    rows = []
    compare_rows = []
    for spec in specs:
        daily = simulate_ablation(base, spec)
        stats = fast_metrics(daily)
        focused = validate_daily(
            spec.name,
            daily.select(["date", "nav"]),
            len(specs),
            {
                "max_active": 6.0,
                "trade_days": float(daily["switched"].sum()) if "switched" in daily.columns else 0.0,
                "avg_turnover_trade_day": spec.switch_cost if float(daily["switched"].sum()) else 0.0,
            },
        )
        active_summary, active_rows = compare_active_etfs(spec.name, daily.select(["date", "nav"]), etfs)
        row = {
            "name": spec.name,
            "label": spec.label,
            "complexity_score": spec.complexity_score,
            "selector": spec.selector,
            "gate": spec.gate or "none",
            "schedule": spec.schedule,
            "lookback": spec.lookback,
            "margin": spec.margin,
            "min_hold_days": spec.min_hold_days,
            "confirm_days": spec.confirm_days,
            "use_relative": spec.use_relative,
            "switches": float(daily["switched"].sum()),
            "attack_day_pct": float((daily["selected"] == "attack").sum()) / max(daily.height, 1),
            **stats,
            **focused,
            "cumulative_dsr": strict_dsr(daily.select(["date", "nav"]), cumulative_trials),
            **window_metrics(daily.select(["date", "nav"]), 365),
            **active_summary,
        }
        row["strict_promotable"] = (
            row["cumulative_dsr"] >= 0.95
            and row["pbo"] < 0.50
            and row["boot_cagr_lb"] > 0.10
            and row["oos_mdd"] > -0.45
            and row["max_active"] <= 10.0
            and row["active_etf_wins"] == row["active_etf_count"]
        )
        out_path = RESULTS / f"{spec.name}_daily.csv"
        if spec.name == CHAMPION_NAME or row["strict_promotable"]:
            daily.select(["date", "nav"]).write_csv(out_path)
            row["path"] = str(out_path)
        else:
            row["path"] = ""
        rows.append(row)
        compare_rows.extend(active_rows)

    summary = pl.DataFrame(rows).sort(
        ["strict_promotable", "complexity_score", "oos_sortino", "oos_cagr"],
        descending=[True, False, True, True],
    )
    summary.write_csv(RESULTS / f"{OUT_PREFIX}_complexity_ablation_summary.csv")
    pl.DataFrame(compare_rows).write_csv(RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv")
    return summary


def main() -> None:
    print("[iter69] building production target audit", flush=True)
    days, panel, iter42, iter57, _q3, iter44, squeeze = build_position_books()
    iter56, _iter56_state = build_iter56_targets(days, iter44, squeeze)
    iter61, _iter61_state = build_iter61_targets(days, iter42, iter57)
    iter62, _iter62_state = build_iter62_targets(days, iter61, iter56)
    core63 = build_core63_sharpe_targets(days, iter62)
    attack64 = build_iter64_targets(days, iter42, iter57)
    position_summary, daily_counts, capped_books = write_target_audit(days, core63, attack64)
    print(position_summary.to_pandas().to_string(index=False), flush=True)
    hard_cap_backtest = run_hard_cap_backtest(days, panel, capped_books)
    print(hard_cap_backtest.select(
        [
            "name",
            pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
            pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
            pl.col("oos_sortino").round(3),
            pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
            pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
            pl.col("cumulative_dsr").round(3),
            pl.col("pbo").round(3),
            "max_active",
            pl.col("nav_ratio_final").round(3),
            "lineage_reconciled",
            "active_etf_wins",
            "active_etf_count",
            "strict_promotable",
        ]
    ).to_pandas().to_string(index=False), flush=True)

    print("[iter69] running complexity ablation", flush=True)
    ablation = run_ablation()
    view = ablation.select(
        [
            "name",
            "label",
            "strict_promotable",
            "complexity_score",
            "active_etf_wins",
            "active_etf_count",
            pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
            pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
            pl.col("oos_sortino").round(3),
            pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
            pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
            pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
            pl.col("cumulative_dsr").round(3),
            pl.col("pbo").round(3),
            "switches",
            pl.col("attack_day_pct").mul(100).round(1).alias("attack_day_pct"),
        ]
    )
    print("=" * 150)
    print("iter_69 production audit and complexity ablation")
    print("=" * 150)
    print(view.to_pandas().to_string(index=False))
    print(f"\nPosition days checked: {daily_counts.height}")
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_position_summary.csv'}")
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_iter67_targets.csv'}")
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_hard_cap5_backtest_summary.csv'}")
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_complexity_ablation_summary.csv'}")


if __name__ == "__main__":
    main()
