"""iter_81 - PM allocator over executable hierarchical books.

Iter79 isolated two useful but incomplete executable books:

* cap3 renorm: high OOS CAGR, weaker recent/active-ETF standing.
* cap6 cash: lower OOS CAGR, stronger recent participation.

This pass lets a simple PM allocator switch between those two already lagged
target books using only prior NAV momentum. It remains a single long-only book
with <=6 holdings and next-open execution through the shared simulator.
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

from iter_40_research_campaign import build_price_lookup, simulate, validate_daily  # noqa: E402
from iter_57_cost_aware_switch import confirmed, scheduled_dates  # noqa: E402
from iter_64_active_etf_beater_confirm import compare_active_etfs, load_active_etfs, strict_dsr, window_metrics  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_68_position_level_bridge import Book, BookByDate  # noqa: E402
from iter_70_hierarchical_position_audit import build_hierarchical_books  # noqa: E402
from iter_79_lagged_hierarchical_executor import benchmark_2330, lagged_capped_targets  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_81_hierarchical_pm_allocator"


@dataclass(frozen=True)
class SleeveDef:
    key: str
    max_positions: int
    mode: str
    threshold: float


@dataclass(frozen=True)
class AllocatorSpec:
    lookback: int
    margin: float
    schedule: str
    min_hold_days: int
    confirm_days: int

    @property
    def name(self) -> str:
        return (
            f"iter81_pm_cap3_cap6_{self.schedule}_lb{self.lookback}"
            f"_m{int(self.margin * 100)}_hold{self.min_hold_days}_confirm{self.confirm_days}"
        )


ATTACK = SleeveDef("cap3_renorm_thr20", 3, "renorm", 0.20)
BALANCED = SleeveDef("cap6_cash_thr20", 6, "cash", 0.20)
LOOKBACKS = (21, 42, 63, 126, 252)
MARGINS = (-0.02, 0.0, 0.02, 0.05)
SCHEDULES = ("monthly", "quarterly")
MIN_HOLDS = (20, 40, 60)
CONFIRMS = (1, 2)


def build_specs() -> list[AllocatorSpec]:
    return [
        AllocatorSpec(lookback, margin, schedule, hold, confirm)
        for lookback in LOOKBACKS
        for margin in MARGINS
        for schedule in SCHEDULES
        for hold in MIN_HOLDS
        for confirm in CONFIRMS
    ]


def expand_book_targets(days: list[date], targets: BookByDate) -> dict[date, Book]:
    out: dict[date, Book] = {}
    active: Book = {}
    for d in days:
        if d in targets:
            active = targets[d]
        out[d] = dict(active)
    return out


def build_sleeve(
    days: list[date],
    source_books: BookByDate,
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    sleeve: SleeveDef,
) -> tuple[BookByDate, dict[date, Book], pl.DataFrame, dict[str, float]]:
    targets = lagged_capped_targets(days, source_books, sleeve.max_positions, sleeve.mode, sleeve.threshold)
    daily, stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
    return targets, expand_book_targets(days, targets), daily, stats


def state_path(base: pl.DataFrame, spec: AllocatorSpec) -> tuple[dict[date, str], int]:
    work = base.with_columns(
        [
            pl.col("nav_attack").pct_change(spec.lookback).shift(1).fill_null(0.0).alias("mom_attack"),
            pl.col("nav_balanced").pct_change(spec.lookback).shift(1).fill_null(0.0).alias("mom_balanced"),
        ]
    )
    dates = work["date"].to_list()
    rel = (work["mom_attack"] - work["mom_balanced"]).to_numpy().astype(float)
    attack_enter = confirmed(rel >= spec.margin, spec.confirm_days)
    balanced_enter = confirmed(rel <= -spec.margin, spec.confirm_days)
    sched = scheduled_dates(work, spec.schedule)

    state = "balanced"
    held = 10_000
    switched = 0
    out: dict[date, str] = {}
    for i, d in enumerate(dates):
        new_state = state
        if sched[i] and held >= spec.min_hold_days:
            if state == "balanced" and attack_enter[i]:
                new_state = "attack"
            elif state == "attack" and balanced_enter[i]:
                new_state = "balanced"
        if new_state != state:
            switched += 1
            held = 0
            state = new_state
        else:
            held += 1
        out[d] = state
    return out, switched


def merge_allocator_targets(days: list[date], attack_daily: dict[date, Book], balanced_daily: dict[date, Book], state: dict[date, str]) -> BookByDate:
    out: BookByDate = {}
    last: Book | None = None
    for d in days:
        target = attack_daily[d] if state.get(d) == "attack" else balanced_daily[d]
        if last is None or target != last:
            out[d] = target
            last = target
    return out


def run_variant(
    days: list[date],
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    attack_books: dict[date, Book],
    balanced_books: dict[date, Book],
    base: pl.DataFrame,
    spec: AllocatorSpec,
    n_trials: int,
    etfs: dict[str, pl.DataFrame],
    benchmark: dict[str, object],
) -> tuple[dict[str, object], pl.DataFrame]:
    state, allocator_switches = state_path(base, spec)
    targets = merge_allocator_targets(days, attack_books, balanced_books, state)
    daily, stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
    focused = validate_daily(spec.name, daily, n_trials, stats)
    recent = window_metrics(daily, 365)
    active_summary, active_rows = compare_active_etfs(spec.name, daily, etfs)
    attack_day_pct = sum(1 for d in days if state.get(d) == "attack") / max(len(days), 1)
    daily_path = RESULTS / f"{OUT_PREFIX}_{spec.name}_daily.csv"
    row = {
        "name": spec.name,
        "lookback": spec.lookback,
        "margin": spec.margin,
        "schedule": spec.schedule,
        "min_hold_days": spec.min_hold_days,
        "confirm_days": spec.confirm_days,
        "allocator_switches": allocator_switches,
        "attack_day_pct": attack_day_pct,
        "target_rebalance_days": len(targets),
        **focused,
        "cumulative_dsr": strict_dsr(daily, n_trials),
        **recent,
        **active_summary,
        "excess_oos_cagr_vs_2330": float(focused["oos_cagr"]) - float(benchmark["oos_cagr"]),
        "excess_recent_1y_cagr_vs_2330": float(recent["recent_1y_cagr"]) - float(benchmark["recent_1y_cagr"]),
        "path": str(daily_path),
    }
    row["strict_promotable"] = (
        row["cumulative_dsr"] >= 0.95
        and row["pbo"] < 0.50
        and row["boot_cagr_lb"] > 0.10
        and row["oos_mdd"] > -0.45
        and row["max_active"] <= BALANCED.max_positions
        and row["active_etf_wins"] == row["active_etf_count"]
        and row["excess_oos_cagr_vs_2330"] > 0.0
    )
    if row["strict_promotable"] or row["excess_oos_cagr_vs_2330"] > 0.02:
        daily.write_csv(daily_path)
    else:
        row["path"] = ""
    return row, pl.DataFrame(active_rows)


def main() -> None:
    specs = build_specs()
    print(f"[iter81] specs={len(specs)} PM allocator over {ATTACK.key} and {BALANCED.key}", flush=True)
    days, panel, books_by_name, _state = build_hierarchical_books()
    source_books = books_by_name["iter67_hierarchical"]
    codes = {code for book in source_books.values() for code in book}
    price_lookup = build_price_lookup(panel, codes)

    attack_targets, attack_books, attack_daily, attack_stats = build_sleeve(days, source_books, price_lookup, ATTACK)
    balanced_targets, balanced_books, balanced_daily, balanced_stats = build_sleeve(days, source_books, price_lookup, BALANCED)
    attack_daily.write_csv(RESULTS / f"{OUT_PREFIX}_{ATTACK.key}_daily.csv")
    balanced_daily.write_csv(RESULTS / f"{OUT_PREFIX}_{BALANCED.key}_daily.csv")
    print(
        f"[iter81 sleeve] attack OOS={validate_daily(ATTACK.key, attack_daily, 1, attack_stats)['oos_cagr']:+.2%} "
        f"targets={len(attack_targets)} max_active={attack_stats['max_active']:.0f}",
        flush=True,
    )
    print(
        f"[iter81 sleeve] balanced OOS={validate_daily(BALANCED.key, balanced_daily, 1, balanced_stats)['oos_cagr']:+.2%} "
        f"targets={len(balanced_targets)} max_active={balanced_stats['max_active']:.0f}",
        flush=True,
    )

    base = (
        attack_daily.select(["date", pl.col("nav").alias("nav_attack")])
        .join(balanced_daily.select(["date", pl.col("nav").alias("nav_balanced")]), on="date", how="inner")
        .sort("date")
    )
    etfs = load_active_etfs(days[0], days[-1])
    _bench_daily, benchmark = benchmark_2330(days)
    n_trials = ITER67_CUMULATIVE_TRIALS + len(specs)

    rows = []
    active_frames = []
    for i, spec in enumerate(specs, 1):
        row, active = run_variant(
            days,
            price_lookup,
            attack_books,
            balanced_books,
            base,
            spec,
            n_trials,
            etfs,
            benchmark,
        )
        rows.append(row)
        active_frames.append(active)
        print(
            f"[iter81] {i:03d}/{len(specs)} {spec.schedule} lb={spec.lookback} "
            f"m={spec.margin:+.2f} hold={spec.min_hold_days} c={spec.confirm_days} "
            f"OOS={row['oos_cagr']:+.2%} excess2330={row['excess_oos_cagr_vs_2330']:+.2%} "
            f"1Y={row['recent_1y_cagr']:+.2%} MDD={row['oos_mdd']:.2%} "
            f"DSR={row['cumulative_dsr']:.3f} wins={row['active_etf_wins']:.0f}/{row['active_etf_count']:.0f} "
            f"attackPct={row['attack_day_pct']:.1%}",
            flush=True,
        )

    summary = pl.DataFrame(rows).sort(
        ["strict_promotable", "active_etf_wins", "excess_oos_cagr_vs_2330", "oos_sortino", "recent_1y_cagr"],
        descending=[True, True, True, True, True],
    )
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    active_path = RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv"
    summary.write_csv(summary_path)
    if active_frames:
        pl.concat(active_frames, how="vertical").write_csv(active_path)

    print("=" * 150)
    print("iter_81 hierarchical PM allocator")
    print("=" * 150)
    print(
        summary.head(25).select(
            [
                "name",
                "strict_promotable",
                "schedule",
                "lookback",
                "margin",
                "min_hold_days",
                "confirm_days",
                "allocator_switches",
                pl.col("attack_day_pct").mul(100).round(1).alias("attack_day_pct"),
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("excess_oos_cagr_vs_2330").mul(100).round(2).alias("excess2330_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("cumulative_dsr").round(3),
                pl.col("pbo").round(3),
                "max_active",
                "active_etf_wins",
                "active_etf_count",
            ]
        ).to_pandas().to_string(index=False)
    )
    print(f"\n2330 benchmark OOS={benchmark['oos_cagr']:+.2%} recent1Y={benchmark['recent_1y_cagr']:+.2%}")
    print(f"Saved: {summary_path}")
    print(f"Saved: {active_path}")


if __name__ == "__main__":
    main()
