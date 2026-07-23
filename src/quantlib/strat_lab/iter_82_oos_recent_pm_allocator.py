"""iter_82 - allocator between long-term OOS and recent-strength sleeves.

Iter81 improves long-term executable OOS but still misses the active-ETF recent
ranking. Iter69's executable hard-cap book has the opposite profile: excellent
recent/active-ETF standing but lower long-term OOS. This pass switches between
those two executable target books using only prior NAV momentum.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.dirname(__file__))

from iter_40_research_campaign import build_price_lookup, simulate, validate_daily  # noqa: E402
from iter_57_cost_aware_switch import confirmed, scheduled_dates  # noqa: E402
from iter_64_active_etf_beater_confirm import compare_active_etfs, load_active_etfs, strict_dsr, window_metrics  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_68_position_level_bridge import Book, BookByDate  # noqa: E402
from iter_70_hierarchical_position_audit import build_hierarchical_books  # noqa: E402
from iter_79_lagged_hierarchical_executor import benchmark_2330  # noqa: E402
from iter_81_hierarchical_pm_allocator import (  # noqa: E402
    ATTACK,
    BALANCED,
    AllocatorSpec as InnerAllocatorSpec,
    build_sleeve,
    merge_allocator_targets,
    state_path as inner_state_path,
)


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_82_oos_recent_pm_allocator"
RECENT_TARGETS_PATH = RESULTS / "iter_69_production_audit_hard_cap5_execution_targets.csv"
OOS_SPEC = InnerAllocatorSpec(lookback=42, margin=0.0, schedule="quarterly", min_hold_days=20, confirm_days=2)


@dataclass(frozen=True)
class OuterAllocatorSpec:
    lookback: int
    margin: float
    schedule: str
    min_hold_days: int
    confirm_days: int
    default_state: str

    @property
    def name(self) -> str:
        return (
            f"iter82_oos_recent_{self.default_state}_{self.schedule}_lb{self.lookback}"
            f"_m{int(self.margin * 100)}_hold{self.min_hold_days}_confirm{self.confirm_days}"
        )


LOOKBACKS = (21, 42, 63, 126, 252)
MARGINS = (-0.02, 0.0, 0.02, 0.05)
SCHEDULES = ("monthly", "quarterly")
MIN_HOLDS = (20, 40, 60)
CONFIRMS = (1, 2)
DEFAULT_STATES = ("recent", "oos")


def build_specs() -> list[OuterAllocatorSpec]:
    return [
        OuterAllocatorSpec(lookback, margin, schedule, hold, confirm, default_state)
        for default_state in DEFAULT_STATES
        for lookback in LOOKBACKS
        for margin in MARGINS
        for schedule in SCHEDULES
        for hold in MIN_HOLDS
        for confirm in CONFIRMS
    ]


def load_execution_targets(path: Path) -> BookByDate:
    df = pl.read_csv(path, try_parse_dates=True, schema_overrides={"company_code": pl.Utf8}).sort("date")
    out: BookByDate = {}
    for key, sub in df.group_by("date", maintain_order=True):
        d = key[0] if isinstance(key, tuple) else key
        out[d] = {
            row["company_code"]: float(row["target_weight"])
            for row in sub.iter_rows(named=True)
            if float(row["target_weight"]) > 0
        }
    return out


def expand_book_targets(days: list[date], targets: BookByDate) -> dict[date, Book]:
    active: Book = {}
    out: dict[date, Book] = {}
    for d in days:
        if d in targets:
            active = targets[d]
        out[d] = dict(active)
    return out


def outer_state_path(base: pl.DataFrame, spec: OuterAllocatorSpec) -> tuple[dict[date, str], int]:
    work = base.with_columns(
        [
            pl.col("nav_oos").pct_change(spec.lookback).shift(1).fill_null(0.0).alias("mom_oos"),
            pl.col("nav_recent").pct_change(spec.lookback).shift(1).fill_null(0.0).alias("mom_recent"),
        ]
    )
    dates = work["date"].to_list()
    rel = (work["mom_oos"] - work["mom_recent"]).to_numpy()
    oos_enter = confirmed(rel >= spec.margin, spec.confirm_days)
    recent_enter = confirmed(rel <= -spec.margin, spec.confirm_days)
    sched = scheduled_dates(work, spec.schedule)

    state = spec.default_state
    held = 10_000
    switches = 0
    out: dict[date, str] = {}
    for i, d in enumerate(dates):
        new_state = state
        if sched[i] and held >= spec.min_hold_days:
            if state == "recent" and oos_enter[i]:
                new_state = "oos"
            elif state == "oos" and recent_enter[i]:
                new_state = "recent"
        if new_state != state:
            switches += 1
            held = 0
            state = new_state
        else:
            held += 1
        out[d] = state
    return out, switches


def combine_targets(days: list[date], oos_books: dict[date, Book], recent_books: dict[date, Book], state: dict[date, str]) -> BookByDate:
    out: BookByDate = {}
    last: Book | None = None
    for d in days:
        target = oos_books[d] if state.get(d) == "oos" else recent_books[d]
        if last is None or target != last:
            out[d] = target
            last = target
    return out


def run_variant(
    days: list[date],
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    oos_books: dict[date, Book],
    recent_books: dict[date, Book],
    base: pl.DataFrame,
    spec: OuterAllocatorSpec,
    n_trials: int,
    etfs: dict[str, pl.DataFrame],
    benchmark: dict[str, object],
) -> tuple[dict[str, object], pl.DataFrame]:
    state, switches = outer_state_path(base, spec)
    targets = combine_targets(days, oos_books, recent_books, state)
    daily, stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
    focused = validate_daily(spec.name, daily, n_trials, stats)
    recent = window_metrics(daily, 365)
    active_summary, active_rows = compare_active_etfs(spec.name, daily, etfs)
    oos_day_pct = sum(1 for d in days if state.get(d) == "oos") / max(len(days), 1)
    daily_path = RESULTS / f"{OUT_PREFIX}_{spec.name}_daily.csv"
    row = {
        "name": spec.name,
        "lookback": spec.lookback,
        "margin": spec.margin,
        "schedule": spec.schedule,
        "min_hold_days": spec.min_hold_days,
        "confirm_days": spec.confirm_days,
        "default_state": spec.default_state,
        "allocator_switches": switches,
        "oos_day_pct": oos_day_pct,
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
        and row["max_active"] <= 6.0
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
    print(f"[iter82] specs={len(specs)} allocator over Iter81 OOS sleeve and Iter69 recent sleeve", flush=True)
    days, panel, books_by_name, _iter67_state = build_hierarchical_books()
    source_books = books_by_name["iter67_hierarchical"]
    recent_targets = load_execution_targets(RECENT_TARGETS_PATH)
    codes = {code for book in source_books.values() for code in book} | {code for book in recent_targets.values() for code in book}
    price_lookup = build_price_lookup(panel, codes)

    attack_targets, attack_books, attack_daily, _attack_stats = build_sleeve(days, source_books, price_lookup, ATTACK)
    balanced_targets, balanced_books, balanced_daily, _balanced_stats = build_sleeve(days, source_books, price_lookup, BALANCED)
    inner_base = (
        attack_daily.select(["date", pl.col("nav").alias("nav_attack")])
        .join(balanced_daily.select(["date", pl.col("nav").alias("nav_balanced")]), on="date", how="inner")
        .sort("date")
    )
    inner_state, _inner_switches = inner_state_path(inner_base, OOS_SPEC)
    oos_targets = merge_allocator_targets(days, attack_books, balanced_books, inner_state)
    oos_books = expand_book_targets(days, oos_targets)
    oos_daily, oos_stats = simulate(days, price_lookup, oos_targets, {d: 1.0 for d in days}, persist=True)

    recent_books = expand_book_targets(days, recent_targets)
    recent_daily, recent_stats = simulate(days, price_lookup, recent_targets, {d: 1.0 for d in days}, persist=True)
    oos_daily.write_csv(RESULTS / f"{OUT_PREFIX}_oos_sleeve_daily.csv")
    recent_daily.write_csv(RESULTS / f"{OUT_PREFIX}_recent_sleeve_daily.csv")
    print(
        f"[iter82 sleeve] oos OOS={validate_daily('iter82_oos_sleeve', oos_daily, 1, oos_stats)['oos_cagr']:+.2%} "
        f"recent1Y={window_metrics(oos_daily, 365)['recent_1y_cagr']:+.2%}",
        flush=True,
    )
    print(
        f"[iter82 sleeve] recent OOS={validate_daily('iter82_recent_sleeve', recent_daily, 1, recent_stats)['oos_cagr']:+.2%} "
        f"recent1Y={window_metrics(recent_daily, 365)['recent_1y_cagr']:+.2%}",
        flush=True,
    )

    base = (
        oos_daily.select(["date", pl.col("nav").alias("nav_oos")])
        .join(recent_daily.select(["date", pl.col("nav").alias("nav_recent")]), on="date", how="inner")
        .sort("date")
    )
    etfs = load_active_etfs(days[0], days[-1])
    _bench_daily, benchmark = benchmark_2330(days)
    n_trials = ITER67_CUMULATIVE_TRIALS + len(specs)

    rows = []
    active_frames = []
    for i, spec in enumerate(specs, 1):
        row, active = run_variant(days, price_lookup, oos_books, recent_books, base, spec, n_trials, etfs, benchmark)
        rows.append(row)
        active_frames.append(active)
        print(
            f"[iter82] {i:03d}/{len(specs)} default={spec.default_state} {spec.schedule} lb={spec.lookback} "
            f"m={spec.margin:+.2f} hold={spec.min_hold_days} c={spec.confirm_days} "
            f"OOS={row['oos_cagr']:+.2%} excess2330={row['excess_oos_cagr_vs_2330']:+.2%} "
            f"1Y={row['recent_1y_cagr']:+.2%} MDD={row['oos_mdd']:.2%} "
            f"DSR={row['cumulative_dsr']:.3f} wins={row['active_etf_wins']:.0f}/{row['active_etf_count']:.0f} "
            f"oosPct={row['oos_day_pct']:.1%}",
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
    print("iter_82 OOS/recent PM allocator")
    print("=" * 150)
    print(
        summary.head(25).select(
            [
                "name",
                "strict_promotable",
                "default_state",
                "schedule",
                "lookback",
                "margin",
                "min_hold_days",
                "confirm_days",
                "allocator_switches",
                pl.col("oos_day_pct").mul(100).round(1).alias("oos_day_pct"),
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
