"""iter_80 - top-heavy lagged hierarchical execution research.

Iter79 showed that lagged top-3 renormalization has strong OOS return but weak
recent and active-ETF comparisons, while the uncapped book is more balanced but
less powerful. This pass keeps a <=10 executable book and systematically tilts
capital toward the highest-weighted names, testing whether a top-heavy book can
retain the top-3 edge without discarding the tail completely.
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
from iter_64_active_etf_beater_confirm import compare_active_etfs, load_active_etfs, strict_dsr, window_metrics  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_68_position_level_bridge import Book, BookByDate  # noqa: E402
from iter_70_hierarchical_position_audit import build_hierarchical_books  # noqa: E402
from iter_79_lagged_hierarchical_executor import benchmark_2330, l1_distance  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_80_top_heavy_hierarchical_executor"
TOP_KS = (2, 3, 4)
MAX_POSITIONS = (5, 6, 10)
TOP_WEIGHTS = (0.50, 0.60, 0.70, 0.80, 0.90)
WEIGHT_THRESHOLDS = (0.05, 0.10, 0.20)
TARGET_LAG_DAYS = 1


@dataclass(frozen=True)
class TopHeavySpec:
    top_k: int
    max_positions: int
    top_weight: float
    threshold: float

    @property
    def name(self) -> str:
        return (
            f"iter80_lag1_top{self.top_k}_cap{self.max_positions}"
            f"_w{int(self.top_weight * 100)}_thr{int(self.threshold * 100):02d}"
        )


def proportional_budget(items: list[tuple[str, float]], budget: float) -> Book:
    total = sum(weight for _, weight in items)
    if total <= 0 or budget <= 0:
        return {}
    return {code: budget * weight / total for code, weight in items if weight > 0}


def top_heavy_book(book: Book, spec: TopHeavySpec) -> Book:
    ranked = sorted(book.items(), key=lambda kv: (-kv[1], kv[0]))[: spec.max_positions]
    if not ranked:
        return {}
    gross = min(sum(weight for _, weight in ranked), 1.0)
    top = ranked[: spec.top_k]
    tail = ranked[spec.top_k :]
    if not tail:
        return proportional_budget(top, gross)
    top_budget = gross * spec.top_weight
    tail_budget = gross - top_budget
    out = proportional_budget(top, top_budget)
    out.update(proportional_budget(tail, tail_budget))
    return {code: weight for code, weight in out.items() if weight > 1e-12}


def lagged_targets(days: list[date], source_books: BookByDate, spec: TopHeavySpec) -> BookByDate:
    out: BookByDate = {}
    last_target: Book | None = None
    last_members: tuple[str, ...] | None = None
    last_year: int | None = None
    last_gross: float | None = None
    for i in range(TARGET_LAG_DAYS, len(days)):
        execution_day = days[i]
        signal_day = days[i - TARGET_LAG_DAYS]
        target = top_heavy_book(source_books.get(signal_day, {}), spec)
        members = tuple(sorted(target))
        gross = round(sum(target.values()), 6)
        should_trade = False
        if last_target is None:
            should_trade = True
        elif execution_day.year != last_year:
            should_trade = True
        elif members != last_members:
            should_trade = True
        elif last_gross is not None and abs(gross - last_gross) > 1e-4:
            should_trade = True
        elif l1_distance(target, last_target) >= spec.threshold:
            should_trade = True
        if should_trade:
            out[execution_day] = target
            last_target = target
            last_members = members
            last_year = execution_day.year
            last_gross = gross
    return out


def build_specs() -> list[TopHeavySpec]:
    return [
        TopHeavySpec(top_k, max_positions, top_weight, threshold)
        for top_k in TOP_KS
        for max_positions in MAX_POSITIONS
        if top_k <= max_positions
        for top_weight in TOP_WEIGHTS
        for threshold in WEIGHT_THRESHOLDS
    ]


def run_variant(
    days: list[date],
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    source_books: BookByDate,
    spec: TopHeavySpec,
    n_trials: int,
    etfs: dict[str, pl.DataFrame],
    benchmark: dict[str, object],
) -> tuple[dict[str, object], pl.DataFrame]:
    targets = lagged_targets(days, source_books, spec)
    daily, stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
    focused = validate_daily(spec.name, daily, n_trials, stats)
    recent = window_metrics(daily, 365)
    active_summary, active_rows = compare_active_etfs(spec.name, daily, etfs)
    daily_path = RESULTS / f"{OUT_PREFIX}_{spec.name}_daily.csv"
    row = {
        "name": spec.name,
        "top_k": spec.top_k,
        "max_positions": spec.max_positions,
        "top_weight": spec.top_weight,
        "weight_change_threshold": spec.threshold,
        "target_lag_days": TARGET_LAG_DAYS,
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
        and row["max_active"] <= spec.max_positions
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
    print(f"[iter80] specs={len(specs)} top-heavy lagged hierarchical execution", flush=True)
    days, panel, books_by_name, _state = build_hierarchical_books()
    source_books = books_by_name["iter67_hierarchical"]
    codes = {code for book in source_books.values() for code in book}
    price_lookup = build_price_lookup(panel, codes)
    etfs = load_active_etfs(days[0], days[-1])
    _bench_daily, benchmark = benchmark_2330(days)
    n_trials = ITER67_CUMULATIVE_TRIALS + len(specs)

    rows = []
    active_frames = []
    for i, spec in enumerate(specs, 1):
        row, active = run_variant(days, price_lookup, source_books, spec, n_trials, etfs, benchmark)
        rows.append(row)
        active_frames.append(active)
        print(
            f"[iter80] {i:03d}/{len(specs)} top={spec.top_k} cap={spec.max_positions} "
            f"w={spec.top_weight:.2f} thr={spec.threshold:.2f} "
            f"OOS={row['oos_cagr']:+.2%} excess2330={row['excess_oos_cagr_vs_2330']:+.2%} "
            f"1Y={row['recent_1y_cagr']:+.2%} MDD={row['oos_mdd']:.2%} "
            f"DSR={row['cumulative_dsr']:.3f} wins={row['active_etf_wins']:.0f}/{row['active_etf_count']:.0f}",
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
    print("iter_80 top-heavy lagged hierarchical execution research")
    print("=" * 150)
    print(
        summary.head(25).select(
            [
                "name",
                "strict_promotable",
                "top_k",
                "max_positions",
                "top_weight",
                "weight_change_threshold",
                "target_rebalance_days",
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
