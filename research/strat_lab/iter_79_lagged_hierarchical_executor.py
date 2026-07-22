"""iter_79 - lagged hierarchical execution research.

Iter71 found a strong hierarchical Iter67 book, but it was diagnostic because
the book weights are close-of-day state. This pass treats those weights as
signals known only after the close and executes them on the next trading day's
open. That gives a cleaner executable benchmark for cap/mode/turnover choices.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from research.db import connect  # noqa: E402
from research.prices import fetch_adjusted_panel  # noqa: E402
from iter_40_research_campaign import CAPITAL, START, build_price_lookup, simulate, validate_daily  # noqa: E402
from iter_64_active_etf_beater_confirm import compare_active_etfs, load_active_etfs, strict_dsr, window_metrics  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_68_position_level_bridge import Book, BookByDate  # noqa: E402
from iter_70_hierarchical_position_audit import build_hierarchical_books  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_79_lagged_hierarchical_executor"
CAPS = (3, 4, 5, 6, 7, 8, 10)
MODES = ("cash", "renorm")
WEIGHT_THRESHOLDS = (0.00, 0.05, 0.10, 0.20, 0.30, 0.50)
TARGET_LAG_DAYS = 1


def cap_book(book: Book, max_positions: int, mode: str) -> Book:
    if len(book) <= max_positions:
        return dict(book)
    kept = dict(sorted(book.items(), key=lambda kv: (-kv[1], kv[0]))[:max_positions])
    if mode == "cash":
        return kept
    if mode != "renorm":
        raise ValueError(f"unknown cap mode: {mode}")
    old_total = min(sum(book.values()), 1.0)
    kept_total = sum(kept.values())
    if old_total <= 0 or kept_total <= 0:
        return kept
    scale = old_total / kept_total
    return {code: weight * scale for code, weight in kept.items()}


def l1_distance(a: Book, b: Book) -> float:
    codes = set(a) | set(b)
    return sum(abs(a.get(code, 0.0) - b.get(code, 0.0)) for code in codes)


def lagged_capped_targets(
    days: list[date],
    source_books: BookByDate,
    max_positions: int,
    mode: str,
    threshold: float,
    lag_days: int = TARGET_LAG_DAYS,
) -> BookByDate:
    """Convert close-state books into next-open executable targets."""
    if lag_days < 1:
        raise ValueError("lag_days must be >= 1 for executable research")
    out: BookByDate = {}
    last_target: Book | None = None
    last_members: tuple[str, ...] | None = None
    last_year: int | None = None
    last_gross: float | None = None

    for i in range(lag_days, len(days)):
        execution_day = days[i]
        signal_day = days[i - lag_days]
        target = cap_book(source_books.get(signal_day, {}), max_positions, mode)
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
        elif threshold <= 0.0 or l1_distance(target, last_target) >= threshold:
            should_trade = True

        if should_trade:
            out[execution_day] = target
            last_target = target
            last_members = members
            last_year = execution_day.year
            last_gross = gross
    return out


def count_days_over_cap(source_books: BookByDate, max_positions: int) -> int:
    return sum(len(book) > max_positions for book in source_books.values())


def benchmark_2330(days: list[date]) -> tuple[pl.DataFrame, dict[str, object]]:
    con = connect(read_only=True)
    try:
        panel = fetch_adjusted_panel(
            con,
            START.isoformat(),
            days[-1].isoformat(),
            codes=["2330"],
            market="twse",
            include_extra_history_days=320,
        )
    finally:
        con.close()
    daily = (
        panel.filter((pl.col("date") >= days[0]) & (pl.col("date") <= days[-1]))
        .sort("date")
        .select(["date", pl.col("close").alias("px")])
        .with_columns((CAPITAL * pl.col("px") / pl.col("px").first()).alias("nav"))
        .select(["date", "nav"])
    )
    row = {
        **validate_daily("benchmark_2330_total_return", daily, 1, {"max_active": 1.0, "trade_days": 1.0, "avg_turnover_trade_day": 1.0}),
        "cumulative_dsr": strict_dsr(daily, 1),
        **window_metrics(daily, 365),
    }
    daily.write_csv(RESULTS / f"{OUT_PREFIX}_benchmark_2330_daily.csv")
    return daily, row


def run_variant(
    days: list[date],
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    source_books: BookByDate,
    max_positions: int,
    mode: str,
    threshold: float,
    n_trials: int,
    etfs: dict[str, pl.DataFrame],
    benchmark: dict[str, object],
) -> tuple[dict[str, object], pl.DataFrame]:
    targets = lagged_capped_targets(days, source_books, max_positions, mode, threshold)
    daily, stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
    name = f"iter79_lag1_cap{max_positions}_{mode}_thr{int(threshold * 100):02d}"
    daily_path = RESULTS / f"{OUT_PREFIX}_{name}_daily.csv"
    focused = validate_daily(name, daily, n_trials, stats)
    active_summary, active_rows = compare_active_etfs(name, daily, etfs)
    row = {
        "name": name,
        "max_positions": max_positions,
        "cap_mode": mode,
        "weight_change_threshold": threshold,
        "target_lag_days": TARGET_LAG_DAYS,
        "source_days_over_cap": count_days_over_cap(source_books, max_positions),
        "target_rebalance_days": len(targets),
        **focused,
        "cumulative_dsr": strict_dsr(daily, n_trials),
        **window_metrics(daily, 365),
        **active_summary,
        "excess_oos_cagr_vs_2330": float(focused["oos_cagr"]) - float(benchmark["oos_cagr"]),
        "excess_recent_1y_cagr_vs_2330": float(window_metrics(daily, 365)["recent_1y_cagr"]) - float(benchmark["recent_1y_cagr"]),
        "path": str(daily_path),
    }
    row["strict_promotable"] = (
        row["cumulative_dsr"] >= 0.95
        and row["pbo"] < 0.50
        and row["boot_cagr_lb"] > 0.10
        and row["oos_mdd"] > -0.45
        and row["max_active"] <= max_positions
        and row["active_etf_wins"] == row["active_etf_count"]
        and row["excess_oos_cagr_vs_2330"] > 0.0
    )
    if row["strict_promotable"] or row["excess_oos_cagr_vs_2330"] > 0.02:
        daily.write_csv(daily_path)
    else:
        row["path"] = ""
    return row, pl.DataFrame(active_rows)


def main() -> None:
    print("[iter79] building lagged hierarchical execution variants", flush=True)
    days, panel, books_by_name, _state = build_hierarchical_books()
    source_books = books_by_name["iter67_hierarchical"]
    codes = {code for book in source_books.values() for code in book}
    price_lookup = build_price_lookup(panel, codes)
    etfs = load_active_etfs(days[0], days[-1])
    _bench_daily, benchmark = benchmark_2330(days)
    n_trials = ITER67_CUMULATIVE_TRIALS + len(CAPS) * len(MODES) * len(WEIGHT_THRESHOLDS)

    rows = [
        {
            "name": "benchmark_2330_total_return",
            "max_positions": 1,
            "cap_mode": "benchmark",
            "weight_change_threshold": 0.0,
            "target_lag_days": 0,
            "source_days_over_cap": 0,
            "target_rebalance_days": 1,
            **benchmark,
            "active_etf_wins": None,
            "active_etf_count": None,
            "excess_oos_cagr_vs_2330": 0.0,
            "excess_recent_1y_cagr_vs_2330": 0.0,
            "strict_promotable": False,
            "path": str(RESULTS / f"{OUT_PREFIX}_benchmark_2330_daily.csv"),
        }
    ]
    active_frames = []
    for max_positions in CAPS:
        for mode in MODES:
            for threshold in WEIGHT_THRESHOLDS:
                row, active = run_variant(days, price_lookup, source_books, max_positions, mode, threshold, n_trials, etfs, benchmark)
                rows.append(row)
                active_frames.append(active)
                print(
                    f"[iter79] cap={max_positions} mode={mode} thr={threshold:.2f} "
                    f"OOS={row['oos_cagr']:+.2%} excess2330={row['excess_oos_cagr_vs_2330']:+.2%} "
                    f"1Y={row['recent_1y_cagr']:+.2%} MDD={row['oos_mdd']:.2%} "
                    f"DSR={row['cumulative_dsr']:.3f} PBO={row['pbo']:.3f} "
                    f"targets={row['target_rebalance_days']}",
                    flush=True,
                )

    summary = pl.DataFrame(rows).sort(
        ["strict_promotable", "excess_oos_cagr_vs_2330", "oos_sortino", "recent_1y_cagr"],
        descending=[True, True, True, True],
    )
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    active_path = RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv"
    summary.write_csv(summary_path)
    if active_frames:
        pl.concat(active_frames, how="vertical").write_csv(active_path)

    print("=" * 150)
    print("iter_79 lagged hierarchical execution research")
    print("=" * 150)
    print(
        summary.head(20).select(
            [
                "name",
                "strict_promotable",
                "max_positions",
                "cap_mode",
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
