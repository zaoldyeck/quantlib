"""iter_71 - diagnostic holding-count cap sweep for Iter67.

This is a diagnostic, not a production promotion script. It starts from the
Iter70 hierarchical book because the flat Iter69 target book failed NAV
reconciliation. Each cap is simulated under the same order-level approximation:

  - cap to the largest weights by date;
  - rebalance only when membership changes, the year changes, or gross exposure
    changes materially;
  - test both renormalized caps and cash-preserving caps.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))

from iter_40_research_campaign import build_price_lookup, simulate, validate_daily  # noqa: E402
from iter_64_active_etf_beater_confirm import compare_active_etfs, load_active_etfs, strict_dsr, window_metrics  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_68_position_level_bridge import Book, BookByDate  # noqa: E402
from iter_69_production_audit_and_ablation import ITER67_NAV_PATH, NAV_RECONCILIATION_TOL, load_nav  # noqa: E402
from iter_70_hierarchical_position_audit import build_hierarchical_books  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_71_hierarchical_cap_sweep"
CAPS = (3, 4, 5, 6, 7, 10)
MODES = ("renorm", "cash")
WEIGHT_CHANGE_THRESHOLD = 0.10


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


def compress_targets(books: BookByDate, max_positions: int, mode: str) -> BookByDate:
    out: BookByDate = {}
    last_target: Book | None = None
    last_members: tuple[str, ...] | None = None
    last_year: int | None = None
    last_gross: float | None = None
    for d in sorted(books):
        target = cap_book(books[d], max_positions, mode)
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
        elif l1_distance(target, last_target) >= WEIGHT_CHANGE_THRESHOLD:
            should_trade = True

        if should_trade:
            out[d] = target
            last_target = target
            last_members = members
            last_year = d.year
            last_gross = gross
    return out


def reconciliation(daily: pl.DataFrame) -> dict[str, float | bool]:
    source_nav = load_nav(ITER67_NAV_PATH, "source_nav")
    row = (
        source_nav.join(daily.select(["date", pl.col("nav").alias("variant_nav")]), on="date", how="inner")
        .with_columns((pl.col("variant_nav") / pl.col("source_nav")).alias("nav_ratio"))
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
    row["lineage_reconciled"] = (
        abs(float(row["nav_ratio_final"]) - 1.0) <= NAV_RECONCILIATION_TOL
        and float(row["nav_ratio_min"]) >= 1.0 - NAV_RECONCILIATION_TOL
        and float(row["nav_ratio_max"]) <= 1.0 + NAV_RECONCILIATION_TOL
    )
    return row


def count_dropped_days(books: BookByDate, max_positions: int) -> int:
    return sum(len(book) > max_positions for book in books.values())


def run_variant(
    days: list[date],
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    source_books: BookByDate,
    max_positions: int,
    mode: str,
    n_trials: int,
    etfs: dict[str, pl.DataFrame],
) -> tuple[dict[str, object], pl.DataFrame]:
    targets = compress_targets(source_books, max_positions, mode)
    daily, stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
    name = f"iter71_cap{max_positions}_{mode}"
    daily_path = RESULTS / f"{OUT_PREFIX}_cap{max_positions}_{mode}_daily.csv"
    daily.write_csv(daily_path)
    focused = validate_daily(name, daily, n_trials, stats)
    active_summary, active_rows = compare_active_etfs(name, daily, etfs)
    row = {
        "name": name,
        "max_positions": max_positions,
        "cap_mode": mode,
        "diagnostic_only": True,
        "target_rebalance_days": len(targets),
        "source_days_over_cap": count_dropped_days(source_books, max_positions),
        **focused,
        "cumulative_dsr": strict_dsr(daily, n_trials),
        **window_metrics(daily, 365),
        **active_summary,
        **reconciliation(daily),
        "path": str(daily_path),
    }
    row["strict_promotable"] = (
        row["lineage_reconciled"]
        and row["cumulative_dsr"] >= 0.95
        and row["pbo"] < 0.50
        and row["boot_cagr_lb"] > 0.10
        and row["oos_mdd"] > -0.45
        and row["max_active"] <= max_positions
        and row["active_etf_wins"] == row["active_etf_count"]
    )
    return row, pl.DataFrame(active_rows)


def main() -> None:
    print("[iter71] building hierarchical books", flush=True)
    days, panel, books_by_name, _state = build_hierarchical_books()
    source_books = books_by_name["iter67_hierarchical"]
    codes = {code for book in source_books.values() for code in book}
    price_lookup = build_price_lookup(panel, codes)
    etfs = load_active_etfs(days[0], days[-1])
    n_trials = ITER67_CUMULATIVE_TRIALS + len(CAPS) * len(MODES)

    rows = []
    active_rows = []
    for max_positions in CAPS:
        for mode in MODES:
            row, active = run_variant(days, price_lookup, source_books, max_positions, mode, n_trials, etfs)
            rows.append(row)
            active_rows.append(active)
            print(
                f"[iter71] cap={max_positions} mode={mode} "
                f"full={row['cagr']:+.2%} oos={row['oos_cagr']:+.2%} "
                f"recent1y={row['recent_1y_cagr']:+.2%} "
                f"sortino={row['oos_sortino']:.3f} mdd={row['oos_mdd']:.2%} "
                f"dsr={row['cumulative_dsr']:.3f} reconciled={row['lineage_reconciled']}",
                flush=True,
            )

    summary = pl.DataFrame(rows).sort(
        ["lineage_reconciled", "oos_sortino", "oos_cagr", "recent_1y_cagr"],
        descending=[True, True, True, True],
    )
    summary.write_csv(RESULTS / f"{OUT_PREFIX}_summary.csv")
    if active_rows:
        pl.concat(active_rows, how="vertical").write_csv(RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv")

    print("=" * 150)
    print("iter_71 hierarchical cap sweep diagnostic")
    print("=" * 150)
    print(
        summary.select(
            [
                "name",
                "cap_mode",
                "max_positions",
                "source_days_over_cap",
                "target_rebalance_days",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("cumulative_dsr").round(3),
                pl.col("pbo").round(3),
                "active_etf_wins",
                "active_etf_count",
                pl.col("nav_ratio_final").round(3),
                "lineage_reconciled",
                "strict_promotable",
            ]
        ).to_pandas().to_string(index=False)
    )
    print(f"\nSaved: {RESULTS / f'{OUT_PREFIX}_summary.csv'}")
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_active_etf_comparison.csv'}")


if __name__ == "__main__":
    main()
