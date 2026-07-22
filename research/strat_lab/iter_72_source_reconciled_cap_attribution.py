"""iter_72 - source-reconciled holding cap attribution.

Iter71 used an order-level approximation and still failed NAV reconciliation.
This pass answers a narrower question with a reconciled baseline:

  If Iter67's hierarchical book holds more than N names, what happens if the
  smallest weights are kept in cash instead?

For cap >= 6 this reproduces the source Iter67 NAV exactly. For lower caps, it
subtracts the omitted names' next-day close-to-close contribution from the
source return. This is not a full order simulator, but it is a conservative
source-reconciled attribution test for whether fewer holdings improve the
current NAV-lineage strategy.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))

from iter_40_research_campaign import CAPITAL, validate_daily  # noqa: E402
from iter_64_active_etf_beater_confirm import compare_active_etfs, load_active_etfs, strict_dsr, window_metrics  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_68_position_level_bridge import Book, BookByDate  # noqa: E402
from iter_69_production_audit_and_ablation import ITER67_NAV_PATH  # noqa: E402
from iter_70_hierarchical_position_audit import build_hierarchical_books  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_72_source_reconciled_cap_attribution"
CAPS = (3, 4, 5, 6)


def removed_book(book: Book, max_positions: int) -> Book:
    if len(book) <= max_positions:
        return {}
    return dict(sorted(book.items(), key=lambda kv: (-kv[1], kv[0]))[max_positions:])


def stock_return_lookup(panel: pl.DataFrame, books: BookByDate) -> dict[tuple[object, str], float]:
    codes = sorted({code for book in books.values() for code in book})
    returns = (
        panel.filter(pl.col("company_code").is_in(codes))
        .sort(["company_code", "date"])
        .select(["date", "company_code", "close"])
        .with_columns((pl.col("close") / pl.col("close").shift(1).over("company_code") - 1.0).fill_null(0.0).alias("ret"))
        .select(["date", "company_code", "ret"])
    )
    return {(row["date"], row["company_code"]): float(row["ret"]) for row in returns.iter_rows(named=True)}


def build_cap_daily(source: pl.DataFrame, books: BookByDate, returns: dict[tuple[object, str], float], max_positions: int) -> tuple[pl.DataFrame, int]:
    source = source.sort("date").with_columns(pl.col("nav").pct_change().fill_null(0.0).alias("source_ret"))
    adjusted_returns: list[float] = []
    dates = source["date"].to_list()
    previous_date = None
    omitted_days = 0
    for row in source.iter_rows(named=True):
        d = row["date"]
        source_ret = float(row["source_ret"])
        if previous_date is None:
            adjusted_returns.append(source_ret)
            previous_date = d
            continue
        removed = removed_book(books.get(previous_date, {}), max_positions)
        if removed:
            omitted_days += 1
        omitted_ret = sum(weight * returns.get((d, code), 0.0) for code, weight in removed.items())
        adjusted_returns.append(source_ret - omitted_ret)
        previous_date = d
    nav = CAPITAL * np.cumprod(1.0 + np.asarray(adjusted_returns, dtype=float))
    return pl.DataFrame({"date": dates, "nav": nav}), omitted_days


def run_cap(
    source: pl.DataFrame,
    books: BookByDate,
    returns: dict[tuple[object, str], float],
    max_positions: int,
    n_trials: int,
    etfs: dict[str, pl.DataFrame],
) -> tuple[dict[str, object], pl.DataFrame]:
    daily, omitted_days = build_cap_daily(source, books, returns, max_positions)
    name = f"iter72_cap{max_positions}_cash_source_reconciled"
    path = RESULTS / f"{OUT_PREFIX}_cap{max_positions}_daily.csv"
    daily.write_csv(path)
    focused = validate_daily(
        name,
        daily,
        n_trials,
        {"max_active": float(max_positions), "trade_days": 0.0, "avg_turnover_trade_day": 0.0},
    )
    active_summary, active_rows = compare_active_etfs(name, daily, etfs)
    row = {
        "name": name,
        "max_positions": max_positions,
        "source_reconciled": max_positions >= 6,
        "omitted_days": omitted_days,
        **focused,
        "cumulative_dsr": strict_dsr(daily, n_trials),
        **window_metrics(daily, 365),
        **active_summary,
        "path": str(path),
    }
    row["strict_promotable"] = (
        row["cumulative_dsr"] >= 0.95
        and row["pbo"] < 0.50
        and row["boot_cagr_lb"] > 0.10
        and row["oos_mdd"] > -0.45
        and row["active_etf_wins"] == row["active_etf_count"]
    )
    return row, pl.DataFrame(active_rows)


def main() -> None:
    print("[iter72] building hierarchical books", flush=True)
    _days, panel, books_by_name, _state = build_hierarchical_books()
    source_books = books_by_name["iter67_hierarchical"]
    source = pl.read_csv(ITER67_NAV_PATH, try_parse_dates=True).sort("date").select(["date", "nav"])
    returns = stock_return_lookup(panel, source_books)
    etfs = load_active_etfs(source["date"][0], source["date"][-1])
    n_trials = ITER67_CUMULATIVE_TRIALS + len(CAPS)

    rows = []
    active_frames = []
    for cap in CAPS:
        row, active = run_cap(source, source_books, returns, cap, n_trials, etfs)
        rows.append(row)
        active_frames.append(active)
        print(
            f"[iter72] cap={cap} full={row['cagr']:+.2%} oos={row['oos_cagr']:+.2%} "
            f"recent1y={row['recent_1y_cagr']:+.2%} sortino={row['oos_sortino']:.3f} "
            f"mdd={row['oos_mdd']:.2%} dsr={row['cumulative_dsr']:.3f} "
            f"etf={row['active_etf_wins']:.0f}/{row['active_etf_count']:.0f}",
            flush=True,
        )

    summary = pl.DataFrame(rows).sort(["oos_sortino", "oos_cagr"], descending=[True, True])
    summary.write_csv(RESULTS / f"{OUT_PREFIX}_summary.csv")
    if active_frames:
        pl.concat(active_frames, how="vertical").write_csv(RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv")

    print("=" * 150)
    print("iter_72 source-reconciled cap attribution")
    print("=" * 150)
    print(
        summary.select(
            [
                "name",
                "max_positions",
                "omitted_days",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("cumulative_dsr").round(3),
                pl.col("pbo").round(3),
                "active_etf_wins",
                "active_etf_count",
                "strict_promotable",
            ]
        ).to_pandas().to_string(index=False)
    )
    print(f"\nSaved: {RESULTS / f'{OUT_PREFIX}_summary.csv'}")
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_active_etf_comparison.csv'}")


if __name__ == "__main__":
    main()
