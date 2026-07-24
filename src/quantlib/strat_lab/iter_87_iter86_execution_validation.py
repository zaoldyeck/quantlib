"""iter_87 - broker-aware execution validation for Iter86 Dual Max."""

from __future__ import annotations

import os
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

import polars as pl
from quantlib import paths

REPO_ROOT = paths.REPO
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, str(RESEARCH_ROOT))

from quantlib.db import connect  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from quantlib.constants import CAPITAL  # noqa: E402
from iter_82_oos_recent_pm_allocator import expand_book_targets  # noqa: E402
from iter_86_oos_recent_maximizer import (  # noqa: E402
    MetaSpec,
    build_inputs,
    combine_meta_targets,
    load_base_sleeves,
    meta_weight_path,
    run_spec,
)
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_87_iter86_execution_validation"
SOURCE_DAILY = RESULTS / (
    "iter_86_oos_recent_maximizer_iter86_b15_b08_weekly_lb5_m0_hold20_c1_rw0_100_d75_daily.csv"
)
N_TRIALS = 39_973


def build_iter86_dual_targets() -> tuple[list[date], dict[date, dict[str, float]]]:
    days, price_lookup, oos_sleeves, recent_books, recent_daily, _benchmark, _etfs = build_inputs()
    base_sleeves = {s.base_id: s for s in load_base_sleeves()}
    left = base_sleeves["b15"]
    right = base_sleeves["b08"]
    left_daily, _left_stats, left_targets, _left_meta = run_spec(
        days, price_lookup, oos_sleeves, recent_books, recent_daily, left.spec
    )
    right_daily, _right_stats, right_targets, _right_meta = run_spec(
        days, price_lookup, oos_sleeves, recent_books, recent_daily, right.spec
    )
    left_books = expand_book_targets(days, left_targets)
    right_books = expand_book_targets(days, right_targets)
    spec = MetaSpec(
        left_id="b15",
        right_id="b08",
        mode="dynamic",
        lookback=5,
        margin=0.0,
        schedule="weekly",
        min_hold_days=20,
        confirm_days=1,
        low_right_weight=0.0,
        high_right_weight=1.0,
        default_right_weight=0.75,
    )
    base = (
        left_daily.select(["date", pl.col("nav").alias("nav_left")])
        .join(right_daily.select(["date", pl.col("nav").alias("nav_right")]), on="date", how="inner")
        .sort("date")
    )
    weights, _switches, _avg_right_weight = meta_weight_path(base, spec)
    return days, combine_meta_targets(
        days,
        left_books,
        right_books,
        weights,
    )


def write_target_books(days: list[date], targets: dict[date, dict[str, float]]) -> tuple[Path, Path]:
    daily_books = expand_book_targets(days, targets)
    target_rows = [
        {"date": d, "company_code": code, "target_weight": weight}
        for d, book in sorted(targets.items())
        for code, weight in sorted(book.items())
    ]
    daily_rows = [
        {"date": d, "company_code": code, "target_weight": weight}
        for d, book in sorted(daily_books.items())
        for code, weight in sorted(book.items())
    ]
    target_path = RESULTS / f"{OUT_PREFIX}_iter86_dual_target_weights.csv"
    daily_path = RESULTS / f"{OUT_PREFIX}_iter86_dual_target_weights_daily.csv"
    pl.DataFrame(target_rows).write_csv(target_path)
    pl.DataFrame(daily_rows).write_csv(daily_path)
    return target_path, daily_path


def source_metrics() -> dict[str, object]:
    daily = pl.read_csv(SOURCE_DAILY, try_parse_dates=True).sort("date")
    stats = {
        "max_active": float(daily["active"].max()),
        "trade_days": float((daily["turnover"] > 1e-8).sum()),
        "avg_turnover_trade_day": float(daily.filter(pl.col("turnover") > 1e-8)["turnover"].mean()),
    }
    row = validate_daily_nav(
        "source_next_open_adjusted",
        daily.select(["date", "nav"]),
        n_trials=N_TRIALS,
        extra=stats,
    )
    return {
        **row,
        "fill_ratio": 1.0,
        "total_commission": float("nan"),
        "total_tax": float("nan"),
        "total_slippage_cost": float("nan"),
        "blocked_orders": 0.0,
        "partial_orders": 0.0,
    }


def summarize_execution(name: str, daily: pl.DataFrame, stats: dict[str, float]) -> dict[str, object]:
    row = validate_daily_nav(
        name,
        daily.select(["date", "nav"]),
        n_trials=N_TRIALS,
        extra=stats,
    )
    return {
        **row,
        **stats,
    }


def recent_window_return(daily: pl.DataFrame, start: date, end: date) -> float:
    sub = daily.filter((pl.col("date") >= start) & (pl.col("date") <= end)).sort("date")
    if sub.height < 2:
        return 0.0
    return float(sub["nav"][-1] / sub["nav"][0] - 1.0)


def run_config(
    config: ExecutionConfig,
    bars: pl.DataFrame,
    days: list[date],
    targets: dict[date, dict[str, float]],
) -> dict[str, object]:
    result = RealisticExecutionSimulator(bars, config).simulate(days, targets)
    daily_path = RESULTS / f"{OUT_PREFIX}_{config.name}_daily.csv"
    fills_path = RESULTS / f"{OUT_PREFIX}_{config.name}_fills.csv"
    result.daily.write_csv(daily_path)
    result.fills.write_csv(fills_path)
    row = summarize_execution(config.name, result.daily, result.stats)
    row["daily_path"] = str(daily_path)
    row["fills_path"] = str(fills_path)
    row["config"] = str(asdict(config))
    return row


def main() -> None:
    days, targets = build_iter86_dual_targets()
    target_path, target_daily_path = write_target_books(days, targets)
    codes = sorted({code for book in targets.values() for code in book})
    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, codes, days[0], days[-1])
    finally:
        con.close()

    configs = [
        ExecutionConfig(
            name="fubon_odd_lot_5pct_vol_slip5bp",
            capital=CAPITAL,
            lot_size=1,
            max_participation_rate=0.05,
            fixed_slippage_bps=5.0,
            impact_bps_per_1pct_volume=1.0,
            fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
        ),
        ExecutionConfig(
            name="fubon_board_lot_5pct_vol_slip5bp",
            capital=CAPITAL,
            lot_size=1000,
            max_participation_rate=0.05,
            fixed_slippage_bps=5.0,
            impact_bps_per_1pct_volume=1.0,
            fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
        ),
        ExecutionConfig(
            name="fubon_odd_lot_10pct_vol_slip10bp",
            capital=CAPITAL,
            lot_size=1,
            max_participation_rate=0.10,
            fixed_slippage_bps=10.0,
            impact_bps_per_1pct_volume=1.5,
            fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
        ),
    ]

    rows = [source_metrics()]
    for config in configs:
        print(f"[iter87] running {config.name}", flush=True)
        rows.append(run_config(config, bars, days, targets))

    summary = pl.DataFrame(rows)
    source = summary.filter(pl.col("name") == "source_next_open_adjusted").row(0, named=True)
    summary = summary.with_columns(
        [
            (pl.col("oos_cagr") - float(source["oos_cagr"])).alias("delta_oos_cagr_vs_source"),
            (pl.col("recent_1y_cagr") - float(source["recent_1y_cagr"])).alias("delta_recent_1y_cagr_vs_source"),
        ]
    )

    # Last available one-month window for operational monitoring.
    cutoff = days[-1]
    start_candidates = [d for d in days if d >= cutoff.replace(day=1)]
    month_start = start_candidates[0] if start_candidates else days[-20]
    month_rows = []
    for row in summary.iter_rows(named=True):
        if row["name"] == "source_next_open_adjusted":
            daily = pl.read_csv(SOURCE_DAILY, try_parse_dates=True)
        else:
            daily = pl.read_csv(row["daily_path"], try_parse_dates=True)
        month_rows.append(
            {
                "name": row["name"],
                "start": month_start.isoformat(),
                "end": cutoff.isoformat(),
                "total_return": recent_window_return(daily, month_start, cutoff),
            }
        )

    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    month_path = RESULTS / f"{OUT_PREFIX}_latest_month.csv"
    summary.sort("oos_cagr", descending=True).write_csv(summary_path)
    pl.DataFrame(month_rows).write_csv(month_path)

    print("=" * 150)
    print("iter_87 Iter86 broker-aware execution validation")
    print("=" * 150)
    print(
        summary.sort("oos_cagr", descending=True)
        .select(
            [
                "name",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("delta_oos_cagr_vs_source").mul(100).round(2).alias("delta_oos_pp"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("delta_recent_1y_cagr_vs_source").mul(100).round(2).alias("delta_1y_pp"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("fill_ratio").mul(100).round(2).alias("fill_ratio_pct"),
                "blocked_orders",
                "partial_orders",
                pl.col("total_commission").round(0),
                pl.col("total_tax").round(0),
                pl.col("total_slippage_cost").round(0),
            ]
        )
        .to_pandas()
        .to_string(index=False)
    )
    print("\nLatest month:")
    print(
        pl.DataFrame(month_rows)
        .with_columns(pl.col("total_return").mul(100).round(2).alias("total_return_pct"))
        .select(["name", "start", "end", "total_return_pct"])
        .to_pandas()
        .to_string(index=False)
    )
    print(f"Saved: {target_path}")
    print(f"Saved: {target_daily_path}")
    print(f"Saved: {summary_path}")
    print(f"Saved: {month_path}")


if __name__ == "__main__":
    main()
