"""iter_89 - search for the best realistic-execution champion.

Iter86 chose the champion by paper target-book NAV. Iter87 corrected execution
simulation, and Iter88 tuned the current champion's target book. This pass asks
the more important question: among the strongest Iter86 candidate pool, which
strategy remains best after realistic Fubon execution costs and fill limits?
"""

from __future__ import annotations

import os
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

import polars as pl
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, str(RESEARCH_ROOT))

from research.constants import CAPITAL  # noqa: E402
from research.db import connect  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from iter_82_oos_recent_pm_allocator import expand_book_targets  # noqa: E402
from iter_86_oos_recent_maximizer import (  # noqa: E402
    BaseSleeve,
    MetaSpec,
    build_inputs,
    build_meta_specs,
    combine_meta_targets,
    load_base_sleeves,
    meta_weight_path,
    run_spec,
)
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_89_execution_champion_search"
ITER86_FAST = RESULTS / "iter_86_oos_recent_maximizer_fast_screen.csv"
ITER87_SUMMARY = RESULTS / "iter_87_iter86_execution_validation_summary.csv"
ITER87_DAILY = RESULTS / "iter_87_iter86_execution_validation_fubon_odd_lot_5pct_vol_slip5bp_daily.csv"
ITER87_TARGETS = RESULTS / "iter_87_iter86_execution_validation_iter86_dual_target_weights_daily.csv"
N_TRIALS = 40_160


def select_candidate_ids(fast: pl.DataFrame, limit: int = 120) -> list[str]:
    eligible = fast.filter((pl.col("max_active") <= 10) & (pl.col("oos_mdd") > -0.45))
    picks: list[str] = []

    def add(frame: pl.DataFrame, cols: list[str], descending: list[bool], n: int) -> None:
        for sid in frame.sort(cols, descending=descending).head(n)["strategy_id"].to_list():
            if sid not in picks:
                picks.append(str(sid))

    add(eligible, ["dual_min_ratio", "oos_cagr", "recent_1y_cagr"], [True, True, True], 40)
    add(eligible, ["oos_cagr", "recent_1y_cagr"], [True, True], 35)
    add(eligible, ["recent_1y_cagr", "oos_cagr"], [True, True], 35)
    add(eligible, ["oos_sortino", "oos_cagr"], [True, True], 25)
    low_turnover = eligible.filter((pl.col("oos_cagr") >= 0.35) & (pl.col("recent_1y_cagr") >= 2.0))
    add(low_turnover, ["avg_turnover_trade_day", "oos_cagr"], [False, True], 25)
    return picks[:limit]


def source_row_map() -> dict[str, dict[str, object]]:
    return {
        str(row["strategy_id"]): row
        for row in pl.read_csv(ITER86_FAST, infer_schema_length=10000).to_dicts()
    }


def build_base_runs(days: list[date], price_lookup: dict[tuple[date, str], float]) -> dict[str, dict[str, object]]:
    _days, _price_lookup, oos_sleeves, recent_books, recent_daily, _benchmark, _etfs = build_inputs()
    if _days != days:
        raise RuntimeError("Iter86 build_inputs days changed during Iter89 run.")
    runs: dict[str, dict[str, object]] = {}
    for sleeve in load_base_sleeves():
        daily, stats, targets, meta = run_spec(days, price_lookup, oos_sleeves, recent_books, recent_daily, sleeve.spec)
        runs[sleeve.base_id] = {
            "sleeve": sleeve,
            "daily": daily,
            "stats": stats,
            "targets": targets,
            "books": expand_book_targets(days, targets),
            "meta": meta,
        }
    return runs


def target_for_strategy(
    strategy_id: str,
    days: list[date],
    base_runs: dict[str, dict[str, object]],
    specs_by_id: dict[str, MetaSpec],
) -> tuple[dict[date, dict[str, float]], dict[str, object]]:
    if strategy_id in base_runs:
        run = base_runs[strategy_id]
        return run["targets"], {"target_rebalance_days": float(len(run["targets"]))}  # type: ignore[arg-type]

    spec = specs_by_id[strategy_id]
    left = base_runs[spec.left_id]
    right = base_runs[spec.right_id]
    left_daily = left["daily"]  # type: ignore[assignment]
    right_daily = right["daily"]  # type: ignore[assignment]
    base = (
        left_daily.select(["date", pl.col("nav").alias("nav_left")])
        .join(right_daily.select(["date", pl.col("nav").alias("nav_right")]), on="date", how="inner")
        .sort("date")
    )
    weights, switches, avg_right_weight = meta_weight_path(base, spec)
    targets = combine_meta_targets(
        days,
        left["books"],  # type: ignore[arg-type]
        right["books"],  # type: ignore[arg-type]
        weights,
    )
    return targets, {
        "target_rebalance_days": float(len(targets)),
        "allocator_switches": float(switches),
        "avg_right_weight": avg_right_weight,
    }


def load_execution_bars(days: list[date], target_sets: list[dict[date, dict[str, float]]]) -> pl.DataFrame:
    codes = sorted({code for targets in target_sets for book in targets.values() for code in book})
    con = connect(read_only=True)
    try:
        return load_adjusted_execution_bars(con, codes, days[0], days[-1])
    finally:
        con.close()


def benchmark_row() -> dict[str, object] | None:
    if not ITER87_SUMMARY.exists():
        return None
    rows = (
        pl.read_csv(ITER87_SUMMARY, try_parse_dates=True)
        .filter(pl.col("name") == "fubon_odd_lot_5pct_vol_slip5bp")
        .to_dicts()
    )
    if not rows:
        return None
    row = rows[0]
    row["strategy_id"] = "iter87_baseline"
    row["name"] = "iter87_baseline"
    row["daily_path"] = str(ITER87_DAILY)
    row["target_weights_path"] = str(ITER87_TARGETS)
    row["execution_objective"] = execution_objective(row)
    return row


def execution_objective(row: dict[str, object]) -> float:
    oos = float(row.get("oos_cagr") or 0.0)
    recent = min(float(row.get("recent_1y_cagr") or 0.0), 3.0)
    mdd = abs(float(row.get("oos_mdd") or 0.0))
    sortino = max(float(row.get("oos_sortino") or 0.0), 0.0)
    fill = float(row.get("fill_ratio") or 0.0)
    mdd_penalty = 1.0 if mdd <= 0.35 else max(0.70, 0.35 / max(mdd, 1e-9))
    fill_penalty = 1.0 if fill >= 0.80 else max(0.70, fill / 0.80)
    sortino_factor = min(1.20, max(0.60, sortino / 2.0))
    return oos * (1.0 + recent) * mdd_penalty * fill_penalty * sortino_factor


def main() -> None:
    fast = pl.read_csv(ITER86_FAST, infer_schema_length=10000)
    candidate_ids = select_candidate_ids(fast)
    print(f"[iter89] candidates={len(candidate_ids)}", flush=True)

    days, price_lookup, _oos_sleeves, _recent_books, _recent_daily, _benchmark, _etfs = build_inputs()
    base_runs = build_base_runs(days, price_lookup)
    base_screens = {sid: source_row_map()[sid] for sid in base_runs if sid in source_row_map()}
    specs_by_id = {spec.strategy_id: spec for spec in build_meta_specs(
        [base_runs[sid]["sleeve"] for sid in sorted(base_runs)],  # type: ignore[list-item]
        base_screens,
    )}

    resolved: list[tuple[str, dict[date, dict[str, float]], dict[str, object]]] = []
    for sid in candidate_ids:
        if sid not in base_runs and sid not in specs_by_id:
            print(f"[iter89] skip missing spec {sid}", flush=True)
            continue
        targets, extra = target_for_strategy(sid, days, base_runs, specs_by_id)
        resolved.append((sid, targets, extra))

    bars = load_execution_bars(days, [targets for _sid, targets, _extra in resolved])
    config = ExecutionConfig(
        name="fubon_odd_lot_5pct_vol_slip5bp",
        capital=CAPITAL,
        lot_size=1,
        max_participation_rate=0.05,
        fixed_slippage_bps=5.0,
        impact_bps_per_1pct_volume=1.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
    )
    simulator = RealisticExecutionSimulator(bars, config)
    source_rows = source_row_map()

    rows: list[dict[str, object]] = []
    top_artifacts: list[tuple[float, str, pl.DataFrame, pl.DataFrame, dict[date, dict[str, float]]]] = []
    for i, (sid, targets, extra) in enumerate(resolved, 1):
        print(f"[iter89] run {i:03d}/{len(resolved)} {sid}", flush=True)
        result = simulator.simulate(days, targets)
        source = source_rows.get(sid, {})
        row = validate_daily_nav(
            sid,
            result.daily.select(["date", "nav"]),
            n_trials=N_TRIALS,
            extra={
                **result.stats,
                **extra,
                "source_oos_cagr": source.get("oos_cagr"),
                "source_recent_1y_cagr": source.get("recent_1y_cagr"),
                "source_max_active": source.get("max_active"),
                "source_avg_turnover_trade_day": source.get("avg_turnover_trade_day"),
            },
        )
        row["strategy_id"] = sid
        row["execution_objective"] = execution_objective(row)
        row["config"] = str(asdict(config))
        rows.append(row)
        top_artifacts.append((float(row["execution_objective"]), sid, result.daily, result.fills, targets))
        top_artifacts = sorted(top_artifacts, key=lambda item: item[0], reverse=True)[:12]

    baseline = benchmark_row()
    if baseline is not None:
        rows.append(baseline)

    summary = pl.DataFrame(rows).sort("execution_objective", descending=True)
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.write_csv(summary_path)

    saved_ids = set(summary.head(8)["strategy_id"].to_list())
    for _score, sid, daily, fills, targets in top_artifacts:
        if sid not in saved_ids:
            continue
        daily_path = RESULTS / f"{OUT_PREFIX}_{sid}_daily.csv"
        fills_path = RESULTS / f"{OUT_PREFIX}_{sid}_fills.csv"
        target_path = RESULTS / f"{OUT_PREFIX}_{sid}_target_weights.csv"
        daily.write_csv(daily_path)
        fills.write_csv(fills_path)
        target_rows = [
            {"date": d, "company_code": code, "target_weight": weight}
            for d, book in sorted(targets.items())
            for code, weight in sorted(book.items())
        ]
        pl.DataFrame(target_rows).write_csv(target_path)

    print("=" * 150)
    print("iter_89 realistic-execution champion search")
    print("=" * 150)
    print(
        summary.select(
            [
                "strategy_id",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("fill_ratio").mul(100).round(2).alias("fill_ratio_pct"),
                "blocked_orders",
                "partial_orders",
                pl.col("source_oos_cagr").mul(100).round(2).alias("source_oos_pct"),
                pl.col("source_recent_1y_cagr").mul(100).round(2).alias("source_1y_pct"),
                pl.col("execution_objective").round(4),
            ]
        )
        .head(15)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"Saved: {summary_path}")


if __name__ == "__main__":
    main()
