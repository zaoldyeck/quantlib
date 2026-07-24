"""iter_90 - active-ETF-aware realistic execution search.

Iter89 selected the best target-book strategy after Fubon-like realistic
execution.  This pass keeps the same execution model, then adds a formal
same-window comparison against the active ETF universe so the champion selection
can distinguish "strong strategy" from "strong enough to beat the active ETF
alternatives the user is considering."
"""

from __future__ import annotations

import argparse
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

from active_etf_validator import compare_to_active_etfs, load_active_etf_series  # noqa: E402
from quantlib.constants import CAPITAL  # noqa: E402
from quantlib.db import connect  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
)
from iter_86_oos_recent_maximizer import build_inputs, build_meta_specs  # noqa: E402
from iter_89_execution_champion_search import (  # noqa: E402
    build_base_runs,
    execution_objective,
    load_execution_bars,
    source_row_map,
    target_for_strategy,
)
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_90_active_etf_aware_search"
ITER86_FAST = RESULTS / "iter_86_oos_recent_maximizer_fast_screen.csv"
BASE_TRIALS = 40_160
DEFAULT_LIMIT = 360


def _dedupe_append(picks: list[str], rows: list[str]) -> None:
    for sid in rows:
        if sid not in picks:
            picks.append(str(sid))


def select_candidate_ids(fast: pl.DataFrame, limit: int = DEFAULT_LIMIT) -> list[str]:
    """Select a broad but bounded set of paper candidates for realistic rerank.

    The pool deliberately overweights high recent CAGR because the only active
    ETF losses after Iter89 came from very short live windows.  It still keeps
    OOS, Sortino, drawdown, and turnover slices so the search does not collapse
    into a pure recent-performance chase.
    """
    eligible = fast.filter((pl.col("max_active") <= 10) & (pl.col("oos_mdd") > -0.45))
    picks: list[str] = []

    def add(cols: list[str], descending: list[bool], n: int, frame: pl.DataFrame | None = None) -> None:
        source = eligible if frame is None else frame
        if source.is_empty():
            return
        _dedupe_append(picks, source.sort(cols, descending=descending).head(n)["strategy_id"].to_list())

    add(["recent_1y_cagr", "oos_cagr"], [True, True], 110)
    add(["oos_cagr", "recent_1y_cagr"], [True, True], 90)
    add(["dual_min_ratio", "dual_product"], [True, True], 110)
    add(["oos_sortino", "oos_cagr"], [True, True], 70)
    add(["recent_1y_sortino", "recent_1y_cagr"], [True, True], 70)

    high_recent = eligible.filter((pl.col("recent_1y_cagr") >= 2.30) & (pl.col("oos_cagr") >= 0.30))
    add(["avg_turnover_trade_day", "recent_1y_cagr"], [False, True], 80, high_recent)
    add(["recent_1y_mdd", "recent_1y_cagr"], [True, True], 80, high_recent)

    strong_oos_recent = eligible.filter((pl.col("oos_cagr") >= 0.38) & (pl.col("recent_1y_cagr") >= 2.20))
    add(["oos_mdd", "recent_1y_cagr"], [True, True], 80, strong_oos_recent)
    return picks[:limit]


def active_rank_tuple(row: dict[str, object]) -> tuple[float, float, float, float, float, float]:
    user_limit_ok = 1.0 if float(row.get("max_active") or 0.0) <= 10.0 else 0.0
    all_win = 1.0 if bool(row.get("active_etf_all_win")) else 0.0
    wins = float(row.get("active_etf_wins") or 0.0)
    worst_total = float(row.get("active_etf_worst_total_return_alpha") or 0.0)
    oos = float(row.get("oos_cagr") or 0.0)
    objective = float(row.get("execution_objective") or 0.0)
    return (user_limit_ok, all_win, wins, worst_total, oos, objective)


def active_execution_score(row: dict[str, object]) -> float:
    count = max(float(row.get("active_etf_count") or 0.0), 1.0)
    wins_ratio = float(row.get("active_etf_wins") or 0.0) / count
    worst_total = max(float(row.get("active_etf_worst_total_return_alpha") or 0.0), -0.50)
    base = float(row.get("execution_objective") or 0.0)
    if float(row.get("max_active") or 0.0) > 10.0:
        return -100.0 + base
    return base * (0.35 + 0.65 * wins_ratio) + 2.0 * wins_ratio + worst_total


def load_active_etfs(end: date) -> dict[str, pl.DataFrame]:
    con = connect(read_only=True)
    try:
        return load_active_etf_series(con, end=end.isoformat())
    finally:
        con.close()


def main(limit: int = DEFAULT_LIMIT) -> None:
    fast = pl.read_csv(ITER86_FAST, infer_schema_length=10000)
    candidate_ids = select_candidate_ids(fast, limit=limit)
    print(f"[iter90] candidates={len(candidate_ids)} limit={limit}", flush=True)

    days, price_lookup, _oos_sleeves, _recent_books, _recent_daily, _benchmark, _etfs = build_inputs()
    active_etfs = load_active_etfs(days[-1])
    print(f"[iter90] data_start={days[0]} data_end={days[-1]} active_etfs={len(active_etfs)}", flush=True)

    base_runs = build_base_runs(days, price_lookup)
    source_rows = source_row_map()
    base_screens = {sid: source_rows[sid] for sid in base_runs if sid in source_rows}
    specs_by_id = {
        spec.strategy_id: spec
        for spec in build_meta_specs(
            [base_runs[sid]["sleeve"] for sid in sorted(base_runs)],  # type: ignore[list-item]
            base_screens,
        )
    }

    resolved: list[tuple[str, dict[date, dict[str, float]], dict[str, object]]] = []
    for sid in candidate_ids:
        if sid not in base_runs and sid not in specs_by_id:
            print(f"[iter90] skip missing spec {sid}", flush=True)
            continue
        targets, extra = target_for_strategy(sid, days, base_runs, specs_by_id)
        resolved.append((sid, targets, extra))
    print(f"[iter90] resolved={len(resolved)}", flush=True)

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
    n_trials = BASE_TRIALS + len(resolved)

    rows: list[dict[str, object]] = []
    active_frames: list[pl.DataFrame] = []
    top_artifacts: list[
        tuple[tuple[float, float, float, float, float, float], str, pl.DataFrame, pl.DataFrame, dict[date, dict[str, float]]]
    ] = []
    for i, (sid, targets, extra) in enumerate(resolved, 1):
        print(f"[iter90] run {i:03d}/{len(resolved)} {sid}", flush=True)
        result = simulator.simulate(days, targets)
        source = source_rows.get(sid, {})
        row = validate_daily_nav(
            sid,
            result.daily.select(["date", "nav"]),
            n_trials=n_trials,
            extra={
                **result.stats,
                **extra,
                "source_oos_cagr": source.get("oos_cagr"),
                "source_recent_1y_cagr": source.get("recent_1y_cagr"),
                "source_max_active": source.get("max_active"),
                "source_avg_turnover_trade_day": source.get("avg_turnover_trade_day"),
            },
        )
        active_summary, active_detail = compare_to_active_etfs(sid, result.daily.select(["date", "nav"]), active_etfs)
        row["strategy_id"] = sid
        row.update(active_summary.as_dict())
        row["execution_objective"] = execution_objective(row)
        row["active_execution_score"] = active_execution_score(row)
        row["eligible_for_user_limit"] = float(row.get("max_active") or 0.0) <= 10.0
        row["config"] = str(asdict(config))
        rows.append(row)
        active_frames.append(active_detail)
        top_artifacts.append((active_rank_tuple(row), sid, result.daily, result.fills, targets))
        top_artifacts = sorted(top_artifacts, key=lambda item: item[0], reverse=True)[:20]

    summary = pl.DataFrame(rows).sort(
        [
            "eligible_for_user_limit",
            "active_etf_all_win",
            "active_etf_wins",
            "active_etf_worst_total_return_alpha",
            "execution_objective",
        ],
        descending=[True, True, True, True, True],
    )
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.write_csv(summary_path)

    if active_frames:
        pl.concat(active_frames, how="vertical").write_csv(RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv")

    saved_ids = set(summary.head(12)["strategy_id"].to_list())
    for _rank, sid, daily, fills, targets in top_artifacts:
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
    print("iter_90 active-ETF-aware realistic-execution search")
    print("=" * 150)
    print(
        summary.select(
            [
                "strategy_id",
                "active_etf_wins",
                "active_etf_count",
                "active_etf_loss_list",
                pl.col("active_etf_worst_total_return_alpha").mul(100).round(2).alias("worst_total_alpha_pp"),
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("dsr").round(3),
                pl.col("pbo").round(3),
                "max_active",
                pl.col("fill_ratio").mul(100).round(2).alias("fill_ratio_pct"),
                pl.col("execution_objective").round(4),
                pl.col("active_execution_score").round(4),
            ]
        )
        .head(20)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"Saved: {summary_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    args = parser.parse_args()
    main(limit=args.limit)
