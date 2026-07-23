"""iter_95 - global exit-aware realistic strategy search.

This is the production-relevant restart after adding the execution-layer exit
engine.  It does not only patch Iter92.  Stage 1 re-screens a broad set of
Iter86 target-book/allocator candidates through realistic Fubon execution.
Stage 2 lets the strongest target books compete again with professional exit
rules, all under the same fees, sell tax, slippage, volume caps, and limit
blocks.
"""

from __future__ import annotations

import os
import sys
from dataclasses import asdict, replace
from datetime import date
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, str(RESEARCH_ROOT))

from quantlib.constants import CAPITAL  # noqa: E402
from quantlib.db import connect  # noqa: E402
from evaluation import trade_distribution_metrics  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    ExitConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from iter_82_oos_recent_pm_allocator import load_execution_targets  # noqa: E402
from iter_86_oos_recent_maximizer import build_inputs, build_meta_specs  # noqa: E402
from iter_89_execution_champion_search import (  # noqa: E402
    ITER86_FAST,
    build_base_runs,
    source_row_map,
    target_for_strategy,
)
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_95_global_exit_aware_search"
N_TRIALS = 41_116 + 229 + 960


def trailing_calendar_return(daily: pl.DataFrame, months: int) -> float:
    ordered = daily.sort("date")
    end = ordered["date"][-1]
    anchor = end - relativedelta(months=months)
    start = ordered.filter(pl.col("date") <= anchor).tail(1)
    if start.is_empty():
        return float(ordered["nav"][-1] / CAPITAL - 1.0)
    return float(ordered["nav"][-1] / start["nav"][0] - 1.0)


def objective(row: dict[str, object]) -> float:
    oos = max(float(row.get("oos_cagr") or 0.0), 0.0)
    recent = min(max(float(row.get("recent_1y_cagr") or 0.0), 0.0), 3.0)
    mdd = abs(float(row.get("oos_mdd") or 0.0))
    sortino = max(float(row.get("oos_sortino") or 0.0), 0.0)
    dsr = max(float(row.get("dsr") or 0.0), 0.0)
    pbo = max(float(row.get("pbo") or 0.0), 0.0)
    fill = max(float(row.get("fill_ratio") or 0.0), 0.0)
    mdd_factor = 1.0 if mdd <= 0.30 else max(0.55, 0.30 / max(mdd, 1e-9))
    return (
        oos
        * (1.0 + recent)
        * mdd_factor
        * min(1.20, max(0.55, sortino / 2.0))
        * min(1.0, max(0.50, dsr / 0.95))
        * min(1.0, max(0.50, (0.50 - pbo) / 0.50))
        * min(1.0, max(0.60, fill / 0.80))
    )


def select_global_candidate_ids(fast: pl.DataFrame, limit: int = 96) -> list[str]:
    eligible = fast.filter((pl.col("oos_mdd") > -0.45) & (pl.col("oos_cagr") > 0.25))
    picks: list[str] = []

    def add(frame: pl.DataFrame, cols: list[str], descending: list[bool], n: int) -> None:
        for sid in frame.sort(cols, descending=descending).head(n)["strategy_id"].to_list():
            if sid not in picks:
                picks.append(str(sid))

    add(eligible, ["dual_min_ratio", "oos_cagr", "recent_1y_cagr"], [True, True, True], 36)
    add(eligible, ["oos_cagr", "recent_1y_cagr"], [True, True], 30)
    add(eligible, ["recent_1y_cagr", "oos_cagr"], [True, True], 30)
    add(eligible, ["oos_sortino", "oos_cagr"], [True, True], 24)
    add(eligible.filter(pl.col("max_active") <= 6), ["dual_min_ratio", "oos_cagr"], [True, True], 18)
    add(eligible.filter(pl.col("max_active") <= 10), ["dual_min_ratio", "oos_cagr"], [True, True], 18)
    add(
        eligible.filter((pl.col("avg_turnover_trade_day") <= 0.40) & (pl.col("recent_1y_cagr") >= 2.0)),
        ["oos_cagr", "recent_1y_cagr"],
        [True, True],
        18,
    )
    return picks[:limit]


def stage2_exit_configs() -> list[ExitConfig]:
    configs = [ExitConfig(name="no_exit")]
    for stop in (0.10, 0.12, 0.15):
        configs.append(ExitConfig(name=f"sl{int(stop * 100)}", stop_loss_pct=stop))
    for take in (1.00, 1.50):
        configs.append(ExitConfig(name=f"tp{int(take * 100)}", take_profit_pct=take))
    for trail in (0.22, 0.26, 0.30):
        configs.append(ExitConfig(name=f"tr{int(trail * 100)}", trailing_stop_pct=trail))
    for days in (40, 50, 60, 70, 80):
        for min_ret in (-0.03, -0.01, 0.0, 0.02):
            configs.append(
                ExitConfig(
                    name=f"time{days}_r{int(min_ret * 100):+d}",
                    time_stop_days=days,
                    time_stop_min_return_pct=min_ret,
                )
            )
    for days in (50, 60, 70):
        for min_ret in (-0.01, 0.0, 0.02):
            for stop in (0.10, 0.12, 0.15):
                configs.append(
                    ExitConfig(
                        name=f"sl{int(stop * 100)}_time{days}_r{int(min_ret * 100):+d}",
                        stop_loss_pct=stop,
                        time_stop_days=days,
                        time_stop_min_return_pct=min_ret,
                    )
                )
    for days in (50, 60, 70):
        for min_ret in (-0.01, 0.0, 0.02):
            for take in (1.00, 1.50):
                configs.append(
                    ExitConfig(
                        name=f"tp{int(take * 100)}_time{days}_r{int(min_ret * 100):+d}",
                        take_profit_pct=take,
                        time_stop_days=days,
                        time_stop_min_return_pct=min_ret,
                    )
                )
    # Diagnostic configs from the Iter94 MFE/MAE pass.
    configs.extend(
        [
            ExitConfig(name="time20_r-3", time_stop_days=20, time_stop_min_return_pct=-0.03),
            ExitConfig(name="time60_r+0", time_stop_days=60, time_stop_min_return_pct=0.0),
            ExitConfig(name="tr30_time60_r+0", trailing_stop_pct=0.30, time_stop_days=60, time_stop_min_return_pct=0.0),
        ]
    )
    return list({config.name: config for config in configs}.values())


def coarse_exit_configs() -> list[ExitConfig]:
    """Small representative grid for global successive-halving stage."""
    configs = [
        ExitConfig(name="no_exit"),
        ExitConfig(name="sl10", stop_loss_pct=0.10),
        ExitConfig(name="sl12", stop_loss_pct=0.12),
        ExitConfig(name="sl15", stop_loss_pct=0.15),
        ExitConfig(name="tp100", take_profit_pct=1.00),
        ExitConfig(name="tp150", take_profit_pct=1.50),
        ExitConfig(name="tr30", trailing_stop_pct=0.30),
        ExitConfig(name="time50_r-1", time_stop_days=50, time_stop_min_return_pct=-0.01),
        ExitConfig(name="time60_r+0", time_stop_days=60, time_stop_min_return_pct=0.0),
        ExitConfig(name="time70_r-1", time_stop_days=70, time_stop_min_return_pct=-0.01),
        ExitConfig(name="tp100_time50_r-1", take_profit_pct=1.00, time_stop_days=50, time_stop_min_return_pct=-0.01),
        ExitConfig(name="tp150_time50_r-1", take_profit_pct=1.50, time_stop_days=50, time_stop_min_return_pct=-0.01),
        ExitConfig(name="tp100_time70_r-1", take_profit_pct=1.00, time_stop_days=70, time_stop_min_return_pct=-0.01),
        ExitConfig(name="sl15_time70_r-1", stop_loss_pct=0.15, time_stop_days=70, time_stop_min_return_pct=-0.01),
        ExitConfig(name="time20_r-3", time_stop_days=20, time_stop_min_return_pct=-0.03),
        ExitConfig(name="tr30_time60_r+0", trailing_stop_pct=0.30, time_stop_days=60, time_stop_min_return_pct=0.0),
    ]
    return list({config.name: config for config in configs}.values())


def row_for_result(
    strategy_id: str,
    exit_config: ExitConfig,
    result,
    extra: dict[str, object],
) -> dict[str, object]:
    trade_metrics = trade_distribution_metrics(result.trades["net_pnl"].to_list(), prefix="trade_")
    row = validate_daily_nav(
        f"{strategy_id}__{exit_config.name}",
        result.daily.select(["date", "nav"]),
        n_trials=N_TRIALS,
        extra={
            **result.stats,
            **trade_metrics,
            **extra,
            "strategy_id": strategy_id,
            "exit_config": exit_config.name,
            "ret_1y": trailing_calendar_return(result.daily, 12),
            "ret_6m": trailing_calendar_return(result.daily, 6),
            "ret_3m": trailing_calendar_return(result.daily, 3),
            "ret_1m": trailing_calendar_return(result.daily, 1),
            "exit_config_detail": str(asdict(exit_config)),
        },
    )
    row["global_exit_objective"] = objective(row)
    return row


def target_rows(targets: dict[date, dict[str, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {"date": day, "company_code": code, "target_weight": weight}
            for day, book in sorted(targets.items())
            for code, weight in sorted(book.items())
        ]
    )


def load_iter92_targets() -> dict[date, dict[str, float]]:
    return load_execution_targets(RESULTS / "iter_92_execution_meta_switch_target_weights.csv")


def main() -> None:
    fast = pl.read_csv(ITER86_FAST, infer_schema_length=10000)
    candidate_ids = select_global_candidate_ids(fast)
    print(f"[iter95] stage1_candidate_ids={len(candidate_ids)}", flush=True)

    days, price_lookup, _oos_sleeves, _recent_books, _recent_daily, _benchmark, _etfs = build_inputs()
    base_runs = build_base_runs(days, price_lookup)
    base_screens = {sid: source_row_map()[sid] for sid in base_runs if sid in source_row_map()}
    specs_by_id = {
        spec.strategy_id: spec
        for spec in build_meta_specs(
            [base_runs[sid]["sleeve"] for sid in sorted(base_runs)],
            base_screens,
        )
    }

    candidates: dict[str, tuple[dict[date, dict[str, float]], dict[str, object]]] = {}
    for sid in candidate_ids:
        if sid not in base_runs and sid not in specs_by_id:
            continue
        targets, extra = target_for_strategy(sid, days, base_runs, specs_by_id)
        candidates[sid] = (targets, {"source": "iter86_pool", **extra})
    if (RESULTS / "iter_92_execution_meta_switch_target_weights.csv").exists():
        candidates["iter92_unconstrained_meta_switch"] = (
            load_iter92_targets(),
            {"source": "iter92_meta_switch"},
        )

    codes = sorted({code for targets, _extra in candidates.values() for book in targets.values() for code in book})
    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, codes, days[0], days[-1])
        data_cutoff = con.sql("select max(date) from daily_quote").fetchone()[0]
    finally:
        con.close()

    base_config = ExecutionConfig(
        name="fubon_odd_lot_5pct_vol_slip5bp_global_exit",
        capital=CAPITAL,
        lot_size=1,
        max_participation_rate=0.05,
        fixed_slippage_bps=5.0,
        impact_bps_per_1pct_volume=1.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
    )
    simulator = RealisticExecutionSimulator(bars, base_config)

    no_exit = ExitConfig(name="no_exit")
    stage1_rows: list[dict[str, object]] = []
    stage1_results: dict[str, object] = {}
    for i, (sid, (targets, extra)) in enumerate(candidates.items(), 1):
        simulator.config = replace(base_config, exit_config=no_exit)
        result = simulator.simulate(days, targets)
        row = row_for_result(sid, no_exit, result, {"stage": "stage1", **extra})
        stage1_rows.append(row)
        stage1_results[sid] = result
        print(
            f"[iter95 stage1] {i:03d}/{len(candidates)} {sid} "
            f"OOS={float(row['oos_cagr']):+.2%} 1Y={float(row['recent_1y_cagr']):+.2%} "
            f"MDD={float(row['oos_mdd']):.2%} obj={float(row['global_exit_objective']):.3f}",
            flush=True,
        )

    stage1 = pl.DataFrame(stage1_rows).sort("global_exit_objective", descending=True)
    stage1.write_csv(RESULTS / f"{OUT_PREFIX}_stage1_summary.csv")

    selected: list[str] = []
    for cols, descending, n in [
        (["global_exit_objective"], [True], 10),
        (["oos_cagr", "recent_1y_cagr"], [True, True], 8),
        (["recent_1y_cagr", "oos_cagr"], [True, True], 8),
        (["oos_sortino", "oos_cagr"], [True, True], 6),
    ]:
        for sid in stage1.sort(cols, descending=descending).head(n)["strategy_id"].to_list():
            if sid not in selected:
                selected.append(str(sid))
    selected = selected[:16]

    coarse_configs = coarse_exit_configs()
    print(
        f"[iter95] data_cutoff={data_cutoff} coarse_strategies={len(selected)} "
        f"coarse_exit_configs={len(coarse_configs)}",
        flush=True,
    )

    coarse_rows: list[dict[str, object]] = []
    artifacts: list[tuple[float, str, str, object, dict[date, dict[str, float]]]] = []
    for sidx, sid in enumerate(selected, 1):
        targets, extra = candidates[sid]
        for cidx, exit_config in enumerate(coarse_configs, 1):
            simulator.config = replace(base_config, exit_config=exit_config)
            result = simulator.simulate(days, targets)
            row = row_for_result(sid, exit_config, result, {"stage": "coarse", **extra})
            coarse_rows.append(row)
            artifacts.append((float(row["global_exit_objective"]), sid, exit_config.name, result, targets))
            print(
                f"[iter95 coarse] s{sidx:02d}/{len(selected)} c{cidx:02d}/{len(coarse_configs)} "
                f"{sid} {exit_config.name} OOS={float(row['oos_cagr']):+.2%} "
                f"1Y={float(row['recent_1y_cagr']):+.2%} MDD={float(row['oos_mdd']):.2%} "
                f"DSR={float(row['dsr']):.3f} PBO={float(row['pbo']):.3f} "
                f"obj={float(row['global_exit_objective']):.3f}",
                flush=True,
            )

    coarse = pl.DataFrame(coarse_rows).sort("global_exit_objective", descending=True)
    coarse.write_csv(RESULTS / f"{OUT_PREFIX}_coarse_summary.csv")
    focused_sids: list[str] = []
    for sid in coarse.head(24)["strategy_id"].to_list():
        if sid not in focused_sids:
            focused_sids.append(str(sid))
    focused_sids = focused_sids[:6]
    focus_configs = stage2_exit_configs()
    print(
        f"[iter95] focused_strategies={len(focused_sids)} focused_exit_configs={len(focus_configs)}",
        flush=True,
    )

    focus_rows: list[dict[str, object]] = []
    for sidx, sid in enumerate(focused_sids, 1):
        targets, extra = candidates[sid]
        for cidx, exit_config in enumerate(focus_configs, 1):
            simulator.config = replace(base_config, exit_config=exit_config)
            result = simulator.simulate(days, targets)
            row = row_for_result(sid, exit_config, result, {"stage": "focused", **extra})
            focus_rows.append(row)
            artifacts.append((float(row["global_exit_objective"]), sid, exit_config.name, result, targets))
            print(
                f"[iter95 focused] s{sidx:02d}/{len(focused_sids)} c{cidx:02d}/{len(focus_configs)} "
                f"{sid} {exit_config.name} OOS={float(row['oos_cagr']):+.2%} "
                f"1Y={float(row['recent_1y_cagr']):+.2%} MDD={float(row['oos_mdd']):.2%} "
                f"DSR={float(row['dsr']):.3f} PBO={float(row['pbo']):.3f} "
                f"obj={float(row['global_exit_objective']):.3f}",
                flush=True,
            )

    summary = (
        pl.concat([coarse, pl.DataFrame(focus_rows)], how="diagonal")
        .unique(subset=["strategy_id", "exit_config"], keep="last")
        .sort("global_exit_objective", descending=True)
    )
    summary.write_csv(RESULTS / f"{OUT_PREFIX}_summary.csv")
    saved_keys = set(
        f"{row['strategy_id']}__{row['exit_config']}"
        for row in summary.head(10).iter_rows(named=True)
    )
    for _score, sid, exit_name, result, targets in sorted(artifacts, key=lambda item: item[0], reverse=True):
        key = f"{sid}__{exit_name}"
        if key not in saved_keys:
            continue
        safe_key = key.replace("/", "_")
        result.daily.write_csv(RESULTS / f"{OUT_PREFIX}_{safe_key}_daily.csv")
        result.fills.write_csv(RESULTS / f"{OUT_PREFIX}_{safe_key}_fills.csv")
        result.trades.write_csv(RESULTS / f"{OUT_PREFIX}_{safe_key}_trades.csv")
        target_rows(targets).write_csv(RESULTS / f"{OUT_PREFIX}_{safe_key}_target_weights.csv")

    print("=" * 160)
    print("iter95 global exit-aware realistic search")
    print("=" * 160)
    print(
        summary.select(
            [
                "strategy_id",
                "exit_config",
                "source",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("ret_6m").mul(100).round(2).alias("ret_6m_pct"),
                pl.col("ret_3m").mul(100).round(2).alias("ret_3m_pct"),
                pl.col("ret_1m").mul(100).round(2).alias("ret_1m_pct"),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("dsr").round(3),
                pl.col("pbo").round(3),
                pl.col("fill_ratio").mul(100).round(2).alias("fill_ratio_pct"),
                "max_active",
                pl.col("trade_profit_factor").round(3),
                pl.col("trade_sqn").round(3),
                pl.col("exit_orders").round(0),
                pl.col("global_exit_objective").round(3),
            ]
        )
        .head(20)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_stage1_summary.csv'}")
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_summary.csv'}")


if __name__ == "__main__":
    main()
