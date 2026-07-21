"""iter_97 - regime risk overlay follow-up for robust alpha.

Iter96 showed that first-principles leader selection can capture recent upside
but fails production-grade robustness because drawdowns are too deep and the
strategy never permanently beats 2330.  This pass keeps the same stock-selection
families and specifically tests whether a market-regime gross overlay fixes the
failure mode after realistic execution.
"""

from __future__ import annotations

import sys
import time
from dataclasses import asdict
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from constants import CAPITAL  # noqa: E402
from db import connect  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    ExitConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from iter_96_robust_alpha_research import (  # noqa: E402
    N_TRIALS_PRIOR,
    LeaderConfig,
    build_price_lookup,
    build_targets,
    latest_0050_day,
    load_benchmark_nav,
    load_research_panel,
    paper_simulate,
    quick_nav_row,
    relative_metrics,
    robust_alpha_objective,
    target_rows,
)
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path("research/strat_lab/results")
OUT_PREFIX = "iter_97_regime_risk_overlay_research"


def log(message: str) -> None:
    print(message, flush=True)


def build_configs() -> list[LeaderConfig]:
    configs: list[LeaderConfig] = []
    for score_kind in ("structural_leader", "smooth_compounder", "balanced_alpha", "acceleration_leader"):
        for schedule in ("weekly", "monthly"):
            for topn in (3, 5, 10, 20):
                for risk_mode in ("half", "cash"):
                    quality_floor = "quality" if score_kind in {"structural_leader", "smooth_compounder"} else "loose"
                    weight_modes = ("equal", "rank_tilt") if topn in {5, 10} else ("equal",)
                    for weight_mode in weight_modes:
                        configs.append(
                            LeaderConfig(
                                name=(
                                    f"iter97_{score_kind}_{schedule}_top{topn}"
                                    f"_risk{risk_mode}_{quality_floor}_{weight_mode}"
                                ),
                                score_kind=score_kind,
                                schedule=schedule,
                                topn=topn,
                                min_adv=50_000_000.0,
                                trend_mode="ma200",
                                risk_mode=risk_mode,
                                quality_floor=quality_floor,
                                weight_mode=weight_mode,
                                max_weight=0.35 if weight_mode != "equal" else 1.0,
                            )
                        )
    return configs


def main() -> None:
    t0 = time.time()
    RESULTS.mkdir(parents=True, exist_ok=True)
    end = latest_0050_day()
    panel, days, market_ma200, market_close = load_research_panel(end)
    configs = build_configs()
    n_trials = N_TRIALS_PRIOR + 168 + len(configs)
    log(f"[iter97] configs={len(configs)} n_trials={n_trials}")

    target_sets = {}
    build_rows = []
    all_codes: set[str] = set()
    for i, cfg in enumerate(configs, 1):
        targets = build_targets(panel, days, market_ma200, market_close, cfg)
        codes = {code for book in targets.values() for code in book}
        all_codes |= codes
        target_sets[cfg.name] = targets
        build_rows.append({**asdict(cfg), "target_rebalance_days": len(targets), "candidate_codes": len(codes)})
        if i % 24 == 0 or i == len(configs):
            log(f"[iter97] target build {i:03d}/{len(configs)} codes={len(all_codes):,}")

    benchmarks = {
        "b0050": load_benchmark_nav("0050", days[0], days[-1], "0050 total return"),
        "b2330": load_benchmark_nav("2330", days[0], days[-1], "2330 total return"),
    }
    con = connect(read_only=True)
    try:
        data_cutoff = con.sql("SELECT MAX(date) FROM daily_quote").fetchone()[0]
    finally:
        con.close()

    build_by_name = {str(row["name"]): row for row in build_rows}
    price_lookup = build_price_lookup(panel, all_codes)
    paper_rows = []
    for i, cfg in enumerate([cfg for cfg in configs if target_sets[cfg.name]], 1):
        daily, stats = paper_simulate(days, price_lookup, target_sets[cfg.name])
        row = quick_nav_row(
            f"{cfg.name}__paper",
            daily.select(["date", "nav"]),
            extra={
                **stats,
                **build_by_name[cfg.name],
                "exit_config": "paper",
                "search_stage": "paper_screen",
                "fill_ratio": 1.0,
                "data_cutoff": data_cutoff,
            },
        )
        for prefix, bench in benchmarks.items():
            row.update(relative_metrics(daily.select(["date", "nav"]), bench, prefix))
        row["robust_alpha_objective"] = robust_alpha_objective(row)
        paper_rows.append(row)
        if i % 24 == 0 or i == len(configs):
            log(f"[iter97 paper] {i:03d}/{len(configs)} best_obj={max(float(r['robust_alpha_objective']) for r in paper_rows):.4f}")

    paper = pl.DataFrame(paper_rows).sort("robust_alpha_objective", descending=True)
    paper_path = RESULTS / f"{OUT_PREFIX}_paper_screen.csv"
    paper.write_csv(paper_path)

    selected: list[str] = []
    for cols, descending, n in [
        (["robust_alpha_objective"], [True], 14),
        (["oos_cagr", "oos_mdd"], [True, True], 8),
        (["b2330_rolling_3y_win_rate", "b2330_final_relative_nav"], [True, True], 8),
        (["b0050_rolling_3y_win_rate", "b0050_final_relative_nav"], [True, True], 6),
    ]:
        for name in paper.sort(cols, descending=descending).head(n)["name"].to_list():
            base_name = str(name).removesuffix("__paper")
            if base_name not in selected:
                selected.append(base_name)
    selected = selected[:18]
    selected_codes = {code for cfg_name in selected for book in target_sets[cfg_name].values() for code in book}
    log(f"[iter97] realistic_candidates={len(selected)} selected_codes={len(selected_codes):,}")

    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, sorted(selected_codes), days[0], days[-1])
    finally:
        con.close()
    log(f"[iter97] execution bars rows={bars.height:,} codes={bars['company_code'].n_unique():,} cutoff={data_cutoff}")

    exit_configs = [
        ExitConfig(name="no_exit"),
        ExitConfig(name="time50_r-1", time_stop_days=50, time_stop_min_return_pct=-0.01),
        ExitConfig(name="time70_r-1", time_stop_days=70, time_stop_min_return_pct=-0.01),
        ExitConfig(name="tr30", trailing_stop_pct=0.30),
    ]
    simulator = RealisticExecutionSimulator(
        bars,
        ExecutionConfig(
            name="fubon_odd_lot_5pct_vol_slip5bp_iter97",
            capital=CAPITAL,
            lot_size=1,
            max_participation_rate=0.05,
            fixed_slippage_bps=5.0,
            impact_bps_per_1pct_volume=1.0,
            fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
        ),
    )
    cfg_by_name = {cfg.name: cfg for cfg in configs}
    rows = []
    artifacts = []
    total = len(selected) * len(exit_configs)
    idx = 0
    for cfg_name in selected:
        cfg = cfg_by_name[cfg_name]
        for exit_config in exit_configs:
            idx += 1
            simulator.config = ExecutionConfig(
                name="fubon_odd_lot_5pct_vol_slip5bp_iter97",
                capital=CAPITAL,
                lot_size=1,
                max_participation_rate=0.05,
                fixed_slippage_bps=5.0,
                impact_bps_per_1pct_volume=1.0,
                fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
                exit_config=exit_config,
            )
            result = simulator.simulate(days, target_sets[cfg.name])
            row = validate_daily_nav(
                f"{cfg.name}__{exit_config.name}",
                result.daily.select(["date", "nav"]),
                n_trials=n_trials,
                extra={
                    **result.stats,
                    **build_by_name[cfg.name],
                    "exit_config": exit_config.name,
                    "search_stage": "realistic_final",
                    "data_cutoff": data_cutoff,
                },
            )
            for prefix, bench in benchmarks.items():
                row.update(relative_metrics(result.daily.select(["date", "nav"]), bench, prefix))
            row["robust_alpha_objective"] = robust_alpha_objective(row)
            rows.append(row)
            artifacts.append((float(row["robust_alpha_objective"]), cfg.name, exit_config, result))
            artifacts = sorted(artifacts, key=lambda item: item[0], reverse=True)[:10]
            if idx % 12 == 0 or idx == total:
                log(f"[iter97 realistic] {idx:03d}/{total} best_obj={max(float(r['robust_alpha_objective']) for r in rows):.4f}")

    summary = pl.DataFrame(rows).sort("robust_alpha_objective", descending=True)
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.write_csv(summary_path)
    pl.DataFrame(build_rows).write_csv(RESULTS / f"{OUT_PREFIX}_target_build_summary.csv")

    keep = {f"{row['name']}__{row['exit_config']}" for row in summary.head(8).iter_rows(named=True)}
    for _score, cfg_name, exit_config, result in sorted(artifacts, key=lambda item: item[0], reverse=True):
        key = f"{cfg_name}__{exit_config.name}"
        if key not in keep:
            continue
        safe = key.replace("/", "_")
        result.daily.write_csv(RESULTS / f"{OUT_PREFIX}_{safe}_daily.csv")
        result.fills.write_csv(RESULTS / f"{OUT_PREFIX}_{safe}_fills.csv")
        result.trades.write_csv(RESULTS / f"{OUT_PREFIX}_{safe}_trades.csv")
        target_rows(target_sets[cfg_name]).write_csv(RESULTS / f"{OUT_PREFIX}_{safe}_target_weights.csv")

    view = summary.select(
        [
            "name",
            "score_kind",
            "schedule",
            "topn",
            "risk_mode",
            "weight_mode",
            "exit_config",
            pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
            pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
            pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
            pl.col("oos_sortino").round(3),
            pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
            pl.col("dsr").round(3),
            pl.col("pbo").round(3),
            pl.col("fill_ratio").mul(100).round(2).alias("fill_ratio_pct"),
            pl.col("b0050_final_relative_nav").round(3),
            pl.col("b0050_rolling_3y_win_rate").mul(100).round(1).alias("b0050_roll3y_win_pct"),
            pl.col("b2330_final_relative_nav").round(3),
            pl.col("b2330_rolling_3y_win_rate").mul(100).round(1).alias("b2330_roll3y_win_pct"),
            pl.col("b2330_start_5y_win_rate").mul(100).round(1).alias("b2330_start5y_win_pct"),
            pl.col("b2330_longest_below_start_days"),
            pl.col("robust_alpha_objective").round(4),
        ]
    ).head(20)
    print("=" * 180)
    print("iter_97 regime risk-overlay research")
    print("=" * 180)
    print(view.to_pandas().to_string(index=False))
    print(f"Saved: {summary_path}")
    print(f"[iter97] elapsed={time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
