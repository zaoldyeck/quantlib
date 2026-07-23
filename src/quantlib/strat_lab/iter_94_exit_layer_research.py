"""iter_94 - MFE/MAE-informed exit layer research.

This pass keeps the current strongest target-book strategy intact and tests
whether a professional long-only exit layer improves realistic Fubon execution
results.  Exits are evaluated in the execution simulator so the same fees,
sell tax, slippage, volume caps, and limit-up/down blocks apply.
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
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_94_exit_layer_research"
SOURCE_DAILY = RESULTS / "iter_92_execution_meta_switch_daily.csv"
SOURCE_TARGETS = RESULTS / "iter_92_execution_meta_switch_target_weights.csv"
N_TRIALS = 41_116 + 96


def trailing_calendar_return(daily: pl.DataFrame, months: int) -> float:
    ordered = daily.sort("date")
    end = ordered["date"][-1]
    anchor = end - relativedelta(months=months)
    start = ordered.filter(pl.col("date") <= anchor).tail(1)
    if start.is_empty():
        return float(ordered["nav"][-1] / CAPITAL - 1.0)
    return float(ordered["nav"][-1] / start["nav"][0] - 1.0)


def exit_objective(row: dict[str, object]) -> float:
    oos = max(float(row.get("oos_cagr") or 0.0), 0.0)
    recent = min(max(float(row.get("recent_1y_cagr") or 0.0), 0.0), 3.0)
    mdd = abs(float(row.get("oos_mdd") or 0.0))
    sortino = max(float(row.get("oos_sortino") or 0.0), 0.0)
    dsr = max(float(row.get("dsr") or 0.0), 0.0)
    pbo = max(float(row.get("pbo") or 0.0), 0.0)
    fill = max(float(row.get("fill_ratio") or 0.0), 0.0)
    mdd_penalty = 1.0 if mdd <= 0.30 else max(0.55, 0.30 / max(mdd, 1e-9))
    sortino_factor = min(1.20, max(0.55, sortino / 2.0))
    dsr_factor = min(1.0, max(0.50, dsr / 0.95))
    pbo_factor = min(1.0, max(0.50, (0.50 - pbo) / 0.50))
    fill_factor = min(1.0, max(0.60, fill / 0.80))
    return oos * (1.0 + recent) * mdd_penalty * sortino_factor * dsr_factor * pbo_factor * fill_factor


def build_exit_configs() -> list[ExitConfig]:
    configs = [ExitConfig(name="no_exit")]

    for stop in (0.08, 0.10, 0.12, 0.15, 0.18):
        configs.append(ExitConfig(name=f"sl{int(stop * 100)}", stop_loss_pct=stop))

    for trail in (0.10, 0.12, 0.15, 0.18, 0.22, 0.26, 0.30):
        configs.append(ExitConfig(name=f"tr{int(trail * 100)}", trailing_stop_pct=trail))

    for take in (0.35, 0.50, 0.75, 1.00, 1.50):
        configs.append(ExitConfig(name=f"tp{int(take * 100)}", take_profit_pct=take))

    for stop in (0.10, 0.12, 0.15):
        for trail in (0.12, 0.15, 0.18, 0.22, 0.26):
            configs.append(
                ExitConfig(
                    name=f"sl{int(stop * 100)}_tr{int(trail * 100)}",
                    stop_loss_pct=stop,
                    trailing_stop_pct=trail,
                )
            )

    for trail in (0.12, 0.15, 0.18, 0.22, 0.26):
        for trigger in (0.12, 0.18, 0.25):
            configs.append(
                ExitConfig(
                    name=f"tr{int(trail * 100)}_be{int(trigger * 100)}",
                    trailing_stop_pct=trail,
                    breakeven_trigger_pct=trigger,
                    breakeven_buffer_pct=0.01,
                )
            )

    for stop in (0.10, 0.12, 0.15):
        for take in (0.50, 0.75, 1.00):
            configs.append(
                ExitConfig(
                    name=f"sl{int(stop * 100)}_tp{int(take * 100)}",
                    stop_loss_pct=stop,
                    take_profit_pct=take,
                )
            )

    for days in (20, 30, 40, 50, 60, 70, 80, 100):
        for min_ret in (-0.05, -0.03, -0.01, 0.0, 0.02, 0.05, 0.10):
            configs.append(
                ExitConfig(
                    name=f"time{days}_r{int(min_ret * 100):+d}",
                    time_stop_days=days,
                    time_stop_min_return_pct=min_ret,
                )
            )

    for days in (40, 50, 60, 70, 80):
        for min_ret in (-0.03, -0.01, 0.0, 0.02):
            for stop in (0.10, 0.12, 0.15):
                configs.append(
                    ExitConfig(
                        name=f"sl{int(stop * 100)}_time{days}_r{int(min_ret * 100):+d}",
                        stop_loss_pct=stop,
                        time_stop_days=days,
                        time_stop_min_return_pct=min_ret,
                    )
                )

    for days in (40, 60, 80):
        for min_ret in (-0.03, 0.0, 0.02):
            for take in (0.75, 1.00, 1.50):
                configs.append(
                    ExitConfig(
                        name=f"tp{int(take * 100)}_time{days}_r{int(min_ret * 100):+d}",
                        take_profit_pct=take,
                        time_stop_days=days,
                        time_stop_min_return_pct=min_ret,
                    )
                )

    for days in (40, 60, 80):
        for min_ret in (-0.03, 0.0, 0.02):
            for trail in (0.22, 0.26, 0.30):
                configs.append(
                    ExitConfig(
                        name=f"tr{int(trail * 100)}_time{days}_r{int(min_ret * 100):+d}",
                        trailing_stop_pct=trail,
                        time_stop_days=days,
                        time_stop_min_return_pct=min_ret,
                    )
                )

    # Two slightly more professional combined rules suggested by the baseline
    # MFE/MAE pattern: cap early losers, keep large winners on a wide trail.
    configs.extend(
        [
            ExitConfig(
                name="pro_sl12_tr22_be18",
                stop_loss_pct=0.12,
                trailing_stop_pct=0.22,
                breakeven_trigger_pct=0.18,
                breakeven_buffer_pct=0.01,
            ),
            ExitConfig(
                name="pro_sl15_tr26_be25_time60",
                stop_loss_pct=0.15,
                trailing_stop_pct=0.26,
                breakeven_trigger_pct=0.25,
                breakeven_buffer_pct=0.01,
                time_stop_days=60,
                time_stop_min_return_pct=0.0,
            ),
        ]
    )
    dedup: dict[str, ExitConfig] = {}
    for config in configs:
        dedup[config.name] = config
    return list(dedup.values())


def mfe_mae_summary(trades: pl.DataFrame) -> dict[str, object]:
    if trades.is_empty():
        return {}
    return {
        "trade_count": trades.height,
        "win_rate": float((trades["gross_return"] > 0).mean()),
        "avg_mfe": float(trades["mfe_pct"].mean()),
        "avg_mae": float(trades["mae_pct"].mean()),
        "mfe_q25": float(trades["mfe_pct"].quantile(0.25)),
        "mfe_q50": float(trades["mfe_pct"].quantile(0.50)),
        "mfe_q75": float(trades["mfe_pct"].quantile(0.75)),
        "mae_q25": float(trades["mae_pct"].quantile(0.25)),
        "mae_q50": float(trades["mae_pct"].quantile(0.50)),
        "mae_q75": float(trades["mae_pct"].quantile(0.75)),
        "loser_avg_mfe": float(
            trades.filter(pl.col("gross_return") <= 0)["mfe_pct"].mean()
        ),
        "winner_avg_mae": float(
            trades.filter(pl.col("gross_return") > 0)["mae_pct"].mean()
        ),
    }


def run_config(
    simulator: RealisticExecutionSimulator,
    days: list[date],
    targets: dict[date, dict[str, float]],
    base_config: ExecutionConfig,
    exit_config: ExitConfig,
) -> tuple[dict[str, object], object]:
    simulator.config = replace(base_config, exit_config=exit_config)
    result = simulator.simulate(days, targets)
    trade_metrics = trade_distribution_metrics(result.trades["net_pnl"].to_list(), prefix="trade_")
    row = validate_daily_nav(
        f"iter94_{exit_config.name}",
        result.daily.select(["date", "nav"]),
        n_trials=N_TRIALS,
        extra={
            **result.stats,
            **trade_metrics,
            "exit_config": exit_config.name,
            "ret_1y": trailing_calendar_return(result.daily, 12),
            "ret_6m": trailing_calendar_return(result.daily, 6),
            "ret_3m": trailing_calendar_return(result.daily, 3),
            "ret_1m": trailing_calendar_return(result.daily, 1),
            "config": str(asdict(exit_config)),
        },
    )
    row["exit_objective"] = exit_objective(row)
    return row, result


def main() -> None:
    base_daily = pl.read_csv(SOURCE_DAILY, try_parse_dates=True).sort("date")
    days = base_daily["date"].to_list()
    targets = load_execution_targets(SOURCE_TARGETS)
    codes = sorted({code for book in targets.values() for code in book if code != "CASH"})

    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, codes, days[0], days[-1])
        data_cutoff = con.sql("select max(date) from daily_quote").fetchone()[0]
    finally:
        con.close()

    base_config = ExecutionConfig(
        name="fubon_odd_lot_5pct_vol_slip5bp_exit_layer",
        capital=CAPITAL,
        lot_size=1,
        max_participation_rate=0.05,
        fixed_slippage_bps=5.0,
        impact_bps_per_1pct_volume=1.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
    )
    simulator = RealisticExecutionSimulator(bars, base_config)
    rows: list[dict[str, object]] = []
    artifacts: list[tuple[float, str, object]] = []
    baseline_trades: pl.DataFrame | None = None

    configs = build_exit_configs()
    print(f"[iter94] data_cutoff={data_cutoff} configs={len(configs)} codes={len(codes)}", flush=True)
    for i, exit_config in enumerate(configs, 1):
        row, result = run_config(simulator, days, targets, base_config, exit_config)
        rows.append(row)
        artifacts.append((float(row["exit_objective"]), exit_config.name, result))
        if exit_config.name == "no_exit":
            baseline_trades = result.trades
        print(
            f"[iter94] {i:03d}/{len(configs)} {exit_config.name:24s} "
            f"OOS={float(row['oos_cagr']):+.2%} 1Y={float(row['recent_1y_cagr']):+.2%} "
            f"MDD={float(row['oos_mdd']):.2%} Sortino={float(row['oos_sortino']):.3f} "
            f"DSR={float(row['dsr']):.3f} PBO={float(row['pbo']):.3f} "
            f"exits={float(row['exit_orders']):.0f} obj={float(row['exit_objective']):.3f}",
            flush=True,
        )

    summary = pl.DataFrame(rows).sort("exit_objective", descending=True)
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.write_csv(summary_path)
    if baseline_trades is not None:
        baseline_trades.write_csv(RESULTS / f"{OUT_PREFIX}_baseline_trades.csv")
        pl.DataFrame([mfe_mae_summary(baseline_trades)]).write_csv(
            RESULTS / f"{OUT_PREFIX}_mfe_mae_summary.csv"
        )

    saved = set(summary.head(8)["exit_config"].to_list())
    for _score, name, result in sorted(artifacts, key=lambda item: item[0], reverse=True):
        if name not in saved:
            continue
        result.daily.write_csv(RESULTS / f"{OUT_PREFIX}_{name}_daily.csv")
        result.fills.write_csv(RESULTS / f"{OUT_PREFIX}_{name}_fills.csv")
        result.trades.write_csv(RESULTS / f"{OUT_PREFIX}_{name}_trades.csv")

    print("=" * 140)
    print("iter94 MFE/MAE-informed exit layer search")
    print("=" * 140)
    print(
        summary.select(
            [
                "exit_config",
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
                pl.col("trade_profit_factor").round(3),
                pl.col("trade_sqn").round(3),
                pl.col("exit_orders").round(0),
                pl.col("exit_objective").round(3),
            ]
        )
        .head(15)
        .to_pandas()
        .to_string(index=False)
    )
    if baseline_trades is not None:
        print("\nBaseline MFE/MAE diagnostic")
        print(pl.DataFrame([mfe_mae_summary(baseline_trades)]).to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
