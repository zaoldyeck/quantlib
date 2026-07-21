"""Iter100 - ICT2022 / SMC / TPO-proxy overlay on Iter95 champion.

This runner is deliberately bounded.  It tests whether structure-aware daily
proxies can improve the existing Iter95 target book under the same realistic
execution assumptions.  It does not alter live trading configuration.
"""

from __future__ import annotations

import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
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
from iter100_features import add_iter100_features  # noqa: E402
from iter100_objective import add_iter100_objective, portfolio_below_cost_metrics  # noqa: E402
from iter_82_oos_recent_pm_allocator import load_execution_targets  # noqa: E402
from iter_96_robust_alpha_research import load_benchmark_nav, relative_metrics  # noqa: E402
from validator import validate_daily_nav  # noqa: E402


RESULTS = REPO_ROOT / "research/strat_lab/results"
OUT_PREFIX = "iter_100_ict_smc_tpo_overlay"
SOURCE_TARGETS = RESULTS / "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_target_weights.csv"
SOURCE_DAILY = RESULTS / "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_daily.csv"
N_TRIALS_PRIOR = 41_116 + 229 + 960 + 9 + 36


Book = dict[str, float]
BookByDate = dict[date, Book]


@dataclass(frozen=True)
class OverlayConfig:
    name: str
    min_entry_score: float = 0.0
    require_entry_mss: bool = False
    require_entry_fvg_or_sweep: bool = False
    exit_score_below: float | None = None
    exit_on_structure_break: bool = False
    regime_gate: bool = False
    renormalize: bool = False


def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def configs() -> list[OverlayConfig]:
    return [
        OverlayConfig(name="baseline_iter95_targets"),
        OverlayConfig(name="entry_score_1p5", min_entry_score=1.5),
        OverlayConfig(name="entry_score_2p0", min_entry_score=2.0),
        OverlayConfig(name="entry_mss_score_1p5", min_entry_score=1.5, require_entry_mss=True),
        OverlayConfig(
            name="entry_mss_fvg_or_sweep_score_1p5",
            min_entry_score=1.5,
            require_entry_mss=True,
            require_entry_fvg_or_sweep=True,
        ),
        OverlayConfig(name="exit_structure_break", exit_on_structure_break=True),
        OverlayConfig(name="entry_score_1p5_exit_break", min_entry_score=1.5, exit_on_structure_break=True),
        OverlayConfig(name="regime_entry_score_1p5", min_entry_score=1.5, regime_gate=True),
        OverlayConfig(
            name="regime_entry_score_1p5_exit_break",
            min_entry_score=1.5,
            exit_on_structure_break=True,
            regime_gate=True,
        ),
        OverlayConfig(
            name="renorm_regime_entry_score_1p5_exit_break",
            min_entry_score=1.5,
            exit_on_structure_break=True,
            regime_gate=True,
            renormalize=True,
        ),
    ]


def load_days() -> list[date]:
    daily = pl.read_csv(SOURCE_DAILY, try_parse_dates=True).sort("date")
    return daily["date"].to_list()


def expand_targets(days: list[date], sparse_targets: BookByDate) -> BookByDate:
    active: Book = {}
    out: BookByDate = {}
    for day in days:
        if day in sparse_targets:
            active = dict(sparse_targets[day])
        out[day] = dict(active)
    return out


def compact_targets(days: list[date], daily_targets: BookByDate) -> BookByDate:
    out: BookByDate = {}
    last: Book | None = None
    for day in days:
        book = daily_targets.get(day, {})
        if last is None or book != last:
            out[day] = dict(book)
            last = dict(book)
    return out


def load_feature_lookup(codes: list[str], days: list[date]) -> tuple[pl.DataFrame, dict[tuple[date, str], dict[str, object]]]:
    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, sorted(set(codes) | {"0050"}), days[0], days[-1], markets=("twse", "tpex"))
    finally:
        con.close()
    features = add_iter100_features(
        bars.select(["date", "company_code", "open", "high", "low", "close", "volume", "trade_value"])
    )
    lookup = {
        (row["date"], str(row["company_code"])): row
        for row in features.iter_rows(named=True)
    }
    return bars, lookup


def market_ok(row: dict[str, object] | None) -> bool:
    if row is None:
        return False
    close = float(row.get("close") or 0.0)
    poc = float(row.get("tpo_proxy_poc20") or 0.0)
    return close > 0.0 and poc > 0.0 and close >= poc


def entry_allowed(row: dict[str, object] | None, cfg: OverlayConfig) -> bool:
    if row is None:
        return False
    score = float(row.get("iter100_structure_score") or 0.0)
    if score < cfg.min_entry_score:
        return False
    if cfg.require_entry_mss and not bool(row.get("mss_up20")) and not bool(row.get("bos_up60")):
        return False
    if cfg.require_entry_fvg_or_sweep and not (
        bool(row.get("bullish_fvg_daily_proxy")) or bool(row.get("liquidity_sweep_reclaim20"))
    ):
        return False
    return True


def exit_triggered(row: dict[str, object] | None, cfg: OverlayConfig) -> bool:
    if row is None:
        return True
    score = float(row.get("iter100_structure_score") or 0.0)
    if cfg.exit_score_below is not None and score < cfg.exit_score_below:
        return True
    if cfg.exit_on_structure_break:
        close = float(row.get("close") or 0.0)
        swing_low = float(row.get("swing_low20_prev") or 0.0)
        if close > 0.0 and swing_low > 0.0 and close < swing_low:
            return True
    return False


def apply_overlay(days: list[date], base_sparse: BookByDate, features: dict[tuple[date, str], dict[str, object]], cfg: OverlayConfig) -> BookByDate:
    base_daily = expand_targets(days, base_sparse)
    overlay_daily: BookByDate = {}
    active: Book = {}
    for i, day in enumerate(days):
        signal_day = days[max(i - 1, 0)]
        base_book = base_daily.get(day, {})
        desired: Book = {}
        if cfg.regime_gate and not market_ok(features.get((signal_day, "0050"))):
            overlay_daily[day] = {}
            active = {}
            continue
        for code, weight in base_book.items():
            row = features.get((signal_day, code))
            is_existing = code in active
            if is_existing and exit_triggered(row, cfg):
                continue
            if not is_existing and not entry_allowed(row, cfg):
                continue
            desired[code] = float(weight)
        if cfg.renormalize and desired:
            total = sum(desired.values())
            if total > 0:
                desired = {code: weight / total for code, weight in desired.items()}
        overlay_daily[day] = desired
        active = desired
    return compact_targets(days, overlay_daily)


def target_rows(targets: BookByDate) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {"date": day, "company_code": code, "target_weight": weight}
            for day, book in sorted(targets.items())
            for code, weight in sorted(book.items())
        ]
    )


def benchmark(code: str, start: date, end: date, label: str) -> pl.DataFrame:
    return load_benchmark_nav(code, start, end, label).select(["date", "nav"]).sort("date")


def run() -> None:
    t0 = time.time()
    days = load_days()
    base_targets = load_execution_targets(SOURCE_TARGETS)
    codes = sorted({code for book in base_targets.values() for code in book})
    bars, features = load_feature_lookup(codes, days)
    b0050 = benchmark("0050", days[0], days[-1], "0050 TR")
    b2330 = benchmark("2330", days[0], days[-1], "2330 TR")
    exit_cfg = ExitConfig(name="time50_r-1", time_stop_days=50, time_stop_min_return_pct=-0.01)
    sim_cfg = ExecutionConfig(
        name="fubon_odd_lot_iter100",
        lot_size=1,
        max_participation_rate=0.05,
        fixed_slippage_bps=5.0,
        impact_bps_per_1pct_volume=1.0,
        fee_schedule=FubonFeeSchedule(),
        exit_config=exit_cfg,
    )
    simulator = RealisticExecutionSimulator(bars, sim_cfg)

    rows: list[dict[str, object]] = []
    for idx, cfg in enumerate(configs(), 1):
        print(f"[iter100] {idx}/{len(configs())} {cfg.name}", flush=True)
        targets = base_targets if cfg.name == "baseline_iter95_targets" else apply_overlay(days, base_targets, features, cfg)
        result = simulator.simulate(days, targets)
        key = safe_name(cfg.name)
        result.daily.write_csv(RESULTS / f"{OUT_PREFIX}_{key}_daily.csv")
        result.fills.write_csv(RESULTS / f"{OUT_PREFIX}_{key}_fills.csv")
        result.trades.write_csv(RESULTS / f"{OUT_PREFIX}_{key}_trades.csv")
        target_rows(targets).write_csv(RESULTS / f"{OUT_PREFIX}_{key}_target_weights.csv")
        row = validate_daily_nav(
            cfg.name,
            result.daily.select(["date", "nav"]),
            n_trials=N_TRIALS_PRIOR + len(configs()),
            extra={**asdict(cfg), **result.stats, "target_rebalance_days": len(targets)},
        )
        row.update(relative_metrics(result.daily.select(["date", "nav"]), b0050, "b0050"))
        row.update(relative_metrics(result.daily.select(["date", "nav"]), b2330, "b2330"))
        row.update(portfolio_below_cost_metrics(result.fills, bars, result.daily))
        rows.append(add_iter100_objective(row, result.trades))

    summary = pl.DataFrame(rows, infer_schema_length=10_000).sort("iter100_cost_below_objective", descending=True)
    path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.write_csv(path)
    print(f"[iter100] wrote {path}", flush=True)
    print(
        summary.select(
            [
                "name",
                "cagr",
                "oos_cagr",
                "recent_1y_cagr",
                "mdd",
                "oos_mdd",
                "dsr",
                "pbo",
                "fill_ratio",
                "trade_below_cost_mae_p95",
                "trade_mfe_retention_mean",
                "iter100_cost_below_objective",
                "b0050_final_relative_nav",
                "b2330_final_relative_nav",
                "max_active",
                "target_rebalance_days",
            ]
        ).to_pandas().to_string(index=False),
        flush=True,
    )
    print(f"[iter100] elapsed={time.time() - t0:.1f}s", flush=True)


if __name__ == "__main__":
    run()
