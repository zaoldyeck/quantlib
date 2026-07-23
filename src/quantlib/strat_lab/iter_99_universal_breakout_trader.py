"""Iter99 - universal event-driven breakout/trend strategy research.

Goal: a single long-only strategy that scans the whole Taiwan stock universe
for opportunity, enters when market structure breaks in its favor, cuts losers
quickly, and holds winners with a trailing/structure-aware exit.

This is intentionally not a stock-specific pattern.  All signals are
cross-sectional and point-in-time from adjusted OHLCV/liquidity features.
"""

from __future__ import annotations

import re
import sys
import time
import os
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from quantlib.constants import CAPITAL  # noqa: E402
from quantlib.db import connect  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    ExitConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from iter_96_robust_alpha_research import load_benchmark_nav, relative_metrics, robust_alpha_objective  # noqa: E402
from quantlib.prices import fetch_adjusted_panel  # noqa: E402
from validator import validate_daily_nav  # noqa: E402


START = date(2008, 1, 2)
RESULTS = REPO_ROOT / f"{paths.OUT_STRAT_LAB}"
PROFILE = os.environ.get("ITER99_PROFILE", "baseline")
OUT_PREFIX = "iter_99_universal_breakout_trader" if PROFILE == "baseline" else f"iter_99_universal_breakout_trader_{PROFILE}"
N_TRIALS_PRIOR = 41_116 + 229 + 960 + 9


@dataclass(frozen=True)
class UniversalConfig:
    name: str
    entry_mode: str
    max_positions: int
    min_adv60: float
    stop_loss_pct: float
    trailing_stop_pct: float
    breakeven_trigger_pct: float
    time_stop_days: int
    time_stop_min_return_pct: float
    virtual_trail_pct: float
    virtual_ma_exit: int
    max_weight: float
    rebalance_interval_days: int = 0
    min_vol_surge: float = 1.35
    max_atr20_pct: float = 0.12
    max_ma20_gap: float = 0.28
    min_near_52w_high: float = 0.72
    min_score: float = -9.0
    require_market_regime: bool = False
    allow_tpex: bool = True


def log(message: str) -> None:
    print(message, flush=True)


def latest_0050_day() -> date:
    con = connect(read_only=True)
    try:
        return con.sql(
            """
            SELECT MAX(date)
            FROM daily_quote
            WHERE market='twse' AND company_code='0050'
            """
        ).fetchone()[0]
    finally:
        con.close()


def z(col: str) -> pl.Expr:
    mean = pl.col(col).mean().over("date")
    std = pl.col(col).std().over("date")
    return ((pl.col(col) - mean) / std.clip(1e-9, None)).clip(-3.0, 3.0).fill_null(0.0)


def load_feature_panel(start: date, end: date) -> tuple[pl.DataFrame, list[date]]:
    t0 = time.time()
    con = connect(read_only=True)
    try:
        panels = [
            fetch_adjusted_panel(
                con,
                start.isoformat(),
                end.isoformat(),
                market=market,
                include_extra_history_days=420,
            )
            for market in ("twse", "tpex")
        ]
        etf = con.sql("SELECT DISTINCT company_code FROM etf").pl().with_columns(pl.lit(True).alias("is_etf"))
    finally:
        con.close()

    raw_panel = (
        pl.concat([p for p in panels if not p.is_empty()], how="diagonal")
        .sort(["company_code", "date", "trade_value"], descending=[False, False, True])
        .unique(subset=["company_code", "date"], keep="first", maintain_order=True)
        .sort(["company_code", "date"])
    )
    market_signal = (
        raw_panel.filter((pl.col("market") == "twse") & (pl.col("company_code") == "0050"))
        .sort("date")
        .with_columns(
            [
                pl.col("close").rolling_mean(50).alias("market_ma50"),
                pl.col("close").rolling_mean(100).alias("market_ma100"),
                pl.col("close").rolling_mean(200).alias("market_ma200"),
                pl.col("close").pct_change(20).alias("market_ret20"),
                pl.col("close").pct_change(60).alias("market_ret60"),
            ]
        )
        .with_columns(
            (
                (pl.col("close") > pl.col("market_ma200"))
                & (pl.col("market_ma50") > pl.col("market_ma200"))
                & (pl.col("market_ret20").fill_null(0.0) > -0.10)
                & (pl.col("market_ret60").fill_null(0.0) > -0.15)
            ).alias("market_ok")
        )
        .select(["date", "market_ok", "market_ret20", "market_ret60"])
    )
    panel = (
        raw_panel
        .join(etf, on="company_code", how="left")
        .with_columns(pl.col("is_etf").fill_null(False))
        .filter(pl.col("company_code").str.contains(r"^[1-9][0-9]{3}$"))
        .filter(~pl.col("is_etf"))
        .join(market_signal, on="date", how="left")
        .with_columns(
            [
                pl.col("market_ok").fill_null(False),
                pl.col("market_ret20").fill_null(0.0),
                pl.col("market_ret60").fill_null(0.0),
            ]
        )
        .sort(["company_code", "date"])
    )
    if panel.is_empty():
        raise RuntimeError("empty universal feature panel")

    tr = pl.max_horizontal(
        [
            pl.col("high") - pl.col("low"),
            (pl.col("high") - pl.col("close").shift(1).over("company_code")).abs(),
            (pl.col("low") - pl.col("close").shift(1).over("company_code")).abs(),
        ]
    )
    panel = (
        panel.with_columns(
            [
                pl.col("close").pct_change(5).over("company_code").alias("ret5"),
                pl.col("close").pct_change(10).over("company_code").alias("ret10"),
                pl.col("close").pct_change(20).over("company_code").alias("ret20"),
                pl.col("close").pct_change(60).over("company_code").alias("ret60"),
                pl.col("close").pct_change(120).over("company_code").alias("ret120"),
                pl.col("close").rolling_mean(10).over("company_code").alias("ma10"),
                pl.col("close").rolling_mean(20).over("company_code").alias("ma20"),
                pl.col("close").rolling_mean(60).over("company_code").alias("ma60"),
                pl.col("close").rolling_mean(200).over("company_code").alias("ma200"),
                pl.col("close").rolling_max(20).over("company_code").shift(1).over("company_code").alias("high20_prev"),
                pl.col("close").rolling_max(60).over("company_code").shift(1).over("company_code").alias("high60_prev"),
                pl.col("close").rolling_max(120).over("company_code").shift(1).over("company_code").alias("high120_prev"),
                pl.col("close").rolling_max(252).over("company_code").alias("high252"),
                pl.col("close").rolling_min(20).over("company_code").shift(1).over("company_code").alias("low20_prev"),
                pl.col("close").rolling_min(60).over("company_code").shift(1).over("company_code").alias("low60_prev"),
                pl.col("trade_value").rolling_mean(5).over("company_code").alias("tv_avg5"),
                pl.col("trade_value").rolling_mean(60).over("company_code").alias("tv_avg60"),
                pl.col("trade_value").rolling_median(60).over("company_code").alias("adv60"),
                tr.rolling_mean(14).over("company_code").alias("atr14"),
                tr.rolling_mean(20).over("company_code").alias("atr20"),
            ]
        )
        .with_columns(
            [
                (pl.col("tv_avg5") / pl.col("tv_avg60").clip(1.0, None)).alias("vol_surge"),
                (pl.col("atr14") / pl.col("close")).alias("atr14_pct"),
                (pl.col("atr20") / pl.col("close")).alias("atr20_pct"),
                ((pl.col("high20_prev") - pl.col("low20_prev")) / pl.col("close")).alias("range20_pct"),
                ((pl.col("high60_prev") - pl.col("low60_prev")) / pl.col("close")).alias("range60_pct"),
                (pl.col("close") / pl.col("high252")).alias("near_52w_high"),
                (pl.col("close") / pl.col("ma20") - 1.0).alias("ma20_gap"),
                (pl.col("close") / pl.col("ma60") - 1.0).alias("ma60_gap"),
                (pl.col("close") / pl.col("ma200") - 1.0).alias("ma200_gap"),
                (pl.col("close") / pl.col("high20_prev") - 1.0).alias("bos20"),
                (pl.col("close") / pl.col("high60_prev") - 1.0).alias("bos60"),
                (pl.col("close") / pl.col("high120_prev") - 1.0).alias("bos120"),
            ]
        )
        .with_columns(
            [
                z("ret20").alias("z_ret20"),
                z("ret60").alias("z_ret60"),
                z("ret120").alias("z_ret120"),
                z("vol_surge").alias("z_vol_surge"),
                z("near_52w_high").alias("z_near_high"),
                z("ma60_gap").alias("z_ma60_gap"),
                z("atr20_pct").alias("z_atr20"),
                z("range20_pct").alias("z_range20"),
            ]
        )
        .with_columns(
            (
                0.22 * pl.col("z_ret20")
                + 0.24 * pl.col("z_ret60")
                + 0.14 * pl.col("z_ret120")
                + 0.18 * pl.col("z_vol_surge")
                + 0.14 * pl.col("z_near_high")
                + 0.08 * pl.col("z_ma60_gap")
                - 0.13 * pl.col("z_atr20")
                - 0.06 * pl.col("z_range20")
            ).alias("universal_score")
        )
        .filter((pl.col("date") >= start) & (pl.col("date") <= end))
    )
    days = sorted(panel.filter(pl.col("market") == "twse")["date"].unique().to_list())
    log(
        f"[iter99] feature panel rows={panel.height:,} codes={panel['company_code'].n_unique():,} "
        f"days={len(days):,} elapsed={time.time() - t0:.1f}s"
    )
    return panel, days


def entry_expr(cfg: UniversalConfig) -> pl.Expr:
    base = (
        (pl.col("adv60").fill_null(0.0) >= cfg.min_adv60)
        & (pl.col("close") > pl.col("ma20"))
        & (pl.col("ma20") > pl.col("ma60"))
        & (pl.col("vol_surge").fill_null(0.0) >= cfg.min_vol_surge)
        & (pl.col("atr20_pct").fill_null(9.0) <= cfg.max_atr20_pct)
        & (pl.col("ma20_gap").fill_null(9.0) <= cfg.max_ma20_gap)
        & (pl.col("near_52w_high").fill_null(0.0) >= cfg.min_near_52w_high)
        & (pl.col("universal_score").fill_null(-9.0) >= cfg.min_score)
    )
    if not cfg.allow_tpex:
        base &= pl.col("market") == "twse"
    if cfg.require_market_regime:
        base &= pl.col("market_ok")
    if cfg.entry_mode == "bos60":
        return base & (pl.col("bos60") >= 0.005) & (pl.col("ret20") >= 0.08)
    if cfg.entry_mode == "bos120":
        return base & (pl.col("bos120") >= 0.003) & (pl.col("ret60") >= 0.16)
    if cfg.entry_mode == "compression_bos20":
        return (
            base
            & (pl.col("bos20") >= 0.01)
            & (pl.col("range20_pct").fill_null(9.0) <= 0.22)
            & (pl.col("vol_surge").fill_null(0.0) >= 1.70)
            & (pl.col("ret5") >= 0.035)
        )
    if cfg.entry_mode == "smc_reclaim":
        return (
            base
            & (pl.col("close") > pl.col("high20_prev"))
            & (pl.col("low").rolling_min(10).over("company_code") <= pl.col("low20_prev") * 1.01)
            & (pl.col("ret10") >= 0.04)
        )
    raise ValueError(f"unknown entry_mode {cfg.entry_mode}")


def build_target_book(panel: pl.DataFrame, days: list[date], cfg: UniversalConfig) -> tuple[dict[date, dict[str, float]], pl.DataFrame]:
    signals = (
        panel.with_columns(entry_expr(cfg).alias("entry"))
        .filter(pl.col("entry"))
        .select(
            [
                "date",
                "market",
                "company_code",
                "close",
                "ma10",
                "ma20",
                "ma60",
                "high20_prev",
                "low20_prev",
                "universal_score",
                "ret20",
                "ret60",
                "vol_surge",
                "atr14_pct",
                "adv60",
                "near_52w_high",
            ]
        )
        .sort(["date", "universal_score"], descending=[False, True])
    )
    features_by_day: dict[date, dict[str, dict[str, float]]] = {}
    for row in panel.select(
        [
            "date",
            "company_code",
            "close",
            "ma10",
            "ma20",
            "ma60",
            "high20_prev",
            "low20_prev",
            "universal_score",
            "market_ok",
        ]
    ).iter_rows(named=True):
        features_by_day.setdefault(row["date"], {})[str(row["company_code"])] = row
    market_ok_by_day = (
        panel.select(["date", "market_ok"])
        .unique(subset=["date"], keep="first", maintain_order=True)
        .sort("date")
    )
    market_ok_lookup = {row["date"]: bool(row["market_ok"]) for row in market_ok_by_day.iter_rows(named=True)}
    signals_by_day: dict[date, list[dict[str, object]]] = {}
    for row in signals.iter_rows(named=True):
        signals_by_day.setdefault(row["date"], []).append(row)

    # Signal at close on day D is executed at next trading day's open.
    targets: dict[date, dict[str, float]] = {}
    records: list[dict[str, object]] = []
    positions: dict[str, dict[str, object]] = {}
    day_index = {day: i for i, day in enumerate(days)}
    last_target_codes: tuple[str, ...] = ()
    last_target_weights: dict[str, float] = {}
    last_rebalance_index = -10**9

    for i, signal_day in enumerate(days[:-1]):
        target_day = days[i + 1]
        day_features = features_by_day.get(signal_day, {})
        changed = False

        if cfg.require_market_regime and not market_ok_lookup.get(signal_day, False):
            if positions:
                positions.clear()
                targets[target_day] = {}
                last_target_codes = ()
                last_target_weights = {}
                last_rebalance_index = i
            continue

        # Virtual close-based exit layer; execution simulator adds intraday hard stops.
        for code in list(positions):
            feat = day_features.get(code)
            if feat is None:
                positions.pop(code, None)
                changed = True
                continue
            close = float(feat["close"])
            pos = positions[code]
            pos["peak_close"] = max(float(pos["peak_close"]), close)
            held_days = day_index[signal_day] - int(pos["entry_signal_index"])
            entry_close = float(pos["entry_close"])
            ret = close / entry_close - 1.0 if entry_close > 0 else -1.0
            trail_broken = close <= float(pos["peak_close"]) * (1.0 - cfg.virtual_trail_pct)
            ma_broken = close < float(feat[f"ma{cfg.virtual_ma_exit}"] or 0.0)
            time_failed = held_days >= cfg.time_stop_days and ret < cfg.time_stop_min_return_pct
            structure_broken = close < float(feat["low20_prev"] or 0.0)
            if trail_broken or ma_broken or time_failed or structure_broken:
                positions.pop(code, None)
                changed = True

        # New entries: ranked opportunities not already held.
        slots = cfg.max_positions - len(positions)
        if slots > 0:
            for row in signals_by_day.get(signal_day, []):
                code = str(row["company_code"])
                if code in positions:
                    continue
                positions[code] = {
                    "entry_signal_date": signal_day,
                    "entry_target_date": target_day,
                    "entry_signal_index": i,
                    "entry_close": float(row["close"]),
                    "peak_close": float(row["close"]),
                    "entry_score": float(row["universal_score"]),
                    "entry_mode": cfg.entry_mode,
                }
                changed = True
                slots -= 1
                if slots <= 0:
                    break

        if positions:
            target_codes = tuple(sorted(positions))
            weight = min(cfg.max_weight, 1.0 / len(positions))
            target_weights = {code: weight for code in target_codes}
            interval_due = (
                cfg.rebalance_interval_days > 0
                and i - last_rebalance_index >= cfg.rebalance_interval_days
            )
            if changed or target_codes != last_target_codes or interval_due:
                targets[target_day] = target_weights
                last_target_codes = target_codes
                last_target_weights = target_weights
                last_rebalance_index = i
                for code, pos in sorted(positions.items()):
                    feat = day_features.get(code, {})
                    records.append(
                        {
                            "date": target_day,
                            "signal_date": signal_day,
                            "company_code": code,
                            "target_weight": weight,
                            "entry_signal_date": pos["entry_signal_date"],
                            "entry_score": pos["entry_score"],
                            "close": feat.get("close"),
                            "universal_score": feat.get("universal_score"),
                        }
                    )
        else:
            if last_target_codes:
                targets[target_day] = {}
                last_target_codes = ()
                last_target_weights = {}
                last_rebalance_index = i

    selected = pl.DataFrame(records, infer_schema_length=10_000) if records else pl.DataFrame()
    log(
        f"[iter99] {cfg.name}: target_days={len(targets):,} "
        f"selected_rows={selected.height:,} max_virtual_pos={cfg.max_positions}"
    )
    return targets, selected


def configs() -> list[UniversalConfig]:
    out: list[UniversalConfig] = []
    if PROFILE == "strict_regime":
        for entry_mode in ("bos120", "compression_bos20", "smc_reclaim"):
            for max_pos in (5, 8, 10):
                for stop, trail in ((0.10, 0.22), (0.12, 0.28)):
                    for min_score in (0.75, 1.25):
                        name = (
                            f"iter99_strict_{entry_mode}_cap{max_pos}_"
                            f"s{int(stop*100)}_tr{int(trail*100)}_score{int(min_score*100)}"
                        )
                        out.append(
                            UniversalConfig(
                                name=name,
                                entry_mode=entry_mode,
                                max_positions=max_pos,
                                min_adv60=80_000_000.0,
                                stop_loss_pct=stop,
                                trailing_stop_pct=trail,
                                breakeven_trigger_pct=0.14,
                                time_stop_days=35,
                                time_stop_min_return_pct=-0.02,
                                virtual_trail_pct=trail,
                                virtual_ma_exit=20,
                                max_weight=min(0.25, 1.0 / max_pos),
                                min_vol_surge=1.50,
                                max_atr20_pct=0.09,
                                max_ma20_gap=0.22,
                                min_near_52w_high=0.80,
                                min_score=min_score,
                                require_market_regime=True,
                            )
                        )
        return out

    for entry_mode in ("bos60", "bos120", "compression_bos20", "smc_reclaim"):
        for max_pos in (5, 10, 20):
            for stop, trail in ((0.08, 0.18), (0.10, 0.22), (0.12, 0.28)):
                name = f"iter99_{entry_mode}_cap{max_pos}_s{int(stop*100)}_tr{int(trail*100)}"
                out.append(
                    UniversalConfig(
                        name=name,
                        entry_mode=entry_mode,
                        max_positions=max_pos,
                        min_adv60=30_000_000.0,
                        stop_loss_pct=stop,
                        trailing_stop_pct=trail,
                        breakeven_trigger_pct=0.12,
                        time_stop_days=25,
                        time_stop_min_return_pct=0.0,
                        virtual_trail_pct=trail,
                        virtual_ma_exit=20,
                        max_weight=0.20,
                    )
                )
    return out


def load_iter95(start: date, end: date) -> pl.DataFrame:
    path = RESULTS / "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_daily.csv"
    if not path.exists():
        return pl.DataFrame()
    daily = pl.read_csv(path, try_parse_dates=True).select(["date", "nav"]).sort("date")
    daily = daily.filter((pl.col("date") >= start) & (pl.col("date") <= end))
    if daily.is_empty():
        return daily
    base = float(daily["nav"][0])
    return daily.with_columns((pl.col("nav") / base * CAPITAL).alias("nav"))


def benchmark(code: str, start: date, end: date, label: str) -> pl.DataFrame:
    return load_benchmark_nav(code, start, end, label).select(["date", "nav"]).sort("date")


def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def evaluate(cfg: UniversalConfig, daily: pl.DataFrame, selected: pl.DataFrame, stats: dict[str, float], b0050, b2330) -> dict[str, object]:
    extra = {
        **asdict(cfg),
        "avg_positions": float(daily["active"].mean()) if "active" in daily.columns and daily.height else 0.0,
        "max_active": stats.get("max_active", 0.0),
        "fill_ratio": stats.get("fill_ratio", 0.0),
        "avg_turnover_trade_day": stats.get("avg_turnover_trade_day", 0.0),
        "total_commission": stats.get("total_commission", 0.0),
        "total_tax": stats.get("total_tax", 0.0),
        "total_slippage_cost": stats.get("total_slippage_cost", 0.0),
        "blocked_orders": stats.get("blocked_orders", 0.0),
        "partial_orders": stats.get("partial_orders", 0.0),
        "selection_rows": selected.height,
    }
    row = validate_daily_nav(cfg.name, daily.select(["date", "nav"]), n_trials=N_TRIALS_PRIOR + len(configs()), extra=extra)
    row.update(relative_metrics(daily.select(["date", "nav"]), b0050, "b0050"))
    row.update(relative_metrics(daily.select(["date", "nav"]), b2330, "b2330"))
    row["robust_alpha_objective"] = robust_alpha_objective(row)
    return row


def run() -> None:
    t0 = time.time()
    end = latest_0050_day()
    panel, days = load_feature_panel(START, end)
    common_start = days[0]
    b0050 = benchmark("0050", common_start, end, "0050 TR")
    b2330 = benchmark("2330", common_start, end, "2330 TR")
    cfgs = configs()

    # Only load bars for codes that can pass any universal signal.
    signal_union = pl.concat(
        [panel.with_columns(entry_expr(cfg).alias("entry")).filter(pl.col("entry")).select("company_code") for cfg in cfgs],
        how="diagonal",
    )
    codes = sorted(set(signal_union["company_code"].to_list()))
    log(f"[iter99] execution universe codes={len(codes):,}")
    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, codes, common_start, end, markets=("twse", "tpex"))
    finally:
        con.close()
    simulator_cache: dict[tuple[float, float, int, float], RealisticExecutionSimulator] = {}

    rows: list[dict[str, object]] = []
    for idx, cfg in enumerate(cfgs, start=1):
        log(f"[iter99] ({idx}/{len(cfgs)}) {cfg.name}")
        targets, selected = build_target_book(panel, days, cfg)
        exit_cfg = ExitConfig(
            name=f"s{int(cfg.stop_loss_pct*100)}_tr{int(cfg.trailing_stop_pct*100)}_be{int(cfg.breakeven_trigger_pct*100)}_t{cfg.time_stop_days}",
            stop_loss_pct=cfg.stop_loss_pct,
            trailing_stop_pct=cfg.trailing_stop_pct,
            breakeven_trigger_pct=cfg.breakeven_trigger_pct,
            breakeven_buffer_pct=0.0,
            time_stop_days=cfg.time_stop_days,
            time_stop_min_return_pct=cfg.time_stop_min_return_pct,
        )
        key = (cfg.stop_loss_pct, cfg.trailing_stop_pct, cfg.time_stop_days, cfg.time_stop_min_return_pct)
        sim = simulator_cache.get(key)
        if sim is None:
            sim = RealisticExecutionSimulator(
                bars,
                ExecutionConfig(
                    name="fubon_odd_lot_iter99",
                    lot_size=1,
                    max_participation_rate=0.05,
                    fixed_slippage_bps=5.0,
                    impact_bps_per_1pct_volume=1.0,
                    fee_schedule=FubonFeeSchedule(),
                    exit_config=exit_cfg,
                ),
            )
            simulator_cache[key] = sim
        result = sim.simulate(days, targets)
        key_name = safe_name(cfg.name)
        result.daily.write_csv(RESULTS / f"{OUT_PREFIX}_{key_name}_daily.csv")
        result.fills.write_csv(RESULTS / f"{OUT_PREFIX}_{key_name}_fills.csv")
        result.trades.write_csv(RESULTS / f"{OUT_PREFIX}_{key_name}_trades.csv")
        selected.write_csv(RESULTS / f"{OUT_PREFIX}_{key_name}_target_weights.csv")
        rows.append(evaluate(cfg, result.daily, selected, result.stats, b0050, b2330))

    iter95 = load_iter95(common_start, end)
    if not iter95.is_empty():
        row = validate_daily_nav("Iter95 champion", iter95, n_trials=1)
        row.update(relative_metrics(iter95, b0050, "b0050"))
        row.update(relative_metrics(iter95, b2330, "b2330"))
        row["robust_alpha_objective"] = robust_alpha_objective(row)
        rows.append(row)
    for label, daily in [("0050 TR", b0050), ("2330 TR", b2330)]:
        rows.append(validate_daily_nav(label, daily, n_trials=1))

    summary = pl.DataFrame(rows, infer_schema_length=10_000).sort("robust_alpha_objective", descending=True)
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.write_csv(summary_path)
    log(f"[iter99] wrote {summary_path}")
    print(
        summary.select(
            [
                "name",
                "cagr",
                "oos_cagr",
                "recent_1y_cagr",
                "sortino",
                "calmar",
                "mdd",
                "dsr",
                "pbo",
                "fill_ratio",
                "b0050_final_relative_nav",
                "b2330_final_relative_nav",
                "avg_positions",
                "max_active",
            ]
        )
        .head(20)
        .to_pandas()
        .to_string(index=False),
        flush=True,
    )
    log(f"[iter99] elapsed={time.time() - t0:.1f}s")


if __name__ == "__main__":
    run()
