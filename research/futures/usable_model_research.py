"""Regime-adaptive usable-model research for TAIFEX index futures.

The earlier broad sweeps mostly tested binary sleeves.  This runner tests a
small set of continuous-exposure models designed from first principles:

- Trend sleeve when the tape is directional.
- Mean-reversion sleeve when the tape is range-bound.
- Flow/basis overlays as modest confirmations, not stand-alone predictors.
- Volatility de-risking before the simulator decides contracts.

All signals are shifted one trading day and executed by the same realistic
futures simulator used by the rest of the futures research stack.
"""

from __future__ import annotations

import math
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
STRAT_LAB = ROOT / "strat_lab"
if str(STRAT_LAB) not in sys.path:
    sys.path.insert(0, str(STRAT_LAB))

import numpy as np
import polars as pl

from futures.simulator import FuturesExecutionConfig, simulate_single_product
from futures.specs import FuturesCostConfig, FuturesMarginConfig
from futures.strategies import add_common_features, load_product_frame
from futures.validation import futures_objective, multi_config_pbo, validate_futures_daily, verdict
from strat_lab.evaluation import nav_metrics
from strat_lab.validator import ValidationConfig


BASE = Path(__file__).resolve().parents[2]
DB_PATH = BASE / "research" / "cache.duckdb"
OUT_DIR = BASE / "research" / "strat_lab" / "results" / "futures_tx_usable_model"
DOC_PATH = BASE / "docs" / "strategy_research" / "futures_usable_model_strategy_ranking.md"


@dataclass(frozen=True)
class UsableConfig:
    name: str
    product: str
    mode: str
    target_vol: float
    trend_weight: float
    reversion_weight: float
    flow_weight: float
    basis_weight: float
    activation: float
    vol_cap: float
    stop_profile: str
    cost_multiplier: float = 1.0


def _clip_expr(expr: pl.Expr, lo: float = -1.0, hi: float = 1.0) -> pl.Expr:
    return expr.clip(lo, hi)


def _format_pct(value: object) -> str:
    try:
        return f"{float(value):+.2%}"
    except Exception:
        return "n/a"


def _format_num(value: object, digits: int = 3) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "n/a"


def _signal_frame(base: pl.DataFrame, cfg: UsableConfig) -> pl.DataFrame:
    f = add_common_features(base).sort("date")
    px = pl.col("continuous_close")
    ret5 = px / px.shift(5) - 1.0
    ret20 = px / px.shift(20) - 1.0
    ret60 = px / px.shift(60) - 1.0
    ret120 = px / px.shift(120) - 1.0
    vol20 = pl.max_horizontal(pl.col("vol20"), pl.lit(0.004))
    vol63 = pl.max_horizontal(pl.col("vol63"), pl.lit(0.004))

    trend_core = _clip_expr(
        0.20 * ret20 / vol20
        + 0.25 * ret60 / vol63
        + 0.20 * ret120 / vol63
        + 0.18 * ((pl.col("ma50") / pl.max_horizontal(pl.col("ma200"), pl.lit(1e-9))) - 1.0) / vol63
        + 0.17 * pl.col("macd_hist") / pl.max_horizontal(pl.col("continuous_close") * vol20, pl.lit(1.0)),
        -2.0,
        2.0,
    )
    breakout_core = (
        pl.when(pl.col("donchian_pos55") > 0.65).then(1.0)
        .when(pl.col("donchian_pos55") < -0.65).then(-1.0)
        .otherwise(0.0)
    )
    trend_score = _clip_expr(0.70 * trend_core + 0.30 * breakout_core, -1.0, 1.0)

    range_regime = (pl.col("adx14").fill_null(0.0) < 20.0) & (vol20 <= vol63 * 1.15)
    trend_regime = (pl.col("adx14").fill_null(0.0) >= 18.0) | ((pl.col("ma50") - pl.col("ma200")).abs() / px > 0.015)
    reversion_core = _clip_expr(-0.55 * pl.col("bb_z20").fill_null(0.0) - 0.45 * (pl.col("stoch_k14").fill_null(50.0) - 50.0) / 35.0)
    reversion_score = pl.when(range_regime).then(reversion_core).otherwise(0.0)

    flow_score = _clip_expr(
        0.45 * pl.col("chip_score").fill_null(0.0)
        + 0.25 * pl.col("foreign_oi_z").fill_null(0.0)
        + 0.15 * pl.col("cash_foreign_z").fill_null(0.0)
        + 0.15 * pl.col("cash_trust_z").fill_null(0.0),
        -1.0,
        1.0,
    )
    basis_score = _clip_expr(-pl.col("basis_z63").fill_null(0.0) / 2.0)

    if cfg.mode == "adaptive":
        raw_score = (
            cfg.trend_weight * pl.when(trend_regime).then(trend_score).otherwise(0.0)
            + cfg.reversion_weight * reversion_score
            + cfg.flow_weight * flow_score
            + cfg.basis_weight * basis_score
        )
    elif cfg.mode == "trend_flow":
        raw_score = (
            cfg.trend_weight * trend_score
            + cfg.flow_weight * flow_score
            + cfg.basis_weight * basis_score
            - cfg.reversion_weight * pl.when((ret5 > 2.2 * vol20) & (pl.col("rsi14") > 70)).then(1.0)
              .when((ret5 < -2.2 * vol20) & (pl.col("rsi14") < 30)).then(-1.0)
              .otherwise(0.0)
        )
    elif cfg.mode == "range_breakout":
        raw_score = (
            cfg.trend_weight * pl.when(trend_regime & (pl.col("volume_z63") > -0.5)).then(trend_score).otherwise(0.0)
            + cfg.reversion_weight * reversion_score
            + cfg.flow_weight * flow_score
        )
    else:
        raise ValueError(f"unknown mode: {cfg.mode}")

    active = raw_score.abs() >= cfg.activation
    vol_scale = (
        pl.when(vol20 <= cfg.vol_cap * vol63).then(1.0)
        .when(vol20 <= (cfg.vol_cap + 0.35) * vol63).then(0.5)
        .otherwise(0.0)
    )
    raw_signal = pl.when(active).then(_clip_expr(raw_score) * vol_scale).otherwise(0.0)
    return (
        f.with_columns(
            [
                raw_signal.alias("raw_signal"),
                pl.col("atr14").shift(1).fill_null(pl.col("close").shift(1) * 0.01).alias("atr"),
            ]
        )
        .with_columns(pl.col("raw_signal").shift(1).fill_null(0.0).alias("signal"))
        .select(["date", "signal", "raw_signal", "atr"])
        .sort("date")
    )


def _execution_config(cfg: UsableConfig) -> FuturesExecutionConfig:
    if cfg.stop_profile == "balanced":
        stop_loss_atr = 2.5
        trailing_stop_atr = 4.5
        time_stop_days = 35
        time_stop_min_return = -0.004
    elif cfg.stop_profile == "wide":
        stop_loss_atr = None
        trailing_stop_atr = 6.0
        time_stop_days = 55
        time_stop_min_return = -0.008
    elif cfg.stop_profile == "fast":
        stop_loss_atr = 1.8
        trailing_stop_atr = 3.0
        time_stop_days = 20
        time_stop_min_return = -0.002
    else:
        raise ValueError(f"unknown stop profile: {cfg.stop_profile}")
    return FuturesExecutionConfig(
        target_vol=cfg.target_vol,
        cost=FuturesCostConfig(cost_multiplier=cfg.cost_multiplier),
        margin=FuturesMarginConfig(
            initial_margin_ratio=0.135,
            maintenance_margin_ratio=0.105,
            required_buffer=1.35,
            liquidation_buffer=1.00,
            max_notional_leverage=5.0,
            stress_notional_move=0.12,
        ),
        stop_loss_atr=stop_loss_atr,
        trailing_stop_atr=trailing_stop_atr,
        take_profit_atr=None,
        time_stop_days=time_stop_days,
        time_stop_min_return=time_stop_min_return,
    )


def candidate_grid(cost_multiplier: float = 1.0) -> list[UsableConfig]:
    configs: list[UsableConfig] = []
    mode_params = {
        "adaptive": [(0.65, 0.35, 0.20, 0.10), (0.75, 0.25, 0.25, 0.05), (0.55, 0.45, 0.20, 0.05)],
        "trend_flow": [(0.75, 0.15, 0.30, 0.05), (0.85, 0.10, 0.20, 0.05)],
        "range_breakout": [(0.55, 0.45, 0.20, 0.00), (0.70, 0.30, 0.25, 0.00)],
    }
    for product in ["TX", "MTX"]:
        for mode, weights in mode_params.items():
            for trend_w, rev_w, flow_w, basis_w in weights:
                for target_vol in [0.25, 0.35, 0.50]:
                    for activation in [0.20, 0.35]:
                        for vol_cap in [1.25, 1.60]:
                            for stop_profile in ["balanced", "wide", "fast"]:
                                name = (
                                    f"{product}_usable_{mode}_tw{trend_w:g}_rw{rev_w:g}_fw{flow_w:g}"
                                    f"_bw{basis_w:g}_tv{target_vol:g}_a{activation:g}_vc{vol_cap:g}_{stop_profile}"
                                )
                                configs.append(
                                    UsableConfig(
                                        name=name,
                                        product=product,
                                        mode=mode,
                                        target_vol=target_vol,
                                        trend_weight=trend_w,
                                        reversion_weight=rev_w,
                                        flow_weight=flow_w,
                                        basis_weight=basis_w,
                                        activation=activation,
                                        vol_cap=vol_cap,
                                        stop_profile=stop_profile,
                                        cost_multiplier=cost_multiplier,
                                    )
                                )
    return configs


def _simulate(base: pl.DataFrame, cfg: UsableConfig) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, dict[str, object]]:
    signal = _signal_frame(base, cfg)
    bars = base.join(signal.select(["date", "atr"]), on="date", how="left").sort("date")
    result = simulate_single_product(
        bars,
        signal.select(["date", "signal"]),
        product=cfg.product,
        name=cfg.name,
        cfg=_execution_config(cfg),
    )
    return result.daily, result.fills, result.trades, result.summary


def _write_doc(summary: pl.DataFrame, cutoff: str, elapsed: float, pbo: float) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    ranked = summary.sort(["verdict", "objective", "oos_cagr"], descending=[False, True, True])
    passed = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    diagnostic = summary.sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True])
    top = (passed if not passed.is_empty() else diagnostic).head(18).to_dicts()
    recent_start = diagnostic["recent_1y_start"][0] if "recent_1y_start" in diagnostic.columns else "n/a"
    recent_end = diagnostic["recent_1y_end"][0] if "recent_1y_end" in diagnostic.columns else "n/a"
    lines = [
        "# 臺指期 Regime-Adaptive 可用模型研究",
        "",
        f"資料截止：`{cutoff}`。最近一年視窗：`{recent_start}` 至 `{recent_end}`。本輪測試少參數、連續曝險的 regime-adaptive 模型；執行時間約 `{elapsed:.1f}` 秒；群組 PBO `{pbo:.3f}`。",
        "",
        "## 模型邏輯",
        "",
        "- Trend sleeve：多週期 momentum、MA slope、MACD、Donchian 位置，只有趨勢/方向性足夠時才主導。",
        "- Mean-reversion sleeve：ADX 較低且波動未失控時，用 Bollinger z-score 與 stochastic 反向交易。",
        "- 籌碼 overlay：期貨外資 OI、現貨外資/投信/自營商與整體 chip_score 只作確認，不讓三年資料成為長期主模型。",
        "- Basis overlay：期現價差只給小權重，避免重演 H 模型對 basis 過度依賴。",
        "- Volatility de-risk：短波動相對長波動過熱時降曝險或空手，再交給 simulator 做保證金 survival sizing。",
        "",
        "## 結論",
        "",
    ]
    if passed.is_empty():
        lines.append("本輪沒有候選通過嚴格 gate；此模型族群暫時不能升級為可上線臺指期策略。")
    else:
        best = passed.head(1).to_dicts()[0]
        lines.append(f"本輪通過 gate 的第一名是 **{best['name']}**。")
    lines += [
        "",
        "| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | Boot CAGR LB | 2x Cost OOS | 5x Cost OOS | PF | SQN | Trades |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(top, start=1):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(idx),
                    str(row.get("name")),
                    str(row.get("verdict")),
                    _format_pct(row.get("cagr")),
                    _format_pct(row.get("oos_cagr")),
                    _format_pct(row.get("recent_1y_cagr")),
                    _format_pct(row.get("ret_6m")),
                    _format_pct(row.get("ret_3m")),
                    _format_pct(row.get("ret_1m")),
                    _format_pct(row.get("oos_mdd")),
                    _format_num(row.get("oos_sortino")),
                    _format_num(row.get("dsr")),
                    _format_num(row.get("pbo")),
                    _format_pct(row.get("boot_cagr_lb")),
                    _format_pct(row.get("stress_2x_oos_cagr")),
                    _format_pct(row.get("stress_5x_oos_cagr")),
                    _format_num(row.get("profit_factor")),
                    _format_num(row.get("sqn")),
                    _format_num(row.get("trade_count"), 0),
                ]
            )
            + " |"
        )
    lines += [
        "",
        "## Artifacts",
        "",
        "- `research/strat_lab/results/futures_tx_usable_model/usable_model_summary.csv`",
        "- `research/strat_lab/results/futures_tx_usable_model/top_daily.csv`",
        "- `research/strat_lab/results/futures_tx_usable_model/top_trades.csv`",
    ]
    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run() -> None:
    start = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    configs = candidate_grid()
    products = sorted({cfg.product for cfg in configs})
    frames = {product: load_product_frame(DB_PATH, product).sort("date") for product in products}
    print(f"[usable] candidates={len(configs)} products={products}", flush=True)
    rows: list[dict[str, object]] = []
    dailies: dict[str, pl.DataFrame] = {}
    trades_by_name: dict[str, pl.DataFrame] = {}
    stress2: dict[str, pl.DataFrame] = {}
    stress5: dict[str, pl.DataFrame] = {}
    for idx, cfg in enumerate(configs, start=1):
        if idx == 1 or idx % 50 == 0:
            print(f"[usable] {idx}/{len(configs)} {cfg.name}", flush=True)
        daily, _fills, trades, sim_summary = _simulate(frames[cfg.product], cfg)
        if daily.height < 500:
            continue
        s2_daily, _, _, _ = _simulate(frames[cfg.product], replace(cfg, cost_multiplier=2.0))
        s5_daily, _, _, _ = _simulate(frames[cfg.product], replace(cfg, cost_multiplier=5.0))
        dailies[cfg.name] = daily
        trades_by_name[cfg.name] = trades
        stress2[cfg.name] = s2_daily
        stress5[cfg.name] = s5_daily
        rows.append(
            validate_futures_daily(
                cfg.name,
                daily,
                trades=trades,
                simulator_summary=sim_summary,
                n_trials=len(configs),
                config=ValidationConfig(oos_start_year=2012, oos_end_year=2026, min_trials_for_dsr=len(configs)),
            )
        )
    pbo = multi_config_pbo(dailies)
    final_rows = []
    for row in rows:
        name = str(row["name"])
        row["pbo"] = pbo
        row["stress_2x_oos_cagr"] = nav_metrics(
            stress2[name].filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"]),
            prefix="oos_",
        )["oos_cagr"]
        row["stress_5x_oos_cagr"] = nav_metrics(
            stress5[name].filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"]),
            prefix="oos_",
        )["oos_cagr"]
        row["verdict"] = verdict(row)
        row["objective"] = futures_objective(row)
        final_rows.append(row)
    summary = pl.DataFrame(final_rows).sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True])
    summary.write_csv(OUT_DIR / "usable_model_summary.csv")
    best_name = str(summary["name"][0])
    dailies[best_name].write_csv(OUT_DIR / "top_daily.csv")
    trades_by_name[best_name].write_csv(OUT_DIR / "top_trades.csv")
    cutoff = max(frame["date"].max() for frame in frames.values()).isoformat()
    _write_doc(summary, cutoff, time.time() - start, pbo)
    champion = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    print(f"[done] usable rows={summary.height} pbo={pbo:.3f} champion={champion['name'][0] if not champion.is_empty() else 'NONE'}")
    print(f"[artifacts] {OUT_DIR}")
    print(f"[doc] {DOC_PATH}")


if __name__ == "__main__":
    run()
