"""Public-methodology H-model research for TAIFEX index futures.

Publicly accessible descriptions of the H model identify two indicators and one
principle: a volume indicator, a futures/spot basis indicator, and leverage
control.  The exact book/course formulas are not fully public, so this runner
tests auditable variants that map directly to the public claims:

- basis: negative basis (futures below spot) is bullish; positive basis is
  bearish.
- volume: volume above its average is bullish; volume below its average is
  bearish.
- H blend: combine the two signals with basis receiving the larger weight.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from quantlib import paths

ROOT = paths.REPO
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


BASE = paths.REPO
DB_PATH = paths.CACHE_DB
OUT_DIR = paths.OUT_STRAT_LAB / "futures_tx_h_model_public"
DOC_PATH = BASE / "docs" / "strategy_research" / "futures_h_model_public_strategy_ranking.md"


@dataclass(frozen=True)
class HConfig:
    name: str
    mode: str
    basis_threshold: float
    volume_window: int
    volume_threshold: float
    basis_weight: float
    target_vol: float
    stop_profile: str
    cost_multiplier: float = 1.0


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


def _signal_frame(base: pl.DataFrame, cfg: HConfig) -> pl.DataFrame:
    frame = add_common_features(base).sort("date")
    vol_avg = pl.col("volume").ewm_mean(span=cfg.volume_window, adjust=False)
    basis_signal = (
        pl.when(pl.col("tx_spot_basis_pct") < -cfg.basis_threshold)
        .then(1.0)
        .when(pl.col("tx_spot_basis_pct") > cfg.basis_threshold)
        .then(-1.0)
        .otherwise(0.0)
    )
    volume_rel = pl.col("volume") / pl.max_horizontal(vol_avg, pl.lit(1.0)) - 1.0
    volume_signal = (
        pl.when(volume_rel > cfg.volume_threshold)
        .then(1.0)
        .when(volume_rel < -cfg.volume_threshold)
        .then(-1.0)
        .otherwise(0.0)
    )
    weighted = cfg.basis_weight * basis_signal + (1.0 - cfg.basis_weight) * volume_signal
    if cfg.mode == "basis":
        raw = basis_signal
    elif cfg.mode == "volume":
        raw = volume_signal
    elif cfg.mode == "weighted":
        raw = pl.when(weighted > 0.0).then(1.0).when(weighted < 0.0).then(-1.0).otherwise(0.0)
    elif cfg.mode == "agree":
        raw = (
            pl.when((basis_signal > 0.0) & (volume_signal > 0.0))
            .then(1.0)
            .when((basis_signal < 0.0) & (volume_signal < 0.0))
            .then(-1.0)
            .otherwise(0.0)
        )
    elif cfg.mode == "basis_dominant":
        raw = (
            pl.when((basis_signal != 0.0) & (volume_signal == -basis_signal)).then(0.0)
            .when(basis_signal != 0.0).then(basis_signal)
            .otherwise(volume_signal)
        )
    else:
        raise ValueError(f"unknown H mode: {cfg.mode}")
    return (
        frame.with_columns(
            [
                basis_signal.alias("basis_signal"),
                volume_signal.alias("volume_signal"),
                volume_rel.alias("volume_rel"),
                raw.alias("raw_signal"),
                pl.col("atr14").shift(1).fill_null(pl.col("close").shift(1) * 0.01).alias("atr"),
            ]
        )
        .with_columns(pl.col("raw_signal").shift(1).fill_null(0.0).alias("signal"))
        .select(["date", "signal", "raw_signal", "basis_signal", "volume_signal", "volume_rel", "atr"])
        .sort("date")
    )


def _execution_config(cfg: HConfig) -> FuturesExecutionConfig:
    if cfg.stop_profile == "h_no_stop":
        stop_loss_atr = None
        trailing_stop_atr = None
        time_stop_days = None
        time_stop_min_return = -1.0
    elif cfg.stop_profile == "professional_trail":
        stop_loss_atr = 2.5
        trailing_stop_atr = 4.0
        time_stop_days = 40
        time_stop_min_return = -0.005
    elif cfg.stop_profile == "wide_trail":
        stop_loss_atr = None
        trailing_stop_atr = 6.0
        time_stop_days = 60
        time_stop_min_return = -0.01
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
            max_notional_leverage=6.0,
            stress_notional_move=0.12,
        ),
        stop_loss_atr=stop_loss_atr,
        trailing_stop_atr=trailing_stop_atr,
        take_profit_atr=None,
        time_stop_days=time_stop_days,
        time_stop_min_return=time_stop_min_return,
    )


def candidate_grid(cost_multiplier: float = 1.0) -> list[HConfig]:
    configs: list[HConfig] = []
    stop_profiles = ["h_no_stop", "professional_trail", "wide_trail"]
    target_vols = [0.30, 0.45, 0.70, 1.00]
    for basis_threshold in [0.0, 0.001, 0.002, 0.005]:
        for target_vol in target_vols:
            for stop_profile in stop_profiles:
                configs.append(
                    HConfig(
                        name=f"H_basis_b{basis_threshold:g}_tv{target_vol:g}_{stop_profile}",
                        mode="basis",
                        basis_threshold=basis_threshold,
                        volume_window=60,
                        volume_threshold=0.0,
                        basis_weight=1.0,
                        target_vol=target_vol,
                        stop_profile=stop_profile,
                        cost_multiplier=cost_multiplier,
                    )
                )
    for volume_window in [20, 60, 120]:
        for volume_threshold in [0.0, 0.05, 0.10]:
            for target_vol in target_vols:
                for stop_profile in stop_profiles:
                    configs.append(
                        HConfig(
                            name=f"H_volume_w{volume_window}_v{volume_threshold:g}_tv{target_vol:g}_{stop_profile}",
                            mode="volume",
                            basis_threshold=0.0,
                            volume_window=volume_window,
                            volume_threshold=volume_threshold,
                            basis_weight=0.0,
                            target_vol=target_vol,
                            stop_profile=stop_profile,
                            cost_multiplier=cost_multiplier,
                        )
                    )
    for mode in ["weighted", "agree", "basis_dominant"]:
        for volume_window in [20, 60, 120]:
            for volume_threshold in [0.0, 0.05]:
                for basis_threshold in [0.0, 0.001, 0.002]:
                    weights = [0.65, 0.80] if mode == "weighted" else [0.80]
                    for basis_weight in weights:
                        for target_vol in target_vols:
                            for stop_profile in stop_profiles:
                                configs.append(
                                    HConfig(
                                        name=(
                                            f"H_{mode}_b{basis_threshold:g}_w{volume_window}_v{volume_threshold:g}"
                                            f"_bw{basis_weight:g}_tv{target_vol:g}_{stop_profile}"
                                        ),
                                        mode=mode,
                                        basis_threshold=basis_threshold,
                                        volume_window=volume_window,
                                        volume_threshold=volume_threshold,
                                        basis_weight=basis_weight,
                                        target_vol=target_vol,
                                        stop_profile=stop_profile,
                                        cost_multiplier=cost_multiplier,
                                    )
                                )
    return configs


def _simulate(base: pl.DataFrame, cfg: HConfig) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, dict[str, object]]:
    signal = _signal_frame(base, cfg)
    bars = base.join(signal.select(["date", "atr"]), on="date", how="left").sort("date")
    result = simulate_single_product(
        bars,
        signal.select(["date", "signal"]),
        product="TX",
        name=cfg.name,
        cfg=_execution_config(cfg),
    )
    return result.daily, result.fills, result.trades, result.summary


def _write_doc(summary: pl.DataFrame, cutoff: str, elapsed: float, pbo: float) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    ranked = summary.sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True])
    passed = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    top = (passed if not passed.is_empty() else ranked).head(18).to_dicts()
    recent_start = ranked["recent_1y_start"][0] if "recent_1y_start" in ranked.columns else "n/a"
    recent_end = ranked["recent_1y_end"][0] if "recent_1y_end" in ranked.columns else "n/a"
    lines = [
        "# 臺指期 H 模型公開方法論回測",
        "",
        f"資料截止：`{cutoff}`。最近一年視窗：`{recent_start}` 至 `{recent_end}`。本輪依公開描述實作 H 模型可審計版本：價差指標、量指標與槓桿控管。執行時間約 `{elapsed:.1f}` 秒；群組 PBO `{pbo:.3f}`。",
        "",
        "## 公開資料來源與限制",
        "",
        "- Alex Huang 課程頁明確說明 H 模型是「兩個指標加一個原理」：量指標、價差指標、槓桿原理，且價差指標比量指標重要。",
        "- 課程商品頁列出歷史資料、均量均價、量指標、價差指標、納入交易成本、凱利槓桿、多策略與量價投資組合等章節。",
        "- 博客來書頁與讀者評論顯示該模型核心被理解為期貨現貨價差與交易量混合使用，但書籍/課程完整公式與參數並非公開網頁完整揭露。",
        "- 因此本文件只驗證「公開可審計 H-like 方法」是否仍有穩健 alpha；不能宣稱已完整複製作者付費課程或書中全部專有版本。",
        "",
        "## 公開方法論映射",
        "",
        "- 價差指標：期貨正價差偏空、逆價差偏多。",
        "- 量指標：成交量高於均量偏多、低於均量偏空。",
        "- H blend：用 basis-only、volume-only、weighted、agree、basis-dominant 五類候選檢驗兩個指標是否互補。",
        "- 槓桿：不使用無限制槓桿；由 target volatility 與保證金 survival constraint 共同決定口數。",
        "",
        "## 結論",
        "",
    ]
    if passed.is_empty():
        lines += ["本輪沒有候選通過嚴格 gate；H 模型公開可審計版本目前不能升級為可上線臺指期策略。", ""]
    else:
        best = passed.head(1).to_dicts()[0]
        lines += [f"本輪通過 gate 的第一名是 **{best['name']}**。", ""]
    lines += [
        "| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | Boot CAGR LB | 2x Cost OOS | 5x Cost OOS | Trades |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
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
                    _format_num(row.get("trade_count"), 0),
                ]
            )
            + " |"
        )
    lines += [
        "",
        "## Artifacts",
        "",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_h_model_public/h_model_public_summary.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_h_model_public/top_daily.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_h_model_public/top_trades.csv`",
    ]
    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run() -> None:
    start = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    frame = load_product_frame(DB_PATH, "TX").sort("date")
    configs = candidate_grid()
    print(f"[h-public] candidates={len(configs)}", flush=True)
    rows: list[dict[str, object]] = []
    dailies: dict[str, pl.DataFrame] = {}
    trades_by_name: dict[str, pl.DataFrame] = {}
    stress2: dict[str, pl.DataFrame] = {}
    stress5: dict[str, pl.DataFrame] = {}
    for idx, cfg in enumerate(configs, start=1):
        if idx == 1 or idx % 50 == 0:
            print(f"[h-public] {idx}/{len(configs)} {cfg.name}", flush=True)
        daily, _fills, trades, sim_summary = _simulate(frame, cfg)
        if daily.height < 500:
            continue
        s2_daily, _, _, _ = _simulate(frame, replace(cfg, cost_multiplier=2.0))
        s5_daily, _, _, _ = _simulate(frame, replace(cfg, cost_multiplier=5.0))
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
        row["stress_2x_oos_cagr"] = nav_metrics(stress2[name].filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"]), prefix="oos_")["oos_cagr"]
        row["stress_5x_oos_cagr"] = nav_metrics(stress5[name].filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"]), prefix="oos_")["oos_cagr"]
        row["verdict"] = verdict(row)
        row["objective"] = futures_objective(row)
        final_rows.append(row)
    summary = pl.DataFrame(final_rows).sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True])
    summary.write_csv(OUT_DIR / "h_model_public_summary.csv")
    best_name = str(summary["name"][0])
    dailies[best_name].write_csv(OUT_DIR / "top_daily.csv")
    trades_by_name[best_name].write_csv(OUT_DIR / "top_trades.csv")
    cutoff = frame["date"].max().isoformat()
    _write_doc(summary, cutoff, time.time() - start, pbo)
    champion = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    print(f"[done] h-public rows={summary.height} pbo={pbo:.3f} champion={champion['name'][0] if not champion.is_empty() else 'NONE'}")
    print(f"[artifacts] {OUT_DIR}")
    print(f"[doc] {DOC_PATH}")


if __name__ == "__main__":
    run()
