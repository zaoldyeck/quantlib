"""Walk-forward ML/regime research for TAIFEX index futures.

This runner deliberately separates machine-learning research from the hand-built
daily/intraday/session sleeves.  The model never trains on the OOS year it is
predicting, features are shifted by one trading day, and predicted signals are
still executed by the same costed futures simulator.
"""

from __future__ import annotations

import math
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from quantlib import paths

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
STRAT_LAB = ROOT / "strat_lab"
if str(STRAT_LAB) not in sys.path:
    sys.path.insert(0, str(STRAT_LAB))

import duckdb
import numpy as np
import polars as pl
from lightgbm import LGBMRegressor

from futures.simulator import FuturesExecutionConfig, simulate_single_product
from futures.specs import FuturesCostConfig, FuturesMarginConfig
from futures.strategies import add_common_features, load_product_frame
from futures.validation import futures_objective, multi_config_pbo, validate_futures_daily, verdict
from strat_lab.evaluation import nav_metrics
from strat_lab.validator import ValidationConfig


BASE = Path(__file__).resolve().parents[2]
DB_PATH = paths.CACHE_DB
OUT_DIR = paths.OUT_STRAT_LAB / "futures_tx_ml_regime"
DOC_PATH = BASE / "docs" / "strategy_research" / "futures_ml_regime_strategy_ranking.md"

FEATURE_COLUMNS = [
    "ret",
    "vol20",
    "vol63",
    "continuous_close",
    "ma20",
    "ma50",
    "ma120",
    "ma200",
    "roc10",
    "roc20",
    "basis_z63",
    "term_z63",
    "foreign_oi_z",
    "cash_foreign_z",
    "cash_trust_z",
    "cash_dealer_z",
    "margin_balance_chg_z",
    "short_balance_chg_z",
    "sbl_balance_chg_z",
    "foreign_holding_chg_z",
    "chip_score",
    "rsi14",
    "stoch_k14",
    "bb_z20",
    "donchian_pos55",
    "volume_z63",
    "plus_di14",
    "minus_di14",
    "adx14",
    "macd_hist",
    "ss_macdh",
    "ss_rsi",
    "ss_kdjk",
    "ss_kdjd",
    "ss_adx",
    "ss_pdi",
    "ss_ndi",
    "ss_cci",
    "ss_wr_14",
    "ss_mfi",
    "rpt_5m_total_ret_z",
    "rpt_5m_regular_ret_z",
    "rpt_5m_night_ret_z",
    "rpt_5m_first30_ret_z",
    "rpt_5m_last60_ret_z",
    "rpt_5m_rv_z",
    "rpt_5m_range_z",
    "rpt_15m_total_ret_z",
    "rpt_15m_last60_ret_z",
    "rpt_60m_total_ret_z",
]


@dataclass(frozen=True)
class MlConfig:
    name: str
    product: str
    train_years: int
    threshold_q: float
    target_vol: float
    direction: str
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


def _with_lagged_features(frame: pl.DataFrame) -> tuple[pl.DataFrame, list[str]]:
    featured = add_common_features(frame).sort("date")
    available = [col for col in FEATURE_COLUMNS if col in featured.columns]
    derived = [
        (pl.col("continuous_close") / pl.max_horizontal(pl.col("ma20"), pl.lit(1e-9)) - 1.0).alias("px_ma20"),
        (pl.col("continuous_close") / pl.max_horizontal(pl.col("ma50"), pl.lit(1e-9)) - 1.0).alias("px_ma50"),
        (pl.col("continuous_close") / pl.max_horizontal(pl.col("ma200"), pl.lit(1e-9)) - 1.0).alias("px_ma200"),
        (pl.col("open") / pl.max_horizontal(pl.col("continuous_close").shift(1), pl.lit(1e-9)) - 1.0).alias("gap_open"),
        (pl.col("close") / pl.max_horizontal(pl.col("open"), pl.lit(1e-9)) - 1.0).alias("target_ret"),
    ]
    featured = featured.with_columns(derived)
    feature_cols = available + ["px_ma20", "px_ma50", "px_ma200", "gap_open"]
    shifted = [pl.col(col).shift(1).alias(col) for col in feature_cols]
    return (
        featured.with_columns(
            shifted
            + [
                pl.col("atr14").shift(1).fill_null(pl.col("close").shift(1) * 0.01).alias("atr"),
                pl.col("date").dt.year().alias("year"),
            ]
        ).select(["date", "year", "target_ret", "atr", *feature_cols]),
        feature_cols,
    )


def _to_numpy(frame: pl.DataFrame, feature_cols: list[str]) -> tuple[np.ndarray, np.ndarray]:
    x = frame.select(feature_cols).to_numpy().astype(float)
    y = frame["target_ret"].to_numpy().astype(float)
    x[~np.isfinite(x)] = np.nan
    y[~np.isfinite(y)] = np.nan
    return x, y


def build_oos_predictions(frame: pl.DataFrame, train_years: int, feature_cols: list[str]) -> pl.DataFrame:
    dates = frame["date"].to_list()
    years = frame["year"].to_numpy()
    pred = np.zeros(frame.height, dtype=float)
    thresholds = {0.60: np.full(frame.height, np.nan), 0.70: np.full(frame.height, np.nan), 0.80: np.full(frame.height, np.nan)}
    first_year = max(2012, int(np.nanmin(years)) + train_years)
    last_year = int(np.nanmax(years))
    for test_year in range(first_year, last_year + 1):
        train_mask = (years >= test_year - train_years) & (years < test_year)
        test_mask = years == test_year
        train_frame = frame.filter(pl.Series(train_mask))
        test_frame = frame.filter(pl.Series(test_mask))
        if train_frame.height < 500 or test_frame.is_empty():
            continue
        x_train, y_train = _to_numpy(train_frame, feature_cols)
        valid = np.isfinite(y_train)
        if int(valid.sum()) < 500:
            continue
        x_train = x_train[valid]
        y_train = y_train[valid]
        train_year_vector = train_frame["year"].to_numpy()[valid]
        sample_weight = np.power(0.65, np.maximum(0, test_year - 1 - train_year_vector) / 2.0)
        model = LGBMRegressor(
            objective="regression",
            n_estimators=90,
            learning_rate=0.035,
            num_leaves=15,
            max_depth=4,
            min_child_samples=80,
            subsample=0.80,
            subsample_freq=1,
            colsample_bytree=0.70,
            reg_alpha=0.10,
            reg_lambda=1.50,
            random_state=17 + test_year + train_years,
            n_jobs=-1,
            verbose=-1,
        )
        model.fit(x_train, y_train, sample_weight=sample_weight)
        train_pred = model.predict(x_train)
        x_test, _ = _to_numpy(test_frame, feature_cols)
        test_pred = model.predict(x_test)
        idx = np.where(test_mask)[0]
        pred[idx] = test_pred
        abs_train = np.abs(train_pred[np.isfinite(train_pred)])
        for q in thresholds:
            threshold = float(np.quantile(abs_train, q)) if abs_train.size else math.inf
            thresholds[q][idx] = max(threshold, 1e-6)
    cols = {
        "date": dates,
        "pred": pred,
        "atr": frame["atr"].to_list(),
    }
    for q, values in thresholds.items():
        cols[f"thr_{int(q * 100)}"] = values
    return pl.DataFrame(cols).with_columns(pl.col("date").cast(pl.Date)).sort("date")


def _execution_config(cfg: MlConfig) -> FuturesExecutionConfig:
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
        stop_loss_atr=2.5,
        trailing_stop_atr=4.0,
        take_profit_atr=None,
        time_stop_days=30,
        time_stop_min_return=-0.004,
    )


def _targets(predictions: pl.DataFrame, cfg: MlConfig) -> pl.DataFrame:
    thr_col = f"thr_{int(cfg.threshold_q * 100)}"
    sign = -1.0 if cfg.direction == "reverse" else 1.0
    signal = (
        pl.when(pl.col("pred") > pl.col(thr_col)).then(1.0)
        .when(pl.col("pred") < -pl.col(thr_col)).then(-1.0)
        .otherwise(0.0)
        * sign
    )
    return predictions.with_columns(signal.alias("signal")).select(["date", "signal", "atr"])


def candidate_grid(cost_multiplier: float = 1.0) -> list[MlConfig]:
    configs: list[MlConfig] = []
    for product in ["TX", "MTX"]:
        for train_years in [3, 5, 8]:
            for threshold_q in [0.60, 0.70, 0.80]:
                for target_vol in [0.20, 0.35, 0.50]:
                    for direction in ["follow", "reverse"]:
                        name = f"{product}_lgbm_train{train_years}_q{int(threshold_q*100)}_tv{target_vol:g}_{direction}"
                        configs.append(
                            MlConfig(
                                name=name,
                                product=product,
                                train_years=train_years,
                                threshold_q=threshold_q,
                                target_vol=target_vol,
                                direction=direction,
                                cost_multiplier=cost_multiplier,
                            )
                        )
    return configs


def _simulate(
    frames: dict[str, pl.DataFrame],
    predictions: dict[tuple[str, int], pl.DataFrame],
    cfg: MlConfig,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, dict[str, object]]:
    frame = frames[cfg.product]
    targets = _targets(predictions[(cfg.product, cfg.train_years)], cfg)
    bars = frame.join(targets.select(["date", "atr"]), on="date", how="left").sort("date")
    result = simulate_single_product(
        bars,
        targets.select(["date", "signal"]),
        product=cfg.product,
        name=cfg.name,
        cfg=_execution_config(cfg),
    )
    return result.daily, result.fills, result.trades, result.summary


def _write_doc(summary: pl.DataFrame, daily_cutoff: str, elapsed: float) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    ranked = summary.sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True])
    passed = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    top = (passed if not passed.is_empty() else ranked).head(18).to_dicts()
    lines = [
        "# 臺指期 Walk-Forward ML/Regime 策略研究排行",
        "",
        f"資料截止：`{daily_cutoff}`。本輪使用 LightGBM walk-forward：特徵全部 lag 一天，每個 OOS 年只用先前 3/5/8 年訓練，執行時間約 `{elapsed:.1f}` 秒。",
        "",
        "## 結論",
        "",
    ]
    if passed.is_empty():
        lines += ["本輪沒有 ML/regime 候選通過嚴格 gate；不可升級為可上線策略。", ""]
    else:
        best = passed.head(1).to_dicts()[0]
        lines += [f"本輪通過 gate 的第一名是 **{best['name']}**。", ""]
    lines += [
        "| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | Boot CAGR LB | 2x Cost OOS | 5x Cost OOS |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
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
                ]
            )
            + " |"
        )
    lines += [
        "",
        "## Artifacts",
        "",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_ml_regime/ml_regime_strategy_summary.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_ml_regime/top_daily.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_ml_regime/top_trades.csv`",
    ]
    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run() -> None:
    start = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    raw_frames = {product: load_product_frame(DB_PATH, product).sort("date") for product in ["TX", "MTX"]}
    feature_frames: dict[str, pl.DataFrame] = {}
    feature_cols: dict[str, list[str]] = {}
    for product, frame in raw_frames.items():
        feature_frames[product], feature_cols[product] = _with_lagged_features(frame)
    predictions = {
        (product, train_years): build_oos_predictions(feature_frames[product], train_years, feature_cols[product])
        for product in ["TX", "MTX"]
        for train_years in [3, 5, 8]
    }
    configs = candidate_grid()
    print(f"[ml] candidates={len(configs)}", flush=True)
    rows: list[dict[str, object]] = []
    daily_by_name: dict[str, pl.DataFrame] = {}
    trades_by_name: dict[str, pl.DataFrame] = {}
    stress2: dict[str, pl.DataFrame] = {}
    stress5: dict[str, pl.DataFrame] = {}
    for idx, cfg in enumerate(configs, start=1):
        if idx == 1 or idx % 20 == 0:
            print(f"[ml] {idx}/{len(configs)} {cfg.name}", flush=True)
        daily, _fills, trades, sim_summary = _simulate(raw_frames, predictions, cfg)
        if daily.height < 500:
            continue
        s2_daily, _, _, _ = _simulate(raw_frames, predictions, replace(cfg, cost_multiplier=2.0))
        s5_daily, _, _, _ = _simulate(raw_frames, predictions, replace(cfg, cost_multiplier=5.0))
        daily_by_name[cfg.name] = daily
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

    pbo = multi_config_pbo(daily_by_name)
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
    summary.write_csv(OUT_DIR / "ml_regime_strategy_summary.csv")
    best_name = str(summary["name"][0])
    daily_by_name[best_name].write_csv(OUT_DIR / "top_daily.csv")
    trades_by_name[best_name].write_csv(OUT_DIR / "top_trades.csv")
    cutoff = max(frame["date"].max() for frame in raw_frames.values()).isoformat()
    _write_doc(summary, cutoff, time.time() - start)
    champion = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    print(f"[done] ml rows={summary.height} pbo={pbo:.3f} champion={champion['name'][0] if not champion.is_empty() else 'NONE'}")
    print(f"[artifacts] {OUT_DIR}")
    print(f"[doc] {DOC_PATH}")


if __name__ == "__main__":
    run()
