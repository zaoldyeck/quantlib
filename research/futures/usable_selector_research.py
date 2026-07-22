"""Walk-forward selector for the regime-adaptive futures model family."""

from __future__ import annotations

import math
import sys
import time
from dataclasses import replace
from pathlib import Path
from research import paths

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
STRAT_LAB = ROOT / "strat_lab"
if str(STRAT_LAB) not in sys.path:
    sys.path.insert(0, str(STRAT_LAB))

import numpy as np
import polars as pl

from futures.usable_model_research import DB_PATH, OUT_DIR as USABLE_OUT_DIR, UsableConfig, _simulate, candidate_grid
from futures.strategies import load_product_frame
from futures.validation import futures_objective, multi_config_pbo, validate_futures_daily, verdict
from strat_lab.evaluation import CAPITAL_DEFAULT, nav_metrics
from strat_lab.validator import ValidationConfig


BASE = Path(__file__).resolve().parents[2]
OUT_DIR = paths.OUT_STRAT_LAB / "futures_tx_usable_selector"
DOC_PATH = BASE / "docs" / "strategy_research" / "futures_usable_selector_strategy_ranking.md"


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


def _read_top_names(n: int = 28) -> list[str]:
    path = USABLE_OUT_DIR / "usable_model_summary.csv"
    if not path.exists():
        raise FileNotFoundError(f"missing usable model summary: {path}")
    df = pl.read_csv(path, try_parse_dates=True)
    filtered = df.filter(
        (pl.col("oos_mdd").abs() <= 0.46)
        & (pl.col("stress_2x_oos_cagr") > 0.0)
        & (pl.col("stress_5x_oos_cagr") > 0.0)
        & (pl.col("profit_factor") > 1.15)
    )
    if filtered.is_empty():
        filtered = df
    return (
        filtered.sort(["oos_cagr", "recent_1y_cagr", "oos_mdd"], descending=[True, True, True])
        .head(n)["name"]
        .to_list()
    )


def _returns_panel(daily_by_name: dict[str, pl.DataFrame]) -> tuple[list[object], list[str], np.ndarray]:
    names = sorted(daily_by_name)
    panel: pl.DataFrame | None = None
    for name in names:
        part = (
            daily_by_name[name]
            .select(["date", "nav"])
            .sort("date")
            .with_columns((pl.col("nav") / pl.col("nav").shift(1) - 1.0).fill_null(0.0).alias(name))
            .select(["date", name])
        )
        panel = part if panel is None else panel.join(part, on="date", how="inner")
    if panel is None:
        raise ValueError("no daily streams")
    panel = panel.sort("date")
    return panel["date"].to_list(), names, panel.select(names).to_numpy().astype(float)


def lagged_selector_from_panel(
    dates: list[object],
    names: list[str],
    rets: np.ndarray,
    *,
    lookback: int,
    top_k: int,
    target_vol: float,
    max_scale: float,
    min_score: float,
    dd_cut: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    n, m = rets.shape
    weights = np.zeros((n, m), dtype=float)
    for i in range(n):
        hist = rets[max(0, i - lookback):i]
        if hist.shape[0] < max(63, lookback // 3):
            continue
        safe = np.clip(hist, -0.999, None)
        log_ret = np.sum(np.log1p(safe), axis=0)
        vol = np.std(hist, axis=0, ddof=1) * math.sqrt(252.0)
        wealth = np.cumprod(1.0 + hist, axis=0)
        dd = wealth / np.maximum.accumulate(wealth, axis=0) - 1.0
        worst_dd = np.min(dd, axis=0)
        recent = np.sum(np.log1p(safe[-min(126, hist.shape[0]):]), axis=0)
        score = 0.65 * log_ret + 0.35 * recent - 0.25 * vol - 0.75 * np.abs(worst_dd)
        valid = np.where(np.isfinite(score) & (score >= min_score) & (worst_dd >= -abs(dd_cut)))[0]
        if valid.size == 0:
            continue
        selected = valid[np.argsort(score[valid])[::-1][:top_k]]
        inv_vol = 1.0 / np.maximum(vol[selected], 1e-6)
        raw = np.zeros(m, dtype=float)
        raw[selected] = inv_vol / inv_vol.sum()
        port_hist = hist @ raw
        port_vol = float(np.std(port_hist, ddof=1) * math.sqrt(252.0)) if port_hist.size > 2 else 0.0
        scale = min(max_scale, target_vol / port_vol) if port_vol > 0 else 1.0
        weights[i] = raw * scale
    port_rets = np.sum(rets * weights, axis=1)
    daily = pl.DataFrame({"date": dates, "nav": CAPITAL_DEFAULT * np.cumprod(1.0 + port_rets), "ret": port_rets})
    weight_df = pl.DataFrame({"date": dates, **{f"{sleeve}__weight": weights[:, j] for j, sleeve in enumerate(names)}})
    return daily, weight_df


def lagged_selector(
    daily_by_name: dict[str, pl.DataFrame],
    *,
    name: str,
    lookback: int,
    top_k: int,
    target_vol: float,
    max_scale: float,
    min_score: float,
    dd_cut: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    dates, names, rets = _returns_panel(daily_by_name)
    return lagged_selector_from_panel(
        dates,
        names,
        rets,
        lookback=lookback,
        top_k=top_k,
        target_vol=target_vol,
        max_scale=max_scale,
        min_score=min_score,
        dd_cut=dd_cut,
    )


def guard_grid() -> list[dict[str, object]]:
    return [
        {"guard_lb": 0, "guard_min_log_return": -999.0, "guard_dd": 1.0, "guard_vol_target": 1.0},
        {"guard_lb": 63, "guard_min_log_return": -0.02, "guard_dd": 0.12, "guard_vol_target": 0.35},
        {"guard_lb": 126, "guard_min_log_return": 0.00, "guard_dd": 0.18, "guard_vol_target": 0.35},
        {"guard_lb": 252, "guard_min_log_return": 0.00, "guard_dd": 0.25, "guard_vol_target": 0.40},
    ]


def base_selector_grid() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for lookback in [252, 504, 756]:
        for top_k in [1, 2, 3]:
            for target_vol in [0.25, 0.35, 0.50]:
                for min_score in [-0.10, -0.02, 0.03]:
                    rows.append(
                        {
                            "lookback": lookback,
                            "top_k": top_k,
                            "target_vol": target_vol,
                            "max_scale": 1.0,
                            "min_score": min_score,
                            "dd_cut": 0.40,
                        }
                    )
    return rows


def selector_grid() -> list[dict[str, object]]:
    return [{**base, **guard} for base in base_selector_grid() for guard in guard_grid()]


def apply_equity_guard(
    daily: pl.DataFrame,
    *,
    guard_lb: int,
    min_log_return: float,
    max_dd: float,
    vol_target: float,
    max_scale: float = 1.0,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    ordered = daily.select(["date", "nav"]).sort("date")
    dates = ordered["date"].to_list()
    nav = ordered["nav"].to_numpy().astype(float)
    prev = np.concatenate([[CAPITAL_DEFAULT], nav[:-1]])
    rets = np.divide(nav - prev, prev, out=np.zeros_like(nav), where=prev != 0)
    if guard_lb <= 0:
        weight = np.ones_like(rets)
        guarded = pl.DataFrame({"date": dates, "nav": CAPITAL_DEFAULT * np.cumprod(1.0 + rets), "ret": rets})
        return guarded, pl.DataFrame({"date": dates, "guard_weight": weight})

    weight = np.zeros_like(rets)
    for i in range(len(rets)):
        hist = rets[max(0, i - guard_lb):i]
        if hist.size < max(21, guard_lb // 3):
            continue
        safe = np.clip(hist, -0.999, None)
        log_ret = float(np.sum(np.log1p(safe)))
        wealth = np.cumprod(1.0 + hist)
        dd = wealth / np.maximum.accumulate(wealth) - 1.0
        worst_dd = float(np.min(dd)) if dd.size else 0.0
        if log_ret < min_log_return or worst_dd < -abs(max_dd):
            continue
        vol = float(np.std(hist, ddof=1) * math.sqrt(252.0)) if hist.size > 2 else 0.0
        weight[i] = min(max_scale, vol_target / vol) if vol > 0 else 1.0
    guarded_rets = rets * weight
    guarded = pl.DataFrame({"date": dates, "nav": CAPITAL_DEFAULT * np.cumprod(1.0 + guarded_rets), "ret": guarded_rets})
    return guarded, pl.DataFrame({"date": dates, "guard_weight": weight})


def daily_from_existing_weights(
    panel: tuple[list[object], list[str], np.ndarray],
    weight: pl.DataFrame,
) -> pl.DataFrame:
    dates, names, rets = panel
    ordered_weight = weight.select(["date", *[f"{sleeve}__weight" for sleeve in names]]).sort("date")
    if ordered_weight["date"].to_list() != dates:
        weight_dates = pl.DataFrame({"date": dates}).join(ordered_weight, on="date", how="left").fill_null(0.0)
        weights = weight_dates.select([f"{sleeve}__weight" for sleeve in names]).to_numpy().astype(float)
    else:
        weights = ordered_weight.select([f"{sleeve}__weight" for sleeve in names]).to_numpy().astype(float)
    port_rets = np.sum(rets * weights, axis=1)
    return pl.DataFrame({"date": dates, "nav": CAPITAL_DEFAULT * np.cumprod(1.0 + port_rets), "ret": port_rets})


def apply_guard_weight_to_daily(daily: pl.DataFrame, guard_weight: pl.DataFrame) -> pl.DataFrame:
    ordered = daily.select(["date", "nav"]).sort("date")
    dates = ordered["date"].to_list()
    nav = ordered["nav"].to_numpy().astype(float)
    prev = np.concatenate([[CAPITAL_DEFAULT], nav[:-1]])
    rets = np.divide(nav - prev, prev, out=np.zeros_like(nav), where=prev != 0)
    weights = (
        pl.DataFrame({"date": dates})
        .join(guard_weight.select(["date", "guard_weight"]), on="date", how="left")
        .fill_null(0.0)["guard_weight"]
        .to_numpy()
        .astype(float)
    )
    guarded_rets = rets * weights
    return pl.DataFrame({"date": dates, "nav": CAPITAL_DEFAULT * np.cumprod(1.0 + guarded_rets), "ret": guarded_rets})


def _write_doc(summary: pl.DataFrame, cutoff: str, elapsed: float, pbo: float) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    passed = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    ranked = (passed if not passed.is_empty() else summary.sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True])).head(18)
    first = ranked.to_dicts()[0]
    lines = [
        "# 臺指期 Regime-Adaptive Walk-Forward Selector",
        "",
        f"資料截止：`{cutoff}`。本輪從上一輪 regime-adaptive 模型中選出候選池，再用 lagged walk-forward selector 決定當日可用 sleeve 與權重，並額外加入只看過去 NAV 的 portfolio-level equity-curve guard；執行時間約 `{elapsed:.1f}` 秒；群組 PBO `{pbo:.3f}`。",
        "",
        "## 結論",
        "",
    ]
    if passed.is_empty():
        lines.append("本輪仍無候選通過嚴格 gate；目前只能視為研究診斷，不能升級為可上線臺指期策略。")
    else:
        lines.append(f"本輪通過 gate 的第一名是 **{first['name']}**。")
    lines += [
        "",
        "| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | Boot CAGR LB | 2x Cost OOS | 5x Cost OOS |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(ranked.to_dicts(), start=1):
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
        "## 方法",
        "",
        "- Selector 權重只使用當日以前的 sleeve NAV、波動與 drawdown，不使用當日或未來績效。",
        "- Portfolio-level equity-curve guard 也只看組合自身過去 NAV；資金曲線不穩時降低曝險或空手。",
        "- 2x / 5x 成本壓力使用同一套 base selector 權重與 guard 權重套到高成本 sleeve returns；不允許在壓力情境重新選權重。",
        "- 目前 gate 同時要求 DSR、PBO、bootstrap、成本壓力、OOS MDD 與保證金條件通過；未通過者只列為研究診斷。",
        "",
        "## Artifacts",
        "",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_usable_selector/usable_selector_summary.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_usable_selector/top_daily.csv`",
        f"- `{paths.OUT_STRAT_LAB}/futures_tx_usable_selector/top_weights.csv`",
    ]
    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run() -> None:
    start = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    selected_names = _read_top_names()
    configs_by_name: dict[str, UsableConfig] = {cfg.name: cfg for cfg in candidate_grid()}
    configs = [configs_by_name[name] for name in selected_names if name in configs_by_name]
    products = sorted({cfg.product for cfg in configs})
    frames = {product: load_product_frame(DB_PATH, product).sort("date") for product in products}
    print(f"[selector] sleeves={len(configs)} products={products}", flush=True)
    daily_by_name: dict[str, pl.DataFrame] = {}
    stress2_by_name: dict[str, pl.DataFrame] = {}
    stress5_by_name: dict[str, pl.DataFrame] = {}
    for idx, cfg in enumerate(configs, start=1):
        print(f"[selector] sleeve {idx}/{len(configs)} {cfg.name}", flush=True)
        daily, _, _, _ = _simulate(frames[cfg.product], cfg)
        stress2, _, _, _ = _simulate(frames[cfg.product], replace(cfg, cost_multiplier=2.0))
        stress5, _, _, _ = _simulate(frames[cfg.product], replace(cfg, cost_multiplier=5.0))
        daily_by_name[cfg.name] = daily
        stress2_by_name[cfg.name] = stress2
        stress5_by_name[cfg.name] = stress5

    rows: list[dict[str, object]] = []
    dailies: dict[str, pl.DataFrame] = {}
    weights: dict[str, pl.DataFrame] = {}
    stress2_dailies: dict[str, pl.DataFrame] = {}
    stress5_dailies: dict[str, pl.DataFrame] = {}
    selectors = selector_grid()
    base_selectors = base_selector_grid()
    guards = guard_grid()
    panel = _returns_panel(daily_by_name)
    stress2_panel = _returns_panel(stress2_by_name)
    stress5_panel = _returns_panel(stress5_by_name)
    idx = 0
    for base_params in base_selectors:
        base_daily, base_weight = lagged_selector_from_panel(*panel, **base_params)
        # Cost stress must use the exact same lagged sleeve weights selected under
        # normal costs. Re-selecting weights inside the stressed panels would test
        # a different strategy and can make the stress result non-monotonic.
        base_s2_daily = daily_from_existing_weights(stress2_panel, base_weight)
        base_s5_daily = daily_from_existing_weights(stress5_panel, base_weight)
        for guard in guards:
            idx += 1
            guard_lb = int(guard["guard_lb"])
            guard_min = float(guard["guard_min_log_return"])
            guard_dd = float(guard["guard_dd"])
            guard_tv = float(guard["guard_vol_target"])
            guard_token = "noguard" if guard_lb <= 0 else f"g{guard_lb}_min{guard_min:g}_dd{guard_dd:g}_gtv{guard_tv:g}"
            name = (
                f"USABLE_WF_lb{base_params['lookback']}_top{base_params['top_k']}"
                f"_tv{base_params['target_vol']}_min{base_params['min_score']}_dd{base_params['dd_cut']}_{guard_token}"
            )
            print(f"[selector] portfolio {idx}/{len(selectors)} {name}", flush=True)
            daily, guard_weight = apply_equity_guard(
                base_daily,
                guard_lb=guard_lb,
                min_log_return=guard_min,
                max_dd=guard_dd,
                vol_target=guard_tv,
            )
            weight = base_weight.join(guard_weight, on="date", how="left")
            s2_daily = apply_guard_weight_to_daily(base_s2_daily, guard_weight)
            s5_daily = apply_guard_weight_to_daily(base_s5_daily, guard_weight)
            dailies[name] = daily
            weights[name] = weight
            stress2_dailies[name] = s2_daily
            stress5_dailies[name] = s5_daily
            rows.append(
                validate_futures_daily(
                    name,
                    daily,
                    n_trials=len(selectors),
                    config=ValidationConfig(oos_start_year=2012, oos_end_year=2026, min_trials_for_dsr=len(selectors)),
                )
            )
    pbo = multi_config_pbo(dailies)
    final_rows: list[dict[str, object]] = []
    for row in rows:
        name = str(row["name"])
        row["pbo"] = pbo
        row["stress_2x_oos_cagr"] = nav_metrics(
            stress2_dailies[name].filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"]),
            prefix="oos_",
        )["oos_cagr"]
        row["stress_5x_oos_cagr"] = nav_metrics(
            stress5_dailies[name].filter((pl.col("date").dt.year() >= 2012) & (pl.col("date").dt.year() <= 2026)).select(["date", "nav"]),
            prefix="oos_",
        )["oos_cagr"]
        row["min_margin_buffer"] = 999.0
        row["margin_breach"] = False
        row["verdict"] = verdict(row)
        row["objective"] = futures_objective(row)
        final_rows.append(row)
    summary = pl.DataFrame(final_rows).sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True])
    summary.write_csv(OUT_DIR / "usable_selector_summary.csv")
    best_name = str(summary["name"][0])
    dailies[best_name].write_csv(OUT_DIR / "top_daily.csv")
    weights[best_name].write_csv(OUT_DIR / "top_weights.csv")
    cutoff = max(frame["date"].max() for frame in frames.values()).isoformat()
    _write_doc(summary, cutoff, time.time() - start, pbo)
    champion = summary.filter(pl.col("verdict") == "pass").sort("objective", descending=True)
    print(f"[done] selector rows={summary.height} pbo={pbo:.3f} champion={champion['name'][0] if not champion.is_empty() else 'NONE'}")
    print(f"[artifacts] {OUT_DIR}")
    print(f"[doc] {DOC_PATH}")


if __name__ == "__main__":
    run()
