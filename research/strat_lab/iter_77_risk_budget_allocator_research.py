"""iter_77 - risk-budgeted PM allocator research.

This pass keeps the Iter75 dynamic-industry sleeves as the alpha source, then
tests whether a lagged risk-budget allocator can reduce drawdown without
destroying OOS growth.  It is NAV-level research only; execution-ready status
still requires target-book reconciliation across underlying stock holdings.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))
from pm_allocator import (  # noqa: E402
    RiskBudgetAllocatorConfig,
    Sleeve,
    build_return_panel,
)
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
CAPITAL = 1_000_000.0
TDPY = 252


class LookbackState:
    def __init__(
        self,
        log_ret: np.ndarray,
        vol: np.ndarray,
        current_dd: np.ndarray,
        max_dd: np.ndarray,
        cov: np.ndarray,
        observations: np.ndarray,
    ) -> None:
        self.log_ret = log_ret
        self.vol = vol
        self.current_dd = current_dd
        self.max_dd = max_dd
        self.cov = cov
        self.observations = observations


def load_top_sleeves(summary_path: Path, top_n: int) -> list[Sleeve]:
    summary = (
        pl.read_csv(summary_path, try_parse_dates=True)
        .sort(["robust_growth_score", "oos_log_cagr", "oos_cagr"], descending=[True, True, True])
        .head(top_n)
    )
    sleeves = []
    for row in summary.iter_rows(named=True):
        name = row["name"]
        path = RESULTS / f"{name}_daily.csv"
        if path.exists():
            sleeves.append(Sleeve(name=name, daily_path=path))
    if len(sleeves) < 2:
        raise RuntimeError("need at least two sleeve daily files for allocator research")
    return sleeves


def build_configs() -> list[RiskBudgetAllocatorConfig]:
    configs: list[RiskBudgetAllocatorConfig] = []
    for lookback in (21, 42, 63):
        for top_k in (1, 2, 3):
            for vol_penalty in (0.0, 0.5, 1.0):
                for current_dd_penalty in (0.5, 2.0, 4.0):
                    for max_dd_penalty in (0.0, 0.5):
                        for target_vol in (None, 0.20, 0.30):
                            for max_sleeve_weight in (0.50, 0.70):
                                configs.append(
                                    RiskBudgetAllocatorConfig(
                                        lookback_days=lookback,
                                        top_k=top_k,
                                        min_history_days=min(21, lookback),
                                        min_score=-0.05,
                                        vol_penalty=vol_penalty,
                                        current_dd_penalty=current_dd_penalty,
                                        max_dd_penalty=max_dd_penalty,
                                        target_vol=target_vol,
                                        max_gross=1.0,
                                        max_sleeve_weight=max_sleeve_weight,
                                        cash_drawdown_limit=None,
                                        cash_when_no_positive=True,
                                    )
                                )
    return configs


def _cumsum0(arr: np.ndarray) -> np.ndarray:
    zeros = np.zeros((1, *arr.shape[1:]), dtype=float)
    return np.concatenate([zeros, np.cumsum(arr, axis=0)], axis=0)


def precompute_lookback_state(rets: np.ndarray, lookback: int) -> LookbackState:
    n, m = rets.shape
    safe_rets = np.nan_to_num(rets, nan=0.0, posinf=0.0, neginf=0.0)
    safe_logs = np.log1p(np.clip(safe_rets, -0.999, None))
    cum_log = _cumsum0(safe_logs)
    cum_ret = _cumsum0(safe_rets)
    cum_ret2 = _cumsum0(safe_rets**2)
    cross = np.einsum("ti,tj->tij", safe_rets, safe_rets)
    cum_cross = _cumsum0(cross)

    log_ret = np.full((n, m), np.nan, dtype=float)
    vol = np.full((n, m), np.nan, dtype=float)
    cov = np.full((n, m, m), np.nan, dtype=float)
    current_dd = np.full((n, m), np.nan, dtype=float)
    max_dd = np.full((n, m), np.nan, dtype=float)
    observations = np.zeros(n, dtype=int)

    for t in range(n):
        start = max(0, t - lookback)
        count = t - start
        observations[t] = count
        if count < 2:
            continue
        s1 = cum_ret[t] - cum_ret[start]
        s2 = cum_ret2[t] - cum_ret2[start]
        var = np.maximum((s2 - (s1 * s1 / count)) / (count - 1), 0.0)
        log_ret[t] = cum_log[t] - cum_log[start]
        vol[t] = np.sqrt(var) * np.sqrt(TDPY)

        sx = cum_cross[t] - cum_cross[start]
        cov[t] = (sx - np.outer(s1, s1) / count) / (count - 1)

        window = safe_rets[start:t]
        wealth = np.cumprod(1.0 + window, axis=0)
        peaks = np.maximum.accumulate(wealth, axis=0)
        dd = wealth / np.where(peaks == 0, np.nan, peaks) - 1.0
        current_dd[t] = dd[-1]
        max_dd[t] = np.nanmin(dd, axis=0)

    return LookbackState(log_ret, vol, current_dd, max_dd, cov, observations)


def precompute_states(rets: np.ndarray, configs: list[RiskBudgetAllocatorConfig]) -> dict[int, LookbackState]:
    return {
        lookback: precompute_lookback_state(rets, lookback)
        for lookback in sorted({cfg.lookback_days for cfg in configs})
    }


def simulate_fast_config(
    dates: list[object],
    names: list[str],
    rets: np.ndarray,
    state: LookbackState,
    cfg: RiskBudgetAllocatorConfig,
    *,
    name: str,
    n_trials: int,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, object]]:
    n, m = rets.shape
    weights = np.zeros((n, m), dtype=float)
    for t in range(n):
        if state.observations[t] < cfg.min_history_days:
            continue
        scores = (
            state.log_ret[t]
            - cfg.vol_penalty * np.nan_to_num(state.vol[t], nan=10.0, posinf=10.0)
            - cfg.current_dd_penalty * np.abs(np.nan_to_num(state.current_dd[t], nan=-1.0))
            - cfg.max_dd_penalty * np.abs(np.nan_to_num(state.max_dd[t], nan=-1.0))
        )
        valid = np.where(np.isfinite(scores) & (scores >= cfg.min_score))[0]
        if valid.size == 0:
            if cfg.cash_when_no_positive:
                continue
            valid = np.arange(m)

        selected = valid[np.argsort(scores[valid])[::-1][: cfg.top_k]]
        if selected.size == 0:
            continue

        row_w = np.zeros(m, dtype=float)
        row_w[selected] = min(cfg.max_sleeve_weight, cfg.max_gross / selected.size)
        gross = row_w.sum()
        if gross > cfg.max_gross > 0:
            row_w *= cfg.max_gross / gross

        if cfg.cash_drawdown_limit is not None:
            start = max(0, t - cfg.lookback_days)
            selected_window = rets[start:t] @ row_w
            if selected_window.size:
                port_wealth = np.cumprod(1.0 + selected_window)
                port_peak = np.maximum.accumulate(port_wealth)
                port_current_dd = port_wealth[-1] / port_peak[-1] - 1.0 if port_peak[-1] > 0 else -1.0
                if port_current_dd <= -abs(cfg.cash_drawdown_limit):
                    continue

        if cfg.target_vol is not None and cfg.target_vol > 0:
            row_cov = np.nan_to_num(state.cov[t], nan=0.0, posinf=0.0, neginf=0.0)
            port_var = float(row_w @ row_cov @ row_w) * TDPY
            port_vol = np.sqrt(max(port_var, 0.0))
            if port_vol > cfg.target_vol:
                row_w *= cfg.target_vol / port_vol

        weights[t] = row_w

    port_rets = np.sum(np.nan_to_num(rets, nan=0.0) * weights, axis=1)
    daily = pl.DataFrame({"date": dates, "nav": CAPITAL * np.cumprod(1.0 + port_rets)})
    weights_df = pl.DataFrame(
        {"date": dates, **{f"{sleeve}__weight": weights[:, i] for i, sleeve in enumerate(names)}}
    )
    metrics = validate_daily_nav(name, daily, n_trials=n_trials)
    return daily, weights_df, metrics


def weight_stats(weights: pl.DataFrame) -> dict[str, float]:
    weight_cols = [c for c in weights.columns if c.endswith("__weight")]
    if not weight_cols:
        return {"avg_gross": 0.0, "max_gross": 0.0, "active_days": 0.0, "avg_active_sleeves": 0.0}
    gross = pl.sum_horizontal([pl.col(c) for c in weight_cols]).alias("gross")
    active = pl.sum_horizontal([(pl.col(c) > 0).cast(pl.Float64) for c in weight_cols]).alias("active")
    stats = weights.select([gross, active])
    return {
        "avg_gross": float(stats["gross"].mean()),
        "max_gross": float(stats["gross"].max()),
        "active_days": float((stats["gross"] > 0).sum()),
        "avg_active_sleeves": float(stats["active"].mean()),
    }


def run(summary_path: Path, top_n: int) -> None:
    sleeves = load_top_sleeves(summary_path, top_n)
    configs = build_configs()
    ret_panel = build_return_panel(sleeves)
    names = [c for c in ret_panel.columns if c != "date"]
    dates = ret_panel["date"].to_list()
    rets = ret_panel.select(names).to_numpy().astype(float)
    states = precompute_states(rets, configs)
    rows = []
    best: tuple[dict[str, object], pl.DataFrame, pl.DataFrame] | None = None
    for i, cfg in enumerate(configs, 1):
        target_vol = "none" if cfg.target_vol is None else f"{cfg.target_vol:.2f}"
        name = (
            f"iter77_rb_top{top_n}_lb{cfg.lookback_days}_k{cfg.top_k}"
            f"_vp{cfg.vol_penalty:g}_cdd{cfg.current_dd_penalty:g}"
            f"_mdd{cfg.max_dd_penalty:g}_tv{target_vol}_mw{cfg.max_sleeve_weight:g}"
        )
        daily, weights, metrics = simulate_fast_config(
            dates,
            names,
            rets,
            states[cfg.lookback_days],
            cfg,
            name=name,
            n_trials=len(configs),
        )
        row = {
            "name": name,
            "top_n_sleeves": top_n,
            "config_index": i,
            **asdict(cfg),
            **weight_stats(weights),
            **metrics,
        }
        rows.append(row)
        if best is None or float(row["robust_growth_score"]) > float(best[0]["robust_growth_score"]):
            best = (row, daily, weights)
        if i % 100 == 0 or i == len(configs):
            print(f"[iter77] {i}/{len(configs)} best_score={best[0]['robust_growth_score']:.6g}", flush=True)

    out = pl.DataFrame(rows).sort(
        ["robust_growth_score", "oos_log_cagr", "oos_cagr"],
        descending=[True, True, True],
    )
    out_path = RESULTS / "iter_77_risk_budget_allocator_summary.csv"
    out.write_csv(out_path)
    (RESULTS / "iter_77_risk_budget_allocator_sleeves.json").write_text(
        json.dumps([s.name for s in sleeves], ensure_ascii=False, indent=2)
    )
    if best is not None:
        best_name = str(best[0]["name"])
        best[1].write_csv(RESULTS / f"{best_name}_daily.csv")
        best[2].write_csv(RESULTS / f"{best_name}_weights.csv")

    cols = [
        "name",
        "oos_cagr",
        "recent_1y_cagr",
        "oos_sortino",
        "oos_mdd",
        "oos_cdar_95",
        "oos_ulcer_index",
        "oos_k_ratio",
        "robust_growth_score",
        "boot_cagr_lb",
        "dsr",
        "pbo",
        "avg_gross",
        "avg_active_sleeves",
    ]
    print(out.select([c for c in cols if c in out.columns]).head(20).to_pandas().to_string(index=False))
    print(f"\nSaved: {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", type=Path, default=RESULTS / "iter_75_dynamic_industry_summary.csv")
    ap.add_argument("--top-n", type=int, default=20)
    args = ap.parse_args()
    run(args.summary, args.top_n)


if __name__ == "__main__":
    main()
