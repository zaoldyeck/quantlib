"""Validation helpers for futures strategy research."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping

import numpy as np
import polars as pl

from strat_lab.evaluation import CAPITAL_DEFAULT, nav_metrics, trade_distribution_metrics
from strat_lab.validator import ValidationConfig, recent_one_year_metrics, validate_daily_nav


@dataclass(frozen=True)
class FuturesValidationGate:
    min_dsr: float = 0.95
    max_pbo: float = 0.20
    max_oos_mdd: float = 0.45
    min_boot_cagr_lb: float = 0.0
    min_cost_stress_oos_cagr: float = 0.0
    min_margin_buffer: float = 1.0


def add_recent_window_returns(row: dict[str, object], daily: pl.DataFrame, capital: float = CAPITAL_DEFAULT) -> dict[str, object]:
    ordered = daily.sort("date")
    dates = ordered["date"].to_list()
    nav = ordered["nav"].to_numpy().astype(float)
    if not dates:
        row.update({"ret_6m": 0.0, "ret_3m": 0.0, "ret_1m": 0.0})
        return row
    end = dates[-1]
    lookup = dict(zip(dates, nav, strict=True))
    for label, days in [("ret_6m", 183), ("ret_3m", 92), ("ret_1m", 31)]:
        anchor_ord = end.toordinal() - days
        candidates = [d for d in dates if d.toordinal() <= anchor_ord]
        start = candidates[-1] if candidates else dates[0]
        base = lookup.get(start, capital)
        row[label] = lookup[end] / base - 1.0 if base > 0 else -1.0
    return row


def validate_futures_daily(
    name: str,
    daily: pl.DataFrame,
    *,
    trades: pl.DataFrame | None = None,
    simulator_summary: Mapping[str, object] | None = None,
    n_trials: int = 1,
    group_pbo: float | None = None,
    config: ValidationConfig | None = None,
) -> dict[str, object]:
    cfg = config or ValidationConfig(oos_start_year=2010, oos_end_year=2026, min_trials_for_dsr=max(66, n_trials))
    row = validate_daily_nav(name, daily.select(["date", "nav"]), n_trials=n_trials, config=cfg)
    row = add_recent_window_returns(row, daily)
    if group_pbo is not None:
        row["pbo"] = group_pbo
    if simulator_summary:
        row.update({k: v for k, v in simulator_summary.items() if k not in {"name"}})
    if trades is not None and not trades.is_empty() and "pnl" in trades.columns:
        row.update(trade_distribution_metrics(trades["pnl"].to_list()))
    else:
        row.update({"profit_factor": 0.0, "sqn": 0.0, "trade_count": float(row.get("trade_count", 0.0) or 0.0)})
    row["passed_margin"] = not bool(row.get("margin_breach", False)) and float(row.get("min_margin_buffer", 999.0) or 0.0) >= 1.0
    return row


def _returns(daily: pl.DataFrame, name: str) -> pl.DataFrame:
    return (
        daily.select(["date", "nav"])
        .sort("date")
        .with_columns((pl.col("nav") / pl.col("nav").shift(1) - 1.0).fill_null(0.0).alias(name))
        .select(["date", name])
    )


def multi_config_pbo(daily_by_name: dict[str, pl.DataFrame], *, n_splits: int = 500, seed: int = 42) -> float:
    names = sorted(daily_by_name)
    if len(names) < 4:
        return 0.5
    panel = _returns(daily_by_name[names[0]], names[0])
    for name in names[1:]:
        panel = panel.join(_returns(daily_by_name[name], name), on="date", how="inner")
    panel = panel.with_columns(pl.col("date").dt.year().alias("year"))
    years = sorted(y for y in panel["year"].unique().to_list() if y is not None and 2010 <= int(y) <= 2026)
    if len(years) < 6:
        return 0.5

    matrix = panel.select(names).to_numpy().astype(float)
    year_values = panel["year"].to_numpy()
    year_idx = {int(year): np.where(year_values == int(year))[0] for year in years}
    year_counts = np.asarray([len(year_idx[int(year)]) for year in years], dtype=float)
    log_sums = []
    neg_counts = []
    neg_sums = []
    neg_sumsq = []
    for year in years:
        arr = matrix[year_idx[int(year)]]
        safe = np.clip(arr, -0.999999, None)
        log_sums.append(np.log1p(safe).sum(axis=0))
        neg = np.where(arr < 0.0, arr, np.nan)
        count = np.sum(np.isfinite(neg), axis=0)
        values = np.nan_to_num(neg, nan=0.0)
        neg_counts.append(count)
        neg_sums.append(values.sum(axis=0))
        neg_sumsq.append((values * values).sum(axis=0))
    log_sums_arr = np.vstack(log_sums)
    neg_counts_arr = np.vstack(neg_counts).astype(float)
    neg_sums_arr = np.vstack(neg_sums)
    neg_sumsq_arr = np.vstack(neg_sumsq)

    def split_sortino(indices: np.ndarray) -> np.ndarray:
        count = float(year_counts[indices].sum())
        if count <= 1.0:
            return np.zeros(len(names), dtype=float)
        log_sum = log_sums_arr[indices].sum(axis=0)
        cagr = np.exp(log_sum * (252.0 / count)) - 1.0
        n_neg = neg_counts_arr[indices].sum(axis=0)
        s_neg = neg_sums_arr[indices].sum(axis=0)
        ss_neg = neg_sumsq_arr[indices].sum(axis=0)
        variance = np.where(n_neg > 1.0, (ss_neg - (s_neg * s_neg) / np.maximum(n_neg, 1.0)) / np.maximum(n_neg - 1.0, 1.0), 0.0)
        downvol = np.sqrt(np.maximum(variance, 0.0)) * np.sqrt(252.0)
        return np.where(downvol > 1e-12, (cagr - 0.01) / downvol, 0.0)

    rng = np.random.default_rng(seed)
    below = 0
    trials = 0
    half = max(2, len(years) // 2)
    for _ in range(n_splits):
        perm = rng.permutation(len(years))
        is_scores = split_sortino(perm[:half])
        oos_scores = split_sortino(perm[half:])
        best = int(np.nanargmax(is_scores))
        oos = np.asarray(oos_scores, dtype=float)
        if not np.isfinite(oos[best]):
            continue
        rank = int(np.sum(oos <= oos[best]))
        if rank <= len(names) // 2:
            below += 1
        trials += 1
    return below / trials if trials else 0.5


def verdict(row: Mapping[str, object], gate: FuturesValidationGate = FuturesValidationGate()) -> str:
    if int(float(row.get("oos_days", 0.0) or 0.0)) < 252:
        return "reject_oos_coverage"
    if bool(row.get("margin_breach", False)):
        return "reject_margin_breach"
    if float(row.get("min_margin_buffer", 0.0) or 0.0) < gate.min_margin_buffer:
        return "reject_margin_buffer"
    if abs(float(row.get("oos_mdd", 0.0) or 0.0)) > gate.max_oos_mdd:
        return "reject_mdd"
    if float(row.get("dsr", 0.0) or 0.0) < gate.min_dsr:
        return "reject_dsr"
    if float(row.get("pbo", 0.5) or 0.5) > gate.max_pbo:
        return "reject_pbo"
    if float(row.get("boot_cagr_lb", -1.0) or -1.0) <= gate.min_boot_cagr_lb:
        return "reject_bootstrap"
    if float(row.get("stress_2x_oos_cagr", 0.0) or 0.0) <= gate.min_cost_stress_oos_cagr:
        return "reject_cost_stress"
    return "pass"


def futures_objective(row: Mapping[str, object]) -> float:
    if str(row.get("verdict", "")).startswith("reject"):
        return -999.0
    oos = float(row.get("oos_log_cagr", row.get("oos_cagr", 0.0)) or 0.0)
    recent = float(row.get("recent_1y_cagr", 0.0) or 0.0)
    sortino = max(0.0, min(float(row.get("oos_sortino", 0.0) or 0.0) / 2.0, 1.0))
    calmar = max(0.0, min(float(row.get("oos_calmar", 0.0) or 0.0) / 1.0, 1.0))
    k_ratio = max(0.0, min(float(row.get("oos_k_ratio", 0.0) or 0.0) / 3.0, 1.0))
    mdd_penalty = max(0.05, 1.0 - abs(float(row.get("oos_mdd", 0.0) or 0.0)))
    pbo_penalty = max(0.05, 1.0 - float(row.get("pbo", 0.5) or 0.5))
    return (0.70 * oos + 0.30 * np.log1p(max(recent, -0.95))) * sortino * calmar * k_ratio * mdd_penalty * pbo_penalty


def daily_nav_from_returns(dates: list[date], rets: np.ndarray, capital: float = CAPITAL_DEFAULT) -> pl.DataFrame:
    nav = capital * np.cumprod(1.0 + np.asarray(rets, dtype=float))
    return pl.DataFrame({"date": dates, "nav": nav})
