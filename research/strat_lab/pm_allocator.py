"""Portfolio-manager allocator framework for multi-strategy sleeve research.

This layer allocates capital across already-validated sleeve NAV streams using
only lagged, observable information.  It is a research allocator; promotion to
execution-ready still requires target-book reconciliation across underlying
stock holdings so the total portfolio respects the 10-stock limit.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

from evaluation import CAPITAL_DEFAULT, nav_metrics
from validator import validate_daily_nav


@dataclass(frozen=True)
class Sleeve:
    name: str
    daily_path: Path


@dataclass(frozen=True)
class MomentumAllocatorConfig:
    lookback_days: int = 63
    top_k: int = 1
    min_score: float = 0.0
    vol_penalty: float = 0.50
    max_sleeve_weight: float = 1.0
    cash_when_no_positive: bool = True


@dataclass(frozen=True)
class RiskBudgetAllocatorConfig:
    lookback_days: int = 63
    top_k: int = 2
    min_history_days: int = 21
    min_score: float = 0.0
    vol_penalty: float = 0.50
    current_dd_penalty: float = 1.0
    max_dd_penalty: float = 0.50
    target_vol: float | None = 0.25
    max_gross: float = 1.0
    max_sleeve_weight: float = 0.70
    cash_drawdown_limit: float | None = None
    cash_when_no_positive: bool = True


def load_nav(path: Path, name: str) -> pl.DataFrame:
    df = pl.read_csv(path, try_parse_dates=True).select(["date", "nav"]).sort("date")
    return df.with_columns(
        [
            (pl.col("nav") / pl.col("nav").shift(1) - 1).fill_null(0.0).alias(name),
        ]
    ).select(["date", name])


def build_return_panel(sleeves: list[Sleeve]) -> pl.DataFrame:
    if not sleeves:
        raise ValueError("at least one sleeve is required")
    panel = load_nav(sleeves[0].daily_path, sleeves[0].name)
    for sleeve in sleeves[1:]:
        panel = panel.join(load_nav(sleeve.daily_path, sleeve.name), on="date", how="inner")
    return panel.sort("date")


def lagged_momentum_allocations(
    ret_panel: pl.DataFrame,
    cfg: MomentumAllocatorConfig = MomentumAllocatorConfig(),
) -> pl.DataFrame:
    """Create lagged sleeve weights from trailing return minus volatility.

    Scores at date t are based on returns through t-1 and applied to t returns.
    """
    if cfg.top_k < 1:
        raise ValueError("top_k must be >= 1")
    names = [c for c in ret_panel.columns if c != "date"]
    if not names:
        raise ValueError("return panel has no sleeves")

    exprs = []
    for name in names:
        trailing_ret = pl.col(name).log1p().rolling_sum(cfg.lookback_days).shift(1).exp() - 1
        trailing_vol = pl.col(name).rolling_std(cfg.lookback_days).shift(1) * np.sqrt(252)
        exprs.append((trailing_ret - cfg.vol_penalty * trailing_vol).alias(f"{name}__score"))
    scored = ret_panel.select(["date", *names]).with_columns(exprs)

    rows = []
    for row in scored.iter_rows(named=True):
        scores = {name: row.get(f"{name}__score") for name in names}
        valid = {
            name: float(score)
            for name, score in scores.items()
            if score is not None and np.isfinite(float(score)) and float(score) >= cfg.min_score
        }
        weights = {name: 0.0 for name in names}
        if valid:
            selected = sorted(valid, key=valid.get, reverse=True)[: cfg.top_k]
            raw_weight = min(cfg.max_sleeve_weight, 1.0 / len(selected))
            for name in selected:
                weights[name] = raw_weight
        elif not cfg.cash_when_no_positive:
            for name in names:
                weights[name] = min(cfg.max_sleeve_weight, 1.0 / len(names))
        rows.append({"date": row["date"], **{f"{name}__weight": weights[name] for name in names}})
    return pl.DataFrame(rows).sort("date")


def risk_budget_allocations(
    ret_panel: pl.DataFrame,
    cfg: RiskBudgetAllocatorConfig = RiskBudgetAllocatorConfig(),
) -> pl.DataFrame:
    """Create lagged risk-budgeted weights from trailing sleeve diagnostics.

    Scores and volatility estimates at date t use only returns through t-1.
    Gross exposure is optionally scaled down to a target annualized volatility.
    """
    if cfg.top_k < 1:
        raise ValueError("top_k must be >= 1")
    if cfg.lookback_days < 2:
        raise ValueError("lookback_days must be >= 2")
    names = [c for c in ret_panel.columns if c != "date"]
    if not names:
        raise ValueError("return panel has no sleeves")

    ordered = ret_panel.sort("date")
    dates = ordered["date"].to_list()
    rets = ordered.select(names).to_numpy().astype(float)
    n, m = rets.shape
    out = np.zeros((n, m), dtype=float)

    for t in range(n):
        start = max(0, t - cfg.lookback_days)
        window = rets[start:t]
        if window.shape[0] < cfg.min_history_days:
            continue

        log_window = np.log1p(np.clip(window, -0.999, None))
        trailing_log_ret = np.sum(log_window, axis=0)
        trailing_vol = np.std(window, axis=0, ddof=1) * np.sqrt(252)

        wealth = np.cumprod(1.0 + window, axis=0)
        peaks = np.maximum.accumulate(wealth, axis=0)
        dd = wealth / np.where(peaks == 0, np.nan, peaks) - 1.0
        current_dd = dd[-1]
        max_dd = np.nanmin(dd, axis=0)

        scores = (
            trailing_log_ret
            - cfg.vol_penalty * np.nan_to_num(trailing_vol, nan=10.0, posinf=10.0)
            - cfg.current_dd_penalty * np.abs(np.nan_to_num(current_dd, nan=-1.0))
            - cfg.max_dd_penalty * np.abs(np.nan_to_num(max_dd, nan=-1.0))
        )
        valid = np.where(np.isfinite(scores) & (scores >= cfg.min_score))[0]
        if valid.size == 0:
            if cfg.cash_when_no_positive:
                continue
            valid = np.arange(m)

        selected = valid[np.argsort(scores[valid])[::-1][: cfg.top_k]]
        if selected.size == 0:
            continue

        weights = np.zeros(m, dtype=float)
        base_weight = min(cfg.max_sleeve_weight, cfg.max_gross / selected.size)
        weights[selected] = base_weight
        gross = weights.sum()
        if gross > cfg.max_gross > 0:
            weights *= cfg.max_gross / gross

        selected_window = window @ weights
        if cfg.cash_drawdown_limit is not None:
            port_wealth = np.cumprod(1.0 + selected_window)
            port_peak = np.maximum.accumulate(port_wealth)
            port_current_dd = port_wealth[-1] / port_peak[-1] - 1.0 if port_peak[-1] > 0 else -1.0
            if port_current_dd <= -abs(cfg.cash_drawdown_limit):
                continue

        if cfg.target_vol is not None and cfg.target_vol > 0:
            port_vol = float(np.std(selected_window, ddof=1) * np.sqrt(252)) if selected_window.size > 1 else 0.0
            if port_vol > cfg.target_vol:
                weights *= cfg.target_vol / port_vol

        out[t] = weights

    return pl.DataFrame(
        {"date": dates, **{f"{name}__weight": out[:, i] for i, name in enumerate(names)}}
    ).sort("date")


def simulate_allocator_nav(
    ret_panel: pl.DataFrame,
    weights: pl.DataFrame,
    *,
    capital: float = CAPITAL_DEFAULT,
    name: str = "pm_allocator",
    n_trials: int = 1,
) -> tuple[pl.DataFrame, dict[str, object]]:
    names = [c for c in ret_panel.columns if c != "date"]
    joined = ret_panel.join(weights, on="date", how="inner").sort("date")
    sleeve_ret_exprs = [pl.col(s) * pl.col(f"{s}__weight") for s in names]
    daily = (
        joined.with_columns(pl.sum_horizontal(sleeve_ret_exprs).alias("ret"))
        .with_columns((capital * (1 + pl.col("ret")).cum_prod()).alias("nav"))
        .select(["date", "nav"])
    )
    metrics = validate_daily_nav(name, daily, n_trials=n_trials)
    metrics.update(nav_metrics(daily, capital=capital, prefix="pm_"))
    return daily, metrics


def run_momentum_allocator(
    sleeves: list[Sleeve],
    cfg: MomentumAllocatorConfig = MomentumAllocatorConfig(),
    *,
    capital: float = CAPITAL_DEFAULT,
    name: str = "pm_momentum_allocator",
    n_trials: int = 1,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, object]]:
    ret_panel = build_return_panel(sleeves)
    weights = lagged_momentum_allocations(ret_panel, cfg)
    daily, metrics = simulate_allocator_nav(ret_panel, weights, capital=capital, name=name, n_trials=n_trials)
    return daily, weights, metrics


def run_risk_budget_allocator(
    sleeves: list[Sleeve],
    cfg: RiskBudgetAllocatorConfig = RiskBudgetAllocatorConfig(),
    *,
    capital: float = CAPITAL_DEFAULT,
    name: str = "pm_risk_budget_allocator",
    n_trials: int = 1,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, object]]:
    ret_panel = build_return_panel(sleeves)
    weights = risk_budget_allocations(ret_panel, cfg)
    daily, metrics = simulate_allocator_nav(ret_panel, weights, capital=capital, name=name, n_trials=n_trials)
    return daily, weights, metrics
