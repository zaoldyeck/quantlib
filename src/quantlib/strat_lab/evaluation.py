"""Shared strategy evaluation metrics and robust-growth objective.

The objective here is deliberately not a single magic ratio.  It keeps OOS
geometric growth as the primary target, then applies bounded penalties for
drawdown pain, unstable equity curves, bad tails, and overfit diagnostics.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Iterable, Mapping

import empyrical as ep  # 學理正解基準:Sharpe/Sortino 一律走它,不手刻(2026-07-23 稽核)
import numpy as np
import polars as pl


CAPITAL_DEFAULT = 1_000_000.0
RF_DEFAULT = 0.01
TDPY = 252


def _finite(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _years(dates: list[date], n_returns: int) -> float:
    if len(dates) >= 2:
        return max((dates[-1] - dates[0]).days / 365.25, n_returns / TDPY, 1e-9)
    return max(n_returns / TDPY, 1e-9)


def nav_returns(nav: np.ndarray, capital: float = CAPITAL_DEFAULT) -> np.ndarray:
    """Return daily returns aligned to a daily NAV series without mutating input."""
    nav = np.asarray(nav, dtype=float)
    if nav.size == 0:
        return np.array([], dtype=float)
    prev = np.concatenate([[capital], nav[:-1]])
    return np.divide(nav - prev, prev, out=np.zeros_like(nav), where=prev != 0)


def drawdown_series(nav: np.ndarray, capital: float = CAPITAL_DEFAULT) -> np.ndarray:
    """Return percentage drawdowns including the initial capital anchor."""
    nav = np.asarray(nav, dtype=float)
    if nav.size == 0:
        return np.array([], dtype=float)
    anchored = np.concatenate([[capital], nav])
    peaks = np.maximum.accumulate(anchored)
    dd = np.divide(anchored - peaks, peaks, out=np.zeros_like(anchored), where=peaks != 0)
    return dd[1:]


def k_ratio(nav: np.ndarray, dates: list[date], capital: float = CAPITAL_DEFAULT) -> float:
    """Equity-curve slope t-stat on log NAV.

    This is a stricter replacement for visually pleasing R^2: it rewards both
    positive slope and stability of that slope.
    """
    nav = np.asarray(nav, dtype=float)
    if nav.size < 4 or len(dates) != nav.size or np.any(nav <= 0):
        return 0.0
    x = np.array([(d - dates[0]).days / 365.25 for d in dates], dtype=float)
    if np.allclose(x, x[0]):
        return 0.0
    y = np.log(nav / capital)
    x_centered = x - x.mean()
    denom = float(np.sum(x_centered**2))
    if denom <= 0:
        return 0.0
    slope = float(np.sum(x_centered * (y - y.mean())) / denom)
    intercept = float(y.mean() - slope * x.mean())
    resid = y - (slope * x + intercept)
    dof = max(len(y) - 2, 1)
    sigma2 = float(np.sum(resid**2) / dof)
    if sigma2 <= 0:
        return 10.0 if slope > 0 else -10.0
    se_slope = math.sqrt(sigma2 / denom)
    return slope / se_slope if se_slope > 0 else 0.0


def nav_metrics(
    daily: pl.DataFrame,
    *,
    capital: float = CAPITAL_DEFAULT,
    rf: float = RF_DEFAULT,
    date_col: str = "date",
    nav_col: str = "nav",
    prefix: str = "",
) -> dict[str, float]:
    """Compute growth, drawdown, path-quality, and tail metrics from daily NAV."""
    if daily.is_empty():
        return {
            f"{prefix}cagr": 0.0,
            f"{prefix}log_cagr": 0.0,
            f"{prefix}sortino": 0.0,
            f"{prefix}sharpe": 0.0,
            f"{prefix}mdd": 0.0,
            f"{prefix}calmar": 0.0,
            f"{prefix}ulcer_index": 0.0,
            f"{prefix}upi": 0.0,
            f"{prefix}cdar_95": 0.0,
            f"{prefix}tail_ratio": 0.0,
            f"{prefix}k_ratio": 0.0,
        }

    ordered = daily.sort(date_col)
    dates = ordered[date_col].to_list()
    nav = ordered[nav_col].to_numpy().astype(float)
    rets = nav_returns(nav, capital)
    yrs = _years(dates, len(rets))
    ending_ratio = nav[-1] / capital if nav[-1] > 0 and capital > 0 else 0.0
    log_cagr = math.log(ending_ratio) / yrs if ending_ratio > 0 else -math.inf
    cagr = math.exp(log_cagr) - 1 if math.isfinite(log_cagr) else -1.0

    vol = float(np.std(rets, ddof=1) * math.sqrt(TDPY)) if rets.size > 1 else 0.0
    # Sharpe/Sortino 一律走 empyrical(學理正解;2026-07-23 稽核 D-metrics 修):
    # 舊版 Sharpe 分子用幾何 CAGR(波動拖累使其低估約 4 成)、Sortino 下行差用「只取
    # 負報酬對自身均值 ddof=1」(非 sqrt(mean(min(r−MAR,0)²)) 對全期取平均、MAR 錨),
    # 同序列給出的值差達 3 倍。rf 以日頻超額餵入(annual/TDPY),年化因子 = TDPY。
    rf_daily = rf / TDPY
    sharpe = _finite(ep.sharpe_ratio(rets, risk_free=rf_daily, annualization=TDPY)) if rets.size > 1 else 0.0
    sortino = _finite(ep.sortino_ratio(rets, required_return=rf_daily, annualization=TDPY)) if rets.size > 1 else 0.0
    dd = drawdown_series(nav, capital)
    mdd = float(np.min(dd)) if dd.size else 0.0
    ulcer = float(math.sqrt(np.mean(np.square(np.minimum(dd, 0.0))))) if dd.size else 0.0
    q05 = float(np.quantile(dd, 0.05)) if dd.size else 0.0
    worst_dd = dd[dd <= q05] if dd.size else np.array([], dtype=float)
    cdar = float(abs(np.mean(worst_dd))) if worst_dd.size else abs(mdd)
    p95 = float(np.quantile(rets, 0.95)) if rets.size else 0.0
    p05 = float(np.quantile(rets, 0.05)) if rets.size else 0.0
    tail = p95 / abs(p05) if p05 < 0 else (math.inf if p95 > 0 else 0.0)

    return {
        f"{prefix}cagr": float(cagr),
        f"{prefix}log_cagr": float(log_cagr) if math.isfinite(log_cagr) else -1.0,
        f"{prefix}sortino": sortino,
        f"{prefix}sharpe": sharpe,
        f"{prefix}mdd": mdd,
        f"{prefix}calmar": float(cagr / abs(mdd)) if mdd < 0 else 0.0,
        f"{prefix}ulcer_index": ulcer,
        f"{prefix}upi": float((cagr - rf) / ulcer) if ulcer > 0 else 0.0,
        f"{prefix}cdar_95": cdar,
        f"{prefix}tail_ratio": float(tail) if math.isfinite(tail) else 10.0,
        f"{prefix}k_ratio": float(k_ratio(nav, dates, capital)),
    }


def _bounded_factor(value: float, target: float, *, higher_is_better: bool = True, floor: float = 0.05) -> float:
    if not math.isfinite(value) or target <= 0:
        return floor
    if higher_is_better:
        return min(1.0, max(floor, value / target))
    if value <= 0:
        return 1.0
    return min(1.0, max(floor, target / value))


def robust_growth_score(metrics: Mapping[str, object], prefix: str = "oos_") -> float:
    """Score for ranking candidates after hard constraints are enforced.

    Primary target is OOS log CAGR.  Factors are capped at 1, so excellent
    smoothness does not compensate for weak growth; weak risk/path diagnostics
    only discount the growth score.
    """
    log_cagr = _finite(metrics.get(f"{prefix}log_cagr"), _finite(metrics.get(f"{prefix}cagr")))
    if log_cagr <= 0:
        return log_cagr

    calmar = _finite(metrics.get(f"{prefix}calmar"))
    upi = _finite(metrics.get(f"{prefix}upi"))
    cdar = _finite(metrics.get(f"{prefix}cdar_95"))
    tail = _finite(metrics.get(f"{prefix}tail_ratio"))
    k = _finite(metrics.get(f"{prefix}k_ratio"))
    mdd_abs = abs(_finite(metrics.get(f"{prefix}mdd")))
    dsr = _finite(metrics.get("dsr"), 0.0)
    pbo = _finite(metrics.get("pbo"), 0.5)

    factors = [
        _bounded_factor(calmar, 0.75),
        _bounded_factor(upi, 1.25),
        _bounded_factor(tail, 1.0),
        _bounded_factor(k, 2.0),
        _bounded_factor(mdd_abs, 0.35, higher_is_better=False),
        _bounded_factor(cdar, 0.25, higher_is_better=False),
        _bounded_factor(dsr, 0.95),
        _bounded_factor(max(0.0, 0.50 - pbo), 0.50),
    ]
    penalty = float(np.prod(factors))
    return log_cagr * penalty


def trade_distribution_metrics(pnls: Iterable[float], prefix: str = "") -> dict[str, float]:
    """Profit Factor and SQN from realized trade PnL values."""
    arr = np.asarray([float(v) for v in pnls if math.isfinite(float(v))], dtype=float)
    if arr.size == 0:
        return {f"{prefix}profit_factor": 0.0, f"{prefix}sqn": 0.0, f"{prefix}trade_count": 0.0}
    gains = arr[arr > 0].sum()
    losses = abs(arr[arr < 0].sum())
    profit_factor = gains / losses if losses > 0 else (10.0 if gains > 0 else 0.0)
    std = float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0
    sqn = float(np.mean(arr) / std * math.sqrt(arr.size)) if std > 0 else 0.0
    return {
        f"{prefix}profit_factor": float(min(profit_factor, 10.0)),
        f"{prefix}sqn": sqn,
        f"{prefix}trade_count": float(arr.size),
    }


def add_objective_columns(
    rows: list[dict[str, object]],
    *,
    prefix: str = "oos_",
) -> list[dict[str, object]]:
    """Mutate-free helper to append robust-growth objective columns to rows."""
    out: list[dict[str, object]] = []
    for row in rows:
        r = dict(row)
        r["robust_growth_score"] = robust_growth_score(r, prefix=prefix)
        out.append(r)
    return out
