"""Iter100 cost-below-entry metrics and objective.

The user-facing optimization intent is intentionally different from a normal
drawdown objective: maximize OOS growth, but do not treat above-cost profit
giveback as a loss.  Above-cost giveback is tracked as profit-retention quality;
loss penalties start only when a trade moves below its entry cost.
"""

from __future__ import annotations

import math
from typing import Mapping

import numpy as np
import polars as pl


def _finite(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _col_or_default(frame: pl.DataFrame, column: str, default: float = 0.0) -> np.ndarray:
    if column not in frame.columns or frame.is_empty():
        return np.full(frame.height, default, dtype=float)
    return frame[column].to_numpy().astype(float)


def cost_below_trade_metrics(trades: pl.DataFrame, *, prefix: str = "trade_") -> dict[str, float]:
    """Measure trade-level loss exposure below entry cost.

    Required trade columns are the existing execution simulator fields:
    ``gross_return`` and ``mae_pct``.  ``mfe_pct`` and ``below_cost_days`` are
    used when present.  ``mae_pct`` is negative when the trade moved below
    entry; a winner that gives back profit but never goes below entry therefore
    carries zero below-cost loss penalty.
    """
    if trades.is_empty():
        return {
            f"{prefix}count": 0.0,
            f"{prefix}below_cost_mae_mean": 0.0,
            f"{prefix}below_cost_mae_p95": 0.0,
            f"{prefix}below_cost_days_mean": 0.0,
            f"{prefix}loss_tail_ratio": 0.0,
            f"{prefix}mfe_retention_mean": 0.0,
            f"{prefix}winner_giveback_above_cost_mean": 0.0,
            f"{prefix}below_cost_penalty": 0.0,
        }

    gross_return = _col_or_default(trades, "gross_return")
    mae = _col_or_default(trades, "mae_pct")
    mfe = np.maximum(_col_or_default(trades, "mfe_pct"), 0.0)
    below_cost_days = np.maximum(_col_or_default(trades, "below_cost_days"), 0.0)

    below_cost_mae = np.maximum(-mae, 0.0)
    mfe_retention = np.divide(
        np.maximum(gross_return, 0.0),
        mfe,
        out=np.zeros_like(gross_return),
        where=mfe > 1e-12,
    )
    mfe_retention = np.clip(mfe_retention, 0.0, 1.5)
    winner_giveback_above_cost = np.maximum(mfe - np.maximum(gross_return, 0.0), 0.0)

    loser_returns = gross_return[gross_return < 0.0]
    winner_returns = gross_return[gross_return > 0.0]
    loss_tail = abs(float(np.quantile(loser_returns, 0.05))) if loser_returns.size else 0.0
    win_median = float(np.median(winner_returns)) if winner_returns.size else 0.0
    loss_tail_ratio = loss_tail / max(win_median, 1e-9) if loss_tail > 0.0 else 0.0

    mae_mean = float(np.mean(below_cost_mae)) if below_cost_mae.size else 0.0
    mae_p95 = float(np.quantile(below_cost_mae, 0.95)) if below_cost_mae.size else 0.0
    days_mean = float(np.mean(below_cost_days)) if below_cost_days.size else 0.0
    penalty = (
        1.70 * mae_mean
        + 1.15 * mae_p95
        + 0.018 * days_mean
        + 0.08 * min(loss_tail_ratio, 5.0)
    )

    return {
        f"{prefix}count": float(trades.height),
        f"{prefix}below_cost_mae_mean": mae_mean,
        f"{prefix}below_cost_mae_p95": mae_p95,
        f"{prefix}below_cost_days_mean": days_mean,
        f"{prefix}loss_tail_ratio": float(loss_tail_ratio),
        f"{prefix}mfe_retention_mean": float(np.mean(mfe_retention)) if mfe_retention.size else 0.0,
        f"{prefix}winner_giveback_above_cost_mean": float(np.mean(winner_giveback_above_cost))
        if winner_giveback_above_cost.size
        else 0.0,
        f"{prefix}below_cost_penalty": float(penalty),
    }


def cost_below_entry_objective(row: Mapping[str, object], *, prefix: str = "trade_") -> float:
    """Rank candidates by growth after below-cost loss penalties.

    OOS log CAGR is the primary target.  Recent 1Y CAGR can help if positive,
    but it is clipped so one hot year cannot rescue a bad OOS profile.  Above
    cost giveback does not directly reduce this score; it is only reported via
    MFE retention diagnostics.
    """
    oos_log = _finite(row.get("oos_log_cagr"), _finite(row.get("oos_cagr")))
    if oos_log <= 0.0:
        return oos_log

    recent = min(max(_finite(row.get("recent_1y_cagr")), 0.0), 3.0)
    dsr = min(max(_finite(row.get("dsr")), 0.0), 1.0)
    pbo = min(max(_finite(row.get("pbo"), 0.5), 0.0), 1.0)
    fill = min(max(_finite(row.get("fill_ratio"), 1.0), 0.0), 1.0)
    below_penalty = max(_finite(row.get(f"{prefix}below_cost_penalty")), 0.0)
    mae_p95 = max(_finite(row.get(f"{prefix}below_cost_mae_p95")), 0.0)
    portfolio_ulcer = max(_finite(row.get("portfolio_below_cost_ulcer")), 0.0)
    portfolio_cdar = max(_finite(row.get("portfolio_below_cost_cdar_95")), 0.0)
    portfolio_days = max(_finite(row.get("portfolio_below_cost_days_ratio")), 0.0)

    validation_factor = min(1.0, max(0.35, dsr / 0.95))
    overfit_factor = min(1.0, max(0.35, (0.55 - pbo) / 0.55))
    fill_factor = min(1.0, max(0.50, fill / 0.90))
    below_cost_factor = math.exp(-below_penalty) * min(1.0, max(0.35, 0.18 / max(mae_p95, 1e-9)))
    portfolio_factor = math.exp(-(2.0 * portfolio_ulcer + 1.25 * portfolio_cdar + 0.35 * portfolio_days))

    return float(
        oos_log
        * (1.0 + 0.25 * recent)
        * validation_factor
        * overfit_factor
        * fill_factor
        * below_cost_factor
        * portfolio_factor
    )


def add_iter100_objective(row: Mapping[str, object], trades: pl.DataFrame, *, prefix: str = "trade_") -> dict[str, object]:
    """Return a result row augmented with Iter100 trade metrics and objective."""
    out = dict(row)
    out.update(cost_below_trade_metrics(trades, prefix=prefix))
    out["iter100_cost_below_objective"] = cost_below_entry_objective(out, prefix=prefix)
    return out


def portfolio_below_cost_metrics(
    fills: pl.DataFrame,
    bars: pl.DataFrame,
    daily: pl.DataFrame,
    *,
    prefix: str = "portfolio_",
) -> dict[str, float]:
    """Reconstruct all-in cost basis and measure portfolio loss below cost.

    This avoids changing the shared execution simulator.  Buy commission is
    included in cost basis; sells reduce cost basis proportionally.  Daily loss
    exposure is measured after that day's fills using adjusted low/close prices.
    """
    if daily.is_empty():
        return {
            f"{prefix}below_cost_ulcer": 0.0,
            f"{prefix}below_cost_cdar_95": 0.0,
            f"{prefix}below_cost_area_mean": 0.0,
            f"{prefix}below_cost_days_ratio": 0.0,
            f"{prefix}below_cost_exposure_mean": 0.0,
            f"{prefix}below_cost_positions_max": 0.0,
        }

    ordered_daily = daily.select(["date", "nav"]).sort("date")
    dates = ordered_daily["date"].to_list()
    nav_by_day = dict(ordered_daily.iter_rows())
    bar_lookup = {
        (row["date"], str(row["company_code"])): row
        for row in bars.select(["date", "company_code", "low", "close"]).iter_rows(named=True)
    }
    fills_by_day: dict[object, list[dict[str, object]]] = {}
    if not fills.is_empty():
        fill_rows = fills.with_row_index("_row").sort(["date", "_row"]).iter_rows(named=True)
        for row in fill_rows:
            if float(row.get("filled_shares") or 0.0) > 0.0:
                fills_by_day.setdefault(row["date"], []).append(row)

    qty: dict[str, float] = {}
    cost: dict[str, float] = {}
    weighted_gaps: list[float] = []
    exposures: list[float] = []
    below_counts: list[float] = []

    for day in dates:
        for fill in fills_by_day.get(day, []):
            code = str(fill["company_code"])
            shares = float(fill["filled_shares"])
            notional = float(fill["notional"])
            commission = float(fill.get("commission") or 0.0)
            side = str(fill["side"])
            if side == "buy":
                qty[code] = qty.get(code, 0.0) + shares
                cost[code] = cost.get(code, 0.0) + notional + commission
            elif side == "sell":
                old_qty = qty.get(code, 0.0)
                if old_qty <= 1e-12:
                    continue
                sold = min(shares, old_qty)
                ratio = sold / old_qty
                qty[code] = old_qty - sold
                cost[code] = cost.get(code, 0.0) * max(0.0, 1.0 - ratio)
                if qty[code] <= 1e-9:
                    qty.pop(code, None)
                    cost.pop(code, None)

        nav = max(float(nav_by_day.get(day) or 0.0), 1e-9)
        weighted_gap = 0.0
        exposure = 0.0
        below_count = 0.0
        for code, shares in list(qty.items()):
            basis = cost.get(code, 0.0) / max(shares, 1e-9)
            bar = bar_lookup.get((day, code))
            if basis <= 0.0 or bar is None:
                continue
            low = float(bar["low"] or 0.0)
            close = float(bar["close"] or 0.0)
            if low <= 0.0 or close <= 0.0:
                continue
            gap = min(low / basis - 1.0, 0.0)
            weight = shares * close / nav
            if gap < 0.0:
                below_count += 1.0
                exposure += weight
                weighted_gap += weight * gap
        weighted_gaps.append(weighted_gap)
        exposures.append(exposure)
        below_counts.append(below_count)

    gaps = np.asarray(weighted_gaps, dtype=float)
    exposures_arr = np.asarray(exposures, dtype=float)
    counts = np.asarray(below_counts, dtype=float)
    negative = gaps[gaps < 0.0]
    q05 = float(np.quantile(gaps, 0.05)) if gaps.size else 0.0
    tail = gaps[gaps <= q05] if gaps.size else np.array([], dtype=float)
    return {
        f"{prefix}below_cost_ulcer": float(math.sqrt(np.mean(np.square(np.minimum(gaps, 0.0))))) if gaps.size else 0.0,
        f"{prefix}below_cost_cdar_95": float(abs(np.mean(tail))) if tail.size else 0.0,
        f"{prefix}below_cost_area_mean": float(abs(np.mean(np.minimum(gaps, 0.0)))) if gaps.size else 0.0,
        f"{prefix}below_cost_days_ratio": float(negative.size / max(gaps.size, 1)),
        f"{prefix}below_cost_exposure_mean": float(np.mean(exposures_arr)) if exposures_arr.size else 0.0,
        f"{prefix}below_cost_positions_max": float(np.max(counts)) if counts.size else 0.0,
    }
