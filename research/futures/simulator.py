"""Realistic daily TAIFEX futures simulator.

The simulator is daily-bar based by design.  Signals are expected to be lagged
by the strategy builder; orders execute at the next available front-contract
open with fees, transaction tax, slippage, margin checks, and stop exits.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal

import math
import numpy as np
import polars as pl

from .specs import (
    CONTRACT_SPECS,
    FuturesContractSpec,
    FuturesCostConfig,
    FuturesMarginConfig,
    contract_spec,
)

Side = Literal["buy", "sell"]


@dataclass(frozen=True)
class FuturesExecutionConfig:
    capital: float = 1_000_000.0
    target_vol: float = 0.35
    max_abs_signal: float = 1.0
    cost: FuturesCostConfig = field(default_factory=FuturesCostConfig)
    margin: FuturesMarginConfig = field(default_factory=FuturesMarginConfig)
    stop_loss_atr: float | None = 2.5
    trailing_stop_atr: float | None = 4.0
    take_profit_atr: float | None = None
    time_stop_days: int | None = 40
    time_stop_min_return: float = -0.005
    atr_floor_pct: float = 0.005
    min_contracts_to_trade: int = 1
    allow_margin_failure: bool = False


@dataclass(frozen=True)
class FuturesBacktestResult:
    daily: pl.DataFrame
    fills: pl.DataFrame
    trades: pl.DataFrame
    summary: dict[str, float | int | str | bool]


@dataclass
class PositionState:
    product: str
    contract_month: str
    contracts: int
    entry_date: date
    entry_price: float
    entry_costs: float
    peak_price: float
    trough_price: float
    bars_held: int = 0
    realized_pnl: float = 0.0

    @property
    def side(self) -> int:
        return 1 if self.contracts > 0 else -1 if self.contracts < 0 else 0


def _empty_result(capital: float, name: str) -> FuturesBacktestResult:
    daily = pl.DataFrame({"date": [], "nav": []}, schema={"date": pl.Date, "nav": pl.Float64})
    return FuturesBacktestResult(daily, pl.DataFrame(), pl.DataFrame(), {"name": name, "capital": capital})


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _exec_price(open_price: float, spec: FuturesContractSpec, side: Side, ticks: float) -> float:
    bump = spec.ticks_to_price(ticks)
    return open_price + bump if side == "buy" else open_price - bump


def _cost(product: str, price: float, contracts: int, cfg: FuturesExecutionConfig, *, extra_ticks: float = 0.0) -> tuple[float, float, float]:
    spec = contract_spec(product)
    n = abs(int(contracts))
    if n == 0:
        return 0.0, 0.0, 0.0
    notional = spec.notional(price, n)
    commission = cfg.cost.commission(product, n)
    tax = cfg.cost.cost_multiplier * notional * spec.tax_rate
    slippage = cfg.cost.cost_multiplier * spec.ticks_to_price(cfg.cost.slippage_ticks + extra_ticks) * spec.multiplier * n
    return commission, tax, slippage


def _margin_capacity(equity: float, price: float, spec: FuturesContractSpec, cfg: FuturesExecutionConfig) -> int:
    if equity <= 0 or price <= 0:
        return 0
    notional_per_contract = spec.notional(price, 1)
    by_margin = math.floor(equity / (cfg.margin.initial_margin(notional_per_contract) * cfg.margin.required_buffer))
    by_lev = math.floor((equity * cfg.margin.max_notional_leverage) / notional_per_contract)
    return max(0, min(by_margin, by_lev))


def _target_contracts(signal: float, equity: float, price: float, atr: float, spec: FuturesContractSpec, cfg: FuturesExecutionConfig) -> int:
    if equity <= 0 or price <= 0:
        return 0
    signal = max(-cfg.max_abs_signal, min(cfg.max_abs_signal, float(signal)))
    if abs(signal) < 1e-12:
        return 0
    daily_target_vol = cfg.target_vol / math.sqrt(252.0)
    dollar_risk_budget = equity * daily_target_vol * abs(signal)
    atr = max(float(atr), price * cfg.atr_floor_pct)
    risk_per_contract = atr * spec.multiplier
    by_risk = math.floor(dollar_risk_budget / risk_per_contract) if risk_per_contract > 0 else 0
    capacity = _margin_capacity(equity, price, spec, cfg)
    contracts = max(0, min(by_risk, capacity))
    return int(math.copysign(contracts, signal))


def _closed_trade_pnl(pos: PositionState, exit_price: float, closed_contracts: int, exit_costs: float) -> float:
    n = abs(int(closed_contracts))
    if n == 0 or pos.contracts == 0:
        return 0.0
    side = 1 if pos.contracts > 0 else -1
    entry_costs = pos.entry_costs * min(1.0, n / abs(pos.contracts))
    return side * n * contract_spec(pos.product).multiplier * (exit_price - pos.entry_price) - entry_costs - exit_costs


def _trigger_exit(pos: PositionState, row: dict[str, object], cfg: FuturesExecutionConfig) -> tuple[str, float] | None:
    high = _safe_float(row.get("high"))
    low = _safe_float(row.get("low"))
    open_price = _safe_float(row.get("open"))
    atr = max(_safe_float(row.get("atr")), open_price * cfg.atr_floor_pct)
    if pos.side == 0 or open_price <= 0 or atr <= 0:
        return None

    if cfg.time_stop_days is not None and pos.bars_held >= cfg.time_stop_days:
        current_return = pos.side * (open_price / pos.entry_price - 1.0)
        if current_return <= cfg.time_stop_min_return:
            return ("time_stop", open_price)

    if pos.side > 0:
        stop_candidates = []
        if cfg.stop_loss_atr is not None:
            stop_candidates.append(pos.entry_price - cfg.stop_loss_atr * atr)
        if cfg.trailing_stop_atr is not None:
            stop_candidates.append(pos.peak_price - cfg.trailing_stop_atr * atr)
        stop_price = max(stop_candidates) if stop_candidates else None
        take_price = pos.entry_price + cfg.take_profit_atr * atr if cfg.take_profit_atr is not None else None
        if stop_price is not None and low <= stop_price:
            return ("stop_loss", min(open_price, stop_price) if open_price < stop_price else stop_price)
        if take_price is not None and high >= take_price:
            return ("take_profit", max(open_price, take_price) if open_price > take_price else take_price)
    else:
        stop_candidates = []
        if cfg.stop_loss_atr is not None:
            stop_candidates.append(pos.entry_price + cfg.stop_loss_atr * atr)
        if cfg.trailing_stop_atr is not None:
            stop_candidates.append(pos.trough_price + cfg.trailing_stop_atr * atr)
        stop_price = min(stop_candidates) if stop_candidates else None
        take_price = pos.entry_price - cfg.take_profit_atr * atr if cfg.take_profit_atr is not None else None
        if stop_price is not None and high >= stop_price:
            return ("stop_loss", max(open_price, stop_price) if open_price > stop_price else stop_price)
        if take_price is not None and low <= take_price:
            return ("take_profit", min(open_price, take_price) if open_price < take_price else take_price)
    return None


def simulate_single_product(
    bars: pl.DataFrame,
    targets: pl.DataFrame,
    *,
    product: str,
    name: str,
    cfg: FuturesExecutionConfig = FuturesExecutionConfig(),
) -> FuturesBacktestResult:
    """Simulate one product against lagged target signals.

    `targets` must contain `date` and `signal` where signal is desired signed
    exposure intensity in [-1, 1] for that day's open.
    """
    spec = CONTRACT_SPECS[product]
    if bars.is_empty():
        return _empty_result(cfg.capital, name)

    target_map = {
        row["date"]: _safe_float(row["signal"])
        for row in targets.select(["date", "signal"]).iter_rows(named=True)
    }

    rows = bars.sort("date").iter_rows(named=True)
    equity = float(cfg.capital)
    pos: PositionState | None = None
    prev_mark: float | None = None
    daily_rows: list[dict[str, object]] = []
    fill_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    margin_breach = False
    max_leverage = 0.0
    min_margin_buffer = math.inf
    turnover = 0.0

    for row in rows:
        day = row["date"]
        contract_month = str(row["contract_month"])
        open_price = _safe_float(row.get("open") or row.get("close"))
        high = _safe_float(row.get("high") or open_price)
        low = _safe_float(row.get("low") or open_price)
        close = _safe_float(row.get("close") or open_price)
        atr = _safe_float(row.get("atr"), open_price * cfg.atr_floor_pct)
        if open_price <= 0 or close <= 0:
            continue

        if pos is not None and prev_mark is not None:
            equity += pos.contracts * spec.multiplier * (open_price - prev_mark)

        day_costs = 0.0
        day_turnover = 0.0
        exit_reason = ""
        mark_ref_for_close = open_price

        if pos is not None and pos.contract_month != contract_month:
            side: Side = "sell" if pos.contracts > 0 else "buy"
            fill = _exec_price(open_price, spec, side, cfg.cost.slippage_ticks + cfg.cost.roll_extra_ticks)
            pnl = pos.contracts * spec.multiplier * (fill - open_price)
            commission, tax, slip = _cost(product, fill, pos.contracts, cfg, extra_ticks=cfg.cost.roll_extra_ticks)
            costs = commission + tax
            equity += pnl - costs
            day_costs += costs
            notional = spec.notional(fill, pos.contracts)
            day_turnover += notional
            fill_rows.append({
                "date": day, "strategy": name, "product": product, "contract_month": pos.contract_month,
                "side": side, "contracts": abs(pos.contracts), "price": fill,
                "commission": commission, "tax": tax, "slippage_cost": slip, "reason": "roll_close",
            })
            trade_rows.append({
                "entry_date": pos.entry_date, "exit_date": day, "strategy": name, "product": product,
                "side": "long" if pos.contracts > 0 else "short", "contracts": abs(pos.contracts),
                "entry_price": pos.entry_price, "exit_price": fill,
                "bars_held": pos.bars_held, "pnl": _closed_trade_pnl(pos, fill, abs(pos.contracts), costs),
                "reason": "roll",
            })
            pos = None
            prev_mark = open_price

        if pos is not None:
            triggered = _trigger_exit(pos, {**row, "atr": atr}, cfg)
            if triggered is not None:
                reason, raw_exit_price = triggered
                side = "sell" if pos.contracts > 0 else "buy"
                fill = _exec_price(raw_exit_price, spec, side, cfg.cost.slippage_ticks)
                pnl = pos.contracts * spec.multiplier * (fill - open_price)
                commission, tax, slip = _cost(product, fill, pos.contracts, cfg)
                costs = commission + tax
                equity += pnl - costs
                day_costs += costs
                notional = spec.notional(fill, pos.contracts)
                day_turnover += notional
                exit_reason = reason
                fill_rows.append({
                    "date": day, "strategy": name, "product": product, "contract_month": pos.contract_month,
                    "side": side, "contracts": abs(pos.contracts), "price": fill,
                    "commission": commission, "tax": tax, "slippage_cost": slip, "reason": reason,
                })
                trade_rows.append({
                    "entry_date": pos.entry_date, "exit_date": day, "strategy": name, "product": product,
                    "side": "long" if pos.contracts > 0 else "short", "contracts": abs(pos.contracts),
                    "entry_price": pos.entry_price, "exit_price": fill,
                    "bars_held": pos.bars_held, "pnl": _closed_trade_pnl(pos, fill, abs(pos.contracts), costs),
                    "reason": reason,
                })
                pos = None
                prev_mark = open_price

        desired = target_map.get(day, 0.0)
        target = _target_contracts(desired, equity, open_price, atr, spec, cfg)
        current = pos.contracts if pos is not None else 0
        delta = target - current
        if abs(delta) >= cfg.min_contracts_to_trade:
            side: Side = "buy" if delta > 0 else "sell"
            fill = _exec_price(open_price, spec, side, cfg.cost.slippage_ticks)
            commission, tax, slip = _cost(product, fill, delta, cfg)
            costs = commission + tax
            equity -= costs
            day_costs += costs
            notional = spec.notional(fill, delta)
            day_turnover += notional
            fill_rows.append({
                "date": day, "strategy": name, "product": product, "contract_month": contract_month,
                "side": side, "contracts": abs(delta), "price": fill,
                "commission": commission, "tax": tax, "slippage_cost": slip, "reason": "target_rebalance",
            })
            new_contracts = current + delta
            if new_contracts == 0:
                if pos is not None:
                    trade_rows.append({
                        "entry_date": pos.entry_date, "exit_date": day, "strategy": name, "product": product,
                        "side": "long" if pos.contracts > 0 else "short", "contracts": abs(pos.contracts),
                        "entry_price": pos.entry_price, "exit_price": fill,
                        "bars_held": pos.bars_held, "pnl": _closed_trade_pnl(pos, fill, abs(pos.contracts), costs),
                        "reason": "signal_exit",
                    })
                pos = None
            elif pos is None or (pos.contracts > 0) != (new_contracts > 0):
                if pos is not None:
                    total_delta = abs(delta)
                    closed_contracts = abs(current)
                    exit_costs = costs * closed_contracts / total_delta if total_delta else costs
                    entry_costs = costs - exit_costs
                    trade_rows.append({
                        "entry_date": pos.entry_date, "exit_date": day, "strategy": name, "product": product,
                        "side": "long" if pos.contracts > 0 else "short", "contracts": closed_contracts,
                        "entry_price": pos.entry_price, "exit_price": fill,
                        "bars_held": pos.bars_held, "pnl": _closed_trade_pnl(pos, fill, closed_contracts, exit_costs),
                        "reason": "signal_flip",
                    })
                else:
                    entry_costs = costs
                pos = PositionState(product, contract_month, int(new_contracts), day, fill, entry_costs, fill, fill)
                mark_ref_for_close = fill
            else:
                old_abs = abs(pos.contracts)
                new_abs = abs(new_contracts)
                if new_abs < old_abs:
                    closed_contracts = old_abs - new_abs
                    trade_rows.append({
                        "entry_date": pos.entry_date, "exit_date": day, "strategy": name, "product": product,
                        "side": "long" if pos.contracts > 0 else "short", "contracts": closed_contracts,
                        "entry_price": pos.entry_price, "exit_price": fill,
                        "bars_held": pos.bars_held, "pnl": _closed_trade_pnl(pos, fill, closed_contracts, costs),
                        "reason": "partial_reduce",
                    })
                    pos.entry_costs *= new_abs / old_abs
                elif new_abs > old_abs:
                    pos.entry_price = (pos.entry_price * old_abs + fill * (new_abs - old_abs)) / new_abs
                    pos.entry_costs += costs
                pos.contracts = int(new_contracts)
                mark_ref_for_close = fill

        if pos is not None:
            mark_ref = mark_ref_for_close
            equity += pos.contracts * spec.multiplier * (close - mark_ref)
            pos.peak_price = max(pos.peak_price, high)
            pos.trough_price = min(pos.trough_price, low)
            pos.bars_held += 1
            prev_mark = close
        else:
            prev_mark = None

        notional_open = spec.notional(close, pos.contracts) if pos is not None else 0.0
        leverage = notional_open / equity if equity > 0 else math.inf
        maint = cfg.margin.maintenance_margin(notional_open)
        buffer = equity / maint if maint > 0 else math.inf
        stress_buffer = (equity - cfg.margin.stress_loss(notional_open)) / maint if maint > 0 else math.inf
        max_leverage = max(max_leverage, leverage if math.isfinite(leverage) else 999.0)
        min_margin_buffer = min(min_margin_buffer, buffer)
        turnover += day_turnover
        if maint > 0 and equity <= maint * cfg.margin.liquidation_buffer:
            margin_breach = True
            if not cfg.allow_margin_failure:
                pos = None
                prev_mark = None

        daily_rows.append({
            "date": day,
            "strategy": name,
            "product": product,
            "nav": equity,
            "signal": desired,
            "contracts": pos.contracts if pos is not None else 0,
            "contract_month": pos.contract_month if pos is not None else contract_month,
            "close": close,
            "notional": notional_open,
            "gross_leverage": leverage,
            "margin_buffer": buffer,
            "stress_margin_buffer": stress_buffer,
            "costs": day_costs,
            "turnover": day_turnover,
            "exit_reason": exit_reason,
            "margin_breach": margin_breach,
        })

        if equity <= 0:
            margin_breach = True
            if not cfg.allow_margin_failure:
                break

    if not daily_rows:
        return _empty_result(cfg.capital, name)

    daily = pl.DataFrame(daily_rows).sort("date")
    fills = pl.DataFrame(fill_rows) if fill_rows else pl.DataFrame()
    trades = pl.DataFrame(trade_rows) if trade_rows else pl.DataFrame()
    summary = {
        "name": name,
        "product": product,
        "capital": cfg.capital,
        "ending_nav": float(daily["nav"][-1]),
        "trade_count": int(trades.height) if not trades.is_empty() else 0,
        "fill_count": int(fills.height) if not fills.is_empty() else 0,
        "turnover": float(turnover),
        "max_leverage": float(max_leverage),
        "min_margin_buffer": float(min_margin_buffer if math.isfinite(min_margin_buffer) else 999.0),
        "margin_breach": bool(margin_breach),
    }
    return FuturesBacktestResult(daily, fills, trades, summary)


def combine_sleeve_returns(
    daily_by_name: dict[str, pl.DataFrame],
    *,
    capital: float = 1_000_000.0,
    lookback_days: int = 63,
    top_k: int = 2,
    target_vol: float = 0.45,
    vol_penalty: float = 0.75,
    dd_penalty: float = 0.50,
    min_score: float = -0.05,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Lagged PM allocator across already costed sleeve NAV streams."""
    names = sorted(daily_by_name)
    if not names:
        raise ValueError("at least one sleeve is required")
    panel = None
    for name in names:
        df = (
            daily_by_name[name]
            .select(["date", "nav"])
            .sort("date")
            .with_columns((pl.col("nav") / pl.col("nav").shift(1) - 1.0).fill_null(0.0).alias(name))
            .select(["date", name])
        )
        panel = df if panel is None else panel.join(df, on="date", how="inner")
    assert panel is not None
    panel = panel.sort("date")
    dates = panel["date"].to_list()
    rets = panel.select(names).to_numpy().astype(float)
    n, m = rets.shape
    weights = np.zeros((n, m), dtype=float)

    for i in range(n):
        window = rets[max(0, i - lookback_days):i]
        if len(window) < max(21, lookback_days // 3):
            continue
        log_ret = np.sum(np.log1p(np.clip(window, -0.999, None)), axis=0)
        vol = np.std(window, axis=0, ddof=1) * math.sqrt(252.0)
        wealth = np.cumprod(1.0 + window, axis=0)
        dd = wealth / np.maximum.accumulate(wealth, axis=0) - 1.0
        current_dd = dd[-1]
        score = log_ret - vol_penalty * vol - dd_penalty * np.abs(current_dd)
        valid = np.where(np.isfinite(score) & (score >= min_score))[0]
        if valid.size == 0:
            continue
        selected = valid[np.argsort(score[valid])[::-1][:top_k]]
        raw = np.zeros(m, dtype=float)
        inv_vol = 1.0 / np.maximum(vol[selected], 1e-6)
        raw[selected] = inv_vol / inv_vol.sum()
        port_window = window @ raw
        port_vol = float(np.std(port_window, ddof=1) * math.sqrt(252.0)) if len(port_window) > 2 else 0.0
        scale = min(1.0, target_vol / port_vol) if port_vol > 0 else 1.0
        weights[i] = raw * scale

    port_rets = np.sum(rets * weights, axis=1)
    nav = capital * np.cumprod(1.0 + port_rets)
    daily = pl.DataFrame({"date": dates, "nav": nav, "ret": port_rets})
    weight_df = pl.DataFrame({"date": dates, **{f"{name}__weight": weights[:, j] for j, name in enumerate(names)}})
    return daily, weight_df
