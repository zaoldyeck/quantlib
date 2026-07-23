"""Realistic target-book execution simulator.

The strategy layer emits target weights. This module turns those weights into
broker-like fills with order sizing, tiered fees, lot rounding, volume caps,
limit-price blocks, and slippage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal

import duckdb
import numpy as np
import polars as pl

from quantlib.constants import CAPITAL
from quantlib.prices import fetch_adjusted_panel
from quantlib.execsim.broker_fee import FubonFeeSchedule, MonthlyFeeMeter

Side = Literal["buy", "sell"]
Book = dict[str, float]
BookByDate = dict[date, Book]
IntradayPriority = Literal["stop_first", "take_profit_first"]


@dataclass(frozen=True)
class ExitConfig:
    """Optional long-only exit layer evaluated inside the execution simulator.

    Percent inputs are decimals, e.g. 0.12 means 12%.  Price-triggered exits use
    daily adjusted OHLC bars and a conservative OHLC ambiguity policy: if a stop
    and a take-profit can both trigger in the same daily bar, the default assumes
    the stop happens first.
    """

    name: str = "none"
    stop_loss_pct: float | None = None
    take_profit_pct: float | None = None
    trailing_stop_pct: float | None = None
    breakeven_trigger_pct: float | None = None
    breakeven_buffer_pct: float = 0.0
    time_stop_days: int | None = None
    time_stop_min_return_pct: float | None = None
    intraday_priority: IntradayPriority = "stop_first"
    trigger_on_entry_day: bool = True

    @property
    def enabled(self) -> bool:
        return any(
            value is not None
            for value in (
                self.stop_loss_pct,
                self.take_profit_pct,
                self.trailing_stop_pct,
                self.breakeven_trigger_pct,
                self.time_stop_days,
            )
        )


@dataclass(frozen=True)
class ExecutionConfig:
    """Execution assumptions for target-book validation."""

    name: str = "fubon_odd_lot"
    capital: float = CAPITAL
    lot_size: int = 1
    max_participation_rate: float = 0.05
    fixed_slippage_bps: float = 5.0
    impact_bps_per_1pct_volume: float = 1.0
    daily_limit_pct: float = 0.10
    limit_tolerance_bps: float = 5.0
    fee_schedule: FubonFeeSchedule = field(default_factory=FubonFeeSchedule)
    exit_config: ExitConfig = field(default_factory=ExitConfig)


@dataclass(frozen=True)
class ExecutionResult:
    daily: pl.DataFrame
    fills: pl.DataFrame
    stats: dict[str, float]
    trades: pl.DataFrame


@dataclass(frozen=True)
class Bar:
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_value: float
    prev_close: float | None
    adv60: float | None


@dataclass
class Fill:
    date: date
    company_code: str
    side: Side
    requested_shares: float
    filled_shares: float
    open_price: float
    fill_price: float
    notional: float
    commission: float
    tax: float
    slippage_cost: float
    reason: str

    def as_row(self) -> dict[str, object]:
        return {
            "date": self.date,
            "company_code": self.company_code,
            "side": self.side,
            "requested_shares": self.requested_shares,
            "filled_shares": self.filled_shares,
            "open_price": self.open_price,
            "fill_price": self.fill_price,
            "notional": self.notional,
            "commission": self.commission,
            "tax": self.tax,
            "slippage_cost": self.slippage_cost,
            "reason": self.reason,
        }


@dataclass
class PositionState:
    entry_date: date
    entry_index: int
    entry_price: float
    shares: float
    peak_price: float
    trough_price: float


def load_adjusted_execution_bars(
    con: duckdb.DuckDBPyConnection,
    codes: list[str],
    start: date,
    end: date,
    markets: tuple[str, ...] = ("twse", "tpex"),
) -> pl.DataFrame:
    """Load total-return-adjusted OHLCV with raw liquidity fields.

    Prices are adjusted so long-window NAV remains total-return consistent.
    Volume and trade_value remain raw exchange fields and are used only for
    liquidity and participation constraints.
    """
    panels = [
        fetch_adjusted_panel(
            con,
            start.isoformat(),
            end.isoformat(),
            codes=sorted(set(codes)),
            market=market,
            include_extra_history_days=90,
        )
        for market in markets
    ]
    panels = [panel for panel in panels if not panel.is_empty()]
    if not panels:
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "company_code": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "trade_value": pl.Float64,
                "prev_close": pl.Float64,
                "adv60": pl.Float64,
            }
        )
    panel = (
        pl.concat(panels, how="diagonal")
        .sort(["company_code", "date", "trade_value"], descending=[False, False, True])
        .unique(subset=["company_code", "date"], keep="first", maintain_order=True)
    )
    if panel.is_empty():
        return panel
    return (
        panel.sort(["company_code", "date"])
        .with_columns(
            [
                pl.col("close").shift(1).over("company_code").alias("prev_close"),
                pl.col("trade_value").rolling_mean(60).over("company_code").shift(1).alias("adv60"),
            ]
        )
        .filter((pl.col("date") >= start) & (pl.col("date") <= end))
        .select(
            [
                "date",
                "company_code",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "trade_value",
                "prev_close",
                "adv60",
            ]
        )
    )


class RealisticExecutionSimulator:
    def __init__(self, bars: pl.DataFrame, config: ExecutionConfig | None = None):
        self.config = config or ExecutionConfig()
        self.bars = self._build_bar_map(bars)

    @staticmethod
    def _build_bar_map(bars: pl.DataFrame) -> dict[tuple[date, str], Bar]:
        out: dict[tuple[date, str], Bar] = {}
        for row in bars.iter_rows(named=True):
            out[(row["date"], str(row["company_code"]))] = Bar(
                open=float(row["open"] or 0.0),
                high=float(row["high"] or row["open"] or 0.0),
                low=float(row["low"] or row["open"] or 0.0),
                close=float(row["close"] or 0.0),
                volume=float(row["volume"] or 0.0),
                trade_value=float(row["trade_value"] or 0.0),
                prev_close=float(row["prev_close"]) if row["prev_close"] is not None else None,
                adv60=float(row["adv60"]) if row["adv60"] is not None else None,
            )
        return out

    def simulate(self, days: list[date], targets: BookByDate) -> ExecutionResult:
        cash = self.config.capital
        shares: dict[str, float] = {}
        positions: dict[str, PositionState] = {}
        fee_meter = MonthlyFeeMeter(self.config.fee_schedule)
        active_target: Book = {}
        daily_rows: list[dict[str, object]] = []
        fills: list[Fill] = []
        trades: list[dict[str, object]] = []
        max_active = 0
        trade_days = 0
        turnover_sum = 0.0
        requested_notional_sum = 0.0
        filled_notional_sum = 0.0
        exit_orders = 0
        exit_notional_sum = 0.0

        for day_index, day in enumerate(days):
            open_values = {code: qty * self._price(day, code, "open") for code, qty in shares.items()}
            nav_open = cash + sum(open_values.values())
            day_commission = 0.0
            day_tax = 0.0
            day_slippage = 0.0
            day_requested = 0.0
            day_filled = 0.0
            blocked_orders = 0
            partial_orders = 0
            filled_orders = 0

            if day in targets:
                active_target = {code: weight for code, weight in targets[day].items() if weight > 1e-12}
                target_values = {
                    code: nav_open * active_target.get(code, 0.0)
                    for code in sorted(set(shares) | set(active_target))
                }
                deltas = {
                    code: target_values[code] - open_values.get(code, 0.0)
                    for code in target_values
                }
                sells = [(code, -delta) for code, delta in deltas.items() if delta < -1e-9]
                buys = [(code, delta) for code, delta in deltas.items() if delta > 1e-9]

                for code, desired_notional in sorted(sells, key=lambda item: item[1], reverse=True):
                    result = self._execute_order(day, code, "sell", desired_notional, shares.get(code, 0.0), cash, fee_meter)
                    fills.append(result)
                    day_requested += result.requested_shares * result.open_price
                    requested_notional_sum += result.requested_shares * result.open_price
                    if result.filled_shares <= 0:
                        blocked_orders += 1
                        continue
                    if result.filled_shares + 1e-9 < result.requested_shares:
                        partial_orders += 1
                    filled_orders += 1
                    trade = self._record_sell_trade(positions, day, day_index, code, result, "target_rebalance")
                    if trade is not None:
                        trades.append(trade)
                    shares[code] = max(shares.get(code, 0.0) - result.filled_shares, 0.0)
                    if shares[code] <= 1e-9:
                        shares.pop(code, None)
                        positions.pop(code, None)
                    cash += result.notional - result.commission - result.tax
                    day_commission += result.commission
                    day_tax += result.tax
                    day_slippage += result.slippage_cost
                    day_filled += result.notional
                    filled_notional_sum += result.notional

                for code, desired_notional in sorted(buys, key=lambda item: item[1], reverse=True):
                    result = self._execute_order(day, code, "buy", desired_notional, shares.get(code, 0.0), cash, fee_meter)
                    fills.append(result)
                    day_requested += result.requested_shares * result.open_price
                    requested_notional_sum += result.requested_shares * result.open_price
                    if result.filled_shares <= 0:
                        blocked_orders += 1
                        continue
                    if result.filled_shares + 1e-9 < result.requested_shares:
                        partial_orders += 1
                    filled_orders += 1
                    old_qty = shares.get(code, 0.0)
                    shares[code] = old_qty + result.filled_shares
                    self._record_buy_position(positions, day, day_index, code, old_qty, result)
                    cash -= result.notional + result.commission
                    day_commission += result.commission
                    day_slippage += result.slippage_cost
                    day_filled += result.notional
                    filled_notional_sum += result.notional

            if self.config.exit_config.enabled:
                exit_results = self._execute_exit_rules(day, day_index, shares, positions, cash, fee_meter)
                for result, trade in exit_results:
                    fills.append(result)
                    day_requested += result.requested_shares * result.open_price
                    requested_notional_sum += result.requested_shares * result.open_price
                    if result.filled_shares <= 0:
                        blocked_orders += 1
                        continue
                    if result.filled_shares + 1e-9 < result.requested_shares:
                        partial_orders += 1
                    filled_orders += 1
                    exit_orders += 1
                    if trade is not None:
                        trades.append(trade)
                    shares[result.company_code] = max(shares.get(result.company_code, 0.0) - result.filled_shares, 0.0)
                    if shares[result.company_code] <= 1e-9:
                        shares.pop(result.company_code, None)
                        positions.pop(result.company_code, None)
                    cash += result.notional - result.commission - result.tax
                    day_commission += result.commission
                    day_tax += result.tax
                    day_slippage += result.slippage_cost
                    day_filled += result.notional
                    filled_notional_sum += result.notional
                    exit_notional_sum += result.notional

            self._mark_position_extremes(day, positions)

            close_nav = cash + sum(qty * self._price(day, code, "close") for code, qty in shares.items())
            turnover = day_filled / max(nav_open, 1e-9)
            if turnover > 1e-8:
                trade_days += 1
                turnover_sum += turnover
            max_active = max(max_active, len(shares))
            daily_rows.append(
                {
                    "date": day,
                    "nav": close_nav,
                    "cash": cash,
                    "active": len(shares),
                    "turnover": turnover,
                    "requested_turnover": day_requested / max(nav_open, 1e-9),
                    "commission": day_commission,
                    "tax": day_tax,
                    "slippage_cost": day_slippage,
                    "blocked_orders": blocked_orders,
                    "partial_orders": partial_orders,
                    "filled_orders": filled_orders,
                }
            )

        daily = pl.DataFrame(daily_rows)
        fills_df = pl.DataFrame([fill.as_row() for fill in fills]) if fills else self._empty_fills()
        trades_df = pl.DataFrame(trades) if trades else self._empty_trades()
        stats = {
            "max_active": float(max_active),
            "trade_days": float(trade_days),
            "avg_turnover_trade_day": turnover_sum / trade_days if trade_days else 0.0,
            "requested_notional": requested_notional_sum,
            "filled_notional": filled_notional_sum,
            "fill_ratio": filled_notional_sum / requested_notional_sum if requested_notional_sum else 1.0,
            "total_commission": float(daily["commission"].sum()) if daily.height else 0.0,
            "total_tax": float(daily["tax"].sum()) if daily.height else 0.0,
            "total_slippage_cost": float(daily["slippage_cost"].sum()) if daily.height else 0.0,
            "blocked_orders": float(daily["blocked_orders"].sum()) if daily.height else 0.0,
            "partial_orders": float(daily["partial_orders"].sum()) if daily.height else 0.0,
            "exit_orders": float(exit_orders),
            "exit_notional": float(exit_notional_sum),
            "exit_notional_ratio": exit_notional_sum / filled_notional_sum if filled_notional_sum else 0.0,
        }
        return ExecutionResult(daily=daily, fills=fills_df, stats=stats, trades=trades_df)

    def _record_buy_position(
        self,
        positions: dict[str, PositionState],
        day: date,
        day_index: int,
        code: str,
        old_qty: float,
        fill: Fill,
    ) -> None:
        if fill.filled_shares <= 0 or fill.fill_price <= 0:
            return
        bar = self.bars.get((day, code))
        peak = max(fill.fill_price, bar.high if bar is not None else fill.fill_price)
        trough = min(fill.fill_price, bar.low if bar is not None else fill.fill_price)
        if code not in positions or old_qty <= 1e-9:
            positions[code] = PositionState(
                entry_date=day,
                entry_index=day_index,
                entry_price=fill.fill_price,
                shares=fill.filled_shares,
                peak_price=peak,
                trough_price=trough,
            )
            return

        pos = positions[code]
        new_qty = old_qty + fill.filled_shares
        pos.entry_price = (
            pos.entry_price * old_qty + fill.fill_price * fill.filled_shares
        ) / max(new_qty, 1e-9)
        pos.shares = new_qty
        pos.peak_price = max(pos.peak_price, peak)
        pos.trough_price = min(pos.trough_price, trough)

    def _record_sell_trade(
        self,
        positions: dict[str, PositionState],
        day: date,
        day_index: int,
        code: str,
        fill: Fill,
        exit_reason: str,
    ) -> dict[str, object] | None:
        pos = positions.get(code)
        if pos is None or fill.filled_shares <= 0 or fill.fill_price <= 0 or pos.entry_price <= 0:
            return None
        sold = min(fill.filled_shares, pos.shares)
        gross_pnl = (fill.fill_price - pos.entry_price) * sold
        cost_ratio = sold / max(fill.filled_shares, 1e-9)
        net_pnl = gross_pnl - (fill.commission + fill.tax) * cost_ratio
        row = {
            "entry_date": pos.entry_date,
            "exit_date": day,
            "company_code": code,
            "shares": sold,
            "entry_price": pos.entry_price,
            "exit_price": fill.fill_price,
            "gross_return": fill.fill_price / pos.entry_price - 1.0,
            "gross_pnl": gross_pnl,
            "net_pnl": net_pnl,
            "holding_days": day_index - pos.entry_index + 1,
            "mfe_pct": pos.peak_price / pos.entry_price - 1.0,
            "mae_pct": pos.trough_price / pos.entry_price - 1.0,
            "exit_reason": exit_reason,
        }
        pos.shares = max(pos.shares - sold, 0.0)
        return row

    def _mark_position_extremes(self, day: date, positions: dict[str, PositionState]) -> None:
        for code, pos in positions.items():
            bar = self.bars.get((day, code))
            if bar is None:
                continue
            pos.peak_price = max(pos.peak_price, bar.high)
            pos.trough_price = min(pos.trough_price, bar.low)

    @staticmethod
    def _empty_trades() -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "entry_date": pl.Date,
                "exit_date": pl.Date,
                "company_code": pl.Utf8,
                "shares": pl.Float64,
                "entry_price": pl.Float64,
                "exit_price": pl.Float64,
                "gross_return": pl.Float64,
                "gross_pnl": pl.Float64,
                "net_pnl": pl.Float64,
                "holding_days": pl.Int64,
                "mfe_pct": pl.Float64,
                "mae_pct": pl.Float64,
                "exit_reason": pl.Utf8,
            }
        )

    @staticmethod
    def _empty_fills() -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "company_code": pl.Utf8,
                "side": pl.Utf8,
                "requested_shares": pl.Float64,
                "filled_shares": pl.Float64,
                "open_price": pl.Float64,
                "fill_price": pl.Float64,
                "notional": pl.Float64,
                "commission": pl.Float64,
                "tax": pl.Float64,
                "slippage_cost": pl.Float64,
                "reason": pl.Utf8,
            }
        )

    def _price(self, day: date, code: str, field: Literal["open", "close"]) -> float:
        bar = self.bars.get((day, code))
        if bar is None:
            return 0.0
        return bar.open if field == "open" else bar.close

    def _execute_order(
        self,
        day: date,
        code: str,
        side: Side,
        desired_notional: float,
        current_shares: float,
        available_cash: float,
        fee_meter: MonthlyFeeMeter,
    ) -> Fill:
        bar = self.bars.get((day, code))
        if bar is None or bar.open <= 0 or bar.close <= 0:
            return self._blocked(day, code, side, 0.0, 0.0, "missing_price")
        if self._is_limit_blocked(side, bar):
            requested = self._round_shares(desired_notional / max(bar.open, 1e-9))
            return self._blocked(day, code, side, requested, bar.open, "limit_blocked")

        requested_shares = self._round_shares(desired_notional / max(bar.open, 1e-9))
        if side == "sell":
            requested_shares = min(requested_shares, self._round_shares(current_shares))
        if requested_shares <= 0:
            return self._blocked(day, code, side, 0.0, bar.open, "below_lot_size")

        max_shares = self._max_fillable_shares(bar)
        fill_shares = min(requested_shares, max_shares)
        reason = "filled"
        if fill_shares <= 0:
            return self._blocked(day, code, side, requested_shares, bar.open, "volume_cap_zero")
        if fill_shares + 1e-9 < requested_shares:
            reason = "partial_volume_cap"

        fill_price = self._fill_price(side, bar, fill_shares)
        if side == "buy":
            volume_capped_shares = fill_shares
            fill_shares = self._affordable_buy_shares(day, fill_shares, fill_price, available_cash, fee_meter)
            if fill_shares <= 0:
                return self._blocked(day, code, side, requested_shares, bar.open, "insufficient_cash")
            if fill_shares + 1e-9 < volume_capped_shares:
                reason = "partial_cash_or_volume"
            fill_price = self._fill_price(side, bar, fill_shares)

        notional = fill_shares * fill_price
        commission = fee_meter.commission(day, notional)
        tax = fee_meter.sell_tax(notional) if side == "sell" else 0.0
        slippage = abs(fill_price - bar.open) * fill_shares
        return Fill(
            date=day,
            company_code=code,
            side=side,
            requested_shares=requested_shares,
            filled_shares=fill_shares,
            open_price=bar.open,
            fill_price=fill_price,
            notional=notional,
            commission=commission,
            tax=tax,
            slippage_cost=slippage,
            reason=reason,
        )

    def _execute_exit_rules(
        self,
        day: date,
        day_index: int,
        shares: dict[str, float],
        positions: dict[str, PositionState],
        cash: float,
        fee_meter: MonthlyFeeMeter,
    ) -> list[tuple[Fill, dict[str, object] | None]]:
        del cash  # sell exits only add cash; affordability is irrelevant.
        out: list[tuple[Fill, dict[str, object] | None]] = []
        for code in sorted(list(shares)):
            pos = positions.get(code)
            qty = shares.get(code, 0.0)
            if pos is None or qty <= 0:
                continue
            trigger = self._exit_trigger(day, day_index, code, pos)
            if trigger is None:
                continue
            reason, trigger_price = trigger
            fill = self._execute_exit_order(day, code, qty, trigger_price, fee_meter, reason)
            trade = self._record_sell_trade(positions, day, day_index, code, fill, reason)
            out.append((fill, trade))
        return out

    def _exit_trigger(
        self,
        day: date,
        day_index: int,
        code: str,
        pos: PositionState,
    ) -> tuple[str, float] | None:
        config = self.config.exit_config
        bar = self.bars.get((day, code))
        if bar is None or bar.open <= 0 or bar.high <= 0 or bar.low <= 0 or pos.entry_price <= 0:
            return None
        if not config.trigger_on_entry_day and day_index <= pos.entry_index:
            return None

        stop_hits: list[tuple[str, float]] = []
        profit_hits: list[tuple[str, float]] = []

        if config.stop_loss_pct is not None and config.stop_loss_pct > 0:
            stop_price = pos.entry_price * (1.0 - config.stop_loss_pct)
            if bar.low <= stop_price:
                stop_hits.append(("stop_loss", stop_price))

        if config.trailing_stop_pct is not None and config.trailing_stop_pct > 0:
            trail_price = pos.peak_price * (1.0 - config.trailing_stop_pct)
            if trail_price > pos.entry_price * 0.01 and bar.low <= trail_price:
                stop_hits.append(("trailing_stop", trail_price))

        if config.breakeven_trigger_pct is not None and config.breakeven_trigger_pct > 0:
            activated = pos.peak_price >= pos.entry_price * (1.0 + config.breakeven_trigger_pct)
            breakeven_price = pos.entry_price * (1.0 + config.breakeven_buffer_pct)
            if activated and bar.low <= breakeven_price:
                stop_hits.append(("breakeven_stop", breakeven_price))

        if config.time_stop_days is not None and config.time_stop_days > 0:
            held_days = day_index - pos.entry_index + 1
            min_return = config.time_stop_min_return_pct
            open_return = bar.open / pos.entry_price - 1.0
            if held_days >= config.time_stop_days and (min_return is None or open_return <= min_return):
                stop_hits.append(("time_stop", bar.open))

        if config.take_profit_pct is not None and config.take_profit_pct > 0:
            take_price = pos.entry_price * (1.0 + config.take_profit_pct)
            if bar.high >= take_price:
                profit_hits.append(("take_profit", take_price))

        if stop_hits and (config.intraday_priority == "stop_first" or not profit_hits):
            return min(stop_hits, key=lambda item: item[1])
        if profit_hits:
            return max(profit_hits, key=lambda item: item[1])
        return None

    def _execute_exit_order(
        self,
        day: date,
        code: str,
        current_shares: float,
        trigger_price: float,
        fee_meter: MonthlyFeeMeter,
        reason: str,
    ) -> Fill:
        bar = self.bars.get((day, code))
        if bar is None or bar.open <= 0 or bar.close <= 0 or trigger_price <= 0:
            return self._blocked(day, code, "sell", 0.0, 0.0, f"{reason}_missing_price")
        requested_shares = self._round_shares(current_shares)
        if requested_shares <= 0:
            return self._blocked(day, code, "sell", 0.0, trigger_price, f"{reason}_below_lot_size")
        if self._is_limit_blocked("sell", bar):
            return self._blocked(day, code, "sell", requested_shares, bar.open, f"{reason}_limit_blocked")

        max_shares = self._max_fillable_shares(bar)
        fill_shares = min(requested_shares, max_shares)
        fill_reason = reason
        if fill_shares <= 0:
            return self._blocked(day, code, "sell", requested_shares, trigger_price, f"{reason}_volume_cap_zero")
        if fill_shares + 1e-9 < requested_shares:
            fill_reason = f"{reason}_partial_volume_cap"

        base_price = self._exit_base_price(reason, bar, trigger_price)
        fill_price = self._fill_price_from_base("sell", bar, fill_shares, base_price)
        notional = fill_shares * fill_price
        commission = fee_meter.commission(day, notional)
        tax = fee_meter.sell_tax(notional)
        slippage = abs(fill_price - base_price) * fill_shares
        return Fill(
            date=day,
            company_code=code,
            side="sell",
            requested_shares=requested_shares,
            filled_shares=fill_shares,
            open_price=base_price,
            fill_price=fill_price,
            notional=notional,
            commission=commission,
            tax=tax,
            slippage_cost=slippage,
            reason=fill_reason,
        )

    @staticmethod
    def _exit_base_price(reason: str, bar: Bar, trigger_price: float) -> float:
        if reason == "take_profit":
            return bar.open if bar.open >= trigger_price else trigger_price
        if reason == "time_stop":
            return bar.open
        return bar.open if bar.open <= trigger_price else trigger_price

    def _blocked(self, day: date, code: str, side: Side, requested: float, open_price: float, reason: str) -> Fill:
        return Fill(day, code, side, requested, 0.0, open_price, open_price, 0.0, 0.0, 0.0, 0.0, reason)

    def _is_limit_blocked(self, side: Side, bar: Bar) -> bool:
        if bar.prev_close is None or bar.prev_close <= 0:
            return False
        tolerance = self.config.limit_tolerance_bps / 10_000.0
        if side == "buy":
            return bar.open >= bar.prev_close * (1.0 + self.config.daily_limit_pct) * (1.0 - tolerance)
        return bar.open <= bar.prev_close * (1.0 - self.config.daily_limit_pct) * (1.0 + tolerance)

    def _round_shares(self, shares: float) -> float:
        shares = max(float(shares), 0.0)
        lot = max(int(self.config.lot_size), 1)
        return float(np.floor(shares / lot) * lot)

    def _max_fillable_shares(self, bar: Bar) -> float:
        if self.config.max_participation_rate <= 0:
            return float("inf")
        return self._round_shares(max(bar.volume, 0.0) * self.config.max_participation_rate)

    def _fill_price(self, side: Side, bar: Bar, shares: float) -> float:
        return self._fill_price_from_base(side, bar, shares, bar.open)

    def _fill_price_from_base(self, side: Side, bar: Bar, shares: float, base_price: float) -> float:
        participation_pct = 100.0 * shares / max(bar.volume, 1.0)
        slip_bps = self.config.fixed_slippage_bps + self.config.impact_bps_per_1pct_volume * participation_pct
        slip = slip_bps / 10_000.0
        if side == "buy":
            slipped = base_price * (1.0 + slip)
            return min(slipped, bar.high if bar.high > 0 else slipped)
        slipped = base_price * (1.0 - slip)
        bounded = max(slipped, bar.low if bar.low > 0 else slipped)
        return min(bounded, bar.high if bar.high > 0 else bounded)

    def _affordable_buy_shares(
        self,
        day: date,
        shares: float,
        price: float,
        available_cash: float,
        fee_meter: MonthlyFeeMeter,
    ) -> float:
        lot = max(int(self.config.lot_size), 1)
        candidate = self._round_shares(shares)
        while candidate > 0:
            notional = candidate * price
            preview = self._preview_commission(fee_meter, day, notional)
            if notional + preview <= available_cash + 1e-6:
                return candidate
            candidate -= lot
        return 0.0

    @staticmethod
    def _preview_commission(fee_meter: MonthlyFeeMeter, day: date, notional: float) -> float:
        schedule = fee_meter.schedule
        used = fee_meter.monthly_notional.get(fee_meter.month_key(day), 0.0)
        low_remaining = max(schedule.monthly_discount_threshold - used, 0.0)
        low_notional = min(notional, low_remaining)
        high_notional = notional - low_notional
        commission = low_notional * schedule.low_tier_rate() + high_notional * schedule.high_tier_rate()
        return max(commission, schedule.minimum_commission) if notional > 0 and schedule.minimum_commission > 0 else commission
