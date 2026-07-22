"""Convert validated target weights into broker order plans."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
import json
import math
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import polars as pl

from research.brokers.fubon import StockOrderRequest
from research.execsim.broker_fee import FubonFeeSchedule
from research.trading.live_config import LiveTradingConfig
from research.trading.portfolio import PortfolioSnapshot
from research.trading.strategy_registry import StrategyRegistration


TAIPEI = ZoneInfo("Asia/Taipei")


@dataclass(frozen=True)
class TargetWeight:
    symbol: str
    weight: float
    reference_price: float
    desired_shares: int
    desired_notional: float


@dataclass(frozen=True)
class PlannedOrder:
    symbol: str
    side: str
    quantity: int
    market_type: str
    price_type: str
    time_in_force: str
    order_type: str
    reference_price: float
    estimated_notional: float
    estimated_fee: float
    estimated_tax: float
    user_def: str

    def to_request(self) -> StockOrderRequest:
        return StockOrderRequest(
            symbol=self.symbol,
            side=self.side,
            quantity=self.quantity,
            price_type=self.price_type,
            market_type=self.market_type,
            time_in_force=self.time_in_force,
            order_type=self.order_type,
            price=None,
            user_def=self.user_def,
        )


@dataclass(frozen=True)
class OrderPlan:
    plan_id: str
    created_at: str
    strategy_id: str
    strategy_name: str
    data_cutoff: str
    target_date: str
    expected_submit_after: str
    capital_ceiling_twd: float
    broker_available_balance_twd: float | None
    deployable_capital_twd: float
    cash_buffer_pct: float
    cash_buffer_twd: float
    current_positions: dict[str, int]
    desired_positions: dict[str, int]
    targets: list[TargetWeight]
    orders: list[PlannedOrder]
    diagnostics: dict[str, float | int | str | None]
    placed_order: bool = False

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        return payload

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def next_weekday(day: date) -> date:
    candidate = day + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def expected_submit_after(target_date: date) -> str:
    submit_day = next_weekday(target_date)
    return datetime.combine(submit_day, time(8, 30), tzinfo=TAIPEI).isoformat()


def load_latest_target_weights(path: Path) -> tuple[date, dict[str, float]]:
    df = pl.read_csv(
        path,
        try_parse_dates=True,
        schema_overrides={"company_code": pl.Utf8, "target_weight": pl.Float64},
    )
    if df.is_empty():
        raise ValueError(f"Target weights file is empty: {path}")
    latest = df["date"].max()
    rows = df.filter(pl.col("date") == latest)
    weights = {
        str(row["company_code"]).zfill(4): float(row["target_weight"])
        for row in rows.to_dicts()
        if float(row["target_weight"]) > 0
    }
    return latest, weights


def latest_daily_quote_date(cache_db: Path) -> date:
    with duckdb.connect(str(cache_db), read_only=True) as con:
        value = con.execute("select max(date) from daily_quote").fetchone()[0]
    if value is None:
        raise RuntimeError(f"No daily_quote rows found in {cache_db}.")
    return value


def load_latest_close_prices(cache_db: Path, codes: set[str], price_date: date) -> dict[str, float]:
    if not codes:
        return {}
    safe_codes = sorted(code for code in codes if code.isdigit())
    if len(safe_codes) != len(codes):
        raise ValueError("Company codes must be numeric strings.")
    quoted = ",".join(f"'{code}'" for code in safe_codes)
    sql = f"""
        select company_code::varchar as symbol, closing_price::double as close
        from daily_quote
        where date = ? and company_code::varchar in ({quoted})
    """
    with duckdb.connect(str(cache_db), read_only=True) as con:
        rows = con.execute(sql, [price_date]).fetchall()
    prices = {str(symbol).zfill(4): float(close) for symbol, close in rows if close is not None}
    missing = sorted(codes - set(prices))
    if missing:
        raise RuntimeError(f"Missing close prices on {price_date}: {', '.join(missing)}")
    return prices


def commission_estimate(notional: float, schedule: FubonFeeSchedule) -> float:
    if notional <= 0:
        return 0.0
    return max(notional * schedule.low_tier_rate(), schedule.minimum_commission)


def split_order_quantity(
    quantity: int,
    *,
    side: str,
    reference_price: float,
    user_def: str,
    order_price_policy: str = "limit_up_down",
) -> list[PlannedOrder]:
    if quantity <= 0:
        return []
    schedule = FubonFeeSchedule()
    chunks: list[tuple[int, str]] = []
    board = (quantity // 1000) * 1000
    odd = quantity - board
    if board:
        chunks.append((board, "Common"))
    if odd:
        chunks.append((odd, "IntradayOdd"))

    if side not in {"Buy", "Sell"}:
        raise ValueError(f"Unsupported side: {side}")
    if order_price_policy == "limit_up_down":
        price_type = "LimitUp" if side == "Buy" else "LimitDown"
    elif order_price_policy == "market":
        price_type = "Market"
    else:
        raise ValueError(f"Unsupported order_price_policy: {order_price_policy}")

    orders: list[PlannedOrder] = []
    for qty, market_type in chunks:
        notional = qty * reference_price
        orders.append(
            PlannedOrder(
                symbol="",
                side=side,
                quantity=qty,
                market_type=market_type,
                price_type=price_type,
                time_in_force="ROD",
                order_type="Stock",
                reference_price=reference_price,
                estimated_notional=notional,
                estimated_fee=commission_estimate(notional, schedule),
                estimated_tax=(notional * schedule.sell_tax_rate if side == "Sell" else 0.0),
                user_def=user_def,
            )
        )
    return orders


def _with_symbol(order: PlannedOrder, symbol: str) -> PlannedOrder:
    return PlannedOrder(**{**asdict(order), "symbol": symbol})


def minimum_capital_for_full_basket(
    weights: dict[str, float],
    prices: dict[str, float],
    schedule: FubonFeeSchedule | None = None,
    *,
    buy_price_buffer_pct: float = 0.0,
    cash_buffer_pct: float = 0.0,
    cash_buffer_twd: float = 0.0,
) -> float:
    schedule = schedule or FubonFeeSchedule()
    required_deployable = 0.0
    for code, weight in weights.items():
        if weight <= 0:
            continue
        one_share_cost = prices[code] * (1.0 + buy_price_buffer_pct) + schedule.minimum_commission
        required_deployable = max(required_deployable, one_share_cost / weight)
    denominator = max(1.0 - cash_buffer_pct, 1e-9)
    return (required_deployable + cash_buffer_twd) / denominator


def build_order_plan(
    *,
    strategy: StrategyRegistration,
    cache_db: Path,
    config: LiveTradingConfig,
    current: PortfolioSnapshot,
    broker_available_balance: float | None = None,
) -> OrderPlan:
    if strategy.target_weights_path is None:
        raise RuntimeError(f"Strategy {strategy.strategy_id} has no target_weights_path.")

    target_date, weights = load_latest_target_weights(strategy.target_weights_path)
    data_cutoff = latest_daily_quote_date(cache_db)
    if data_cutoff < target_date:
        raise RuntimeError(
            f"Cache daily_quote cutoff {data_cutoff} is older than target-book date {target_date}."
        )
    current_positions = current.normalized_positions()
    prices = load_latest_close_prices(
        cache_db,
        set(weights) | set(current_positions),
        data_cutoff,
    )
    deployable_capital = config.deployable_capital(broker_available_balance)
    if deployable_capital <= 0:
        raise RuntimeError("Deployable capital is zero after cash buffers.")

    desired_positions: dict[str, int] = {}
    target_rows: list[TargetWeight] = []
    buy_price_multiplier = 1.0 + config.buy_price_buffer_pct
    for code, weight in sorted(weights.items()):
        reference_price = prices[code]
        desired = math.floor((deployable_capital * weight) / (reference_price * buy_price_multiplier))
        desired = max(desired, 0)
        desired_positions[code] = desired
        target_rows.append(
            TargetWeight(
                symbol=code,
                weight=weight,
                reference_price=reference_price,
                desired_shares=desired,
                desired_notional=desired * reference_price,
            )
        )

    for code in current_positions:
        desired_positions.setdefault(code, 0)

    now = datetime.now(TAIPEI)
    plan_id = f"{strategy.strategy_id}_{data_cutoff:%Y%m%d}_{now:%H%M%S}"
    user_def = f"QL95-{data_cutoff:%Y%m%d}"

    deltas: list[tuple[int, str, str, int, float]] = []
    for code in sorted(desired_positions):
        current_qty = current_positions.get(code, 0)
        desired_qty = desired_positions.get(code, 0)
        delta = desired_qty - current_qty
        if delta == 0:
            continue
        reference_price = prices.get(code)
        if reference_price is None:
            continue
        side = "Buy" if delta > 0 else "Sell"
        quantity = abs(delta)
        notional = quantity * reference_price
        if side == "Buy" and notional < config.min_order_notional_twd:
            continue
        side_priority = 0 if side == "Sell" else 1
        deltas.append((side_priority, code, side, quantity, reference_price))

    orders: list[PlannedOrder] = []
    for _side_priority, code, side, quantity, reference_price in sorted(deltas):
        orders.extend(
            _with_symbol(order, code)
            for order in split_order_quantity(
                quantity,
                side=side,
                reference_price=reference_price,
                user_def=user_def,
                order_price_policy=config.order_price_policy,
            )
        )

    estimated_buy_notional = sum(o.estimated_notional for o in orders if o.side == "Buy")
    estimated_sell_notional = sum(o.estimated_notional for o in orders if o.side == "Sell")
    estimated_fees = sum(o.estimated_fee for o in orders)
    estimated_tax = sum(o.estimated_tax for o in orders)
    diagnostics = {
        "target_weight_sum": sum(weights.values()),
        "target_count": len(weights),
        "order_count": len(orders),
        "estimated_buy_notional": estimated_buy_notional,
        "estimated_sell_notional": estimated_sell_notional,
        "estimated_fees": estimated_fees,
        "estimated_tax": estimated_tax,
        "minimum_capital_for_one_share_per_target": minimum_capital_for_full_basket(
            weights,
            prices,
            buy_price_buffer_pct=config.buy_price_buffer_pct,
            cash_buffer_pct=config.cash_buffer_pct,
            cash_buffer_twd=config.cash_buffer_twd,
        ),
        "price_reference": f"raw close from daily_quote on data_cutoff {data_cutoff}",
        "submit_model": "prepare after close; submit next trading morning",
    }
    return OrderPlan(
        plan_id=plan_id,
        created_at=now.isoformat(timespec="seconds"),
        strategy_id=strategy.strategy_id,
        strategy_name=strategy.name,
        data_cutoff=data_cutoff.isoformat(),
        target_date=target_date.isoformat(),
        expected_submit_after=expected_submit_after(data_cutoff),
        capital_ceiling_twd=config.strategy_capital_twd,
        broker_available_balance_twd=broker_available_balance,
        deployable_capital_twd=deployable_capital,
        cash_buffer_pct=config.cash_buffer_pct,
        cash_buffer_twd=config.cash_buffer_twd,
        current_positions=current_positions,
        desired_positions=desired_positions,
        targets=target_rows,
        orders=orders,
        diagnostics=diagnostics,
    )
