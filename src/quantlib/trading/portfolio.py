"""Portfolio snapshots and local managed-position ledger.

The local ledger is deliberate. A brokerage inventory query cannot distinguish
bot-managed shares from manual holdings in the same account. Live pilot should
start with an empty managed ledger or a user-approved imported ledger.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PortfolioSnapshot:
    as_of: str
    positions: dict[str, int] = field(default_factory=dict)
    cash_available_twd: float | None = None
    source: str = "local_ledger"

    def normalized_positions(self) -> dict[str, int]:
        return {
            str(code).zfill(4): int(qty)
            for code, qty in self.positions.items()
            if int(qty) != 0
        }


@dataclass(frozen=True)
class TradeFill:
    symbol: str
    side: str
    quantity: int
    price: float | None = None
    user_def: str | None = None


def empty_snapshot(source: str = "local_ledger") -> PortfolioSnapshot:
    return PortfolioSnapshot(
        as_of=datetime.now().isoformat(timespec="seconds"),
        positions={},
        source=source,
    )


def load_managed_positions(path: Path) -> PortfolioSnapshot:
    if not path.exists():
        return empty_snapshot()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return PortfolioSnapshot(
        as_of=str(payload.get("as_of") or datetime.now().isoformat(timespec="seconds")),
        positions={
            str(code).zfill(4): int(qty)
            for code, qty in (payload.get("positions") or {}).items()
        },
        cash_available_twd=payload.get("cash_available_twd"),
        source=str(payload.get("source") or "local_ledger"),
    )


def save_managed_positions(path: Path, snapshot: PortfolioSnapshot) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "as_of": snapshot.as_of,
        "source": snapshot.source,
        "cash_available_twd": snapshot.cash_available_twd,
        "positions": snapshot.normalized_positions(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _int_attr(obj: Any, name: str) -> int:
    value = getattr(obj, name, 0) or 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def positions_from_fubon_inventories(result: Any) -> dict[str, int]:
    """現時持股 = 整股層 today_qty + 零股層 today_qty。

    不可用 tradable_qty + lastday_qty:兩者對「昨日起持有」的部位重複計數
    (川湖 1 股被算成 2 的根因,2026-07-09 修正)、對「今日新買零股」漏計
    (T+1 才 tradable)、對「今日已賣光」殘留(lastday 仍為 1)。
    today_qty 是券商對「此刻持有」的權威欄位,含今日成交增減。
    """
    if not getattr(result, "is_success", False):
        raise RuntimeError(f"Fubon inventory query failed: {getattr(result, 'message', None)}")
    positions: dict[str, int] = {}
    for item in getattr(result, "data", []) or []:
        code = getattr(item, "stock_no", None) or getattr(item, "symbol", None)
        if not code:
            continue
        qty = _int_attr(item, "today_qty")
        odd = getattr(item, "odd", None)
        if odd is not None:
            qty += _int_attr(odd, "today_qty")
        if qty:
            positions[str(code).zfill(4)] = positions.get(str(code).zfill(4), 0) + qty
    return positions


def available_balance_from_fubon_bank_remain(result: Any) -> float:
    if not getattr(result, "is_success", False):
        raise RuntimeError(f"Fubon bank balance query failed: {getattr(result, 'message', None)}")
    data = getattr(result, "data", None)
    value = getattr(data, "available_balance", None)
    if value is None:
        raise RuntimeError("Fubon bank balance query returned no available_balance.")
    return float(value)


def apply_trade_fills(snapshot: PortfolioSnapshot, fills: list[TradeFill]) -> PortfolioSnapshot:
    positions = snapshot.normalized_positions()
    for fill in fills:
        code = fill.symbol.zfill(4)
        qty = int(fill.quantity)
        if qty <= 0:
            continue
        if fill.side == "Buy":
            positions[code] = positions.get(code, 0) + qty
        elif fill.side == "Sell":
            positions[code] = positions.get(code, 0) - qty
            if positions[code] <= 0:
                positions.pop(code, None)
        else:
            raise ValueError(f"Unsupported fill side: {fill.side}")
    return PortfolioSnapshot(
        as_of=datetime.now().isoformat(timespec="seconds"),
        positions=positions,
        cash_available_twd=snapshot.cash_available_twd,
        source=snapshot.source,
    )


def inventory_mismatches(
    *,
    managed_positions: dict[str, int],
    broker_positions: dict[str, int],
    symbols: set[str],
) -> dict[str, dict[str, int]]:
    mismatches: dict[str, dict[str, int]] = {}
    for symbol in sorted({code.zfill(4) for code in symbols}):
        managed = int(managed_positions.get(symbol, 0))
        broker = int(broker_positions.get(symbol, 0))
        if managed != broker:
            mismatches[symbol] = {"managed": managed, "broker": broker}
    return mismatches


def fills_from_order_plan(plan: dict[str, Any]) -> list[TradeFill]:
    fills: list[TradeFill] = []
    for raw in plan.get("orders", []) or []:
        fills.append(
            TradeFill(
                symbol=str(raw["symbol"]).zfill(4),
                side=str(raw["side"]),
                quantity=int(raw["quantity"]),
                price=float(raw["reference_price"]) if raw.get("reference_price") is not None else None,
                user_def=raw.get("user_def"),
            )
        )
    return fills


def _side_from_fubon(value: Any) -> str | None:
    text = str(value)
    if text.endswith(".Buy") or text.lower() == "buy" or "買" in text:
        return "Buy"
    if text.endswith(".Sell") or text.lower() == "sell" or "賣" in text:
        return "Sell"
    return None


def fills_from_fubon_result(result: Any, *, user_def: str | None = None) -> list[TradeFill]:
    if not getattr(result, "is_success", False):
        raise RuntimeError(f"Fubon filled-history query failed: {getattr(result, 'message', None)}")
    fills: list[TradeFill] = []
    for item in getattr(result, "data", []) or []:
        item_user_def = getattr(item, "user_def", None)
        if user_def and item_user_def != user_def:
            continue
        symbol = (
            getattr(item, "stock_no", None)
            or getattr(item, "symbol", None)
            or getattr(item, "stock_id", None)
        )
        side = _side_from_fubon(getattr(item, "buy_sell", None) or getattr(item, "side", None))
        qty = getattr(item, "filled_qty", None) or getattr(item, "quantity", None)
        if not symbol or not side or qty is None:
            continue
        price = getattr(item, "filled_avg_price", None) or getattr(item, "filled_price", None)
        fills.append(
            TradeFill(
                symbol=str(symbol).zfill(4),
                side=side,
                quantity=int(qty),
                price=float(price) if price is not None else None,
                user_def=item_user_def,
            )
        )
    return fills
