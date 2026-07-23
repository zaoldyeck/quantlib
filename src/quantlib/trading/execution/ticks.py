"""台股 tick 級距與價格貼齊(純函式)。"""

from __future__ import annotations

import math

_TICK_TABLE = (
    (10.0, 0.01),
    (50.0, 0.05),
    (100.0, 0.10),
    (500.0, 0.50),
    (1000.0, 1.00),
    (float("inf"), 5.00),
)


def tick_size(price: float) -> float:
    for ceiling, tick in _TICK_TABLE:
        if price < ceiling:
            return tick
    return 5.0


def snap_down(price: float) -> float:
    """貼齊到不高於 price 的合法檔位(買方保守)。"""
    tick = tick_size(price)
    return round(math.floor(price / tick + 1e-9) * tick, 2)


def snap_up(price: float) -> float:
    """貼齊到不低於 price 的合法檔位(賣方保守)。"""
    tick = tick_size(price)
    return round(math.ceil(price / tick - 1e-9) * tick, 2)


def add_ticks(price: float, n: int) -> float:
    """從 price 起跳 n 檔(n 可為負);逐檔走避免跨級距誤差。"""
    p = snap_down(price) if n >= 0 else snap_up(price)
    for _ in range(abs(n)):
        step = tick_size(p + 1e-9) if n > 0 else tick_size(p - 1e-9)
        p = round(p + (step if n > 0 else -step), 2)
    return p
