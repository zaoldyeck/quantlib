"""Live-pilot trading configuration.

The broker account balance is an execution guard, not the strategy mandate.
The strategy must always have an explicit capital ceiling so one brokerage
account can safely contain unrelated cash or manual holdings.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from research.brokers.fubon import DEFAULT_ENV_PATH, load_env_file
from research import paths


STATE_DIR = Path(f"{paths.STATE}/trading")
OUT_DIR = Path(f"{paths.OUT}/trading")


@dataclass(frozen=True)
class LiveTradingConfig:
    strategy_capital_twd: float
    cash_buffer_twd: float = 0.0
    cash_buffer_pct: float = 0.03
    buy_price_buffer_pct: float = 0.10
    min_order_notional_twd: float = 0.0
    order_price_policy: str = "limit_up_down"
    managed_positions_path: Path = STATE_DIR / "managed_positions.json"
    plans_dir: Path = OUT_DIR / "plans"

    @classmethod
    def from_env(
        cls,
        env_path: Path = DEFAULT_ENV_PATH,
        *,
        require_capital: bool = True,
    ) -> "LiveTradingConfig":
        load_env_file(env_path)
        raw_capital = os.environ.get("QL_STRATEGY_CAPITAL_TWD")
        if require_capital and not raw_capital:
            raise ValueError(
                "Missing QL_STRATEGY_CAPITAL_TWD. Set the maximum capital this "
                "strategy is allowed to control before generating live orders."
            )
        capital = float(raw_capital or 0.0)
        return cls(
            strategy_capital_twd=capital,
            cash_buffer_twd=float(os.environ.get("QL_CASH_BUFFER_TWD", "0") or 0),
            cash_buffer_pct=float(os.environ.get("QL_CASH_BUFFER_PCT", "0.03") or 0.03),
            buy_price_buffer_pct=float(
                os.environ.get("QL_BUY_PRICE_BUFFER_PCT", "0.10") or 0.10
            ),
            min_order_notional_twd=float(
                os.environ.get("QL_MIN_ORDER_NOTIONAL_TWD", "0") or 0
            ),
            order_price_policy=os.environ.get(
                "QL_ORDER_PRICE_POLICY", "limit_up_down"
            ).strip().lower(),
            managed_positions_path=Path(
                os.environ.get(
                    "QL_MANAGED_POSITIONS_PATH",
                    str(STATE_DIR / "managed_positions.json"),
                )
            ),
            plans_dir=Path(os.environ.get("QL_PLANS_DIR", str(OUT_DIR / "plans"))),
        )

    def deployable_capital(self, broker_available_balance: float | None = None) -> float:
        capital = self.strategy_capital_twd
        if broker_available_balance is not None:
            capital = min(capital, float(broker_available_balance))
        buffered = capital * (1.0 - self.cash_buffer_pct) - self.cash_buffer_twd
        return max(buffered, 0.0)

