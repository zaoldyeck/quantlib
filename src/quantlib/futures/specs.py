"""Contract, cost, and margin specifications for TAIFEX futures research."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FuturesContractSpec:
    product: str
    multiplier: float
    tick_size: float
    tax_rate: float = 0.00002

    def notional(self, price: float, contracts: float = 1.0) -> float:
        return abs(float(price) * self.multiplier * float(contracts))

    def ticks_to_price(self, ticks: float) -> float:
        return float(ticks) * self.tick_size


@dataclass(frozen=True)
class FuturesCostConfig:
    """Execution cost model.

    `commission_by_product` is a conservative all-in per-contract/per-side
    estimate including broker, exchange, and clearing fees.  Tax is modeled from
    contract notional through each product's `tax_rate`.
    """

    commission_by_product: dict[str, float] = field(
        default_factory=lambda: {
            "TX": 70.0,
            "MTX": 30.0,
            "TMF": 12.0,
            "TE": 70.0,
            "TF": 70.0,
        }
    )
    slippage_ticks: float = 1.0
    roll_extra_ticks: float = 1.0
    cost_multiplier: float = 1.0

    def commission(self, product: str, contracts: float) -> float:
        return self.cost_multiplier * self.commission_by_product.get(product, 70.0) * abs(float(contracts))


@dataclass(frozen=True)
class FuturesMarginConfig:
    """Survival-constrained margin model.

    The project does not yet cache a point-in-time official margin table, so the
    research engine uses a conservative notional-ratio proxy and records margin
    buffers explicitly.  Live execution must replace this with broker/TAIFEX
    margin before promotion beyond research.
    """

    initial_margin_ratio: float = 0.135
    maintenance_margin_ratio: float = 0.105
    required_buffer: float = 1.35
    liquidation_buffer: float = 1.00
    max_notional_leverage: float = 6.0
    stress_notional_move: float = 0.12

    def initial_margin(self, notional: float) -> float:
        return abs(float(notional)) * self.initial_margin_ratio

    def maintenance_margin(self, notional: float) -> float:
        return abs(float(notional)) * self.maintenance_margin_ratio

    def stress_loss(self, notional: float) -> float:
        return abs(float(notional)) * self.stress_notional_move


CONTRACT_SPECS: dict[str, FuturesContractSpec] = {
    "TX": FuturesContractSpec("TX", multiplier=200.0, tick_size=1.0),
    "MTX": FuturesContractSpec("MTX", multiplier=50.0, tick_size=1.0),
    "TMF": FuturesContractSpec("TMF", multiplier=10.0, tick_size=1.0),
    "TE": FuturesContractSpec("TE", multiplier=4000.0, tick_size=0.05),
    "TF": FuturesContractSpec("TF", multiplier=1000.0, tick_size=0.2),
}


def contract_spec(product: str) -> FuturesContractSpec:
    try:
        return CONTRACT_SPECS[product]
    except KeyError as exc:
        raise KeyError(f"Unsupported TAIFEX futures product: {product}") from exc
