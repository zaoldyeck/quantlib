"""Broker fee schedules for execution-grade simulations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass(frozen=True)
class FubonFeeSchedule:
    """Fubon TW equity fee model.

    The default schedule follows the user's current fee terms:
    standard brokerage commission is 0.1425%, the first NT$1M monthly turnover
    is charged at 1.8-discount, and monthly turnover above NT$1M is charged at
    4-discount. Sell tax is charged separately on sell notional.
    """

    standard_commission_rate: float = 0.001425
    monthly_discount_threshold: float = 1_000_000.0
    discount_under_threshold: float = 0.18
    discount_over_threshold: float = 0.40
    sell_tax_rate: float = 0.003
    minimum_commission: float = 20.0

    def low_tier_rate(self) -> float:
        return self.standard_commission_rate * self.discount_under_threshold

    def high_tier_rate(self) -> float:
        return self.standard_commission_rate * self.discount_over_threshold


@dataclass
class MonthlyFeeMeter:
    """Stateful monthly turnover meter for tiered brokerage commission."""

    schedule: FubonFeeSchedule = field(default_factory=FubonFeeSchedule)
    monthly_notional: dict[int, float] = field(default_factory=dict)

    @staticmethod
    def month_key(trade_date: date) -> int:
        return trade_date.year * 100 + trade_date.month

    def commission(self, trade_date: date, notional: float) -> float:
        """Return commission and advance the monthly turnover meter."""
        notional = max(float(notional), 0.0)
        if notional <= 0:
            return 0.0

        key = self.month_key(trade_date)
        used = self.monthly_notional.get(key, 0.0)
        low_remaining = max(self.schedule.monthly_discount_threshold - used, 0.0)
        low_notional = min(notional, low_remaining)
        high_notional = notional - low_notional
        commission = (
            low_notional * self.schedule.low_tier_rate()
            + high_notional * self.schedule.high_tier_rate()
        )
        if self.schedule.minimum_commission > 0:
            commission = max(commission, self.schedule.minimum_commission)
        self.monthly_notional[key] = used + notional
        return float(commission)

    def sell_tax(self, notional: float) -> float:
        return max(float(notional), 0.0) * self.schedule.sell_tax_rate
