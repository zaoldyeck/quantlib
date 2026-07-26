"""Broker fee schedules for execution-grade simulations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

#: 一張 = 1,000 股(交易所定義);未滿一張即零股
LOT_SIZE = 1_000


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
    #: **零股(<1 張)最低手續費 1 元**(富邦盤中/盤後零股;使用者 2026-07-22 提供)。
    #: 這曾只存在於 trading/live/money.py,導致 order_planner 與 MonthlyFeeMeter 對零股
    #: 一律套 20 元下限、系統性高估成本(2026-07-26 修:低消是費率表的事實,歸這裡)。
    odd_lot_minimum_commission: float = 1.0

    def low_tier_rate(self) -> float:
        return self.standard_commission_rate * self.discount_under_threshold

    def high_tier_rate(self) -> float:
        return self.standard_commission_rate * self.discount_over_threshold

    def minimum_for(self, shares: float) -> float:
        """該筆委託適用的最低手續費:未滿一張走零股下限,整張走整股下限。"""
        return (self.odd_lot_minimum_commission
                if 0 < shares < LOT_SIZE else self.minimum_commission)

    def min_efficient_notional(self, shares: float) -> float:
        """折扣完全生效的最小單筆金額 = 最低收費 / 折後費率。

        低於此金額,實付的是「最低收費」而非折後費率,實質費率隨金額變小而發散
        (零股 3,899 元、整股 77,973 元)。金額再大也降不到費率以下,故這是「單筆
        該下多少錢」的唯一有意義門檻。
        """
        rate = self.low_tier_rate()
        return self.minimum_for(shares) / rate if rate > 0 else 0.0


@dataclass
class MonthlyFeeMeter:
    """Stateful monthly turnover meter for tiered brokerage commission."""

    schedule: FubonFeeSchedule = field(default_factory=FubonFeeSchedule)
    monthly_notional: dict[int, float] = field(default_factory=dict)

    @staticmethod
    def month_key(trade_date: date) -> int:
        return trade_date.year * 100 + trade_date.month

    def commission(self, trade_date: date, notional: float, *, shares: float) -> float:
        """Return commission and advance the monthly turnover meter.

        `shares` 為必填:最低手續費依零股/整股而不同(1 元 vs 20 元),少傳這個參數
        就會對零股高估 19 元。設成 keyword-only 且無預設,是為了讓漏傳當場報錯,
        而不是靜默套用整股下限(2026-07-26 修)。
        """
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
        min_fee = self.schedule.minimum_for(shares)
        if min_fee > 0:
            commission = max(commission, min_fee)
        self.monthly_notional[key] = used + notional
        return float(commission)

    def sell_tax(self, notional: float) -> float:
        return max(float(notional), 0.0) * self.schedule.sell_tax_rate
