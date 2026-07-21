"""計劃信金額試算守護:費稅正確、ROI 語義正確、資金不足要喊。

使用者需求(2026-07-22):信裡要有買賣現價、賣出 ROI 與持有損益、帳戶資金、
預計增減、不足要提醒補交割。金額是money-path 的資訊面——算錯會誤導決策。

Run: uv run --project research python -m research.trading.live.tests.test_money
"""
from __future__ import annotations

from research.trading.live.money import (COMMISSION_MIN, Leg, build_settlement,
                                         fee_buy, fee_sell)


def test_min_commission_dominates_odd_lot() -> None:
    """1 股小額單:手續費由最低 20 元決定(這正是零股營運的真實成本結構)。"""
    assert fee_buy(45.0) == COMMISSION_MIN
    # 賣出 = 最低手續費 + 證交稅
    assert abs(fee_sell(45.0) - (COMMISSION_MIN + 45.0 * 0.003)) < 1e-9


def test_large_amount_uses_rate_not_minimum() -> None:
    amt = 1_000_000.0
    assert fee_buy(amt) > COMMISSION_MIN
    assert abs(fee_buy(amt) - amt * 0.001425 * 0.2) < 1e-6


def test_buy_leg_reduces_cash_including_fee() -> None:
    leg = Leg("2886", "buy", 1, 40.0)
    assert leg.net == -(40.0 + COMMISSION_MIN)


def test_sell_roi_and_pnl_are_net_of_fees() -> None:
    """ROI 必須是**扣掉賣出費稅後**的真實報酬——否則會高估獲利誤導決策。"""
    leg = Leg("2466", "sell", 1, 120.0, cost=100.0)
    expected_net = 120.0 - (COMMISSION_MIN + 120.0 * 0.003)
    assert abs(leg.net - expected_net) < 1e-9
    assert abs(leg.pnl - (expected_net - 100.0)) < 1e-9
    assert abs(leg.roi - (expected_net - 100.0) / 100.0) < 1e-9


def test_sell_without_cost_has_no_roi() -> None:
    """無成本資料 → 不得瞎編 ROI(寧可不顯示)。"""
    leg = Leg("9999", "sell", 1, 50.0)
    assert leg.roi is None and leg.pnl is None


def test_missing_price_never_crashes_or_fabricates() -> None:
    """缺價(停牌/下市)→ 金額以 0 計、不拋、不虛構。"""
    leg = Leg("9999", "buy", 1, None)
    assert leg.net == 0.0 and leg.amount == 0.0


def test_settlement_flags_shortfall() -> None:
    """資金不足要算得出缺口(提醒補交割)。"""
    s = build_settlement(cash=50.0, buys=["2886", "1907"], sells=[],
                         shares_per_buy=1, holdings={},
                         prices={"2886": 40.0, "1907": 30.0})
    # 兩筆買進各 (價 + 20 元最低手續費)
    assert abs(s.buy_cost - ((40 + 20) + (30 + 20))) < 1e-9
    assert s.shortfall > 0
    assert abs(s.shortfall - (s.buy_cost - 50.0)) < 1e-9


def test_settlement_no_shortfall_when_cash_enough() -> None:
    s = build_settlement(cash=100_000.0, buys=["2886"], sells=[],
                         shares_per_buy=1, holdings={}, prices={"2886": 40.0})
    assert s.shortfall == 0.0
    assert s.net_change < 0            # 只買 → 現金淨減少


def test_settlement_net_change_with_both_sides() -> None:
    s = build_settlement(cash=10_000.0, buys=["2886"], sells=["2466"],
                         shares_per_buy=1, holdings={"2466": 1},
                         prices={"2886": 40.0, "2466": 120.0},
                         costs={"2466": (100.0, "收養價")})
    assert abs(s.net_change - (s.sell_proceeds - s.buy_cost)) < 1e-9
    assert s.sell_proceeds > 0 and s.buy_cost > 0


def test_shortfall_ignores_sell_proceeds_conservatively() -> None:
    """賣出款 T+2 才入帳:即使今日有賣出,現金不足買進仍須提醒(寧可多提醒)。"""
    s = build_settlement(cash=10.0, buys=["2886"], sells=["2466"],
                         shares_per_buy=1, holdings={"2466": 1},
                         prices={"2886": 40.0, "2466": 10_000.0})
    assert s.shortfall > 0, "有大額賣出就不提醒 = 可能交割不足"


def main() -> None:
    for fn in (test_min_commission_dominates_odd_lot,
               test_large_amount_uses_rate_not_minimum,
               test_buy_leg_reduces_cash_including_fee,
               test_sell_roi_and_pnl_are_net_of_fees,
               test_sell_without_cost_has_no_roi,
               test_missing_price_never_crashes_or_fabricates,
               test_settlement_flags_shortfall,
               test_settlement_no_shortfall_when_cash_enough,
               test_settlement_net_change_with_both_sides,
               test_shortfall_ignores_sell_proceeds_conservatively):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ money 全過")


if __name__ == "__main__":
    main()
