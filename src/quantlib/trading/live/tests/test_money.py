"""計劃信金額試算守護:費稅正確、ROI 語義正確、資金不足要喊。

使用者需求(2026-07-22):信裡要有買賣現價、賣出 ROI 與持有損益、帳戶資金、
預計增減、不足要提醒補交割。金額是 money-path 的資訊面——算錯會誤導決策。

**費率事實來源**(2026-07-22 更正,先前誤用 2 折 + 整股 20 元下限):
- 折數與證交稅一律取自 `src/quantlib/execsim/broker_fee.py`(1.8 折、稅 0.3%),
  那份 schedule 同時餵回測與執行模擬,此處不得另立常數。
- 零股最低手續費 **1 元**:富邦官方「盤中零股單筆手續費未滿新台幣 1 元者,
  按 1 元計收」。對 1 股營運是決定性的——用整股的 20 元下限估,單筆 45 元的買進
  會被高估 19 元成本,ROI 直接失真數十個百分點。

Run: uv run --project . python -m quantlib.trading.live.tests.test_money
"""
from __future__ import annotations

from quantlib.trading.live.money import (COMMISSION_MIN, COMMISSION_RATE,
                                         LOT_SIZE, ODD_LOT_COMMISSION_MIN, Leg,
                                         build_settlement, commission_min,
                                         fee_buy, fee_sell)


def test_rate_comes_from_broker_fee_schedule() -> None:
    """費率必須來自唯一真源(1.8 折),不得在此另立常數而與回測靜默漂移。"""
    from quantlib.execsim.broker_fee import FubonFeeSchedule
    s = FubonFeeSchedule()
    assert abs(COMMISSION_RATE - s.standard_commission_rate * 0.18) < 1e-12
    assert COMMISSION_RATE < s.standard_commission_rate * 0.2, "1.8 折應低於 2 折"


def test_odd_lot_minimum_is_one_dollar() -> None:
    """1 股小額單:最低收費 1 元(不是整股的 20 元)——這是 1 股營運的真實成本。"""
    assert commission_min(1) == ODD_LOT_COMMISSION_MIN == 1.0
    assert commission_min(999) == 1.0
    assert commission_min(LOT_SIZE) == COMMISSION_MIN == 20.0
    assert fee_buy(45.0, shares=1) == 1.0            # 45 × 0.02565% ≈ 0.012 → 補到 1
    assert abs(fee_sell(45.0, shares=1) - (1.0 + 45.0 * 0.003)) < 1e-9


def test_whole_lot_keeps_twenty_dollar_minimum() -> None:
    """整張單仍是 20 元下限——未來放大股數時不得沿用零股優惠。"""
    assert fee_buy(1000.0, shares=LOT_SIZE) == COMMISSION_MIN


def test_large_amount_uses_rate_not_minimum() -> None:
    amt = 1_000_000.0
    assert fee_buy(amt, shares=LOT_SIZE) > COMMISSION_MIN
    assert abs(fee_buy(amt, shares=LOT_SIZE) - amt * COMMISSION_RATE) < 1e-6


def test_buy_leg_reduces_cash_including_fee() -> None:
    leg = Leg("2886", "buy", 1, 40.0)
    assert leg.net == -(40.0 + 1.0)          # 1 股 → 零股下限 1 元


def test_sell_roi_and_pnl_are_net_of_fees() -> None:
    """ROI 必須是**扣掉賣出費稅後**的真實報酬——否則會高估獲利誤導決策。"""
    leg = Leg("2466", "sell", 1, 120.0, cost=100.0)
    expected_net = 120.0 - (1.0 + 120.0 * 0.003)
    assert abs(leg.net - expected_net) < 1e-9
    assert abs(leg.pnl - (expected_net - 100.0)) < 1e-9
    assert abs(leg.roi - (expected_net - 100.0) / 100.0) < 1e-9


def test_odd_lot_fee_no_longer_eats_the_position() -> None:
    """回歸守護:同一筆若誤用 20 元下限,ROI 會從約 −3% 惡化到約 −22%。"""
    leg = Leg("2466", "sell", 1, 120.0, cost=122.0)
    assert leg.roi > -0.05, f"1 股賣出的 ROI 不該被手續費吃掉:{leg.roi:.1%}"


def test_breakdown_spells_out_every_component() -> None:
    """使用者 2026-07-22:「約 −50 元(含費)=> 含費是什麼?沒寫完整」。
    金額拆解必須讓人自己加得回來,且**保留到分**(證交稅常是零點幾元)。"""
    b = Leg("2886", "buy", 1, 48.60)
    assert b.breakdown == "股款 48.60 + 手續費 1.00"
    assert abs(b.net + (48.60 + 1.00)) < 1e-9, "拆解要與淨額對得起來"
    s = Leg("2466", "sell", 1, 119.0)
    assert s.breakdown == "股款 119.00 − 手續費 1.00 − 證交稅 0.36"
    assert abs(s.net - (119.0 - 1.0 - 119.0 * 0.003)) < 1e-9
    assert "含費" not in b.breakdown and "含費" not in s.breakdown


def test_breakdown_never_crashes_without_price() -> None:
    assert Leg("9999", "buy", 1, None).breakdown == "無報價"


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
    assert abs(s.buy_cost - ((40 + 1) + (30 + 1))) < 1e-9
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


def test_no_negative_zero_in_display() -> None:
    """無買進腿時 buy_cost 必須是 +0.0(否則版面印出「−-0 元」)。"""
    s = build_settlement(cash=1.0, buys=[], sells=[], shares_per_buy=1,
                         holdings={}, prices={})
    assert s.buy_cost == 0.0 and str(s.buy_cost) == "0.0"


def test_shortfall_ignores_sell_proceeds_conservatively() -> None:
    """賣出款 T+2 才入帳:即使今日有賣出,現金不足買進仍須提醒(寧可多提醒)。"""
    s = build_settlement(cash=1.0, buys=["2886"], sells=["2466"],
                         shares_per_buy=1, holdings={"2466": 1},
                         prices={"2886": 400.0, "2466": 10_000.0})
    assert s.shortfall > 0, "有大額賣出就不提醒 = 可能交割不足"


def main() -> None:
    for fn in (test_rate_comes_from_broker_fee_schedule,
               test_odd_lot_minimum_is_one_dollar,
               test_whole_lot_keeps_twenty_dollar_minimum,
               test_large_amount_uses_rate_not_minimum,
               test_buy_leg_reduces_cash_including_fee,
               test_sell_roi_and_pnl_are_net_of_fees,
               test_odd_lot_fee_no_longer_eats_the_position,
               test_breakdown_spells_out_every_component,
               test_breakdown_never_crashes_without_price,
               test_sell_without_cost_has_no_roi,
               test_missing_price_never_crashes_or_fabricates,
               test_settlement_flags_shortfall,
               test_settlement_no_shortfall_when_cash_enough,
               test_settlement_net_change_with_both_sides,
               test_no_negative_zero_in_display,
               test_shortfall_ignores_sell_proceeds_conservatively):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ money 全過")


if __name__ == "__main__":
    main()
