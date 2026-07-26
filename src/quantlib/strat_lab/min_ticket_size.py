"""單筆該下多少錢才划算?——最低手續費把「小單」的實質費率推高多少。

## 問題(使用者 2026-07-26)
「手續費最低要收 1 元,我的手續費是 1.8 折,買一檔標的至少花多少錢才划算?」

## 費率(全部取自唯一真源 execsim/broker_fee.py + trading/live/money.py,不另立常數)
- 標準手續費 0.1425%,**1.8 折** → 0.02565%(月成交額 100 萬內);超過部分 4 折 → 0.057%
- 賣出證交稅 0.3%(不打折、無低消)
- 最低手續費**分兩種**:整股(≥1,000 股)20 元、**零股(<1,000 股)1 元**

## 「划算」的定義
最低收費是**固定成本**,單筆越小、攤到金額上的實質費率越高。故臨界點 =
「按折扣算出來的手續費剛好等於最低收費」的金額——在那之上,折扣才算真的拿到;
在那之下,你付的是最低收費,實質費率隨金額變小而發散。

    臨界金額 = 最低收費 / 折後費率

Run: uv run --project . python -m quantlib.strat_lab.min_ticket_size
依賴 cache: 是(要算 S 每筆報酬當作摩擦的分母)。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.strategy_s import DS, prep_cached, run_s_full
from quantlib.trading.live.money import (
    COMMISSION_MIN,
    COMMISSION_RATE,
    ODD_LOT_COMMISSION_MIN,
    SELL_TAX_RATE,
)
from quantlib.execsim.broker_fee import FubonFeeSchedule

_S = FubonFeeSchedule()


def round_trip_cost(x: float, min_fee: float, rate: float = COMMISSION_RATE) -> float:
    """單筆金額 x 的往返總摩擦(手續費買+賣 + 證交稅),回傳佔金額比例。"""
    return (2 * max(min_fee, x * rate) + x * SELL_TAX_RATE) / x


def main() -> None:
    hi_rate = _S.high_tier_rate()
    print("費率(唯一真源 execsim/broker_fee.py + trading/live/money.py)")
    print(f"  標準 {_S.standard_commission_rate:.4%} × 1.8 折 = {COMMISSION_RATE:.5%}"
          f"(月成交額 {_S.monthly_discount_threshold:,.0f} 內)")
    print(f"  超過月額度部分 × 4 折 = {hi_rate:.5%}")
    print(f"  證交稅 {SELL_TAX_RATE:.2%}(賣出,不打折、無低消)")
    print(f"  最低手續費:整股 {COMMISSION_MIN:.0f} 元 / 零股 {ODD_LOT_COMMISSION_MIN:.0f} 元\n")

    print("=== 臨界金額:折扣完全生效的最小單筆 ===")
    for lab, mf in (("零股(<1,000 股),低消 1 元", ODD_LOT_COMMISSION_MIN),
                    ("整股(≥1,000 股),低消 20 元", COMMISSION_MIN)):
        print(f"  {lab}:{mf / COMMISSION_RATE:>10,.0f} 元")
    floor = 2 * COMMISSION_RATE + SELL_TAX_RATE
    print(f"  → 到達臨界後的往返摩擦地板 = {floor:.4%}(2×手續費 + 證交稅);再大也降不下去\n")

    print("=== 往返摩擦率 vs 單筆金額 ===")
    print(f"  {'單筆金額':>12}{'零股摩擦':>11}{'整股摩擦':>11}{'零股 vs 地板':>14}")
    for x in (100, 200, 500, 1_000, 2_000, 3_899, 5_000, 10_000, 20_000,
              50_000, 77_973, 100_000, 300_000):
        odd = round_trip_cost(x, ODD_LOT_COMMISSION_MIN)
        lot = round_trip_cost(x, COMMISSION_MIN)
        print(f"  {x:>12,}{odd:>11.3%}{lot:>11.3%}{odd - floor:>+13.3%}")
    print("  註:同一筆金額,**零股的手續費結構嚴格優於整股**,直到整股臨界 77,973 元才追平。\n")

    # S 的每筆報酬 = 摩擦的分母(摩擦吃掉多少邊際)
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    _, tr = run_s_full(panel, feat, elig, DS)
    r = tr.filter(pl.col("exit_reason") != "open")["ret_net"].to_numpy()
    med, mean = float(np.median(r)), float(r.mean())
    win = float((r > 0).mean())
    print("=== 摩擦相對 S 的每筆邊際(這才是「划算」的實質判準)===")
    print(f"  S 每筆淨報酬:平均 {mean:+.2%}  中位 {med:+.2%}  勝率 {win:.1%}  n={len(r)}")
    print(f"  {'單筆金額':>12}{'零股摩擦':>11}{'佔平均報酬':>12}{'佔中位報酬':>12}")
    for x in (100, 500, 1_000, 2_000, 3_899, 10_000, 100_000):
        c = round_trip_cost(x, ODD_LOT_COMMISSION_MIN)
        print(f"  {x:>12,}{c:>11.3%}{c / abs(mean):>11.1%}{c / max(abs(med), 1e-9):>11.1%}")

    # 月成交額 100 萬門檻:S 每年約 58.7 筆、每筆買賣各一次
    per_month_orders = 689 / 11.74 / 12 * 2
    thr = _S.monthly_discount_threshold / per_month_orders
    print(f"\n=== 4 折門檻 ===")
    print(f"  S 每月約 {per_month_orders:.1f} 筆委託(買賣各算一次);月成交額超過 "
          f"{_S.monthly_discount_threshold:,.0f} 元的部分費率翻倍(1.8 折 → 4 折)")
    print(f"  → 單筆金額超過約 {thr:,.0f} 元後,超出的部分開始適用 4 折")

    print("\n=== 回測用的成本假設對照 ===")
    print(f"  引擎 ExecSpec 預設 commission 0.0285%(2 折)+ 稅 0.3% + 滑價 0.1%/邊")
    print(f"  → 回測往返 {2 * 0.000285 + 0.003 + 2 * 0.001:.3%};實際 1.8 折且達臨界時 "
          f"{floor:.3%}(未計滑價)")
    print(f"  回測在手續費上是**保守**的(2 折 > 1.8 折),不需修正;但金額小於臨界時,"
          f"實際摩擦會反過來超出回測假設。")


if __name__ == "__main__":
    main()
