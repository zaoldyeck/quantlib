"""費率感知的下單切分:同一筆金額,怎麼下最省手續費(純函式,離線可測)。

## 為什麼需要(使用者 2026-07-26)
「下單策略應該改成最有利做法……多少股以下用零股成交更划算,機制要做進系統。」

台股最低手續費**分兩種**:整股(≥1 張)20 元、零股(<1 張)1 元。折後費率 0.02565%
(1.8 折)下,折扣完全生效的臨界金額是:

    零股 1 / 0.02565% =  3,899 元
    整股 20 / 0.02565% = 77,973 元

低於臨界,實付的是最低收費,實質費率隨金額變小而發散(單筆 100 元 → 手續費 1 元
= 1%,是折後費率的 39 倍)。故有兩個機制要進系統:

1. **守門**:任何一筆委託的金額低於臨界就是在浪費折扣 → `ticket_warning()` 講清楚
   實質費率變成多少,由上層決定要不要下。
2. **切分**:目標股數 ≥1 張時,「湊整張」不一定最省——例如 1,000 股 @ 10 元,
   走整股要 20 元(0.2%),改買 999 股走零股只要 2.56 元。`plan_tickets()` 把兩種
   下法都算出來、選便宜的。

## ⚠ 誠實界線:零股的價差劣勢沒有量測
盤中零股流動性遠低於整股,價差可能更寬。省下的手續費若連**一檔(tick)**都蓋不過,
就一定不划算——故 `plan_tickets` 用「節省 > 1 tick × 股數」當**必要條件**(保守下界,
不是最適門檻)。這不是調出來的參數,是價格的最小刻度,取自 execution/ticks.py。
真要更進一步,需要盤中零股的成交/掛單資料,目前沒有。

## ⚠ 切片(slice)與最低收費的交互
執行器對大單會切片。若券商的最低收費是**逐筆委託**計算,切 N 片就付 N 次下限
——`min_efficient_slice()` 給出「切片後每片仍能吃到折後費率」的最小片值。
(富邦是否逐筆計、還是同日同股合併,官方文件只寫「手續費折扣以單筆計算」,
未明確定義「單筆」;未經證實前一律按最壞情況〔逐筆〕設計。)

Run(試算表):uv run --project . python -m quantlib.strat_lab.min_ticket_size
"""
from __future__ import annotations

from dataclasses import dataclass

from quantlib.execsim.broker_fee import LOT_SIZE, FubonFeeSchedule
from quantlib.trading.execution.ticks import tick_size

_FEE = FubonFeeSchedule()

#: 盤中零股單筆股數上限(交易所:1~999 股)
ODD_LOT_MAX = LOT_SIZE - 1


@dataclass(frozen=True)
class Ticket:
    """一筆實際會送出去的委託。"""
    shares: int
    market_type: str          # "Common"(整股)| "IntradayOdd"(盤中零股)
    price: float

    @property
    def notional(self) -> float:
        return self.shares * self.price

    @property
    def fee(self) -> float:
        return fee_for(self.notional, self.shares)

    @property
    def effective_rate(self) -> float:
        return self.fee / self.notional if self.notional > 0 else 0.0


def fee_for(notional: float, shares: float,
            schedule: FubonFeeSchedule = _FEE) -> float:
    """單筆手續費 = max(該筆適用的最低收費, 金額 × 折後費率)。"""
    if notional <= 0:
        return 0.0
    return max(schedule.minimum_for(shares), notional * schedule.low_tier_rate())


def min_efficient_notional(shares: float, schedule: FubonFeeSchedule = _FEE) -> float:
    """折扣完全生效的最小單筆金額(零股 3,899 / 整股 77,973)。"""
    return schedule.min_efficient_notional(shares)


def min_efficient_slice(schedule: FubonFeeSchedule = _FEE) -> float:
    """切片時每片的最小金額——低於此,切片就是在重複付最低收費。"""
    return schedule.min_efficient_notional(LOT_SIZE)


def natural_split(shares: int) -> list[tuple[int, str]]:
    """交易所規則下的自然拆法:整張走整股市場,餘數走盤中零股。"""
    board = (shares // LOT_SIZE) * LOT_SIZE
    odd = shares - board
    out: list[tuple[int, str]] = []
    if board:
        out.append((board, "Common"))
    if odd:
        out.append((odd, "IntradayOdd"))
    return out


def _tickets(pairs: list[tuple[int, str]], price: float) -> list[Ticket]:
    return [Ticket(q, mt, price) for q, mt in pairs]


def total_fee(tickets: list[Ticket]) -> float:
    return sum(t.fee for t in tickets)


def plan_tickets(shares: int, price: float) -> tuple[list[Ticket], str]:
    """把「要買/賣 shares 股」切成最省手續費的委託組合。

    回傳 (委託清單, 決策說明)。除了交易所的自然拆,額外評估「減到 999 股走純零股」,
    但**必須同時通過兩道保守條件**才採用:

      ① 省下的手續費 > 放棄的曝險金額 —— 絕不為了省手續費而少放錢進場。
         (拿 notional 當放棄的代價是保守的:少買 500 元的期望損失遠小於 500 元,
          但用它當門檻可確保「省的錢 > 放棄的錢」,不需要對報酬做任何假設。)
      ② 省下的手續費 > 一檔價差 × 股數 —— 零股流動性較差,連一個最小刻度都
         蓋不過就一定不划算(見模組 docstring 的誠實界線)。

    實務上只有「股價低、且股數剛過 1,000」的窄區間會通過:1,000 股 @ 5 元少買
    1 股(5 元)換省 18.7 元手續費,划算;1,999 股就會被 ① 擋下(放棄 1,000 股)。
    """
    if shares <= 0 or price <= 0:
        return [], "無需下單"

    base = _tickets(natural_split(shares), price)
    if shares <= ODD_LOT_MAX:
        return base, f"零股單筆({shares} 股),適用 1 元下限"

    base_fee = total_fee(base)
    alt = _tickets([(ODD_LOT_MAX, "IntradayOdd")], price)
    saving = base_fee - total_fee(alt)
    given_up = (shares - ODD_LOT_MAX) * price        # ① 放棄的曝險金額
    hurdle = tick_size(price) * ODD_LOT_MAX          # ② 零股吃一檔價差的代價(下界)
    natural_desc = "+".join(f"{t.shares}{t.market_type[:1]}" for t in base)
    if saving > given_up and saving > hurdle:
        return alt, (f"改走純零股 {ODD_LOT_MAX} 股:省 {saving:.1f} 元 > "
                     f"放棄曝險 {given_up:.1f} 元、一檔價差 {hurdle:.1f} 元")
    if saving <= given_up:
        return base, (f"自然拆({natural_desc}):縮零股只省 {saving:.1f} 元,"
                      f"卻要放棄 {given_up:,.0f} 元曝險")
    return base, (f"自然拆({natural_desc}):縮零股只省 {saving:.1f} 元,"
                  f"蓋不過一檔價差 {hurdle:.1f} 元")


def ticket_warning(ticket: Ticket) -> str | None:
    """單筆金額低於臨界時的白話警告(None = 這筆的折扣完全生效)。"""
    thr = min_efficient_notional(ticket.shares)
    if ticket.notional >= thr:
        return None
    mult = ticket.effective_rate / _FEE.low_tier_rate()
    return (f"{ticket.shares} 股 × {ticket.price:g} = {ticket.notional:,.0f} 元 "
            f"低於臨界 {thr:,.0f} 元:手續費 {ticket.fee:.0f} 元 = "
            f"{ticket.effective_rate:.3%},是折後費率的 {mult:.0f} 倍")
