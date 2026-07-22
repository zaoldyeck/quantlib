"""交易計劃的金額與損益試算(純函式,可測;供計劃信顯示)。

使用者需求(2026-07-22):信裡要看到「買賣的現價、賣出的 ROI 與持有損益、帳戶
資金、預計增減多少、不足要提醒補交割」。

**誠實界定**:計劃於盤前 01:00 產生,當下市場未開,所謂「現價」一律是
**最近收盤價**(cache 最新交易日),不是即時報價——欄位名與註記都必須這樣寫,
不得讓使用者誤以為是成交價。實際成交價由開盤後的執行器決定。

**台股交割慣例**(用於「錢夠不夠」提醒):買進 T+2 扣款、賣出 T+2 入帳;兩者
同日成交時淨額交割。

**費率一律取自 `research/execution/broker_fee.py`(唯一真源)**——那份 schedule
同時餵回測與執行模擬,若此處另立常數,兩邊會靜默漂移(成本假設一漂移,ROI 與
「錢夠不夠」就都不可信)。本模組只補一件 schedule 沒有的事:**零股最低手續費**。
"""
from __future__ import annotations

from dataclasses import dataclass

from research.execution.broker_fee import FubonFeeSchedule

_FEE = FubonFeeSchedule()             # 1.8 折(月成交額 100 萬內)、證交稅 0.3%
COMMISSION_RATE = _FEE.low_tier_rate()
SELL_TAX_RATE = _FEE.sell_tax_rate
#: 整股單最低手續費(TWD);零股另有更低的最低收費,見 ODD_LOT_COMMISSION_MIN
COMMISSION_MIN = _FEE.minimum_commission
#: **零股最低手續費 1 元**(富邦盤中/盤後零股;使用者 2026-07-22 提供之帳戶費率)。
#: 這對 1 股營運是決定性的:用整股的 20 元下限估,單筆 45 元的買進會被高估 19 元
#: 成本,ROI 直接失真數十個百分點。
ODD_LOT_COMMISSION_MIN = 1.0
#: 一張 = 1,000 股;未滿一張即零股(交易所定義)
LOT_SIZE = 1_000


def commission_min(shares: int) -> float:
    """該筆委託適用的最低手續費:未滿一張走零股下限,整張走整股下限。"""
    return ODD_LOT_COMMISSION_MIN if 0 < shares < LOT_SIZE else COMMISSION_MIN


def fee_buy(amount: float, shares: int = LOT_SIZE) -> float:
    """買進手續費(含最低收費)。`shares` 決定適用哪個最低收費(零股 vs 整股)。"""
    return max(commission_min(shares), amount * COMMISSION_RATE) if amount > 0 else 0.0


def fee_sell(amount: float, shares: int = LOT_SIZE) -> float:
    """賣出手續費 + 證交稅。"""
    if amount <= 0:
        return 0.0
    return max(commission_min(shares), amount * COMMISSION_RATE) + amount * SELL_TAX_RATE


@dataclass(frozen=True)
class Leg:
    """一筆預計交易(金額皆以最近收盤價試算,非成交價)。"""
    code: str
    side: str                 # "buy" | "sell"
    shares: int
    px: float | None          # 最近收盤價
    cost: float | None = None # 賣出腿:每股成本(收養者為收養價)
    cost_basis: str = ""      # 成本來源標註(如「收養價」)

    @property
    def amount(self) -> float:
        return (self.px or 0.0) * self.shares

    @property
    def commission(self) -> float:
        """手續費(買賣皆有;零股適用 1 元下限)。"""
        if self.px is None or self.amount <= 0:
            return 0.0
        return max(commission_min(self.shares), self.amount * COMMISSION_RATE)

    @property
    def tax(self) -> float:
        """證交稅(只有賣出課)。"""
        if self.side != "sell" or self.px is None or self.amount <= 0:
            return 0.0
        return self.amount * SELL_TAX_RATE

    @property
    def breakdown(self) -> str:
        """把「含費」拆開講清楚——只寫「含費」等於沒寫,看的人算不出這筆錢怎麼來的。

        買:股款 + 手續費;賣:股款 − 手續費 − 證交稅。
        """
        if self.px is None:
            return "無報價"
        # 保留到分:這幾行是拿來對帳的,四捨五入到元會讓 0.36 元的證交稅顯示成 0,
        # 看的人反而算不平(標題的合計才用整數,那是給人抓量級的)。
        base = f"股款 {self.amount:,.2f}"
        fee = f"手續費 {self.commission:,.2f}"
        if self.side == "buy":
            return f"{base} + {fee}"
        return f"{base} − {fee} − 證交稅 {self.tax:,.2f}"

    @property
    def net(self) -> float:
        """對現金的影響(買為負、賣為正;已含費稅)。"""
        if self.px is None:
            return 0.0
        if self.side == "buy":
            return -(self.amount + self.commission)
        return self.amount - self.commission - self.tax

    @property
    def pnl(self) -> float | None:
        """賣出腿的持有損益(已扣賣出費稅;買進成本的手續費不重複計)。"""
        if self.side != "sell" or self.px is None or not self.cost:
            return None
        return self.net - self.cost * self.shares

    @property
    def roi(self) -> float | None:
        if self.side != "sell" or not self.cost or self.px is None:
            return None
        base = self.cost * self.shares
        return (self.net - base) / base if base > 0 else None


@dataclass(frozen=True)
class Settlement:
    """整份計劃的資金結算試算。"""
    cash: float
    legs: list[Leg]

    @property
    def buy_cost(self) -> float:
        # + 0.0 消除 -0.0(否則版面會印出「−-0 元」)
        return -sum(l.net for l in self.legs if l.side == "buy") + 0.0

    @property
    def sell_proceeds(self) -> float:
        return sum(l.net for l in self.legs if l.side == "sell")

    @property
    def net_change(self) -> float:
        """帳戶現金淨變動(正=入帳、負=扣款)。"""
        return self.sell_proceeds - self.buy_cost

    @property
    def shortfall(self) -> float:
        """需補入的金額;0 = 資金充足。

        保守起見**只以現金對買進金額**判斷:賣出款 T+2 才入帳,若賣出與買進同日
        成交雖可淨額交割,但券商實務上仍可能先要求足額圈存 → 寧可提醒也不漏。
        """
        return max(0.0, self.buy_cost - self.cash)


def build_settlement(cash: float, buys: list[str], sells: list[str],
                     shares_per_buy: int, holdings: dict[str, float],
                     prices: dict[str, float],
                     costs: dict[str, tuple[float, str]] | None = None) -> Settlement:
    """組出結算試算。prices = 最近收盤價;costs = {code: (每股成本, 來源標註)}。"""
    costs = costs or {}
    legs: list[Leg] = []
    for c in buys:
        legs.append(Leg(c, "buy", shares_per_buy, prices.get(c)))
    for c in sells:
        cost, basis = costs.get(c, (None, ""))
        legs.append(Leg(c, "sell", int(holdings.get(c, 0)), prices.get(c),
                        cost=cost, cost_basis=basis))
    return Settlement(cash=cash, legs=legs)
