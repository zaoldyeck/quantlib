"""S 策略 live 決策膠水層(純函式,重用 `s_advisor`,永不下單)。

把現成 `research.tri.advisors.s_advisor` 的建議萃取成「今日可機械執行」的下單清單。
`s_advisor` 已是 S 策略的唯一決策真源(逐檔出場逐日重放、進場池排名、席位收斂);
本模組**不重算任何策略邏輯**,只做「建議 → 今日動作」的純映射:

- **買**:`adv.buys` 中 reason 開頭「今日進場」者(每日上限 2 的實際進場;
  「⏸ 排隊」/「🕒 遞補」是通往完全體的資訊、今天不進場)。營運股數由 `execute`
  決定(現行 1 股)——本層只給代碼,不管股數,保持純粹。
- **賣**:`adv.sells` 全部;但 reason 含「人工確認」(無法取價/停牌/下市)者移入
  `manual_review`——這類需人判斷,**不自動下單**(硬送也會失敗)。其餘(超額席位、
  非本策略標的、觸發出場規則)一律全部庫存賣出。

`DayPlan` 是不可變資料;送單由 `execute` 派工現成 `execution.trade`。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date as Date

#: 出場建議中代表「需人工判斷、不可自動下單」的標記(源自 s_advisor 的 sell 理由)
_MANUAL_MARKER = "人工確認"


def protected_from_env() -> set[str]:
    """QL_S_PROTECTED="2059,6446" → {"2059","6446"}。

    使用者明確要自己控、不許自動賣的持股;唯一真源(premarket 顯示 + execute 執行
    都讀它),改保留清單只需改這個環境變數(VM 的 .env / Secret Manager)。
    """
    raw = os.environ.get("QL_S_PROTECTED", "")
    return {c.strip().zfill(4) for c in raw.split(",") if c.strip()}
#: 進場建議中代表「今日實際進場」的 reason 前綴(其餘為排隊/遞補,今日不動作)
_ENTER_TODAY_PREFIX = "今日進場"


@dataclass(frozen=True)
class DayPlan:
    """某交易日 S 策略的可執行計劃 + 給人看的脈絡。"""

    date: str
    buys: list[str] = field(default_factory=list)                 # 今日進場代碼(自動買)
    sells: list[str] = field(default_factory=list)                # 自動賣(非保留)
    protected_sells: list[str] = field(default_factory=list)      # 保留股卻建議賣→需你回信確認才賣
    manual_review: list[tuple[str, str]] = field(default_factory=list)  # (code, reason)
    keeps: list[tuple[str, str]] = field(default_factory=list)    # 續抱(信件顯示用)
    queued: list[tuple[str, str]] = field(default_factory=list)   # ⏸排隊/🕒遞補(不執行)
    notes: list[str] = field(default_factory=list)
    #: 各持股的持有期最高收盤(由 exit_replay 逐日重算,非增量 state)。
    #: 唯一用途 = 券商端安全網水位(safety_net);不參與任何交易決策。
    peaks: dict[str, float] = field(default_factory=dict)

    @property
    def has_actions(self) -> bool:
        """今天有沒有要自動送出的腿(買或自動賣;保留股需確認、不算自動腿)。"""
        return bool(self.buys or self.sells)

    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "buys": list(self.buys),
            "sells": list(self.sells),
            "protected_sells": list(self.protected_sells),
            "manual_review": [list(x) for x in self.manual_review],
            "keeps": [list(x) for x in self.keeps],
            "queued": [list(x) for x in self.queued],
            "notes": list(self.notes),
        }


def plan_from_advice(adv, today: Date,
                     protected: set[str] | None = None) -> DayPlan:
    """純映射:`Advice` → 今日可執行 `DayPlan`(不碰 DB,可獨立測試)。

    這是 money-path 的核心決策萃取——「哪些是今天要自動送的腿」全在這裡定義:
    - 只有 reason 開頭「今日進場」的 buy 是今日進場;其餘(排隊/遞補)歸 queued。
    - 含「人工確認」的 sell 歸 manual_review(不自動下單)。
    - **保留股**(`protected`,使用者明確要自己控的持股)即使策略建議賣,也**不自動賣**,
      歸 `protected_sells`——除非使用者回信確認,否則保留(execute 為權威閘再擋一次)。
    - 其餘賣單歸 sells(自動全賣)。
    """
    protected = protected or set()
    buys = [code for code, _w, reason in adv.buys
            if reason.startswith(_ENTER_TODAY_PREFIX)]
    queued = [(code, reason) for code, _w, reason in adv.buys
              if not reason.startswith(_ENTER_TODAY_PREFIX)]

    sells: list[str] = []
    protected_sells: list[str] = []
    manual_review: list[tuple[str, str]] = []
    for code, reason in adv.sells:
        if _MANUAL_MARKER in reason:
            manual_review.append((code, reason))
        elif code in protected:
            protected_sells.append(code)
        else:
            sells.append(code)

    # 峰值:s_advisor 已用 exit_replay 逐日重算(不靠增量 state),此處只轉出給
    # 安全網用;缺值(無價格路徑/剛收養)自然不掛安全網,不做任何推估。
    # **絕不因取峰值失敗而拋**:峰值只服務保險層,若讓它炸掉會連今日交易計劃都產不出來
    # (交易 > 保險)。單檔轉型失敗即略過該檔。
    peaks: dict[str, float] = {}
    for code, d in (getattr(adv, "detail", None) or {}).items():
        if not isinstance(d, dict):
            continue
        try:
            v = float(d.get("peak") or 0.0)
        except (TypeError, ValueError):
            continue
        if v > 0:
            peaks[code] = v

    return DayPlan(
        date=today.isoformat(),
        buys=buys,
        sells=sells,
        protected_sells=protected_sells,
        manual_review=manual_review,
        keeps=list(adv.keeps),
        queued=queued,
        notes=list(adv.notes),
        peaks=peaks,
    )


def build_day_plan(con, holdings: dict[str, float], today: Date,
                   nav: float = 0.0, protected: set[str] | None = None) -> DayPlan:
    """呼叫現成 `s_advisor`,萃取今日可執行下單清單。

    `con` = `research.apex.data.connect()` 的 read-only cache 連線;`holdings` = 富邦
    現時持股;`today` = 交易日(run 日,台北);`nav` = 帳戶淨值(顯示用);
    `protected` = 使用者指定要保留、不自動賣的持股代碼集合。
    """
    from research.tri.advisors import s_advisor

    return plan_from_advice(s_advisor(con, holdings, today, nav), today, protected)
