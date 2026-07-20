"""帳戶成本基礎——單一事實來源。

**成本是帳戶的屬性,不是策略的屬性**:同一筆持股,不管哪個策略在看,買進成本
都是同一個數字。故三個 advisor 共用本模組,不各自算各自的。

資料來源與其誠實度(依序):
1. **Serenity live ledger 的 lot 錨,且 `cost_basis == "fill"`** → 那是實際成交價
   (2026-07-09 那批有 `order_results` 佐證)。
2. Serenity ledger 的收養部位(`cost_basis == "adopted_close"`)→ **收養日收盤,
   是成本代理不是真實成本**(那些股票在系統接手前就持有,真實成本不可考)。
3. tri 自己的 state(`research/tri/state/*_positions.json` 的 `cost`)→ 同樣是
   收養日收盤代理。

為什麼不問券商:富邦 `inventories` **沒有成本欄位**(只有當日的 buy_value /
buy_filled_qty),而 `filled_history` 在此帳戶連歷史區間都回空(2026-07-09 實測,
見 memory `fubon-filled-history-dead`)→ 跨日的真實成交價券商端查不到,只能靠
我們自己在成交當天記帳。這也是為什麼執行器的 TCA jsonl 必須永久保存。
"""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SER_LEDGER = REPO_ROOT / "research" / "serenity" / "state" / "live_positions.json"

#: 成本來源標記 → 給人看的短標籤
BASIS_LABEL = {"fill": "成交價", "adopted_close": "收養價", "state": "收養價"}


def _load(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def account_cost_basis() -> dict[str, tuple[float, str]]:
    """{code: (成本, basis)}。basis ∈ {fill, adopted_close}。"""
    out: dict[str, tuple[float, str]] = {}
    for code, pos in (_load(SER_LEDGER).get("positions") or {}).items():
        anchor = pos.get("anchor")
        if anchor:
            out[str(code)] = (float(anchor), pos.get("cost_basis") or "adopted_close")
    return out


def cost_of(code: str, fallback: float | None = None,
            fallback_basis: str = "state") -> tuple[float | None, str]:
    """單檔成本 + 來源標記;查不到就用呼叫端的 fallback(通常是自家 state 的收養價)。"""
    hit = account_cost_basis().get(str(code))
    if hit:
        return hit
    return (fallback, fallback_basis) if fallback else (None, "unknown")


def levels_line(cost: float | None, basis: str, px: float | None,
                stop: float | None, take: float | None,
                stop_note: str = "", take_note: str = "") -> str:
    """報告用的一行:成本(來源)|現價(損益%)|止損|止盈。

    止盈為 None = 該策略沒有固定止盈(靠 trailing/時間出場),如實寫明,
    不要讓使用者以為忘了填。
    """
    if not cost or not px:
        return "|成本/現價不明"
    pnl = (px / cost - 1) * 100
    parts = [f"成本 {cost:g}({BASIS_LABEL.get(basis, basis)})",
             f"現價 {px:g}({pnl:+.1f}%)"]
    parts.append(f"止損 {stop:g}{stop_note}" if stop else "止損 —")
    parts.append(f"止盈 {take:g}{take_note}" if take else f"止盈 —{take_note or '(無固定止盈)'}")
    return "|" + "|".join(parts)
