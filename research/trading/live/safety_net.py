"""券商端災難停損(safety net)——VM 失效時的唯一保護層。

**定位(2026-07-21,以真實資料校準後定案,不是憑感覺)**
S 的出場 379 筆實測分佈:訊號過期 71.5% + 輸家時間止損 26.1% = **97.6% 是
時間/基本面驅動,條件單根本表達不了**;唯一可表達的 trail 35% 六年只觸發 2 次。
且 I01 校準(真實 1 分 K 逐筆重放)顯示把 trail 改成盤中觸發 **統計上無差異**
(配對差 −0.09% ± 0.13%,t=−0.71),卻曾發生 1 次「盤中破線收盤拉回」被洗掉 −46pp。

→ **結論:條件單不當主要出場路徑(那會是表演),只當「VM 掛掉時的保命索」。**
水位 = `peak_close × (1 − WIDE)`,WIDE=0.50 **比策略自身 35% 更寬**:正常運作時
策略的日頻路徑一定先出場,安全網不干擾;只有「VM 死掉 + 崩跌」才接管。
I01 量到此變體成本 −0.17% ± 0.17%(t=−1.0)= 噪音內,可視為免費保險。

**三條 money-path 命門(皆為實測踩到後定案)**
1. **只管自己掛的單**:以本地 guid 台帳(state/safety_net.json)記錄我方委託,
   同步時只撤台帳內的 guid → **絕不動使用者自己掛的條件單**。
2. **不靠狀態字串判活**:實測 `get_condition_order` 會**連已刪除的單一起回傳**
   (status="條件單已刪除(C)")。若靠比對參數判斷「已存在保護」,會把死單誤判成
   有保護 → 實際裸奔。故改為**每日撤舊掛新**(先撤後掛),行為恆定、無需解析狀態。
3. **賣出前必撤**:條件單活在券商端,我方賣掉部位後若仍武裝,日後觸發會賣掉不存在
   的部位(可能變融券)。故 (a) 今日要動的標的不掛;(b) execute 送賣單前再撤一次
   ——該處用 symbol 掃全部條件單(含孤兒與歷史殘留),是孤兒單的最後清道夫。

**孤兒單控管**:效期只給 30 天(`DAYS`)。VM 死一個月內仍受保護;台帳若遺失
(VM 重建),殘留單也會在一個月內自行過期,且賣出時的 symbol 掃描會提前清掉。

Run(唯讀檢視):uv run --project research python -m research.trading.live.safety_net --show
"""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date as Date
from pathlib import Path

WIDE = 0.50            # 安全網回撤水位(> 策略自身 35%,故正常不干擾)
DAYS = 30              # 條件單效期(天);孤兒單自行過期的上限
_ENV_FLAG = "QL_S_SAFETY_NET"   # "0"/"false" → 停用
STATE = (Path(__file__).resolve().parents[3]
         / "research" / "trading" / "live" / "state" / "safety_net.json")


@dataclass(frozen=True)
class NetPlan:
    """一檔的安全網目標狀態。"""
    symbol: str
    quantity: int
    trigger: float          # 觸發價(成交價 ≤ 此價 → 賣出)


def enabled() -> bool:
    return os.environ.get(_ENV_FLAG, "1").lower() not in {"0", "false", "no"}


def plan_for(holdings: dict[str, float], peaks: dict[str, float],
             skip: set[str], wide: float = WIDE) -> list[NetPlan]:
    """純函式:算出應存在的安全網集合。

    holdings 真實庫存(股數)、peaks 各檔持有期最高收盤(由 exit_replay 重算,
    非增量 state)、skip 今日不得掛單者(將被買賣 / 保留股待確認 / 需人工複核)。
    無庫存、無峰值者一律不掛——**不做任何推估**(寧可沒保險,不可憑空掛單)。
    """
    out: list[NetPlan] = []
    for code in sorted(holdings):
        qty = int(holdings[code])
        peak = peaks.get(code)
        if code in skip or qty <= 0 or not peak or peak <= 0:
            continue
        trig = round(float(peak) * (1 - wide), 2)
        if trig > 0:
            out.append(NetPlan(code, qty, trig))
    return out


# ── 自有委託台帳 ────────────────────────────────────────────────────────
def load_ledger() -> dict[str, dict]:
    try:
        return json.loads(STATE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_ledger(led: dict[str, dict]) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(led, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(STATE)                      # 原子換名:中斷不留半檔


def _guid_of(res) -> str | None:
    d = getattr(res, "data", None)
    return getattr(d, "guid", None) if d is not None else None


def _read_current(broker) -> list[dict]:
    """讀券商端條件單並正規化(guid/symbol/status)。查無資料 → 空清單。

    **不用它判斷「是否已有保護」**(會回傳已刪除單,見模組說明命門 2);
    只用於 `cancel_for` 依 symbol 清場(含孤兒單)。
    """
    res = broker.get_condition_orders()
    out = []
    for d in (getattr(res, "data", None) or []):
        guid = getattr(d, "guid", None)
        if not guid:
            continue
        out.append({"guid": guid,
                    "symbol": getattr(d, "symbol", None),
                    "status": str(getattr(d, "status", "") or "")})
    return out


def _is_terminal(status: str) -> bool:
    """已刪除/已失效者無需再撤(撤了也無害,只是少發無謂請求)。"""
    return ("刪除" in status) or ("(C)" in status) or ("失效" in status)


def sync(broker, holdings: dict[str, float], peaks: dict[str, float],
         skip: set[str], *, wide: float = WIDE, dry: bool = False) -> dict:
    """把券商端安全網對齊目標:**先撤自己的舊單,再掛新單**。回摘要 dict。

    先撤後掛(非先掛後撤):同標的雙重武裝會造成「兩張都觸發 → 賣掉沒有的部位」,
    比「數秒無保護」危險得多。盤前 01:00 執行時市場未開,空窗無實質風險。
    """
    if not enabled():
        return {"skipped": "QL_S_SAFETY_NET 已停用"}
    target = plan_for(holdings, peaks, skip, wide)
    tgt_syms = {p.symbol for p in target}
    led = load_ledger()
    cancelled, errs = [], []

    # 1) 撤舊。兩個來源聯集,缺一不可:
    #    (a) 自有台帳全部——涵蓋「已不在 target」的標的(部位已賣出/今日要動),
    #        這些券商端仍武裝,不撤就成孤兒;
    #    (b) 券商端在 target 標的上的**所有活躍單**——台帳是每台機器各自的,
    #        本機掛過的單 VM 看不到(反之亦然),只靠台帳會**同檔雙重武裝**
    #        (兩張都觸發 → 賣掉不存在的部位)。以券商端實況為準即可跨機器自癒,
    #        台帳遺失(VM 重建)亦然。代價:使用者手動掛在「S 管理標的」上的
    #        條件單會被撤——保留股(使用者自控者)本來就不在 target,不受影響。
    guid_syms: dict[str, str] = {}
    for sym, rec in led.items():
        if rec.get("guid"):
            guid_syms[rec["guid"]] = sym
    if not dry:
        try:
            for c in _read_current(broker):
                if c["symbol"] in tgt_syms and not _is_terminal(c["status"]):
                    guid_syms.setdefault(c["guid"], c["symbol"])
        except Exception as exc:  # noqa: BLE001 - 讀不到券商端仍可撤自有台帳
            errs.append(f"讀券商端條件單失敗:{type(exc).__name__} {exc}")

    for guid, sym in guid_syms.items():
        if dry:
            cancelled.append(guid); led.pop(sym, None); continue
        try:
            broker.cancel_condition_order(guid)
            cancelled.append(guid)
            led.pop(sym, None)
        except Exception as exc:  # noqa: BLE001 - 單筆失敗不毀整批;留在台帳下次再撤
            errs.append(f"撤 {sym}/{guid[:8]}: {type(exc).__name__} {exc}")

    placed = []
    for p in target:                              # 2) 掛新單
        if dry:
            placed.append((p.symbol, p.quantity, p.trigger)); continue
        try:
            res = broker.place_condition_sell_stop(
                symbol=p.symbol, quantity=p.quantity, trigger_price=p.trigger,
                days=DAYS, odd_lot=p.quantity < 1000)
            if not getattr(res, "is_success", False):
                errs.append(f"掛 {p.symbol}: {getattr(res, 'message', '未知錯誤')}")
                continue
            led[p.symbol] = {"guid": _guid_of(res), "quantity": p.quantity,
                             "trigger": p.trigger, "placed_on": Date.today().isoformat()}
            placed.append((p.symbol, p.quantity, p.trigger))
        except Exception as exc:  # noqa: BLE001
            errs.append(f"掛 {p.symbol}: {type(exc).__name__} {exc}")
    if not dry:
        save_ledger(led)
    return {"target": len(target), "cancelled": len(cancelled), "placed": placed,
            "errors": errs, "wide": wide, "days": DAYS}


def cancel_for(broker, symbols: set[str]) -> list[str]:
    """撤掉指定標的的**所有**條件單(execute 送賣單前的清道夫)。

    刻意不限於自有台帳:賣出後任何殘留的同標的條件單都會變成「賣不存在的部位」,
    包含台帳遺失的孤兒單。使用者手動掛在該標的的條件單也會被撤——這是正確的:
    我們正要把該部位清光,那些單留著本來就會失效或危險。
    """
    if not symbols:
        return []
    out = []
    led = load_ledger()
    for c in _read_current(broker):
        if c["symbol"] not in symbols or _is_terminal(c["status"]):
            continue
        try:
            broker.cancel_condition_order(c["guid"])
            out.append(c["guid"])
            for sym, rec in list(led.items()):
                if rec.get("guid") == c["guid"]:
                    led.pop(sym, None)
        except Exception as exc:  # noqa: BLE001 - 撤不掉要響亮但不擋交易
            print(f"⚠ 安全網撤單失敗 {c['symbol']}: {type(exc).__name__} {exc}")
    if out:
        save_ledger(led)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="券商端災難停損(安全網)")
    ap.add_argument("--show", action="store_true", help="顯示券商端條件單 + 自有台帳")
    args = ap.parse_args()
    from research.brokers.fubon import FubonBroker
    b = FubonBroker.from_env()
    b.login()
    cur = _read_current(b)
    led = load_ledger()
    print(f"券商端條件單 {len(cur)} 筆(含已刪除):")
    for c in cur:
        mine = "★自有" if any(r.get("guid") == c["guid"] for r in led.values()) else ""
        print(f"  {c['symbol']} {c['status']:<16s} {c['guid'][:8]} {mine}")
    print(f"\n自有台帳 {len(led)} 筆:")
    for sym, r in sorted(led.items()):
        print(f"  {sym} qty={r.get('quantity')} 觸發≤{r.get('trigger')} "
              f"掛於 {r.get('placed_on')}")
    if not args.show:
        print("\n(唯讀;實際同步由每日盤前 premarket 呼叫 sync())")


if __name__ == "__main__":
    main()
