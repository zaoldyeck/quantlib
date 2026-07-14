"""階梯定價策略(純函式,離線可測)。

一條腿(leg)的生命週期:被動 → 中價 → 跨價 → 死線後直取可成交價;
全程受 cap(買)/floor(賣)護欄約束——護欄取自到達價(arrival),
絕不追過。回測依據:進場擇時事件研究(2026-07-07)證明「等回檔」錯過贏家,
故必須有時間升級;停損賣出速度優先(sell_stop 首輪即跨價)。
"""

from __future__ import annotations

from dataclasses import dataclass

from .ticks import add_ticks, snap_down, snap_up


@dataclass(frozen=True)
class LadderProfile:
    name: str
    # 各階段持續的「輪數」(一輪 = 一次撮合週期;盤中零股 ≈ 60s)
    passive_rounds: int  # 掛自己這一側(買=bid、賣=ask)
    mid_rounds: int      # 掛中價
    # 之後 = 跨價(買掛 ask、賣掛 bid)
    cap_pct: float       # 護欄:買 = arrival×(1+cap)上限;賣 = arrival×(1−cap)下限
    # 盤中升級死線(過此時刻直接跨價,仍受護欄);None = 盤中永不因時間跨價,
    # 整場按策略掛結構位撈最低/最高,完成交給盤後定價收尾(14:30 撮合=收盤價,
    # 與回測出場語義「收盤價出場」精確對齊;2026-07-14 使用者指示)
    deadline_hhmm: str | None
    structure_anchor: bool = False  # 被動單掛在結構位(TPO VAL/OB/FVG)而非買一/賣一


BUY_NORMAL = LadderProfile("buy_normal", passive_rounds=3, mid_rounds=3,
                           cap_pct=0.008, deadline_hhmm="12:30")
SELL_NORMAL = LadderProfile("sell_normal", passive_rounds=3, mid_rounds=3,
                            cap_pct=0.008, deadline_hhmm="12:30")
# 急殺:速度 >> 價格,首輪即跨價,護欄放寬。僅限「事實級利空 override」
#(查證協定明文「不等價格」的情境);一般六道門出場用 SELL_EXIT。
SELL_STOP = LadderProfile("sell_stop", passive_rounds=0, mid_rounds=0,
                          cap_pct=0.030, deadline_hhmm="09:00")
# 系統出場(六道門預設):賣在當日相對高點——結構錨定(VAH/昨日高)整場
# 等回升,盤中永不因時間跨價(2026-07-14 使用者指示:死線拖到盤後);收盤
# 未竟由盤後定價收尾(14:30 撮合=收盤價,與回測「收盤價出場」語義精確對齊;
# 護欄 -3% 仍是鐵律,收盤價破欄不掛、盤後未中籤 → 明日出場門重評)。
SELL_EXIT = LadderProfile("sell_exit", passive_rounds=10**6, mid_rounds=0,
                          cap_pct=0.030, deadline_hhmm=None,
                          structure_anchor=True)
# 價格優先(耐心版):整場掛結構位撈最低/最高,盤中永不因時間跨價;
# 只有狙擊級微結構訊號(竭盡/掃蕩)才主動取價,其餘交給盤後定價收尾。
BUY_PATIENT = LadderProfile("buy_patient", passive_rounds=10**6, mid_rounds=0,
                            cap_pct=0.005, deadline_hhmm=None,
                            structure_anchor=True)
SELL_PATIENT = LadderProfile("sell_patient", passive_rounds=10**6, mid_rounds=0,
                             cap_pct=0.005, deadline_hhmm=None,
                             structure_anchor=True)

PROFILES = {p.name: p for p in (BUY_NORMAL, SELL_NORMAL, SELL_STOP, SELL_EXIT, BUY_PATIENT, SELL_PATIENT)}


def price_collar(side: str, arrival: float, profile: LadderProfile) -> float:
    """絕對護欄:買方上限(貼齊向下)、賣方下限(貼齊向上)。"""
    if side == "Buy":
        return snap_down(arrival * (1.0 + profile.cap_pct))
    return snap_up(arrival * (1.0 - profile.cap_pct))


def target_price(
    side: str,
    profile: LadderProfile,
    round_idx: int,
    past_deadline: bool,
    bid: float,
    ask: float,
    arrival: float,
) -> float:
    """本輪應掛的限價(已含護欄)。bid/ask 缺值時以另一側 ±1 tick 代用。"""
    if bid <= 0 and ask > 0:
        bid = add_ticks(ask, -1)
    if ask <= 0 and bid > 0:
        ask = add_ticks(bid, +1)
    mid = (bid + ask) / 2.0
    collar = price_collar(side, arrival, profile)

    if side == "Buy":
        if past_deadline or round_idx >= profile.passive_rounds + profile.mid_rounds:
            raw = ask  # 跨價:零股集合競價下掛到對側即高機率成交
        elif round_idx >= profile.passive_rounds:
            raw = snap_down(mid)
        else:
            raw = bid  # 被動:加入買方最佳檔
        return min(snap_down(max(raw, 0.01)), collar)

    # Sell
    if past_deadline or round_idx >= profile.passive_rounds + profile.mid_rounds:
        raw = bid
    elif round_idx >= profile.passive_rounds:
        raw = snap_up(mid)
    else:
        raw = ask
    return max(snap_up(raw), collar)
