"""微結構訊號層(吸收自 smart_execution.py,修正後版本)。

提供階梯引擎的「擇時加速/減速」訊號——**只調節被動階段的節奏,
不推翻死線升級與護欄**(完成保證與不追價優先於擇時)。

訊號(side-aware,買賣對稱):
- OFI(L1 訂單流失衡,Cont et al.):我方壓力轉正 → 有利
- VPIN(成交毒性,tick rule 分桶):極高 → 逆勢資訊流,先等一輪
- TPO 價值區(1 分 K 疊 profile):買在 VAL 下/賣在 VAH 上 → 價位有利
- SMC:多/空 FVG 與 Order Block 覆蓋、流動性掃蕩(sweep)
- 主動流竭盡:**滾動 90 秒窗**(修正原版「只累加永不衰減」的致命 bug)

原版致命問題(已修):recent_sell_volume 無衰減 → 竭盡條件在首筆主賣後
永久為假;無升級死線 → 開高走高的贏家永遠買不到(引擎的死線機制補上)。
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field


# ── OFI ──
@dataclass
class OFIState:
    prev: tuple[float, int, float, int] | None = None
    history: deque = field(default_factory=lambda: deque(maxlen=15))

    def update(self, bid_p: float, bid_s: int, ask_p: float, ask_s: int) -> None:
        if self.prev is None:
            self.prev = (bid_p, bid_s, ask_p, ask_s)
            return
        pb, ps, pa, pq = self.prev
        if bid_p > pb:
            d_bid = bid_s
        elif bid_p == pb:
            d_bid = bid_s - ps
        else:
            d_bid = -ps
        if ask_p < pa:
            d_ask = ask_s
        elif ask_p == pa:
            d_ask = ask_s - pq
        else:
            d_ask = -pq
        self.history.append(d_bid - d_ask)
        self.prev = (bid_p, bid_s, ask_p, ask_s)

    @property
    def sma(self) -> float:
        return sum(self.history) / len(self.history) if self.history else 0.0


# ── VPIN ──
@dataclass
class VPINState:
    bucket_size: float = 500.0
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    acc_vol: float = 0.0
    prev_price: float = 0.0
    history: deque = field(default_factory=lambda: deque(maxlen=10))

    def update(self, price: float, size: float) -> None:
        if self.prev_price <= 0:
            self.prev_price = price
            return
        if price > self.prev_price:
            self.buy_vol += size
        elif price < self.prev_price:
            self.sell_vol += size
        else:
            self.buy_vol += size / 2.0
            self.sell_vol += size / 2.0
        self.acc_vol += size
        self.prev_price = price
        if self.acc_vol >= self.bucket_size:
            self.history.append(abs(self.buy_vol - self.sell_vol) / self.acc_vol)
            self.buy_vol = self.sell_vol = self.acc_vol = 0.0

    @property
    def current(self) -> float:
        return self.history[-1] if self.history else 0.0


# ── TPO 價值區 ──
def tpo_value_area(bars: list[dict]) -> tuple[float, float, float]:
    """回傳 (VAL, POC, VAH);bars 需含 low/high。"""
    if not bars:
        return 0.0, 0.0, 0.0
    lows = [float(b["low"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    lo, hi = min(lows), max(highs)
    rng = hi - lo
    bin_size = max(0.05, rng / 50.0) if rng > 0 else 1.0
    counts: dict[int, int] = defaultdict(int)
    for b in bars:
        for idx in range(int(float(b["low"]) / bin_size), int(float(b["high"]) / bin_size) + 1):
            counts[idx] += 1
    if not counts:
        return lo, lo, hi
    poc_bin = max(counts, key=counts.get)
    target = int(sum(counts.values()) * 0.70)
    va = {poc_bin}
    covered = counts[poc_bin]
    while covered < target:
        left, right = min(va) - 1, max(va) + 1
        lc, rc = counts.get(left, 0), counts.get(right, 0)
        if lc == 0 and rc == 0:
            break
        if lc >= rc:
            va.add(left)
            covered += lc
        else:
            va.add(right)
            covered += rc
    return min(va) * bin_size, poc_bin * bin_size + bin_size / 2.0, (max(va) + 1) * bin_size


# ── SMC 結構(多空對稱)──
def smc_zones(bars: list[dict], side: str) -> list[tuple[float, float]]:
    """回傳有利進場的 (bottom, top) 區間:買 = 多方 FVG/OB;賣 = 空方 FVG/OB。"""
    zones: list[tuple[float, float]] = []
    if len(bars) < 3:
        return zones
    for i in range(1, len(bars) - 1):
        b1, b2, b3 = bars[i - 1], bars[i], bars[i + 1]
        if side == "Buy":
            if float(b3["low"]) > float(b1["high"]):  # bullish FVG
                zones.append((float(b1["high"]), float(b3["low"])))
                ob = b2 if float(b2["close"]) < float(b2["open"]) else (
                    b1 if float(b1["close"]) < float(b1["open"]) else None)
                if ob is not None:
                    zones.append((float(ob["low"]), float(ob["high"])))
        else:
            if float(b3["high"]) < float(b1["low"]):  # bearish FVG
                zones.append((float(b3["high"]), float(b1["low"])))
                ob = b2 if float(b2["close"]) > float(b2["open"]) else (
                    b1 if float(b1["close"]) > float(b1["open"]) else None)
                if ob is not None:
                    zones.append((float(ob["low"]), float(ob["high"])))
    return zones


def liquidity_sweep(bars: list[dict], side: str, lookback: int = 15) -> bool:
    """買:掃掉前低收回其上;賣:掃掉前高收回其下。"""
    if len(bars) <= lookback:
        return False
    latest = bars[-1]
    window = bars[-(lookback + 1):-1]
    if side == "Buy":
        ref = min(float(b["low"]) for b in window)
        return float(latest["low"]) < ref <= float(latest["close"])
    ref = max(float(b["high"]) for b in window)
    return float(latest["high"]) > ref >= float(latest["close"])


# ── 主偵測器 ──
@dataclass
class MicroSignal:
    accelerate: bool = False
    hold: bool = False
    reasons: list[str] = field(default_factory=list)


class MicrostructureDetector:
    """整合訊號;由 QuoteFeed 餵 tick,引擎每輪呼叫 signal(side)。"""

    AGGRESSIVE_WINDOW_SEC = 90.0  # 滾動窗(修正原版無衰減 bug)
    VPIN_TOXIC = 0.75

    def __init__(self, side: str):
        self.side = side
        self.ofi = OFIState()
        self.vpin = VPINState()
        # 逆勢主動流(買方視角=主動賣;賣方視角=主動買),滾動 (ts, size)
        self.adverse_flow: deque = deque()
        self.peak_window_flow = 0.0
        self.extreme = 0.0          # 買=session low、賣=session high
        self.extreme_ts = time.monotonic()
        self.day_extreme = 0.0      # 買=日低、賣=日高(初始化自 REST)
        self.val = self.poc = self.vah = 0.0
        self.zones: list[tuple[float, float]] = []
        self.sweep = False
        # v3 跨日結構(daily_context 餵入;買=支撐、賣=阻力)
        self.daily_levels: list[tuple[float, str]] = []
        self.prior_va: tuple[float, float, float] = (0.0, 0.0, 0.0)  # 昨日 VAL/POC/VAH
        # v2 智慧層
        self.bid1: tuple[float, int] = (0.0, 0)
        self.ask1: tuple[float, int] = (0.0, 0)
        self._vwap_pv = 0.0
        self._vwap_v = 0.0
        self.atr1m_pct = 0.0

    def set_daily_context(self, levels: list[tuple[float, str]]) -> None:
        """日線結構位(前日低/高、swing、日線 FVG、20 日極值)→ 掛單錨候選。"""
        self.daily_levels = [(float(p), lab) for p, lab in levels if p and p > 0]

    def set_prior_value_area(self, val: float, poc: float, vah: float) -> None:
        """昨日 TPO 價值區 prior:開盤初期今日 TPO 樣本不足時的結構參考。"""
        self.prior_va = (val, poc, vah)

    def warmup_trades(self, trades: list[dict]) -> int:
        """啟動暖機(UMEE 設計):重放今日逐筆,讓 VPIN 量桶與 session 極值在
        websocket 第一筆之前就有意義。回傳重放筆數。"""
        rows = sorted(trades, key=lambda t: t.get("time", 0))
        n = 0
        for t in rows:
            px = float(t.get("price") or 0)
            sz = float(t.get("size") or 0)
            if px <= 0 or sz <= 0:
                continue
            self.vpin.update(px, sz)
            if self.extreme <= 0 or (px < self.extreme if self.side == "Buy" else px > self.extreme):
                self.extreme = px  # 暖機不更新 extreme_ts(歷史新低不重置止穩計時)
            n += 1
        return n

    # tick 輸入
    def on_trade(self, price: float, size: float, bid: float, ask: float) -> None:
        self.vpin.update(price, size)
        if price > 0 and size > 0:  # 當日 VWAP
            self._vwap_pv += price * size
            self._vwap_v += size
        if self.extreme <= 0:
            self.extreme = price
        new_extreme = price < self.extreme if self.side == "Buy" else price > self.extreme
        if new_extreme:
            self.extreme = price
            self.extreme_ts = time.monotonic()
        adverse = (bid > 0 and price <= bid) if self.side == "Buy" else (ask > 0 and price >= ask)
        now = time.monotonic()
        if adverse:
            self.adverse_flow.append((now, size))
        while self.adverse_flow and now - self.adverse_flow[0][0] > self.AGGRESSIVE_WINDOW_SEC:
            self.adverse_flow.popleft()
        cur = sum(s for _, s in self.adverse_flow)
        self.peak_window_flow = max(self.peak_window_flow, cur)

    def on_book(self, bids: list[tuple[float, int]], asks: list[tuple[float, int]]) -> None:
        if bids and asks:
            self.ofi.update(bids[0][0], bids[0][1], asks[0][0], asks[0][1])
            self.bid1 = bids[0]
            self.ask1 = asks[0]
        self._book3 = (sum(s for _, s in bids[:3]), sum(s for _, s in asks[:3])) if bids and asks else (0, 0)

    def on_bars(self, bars: list[dict]) -> None:
        self.val, self.poc, self.vah = tpo_value_area(bars)
        self.zones = smc_zones(bars, self.side)
        self.sweep = liquidity_sweep(bars, self.side)
        tail = bars[-30:]
        if tail:  # 1 分 K 平均振幅(近 30 根)→ 波動自適應護欄的輸入
            self.atr1m_pct = sum(
                (float(b["high"]) - float(b["low"])) / max(float(b["close"]), 1e-9) for b in tail
            ) / len(tail)

    @property
    def vwap(self) -> float:
        return self._vwap_pv / self._vwap_v if self._vwap_v > 0 else 0.0

    def microprice(self) -> float:
        """深度加權公平價:(bid×askSize + ask×bidSize)/(bidSize+askSize)。"""
        (bp, bs), (ap, asz) = self.bid1, self.ask1
        if bp <= 0 or ap <= 0 or (bs + asz) <= 0:
            return 0.0
        return (bp * asz + ap * bs) / (bs + asz)

    def obi3(self) -> float:
        """三檔簿失衡 ∈ [0,1]:>0.5 買方厚。"""
        b3, a3 = getattr(self, "_book3", (0, 0))
        return b3 / (b3 + a3) if (b3 + a3) > 0 else 0.5

    def adaptive_passive(self, bid: float, ask: float) -> tuple[float, str] | None:
        """v2 智慧被動掛位(microprice/OBI):
        我方厚 + 價差 ≥2 tick → 讓一檔搶排隊優先權;對方厚 → 退一檔潛伏;否則掛最佳檔。"""
        from .ticks import add_ticks, tick_size
        if bid <= 0 or ask <= 0:
            return None
        spread_ticks = round((ask - bid) / tick_size(bid))
        obi = self.obi3()
        mp = self.microprice()
        if self.side == "Buy":
            if spread_ticks >= 2 and (obi >= 0.65 or (mp and mp > (bid + ask) / 2)):
                return add_ticks(bid, +1), f"improve(OBI {obi:.2f})"
            if obi <= 0.35:
                return add_ticks(bid, -1), f"lurk(OBI {obi:.2f})"
            return bid, f"join(OBI {obi:.2f})"
        if spread_ticks >= 2 and (obi <= 0.35 or (mp and mp < (bid + ask) / 2)):
            return add_ticks(ask, -1), f"improve(OBI {obi:.2f})"
        if obi >= 0.65:
            return add_ticks(ask, +1), f"lurk(OBI {obi:.2f})"
        return ask, f"join(OBI {obi:.2f})"

    def anchor_level(self, ref_price: float) -> tuple[float, str] | None:
        """結構掛單位:買 = ref 下方最近的結構(TPO VAL/POC、昨日 VAL/POC、多方 OB
        上緣、FVG 中線、日線支撐、日低近旁),取最高者;賣 = 鏡像取最低者。
        回傳 (價位, 依據) 或 None。"""
        cands: list[tuple[float, str]] = []
        pv_val, pv_poc, pv_vah = self.prior_va
        if self.side == "Buy":
            if 0 < self.val < ref_price:
                cands.append((self.val, "TPO VAL"))
            if 0 < self.poc < ref_price:
                cands.append((self.poc, "TPO POC"))
            if 0 < pv_val < ref_price:
                cands.append((pv_val, "昨日VAL"))
            if 0 < pv_poc < ref_price:
                cands.append((pv_poc, "昨日POC"))
            for lv, lab in self.daily_levels:
                if 0 < lv < ref_price:
                    cands.append((lv, lab))
            for b, t in self.zones:
                if 0 < t < ref_price:
                    cands.append((t, "OB/FVG 上緣"))
                mid = (b + t) / 2
                if 0 < mid < ref_price:
                    cands.append((mid, "OB/FVG 中線"))
            # 盤中即時追蹤:day_extreme 是啟動時的日低,extreme 隨每筆成交
            # 更新——取兩者較低者,盤中破低時錨自動下移(2026-07-14 修正)
            lows = [v for v in (self.day_extreme, self.extreme) if v > 0]
            day_lo = min(lows) if lows else 0.0
            if 0 < day_lo < ref_price:
                cands.append((day_lo * 1.002, "日低近旁"))
            if 0 < self.vwap < ref_price:
                cands.append((self.vwap, "VWAP"))
            if not cands:
                return None
            return max(cands, key=lambda x: x[0])
        # Sell
        if self.vah > 0 and self.vah > ref_price:
            cands.append((self.vah, "TPO VAH"))
        if self.poc > ref_price:
            cands.append((self.poc, "TPO POC"))
        if pv_vah > ref_price:
            cands.append((pv_vah, "昨日VAH"))
        if pv_poc > ref_price:
            cands.append((pv_poc, "昨日POC"))
        for lv, lab in self.daily_levels:
            if lv > ref_price:
                cands.append((lv, lab))
        for b, t in self.zones:
            if b > ref_price:
                cands.append((b, "OB/FVG 下緣"))
            mid = (b + t) / 2
            if mid > ref_price:
                cands.append((mid, "OB/FVG 中線"))
        # trailing-high:extreme 隨每筆成交追蹤盤中新高,錨跟著上移——
        # 開高走高日賣單不會卡在啟動時的舊日高(2026-07-14 修正)
        day_hi = max(self.day_extreme, self.extreme)
        if day_hi > ref_price:
            cands.append((day_hi * 0.998, "日高近旁"))
        if self.vwap > ref_price:
            cands.append((self.vwap, "VWAP"))
        if not cands:
            return None
        return min(cands, key=lambda x: x[0])

    # 綜合判定
    def signal(self, ref_price: float, strict: bool = False) -> MicroSignal:
        sig = MicroSignal()
        if self.vpin.current >= self.VPIN_TOXIC:
            sig.hold = True
            sig.reasons.append(f"VPIN {self.vpin.current:.2f} 毒性高")
            return sig

        reasons: list[str] = []
        # 自適應止穩窗(UMEE 條件 1):OFI 強攻(>3)時 60s 放寬為 30s
        stab_window = 30.0 if self.ofi.sma > 3.0 else 60.0
        stabilized = (time.monotonic() - self.extreme_ts) > stab_window
        if stabilized:
            reasons.append("極值 60s 未再刷新")
        cur_flow = sum(s for _, s in self.adverse_flow)
        exhausted = self.peak_window_flow > 0 and cur_flow < 0.3 * self.peak_window_flow
        if exhausted:
            reasons.append("逆勢主動流竭盡(滾動窗)")
        b3, a3 = getattr(self, "_book3", (0, 0))
        support = (b3 > 2.0 * a3) if self.side == "Buy" else (a3 > 2.0 * b3)
        if support:
            reasons.append("三檔簿支撐")
        flow_ok = self.ofi.sma > 0 if self.side == "Buy" else self.ofi.sma < 0
        if flow_ok:
            reasons.append(f"OFI {self.ofi.sma:+.1f}")
        in_value = False
        if self.side == "Buy":
            in_value = (self.val > 0 and ref_price <= self.val) or \
                (self.day_extreme > 0 and ref_price <= self.day_extreme * 1.015)
        else:
            in_value = (self.vah > 0 and ref_price >= self.vah) or \
                (self.day_extreme > 0 and ref_price >= self.day_extreme * 0.985)
        in_value = in_value or any(b <= ref_price <= t for b, t in self.zones) or self.sweep
        if in_value:
            reasons.append("價位在價值區/SMC 區/掃蕩後")

        # 加速門檻:預設 = 四類中至少三類;strict(狙擊模式)= 全部 AND
        buckets = [stabilized, exhausted or self.sweep, flow_ok or support, in_value]
        threshold = 4 if strict else 3
        if sum(buckets) >= threshold:
            sig.accelerate = True
            sig.reasons = reasons
        return sig
