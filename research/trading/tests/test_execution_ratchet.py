"""執行引擎「撈相對低/相對高」不變式的回歸測試。

守護兩條 2026-07-20 定案的合約(買量買在當日低、賣量賣在當日高):

1. **順向棘輪**(`_ratchet_limit`):結構錨定 profile 的被動/錨定段掛價只准往
   有利方向走——賣不改低、買不改高。防止今日 VWAP/TPO 阻力隨盤下滑,把賣單
   錨定「上方最近阻力」一路改低賤賣(台光電 2383 實盤事故)。

2. **lone-sweep 不跨價**:結構錨定 profile 只在狙擊級全 AND 訊號才跨價;單獨
   一個流動性掃蕩訊號不得獨立觸發跨價(否則盤中冷啟動一偵測到掃蕩就急著成交)。

跑:
    cd /Users/zaoldyeck/Documents/scala/quantlib
    uv run --project research python -m pytest research/tests/test_execution_ratchet.py -v
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from research.brokers.fubon import FubonBroker
from research.trading.execution.engine import ExecutionEngine, Quote
from research.trading.execution.microstructure import MicrostructureDetector
from research.trading.execution.policy import (BUY_NORMAL, BUY_PATIENT,
                                               SELL_EXIT, SELL_NORMAL)

TAIPEI = ZoneInfo("Asia/Taipei")
_LOG = Path("/tmp/qlexec_ratchet_test.jsonl")


def _engine(side: str, profile, **kw) -> ExecutionEngine:
    return ExecutionEngine(FubonBroker(dry_run=True), code="0000", side=side,
                           qty=10, profile=profile, round_sec=0.0, live=False,
                           feed=kw.pop("feed", None), log_path=_LOG, **kw)


# ── 1. 順向棘輪 ──────────────────────────────────────────────

def test_patient_sell_ratchet_never_lowers():
    """賣單:今日阻力隨盤下滑(掛價候選一路走低)→ 棘輪守住歷史最高賣價。"""
    eng = _engine("Sell", SELL_EXIT)
    falling = [4495.0, 4480.0, 4460.0, 4450.0, 4470.0]  # 阻力下滑後略回升
    got = [eng._ratchet_limit(p, aggressive=False) for p in falling]
    assert got == [4495.0] * 5, f"賣單掛價被改低:{got}"
    # 真的更高的阻力出現 → 棘輪允許往上(賣更高)
    assert eng._ratchet_limit(4600.0, aggressive=False) == 4600.0


def test_patient_buy_ratchet_never_raises():
    """買單:今日支撐隨盤上移(掛價候選一路走高)→ 棘輪守住歷史最低買價。"""
    eng = _engine("Buy", BUY_PATIENT)
    rising = [306.0, 310.0, 315.0, 308.0]
    got = [eng._ratchet_limit(p, aggressive=False) for p in rising]
    assert got == [306.0] * 4, f"買單掛價被改高:{got}"
    # 真的更低的支撐出現 → 棘輪允許往下(買更低)
    assert eng._ratchet_limit(300.0, aggressive=False) == 300.0


def test_ratchet_exempt_when_aggressive():
    """boost / 過死線的主動取價不受棘輪限制,且不污染棘輪基準。"""
    eng = _engine("Sell", SELL_EXIT)
    assert eng._ratchet_limit(4495.0, aggressive=False) == 4495.0
    # 主動取價允許跨到 bid(低於 floor)
    assert eng._ratchet_limit(4400.0, aggressive=True) == 4400.0
    # 主動取價不更新棘輪 → 下一個被動輪仍守在 4495
    assert eng._ratchet_limit(4460.0, aggressive=False) == 4495.0


def test_non_anchor_profile_not_ratcheted():
    """一般 profile(有死線、該完成就完成)不套棘輪。"""
    eng = _engine("Sell", SELL_NORMAL)  # structure_anchor=False
    got = [eng._ratchet_limit(p, aggressive=False) for p in [100.0, 98.0, 96.0]]
    assert got == [100.0, 98.0, 96.0], f"一般 profile 被誤套棘輪:{got}"


# ── 2. lone-sweep 不跨價 ─────────────────────────────────────

class _StableFeed:
    """固定買賣價差的行情(每次 snapshot 都是新鮮 ts)。"""

    def __init__(self, bid: float, ask: float):
        self.bid, self.ask = bid, ask

    def start(self) -> None:  # pragma: no cover - 介面對齊
        pass

    def refresh_rest(self) -> None:
        pass

    def snapshot(self) -> Quote:
        return Quote(self.bid, self.ask, (self.bid + self.ask) / 2, time.time())


def _run_bounded(side: str, profile, micro, feed, max_rounds: int = 4):
    stop_event = threading.Event()
    state = {"n": 0}

    def _sleep(_s: float) -> None:
        state["n"] += 1
        if state["n"] >= max_rounds:
            stop_event.set()

    clock_state = {"t": datetime(2026, 7, 20, 10, 30, tzinfo=TAIPEI)}  # 盤中冷啟動

    def fake_clock() -> datetime:
        clock_state["t"] += timedelta(seconds=60)
        return clock_state["t"]

    eng = ExecutionEngine(
        FubonBroker(dry_run=True), code="0000", side=side, qty=10, profile=profile,
        round_sec=1.0, live=False, feed=feed, micro=micro, clock=fake_clock,
        sleep=_sleep, stop_event=stop_event, manage_sigint=False, log_path=_LOG)
    eng._guards = lambda: None  # type: ignore[method-assign]
    eng._refresh_bars = lambda: None  # type: ignore[method-assign]  # 不打 SDK
    eng._afterhours_completion = lambda _stop: None  # type: ignore[method-assign]
    return eng.run()


def _placed(result) -> list[float]:
    return [e["price"] for e in result.events if e["event"] == "paper_place"]


def test_patient_buy_lone_sweep_does_not_cross():
    """結構錨定買單:只有掃蕩訊號(無其他 bucket)→ 修正後掛在支撐、不跨到 ask。"""
    micro = MicrostructureDetector("Buy")
    micro.sweep = True  # 只有掃蕩,湊不滿狙擊級全 AND
    micro.set_daily_context([(99.0, "昨日低"), (98.0, "20日低")])  # 下方支撐
    feed = _StableFeed(bid=100.0, ask=100.5)
    r = _run_bounded("Buy", BUY_PATIENT, micro, feed)
    placed = _placed(r)
    assert placed, "應有掛單事件"
    assert max(placed) < 100.5, f"耐心買單被 lone sweep 跨價到 ask:{placed}"
    assert r.filled_qty == 0, "掛在支撐不應成交(市價未觸及)"


def test_balanced_buy_sweep_still_crosses():
    """一般買單(非結構錨定):掃蕩快速通道仍保留,允許跨價取量。"""
    micro = MicrostructureDetector("Buy")
    micro.sweep = True
    feed = _StableFeed(bid=100.0, ask=100.5)
    r = _run_bounded("Buy", BUY_NORMAL, micro, feed)
    placed = _placed(r)
    assert placed, "應有掛單事件"
    assert max(placed) >= 100.5, f"一般 profile 的掃蕩快速通道被誤關:{placed}"


# ── 3. 當日極值錨校準(1 分 K 補上訂閱前的極值)──────────────

def test_on_bars_calibrates_day_extreme():
    """買:day_extreme 只往更低收斂;賣:只往更高收斂(補上開盤那段)。"""
    mbuy = MicrostructureDetector("Buy")
    mbuy.day_extreme = 100.0  # 啟動時 REST 抓到的日低
    mbuy.on_bars([{"open": 101, "high": 102, "low": 98, "close": 99},
                  {"open": 99, "high": 100, "low": 97.5, "close": 98}])
    assert mbuy.day_extreme == 97.5, f"買方日低未收斂到 1 分 K 更低值:{mbuy.day_extreme}"

    msell = MicrostructureDetector("Sell")
    msell.day_extreme = 100.0
    msell.on_bars([{"open": 101, "high": 103, "low": 100, "close": 102}])
    assert msell.day_extreme == 103.0, f"賣方日高未收斂到 1 分 K 更高值:{msell.day_extreme}"

    # 反向不污染:1 分 K 的極值比現有 day_extreme 差 → 不動
    mbuy2 = MicrostructureDetector("Buy")
    mbuy2.day_extreme = 90.0
    mbuy2.on_bars([{"open": 101, "high": 102, "low": 95, "close": 99}])
    assert mbuy2.day_extreme == 90.0, "買方日低被較高的 1 分 K 低點污染"


# ── 4. 目標計分板(買距當日低、賣距當日高)──────────────────

def test_capture_bps_scorecard():
    from research.trading.execution.engine import LegResult
    # 買在 101、當日低 100 → 距低 100 bps
    buy = LegResult(code="0000", side="Buy", qty=10, filled_qty=10,
                    fill_notional=1010.0, day_extreme=100.0)
    assert buy.capture_bps() == 100.0
    # 賣在 99、當日高 100 → 距高 100 bps
    sell = LegResult(code="0000", side="Sell", qty=10, filled_qty=10,
                     fill_notional=990.0, day_extreme=100.0)
    assert sell.capture_bps() == 100.0
    # 買在最低 → 0 bps(完美)
    perfect = LegResult(code="0000", side="Buy", qty=10, filled_qty=10,
                        fill_notional=1000.0, day_extreme=100.0)
    assert perfect.capture_bps() == 0.0
    # 無成交 / 缺極值 → None
    assert LegResult(code="0000", side="Buy", qty=10).capture_bps() is None
    assert LegResult(code="0000", side="Sell", qty=10, filled_qty=5,
                     fill_notional=500.0, day_extreme=0.0).capture_bps() is None


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
