"""盤中執行引擎:行情訂閱 × 階梯改價 × 訂單生命週期 × TCA 日誌。

一次執行一條腿(code/side/qty)。預設 dry-run:訂閱真實行情、模擬成交、
不送任何單;`--live` 由 CLI 閘門(FUBON_DRY_RUN=false 等)武裝,啟動永遠是
使用者的動作。所有事件寫 JSONL(research/out/trading/executions/)。

部位成交帳(live)以「每張委託的 seen_fill 增量」記帳,cancel-replace 前後
都先同步,不重複計。
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from research.brokers.fubon import (FubonBroker, StockOrderRequest,
                                    is_transient_network_error, net_retry)

from .daily_context import dump_candles, load_daily_levels, load_prior_value_area
from .policy import LadderProfile, price_collar, target_price
from .ticks import add_ticks

TAIPEI = ZoneInfo("Asia/Taipei")
STATE_DIR = Path("research/state/trading")
HALT_FILE = STATE_DIR / "HALT"
LOCK_DIR = STATE_DIR / "exec_locks"
OUT_DIR = Path("research/out/trading/executions")

SESSION_START = "09:00"
SESSION_END = "13:30"
# 盤後定價交易(14:30 一次撮合、成交價=當日收盤價):零股 13:40 起收單、
# 整股定價 14:00 起收單;14:25 後不再嘗試(留申報餘裕)
AFTERHOURS_ODD_OPEN, AFTERHOURS_FIXING_OPEN = "13:40", "14:00"
AFTERHOURS_LAST, AFTERHOURS_MATCH_DONE = "14:25", "14:31"
# 官方狀態碼(fbs.com.tw/TradeAPI 錯誤碼與狀態碼對照表):
# 0 預約單、4/8/9 傳送中、10 委託成功、30 未成交刪單、40 部分成交剩餘取消、
# 50 完全成交、90 失敗
_STATUS_FILLED = 50
_TERMINAL_STATUSES = {30, 40, 50, 90}


def _now() -> datetime:
    return datetime.now(TAIPEI)


def _hhmm(dt: datetime) -> str:
    return dt.strftime("%H:%M")


@dataclass
class Quote:
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0
    ts: float = 0.0

    MAX_AGE_SEC = 45.0  # 超過即視為過期(ws 靜默死亡/斷網;REST 兜底會刷新 ts)

    def usable(self) -> bool:
        return self.last > 0 or (self.bid > 0 and self.ask > 0)

    def fresh(self) -> bool:
        """報價夠新才可用來定價。ws 靜默死亡後最後一筆會永遠留在記憶體,
        usable() 恆為 True——不看 ts 就是拿舊價交易(2026-07-15 審計)。"""
        return self.usable() and (time.time() - self.ts) <= self.MAX_AGE_SEC


class QuoteFeed:
    """Fubon 行情:websocket books/trades 為主,REST quote 兜底。"""

    def __init__(self, broker: FubonBroker, symbol: str, detector: Any | None = None):
        self.broker = broker
        self.symbol = symbol
        self.detector = detector
        self.q = Quote()
        self._lock = threading.Lock()

    def start(self) -> None:
        self.broker.sdk.init_realtime()
        ws = self.broker.sdk.marketdata.websocket_client.stock
        ws.on("message", self._on_message)
        ws.connect()
        time.sleep(1.0)
        for channel in ("books", "trades"):
            ws.subscribe({"channel": channel, "symbol": self.symbol})
            time.sleep(0.2)
        self.refresh_rest()

    def _on_message(self, message: str) -> None:
        try:
            data = json.loads(message)
        except Exception:
            return
        if data.get("event") != "data":
            return
        payload = data.get("data", {})
        if payload.get("symbol") != self.symbol:
            return
        if payload.get("isTrial"):  # 開盤前試撮,不作數(吸收自 smart_execution)
            return
        with self._lock:
            bids = payload.get("bids")
            asks = payload.get("asks")
            if bids is not None or asks is not None:  # books 頻道
                if bids:
                    self.q.bid = float(bids[0].get("price") or 0)
                if asks:
                    self.q.ask = float(asks[0].get("price") or 0)
                self.q.ts = time.time()
                if self.detector is not None:
                    try:
                        tb = [(float(b.get("price")), int(b.get("size") or 0)) for b in (bids or []) if b.get("price")]
                        ta = [(float(a.get("price")), int(a.get("size") or 0)) for a in (asks or []) if a.get("price")]
                        self.detector.on_book(tb, ta)
                    except Exception:
                        pass
            elif payload.get("price") is not None:  # trades 頻道
                self.q.last = float(payload["price"])
                self.q.ts = time.time()
                if self.detector is not None:
                    try:
                        self.detector.on_trade(
                            float(payload["price"]), float(payload.get("size") or 0),
                            float(payload.get("bid") or self.q.bid or 0),
                            float(payload.get("ask") or self.q.ask or 0))
                    except Exception:
                        pass

    def refresh_rest(self) -> None:
        try:
            res = self.broker.sdk.marketdata.rest_client.stock.intraday.quote(symbol=self.symbol)
            d = res if isinstance(res, dict) else getattr(res, "__dict__", {})
            with self._lock:
                self.q.last = float(d.get("lastPrice") or d.get("closePrice") or self.q.last or 0)
                bids = d.get("bids") or []
                asks = d.get("asks") or []
                if bids:
                    self.q.bid = float(bids[0].get("price") or self.q.bid or 0)
                if asks:
                    self.q.ask = float(asks[0].get("price") or self.q.ask or 0)
                self.q.ts = time.time()
            return
        except Exception:
            pass  # 換交易通道 snapshot 兜底
        try:
            res = self.broker.sdk.stock.query_symbol_snapshot(self.broker.account, self.symbol)
            d = getattr(res, "data", None)
            if d is not None:
                with self._lock:
                    for attr, key in (("last", "last_price"), ("bid", "buy_price"), ("ask", "sell_price")):
                        v = getattr(d, key, None)
                        if v:
                            setattr(self.q, attr, float(v))
                    self.q.ts = time.time()
        except Exception:
            pass  # websocket 為主;兩層 fallback 都失敗不致命

    def snapshot(self) -> Quote:
        with self._lock:
            q = Quote(self.q.bid, self.q.ask, self.q.last, self.q.ts)
        if q.bid <= 0 and q.last > 0:
            q.bid = add_ticks(q.last, -1)
        if q.ask <= 0 and q.last > 0:
            q.ask = add_ticks(q.last, +1)
        return q



class MarketHubView:
    """單一標的的行情視圖(給 ExecutionEngine 用,介面同 QuoteFeed)。"""

    def __init__(self, hub: "MarketHub", symbol: str):
        self.hub = hub
        self.symbol = symbol

    def snapshot(self) -> Quote:
        return self.hub.snapshot(self.symbol)

    def refresh_rest(self) -> None:
        self.hub.refresh_rest(self.symbol)


class MarketHub:
    """一條 websocket 供多檔併發:訊息按 symbol 派發到各自 Quote 與 detector。"""

    # 行情靜默多久視為 websocket 停擺(斷網/券商端斷線)→ 觸發重連。
    # 台股連續交易無午休,books 變動頻繁;180s 全無訊息幾乎必是連線問題。
    STALE_SEC = 180.0

    def __init__(self, broker: FubonBroker):
        self.broker = broker
        self._q: dict[str, Quote] = {}
        self._det: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._started = False
        self._last_tick = 0.0
        self._stop = threading.Event()

    def add(self, symbol: str, detector: Any | None = None) -> MarketHubView:
        with self._lock:
            self._q.setdefault(symbol, Quote())
            if detector is not None:
                self._det[symbol] = detector
        if self._started:
            self._subscribe(symbol)
        return MarketHubView(self, symbol)

    def start(self) -> None:
        """連線 + 啟動看門狗。沒網路 → 等網路回來(絕不讓呼叫端死掉)。"""
        # 重登會讓 sdk.marketdata 消失 → 訂閱通知,立刻觸發重建(≤30s),
        # 而不是等 180s 靜默才發現(期間引擎會因報價過期自動停手,不會亂掛)
        self.broker.set_on_session_replaced(self._invalidate)
        net_retry(self._connect, what="行情連線")
        threading.Thread(target=self._watchdog, name="mkt-watchdog", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    CONNECT_TIMEOUT = 20.0

    def _connect(self) -> None:
        """重建整條行情鏈。init_realtime() 會造全新的 MarketData(新 ws + 新
        rest),故所有回呼與訂閱都必須重來一次(第一手:fugle sdk.py:23-29)。"""
        self.broker.sdk.init_realtime()  # 需已登入的 session;斷網會拋 → 交給呼叫端重試
        ws = self.broker.sdk.marketdata.websocket_client.stock
        ws.on("message", self._on_message)
        # pyee 對「沒有監聽者的 error 事件」會直接 raise 並殺死 ws 執行緒 → 必須註冊
        ws.on("error", lambda *a: print(f"[market] ws error:{str(a)[:100]}", flush=True))
        ws.on("disconnect", self._on_disconnect)
        self._bounded_connect(ws)
        time.sleep(1.0)
        for symbol in list(self._q):
            self._subscribe(symbol)
        self._started = True
        self._last_tick = time.time()
        for symbol in list(self._q):
            self.refresh_rest(symbol)

    def _bounded_connect(self, ws: Any) -> None:
        """有界連線。SDK 的 connect() 是 `while True:` 無 sleep 的忙等,靠
        auth_status 跳出;沒網路時 on_open 永不觸發 → **永不返回、永不拋例外、
        燒滿一個 CPU 核心**(2026-07-15 實測 + fugle client.py:190-198 原始碼)。
        逾時就用 SDK 自己的逾時出口(check_auth_status 同款:設 error +
        UNAUTHENTICATED)把忙等迴圈叫醒,避免遺留空轉執行緒。
        """
        done = threading.Event()
        err: list[BaseException] = []

        def _do() -> None:
            try:
                ws.connect()
            except BaseException as exc:  # noqa: BLE001 - 轉交主執行緒判斷
                err.append(exc)
            finally:
                done.set()

        threading.Thread(target=_do, name=f"ws-connect", daemon=True).start()
        if not done.wait(self.CONNECT_TIMEOUT):
            timeout = TimeoutError(
                f"行情 websocket 連線逾時(>{self.CONNECT_TIMEOUT:.0f}s,沒網路?)")
            try:  # 叫醒忙等迴圈:它會 close socket 並把 error 拋在自己的執行緒裡
                from fugle_marketdata.websocket.client import AuthenticationState
                ws.error = timeout
                ws.auth_status = AuthenticationState.UNAUTHENTICATED
            except Exception:  # noqa: BLE001
                pass
            done.wait(3.0)
            raise timeout
        if err:
            raise err[0]

    def _invalidate(self) -> None:
        """session/連線失效 → 標記停擺,由看門狗重建整條行情鏈。"""
        self._last_tick = 0.0

    def _on_disconnect(self, *args: Any) -> None:
        print(f"[market] websocket 斷線{str(args)[:80]} —— 看門狗將重建行情鏈", flush=True)
        self._invalidate()

    def _watchdog(self) -> None:
        """行情看門狗:ws 停擺(斷網/券商端斷線/重登換 sdk)→ REST 兜底 + 自動重連。

        斷網後 broker 會建立全新 sdk;本 hub 每次都從 `self.broker.sdk` 取用,
        故重連即自動接上新 session。任何失敗都只記錄不拋——看門狗死掉等於
        失去自癒能力,比暫時沒行情更糟。
        """
        while not self._stop.wait(30.0):
            hh = _hhmm(_now())
            if not (SESSION_START <= hh <= AFTERHOURS_MATCH_DONE):
                continue
            if time.time() - self._last_tick < self.STALE_SEC:
                continue  # _on_disconnect 會把 _last_tick 歸零 → 立刻進入重建
            for symbol in list(self._q):  # 先 REST 兜底:不依賴 ws 也要有報價
                self.refresh_rest(symbol)
            try:
                print(f"[market] 行情靜默 >{self.STALE_SEC:.0f}s → 重連 websocket…", flush=True)
                self._connect()
                print("[market] websocket 已重連,行情續傳", flush=True)
            except Exception as exc:  # noqa: BLE001 - 看門狗永不死
                print(f"[market] 重連失敗({str(exc)[:80]});REST 兜底中,稍後再試", flush=True)

    def _subscribe(self, symbol: str) -> None:
        ws = self.broker.sdk.marketdata.websocket_client.stock
        for channel in ("books", "trades"):
            ws.subscribe({"channel": channel, "symbol": symbol})
            time.sleep(0.15)

    def _on_message(self, message: str) -> None:
        try:
            data = json.loads(message)
        except Exception:
            return
        if data.get("event") != "data":
            return
        payload = data.get("data", {})
        symbol = payload.get("symbol")
        if symbol not in self._q or payload.get("isTrial"):
            return
        q = self._q[symbol]
        det = self._det.get(symbol)
        self._last_tick = time.time()  # 看門狗的活性訊號
        with self._lock:
            bids = payload.get("bids")
            asks = payload.get("asks")
            if bids is not None or asks is not None:
                if bids:
                    q.bid = float(bids[0].get("price") or 0)
                if asks:
                    q.ask = float(asks[0].get("price") or 0)
                q.ts = time.time()
                if det is not None:
                    try:
                        tb = [(float(b.get("price")), int(b.get("size") or 0)) for b in (bids or []) if b.get("price")]
                        ta = [(float(a.get("price")), int(a.get("size") or 0)) for a in (asks or []) if a.get("price")]
                        det.on_book(tb, ta)
                    except Exception:
                        pass
            elif payload.get("price") is not None:
                q.last = float(payload["price"])
                q.ts = time.time()
                if det is not None:
                    try:
                        det.on_trade(float(payload["price"]), float(payload.get("size") or 0),
                                     float(payload.get("bid") or q.bid or 0),
                                     float(payload.get("ask") or q.ask or 0))
                    except Exception:
                        pass

    def snapshot(self, symbol: str) -> Quote:
        with self._lock:
            src = self._q[symbol]
            q = Quote(src.bid, src.ask, src.last, src.ts)
        if q.bid <= 0 and q.last > 0:
            q.bid = add_ticks(q.last, -1)
        if q.ask <= 0 and q.last > 0:
            q.ask = add_ticks(q.last, +1)
        return q

    def refresh_rest(self, symbol: str) -> None:
        try:
            res = self.broker.sdk.marketdata.rest_client.stock.intraday.quote(symbol=symbol)
            d = res if isinstance(res, dict) else getattr(res, "__dict__", {})
            with self._lock:
                q = self._q[symbol]
                q.last = float(d.get("lastPrice") or d.get("closePrice") or q.last or 0)
                bids = d.get("bids") or []
                asks = d.get("asks") or []
                if bids:
                    q.bid = float(bids[0].get("price") or q.bid or 0)
                if asks:
                    q.ask = float(asks[0].get("price") or q.ask or 0)
                q.ts = time.time()
        except Exception:
            pass


@dataclass
class LegResult:
    code: str
    side: str
    qty: int
    filled_qty: int = 0
    fill_notional: float = 0.0
    arrival: float = 0.0
    rounds: int = 0
    aborted: bool = False
    events: list = field(default_factory=list)

    @property
    def avg_price(self) -> float:
        return self.fill_notional / self.filled_qty if self.filled_qty else 0.0

    def shortfall_bps(self) -> float | None:
        if not self.filled_qty or self.arrival <= 0:
            return None
        sgn = 1.0 if self.side == "Buy" else -1.0
        return round(sgn * (self.avg_price / self.arrival - 1.0) * 1e4, 1)


class ExecutionEngine:
    def __init__(
        self,
        broker: FubonBroker,
        *,
        code: str,
        side: str,
        qty: int,
        profile: LadderProfile,
        round_sec: float = 60.0,
        live: bool = False,
        feed: Any | None = None,
        clock: Any = _now,
        sleep: Any = time.sleep,
        log_path: Path | None = None,
        micro: Any | None = None,
        allow_refill: bool = False,
        stop_event: Any | None = None,
        manage_sigint: bool = True,
        board: Any | None = None,
        avoid_open_min: int = 3,
        cap_auto: bool = False,
        slice_qty: int | None = None,
        trigger_strict: bool = False,
    ):
        assert side in ("Buy", "Sell")
        self.broker = broker
        self.code = code
        self.side = side
        self.qty = int(qty)
        self.profile = profile
        self.round_sec = round_sec
        self.live = live
        self.feed = feed
        self.clock = clock
        self.sleep = sleep
        self.micro = micro
        self.allow_refill = allow_refill
        self.stop_event = stop_event
        self.manage_sigint = manage_sigint
        self.board = board
        self.avoid_open_min = int(avoid_open_min)
        self.cap_auto = cap_auto
        self.trigger_strict = trigger_strict
        self.cap_pct_eff = profile.cap_pct
        # 大單 TWAP 切片:整股 ≥2 張自動切 1 張/child(降低衝擊);零股不切
        self.slice_qty = slice_qty or (1000 if qty >= 2000 else qty)
        self._bars_refreshed = 0.0
        self._last_bars: list[dict] = []  # 收盤 dump 用(1 分 K 自建歷史)
        # 進度基準:進度 = 今日累計成交 − 基準(見 _reconcile_progress)
        self._baseline_filled = 0
        self._baseline_money = 0.0
        # 成交即時推播的喚醒訊號(set_on_filled → wake.set() → 立刻結束本輪等待;
        # 輪詢降為備援節拍——2026-07-14 修正「App 都成交了程式過一會兒才停」)
        self.wake = threading.Event()
        self.market_type = "IntradayOdd" if self.qty < 1000 else "Common"
        # working = 現行委託:{price, qty, seq_no, seen_fill}
        self.working: dict[str, Any] | None = None
        # 結構錨定 profile 的順向棘輪(見 _ratchet_limit):賣掛價只准往上、買掛價
        # 只准往下,防止今日 VWAP/TPO 阻力隨盤下滑把耐心單一路改低賤賣
        self._rest_ratchet: float | None = None
        self.result = LegResult(code=code, side=side, qty=qty)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        stamp = _now().strftime("%Y%m%d_%H%M%S")
        self.log_path = log_path or OUT_DIR / f"{stamp}_{side.lower()}_{code}.jsonl"
        self._lock_file: Path | None = None

    # ── 日誌 ──
    def log(self, event: str, **kw: Any) -> None:
        rec = {"ts": _now().isoformat(timespec="seconds"), "event": event,
               "code": self.code, "side": self.side, **kw}
        self.result.events.append(rec)
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print(f"[{rec['ts']}] {event} {kw if kw else ''}")

    # ── 安全閘 ──
    def _guards(self) -> None:
        now = self.clock()
        if not (SESSION_START <= _hhmm(now) <= SESSION_END):
            raise RuntimeError(f"不在交易時段(09:00–13:30 台北),現在 {_hhmm(now)}")
        if HALT_FILE.exists():
            raise RuntimeError(f"偵測到 kill switch:{HALT_FILE}(移除後才可執行)")
        LOCK_DIR.mkdir(parents=True, exist_ok=True)
        self._lock_file = LOCK_DIR / f"{now:%Y%m%d}_{self.side}_{self.code}.lock"
        if self._lock_file.exists():
            try:
                owner_pid = int(json.loads(self._lock_file.read_text(encoding="utf-8")).get("pid", -1))
            except Exception:
                owner_pid = -1
            alive = False
            if owner_pid > 0:
                try:
                    os.kill(owner_pid, 0)
                    alive = True
                except (ProcessLookupError, PermissionError):
                    alive = False
            if alive:
                raise RuntimeError(
                    f"另一個執行器正在跑同一腿(PID {owner_pid},{self._lock_file})——"
                    f"不可同時兩個;要換手先終止它")
            self.log("stale_lock_takeover", path=str(self._lock_file), dead_pid=owner_pid)
            self._lock_file.unlink()
        self._lock_file.write_text(
            json.dumps({"pid": os.getpid(), "log": self.log_path.name}), encoding="utf-8")


    # ── live 訂單輔助 ──
    def _find_order(self, seq_no: Any) -> Any | None:
        orders = self.broker.get_order_results()
        for o in getattr(orders, "data", []) or []:
            if str(getattr(o, "seq_no", "")) == str(seq_no):
                return o
        return None

    @staticmethod
    def _filled_qty_of(obj: Any) -> int:
        for attr in ("filled_qty", "filled_quantity", "filled_lot", "deal_qty"):
            v = getattr(obj, attr, None)
            if v is not None:
                try:
                    return int(v)
                except (TypeError, ValueError):
                    continue
        return 0

    def _register_fill(self, qty: int, price: float) -> None:
        if qty <= 0:
            return
        self.result.filled_qty += qty
        self.result.fill_notional += qty * price
        self.log("fill", qty=qty, price=price,
                 cum=self.result.filled_qty, remaining=self.qty - self.result.filled_qty)
        if self.board:
            left = self.qty - self.result.filled_qty
            self.board.update(self.code,
                              f"✅ 全數成交 @ {self.result.avg_price:g}" if left <= 0
                              else f"部分成交 {self.result.filled_qty}/{self.qty}")

    def _today_totals(self, orders: Any) -> tuple[int, float]:
        """今日「本代碼 × 本方向」的累計成交(股數, 成交金額)。

        委託回報是當日成交的唯一權威源(filled_history 盤中查不到今天的成交
        ——2026-07-09 實盤事故)。金額優先取 filled_money(真實成交價),
        缺欄位才以委託價近似。
        """
        qty_sum, money_sum = 0, 0.0
        for o in getattr(orders, "data", []) or []:
            if str(getattr(o, "stock_no", "")) != self.code:
                continue
            if self.side not in str(getattr(o, "buy_sell", "")):
                continue
            f = self._filled_qty_of(o)
            if f <= 0:
                continue
            qty_sum += f
            money = 0.0
            for attr in ("filled_money", "filled_amount"):
                try:
                    money = float(getattr(o, attr, 0) or 0)
                except (TypeError, ValueError):
                    money = 0.0
                if money > 0:
                    break
            if money <= 0:
                try:
                    money = f * float(getattr(o, "price", 0) or 0)
                except (TypeError, ValueError):
                    money = 0.0
            money_sum += money
        return qty_sum, money_sum

    def _reconcile_progress(self, orders: Any, *, note: str) -> None:
        """絕對對帳:進度 = 今日累計成交 − 基準。冪等,可重複呼叫。

        為什麼是絕對而非增量:增量記帳(seen_fill)只在「每一筆委託我們都知道
        seq_no」時成立——**送單當下斷線**時委託可能已到交易所而我們收不到回應,
        那筆成交會永遠漏帳,接著重掛就是重複下單(2026-07-15 斷網事故暴露的
        真實風險)。以委託回報重算絕對值則天然冪等、自癒。
        """
        if not self.live:
            return
        total, money = self._today_totals(orders)
        progress = max(total - self._baseline_filled, 0)
        self.result.fill_notional = max(money - self._baseline_money, 0.0)
        if progress == self.result.filled_qty:
            return
        delta = progress - self.result.filled_qty
        self.result.filled_qty = progress
        left = self.qty - progress
        self.log("fill" if delta > 0 else "progress_corrected",
                 qty=delta, cum=progress, remaining=max(left, 0),
                 avg_price=round(self.result.avg_price, 4), note=note)
        if self.board:
            self.board.update(self.code,
                              f"✅ 全數成交 @ {self.result.avg_price:g}" if left <= 0
                              else f"部分成交 {progress}/{self.qty}")

    def _adopt_working(self, orders: Any, *, note: str) -> bool:
        """認領交易所端的在途同向委託(防重複下單的關鍵)。"""
        for o in getattr(orders, "data", []) or []:
            if str(getattr(o, "stock_no", "")) != self.code:
                continue
            if self.side not in str(getattr(o, "buy_sell", "")):
                continue
            if getattr(o, "status", None) in _TERMINAL_STATUSES:
                continue
            seq = getattr(o, "seq_no", None)
            if self.working is not None and str(self.working.get("seq_no")) == str(seq):
                return True  # 已在管同一張
            self.working = {"price": float(getattr(o, "price", 0) or 0),
                            "qty": int(getattr(o, "quantity", 0) or 0),
                            "seq_no": seq, "seen_fill": self._filled_qty_of(o)}
            self.log(note, seq_no=str(seq), price=self.working["price"],
                     qty=self.working["qty"], already_filled=self.working["seen_fill"])
            if self.board:
                self.board.update(self.code, f"接管在途單 @ {self.working['price']:g}")
            return True
        return False

    def _sync_live_fills(self) -> None:
        """對帳今日成交(絕對)+ 維護 working 狀態。斷網時 broker 層會等網路回來。"""
        if not self.live:
            return
        orders = self.broker.get_order_results()
        self._reconcile_progress(orders, note="sync")
        if self.working is None:
            return
        cur = None
        for o in getattr(orders, "data", []) or []:
            if str(getattr(o, "seq_no", "")) == str(self.working["seq_no"]):
                cur = o
                break
        if cur is None:
            return
        self.working["seen_fill"] = self._filled_qty_of(cur)
        if (getattr(cur, "status", None) in _TERMINAL_STATUSES
                or self.working["seen_fill"] >= self.working["qty"]):
            self.working = None

    def _net_degraded(self, exc: Exception) -> None:
        """本輪撞到暫時性網路故障:記錄 → 稍候 → 網路回來後對帳續管。

        絕不終止本腿(2026-07-15 事故);恢復後**先對帳再動作**,因為斷線期間
        可能已成交、或有「送出後才斷線」的不知情在途委託。
        """
        self.log("net_degraded", error=str(exc)[:160],
                 note="程式保持執行;網路回來自動重登、對帳、續管")
        if self.board:
            self.board.update(self.code, "⏸ 網路中斷——掛單保留,恢復後續管")
        self.sleep(min(self.round_sec or 10.0, 15.0))
        if not self.live:
            return
        try:
            self._sync_live_fills()  # 內含等網路恢復 + 絕對對帳
            if self.working is None:
                self._adopt_working(self.broker.get_order_results(),
                                    note="resync_working_order")
        except Exception as exc2:  # noqa: BLE001
            if not is_transient_network_error(exc2):
                raise
            # 網路還沒回來:下一輪再對,狀態不動

    def _place(self, price: float, qty: int) -> None:
        req = StockOrderRequest(
            symbol=self.code, side=self.side, quantity=qty,
            price_type="Limit", market_type=self.market_type,
            time_in_force="ROD", order_type="Stock",
            price=f"{price:g}", user_def="QLEXEC",
        )
        if not self.live:
            self.working = {"price": price, "qty": qty, "seq_no": None, "seen_fill": 0}
            self.log("paper_place", price=price, qty=qty, market_type=self.market_type)
            if self.board:
                self.board.update(self.code, f"掛單 {qty} 股 @ {price:g}")
            return
        res = self.broker.place_stock_order(req)
        if not getattr(res, "is_success", False):
            raise RuntimeError(f"下單失敗:{getattr(res, 'message', res)}")
        seq = getattr(getattr(res, "data", None), "seq_no", None)
        self.working = {"price": price, "qty": qty, "seq_no": seq, "seen_fill": 0}
        self.log("place", price=price, qty=qty, seq_no=str(seq), market_type=self.market_type)
        if self.board:
            self.board.update(self.code, f"掛單 {qty} 股 @ {price:g}")

    def _cancel_working(self) -> None:
        """同步成交 → 撤單 → 再同步一次(撮合競態),然後清空 working。"""
        if self.working is None:
            return
        if not self.live:
            self.log("paper_cancel", price=self.working["price"])
            self.working = None
            return
        self._sync_live_fills()
        if self.working is None:  # 同步後發現已全成
            return
        cur = self._find_order(self.working["seq_no"])
        if cur is not None and getattr(cur, "status", None) not in _TERMINAL_STATUSES:
            res = self.broker.cancel_order(cur)  # 斷網 → broker 層等網路回來重登再撤
            self.log("cancel", seq_no=str(self.working["seq_no"]),
                     ok=bool(getattr(res, "is_success", False)))
            time.sleep(0.5)
        self._sync_live_fills()
        self.working = None


    def _reprice(self, new_price: float, remaining: int) -> None:
        """整股走 modify_price(保留委託);盤中零股依交易所規則不得改價 → 刪單重掛。"""
        if self.working is None:
            self._place(new_price, remaining)
            return
        if not self.live or self.market_type != "Common":
            self._cancel_working()
            remaining = self.qty - self.result.filled_qty
            if remaining > 0:
                self._place(new_price, remaining)
            return
        self._sync_live_fills()
        if self.working is None:
            remaining = self.qty - self.result.filled_qty
            if remaining > 0:
                self._place(new_price, remaining)
            return
        cur = self._find_order(self.working["seq_no"])
        try:
            res = self.broker.modify_price(cur, f"{new_price:g}")
            if getattr(res, "is_success", False):
                self.working["price"] = new_price
                self.log("modify_price", price=new_price, seq_no=str(self.working["seq_no"]))
                return
            self.log("modify_price_failed", message=str(getattr(res, "message", res)))
        except Exception as exc:  # noqa: BLE001 - 改價失敗一律退回刪單重掛
            self.log("modify_price_error", error=str(exc)[:200])
        self._cancel_working()
        remaining = self.qty - self.result.filled_qty
        if remaining > 0:
            self._place(new_price, remaining)


    def _afterhours_completion(self, stop: dict) -> None:
        """收盤(13:30)未竟 → 盤後定價交易自動掛當日收盤價完成。

        規則:盤後 14:30 一次集合競價、成交價=當日收盤價(零股 13:40 起收單、
        整股定價 14:00 起收單)。**護欄仍是鐵律**:收盤價破護欄就不掛(那正是
        盤中沒完成的原因),留待明日出場門重評。盤後量不足按隨機順序分配,
        不保證中籤——未中籤如實記錄。
        """
        remaining = self.qty - self.result.filled_qty
        if remaining <= 0 or stop["flag"] or HALT_FILE.exists():
            return
        if _hhmm(self.clock()) > AFTERHOURS_LAST:
            return
        window_open, mtype = (
            (AFTERHOURS_ODD_OPEN, "Odd") if self.market_type == "IntradayOdd"
            else (AFTERHOURS_FIXING_OPEN, "Fixing")
        )
        close_px = 0.0
        try:
            res = self.broker.sdk.marketdata.rest_client.stock.intraday.quote(symbol=self.code)
            d = res if isinstance(res, dict) else {}
            close_px = float(d.get("closePrice") or d.get("lastPrice") or 0)
        except Exception:
            pass
        if close_px <= 0:
            q = self.feed.snapshot()
            close_px = q.last or 0.0
        if close_px <= 0:
            self.log("afterhours_skip", reason="no_close_price")
            return
        from dataclasses import replace as _replace
        collar = price_collar(self.side, self.result.arrival,
                              _replace(self.profile, cap_pct=self.cap_pct_eff))
        within = close_px <= collar if self.side == "Buy" else close_px >= collar
        if not within:
            self.log("afterhours_skip", reason="close_breaches_collar",
                     close=close_px, collar=collar)
            return
        while _hhmm(self.clock()) < window_open:  # 等收單窗開
            if stop["flag"] or HALT_FILE.exists():
                return
            self.sleep(5.0)
        self.market_type = mtype
        self.log("afterhours_place", market_type=mtype, price=close_px, qty=remaining)
        if self.board:
            self.board.update(self.code, f"盤後掛收盤價 {close_px:g} × {remaining} 股(14:30 撮合)")
        if not self.live:
            self._register_fill(remaining, close_px)  # dry-run:視同 14:30 以收盤價成交
            return
        self._place(close_px, remaining)
        while _hhmm(self.clock()) < AFTERHOURS_MATCH_DONE:  # 等 14:30 撮合回報
            if stop["flag"]:
                return
            self.wake.wait(timeout=15.0)
            self.wake.clear()
            self._sync_live_fills()
            if self.result.filled_qty >= self.qty:
                return
        self._sync_live_fills()
        if self.result.filled_qty < self.qty:
            self.log("afterhours_unfilled", note="盤後未中籤/量不足,留待明日規則重評")

    def _refresh_bars(self) -> None:
        """每 60s 抓 1 分 K 更新 TPO/SMC 結構(失敗靜默;selftest 無 SDK 不會進來)。"""
        if self.micro is None or time.time() - self._bars_refreshed < 60.0:
            return
        self._bars_refreshed = time.time()
        try:
            res = self.broker.sdk.marketdata.rest_client.stock.intraday.candles(symbol=self.code)
            bars = res.get("data") if isinstance(res, dict) else getattr(res, "data", None)
            if bars:
                self._last_bars = list(bars)
                self.micro.on_bars(self._last_bars)
                if self.cap_auto and self.micro.atr1m_pct > 0:
                    new_cap = min(max(8.0 * self.micro.atr1m_pct, 0.004), 0.02)
                    if abs(new_cap - self.cap_pct_eff) > 1e-6:
                        self.cap_pct_eff = new_cap
                        self.log("cap_auto", cap_pct=round(new_cap, 4),
                                 atr1m_pct=round(self.micro.atr1m_pct, 5))
        except Exception:
            pass

    def _paper_fill_check(self) -> None:
        """紙上成交:限價跨到對側 → 以對側價全額成交(集合競價近似)。"""
        if self.working is None or self.live:
            return
        q = self.feed.snapshot()
        p = self.working["price"]
        crossed = (self.side == "Buy" and q.ask > 0 and p >= q.ask) or \
                  (self.side == "Sell" and q.bid > 0 and p <= q.bid)
        if crossed:
            fill_px = q.ask if self.side == "Buy" else q.bid
            self._register_fill(self.working["qty"], fill_px)
            self.working = None

    def _ratchet_limit(self, desired: float, aggressive: bool) -> float:
        """結構錨定 profile 的順向棘輪:被動/錨定段的掛價只准往有利方向走
        (賣不低於歷史最佳賣價、買不高於歷史最佳買價)。

        為什麼需要(2026-07-20 台光電賣單「越改越低」事故):賣單錨定「現價
        上方最近阻力」,而今日 VWAP/TPO 阻力會隨盤勢一路下滑,錨就跟著市場
        往下改價 = 盤中不斷賤賣,違背耐心單「賣掛相對高、買掛相對低,盤中永
        不因時間讓步,收盤未竟才由盤後定價以收盤價收尾」的合約。棘輪把這條
        合約寫死成不變式。`aggressive`(過死線/boost 的主動取價)不受限——
        由狙擊級訊號決定何時真正跨價,那才是允許往不利方向動的唯一時機。
        """
        if aggressive or not self.profile.structure_anchor or desired <= 0:
            return desired
        if self._rest_ratchet is not None:
            desired = (min(desired, self._rest_ratchet) if self.side == "Buy"
                       else max(desired, self._rest_ratchet))
        self._rest_ratchet = desired
        return desired

    # ── 主迴圈 ──
    def _takeover_existing(self) -> None:
        """接管語意(冪等):在途同向委託認領續管;今日同向成交計入進度。"""
        if not self.live:
            return
        orders = self.broker.get_order_results()
        total, money = self._today_totals(orders)
        # 進度基準:預設 0 → 今日既有成交直接計入進度(防重複買賣);
        # --allow-refill → 基準=現值,只算「從現在起」的新成交。
        if self.allow_refill:
            self._baseline_filled, self._baseline_money = total, money
        else:
            self._baseline_filled, self._baseline_money = 0, 0.0
            if total > 0:
                self.log("resume_from_today_fills", prior_filled=total,
                         source="order_results", remaining=max(self.qty - total, 0))
        self._adopt_working(orders, note="takeover_working_order")
        self._reconcile_progress(orders, note="啟動對帳")

    def run(self) -> LegResult:
        self._guards()
        # 啟動對帳也必須撐過斷網(2026-07-15):等網路回來再開工,絕不因為
        # 「啟動當下沒網路」就放棄整條腿。契約寫在這裡,不依賴 broker 內部重試。
        net_retry(self._takeover_existing, what=f"[{self.code}] 啟動對帳",
                  should_give_up=lambda: (HALT_FILE.exists()
                                          or _hhmm(self.clock()) > AFTERHOURS_LAST))
        if self.result.filled_qty >= self.qty:
            self.log("already_complete_today", filled=self.result.filled_qty, target=self.qty)
            if self.board:
                self.board.update(self.code, f"✅ 今日已完成 {self.result.filled_qty}/{self.qty}(接續進度)")
            if self.working is not None:
                self._cancel_working()
            self.log("summary", filled=self.result.filled_qty, target=self.qty,
                     avg_price=round(self.result.avg_price, 4), arrival=0.0,
                     shortfall_bps=None, rounds=0, live=self.live)
            self._release_lock()
            return self.result
        # 等第一筆報價:開盤初期尚未撮合、或主機沒網路,都會沒有報價。
        # **絕不放棄**(2026-07-15 事故:斷網讓整天的機會消失)——等到有報價
        # 為止;只有 kill switch 或已過盤後撮合時刻才收工。
        q = self.feed.snapshot()
        waited = 0
        while not q.fresh():
            if HALT_FILE.exists() or _hhmm(self.clock()) > AFTERHOURS_LAST:
                self.log("no_quote_giving_up", waited_sec=waited,
                         note="等到收工仍無報價(網路未恢復?)")
                if self.board:
                    self.board.update(self.code, "⏭ 等不到行情,今日未執行")
                self._release_lock()
                return self.result
            if waited % 60 == 0:
                self.log("waiting_for_quote", waited_sec=waited,
                         note="尚未開盤或網路中斷;程式保持執行,有報價即開工")
                if self.board:
                    self.board.update(self.code, f"⏳ 等待行情 {waited}s(未開盤/網路中斷)")
            self.feed.refresh_rest()  # 內含 try/except,斷網不拋
            self.sleep(10.0)
            waited += 10
            q = self.feed.snapshot()
        if waited:
            self.log("quote_ready", waited_sec=waited)
        self.result.arrival = q.last or (q.bid + q.ask) / 2.0
        from dataclasses import replace as _replace
        collar = price_collar(self.side, self.result.arrival,
                              _replace(self.profile, cap_pct=self.cap_pct_eff))
        if self.micro is not None:  # 暖機不分 dry-run/live(唯讀 REST)
            try:
                res = self.broker.sdk.marketdata.rest_client.stock.intraday.trades(
                    symbol=self.code, limit=500)
                rows = res.get("data") if isinstance(res, dict) else getattr(res, "data", None)
                if rows:
                    n = self.micro.warmup_trades(list(rows))
                    self.log("micro_warmup", replayed=n,
                             vpin=round(self.micro.vpin.current, 3))
            except Exception:
                pass
            # v3 跨日結構:日線支撐/阻力 + 昨日價值區 prior(fail-open,離線自測回空)
            levels = load_daily_levels(self.code, self.side)
            if levels:
                self.micro.set_daily_context(levels)
                self.log("daily_context",
                         levels=[[round(p, 2), lab] for p, lab in levels])
            pva = load_prior_value_area(self.code)
            if pva is not None:
                self.micro.set_prior_value_area(*pva)
                self.log("prior_value_area", val=round(pva[0], 2),
                         poc=round(pva[1], 2), vah=round(pva[2], 2))
        self.log("start", arrival=self.result.arrival, collar=collar, qty=self.qty,
                 live=self.live, profile=self.profile.name, round_sec=self.round_sec,
                 market_type=self.market_type, log=str(self.log_path))

        stop = {"flag": False, "hits": 0}

        def _sigint(_sig, _frame):
            stop["hits"] += 1
            stop["flag"] = True
            if stop["hits"] == 1:
                print("\n[Ctrl+C] 收到中止:本輪結束即撤單退出(再按一次 = 立刻強制)")
            else:
                raise KeyboardInterrupt  # 第二次:立刻中斷(含 sleep/網路呼叫)

        old_handler = signal.signal(signal.SIGINT, _sigint) if self.manage_sigint else None
        try:
            round_idx = 0
            while self.result.filled_qty < self.qty and not stop["flag"] and not (
                    self.stop_event is not None and self.stop_event.is_set()):
                try:
                    now = self.clock()
                    if _hhmm(now) > SESSION_END:
                        self.log("session_end_unfilled", remaining=self.qty - self.result.filled_qty)
                        break
                    if HALT_FILE.exists():
                        self.log("halt_detected")
                        break
                    past_deadline = (self.profile.deadline_hhmm is not None
                                     and _hhmm(now) >= self.profile.deadline_hhmm)
                    in_open_window = _hhmm(now) < f"09:{self.avoid_open_min:02d}"
                    q = self.feed.snapshot()
                    if not q.fresh():
                        # 行情過期(ws 靜默死亡/斷網):先 REST 兜底,仍舊就等——
                        # 拿舊價改價等於盲目下單(2026-07-15 審計);在途單保留,
                        # 它是護欄內的限價單,最壞情況是不成交。
                        self.feed.refresh_rest()
                        q = self.feed.snapshot()
                        if not q.fresh():
                            self.log("quote_stale", age_sec=round(time.time() - q.ts, 1),
                                     note="不以過期報價定價;掛單保留,等行情恢復")
                            if self.board:
                                self.board.update(self.code, "⏸ 行情過期,暫停改價(掛單保留)")
                            self.sleep(min(self.round_sec or 10.0, 15.0))
                            continue
                    effective_round = round_idx
                    boost = False
                    if self.micro is not None and not past_deadline:
                        self._refresh_bars()
                        ref = q.ask if self.side == "Buy" else q.bid
                        # price 模式(structure_anchor)的加速門檻預設就用狙擊級(全 AND):
                        # 錨定單放棄跨價的前提是「訊號才動」,訊號就必須嚴。
                        sig = self.micro.signal(ref or q.last,
                                                strict=self.trigger_strict or self.profile.structure_anchor)
                        if sig.hold:
                            effective_round = min(round_idx, max(self.profile.passive_rounds - 1, 0))
                            self.log("micro_hold", reasons=sig.reasons)
                        elif sig.accelerate:
                            boost = True
                            self.log("micro_accelerate", reasons=sig.reasons)
                        elif self.micro.sweep and not self.profile.structure_anchor:
                            # 掃蕩後回收 = SMC 快速通道,立即取價。但結構錨定
                            # (patient/exit)profile 的跨價前提是「狙擊級全 AND
                            # 訊號」(見上 strict=True),而 sweep 已是該訊號的一個
                            # bucket——不得再獨立觸發跨價,否則盤中冷啟動一偵測到
                            # 掃蕩就急著成交,違背「掛結構位撈低/撈高」的合約。
                            boost = True
                            self.log("micro_sweep_fastpath")
                    if in_open_window:
                        # 開盤前幾分鐘輪動噪音大:壓回被動、不跨價(死線/停損不受此限)
                        boost = False
                        effective_round = 0
                    eff_profile = self.profile if self.cap_pct_eff == self.profile.cap_pct else                     __import__("dataclasses").replace(self.profile, cap_pct=self.cap_pct_eff)
                    desired = target_price(self.side, eff_profile, effective_round,
                                           past_deadline or boost, q.bid, q.ask, self.result.arrival)
                    # v2 智慧被動:microprice/OBI 決定 join/improve/lurk(非結構錨定的被動段)
                    if (self.micro is not None and not (past_deadline or boost)
                            and not self.profile.structure_anchor
                            and effective_round < self.profile.passive_rounds):
                        ap = self.micro.adaptive_passive(q.bid, q.ask)
                        if ap is not None:
                            collar_px = price_collar(self.side, self.result.arrival, eff_profile)
                            lvl = min(ap[0], collar_px) if self.side == "Buy" else max(ap[0], collar_px)
                            if lvl > 0 and lvl != desired:
                                desired = lvl
                                if self.working is None or self.working.get("price") != desired:
                                    self.log("adaptive_passive", level=desired, basis=ap[1])
                    # 結構錨定(patient):被動段把單掛在 TPO/SMC 結構位,而非買一/賣一
                    if (self.profile.structure_anchor and self.micro is not None
                            and not (past_deadline or boost)):
                        mid_px = (q.bid + q.ask) / 2 if (q.bid > 0 and q.ask > 0) else (q.last or 0)
                        anchor = self.micro.anchor_level(mid_px) if mid_px > 0 else None
                        if anchor is not None:
                            from .ticks import snap_down as _sd, snap_up as _su
                            collar_px = price_collar(self.side, self.result.arrival, eff_profile)
                            if self.side == "Buy":
                                lvl = min(_sd(anchor[0]), collar_px, _sd(q.ask - 1e-9) if q.ask > 0 else _sd(anchor[0]))
                            else:
                                lvl = max(_su(anchor[0]), collar_px, _su(q.bid + 1e-9) if q.bid > 0 else _su(anchor[0]))
                            if lvl > 0 and lvl != desired:
                                desired = lvl
                                if self.working is None or self.working.get("price") != desired:
                                    self.log("structure_rest", level=desired, basis=anchor[1])
                    # 順向棘輪:被動/錨定段的掛價只准往有利方向走(賣不改低、買不
                    # 改高),把耐心單「撈相對低/相對高」寫成不變式。boost/過死線的
                    # 主動取價不受限,由訊號決定何時真正跨價。
                    desired = self._ratchet_limit(desired, past_deadline or boost)
                    remaining = self.qty - self.result.filled_qty

                    if self.working is None and remaining > 0:
                        self._place(desired, min(remaining, self.slice_qty))
                    elif self.working is not None and (
                            self.working["price"] != desired
                            or self.working["qty"] - self.working["seen_fill"] != remaining):
                        self._reprice(desired, min(remaining, self.slice_qty))

                    self.log("round", i=round_idx, bid=q.bid, ask=q.ask,
                             working=self.working["price"] if self.working else None,
                             past_deadline=past_deadline)

                    import random as _random
                    wait_s = self.round_sec * _random.uniform(0.85, 1.15) if self.round_sec else 0.0
                    if self.live and wait_s > 0:
                        # 事件驅動:成交推播即刻喚醒;沒有推播才等滿一輪(備援輪詢)
                        self.wake.wait(timeout=wait_s)
                        self.wake.clear()
                    else:
                        self.sleep(wait_s)
                    if stop["flag"] or (self.stop_event is not None and self.stop_event.is_set()):
                        stop["flag"] = stop["flag"] or True
                        break
                    round_idx += 1
                    self.result.rounds = round_idx
                    self._sync_live_fills()
                    self._paper_fill_check()
                except Exception as exc:  # noqa: BLE001
                    # 斷網/斷線不得終止本腿(2026-07-15 事故):記錄 → 短暫等待 →
                    # 下一輪重來(broker 層會等網路回來重登)。恢復後先對帳在途
                    # 委託再決定動作,避免「送單當下斷線」造成重複下單。
                    if not is_transient_network_error(exc):
                        raise
                    self._net_degraded(exc)
                    continue
            # 收盤未竟 → 盤後定價自動收尾(護欄內才出手;14:30 撮合=收盤價)
            self._afterhours_completion(stop)
        finally:
            if old_handler is not None:
                signal.signal(signal.SIGINT, old_handler)
            self.result.aborted = stop["flag"]
            if self.working is not None:
                try:
                    self._cancel_working()
                except Exception as exc:  # noqa: BLE001 - 清理失敗不得吃掉原始例外
                    self.log("cancel_on_exit_failed", error=str(exc)[:160],
                             note="收工撤單失敗(斷網?)——跑 cancel_all 確認殘留")
            self.log("summary",
                     filled=self.result.filled_qty, target=self.qty,
                     avg_price=round(self.result.avg_price, 4),
                     arrival=self.result.arrival,
                     shortfall_bps=self.result.shortfall_bps(),
                     rounds=self.result.rounds, live=self.live)
            if self._last_bars:  # 1 分 K 自建歷史(明日的「昨日價值區」來源)
                path = dump_candles(self.code, self._last_bars)
                if path is not None:
                    self.log("candles_dumped", path=str(path), bars=len(self._last_bars))
            self._release_lock()
        return self.result

    def _release_lock(self) -> None:
        """本腿結束即釋放執行鎖(2026-07-15:先前只留給下次啟動的 PID 活性檢查
        清理,同一程序內重跑同一腿會被自己的死鎖擋住,也留下滿地鎖檔)。"""
        if self._lock_file is None or not self._lock_file.exists():
            return
        try:
            owner = json.loads(self._lock_file.read_text(encoding="utf-8")).get("pid")
            if int(owner) == os.getpid():  # 只清自己的,不動別人的
                self._lock_file.unlink()
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            pass
