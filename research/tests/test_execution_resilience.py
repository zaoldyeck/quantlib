"""盤中執行器的網路韌性與對帳正確性(離線,不連 SDK)。

守護兩條真金白銀的性質:
1. **斷網不得終止**(2026-07-15 事故:開盤瞬間無網路 → FubonSDK() 拋
   IO error → 執行器整個退出、錯過整個交易日)。
2. **斷網不得重複下單**——「送單當下斷線」時委託可能已到交易所而我們收不到
   回應;若恢復後不先對帳就重掛,就是重複買賣。進度改以「今日累計成交 −
   基準」絕對重算(冪等、自癒),取代脆弱的增量記帳。

run:  uv run --project research python -m pytest research/tests/test_execution_resilience.py -q
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from research.brokers.fubon import (  # noqa: E402
    is_transient_network_error, net_retry,
)
from research.trading.execution.engine import ExecutionEngine, Quote  # noqa: E402
from research.trading.execution.policy import PROFILES  # noqa: E402

NET_DOWN = ValueError(
    "IO error: failed to lookup address information: nodename nor servname provided, or not known")


# ── 假件 ────────────────────────────────────────────────────────────
class FakeOrder:
    def __init__(self, seq_no: str, stock_no: str, buy_sell: str, price: float,
                 quantity: int, filled_qty: int = 0, status: int = 10,
                 filled_money: float | None = None):
        self.seq_no = seq_no
        self.stock_no = stock_no
        self.buy_sell = buy_sell
        self.price = price
        self.quantity = quantity
        self.filled_qty = filled_qty
        self.status = status
        self.filled_money = filled_money if filled_money is not None else filled_qty * price


class FakeRes:
    def __init__(self, data: Any, is_success: bool = True, message: str | None = None):
        self.data = data
        self.is_success = is_success
        self.message = message


class FakeBroker:
    """可程式化的券商:orders = 交易所端的真相;down = 主機沒網路。"""

    def __init__(self) -> None:
        self.orders: list[FakeOrder] = []
        self.down = False
        self._fail_left = 0
        self.place_calls: list[dict] = []
        self.place_reaches_exchange = True  # 送單「有沒有真的到交易所」
        self.sdk = None
        self.account = None
        self._seq = 0

    def fail_next(self, n: int) -> None:
        """接下來 n 次網路呼叫斷網,之後自動恢復(模擬暫時性斷線)。"""
        self._fail_left = n
        self.down = True

    def _guard(self) -> None:
        if self._fail_left > 0:
            self._fail_left -= 1
            if self._fail_left == 0:
                self.down = False
            raise NET_DOWN
        if self.down:
            raise NET_DOWN

    def get_order_results(self) -> FakeRes:
        self._guard()
        return FakeRes(list(self.orders))

    def place_stock_order(self, req: Any) -> FakeRes:
        self.place_calls.append({"symbol": req.symbol, "qty": req.quantity, "price": req.price})
        if self.down:
            # 模擬「送出後才斷線」:委託其實已到交易所,但呼叫端收不到回應
            if self.place_reaches_exchange:
                self._seq += 1
                self.orders.append(FakeOrder(f"S{self._seq}", req.symbol, req.side,
                                             float(req.price), req.quantity))
            raise NET_DOWN
        self._seq += 1
        o = FakeOrder(f"S{self._seq}", req.symbol, req.side, float(req.price), req.quantity)
        self.orders.append(o)
        return FakeRes(o)

    def cancel_order(self, order: Any) -> FakeRes:
        self._guard()
        order.status = 30
        return FakeRes(order)

    def modify_price(self, order: Any, price: str) -> FakeRes:
        self._guard()
        order.price = float(price)
        return FakeRes(order)

    # 交易所端動作(測試用)
    def fill(self, seq_no: str, qty: int, price: float) -> None:
        for o in self.orders:
            if o.seq_no == seq_no:
                o.filled_qty += qty
                o.filled_money = o.filled_qty * price
                o.status = 50 if o.filled_qty >= o.quantity else 10


class FakeFeed:
    def __init__(self, bid: float = 100.0, ask: float = 100.5, last: float = 100.2):
        self.q = Quote(bid=bid, ask=ask, last=last, ts=time.time())
        self.stale = False

    def snapshot(self) -> Quote:
        ts = 1.0 if self.stale else time.time()  # stale=True 模擬 ws 靜默死亡
        return Quote(self.q.bid, self.q.ask, self.q.last, ts)

    def refresh_rest(self) -> None:
        pass


def mk_engine(broker: FakeBroker, *, side: str = "Buy", qty: int = 1, live: bool = True,
              allow_refill: bool = False, code: str = "2408",
              tmp_path: Path | None = None) -> ExecutionEngine:
    eng = ExecutionEngine(
        broker, code=code, side=side, qty=qty, profile=PROFILES["buy_patient"],
        round_sec=0, live=live, feed=FakeFeed(),
        clock=lambda: datetime(2026, 7, 15, 10, 0),
        sleep=lambda s: None,
        log_path=(tmp_path / "t.jsonl") if tmp_path else Path("/tmp/qlexec_test.jsonl"),
        micro=None, allow_refill=allow_refill, manage_sigint=False,
    )
    eng.events: list[tuple[str, dict]] = []
    _orig = eng.log

    def _log(event: str, **kw: Any) -> None:
        eng.events.append((event, kw))
        _orig(event, **kw)
    eng.log = _log
    return eng


def events_of(eng: ExecutionEngine, name: str) -> list[dict]:
    return [kw for ev, kw in eng.events if ev == name]


# ── 分類器 ──────────────────────────────────────────────────────────
@pytest.mark.parametrize("exc", [
    NET_DOWN,
    OSError("[Errno 51] Network is unreachable"),
    TimeoutError("timed out"),
    ConnectionResetError("Connection reset by peer"),
    RuntimeError("Fubon login failed: IO error: connection refused"),
])
def test_transient_errors_are_retryable(exc):
    assert is_transient_network_error(exc)


@pytest.mark.parametrize("exc", [
    ValueError("FUBON_API_KEY is required for apikey login."),
    RuntimeError("Fubon login failed: 密碼錯誤"),
    RuntimeError("Fubon login failed: 無簽署完成API使用風險暨聲明書帳號"),
    KeyError("boom"),
])
def test_terminal_errors_fail_fast(exc):
    assert not is_transient_network_error(exc)


def test_net_retry_recovers_then_returns():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise NET_DOWN
        return "ok"

    assert net_retry(flaky, what="t", first_delay=0.001, max_delay=0.001) == "ok"
    assert calls["n"] == 3


def test_net_retry_does_not_retry_terminal():
    def bad():
        raise RuntimeError("Fubon login failed: 憑證已過期")

    with pytest.raises(RuntimeError):
        net_retry(bad, what="t", first_delay=0.001)


# ── 對帳:進度 = 今日累計成交 − 基準 ────────────────────────────────
def test_startup_counts_today_fills_and_skips_when_done(tmp_path):
    b = FakeBroker()
    b.orders.append(FakeOrder("S0", "1101", "BSAction.Buy", 420.0, 1, filled_qty=1, status=50))
    eng = mk_engine(b, code="1101", tmp_path=tmp_path)
    res = eng.run()
    assert res.filled_qty == 1
    assert b.place_calls == [], "今日已成交,不該再下單"
    assert events_of(eng, "already_complete_today")


def test_allow_refill_sets_baseline_and_trades_again(tmp_path):
    b = FakeBroker()
    b.orders.append(FakeOrder("S0", "2408", "BSAction.Buy", 420.0, 1, filled_qty=1, status=50))
    eng = mk_engine(b, allow_refill=True, tmp_path=tmp_path)
    eng._takeover_existing()
    assert eng._baseline_filled == 1
    assert eng.result.filled_qty == 0, "allow-refill 只算從現在起的新成交"


def test_reconcile_is_idempotent(tmp_path):
    b = FakeBroker()
    b.orders.append(FakeOrder("S0", "2408", "BSAction.Buy", 420.0, 2, filled_qty=2,
                              status=50, filled_money=841.0))
    eng = mk_engine(b, qty=2, tmp_path=tmp_path)
    eng._takeover_existing()
    first = eng.result.filled_qty
    for _ in range(5):  # 重複對帳不得累加
        eng._reconcile_progress(b.get_order_results(), note="t")
    assert eng.result.filled_qty == first == 2
    assert eng.result.avg_price == pytest.approx(420.5), "均價取 filled_money 的真實成交價"


# ── 斷網情境 ────────────────────────────────────────────────────────
def test_place_during_outage_then_fill_is_counted_once_no_duplicate(tmp_path):
    """送單當下斷線 → 委託其實已到交易所並成交 → 恢復後只算一次、不重掛。

    這是舊增量記帳(seen_fill)會漏帳並重複下單的致命情境。
    """
    b = FakeBroker()
    eng = mk_engine(b, tmp_path=tmp_path)
    eng._takeover_existing()

    b.down = True
    with pytest.raises(ValueError):
        eng._place(100.5, 1)          # 送出後才斷線:交易所收到了,我們沒收到回應
    assert eng.working is None, "沒有 seq_no,程式並不知道自己有單"
    assert len(b.orders) == 1

    b.fill(b.orders[0].seq_no, 1, 100.5)   # 斷網期間成交
    b.down = False                          # 網路恢復
    eng._net_degraded(NET_DOWN)

    assert eng.result.filled_qty == 1, "斷網期間的不知情成交必須被對回來"
    assert len(b.place_calls) == 1, "恢復後不得重掛(否則就是重複下單)"


def test_recovery_adopts_unknown_working_order(tmp_path):
    """送單當下斷線 → 委託在途未成交 → 恢復後認領,不重掛。"""
    b = FakeBroker()
    eng = mk_engine(b, tmp_path=tmp_path)
    eng._takeover_existing()

    b.down = True
    with pytest.raises(ValueError):
        eng._place(100.5, 1)
    b.down = False
    eng._net_degraded(NET_DOWN)

    assert eng.working is not None, "在途單必須被認領"
    assert str(eng.working["seq_no"]) == "S1"
    assert events_of(eng, "resync_working_order")
    assert len(b.place_calls) == 1


def test_known_order_filled_during_outage_counted_once(tmp_path):
    b = FakeBroker()
    eng = mk_engine(b, tmp_path=tmp_path)
    eng._takeover_existing()
    eng._place(100.5, 1)                    # 正常掛出,拿到 seq_no
    assert eng.working is not None

    b.down = True                            # 斷網
    b.fill("S1", 1, 100.5)                   # 斷網期間成交
    b.down = False                           # 恢復
    eng._net_degraded(NET_DOWN)

    assert eng.result.filled_qty == 1
    assert eng.working is None, "終態委託必須清空"


def test_startup_outage_waits_then_completes(monkeypatch, tmp_path):
    """啟動當下沒網路(今早的事故情境)→ 等到網路回來才開工,絕不退出。"""
    monkeypatch.setattr("research.brokers.fubon.time.sleep", lambda s: None)
    b = FakeBroker()
    b.fail_next(3)  # 啟動對帳連續 3 次斷網後恢復
    eng = mk_engine(b, code="1102", tmp_path=tmp_path)
    orig_sleep = eng.sleep

    def sleep_then_fill(sec: float) -> None:
        if b.orders and not b.down:
            b.fill(b.orders[0].seq_no, 1, 100.5)
        orig_sleep(sec)

    eng.sleep = sleep_then_fill
    res = eng.run()
    assert res.filled_qty == 1, "網路回來後必須照常完成"
    assert not res.aborted
    assert len(b.place_calls) == 1


def test_mid_run_outage_does_not_kill_the_leg(tmp_path):
    """盤中斷網數輪 → 腿還活著(掛單保留);網路回來後對帳並完成。"""
    b = FakeBroker()
    eng = mk_engine(b, code="1104", tmp_path=tmp_path)
    state = {"n": 0}

    def sleep_hook(sec: float) -> None:
        state["n"] += 1
        if state["n"] == 1:
            b.down = True                      # 掛出後斷網
        elif state["n"] >= 4:
            b.down = False                     # 幾輪後恢復
            if b.orders:
                b.fill(b.orders[0].seq_no, 1, 100.5)

    eng.sleep = sleep_hook
    res = eng.run()
    assert res.filled_qty == 1, "網路回來後必須完成"
    assert not res.aborted
    assert events_of(eng, "net_degraded"), "斷網要留下 TCA 軌跡"
    assert len(b.place_calls) == 1, "斷網期間不得重複下單"


def test_terminal_error_still_propagates(tmp_path):
    """非網路的真錯誤不得被韌性層吞掉(否則問題會無聲累積)。"""
    b = FakeBroker()
    eng = mk_engine(b, code="1103", tmp_path=tmp_path)

    def boom() -> None:
        raise KeyError("unexpected schema")

    b.get_order_results = boom  # type: ignore[method-assign]
    with pytest.raises(KeyError):
        eng.run()


def test_dry_run_unaffected_by_absolute_reconcile(tmp_path):
    """dry-run 沒有券商帳:紙上成交照常,不被絕對對帳歸零。"""
    b = FakeBroker()
    eng = mk_engine(b, live=False, tmp_path=tmp_path)
    eng._register_fill(1, 100.2)
    eng._reconcile_progress(b.get_order_results(), note="t")
    assert eng.result.filled_qty == 1


# ── 報價新鮮度:ws 靜默死亡時不得拿舊價交易 ──────────────────────────
def test_quote_freshness_gates_usability():
    assert Quote(bid=1.0, ask=2.0, last=1.5, ts=time.time()).fresh()
    assert not Quote(bid=1.0, ask=2.0, last=1.5, ts=time.time() - 3600).fresh(), "一小時前的報價不可用"
    assert not Quote(ts=time.time()).fresh(), "沒有價格就不可用"


def test_stale_quote_never_places_orders(tmp_path):
    """行情停擺(ws 靜默死亡/斷網)→ 絕不掛單,等到收工如實記錄。"""
    from datetime import timedelta

    b = FakeBroker()
    eng = mk_engine(b, code="1105", tmp_path=tmp_path)
    eng.feed.stale = True
    state = {"t": datetime(2026, 7, 15, 10, 0)}
    eng.clock = lambda: state["t"]
    eng.sleep = lambda sec: state.__setitem__("t", state["t"] + timedelta(minutes=30))

    res = eng.run()
    assert res.filled_qty == 0
    assert b.place_calls == [], "報價過期不得下單(拿舊價=盲目下單)"
    assert events_of(eng, "no_quote_giving_up"), "收工要如實記錄等不到行情"


# ── 送單語意:絕不自動重送(重複下單防線) ──────────────────────────
def test_place_order_never_auto_resends_on_network_error():
    import types

    from research.brokers.fubon import FubonBroker

    broker = FubonBroker(credentials=object(), dry_run=False)
    calls: list[Any] = []

    def place(account: Any, order: Any) -> Any:
        calls.append(order)
        raise NET_DOWN

    broker.sdk = types.SimpleNamespace(stock=types.SimpleNamespace(place_order=place))
    broker.account = object()
    with pytest.raises(ValueError):
        broker._call_with_relogin(
            lambda: broker.sdk.stock.place_order(broker.account, "o"), what="送單", retry=False)
    assert len(calls) == 1, "送單斷線時絕不可自動重送——委託可能已到交易所"


def test_readonly_query_retries_until_network_returns(monkeypatch):
    import types

    from research.brokers.fubon import FubonBroker

    monkeypatch.setattr("research.brokers.fubon.time.sleep", lambda s: None)
    broker = FubonBroker(credentials=object(), dry_run=True)
    n = {"calls": 0}

    def flaky(account: Any) -> Any:
        n["calls"] += 1
        if n["calls"] < 3:
            raise NET_DOWN
        return FakeRes([])

    broker.sdk = types.SimpleNamespace(
        stock=types.SimpleNamespace(get_order_results=flaky))
    broker.account = object()
    res = broker.get_order_results()
    assert res.is_success and n["calls"] == 3, "唯讀查詢重試安全,應等到網路回來"


def test_concurrent_relogin_is_serialized(monkeypatch):
    """多腿併發:同時撞到斷線只該換一次 session(否則互相抽換 sdk → 幽靈狀態)。"""
    import threading

    from research.brokers.fubon import FubonBroker

    broker = FubonBroker(credentials=object(), dry_run=True)
    n = {"logins": 0}

    def fake_login_once(method: Any = None) -> Any:
        n["logins"] += 1
        time.sleep(0.05)
        broker.account = "ACC"
        return broker.account

    monkeypatch.setattr(broker, "_login_once", fake_login_once)
    threads = [threading.Thread(target=broker.login) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert n["logins"] == 1, "5 條腿同時重登只該建立一個新 session"
