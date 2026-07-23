"""安全網 money-path 守護:防重複賣、只管自有單、先撤後掛、不憑空掛單。

命門(2026-07-21 對真帳戶實測後定案):
  1. 今日要動的標的**絕不掛**(否則賣出後條件單裸奔 → 賣掉不存在的部位)
  2. 水位必須**比策略自身 trail 35% 更寬** → 正常運作時日頻路徑先出場,不干擾
  3. 無庫存/無峰值**絕不憑空掛單**
  4. 同步只撤**自有台帳**的 guid → 絕不動使用者手動掛的條件單
  5. **先撤後掛**(同標的雙重武裝比數秒空窗危險得多)
  6. 掛單失敗**不得寫入台帳**(否則下次會去撤一個不存在的 guid,並漏掉重掛)

Run: uv run --project . python -m quantlib.trading.live.tests.test_safety_net
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from quantlib.trading.live import safety_net as sn
from quantlib.trading.live.safety_net import NetPlan, WIDE, plan_for


class _FakeBroker:
    """記錄呼叫順序的假 broker(驗證先撤後掛與失敗處理)。"""

    def __init__(self, *, fail_place: set[str] | None = None):
        self.calls: list[tuple] = []
        self.fail_place = fail_place or set()
        self._n = 0

    def cancel_condition_order(self, guid):
        self.calls.append(("cancel", guid))
        return type("R", (), {"is_success": True})()

    def place_condition_sell_stop(self, *, symbol, quantity, trigger_price,
                                  days=30, odd_lot=True):
        self.calls.append(("place", symbol, quantity, trigger_price))
        if symbol in self.fail_place:
            return type("R", (), {"is_success": False, "message": "模擬失敗"})()
        self._n += 1
        data = type("D", (), {"guid": f"g{self._n}"})()
        return type("R", (), {"is_success": True, "data": data})()

    def __init_broker_side__(self, rows):
        self._broker_side = rows

    def get_condition_orders(self):
        rows = getattr(self, "_broker_side", [])
        objs = [type("D", (), {"guid": g, "symbol": s, "status": st})()
                for g, s, st in rows]
        return type("R", (), {"data": objs})()


def _tmp_state(tmp: Path):
    sn.STATE = tmp / "safety_net.json"
    return sn.STATE


# ── 目標計算(純函式)────────────────────────────────────────────────────
def test_plan_skips_todays_actions() -> None:
    plans = plan_for({"2466": 3, "3704": 1, "2059": 2},
                     {"2466": 100.0, "3704": 50.0, "2059": 80.0},
                     skip={"3704", "2059"})
    assert [p.symbol for p in plans] == ["2466"]


def test_plan_level_is_wider_than_strategy_trail() -> None:
    p = plan_for({"2466": 1}, {"2466": 100.0}, set())[0]
    assert p.trigger == 50.0 and WIDE > 0.35
    assert p.trigger < 100.0 * (1 - 0.35), "安全網不得比策略 trail 更緊"


def test_plan_never_invents_orders() -> None:
    assert plan_for({"2466": 0}, {"2466": 100.0}, set()) == []   # 無庫存
    assert plan_for({"2466": 1}, {}, set()) == []                # 無峰值
    assert plan_for({"2466": 1}, {"2466": 0.0}, set()) == []     # 峰值不合法


# ── 同步(台帳 + 順序 + 失敗處理)────────────────────────────────────────
def test_sync_cancels_before_placing() -> None:
    """先撤後掛:雙重武裝會賣掉不存在的部位,比數秒空窗危險得多。"""
    with tempfile.TemporaryDirectory() as td:
        _tmp_state(Path(td))
        sn.save_ledger({"2466": {"guid": "old-1", "quantity": 1, "trigger": 40.0}})
        b = _FakeBroker()
        sn.sync(b, {"2466": 1}, {"2466": 120.0}, set())
        kinds = [c[0] for c in b.calls]
        assert kinds == ["cancel", "place"], f"順序錯誤:{b.calls}"
        assert b.calls[0][1] == "old-1"
        assert b.calls[1][1:] == ("2466", 1, 60.0)


def test_sync_only_touches_own_ledger_guids() -> None:
    """使用者手動掛的單(不在台帳)絕不被撤。"""
    with tempfile.TemporaryDirectory() as td:
        _tmp_state(Path(td))
        sn.save_ledger({"2466": {"guid": "mine"}})
        b = _FakeBroker()
        sn.sync(b, {"2466": 1}, {"2466": 100.0}, set())
        assert [c[1] for c in b.calls if c[0] == "cancel"] == ["mine"]


def test_sync_failed_place_is_not_recorded() -> None:
    """掛單失敗不得進台帳(否則下次撤幽靈 guid 且漏掉重掛),且錯誤要回報。"""
    with tempfile.TemporaryDirectory() as td:
        st = _tmp_state(Path(td))
        b = _FakeBroker(fail_place={"2466"})
        res = sn.sync(b, {"2466": 1}, {"2466": 100.0}, set())
        assert res["errors"] and "2466" in res["errors"][0]
        assert res["placed"] == []
        assert json.loads(st.read_text()) == {}


def test_sync_records_guid_for_next_day_cancel() -> None:
    """成功掛單要記 guid,隔日才撤得掉(否則變孤兒單)。"""
    with tempfile.TemporaryDirectory() as td:
        st = _tmp_state(Path(td))
        b = _FakeBroker()
        sn.sync(b, {"2466": 2}, {"2466": 100.0}, set())
        led = json.loads(st.read_text())
        assert led["2466"]["guid"] == "g1"
        assert led["2466"]["quantity"] == 2 and led["2466"]["trigger"] == 50.0


def test_sync_disabled_by_env(monkeypatch=None) -> None:
    import os
    old = os.environ.get(sn._ENV_FLAG)
    os.environ[sn._ENV_FLAG] = "0"
    try:
        assert "skipped" in sn.sync(_FakeBroker(), {"2466": 1}, {"2466": 100.0}, set())
    finally:
        os.environ.pop(sn._ENV_FLAG, None)
        if old is not None:
            os.environ[sn._ENV_FLAG] = old


def test_sync_kills_cross_machine_duplicate_arm() -> None:
    """跨機器命門:別台機器(或台帳遺失前)掛在同標的的活躍單也必須先撤,
    否則同檔雙重武裝 → 兩張都觸發 = 賣掉不存在的部位。"""
    with tempfile.TemporaryDirectory() as td:
        _tmp_state(Path(td))
        sn.save_ledger({})                       # 本機台帳是空的(模擬 VM 首跑)
        b = _FakeBroker()
        b.__init_broker_side__([("other-machine", "2466", "預約(N)")])
        sn.sync(b, {"2466": 1}, {"2466": 100.0}, set())
        kinds = [c[0] for c in b.calls]
        assert kinds == ["cancel", "place"], f"未先撤他機殘留:{b.calls}"
        assert b.calls[0][1] == "other-machine"


def test_sync_ignores_terminal_broker_orders() -> None:
    """已刪除單不必再撤(實測查詢會連已刪除單一起回傳)。"""
    with tempfile.TemporaryDirectory() as td:
        _tmp_state(Path(td))
        sn.save_ledger({})
        b = _FakeBroker()
        b.__init_broker_side__([("dead", "2466", "條件單已刪除(C)")])
        sn.sync(b, {"2466": 1}, {"2466": 100.0}, set())
        assert [c[0] for c in b.calls] == ["place"]


def test_sync_never_double_arms_when_cancel_fails() -> None:
    """**最嚴重的 money-path bug 守護**:舊單撤不掉時絕不可再掛同檔——
    兩張都武裝會在觸發時賣掉超過持有量。寧可維持舊單,也不可雙重武裝。"""
    class _CancelFails(_FakeBroker):
        def cancel_condition_order(self, guid):
            self.calls.append(("cancel", guid))
            raise RuntimeError("模擬撤單失敗")

    with tempfile.TemporaryDirectory() as td:
        _tmp_state(Path(td))
        sn.save_ledger({"2466": {"guid": "old-1", "quantity": 1, "trigger": 40.0}})
        b = _CancelFails()
        res = sn.sync(b, {"2466": 1}, {"2466": 120.0}, set())
        assert not any(c[0] == "place" for c in b.calls), f"撤失敗卻仍掛單:{b.calls}"
        assert res["placed"] == []
        assert any("雙重武裝" in e for e in res["errors"]), res["errors"]


def test_terminal_status_detection() -> None:
    """已刪除單不必再撤(實測 get_condition_order 會回傳已刪除單)。"""
    assert sn._is_terminal("條件單已刪除(C)")
    assert not sn._is_terminal("預約(N)")


def main() -> None:
    for fn in (test_plan_skips_todays_actions,
               test_plan_level_is_wider_than_strategy_trail,
               test_plan_never_invents_orders,
               test_sync_cancels_before_placing,
               test_sync_only_touches_own_ledger_guids,
               test_sync_failed_place_is_not_recorded,
               test_sync_records_guid_for_next_day_cancel,
               test_sync_kills_cross_machine_duplicate_arm,
               test_sync_never_double_arms_when_cancel_fails,
               test_sync_ignores_terminal_broker_orders,
               test_sync_disabled_by_env,
               test_terminal_status_detection):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ safety_net 全過")


if __name__ == "__main__":
    main()
