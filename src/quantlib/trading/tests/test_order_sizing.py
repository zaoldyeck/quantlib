"""費率感知下單切分的守護(money-path:算錯就是真的多付錢)。

Run: uv run --project . python -m pytest src/quantlib/trading/tests/test_order_sizing.py
"""
from __future__ import annotations

from quantlib.execsim.broker_fee import FubonFeeSchedule
from quantlib.trading.order_sizing import (
    ODD_LOT_MAX,
    Ticket,
    fee_for,
    min_efficient_notional,
    min_efficient_slice,
    natural_split,
    plan_tickets,
    ticket_warning,
    total_fee,
)

_F = FubonFeeSchedule()


def test_minimum_differs_between_odd_lot_and_board_lot() -> None:
    """零股 1 元 / 整股 20 元——這條錯了,下面全部的結論都跟著錯。"""
    assert _F.minimum_for(1) == 1.0
    assert _F.minimum_for(999) == 1.0
    assert _F.minimum_for(1000) == 20.0
    assert _F.minimum_for(5000) == 20.0


def test_min_efficient_notional_thresholds() -> None:
    """折扣完全生效的臨界 = 低消 / 折後費率(不得寫死,須由費率表算出)。"""
    assert round(min_efficient_notional(1)) == 3_899
    assert round(min_efficient_notional(1000)) == 77_973
    assert round(min_efficient_slice()) == 77_973


def test_fee_floor_binds_on_small_tickets() -> None:
    """小額單付的是低消,不是折後費率——實質費率會發散。"""
    assert fee_for(100 * 1, shares=1) == 1.0                  # 100 元 → 1 元 = 1%
    assert fee_for(3_899, shares=1) == 3_899 * _F.low_tier_rate()  # 剛好臨界
    assert fee_for(20_000, shares=1_000) == 20.0              # 整股低消咬住
    assert fee_for(20_000, shares=999) < 6.0                  # 同金額走零股便宜 3 倍以上


def test_odd_lot_strictly_cheaper_below_board_threshold() -> None:
    """同一筆金額,零股手續費 ≤ 整股,直到整股臨界才追平。"""
    for notional in (1_000, 20_000, 50_000):
        assert fee_for(notional, shares=999) <= fee_for(notional, shares=1_000)
    assert fee_for(77_973, shares=999) == fee_for(77_973, shares=1_000)


def test_natural_split_follows_exchange_rules() -> None:
    assert natural_split(999) == [(999, "IntradayOdd")]
    assert natural_split(1_000) == [(1_000, "Common")]
    assert natural_split(2_300) == [(2_000, "Common"), (300, "IntradayOdd")]


def test_plan_prefers_odd_lot_when_both_hurdles_clear() -> None:
    """1,000 股 @ 5 元:整股低消 20 元 vs 零股 1 元,省 19 元;
    放棄 1 股(5 元)、一檔價差 0.01×999=9.99 元 —— 兩道門都過 → 改零股。"""
    tickets, why = plan_tickets(1_000, 5.0)
    assert len(tickets) == 1
    assert tickets[0].market_type == "IntradayOdd"
    assert tickets[0].shares == ODD_LOT_MAX
    assert "純零股" in why


def test_plan_refuses_to_give_up_exposure_for_fee() -> None:
    """1,999 股 @ 5 元:縮到 999 股要放棄 1,000 股(5,000 元)去省 ~20 元 —— 擋下。
    這是本模組最重要的一條:絕不為了省手續費而少放錢進場。"""
    tickets, why = plan_tickets(1_999, 5.0)
    assert sum(t.shares for t in tickets) == 1_999
    assert "放棄" in why


def test_plan_keeps_board_lot_when_saving_cannot_cover_a_tick() -> None:
    """1,000 股 @ 20 元:省 14.9 元 > 放棄 20 元?否 → 先被曝險門擋;
    即使放行,一檔價差 0.05×999=49.95 元也蓋不過。整股維持。"""
    tickets, why = plan_tickets(1_000, 20.0)
    assert [t.market_type for t in tickets] == ["Common"]
    assert "自然拆" in why


def test_plan_never_downsizes_large_orders() -> None:
    """張數多時縮到 999 股等於放棄大半部位,絕不可為省手續費這樣做。"""
    tickets, _ = plan_tickets(10_000, 10.0)
    assert sum(t.shares for t in tickets) == 10_000
    assert total_fee(tickets) > 0


def test_sub_lot_order_passes_through_unchanged() -> None:
    tickets, why = plan_tickets(300, 120.0)
    assert tickets == [Ticket(300, "IntradayOdd", 120.0)]
    assert "1 元下限" in why


def test_warning_fires_below_threshold_and_reports_multiple() -> None:
    """1 股營運的現況:警告必須講出「實質費率是折後的幾倍」。"""
    w = ticket_warning(Ticket(1, "IntradayOdd", 100.0))
    assert w is not None and "39 倍" in w
    assert ticket_warning(Ticket(40, "IntradayOdd", 100.0)) is None   # 4,000 元 → 過臨界


# ── 切片閘門(engine._effective_slice)────────────────────────────────────────

class _FakeLeg:
    """只帶 _effective_slice 需要的欄位,避免為了測一個純判斷去啟動整台引擎。"""

    from quantlib.trading.execution.engine import ExecutionEngine as _E
    _effective_slice = _E._effective_slice

    def __init__(self, qty: int, slice_qty: int | None):
        self.qty = qty
        self._auto_slice = slice_qty is None
        self.slice_qty = slice_qty or (1000 if qty >= 2000 else qty)
        self.logged: list[tuple] = []

    def log(self, *a, **k) -> None:
        self.logged.append((a, k))


def test_auto_slice_skipped_when_each_child_below_fee_threshold() -> None:
    """5 張 @ 20 元(10 萬元):切成 1 張/片 = 2 萬 < 77,973 → 不切,否則多付 4 次低消。"""
    leg = _FakeLeg(qty=5_000, slice_qty=None)
    assert leg._effective_slice(20.0) == 5_000
    assert leg.logged and leg.logged[0][1]["note"].startswith("每片金額低於折扣臨界")


def test_auto_slice_kept_when_children_clear_threshold() -> None:
    """5 張 @ 200 元(100 萬元):每片 20 萬 > 臨界 → 照切,衝擊考量成立。"""
    leg = _FakeLeg(qty=5_000, slice_qty=None)
    assert leg._effective_slice(200.0) == 1_000
    assert not leg.logged


def test_explicit_slice_qty_is_never_overridden() -> None:
    """使用者顯式 --slice-qty 是明確意圖,費率閘門不得推翻。"""
    leg = _FakeLeg(qty=5_000, slice_qty=500)
    assert leg._effective_slice(20.0) == 500


def test_no_slicing_when_order_fits_in_one_child() -> None:
    leg = _FakeLeg(qty=300, slice_qty=None)
    assert leg._effective_slice(120.0) == 300


# ── 收盤保底豁免護欄(2026-07-26 使用者裁決)──────────────────────────────────

def test_collar_exempts_close_is_default_and_cli_can_restore_old_behaviour() -> None:
    """預設收盤保底不受護欄限制;--collar-blocks-close 才回到舊的「破欄不掛」。

    理由(strat_lab/collar_fill_risk.py 實測):護欄 0.5% 時,買 39.0% / 賣 46.1%
    的日子連收盤都掛不出去,而「沒成交」是 12 年全史證明的最大損失來源。
    """
    from quantlib.trading.execution._cli import build_parser

    p = build_parser("Sell")
    assert p.parse_args(["--code", "2330"]).collar_blocks_close is False
    assert p.parse_args(["--code", "2330", "--collar-blocks-close"]).collar_blocks_close is True


def test_engine_defaults_to_exempting_close_from_collar() -> None:
    """引擎建構參數的預設值本身就要是豁免——CLI 沒接上時也不能悄悄退回舊行為。"""
    import inspect

    from quantlib.trading.execution.engine import ExecutionEngine

    sig = inspect.signature(ExecutionEngine.__init__)
    assert sig.parameters["collar_exempts_close"].default is True
