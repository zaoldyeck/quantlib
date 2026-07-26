"""execute 過量下單守門的 money-path 測試(使用者鐵律:絕不下超過預期股數)。

Run: uv run --project . python -m quantlib.trading.live.tests.test_execute
     或 uv run --project . pytest src/quantlib/trading/live/tests/test_execute.py
"""
from __future__ import annotations

from quantlib.trading.live import execute


def test_safe_one_share_two_legs() -> None:
    """1 股 × ≤2 檔(S 正常型態)→ 放行。"""
    assert execute.order_safety_error(1, ["2466", "3374"]) is None
    assert execute.order_safety_error(1, []) is None
    assert execute.order_safety_error(1, ["2466"]) is None


def test_reject_oversized_shares() -> None:
    """每檔股數超過上限 → 拒絕(防 QL_S_SHARES_PER_BUY 誤設成大數)。"""
    assert execute.order_safety_error(6, ["2466"]) is not None
    assert execute.order_safety_error(1000, ["2466"]) is not None
    # 上限邊界:5 放行、6 拒絕
    assert execute.order_safety_error(execute._MAX_SHARES_PER_BUY, ["2466"]) is None
    assert execute.order_safety_error(execute._MAX_SHARES_PER_BUY + 1, ["2466"]) is not None


def test_reject_too_many_legs() -> None:
    """買入腿數異常(計劃檔損毀)→ 拒絕。"""
    many = [f"{i:04d}" for i in range(6)]
    assert execute.order_safety_error(1, many) is not None
    ok = [f"{i:04d}" for i in range(execute._MAX_BUY_LEGS)]
    assert execute.order_safety_error(1, ok) is None


def test_shares_per_buy_default_one() -> None:
    """未設 env → 預設 1 股(營運模式)。"""
    import os
    old = os.environ.pop("QL_S_SHARES_PER_BUY", None)
    try:
        assert execute._shares_per_buy() == 1
        os.environ["QL_S_SHARES_PER_BUY"] = "0"
        assert execute._shares_per_buy() == 1  # 非正 → 夾回 1
        os.environ["QL_S_SHARES_PER_BUY"] = "abc"
        assert execute._shares_per_buy() == 1  # 壞值 → 1
    finally:
        os.environ.pop("QL_S_SHARES_PER_BUY", None)
        if old is not None:
            os.environ["QL_S_SHARES_PER_BUY"] = old


def main() -> None:
    for fn in (test_safe_one_share_two_legs, test_reject_oversized_shares,
               test_reject_too_many_legs, test_shares_per_buy_default_one):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ execute 過量守門全過")


if __name__ == "__main__":
    main()


# ── 出場語義守護(2026-07-26;回測依據 limit_order_verdict.md ④)──────────────

def test_sell_leg_uses_open_urgency() -> None:
    """S 賣腿必須帶 --urgency open(開盤即出場 = S 的回測語義)。

    若退回 CLI 預設 exit,實盤會變成「整場撈高 + 收盤價收尾」= Serenity 語義,
    對 S 是 −5.39%/年顯著較差且 MDD 惡化。這條測試就是防那個回歸。
    """
    cmd = execute.build_trade_cmd(["2466"], ["3374"], 1, live=False)
    assert "--urgency" in cmd
    assert cmd[cmd.index("--urgency") + 1] == "open"
    assert execute.SELL_URGENCY == "open"


def test_no_urgency_flag_when_no_sell_leg() -> None:
    """純買入日不該帶賣腿參數(避免無意義旗標混淆 log)。"""
    cmd = execute.build_trade_cmd(["2466"], [], 1, live=False)
    assert "--urgency" not in cmd
    assert "--sell" not in cmd
    assert cmd[cmd.index("--buy") + 1] == "2466:1"


def test_live_flag_only_when_live() -> None:
    cmd = execute.build_trade_cmd([], ["3374"], 1, live=True)
    assert "--live" in cmd
    assert execute.build_trade_cmd([], ["3374"], 1, live=False)[-2:] == ["--urgency", "open"]


def test_sell_open_profile_crosses_immediately() -> None:
    """sell_open 必須首輪就跨價(掛買一 = 必成交),否則「必成交」是空話。"""
    import argparse

    from quantlib.trading.execution._cli import resolve_profile
    from quantlib.trading.execution.policy import target_price

    args = argparse.Namespace(urgency="open", patience="price", cap_pct=None, deadline=None)
    prof = resolve_profile("Sell", args)
    assert prof.name == "sell_open"
    assert prof.passive_rounds == 0 and prof.mid_rounds == 0   # round 0 即 >= 0 → 跨價
    # 首輪掛價 = 買一(跨價),不是賣一(被動)
    px = target_price("Sell", prof, round_idx=0, past_deadline=False,
                      bid=99.0, ask=101.0, arrival=100.0)
    assert abs(px - 99.0) < 1e-9


def test_serenity_still_uses_close_semantics() -> None:
    """Serenity 的 exit 語義不得被一起改掉——它的回測本來就是收盤價出場。"""
    import argparse

    from quantlib.trading.execution._cli import resolve_profile

    args = argparse.Namespace(urgency="exit", patience="price", cap_pct=None, deadline=None)
    prof = resolve_profile("Sell", args)
    assert prof.name == "sell_exit"
    assert prof.deadline_hhmm is None and prof.structure_anchor is True
