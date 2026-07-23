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
