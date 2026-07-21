"""S 角色純度判決守護:鎖死「認證只在收養日鎖定、重跑不累積」的決定性。

背景(2026-07-21 使用者實測抓到的狀態污染):舊碼每次持股「在池」都覆寫
vetted_pool=True,於是用不同交易日重跑會累積出不同的認證集——「哪幾天跑過就留
哪些股票」。改為收養當天一次性鎖定認證後,同一部位無論重跑幾次、哪天跑,判決恆定。

Run: uv run --project research python -m research.tri.tests.test_s_role_action
     或 uv run --project research pytest research/tri/tests/test_s_role_action.py
"""
from __future__ import annotations

from research.tri.advisors import _s_role_action


def test_adoption_day_in_pool_vets() -> None:
    """收養當天在池 → keep 且鎖定認證。"""
    assert _s_role_action(in_pool=True, vetted=False, adoption_day=True) == ("keep_pool", True)


def test_later_day_in_pool_does_not_accumulate_vet() -> None:
    """關鍵守護:非收養日、尚未認證的持股即使今天在池,也**不追認證**
    → 這杜絕了「多跑幾天就累積認證」的污染。今天仍因在池而 keep,但不會被永久留住。"""
    assert _s_role_action(in_pool=True, vetted=False, adoption_day=False) == ("keep_pool", False)


def test_already_vetted_stays_vetted_in_pool() -> None:
    assert _s_role_action(in_pool=True, vetted=True, adoption_day=False) == ("keep_pool", True)


def test_vetted_out_of_pool_holds_until_exit() -> None:
    """已認證的合法部位離開新鮮池 → 續抱至出場規則(hold-until-exit)。"""
    assert _s_role_action(in_pool=False, vetted=True, adoption_day=False) == ("keep_vetted", True)


def test_unvetted_out_of_pool_sells() -> None:
    """收養且非今日標的、收養日未過池檢 → 賣(角色純度:S 不會買的名字不留)。"""
    assert _s_role_action(in_pool=False, vetted=False, adoption_day=False) == ("sell_role", False)


def test_adoption_day_not_in_pool_sells() -> None:
    """收養當天就不在池 → 賣(從一開始就不是 S 標的)。"""
    assert _s_role_action(in_pool=False, vetted=False, adoption_day=True) == ("sell_role", False)


def test_determinism_rerun_same_day_idempotent() -> None:
    """同一輸入重跑必得同一輸出 + 同一新 vetted(純函式無副作用、無 run-history)。"""
    args = dict(in_pool=True, vetted=False, adoption_day=False)
    first = _s_role_action(**args)
    for _ in range(5):
        assert _s_role_action(**args) == first
    # 且非收養日的「在池」永不製造認證 → 重跑 N 次也不會把它變成永久部位
    assert first[1] is False


def main() -> None:
    for fn in (test_adoption_day_in_pool_vets,
               test_later_day_in_pool_does_not_accumulate_vet,
               test_already_vetted_stays_vetted_in_pool,
               test_vetted_out_of_pool_holds_until_exit,
               test_unvetted_out_of_pool_sells,
               test_adoption_day_not_in_pool_sells,
               test_determinism_rerun_same_day_idempotent):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ s_role_action 全過")


if __name__ == "__main__":
    main()
