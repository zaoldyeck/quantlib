"""進場錨守護:S 的持股身分與出場時鐘,必須完全由**市場資料**決定。

事故背景(2026-07-22,三連環,同一個病根「用會漂的東西回答不會漂的問題」):
  1. 用**今日池籍**回答「這是誰的部位」→ 決策→成交→入庫存的時間差讓 S 自己買的
     股票掉出池後被當外人砍掉(買入隔天就賣)。
  2. 改用**執行器成交檔**回答 → 檔案在誰下單就留在誰身上,本機與 VM 各一份且不同步
     → 同一天、同一部位,本機判「續抱」、VM 判「賣出」。而且執行器是三個策略共用的,
     成交只證明「有人買了」,不證明「S 買了」(實例:6446 是 Evergreen 的部位)。
  3. 正解(使用者定調):**一旦標的入池,那天就是 S 的進場點**,出場鐘與價位門
     全部從那天起算;從未入池者才是外人。池籍是市場資料的函數 → 任何機器、
     任何時間重算都一樣,零 state、可重現。

本測試守護第 3 條的語義。池定義本身的逐位正確性由 `test_pool_history` 守。
Run: uv run --project research python -m research.tri.tests.test_entry_anchor
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.tri.advisors import C, entry_anchors

D = Date.fromisoformat


def _ph(rows: list[tuple[str, str]]) -> pl.DataFrame:
    """迷你逐日池:[(code, date)]。"""
    return pl.DataFrame({C: [r[0] for r in rows], "date": [D(r[1]) for r in rows]},
                        schema={C: pl.Utf8, "date": pl.Date})


def test_anchor_is_last_pool_day_at_or_before_acquisition() -> None:
    """你在 07-20 買進,S 最近一次選中它是 07-17 → 錨 = 07-17。"""
    ph = _ph([("2466", "2026-06-12"), ("2466", "2026-07-17"), ("2466", "2026-07-24")])
    assert entry_anchors(ph, {"2466": D("2026-07-20")}) == {"2466": D("2026-07-17")}


def test_anchor_never_looks_into_the_future() -> None:
    """只有 07-24 那次入池,而你 07-20 就持有了 → 那不是你買它的理由,無錨。"""
    ph = _ph([("2466", "2026-07-24")])
    assert entry_anchors(ph, {"2466": D("2026-07-20")}) == {}


def test_never_in_pool_has_no_anchor() -> None:
    """從未入池 = 不是 S 的標的(2059/6446 的真實情形:被現金流品質閘連刷 140 次)。"""
    ph = _ph([("2466", "2026-07-17")])
    assert "2059" not in entry_anchors(ph, {"2059": D("2026-07-20")})


def test_same_day_pool_membership_counts() -> None:
    """當天入池、當天持有 → 錨就是當天(≤ 是閉區間,不得漏掉邊界)。"""
    ph = _ph([("3704", "2026-07-20")])
    assert entry_anchors(ph, {"3704": D("2026-07-20")}) == {"3704": D("2026-07-20")}


def test_multiple_positions_resolved_independently() -> None:
    ph = _ph([("A", "2026-01-05"), ("A", "2026-06-05"),
              ("B", "2026-03-03"), ("C", "2026-05-05")])
    got = entry_anchors(ph, {"A": D("2026-04-01"), "B": D("2026-04-01"),
                             "C": D("2026-04-01")})
    assert got == {"A": D("2026-01-05"), "B": D("2026-03-03")}, got


def test_empty_inputs_are_safe() -> None:
    """空持股/空池不得拋——盤前任何一步炸掉,今天就沒有交易計劃。"""
    assert entry_anchors(_ph([("A", "2026-01-05")]), {}) == {}
    assert entry_anchors(_ph([]), {"A": D("2026-01-05")}) == {}


def test_anchor_is_machine_independent() -> None:
    """同樣的池 + 同樣的取得日 → 同樣的錨。這正是跨機分歧事故的守護:
    答案只依賴市場資料,不依賴任何本機檔案(成交紀錄、計劃檔、state)。"""
    ph = _ph([("2466", "2026-07-17"), ("2466", "2026-06-12")])
    a = entry_anchors(ph, {"2466": D("2026-07-20")})
    b = entry_anchors(ph.sample(fraction=1.0, shuffle=True, seed=7),
                      {"2466": D("2026-07-20")})
    assert a == b == {"2466": D("2026-07-17")}


def main() -> None:
    for fn in (test_anchor_is_last_pool_day_at_or_before_acquisition,
               test_anchor_never_looks_into_the_future,
               test_never_in_pool_has_no_anchor,
               test_same_day_pool_membership_counts,
               test_multiple_positions_resolved_independently,
               test_empty_inputs_are_safe,
               test_anchor_is_machine_independent):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ 進場錨全過")


if __name__ == "__main__":
    main()
