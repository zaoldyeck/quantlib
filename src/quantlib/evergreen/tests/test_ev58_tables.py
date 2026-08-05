"""統計表的守護——這些表是蒸餾鐵律「禁止自己算數字」的整個地基。

表算錯的後果比沒有表更糟:蒸餾器被明令只准引表上的數字,所以它會照單全收,
而錯誤會原封不動地變成哲學裡的判別規則,再變成標記依據。

Run: uv run --project . python -m pytest src/quantlib/evergreen/tests/test_ev58_tables.py
"""
from __future__ import annotations

import math

import pytest

from quantlib.evergreen.ev58_tables import auc, contingency, wilson


def test_wilson_stays_inside_unit_interval_on_tiny_n() -> None:
    """小樣本必須守在 [0,1]。常態近似會給出超界區間——看起來像有結論,其實沒有。"""
    for k, n in ((0, 1), (1, 1), (0, 3), (3, 3), (1, 2)):
        lo, hi = wilson(k, n)
        assert 0.0 <= lo <= hi <= 1.0, f"k={k} n={n} → ({lo}, {hi})"


def test_wilson_widens_as_n_shrinks() -> None:
    assert (wilson(5, 10)[1] - wilson(5, 10)[0]) > (wilson(50, 100)[1] - wilson(50, 100)[0])


def test_auc_perfect_and_inverted() -> None:
    assert auc([1, 2, 3, 4], [0, 0, 1, 1])[0] == 1.0
    assert auc([4, 3, 2, 1], [0, 0, 1, 1])[0] == 0.0


def test_auc_ties_average_ranks() -> None:
    """平手不取平均秩的話,AUC 會被輸入順序左右——同一份資料換個排序就換個答案。"""
    a = auc([1, 1, 1, 1], [0, 0, 1, 1])[0]
    b = auc([1, 1, 1, 1], [1, 1, 0, 0])[0]
    assert a == b == 0.5


def test_auc_needs_both_classes() -> None:
    v, n = auc([1, 2, 3], [1, 1, 1])
    assert math.isnan(v) and n == 3


def test_auc_is_invariant_to_class_balance() -> None:
    """AUC 不受類別比例影響——這正是本樣本 50/50 富集卻仍能用它的理由。

    Brier 沒有這個性質,所以被明令不得報。
    """
    base = ([1, 2, 3, 4, 5, 6], [0, 0, 0, 1, 1, 1])
    enriched = ([1, 2, 3, 4, 5, 6, 4.5], [0, 0, 0, 1, 1, 1, 1])
    assert auc(*base)[0] == 1.0 and auc(*enriched)[0] == 1.0


def test_contingency_reports_both_sides() -> None:
    """雙邊計數是入場券:只從贏家身上讀出來的條件,正是上一版哲學的核心缺陷。"""
    rows = [("A", 1), ("A", 1), ("A", 0), ("B", 0), ("B", 0), ("B", 1)]
    got = {r["value"]: r for r in contingency(rows)}
    assert got["A"]["n_pos"] == 2 and got["A"]["n_neg"] == 1
    assert got["B"]["n_pos"] == 1 and got["B"]["n_neg"] == 2
    for r in got.values():
        assert {"rate_in_pos", "rate_in_neg", "lift", "wilson_lo", "wilson_hi"} <= set(r)


def test_contingency_flags_no_discrimination() -> None:
    """完全無分離時,信賴區間必須涵蓋基準率——否則蒸餾器會把雜訊寫成規則。"""
    rows = [("A", i % 2) for i in range(20)]
    assert not contingency(rows)[0]["ci_excludes_base"]


def test_lift_is_none_when_denominator_zero() -> None:
    """負例側為零時 lift 無定義。回 0 或 inf 都會讓蒸餾器讀出一個假的巨大效果。"""
    assert contingency([("A", 1), ("A", 1)])[0]["lift"] is None
