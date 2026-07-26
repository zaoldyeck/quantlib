"""保留股防護的守護(2026-07-27 川湖事故的防復發機制)。

事故:保留清單只活在 VM 的 .env,該檔由 Secret Manager 重新產生時遺失
`QL_S_PROTECTED` 鍵 → 讀不到就回空集合(fail-open、零警告)→ 川湖 2059
被排進當日自動賣單。這裡鎖死「版控是唯一真源、環境變數只能加不能減」。

Run: uv run --project . python -m pytest src/quantlib/trading/live/tests/test_protected_holdings.py
"""
from __future__ import annotations

import os
from unittest import mock

from quantlib.trading.live.s_plan import PROTECTED_HOLDINGS, protected_holdings


def test_protection_survives_missing_env_var() -> None:
    """環境變數完全不存在時,版控清單仍然生效——這就是事故當天缺的那道防線。"""
    with mock.patch.dict(os.environ, {}, clear=True):
        assert "2059" in protected_holdings()


def test_protection_survives_empty_env_var() -> None:
    """變數存在但為空字串(誤設/被清空)同樣不得讓保護消失。"""
    with mock.patch.dict(os.environ, {"QL_S_PROTECTED": ""}):
        assert "2059" in protected_holdings()


def test_env_var_can_add_but_never_remove() -> None:
    """環境變數是聯集不是覆蓋:列了別的股票,也不能把版控清單頂掉。"""
    with mock.patch.dict(os.environ, {"QL_S_PROTECTED": "1101"}):
        got = protected_holdings()
        assert "1101" in got and "2059" in got


def test_env_var_pads_to_four_digits() -> None:
    with mock.patch.dict(os.environ, {"QL_S_PROTECTED": "56"}):
        assert "0056" in protected_holdings()


def test_committed_list_is_the_source_of_truth() -> None:
    """解除保護必須是一次 commit(審計軌跡),不能靠改環境變數繞過。
    2026-07-27 使用者明示 6446 解除保護、2059 續留——以版控清單為憑。"""
    assert PROTECTED_HOLDINGS == frozenset({"2059"})


def test_execute_filters_protected_out_of_auto_sells() -> None:
    """端到端語義:保留股不得出現在自動賣清單(execute 的 defense-in-depth)。"""
    with mock.patch.dict(os.environ, {}, clear=True):
        protected = protected_holdings()
        suggested = ["2466", "6446", "2059"]
        auto = [c for c in suggested if c not in protected]
        assert auto == ["2466", "6446"]
