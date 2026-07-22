"""S 主決策路徑的端到端煙霧測試——**這條路徑先前完全沒有測試覆蓋**。

為什麼補這支:2026-07-22 Phase 2 結構重整後,218 個測試全綠、部署到 VM 才發現
`s_advisor` 一進門就 `UnboundLocalError`(模組級 `paths` 被函式內同名區域變數
遮蔽)。單元測試各自蓋住了 `pool_history`、`entry_anchors`、`s_hold_action` 等
零件,**卻沒有任何一支真的把 s_advisor 從頭跑一遍**——於是整條 money-path 的
「組裝」是無人看守的。

這支刻意跑真 cache(約 4 秒)。它不驗策略結論(那由 pool_history 的逐位 parity
守),只驗**這條路徑跑得完、而且輸出結構完整**:每檔持股都要有判決、買入建議
都要有理由、明細都要有進場錨。零件對、組裝錯,一樣是零分。

依賴:`var/cache/cache.duckdb` 需存在(沒有就 skip,不讓 CI 假紅)。
Run: uv run --project research python -m pytest research/tri/tests/test_s_advisor_smoke.py
"""
from __future__ import annotations

from datetime import date as Date

import pytest

from research import paths
from research.apex import data
from research.tri.advisors import S_NAME, s_advisor

pytestmark = pytest.mark.skipif(not paths.CACHE_DB.exists(),
                                reason=f"{paths.CACHE_DB.name} not present")

HOLDINGS = {"2059": 1.0, "2221": 1.0, "2466": 1.0,
            "3374": 1.0, "3704": 1.0, "6446": 1.0}


@pytest.fixture(scope="module")
def advice():
    con = data.connect()
    try:
        return s_advisor(con, HOLDINGS, Date.today(), nav=15_000.0)
    finally:
        con.close()


def test_every_holding_gets_a_verdict(advice) -> None:
    """每一檔持股都必須被判決(續抱或賣出)。**靜默漏掉一檔 = 該賣的沒賣。**"""
    judged = {c for c, _ in advice.keeps} | {c for c, _ in advice.sells}
    missing = set(HOLDINGS) - judged
    assert not missing, f"這些持股沒有任何判決:{sorted(missing)}"


def test_verdicts_carry_reasons(advice) -> None:
    """每個判決都要講得出理由——沒有理由的賣出指令,人沒辦法覆核。"""
    for code, why in advice.keeps + advice.sells:
        assert why and why.strip(), f"{code} 的判決沒有理由"


def test_buys_are_capped_and_reasoned(advice) -> None:
    """買入清單每筆都要有權重與理由;席位上限 5 檔不得被突破。"""
    assert len(advice.keeps) <= 5, f"續抱 {len(advice.keeps)} 檔 > 席位上限 5"
    for code, weight, why in advice.buys:
        assert 0 < weight <= 1, f"{code} 權重異常 {weight}"
        assert why and why.strip(), f"{code} 的買進建議沒有理由"


def test_keeps_expose_entry_anchor(advice) -> None:
    """續抱的每一檔都要帶出進場錨——出場鐘從那天起算,看不到錨就無法覆核出場。"""
    for code, _ in advice.keeps:
        d = advice.detail.get(code, {})
        assert d.get("entry_date"), f"{code} 的明細沒有進場錨"


def test_strategy_is_named_for_humans(advice) -> None:
    """對外顯示一律用正名,不得漏出研發期代號。"""
    assert advice.strategy == S_NAME
    assert "apex" not in advice.strategy.lower()


def main() -> None:
    con = data.connect()
    try:
        adv = s_advisor(con, HOLDINGS, Date.today(), nav=15_000.0)
    finally:
        con.close()
    for fn in (test_every_holding_gets_a_verdict, test_verdicts_carry_reasons,
               test_buys_are_capped_and_reasoned, test_keeps_expose_entry_anchor,
               test_strategy_is_named_for_humans):
        fn(adv)
        print(f"✓ {fn.__name__}")
    print("✓ S 主決策路徑全過")


if __name__ == "__main__":
    main()
