"""去識別掃描的誤報/漏報守護——兩個方向的代價完全不對稱,所以兩個方向都要測。

**漏報**(真洩漏放行)= 蒸餾器認出年代或公司 ⇒ 對台股那幾年的記憶灌進判斷,
而且事後完全看不出來。不可修。

**誤報**(正常文字被擋)= 該案退回重做。可修,但**大量誤報比漏報更危險**:
第一版把任何四位數字都當代碼,實測「1500 萬元」「2000 人」「3000 片」全部命中
——機制卡本來就有金額、產能、人數,於是幾乎每張卡都退回、零張進語料,而這會發生在
燒完全部歸因成本之後。攔到九成正常文字的網不是網,是牆。

Run: uv run --project . python -m pytest src/quantlib/evergreen/tests/test_ev58_deid_scan.py
"""
from __future__ import annotations

import pytest

from quantlib.evergreen.ev58_collect import scan

BL = {"台積電", "2330", "TSMC"}
#: 實際上市櫃代碼的極小子集——測試不連 cache,只需要幾個真代碼與幾個非代碼。
CODES = {"2330", "2317", "1503"}

CLEAN = [
    "上游供給被外生事件壓縮,少數合格供應商取得議價權,訂單能見度拉長到下 2 季",
    "客戶下單金額約 1500 萬元,佔年營收兩成",
    "擴產後員工增至 2000 人",
    "月產能自 3000 片提升至 8000 片",
    "資本額 2330 萬元",
    "每股營收 1998 元",
    "決定性硬消息落在站位前第 35 個交易日",
]
LEAKS = [
    ("本案代碼", "與 2330 同一供應鏈"),
    ("第三方代碼", "客戶 2317 拉貨"),
    ("日曆年份", "2016 年的產業循環"),
    ("公司名", "取得台積電認證"),
    ("英文名", "certified by TSMC"),
    ("漲跌幅世代", "當年單日漲跌幅上限 7%,連續三根漲停"),
]


@pytest.mark.parametrize("txt", CLEAN)
def test_clean_text_passes(txt: str) -> None:
    """正常路徑必過——只會誤殺的掃描器會逼現場關掉它。"""
    assert scan({"mechanism": txt}, BL, CODES) == [], txt


@pytest.mark.parametrize("name,txt", LEAKS)
def test_real_leak_caught(name: str, txt: str) -> None:
    assert scan({"mechanism": txt}, BL, CODES), name


def test_year_unit_is_not_treated_as_quantity() -> None:
    """四位數字後接「年」是日曆年份,不是時長——時長是一兩位數。

    把「年」列進單位會讓「2016 年的產業循環」放行。實測踩過,故單獨鎖死。
    """
    assert scan({"m": "2016 年的產業循環"}, BL, CODES)
    assert scan({"m": "訂單能見度拉長到 3 年"}, BL, CODES) == []


def test_without_code_universe_still_catches_own_code() -> None:
    """沒有代碼集合時退化為「卷宗黑名單 + 年份」——仍擋得住本案自己的代碼。

    最主要的洩漏面是本案自己;不相干第三方的代碼擋不到,但那是降級不是失效。
    """
    assert scan({"m": "與 2330 同一供應鏈"}, BL, None)
