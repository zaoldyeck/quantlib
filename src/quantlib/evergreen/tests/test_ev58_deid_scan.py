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

from quantlib.evergreen.ev58_collect import _strings, scan

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
    # 時長寫法必須放行:量詞「個」把它與月份分開,不需要例外清單。
    "送樣後約 6 個月才進入量產,三個月內未見客戶導入",
    # 任指詞:「任一月營收」的「一」屬於「任」,不是一月。證偽條件的常用寫法。
    "後續任一月營收若跌回 1,500 萬元以下並連續兩個月年增率轉負",
    "訂單能見度拉長到 3 年",
    "累積虧損等於全部縮減額,連續虧損 10 年",
]
LEAKS = [
    ("本案代碼", "與 2330 同一供應鏈"),
    ("第三方代碼", "客戶 2317 拉貨"),
    ("日曆年份", "2016 年的產業循環"),
    ("公司名", "取得台積電認證"),
    ("英文名", "certified by TSMC"),
    ("漲跌幅世代", "當年單日漲跌幅上限 7%,連續三根漲停"),
    # 民國紀年:當年一手材料全是這個寫法,`_YEAR` 一個都攔不到。
    ("民國年月日", "董事會決議見 100/03/22 重大訊息"),
    ("民國年月", "股東常會於 100/06 通過"),
    ("民國年度", "99 年度營業淨損 7,466 萬元"),
    ("中文月份", "三月那批報導全部同一天"),
    ("阿拉伯月份", "站位落在 8 月的系統性下殺之後"),
    ("月份帶名詞", "6 月營收 19,125 仟元,年增 9.8%"),
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


def test_blacklist_harvests_nested_identity_fields() -> None:
    """黑名單導出必須走進巢狀結構——不然整個守護是空的。

    提示詞規定的卷宗形態是 `{"identity_card": {...}}` / `{"peers": [{...}]}`。
    舊版遞迴只在 key 命中身分字樣時才往下走,於是在最外層就停住,回傳空清單:
    掃描器拿著空黑名單去掃,任何公司名都照樣放行,而報表上寫的是「乾淨」。
    **這一條測的不是掃描邏輯,是黑名單的來源**——原本沒有任何測試覆蓋它。
    """
    cr = {"identity_card": {"code": "5468", "name_today": "甲公司",
                            "name_then": "甲公司舊名"},
          "ex_ante_cards": {"deep_d0": {"peers": [{"code": "3527", "name": "乙公司"}]}}}
    got = set(_strings(cr))
    assert {"5468", "甲公司", "甲公司舊名", "3527", "乙公司"} <= got, got


def test_feature_names_are_not_treated_as_identities() -> None:
    """機制卡自己的 `novel_features[].name` 不是公司名——收了它,每張卡都會退回。

    特徵名**本來就該**同時出現在卷宗與機制卡上(那是同一個特徵)。把光禿禿的
    `name` 一律當身分,等於用「卡片寫了自己的特徵名」當洩漏理由。判準改成:
    `name` 只有在同一層還帶著代碼時才算身分(同業條目的形態)。
    """
    got = set(_strings({"novel_features": [{"name": "年報獨佔度",
                                            "definition": "…", "value": "0 則"}],
                        "peers": [{"code": "3527", "name": "乙公司"}]}))
    assert "年報獨佔度" not in got
    assert {"3527", "乙公司"} <= got, got


def test_sealed_era_code_never_enters_blacklist() -> None:
    """期別碼是**刻意公開**的,不能因為 key 含 "code" 就被當成身分。

    若它進了黑名單,每一張正確寫上期別碼的機制卡都會被退回,而退回理由
    看起來與真洩漏一模一樣——守護反過來擋住正確答案。
    """
    assert _strings({"era_code": "E4", "code_masked": "S007"}) == []
    # 守護點在導出端:只要期別碼沒進黑名單,寫了期別碼的乾淨卡就會過。
    assert scan({"era_code": "E4", "mechanism": "上游供給被壓縮"}, BL, CODES) == []


def test_month_as_duration_passes_but_calendar_month_caught() -> None:
    """月份的兩種寫法必須分開:時長放行、日曆月攔下。

    「6 個月」是機制內容(時滯),「6 月」是年代座標(期別碼 + 月份 = 定位到季)。
    """
    assert scan({"m": "時滯約 6 個月"}, BL, CODES) == []
    assert scan({"m": "6 月營收公告"}, BL, CODES)


def test_without_code_universe_still_catches_own_code() -> None:
    """沒有代碼集合時退化為「卷宗黑名單 + 年份」——仍擋得住本案自己的代碼。

    最主要的洩漏面是本案自己;不相干第三方的代碼擋不到,但那是降級不是失效。
    """
    assert scan({"m": "與 2330 同一供應鏈"}, BL, None)
