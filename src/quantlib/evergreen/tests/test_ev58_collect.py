"""去識別掃描的守護——先紅後綠驗證它真的攔得住,不然它只是裝飾。

掃描器要攔的四類洩漏,每一類都足以讓蒸餾器認出年代或公司;而認出年代之後,模型對
台股那幾年的記憶會灌進判斷,正是這次重做要根除的東西。失效完全無聲:洩漏的機制卡
長得跟乾淨的一模一樣,報告也照樣產得出來。

Run: uv run --project . python -m pytest src/quantlib/evergreen/tests/test_ev58_collect.py
"""
from __future__ import annotations

from quantlib.evergreen.ev58_collect import scan

BL = {"2330", "台積電", "TSMC", "2008"}


def test_clean_card_passes() -> None:
    """正常路徑必過——只會誤殺的掃描器會逼現場繞過它。"""
    ok = {"mechanism": "上游供給被外生事件壓縮,少數合格供應商取得議價權",
          "lag_structure": "決定性硬消息落在站位前 12 個交易日",
          "era_code": "E4"}
    assert scan(ok, BL) == []


def test_catches_four_digit_code() -> None:
    assert scan({"m": "與 2330 同一供應鏈"}, BL)


def test_catches_year() -> None:
    assert scan({"m": "2008 年金融海嘯期間"}, BL)


def test_catches_dossier_name() -> None:
    """黑名單逐案導出——全域清單漏掉「這一案才提到的那家客戶」,而供應鏈敘事裡
    最容易指認公司的往往正是那些名字。"""
    assert scan({"m": "取得台積電認證"}, BL)
    assert scan({"m": "certified by TSMC"}, BL)


def test_catches_limit_era_phrasing() -> None:
    """漲跌幅世代:「7%」等於直說站位在 2015-06 之前,而年份禁令蓋不到它。"""
    assert scan({"m": "當年單日漲跌幅上限 7%,連續三根漲停"}, BL)
    assert scan({"m": "受限於 10% 的單日漲跌幅"}, BL)


def test_long_sentences_do_not_enter_blacklist() -> None:
    """黑名單只收像專名的短字串——收進長句會讓掃描全案誤殺,而誤殺會逼人關掉它。"""
    from quantlib.evergreen.ev58_collect import _strings
    got = _strings({"company_name": "某公司", "narrative": "這是一段很長的敘述" * 5})
    assert got == ["某公司"]
