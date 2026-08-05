"""標記日不得檢索「成員資格編碼了答案」的材料庫。

`ev27_news/` 是歷史樣本的材料庫,而樣本是因為後來大漲才被抽進來的(224 檔中 168 檔
是暴漲正例)。按題材關鍵詞 grep 它,命中就系統性偏向後來漲過的股票——**目錄的成員
資格本身編碼了答案**,而任何 PIT 日期檢查都攔不住:每一條材料的發布日期都合法,
偏掉的是「你看得到哪些材料」。

`ev28_news/` 例外且必須保留:它的成員資格反映「標記 Agent 當時看過什麼」,是這個
系統自己的累積知識,不是後見之明。

Run: uv run --project . python -m pytest src/quantlib/evergreen/tests/test_labeling_no_survivorship_corpus.py
"""
from __future__ import annotations

from pathlib import Path

PROMPT = Path("src/quantlib/evergreen/PROMPT_ev28_labeling.md")
#: 成員資格由「後來的結果」決定的材料庫。新增研究樣本庫時必須加進這裡。
OUTCOME_SELECTED = ("ev27_news", "ev58_news", "ev45_news")


def test_labeling_prompt_does_not_send_agent_to_outcome_selected_corpora() -> None:
    body = PROMPT.read_text(encoding="utf-8")
    # 允許在「不得檢索」的警語裡出現目錄名;禁止的是把它列為檢索來源。
    instructions = body.split("🔴")[0]
    hit = [d for d in OUTCOME_SELECTED if d in instructions]
    assert not hit, f"標記提示詞把倖存者偏誤材料庫列為檢索來源:{hit}"


def test_the_ban_is_stated_not_merely_omitted() -> None:
    """光是拿掉不夠——下一個人會「順手加回去」。禁令必須寫明,而且要寫出理由。"""
    body = PROMPT.read_text(encoding="utf-8")
    assert "成員資格" in body and "不得再檢索" in body, "缺少明文禁令與理由"
    assert "ev58_news" in body, "未把日後產出的研究樣本庫一併納入禁令"
