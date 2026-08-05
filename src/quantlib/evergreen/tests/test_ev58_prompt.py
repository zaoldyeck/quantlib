"""提示詞組裝與真相釋出的守護——把散文承諾變成會紅燈的東西。

三件事在這裡被鎖死,每一件都對應一個「壞掉時完全看不出來」的失效:

1. **渲染後不得殘留編排變數**。殘留 `{news_root}` 的提示詞不會報錯,agent 會照字面
   建一個叫 `{news_root}` 的目錄,材料全落在無人回收的地方,而報告一切正常。
2. **組裝說明區塊必須被剝除**。那段寫著「每批 6 檔 = 2 組配對 + 2 檔未配對」——
   agent 讀到就能反推配對關係,臂別跟著露餡,盲判死得無聲。
3. **真相釋出必須 fail-closed**。階段 A 未落檔就釋出,該批盲判永久失效且事後從
   卡片與報告完全看不出來;晚釋出只是多跑一次指令。不對稱,所以預設拒絕。

Run: uv run --project . python -m pytest src/quantlib/evergreen/tests/test_ev58_prompt.py
"""
from __future__ import annotations

import json
import re

import pytest

from quantlib.evergreen import ev58_prompt as P

_ANY_BATCH = sorted(P.CARDS.glob("batch_*"))


@pytest.fixture(scope="module")
def batch_id() -> str:
    if not _ANY_BATCH:
        pytest.skip("尚未產生卡片(uv run -m quantlib.evergreen.ev58_build_cards --pilot)")
    return _ANY_BATCH[0].name


def test_render_leaves_no_orchestrator_placeholder(batch_id) -> None:
    pa, bs, bc = P.render_attribution(batch_id)
    left = {m[1:-1] for m in re.findall(r"\{[a-z_]+\}", pa + bs + bc)}
    # `{code}` / `{d0}` 刻意保留——那是 agent 逐檔自行代入的模板,填死會鎖在單一檔案。
    assert left <= {"code", "d0"}, f"渲染後仍殘留編排變數:{sorted(left - {'code', 'd0'})}"


def test_render_strips_the_assembly_header(batch_id) -> None:
    pa, _, _ = P.render_attribution(batch_id)
    for leak in ("組裝說明", "2 組配對", "編排契約", "抽樣契約"):
        assert leak not in pa, f"組裝說明未剝除,agent 會讀到「{leak}」"
    assert pa.lstrip().startswith("# 第一部"), "剝除後首行不是階段 A 標題"


def test_body_never_starts_a_line_with_quote_marker() -> None:
    """剝除以「開頭連續的 `> ` 行」為界——正文若也用引用區塊,這個界線就會吃掉正文。"""
    for f in P.DRAFT.glob("PROMPT_ev58_*.md"):
        body = P._strip_header(f.read_text(encoding="utf-8"))
        assert not body.lstrip().startswith(">"), f"{f.name} 剝除後仍以引用區塊起頭"


def test_phase_a_is_one_shared_text(batch_id) -> None:
    """兩分支的階段 A 必須同一份——一字之差就可能讓 agent 察覺自己拿到哪一臂。"""
    pa, _, _ = P.render_attribution(batch_id)          # 不同即 raise
    assert "階段 A" in pa


def test_truth_lives_outside_the_cards_root(batch_id) -> None:
    """真相不得與卡片同根——同根等於把答案放進 agent 已獲授權的目錄。"""
    assert not (P.CARDS / batch_id / "truth.json").exists(), "真相仍留在卡片目錄"
    assert (P.TRUTH / f"{batch_id}.json").exists(), "真相不在獨立根"
    assert P.TRUTH not in P.CARDS.parents and P.CARDS not in P.TRUTH.parents


def test_release_refuses_before_phase_a_lands(batch_id) -> None:
    """階段 A 未落檔即拒絕釋出,且錯誤訊息要指名缺哪個檔(否則沒人知道怎麼往下走)。"""
    cards = json.loads((P.CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    done = all((P.NEWS / f"{c['code']}_{c['d0']}" / n).exists()
               for c in cards for n in ("ex_ante_d_prev.json", "ex_ante_d0.json"))
    if done:
        pytest.skip("該批階段 A 已完成,閘門不適用")
    with pytest.raises(RuntimeError, match="拒絕釋出真相"):
        P.release(batch_id)


def test_era_split_spans_both_limit_generations() -> None:
    """保留組必須橫跨 7% 與 10% 兩個世代,否則測不到跨世代移植。"""
    if not (P.CARDS / "_era_map.json").exists():
        pytest.skip("尚未產生期別對照表")
    d, h = P.era_split()
    m = json.loads((P.CARDS / "_era_map.json").read_text(encoding="utf-8"))
    gen = {k: ("10%" if v.split("~")[0] >= "2015-06-01" else "7%") for k, v in m.items()}
    assert len(h) == 2 and {gen[k] for k in h} == {"7%", "10%"}, f"保留組世代不完整:{h}"
    assert not set(d) & set(h), "同一期別同時在蒸餾組與保留組"
    assert set(d) | set(h) == set(m), "有期別既不蒸餾也不保留,樣本被靜默丟棄"
