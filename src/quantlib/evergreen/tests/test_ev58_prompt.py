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
               for c in cards for n in P._STAGE_A)
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


def test_release_accepts_a_voided_case(tmp_path, monkeypatch, batch_id) -> None:
    """作廢也算結案——閘門與提示詞的契約必須是同一份。

    提示詞明文允許 agent 作廢一檔(觸發鐵律、機械性事件、完全無法檢索)。閘門若仍
    要求兩張卡全員到齊,該批就永遠釋不出來,而現場最可能的反應是「那就別寫 voided,
    硬產一張卡」——把守門機制變成造假誘因。
    """
    monkeypatch.setattr(P, "NEWS", tmp_path / "news")
    monkeypatch.setattr(P, "REVEAL", tmp_path / "news" / "_truth")
    cards = json.loads((P.CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(P, "G1_UNAVAIL", tmp_path / "g1.json")
    P.G1_UNAVAIL.write_text(json.dumps({"scanned": _all(cards), "unavailable": []}))
    for i, c in enumerate(cards):
        d = P.NEWS / f"{c['code']}_{c['d0']}"
        d.mkdir(parents=True)
        if i == 0:
            (d / "voided.json").write_text('{"reason":"mechanical_suspect"}')
        else:
            for n in P._STAGE_A:
                (d / n).write_text("{}")
    assert len(P.release(batch_id)) == len(cards)


def test_prompt_tells_agent_to_write_voided(batch_id) -> None:
    """閘門認得 voided.json,提示詞就必須叫 agent 寫它——否則機制在等一個沒人會產的檔案。"""
    pa, _, _ = P.render_attribution(batch_id)
    assert "voided.json" in pa, "提示詞未告知作廢檔的落檔方式"


def test_era_brief_is_read_only_for_attribution(batch_id) -> None:
    """年代語境卡是跨批共用觀測,歸因 agent 只准讀——並行寫會互相覆寫且無人察覺。"""
    pa, _, _ = P.render_attribution(batch_id)
    assert "_era_brief" in pa and "addenda" in pa, "缺 append-only 的補充管道"
    assert "只讀不寫" in pa, "未明文禁止歸因 agent 覆寫年代語境卡"


def test_era_brief_prompt_renders_for_every_era() -> None:
    """每個期別都要能渲染出前置提示詞——少一個,該期別的歸因批次會全數停在第 0 步。"""
    if not (P.CARDS / "_era_map.json").exists():
        pytest.skip("尚未產生期別對照表")
    m = json.loads((P.CARDS / "_era_map.json").read_text(encoding="utf-8"))
    for era in m:
        t = P.render_era_brief(era)
        left = {x[1:-1] for x in re.findall(r"\{[a-z_]+\}", t)}
        assert not left, f"{era} 的年代提示詞殘留變數:{sorted(left)}"
        assert m[era].split("~")[0] in t, "未代入實際區間,agent 不知道要研究哪幾年"
        assert "組裝說明" not in t


def test_era_brief_prompt_never_touches_single_stocks() -> None:
    """年代語境卡是共用觀測,一旦混進個股就不再共用,還會把個股偏見散佈給整個期別。"""
    t = (P.DRAFT / "PROMPT_ev58_era_brief.md").read_text(encoding="utf-8")
    assert "不碰個股" in t and "不准出現任何個股名稱或代碼" in t


def _all(cards):
    """本批全部案子——當作 G1 清單的 `scanned` 出處,代表「這批都掃過了」。"""
    return [{"code": c["code"], "d0": c["d0"]} for c in cards]


def _stage_a(root, cards, g1_status=None, leak_first=None):
    """依 `_STAGE_A` 的順序落檔(thin 先於 deep)——順序本身被閘門稽核。

    `leak_first` 的內容要在**寫檔當下**就放進 thin 卡,不能事後覆寫:事後覆寫會讓
    thin 的 mtime 晚於 deep,踩到「標記日模擬晚於考掘落檔」那道閘門,測到的就變成
    另一件事了。
    """
    for i, c in enumerate(cards):
        d = root / f"{c['code']}_{c['d0']}"
        d.mkdir(parents=True, exist_ok=True)
        for n in P._STAGE_A:                       # thin 兩張 + deep 兩張
            body = "{}"
            if leak_first is not None and i == 0 and n == "ex_ante_thin_d0.json":
                body = json.dumps({"leakage_log": {"encountered": [leak_first]}})
            (d / n).write_text(body)
        if g1_status:
            (d / "retrieval_log.jsonl").write_text(
                json.dumps({"gate": "G1", "status": g1_status, "queries": []}) + "\n")


def test_release_rejects_g1_recorded_as_miss(tmp_path, monkeypatch, batch_id) -> None:
    """已知 MOPS 回空白頁的案子,G1 記 `miss` 即擋下。

    記成 miss 會讓 `NO_NEWS_EXISTS` 的窮盡條件第 4 條(T1 層不得 blocked)被繞過,
    產出假的「這家公司當年真的沒有消息」——而真相是那份資料不在系統裡。這種錯誤
    在卡片上看不出來,所以必須機械攔。
    """
    cards = json.loads((P.CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(P, "NEWS", tmp_path / "news")
    monkeypatch.setattr(P, "REVEAL", tmp_path / "news" / "_truth")
    monkeypatch.setattr(P, "G1_UNAVAIL", tmp_path / "unavail.json")
    _one = [{"code": cards[0]["code"], "d0": cards[0]["d0"]}]
    P.G1_UNAVAIL.write_text(json.dumps({"scanned": _all(cards), "unavailable": _one}))
    _stage_a(P.NEWS, cards, g1_status="miss")
    with pytest.raises(RuntimeError, match="G1 記錄與實測矛盾"):
        P.release(batch_id)


def test_release_accepts_g1_recorded_as_blocked(tmp_path, monkeypatch, batch_id) -> None:
    """正常路徑必過——只會誤殺的閘門會逼現場繞過它。"""
    cards = json.loads((P.CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(P, "NEWS", tmp_path / "news")
    monkeypatch.setattr(P, "REVEAL", tmp_path / "news" / "_truth")
    monkeypatch.setattr(P, "G1_UNAVAIL", tmp_path / "unavail.json")
    _one = [{"code": cards[0]["code"], "d0": cards[0]["d0"]}]
    P.G1_UNAVAIL.write_text(json.dumps({"scanned": _all(cards), "unavailable": _one}))
    _stage_a(P.NEWS, cards, g1_status="blocked")
    assert len(P.release(batch_id)) == len(cards)


def test_g1_check_ignores_cases_not_on_the_list(tmp_path, monkeypatch, batch_id) -> None:
    """不在清單上的案子記 miss 是合法的——那是真的「查了沒有」。"""
    cards = json.loads((P.CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(P, "NEWS", tmp_path / "news")
    monkeypatch.setattr(P, "REVEAL", tmp_path / "news" / "_truth")
    monkeypatch.setattr(P, "G1_UNAVAIL", tmp_path / "unavail.json")
    P.G1_UNAVAIL.write_text(json.dumps({"scanned": _all(cards), "unavailable": []}))
    _stage_a(P.NEWS, cards, g1_status="miss")
    assert len(P.release(batch_id)) == len(cards)


def test_release_rejects_thin_stamped_after_deep(tmp_path, monkeypatch, batch_id) -> None:
    """thin 戳晚於 deep 落檔即擋下——順序反了代表淺判斷是看過深度證據後補寫的。

    這種汙染從檔案內容完全看不出來(thin 卡的欄位長得一模一樣),只有時序看得出來。
    而 thin/deep 的比較是本研究回答「標記日該不該加深查證」的唯一依據,汙染一次就沒了。
    """
    import time
    cards = json.loads((P.CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(P, "NEWS", tmp_path / "news")
    monkeypatch.setattr(P, "REVEAL", tmp_path / "news" / "_truth")
    monkeypatch.setattr(P, "G1_UNAVAIL", tmp_path / "none.json")
    for i, c in enumerate(cards):
        d = P.NEWS / f"{c['code']}_{c['d0']}"
        d.mkdir(parents=True)
        order = (P._STAGE_A if i else ("ex_ante_d_prev.json", "ex_ante_d0.json",
                                       "ex_ante_thin_d_prev.json", "ex_ante_thin_d0.json"))
        for n in order:
            (d / n).write_text("{}")
            time.sleep(0.01)
    with pytest.raises(RuntimeError, match="標記日模擬晚於考掘落檔"):
        P.release(batch_id)


def test_release_rejects_self_reported_contamination(tmp_path, monkeypatch, batch_id) -> None:
    """agent 自承被站位後的材料改變判斷 ⇒ 該批擋下,直到那些案寫 voided。

    這種污染從卡片內容看不出來(欄位一樣、日期一樣合法),唯一線索是 agent 自己記的
    `changed_my_view`。所以作廢必須自動且不帶懲罰——任何讓「回報」比「隱瞞」更痛的
    設計,都會把誠實變成傻事,而隱瞞事後無法偵測。
    """
    cards = json.loads((P.CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(P, "NEWS", tmp_path / "news")
    monkeypatch.setattr(P, "REVEAL", tmp_path / "news" / "_truth")
    monkeypatch.setattr(P, "G1_UNAVAIL", tmp_path / "none.json")
    P.G1_UNAVAIL.write_text(json.dumps({"scanned": _all(cards), "unavailable": []}))
    _stage_a(P.NEWS, cards, leak_first={"title": "連4漲停", "changed_my_view": True})
    with pytest.raises(RuntimeError, match="自承被後見之明污染"):
        P.release(batch_id)


def test_clean_leakage_log_does_not_block(tmp_path, monkeypatch, batch_id) -> None:
    """撞見但未改變判斷 = PIT 紀律正常運作,不得擋——擋它就是在懲罰誠實記錄。"""
    cards = json.loads((P.CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(P, "NEWS", tmp_path / "news")
    monkeypatch.setattr(P, "REVEAL", tmp_path / "news" / "_truth")
    monkeypatch.setattr(P, "G1_UNAVAIL", tmp_path / "none.json")
    P.G1_UNAVAIL.write_text(json.dumps({"scanned": _all(cards), "unavailable": []}))
    _stage_a(P.NEWS, cards, leak_first={"title": "x", "changed_my_view": False})
    assert len(P.release(batch_id)) == len(cards)


def test_prompt_specifies_thin_schema_as_its_own_block(batch_id) -> None:
    """thin schema 必須是獨立的 JSON 區塊並明令不得套用站位資訊卡——實測 46/48 張套錯。"""
    pa, _, _ = P.render_attribution(batch_id)
    i = pa.index("ex_ante_thin_d_prev.json")
    seg = pa[i:i + 2000]
    assert '"materials_used"' in seg and '"signal_type"' in seg, "thin schema 未以獨立區塊給出"
    assert "絕對不要套用" in seg, "未明令禁止套用站位資訊卡的 schema"


def test_release_rejects_stale_g1_list(tmp_path, monkeypatch, batch_id) -> None:
    """G1 清單與本批樣本零交集 ⇒ 擋下。

    清單是對某一版樣本實測出來的。樣本重抽後新的 (code, d0) 全不在舊清單裡,
    每一案都被跳過 ⇒ **閘門靜默失效**,而報告上完全看不出來。零交集就是過期的證據。
    """
    cards = json.loads((P.CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(P, "NEWS", tmp_path / "news")
    monkeypatch.setattr(P, "REVEAL", tmp_path / "news" / "_truth")
    monkeypatch.setattr(P, "G1_UNAVAIL", tmp_path / "stale.json")
    P.G1_UNAVAIL.write_text(json.dumps(
        {"scanned": [{"code": "0000", "d0": "1990-01-01"}], "unavailable": []}))
    _stage_a(P.NEWS, cards)
    with pytest.raises(RuntimeError, match="零交集"):
        P.release(batch_id)


def test_release_rejects_missing_g1_list(tmp_path, monkeypatch, batch_id) -> None:
    """清單不存在時**不得** fail-open——會讓人以為有在擋,比沒有閘門更糟。"""
    cards = json.loads((P.CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(P, "NEWS", tmp_path / "news")
    monkeypatch.setattr(P, "REVEAL", tmp_path / "news" / "_truth")
    monkeypatch.setattr(P, "G1_UNAVAIL", tmp_path / "does_not_exist.json")
    _stage_a(P.NEWS, cards)
    with pytest.raises(RuntimeError, match="清單不存在"):
        P.release(batch_id)
