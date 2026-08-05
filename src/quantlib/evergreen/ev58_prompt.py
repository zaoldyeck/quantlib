"""EV58 提示詞組裝器 + 真相釋出閘門——把提示詞裡「用散文承諾的隔離」變成機制。

## 為什麼必須有這支(2026-08-05 第六、七輪 review 抓到的缺口)

**缺口一:提示詞是模板,但沒有任何東西會把它渲染出來。**
`PROMPT_ev58_*.md` 內有 `{era_code}`/`{news_root}`/`{truth_root}`/`{sample_period_end}`/
`{distill_eras}`/`{holdout_eras}` 等由編排代入的變數,repo 裡卻找不到填充者。
兩個後果:(a) 有人直接把 `.md` 貼給 agent,agent 看到的是 `{news_root}` 字面值,
落檔路徑全錯、材料散落無法回收;(b) 有人用 `str.format()` 去填——提示詞裡有大量
JSON 範例區塊,`{` 會被當成欄位起頭,當場 `KeyError`/`IndexError`。
本模組只做**具名 token 的字面替換**,JSON 區塊原樣不動,且 `{code}`/`{d0}` 這兩個
**由 agent 逐檔自行代入**的模板變數刻意保留不填。

**缺口二:`truth.json` 與 `cards.json` 同目錄,「物理隔離」名不副實。**
`ev58_build_cards` 的 docstring 寫「編排腳本在階段 A 寫檔完成後才釋出」,但實作
把兩者寫進同一個 `batch_XXX/`,而歸因 agent 從第一秒就拿著 `{truth_root}`。
盲判只剩「請你不要看」——**那正是這整套設計宣稱要取代的東西**。
本模組把真相移到獨立的 `ev58_truth/` 根(從不交給 agent),並提供 `release()`:
逐檔驗明 `ex_ante_d_prev.json` 與 `ex_ante_d0.json` 都已落地,才把該檔真相寫進
agent 看得到的 `_truth/`;任一檔缺件則整批拒絕釋出。**閘門 fail-closed**。

**缺口三:提示詞開頭的「組裝說明」區塊會洩漏編排契約。**
那段寫著「每批 6 檔 = 2 組配對(暴漲 + 其配對對照)+ 2 檔未配對」——agent 讀到
就知道批內恰有兩組配對,配對關係一被反推,臂別跟著露餡。原文標了「此區塊不進入
agent 提示詞」,但沒有任何機制執行它。本模組在渲染時機械剝除。

**缺口四(第七輪):年代語境卡是跨批共用觀測,卻叫每批自己做。**
原提示詞寫「若該檔已由同年代的前一批建立,直接讀用,只補你新學到的語彙」——
讀-改-寫的競態:同年代多批並行時,後寫的把前一批的語彙整段抹掉,而且沒有任何跡象。
批次少時看不出來(試跑每年代只有 1 批),正式跑 E1 有二十幾批必然踩到。改成專責前置
步驟(`PROMPT_ev58_era_brief.md`,每期別一次),歸因 agent **只讀不寫**,現場的補充走
append-only 的 addenda。這同時省掉「同一份年代研究被重做二十次」的 token。

**缺口五(第七輪):作廢的檔沒有出口,閘門與提示詞契約互相打架。**
提示詞明文允許 agent 作廢一檔(觸發鐵律、機械性事件、完全無法檢索),但落檔契約與
本模組的釋出閘門都要求「6 檔的 ex_ante 全部落地」——作廢一檔,該批永遠釋不出來。
現場最可能的反應是「那就別寫作廢,硬產一張卡」,把守門機制變成造假誘因。
現在「結案」= 兩張卡齊備**或**有 `voided.json`,兩邊同一份契約。

Run:
  uv run --project . python -m quantlib.evergreen.ev58_prompt era-brief E1   # 前置,每期別一次
  uv run --project . python -m quantlib.evergreen.ev58_prompt render batch_000
  uv run --project . python -m quantlib.evergreen.ev58_prompt release batch_000
  uv run --project . python -m quantlib.evergreen.ev58_prompt distill
依賴 cache: 否(只讀 ev58_build_cards 的產出)。
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import date as Date
from pathlib import Path

from quantlib import paths
from quantlib.evergreen.ev58_build_cards import OUT as CARDS
from quantlib.evergreen.ev58_build_cards import TRUTH

DRAFT = Path(__file__).parent / "draft"
#: agent 的工作根(材料、身分卡、年代語彙表、階段 A 落檔)。跨批共用,
#: 讓 `_era_brief/` 與 `_identity/` 只採一次(提示詞第 0、1 步的前提)。
NEWS = paths.OUT / "ev58_news"
#: 釋出後的真相落點,位於 agent 工作根之下;**釋出前這個目錄是空的**。
REVEAL = NEWS / "_truth"

_PHASE_B = "# 第二部・階段 B"
#: 開頭的引用區塊 + 緊接其後的水平線。水平線是那個區塊的收尾,留著就成了
#: 提示詞的第一行,渲染出來的檔案看起來像被截斷過。
_HEADER = re.compile(r"\A(?:> .*\n)+\s*(?:-{3,}\s*\n)?\s*", re.MULTILINE)


def _strip_header(text: str) -> str:
    """剝除開頭的組裝說明引用區塊(缺口三)。

    那段自稱「此區塊不進入 agent 提示詞」,卻同時寫著批次的配對結構——它是全文
    洩漏量最大的一段。用「開頭連續的 `> ` 行」為界:那是 markdown 引用區塊的形態,
    正文一律不以 `> ` 起頭(已由測試鎖死)。
    """
    return _HEADER.sub("", text, count=1)


def _split(text: str) -> tuple[str, str]:
    i = text.index(_PHASE_B)
    return text[:i].rstrip() + "\n", text[i:]


def _fill(text: str, **vars: str) -> str:
    """只替換具名 token,JSON 區塊的大括號原樣保留。

    刻意**不填** `{code}` 與 `{d0}`:那兩個是 agent 逐檔自行代入的模板變數,
    填掉會讓路徑鎖死在單一檔案。
    """
    for k, v in vars.items():
        text = text.replace("{" + k + "}", v)
        text = text.replace("`{" + k + "}`", f"`{v}`")   # 反引號包住的同款
    return text


def _batch_ctx(batch_id: str) -> dict[str, str]:
    cards = json.loads((CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    eras = {c["era_code"] for c in cards}
    if len(eras) != 1:
        raise ValueError(f"{batch_id} 跨越多個期別 {eras},年代語彙表無法批內共用")
    era = eras.pop()
    return {
        "era_code": era,
        "era_key": era,                       # 語彙表以密封碼命名,檔名本身不得洩漏年份
        "news_root": str(NEWS),
        "truth_root": str(REVEAL),
        # 批內最晚站位日即本批的記憶天花板。逐檔 PIT 由鐵律 1 管,更嚴;
        # 這條管的是「你記憶中後來發生的事」,只需批級上界。d0 本就印在卡片上,不構成洩漏。
        "sample_period_end": max(c["d0"] for c in cards),
    }


def render_attribution(batch_id: str) -> tuple[str, str, str]:
    """回傳 (階段 A, 階段 B・暴漲分支, 階段 B・偽形分支),皆已填好變數。

    階段 A 只有一份——這是設計的承重點:兩分支的階段 A 若有一字之差,agent 就可能
    從措辭密度察覺自己拿到的是哪一臂。這裡直接**驗證後只留一份**,而不是「發射前
    記得 diff」。
    """
    ctx = _batch_ctx(batch_id)
    a_s, b_s = _split(_strip_header((DRAFT / "PROMPT_ev58_attribution_surge.md")
                                    .read_text(encoding="utf-8")))
    a_c, b_c = _split(_strip_header((DRAFT / "PROMPT_ev58_attribution_control.md")
                                    .read_text(encoding="utf-8")))
    if a_s != a_c:
        raise ValueError("兩分支的階段 A 不一致——盲測對稱性已破,禁止發射")
    return _fill(a_s, **ctx), _fill(b_s, **ctx), _fill(b_c, **ctx)


# ---------------------------------------------------------------- 真相釋出閘門

def _cases(batch_id: str) -> list[tuple[str, str]]:
    cards = json.loads((CARDS / batch_id / "cards.json").read_text(encoding="utf-8"))
    return [(c["code"], c["d0"]) for c in cards]


#: 實測 MOPS 回空白頁的案子(`ev59_retrievability_probe --full-sample` 產出)。
#: 這份清單存在的理由:提示詞可以叫 agent「空白頁記 blocked 不記 miss」,但那只是
#: 一句話。有清單就能機械對照——**規則沒有機制就等於沒有規則**。
G1_UNAVAIL = paths.OUT / "ev59_g1_unavailable.json"

#: 階段 A 的四張卡。thin 兩張是**標記日深度**的判斷戳(第 2.5 步),deep 兩張是
#: 考掘深度(第 3-4 步)。兩個深度都要,因為生產端的標記 Agent 只拿得到 thin 那一種
#: ——只量 deep 等於報一個生產端永遠達不到的天花板。
_STAGE_A = ("ex_ante_thin_d_prev.json", "ex_ante_thin_d0.json",
            "ex_ante_d_prev.json", "ex_ante_d0.json")


def _g1_misrecorded(batch_id: str) -> list[str]:
    """找出「我們已知 MOPS 回空白頁,agent 卻在 G1 記 `miss`」的案子。

    這個錯誤的後果很具體:`NO_NEWS_EXISTS` 的窮盡條件第 4 條要求 T1 層不得有
    `blocked`,記成 `miss` 會讓該案一路通過,產出「這家公司當年真的沒有消息」——
    而真相是那份資料根本不在系統裡。清單以**實測**為準,不用「後來下市」去推:
    實測 40 檔已下市樣本中有 4 檔照樣查得到,推論會誤殺那 4 檔。
    """
    if not G1_UNAVAIL.exists():
        return []
    known = {(r["code"], r["d0"]) for r in
             json.loads(G1_UNAVAIL.read_text(encoding="utf-8"))}
    bad = []
    for code, d0 in _cases(batch_id):
        log = NEWS / f"{code}_{d0}" / "retrieval_log.jsonl"
        if (code, d0) not in known or not log.exists():
            continue
        for line in log.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("gate") == "G1" and row.get("status") == "miss":
                bad.append(f"{code}@{d0}:G1 記為 miss,實測應為 blocked")
    return bad


def release(batch_id: str, *, force: bool = False) -> list[Path]:
    """階段 A 全批落檔後,才把該批真相寫進 agent 看得到的 `_truth/`。

    「結案」= **四張卡齊備**(thin 兩張 + deep 兩張),**或**有 `voided.json`(觸發鐵律、判定機械性事件、
    完全無法檢索)。作廢也算結案是必要的:提示詞明文允許 agent 作廢一檔,若閘門仍
    要求全員到齊,該批永遠釋不出來——閘門與提示詞的契約必須是同一份。反過來,作廢
    **必須留檔**,不能靜默跳過:靜默跳過會讓分母悄悄變小,批次統計失真而毫無跡象。

    fail-closed:任一檔未結案即整批拒絕。理由是不對稱——早釋出一次,該批的盲判永久
    失效且**事後看不出來**(卡片與報告長得一模一樣);晚釋出只是多跑一次指令。
    """
    truth = json.loads((TRUTH / f"{batch_id}.json").read_text(encoding="utf-8"))
    by_case = {(t["code"], t["d0"]): t for t in truth}
    missing: list[str] = []
    stamps: list[float] = []
    order_bad: list[str] = []
    for code, d0 in _cases(batch_id):
        cdir = NEWS / f"{code}_{d0}"
        if (cdir / "voided.json").exists():
            stamps.append((cdir / "voided.json").stat().st_mtime)
            continue
        for name in _STAGE_A:
            f = cdir / name
            if not f.exists():
                missing.append(str(f))
            else:
                stamps.append(f.stat().st_mtime)
        # thin 必須早於 deep。反過來就代表淺判斷是在看過深度證據之後補寫的,
        # 而 thin/deep 的比較整個建立在「thin 那一刻真的只有那些材料」之上。
        # 這種汙染事後從檔案內容完全看不出來,只有時序看得出來。
        for a, b in (("ex_ante_thin_d_prev.json", "ex_ante_d_prev.json"),
                     ("ex_ante_thin_d0.json", "ex_ante_d0.json")):
            fa, fb = cdir / a, cdir / b
            if fa.exists() and fb.exists() and fa.stat().st_mtime > fb.stat().st_mtime:
                order_bad.append(f"{code}@{d0}:{a} 晚於 {b}")
    if order_bad:
        raise RuntimeError(
            f"{batch_id} 的標記日模擬晚於考掘落檔,拒絕釋出真相:\n  " + "\n  ".join(order_bad)
            + "\n  thin 戳必須在九格開跑前蓋下。順序反了就代表淺判斷是看過深度證據後補寫的,"
              "thin/deep 的比較整個失效——而那是本研究用來回答「標記日該不該加深查證」的唯一依據。")
    if mis := _g1_misrecorded(batch_id):
        raise RuntimeError(
            f"{batch_id} 的 G1 記錄與實測矛盾,拒絕釋出真相:\n  " + "\n  ".join(mis)
            + "\n  這些案的 MOPS 查詢**實測回空白頁**(來源不保留已下市公司的公告),"
              "應記 `blocked` 而非 `miss`——記成 miss 會讓 `NO_NEWS_EXISTS` 的窮盡條件"
              "第 4 條被繞過,產出假的「這家公司當年真的沒有消息」。")
    if missing and not force:
        raise RuntimeError(
            f"{batch_id} 階段 A 未完成,拒絕釋出真相(缺 {len(missing)} 個檔案;"
            f"該檔若已作廢,請寫 voided.json 而非留空):\n  "
            + "\n  ".join(missing[:6]) + ("\n  …" if len(missing) > 6 else ""))
    REVEAL.mkdir(parents=True, exist_ok=True)
    out = []
    for (code, d0), t in by_case.items():
        p = REVEAL / f"{code}_{d0}.json"
        p.write_text(json.dumps(t, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        out.append(p)
    # mtime 順序是事後稽核的唯一物證:真相檔必須晚於所有 ex_ante 檔。
    if stamps and min(p.stat().st_mtime for p in out) < max(stamps):
        raise RuntimeError(f"{batch_id} 真相檔早於階段 A 落檔,該批作廢")
    return out


# ---------------------------------------------------------------- 蒸餾期別切分

def era_split() -> tuple[list[str], list[str]]:
    """保留組 = **每個漲跌幅世代各自最近的那個期別**(實測 ≈24% 樣本)。

    純日期規則,不看實際批數——`--pilot` 只建部分批次,若拿批內卡片去數樣本,
    試跑與正式跑會選出不同的保留組,而保留組必須在蒸餾開始前就固定下來。

    為什麼是「最近」而不是「最大」:
    - 最近的期別離下游標記期(2022-07 起)最近,在它身上能複製,才是最強的移植證據。
    - 取最大的 7% 期別會把 2008-2010 整段拿去當保留組——**崩跌 regime 的樣本全在
      那裡**,蒸餾就再也看不到崩跌,而哲學第十道判別正是宏觀 regime 時點。保留組
      的檢力不值得用「蒸餾看不到最稀缺的那類樣本」去換。

    兩種會讓檢驗形同虛設的填法也一併排除:同世代挑兩個(測不到跨世代移植)、
    只挑一個(單世代通過說明不了移植)。
    """
    m = json.loads((CARDS / "_era_map.json").read_text(encoding="utf-8"))
    limit = Date(2015, 6, 1)                       # 漲跌幅 7% → 10% 的市場微結構斷點
    start = {k: Date.fromisoformat(v.split("~")[0]) for k, v in m.items()}
    hold = [max((k for k in m if start[k] >= limit), key=lambda k: start[k]),
            max((k for k in m if start[k] < limit), key=lambda k: start[k])]
    return [k for k in sorted(m) if k not in hold], hold


def render_era_brief(era_code: str) -> str:
    """年代語境卡的前置步驟——每個期別跑一次,在該期別任何歸因批次啟動之前。

    這是唯一一份**需要知道實際年份**的提示詞:重建當年語彙就得去讀當年的報導。
    密封在這裡不適用,也不必適用——它不碰任何個股、不做任何判別,產物是純語境。
    """
    m = json.loads((CARDS / "_era_map.json").read_text(encoding="utf-8"))
    if era_code not in m:
        raise ValueError(f"未知期別 {era_code}(已知:{sorted(m)})")
    start, end = m[era_code].split("~")
    return _fill(_strip_header((DRAFT / "PROMPT_ev58_era_brief.md").read_text(encoding="utf-8")),
                 era_code=era_code, era_key=era_code, era_start=start, era_end=end,
                 news_root=str(NEWS))


def render_distill() -> str:
    d, h = era_split()
    return _fill(_strip_header((DRAFT / "PROMPT_ev58_distill.md").read_text(encoding="utf-8")),
                 distill_eras="、".join(d), holdout_eras="、".join(h))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=("render", "release", "distill", "era-brief"))
    ap.add_argument("batch_id", nargs="?", help="render/release 用批次號;era-brief 用期別碼")
    ap.add_argument("--force", action="store_true", help="繞過階段 A 完成檢查(只供測試)")
    a = ap.parse_args()

    if a.cmd == "era-brief":
        m = json.loads((CARDS / "_era_map.json").read_text(encoding="utf-8"))
        eras = [a.batch_id] if a.batch_id else sorted(m)
        out = CARDS / "_prompts"
        out.mkdir(exist_ok=True)
        for e in eras:
            (out / f"era_brief_{e}.md").write_text(render_era_brief(e), encoding="utf-8")
            print(f"→ {out / f'era_brief_{e}.md'}")
        return
    if a.cmd == "distill":
        d, h = era_split()
        out = CARDS / "_prompts"
        out.mkdir(exist_ok=True)
        (out / "distill.md").write_text(render_distill(), encoding="utf-8")
        print(f"蒸餾期別 {d} / 保留期別 {h} → {out / 'distill.md'}")
        return
    if not a.batch_id:
        ap.error("render / release 需要 batch_id")
    if a.cmd == "render":
        pa, bs, bc = render_attribution(a.batch_id)
        out = CARDS / a.batch_id / "prompt"
        out.mkdir(exist_ok=True)
        (out / "phase_a.md").write_text(pa, encoding="utf-8")
        (out / "phase_b_surge.md").write_text(bs, encoding="utf-8")
        (out / "phase_b_control.md").write_text(bc, encoding="utf-8")
        left = sorted(set(re.findall(r"\{[a-z_]+\}", pa + bs + bc)))
        print(f"→ {out}(階段 A {len(pa.splitlines())} 行);未填變數:{left or '無'}")
        return
    for p in release(a.batch_id, force=a.force):
        print(f"釋出 {p.name}")


if __name__ == "__main__":
    main()
