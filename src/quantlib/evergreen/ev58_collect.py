"""EV58 機制卡匯總 + 去識別掃描——提示詞承諾的那支「程式」。

歸因提示詞白紙黑字寫著:

    「`class`:程式代入,你不要自己填」
    「程式會用該案卷宗自動導出黑名單並以正則掃描,命中即退回重做」

repo 裡沒有這支程式。後果不是不方便,是**整個密封只剩 agent 自律**——蒸餾器一旦
從機制卡認出公司或年代,它對台股那幾年的記憶就會灌進判斷,而這正是這次重做要根除
的東西(上一版哲學就是在「蒸餾期 = 標記期」的地基上寫成的)。而且失效完全無聲:
洩漏的卡片長得跟乾淨的一模一樣。

## 這支做什麼

1. **導出每案的黑名單**——從該案自己的卷宗(`case_record.json` / `_identity/{code}.json`)
   取公司今名、當年名、英文名、代碼、同業名單,加上該案的實際年月。黑名單是**逐案**
   導出的,不是一份全域清單:全域清單會漏掉「這一案才提到的那家客戶」。
2. **正則掃描機制卡**——命中即該案退回重做,**不進蒸餾語料**。掃四類:任何四位數字
   代碼、任何 `19xx`/`20xx` 年份、黑名單詞、漲跌幅世代字樣(`7%`/`10%` 上限——
   「7%」等於直說站位在 2015-06 之前,年份禁令蓋不到它)。
3. **代入 `alias` 與 `class`**——由真相檔供給,agent 不經手。假名首字母即類別
   (`S`=正例、`C`=近乎成功的負例、`Q`=安靜的負例),這在蒸餾階段是**刻意公開**的:
   蒸餾器要寫的就是判別規則,不知道類別無從寫起。
4. **按期別分檔輸出**,讓「第一趟只准讀蒸餾期別」在檔案層面就是物理的。

Run:
  uv run --project . python -m quantlib.evergreen.ev58_collect            # 掃描 + 匯總
  uv run --project . python -m quantlib.evergreen.ev58_collect --scan-only
依賴 cache: 否。
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from quantlib import paths
from quantlib.evergreen.ev58_build_cards import TRUTH
from quantlib.evergreen.ev58_prompt import NEWS

CORPUS = paths.OUT / "ev58_corpus"
#: 四位數字代碼與年份——這兩類不需要卷宗就能掃,是最後一道通用網。
_CODE = re.compile(r"(?<!\d)\d{4}(?!\d)")
_YEAR = re.compile(r"(?:19|20)\d{2}")
#: 漲跌幅世代。年份禁令蓋不到它,但它同樣直接定位年代。
_LIMIT = re.compile(r"漲跌幅[^。]{0,8}(?:7|10)\s*%|(?:7|10)\s*%\s*(?:的)?(?:單日)?(?:漲跌幅|上限)")


def _blacklist(case_dir: Path, code: str, d0: str) -> set[str]:
    """從該案自己的卷宗導出黑名單。

    逐案而非全域:全域清單只有樣本自己的名字,漏掉「這一案才提到的那家客戶/同業」,
    而供應鏈敘事裡最容易指認公司的往往正是那些名字。
    """
    bl = {code, d0[:4], f"{d0[:4]}-{d0[5:7]}"}
    for f in (case_dir / "case_record.json", NEWS / "_identity" / f"{code}.json"):
        if not f.exists():
            continue
        blob = json.loads(f.read_text(encoding="utf-8"))
        for s in _strings(blob):
            # 只收看起來像專名的短字串:長句子進黑名單會讓掃描全案誤殺。
            if 2 <= len(s) <= 24 and not s.startswith("http"):
                bl.add(s)
    return {b for b in bl if b}


def _strings(x) -> list[str]:
    if isinstance(x, str):
        return [x]
    if isinstance(x, dict):
        # 只挖與身分有關的欄位;把整份卷宗攤平會把敘述句也收進黑名單。
        keys = ("name", "name_then", "name_today", "name_en", "english_name", "aliases",
                "peers", "customers", "suppliers", "company", "ticker", "code")
        return [s for k, v in x.items() if any(t in k.lower() for t in keys)
                for s in _strings(v)]
    if isinstance(x, list):
        return [s for i in x for s in _strings(i)]
    return []


def scan(card: dict, bl: set[str]) -> list[str]:
    """回傳命中的洩漏原因;空清單代表乾淨。"""
    txt = json.dumps(card, ensure_ascii=False)
    hits = []
    if m := _CODE.findall(txt):
        hits.append(f"四位數字代碼:{sorted(set(m))[:5]}")
    if m := _YEAR.findall(txt):
        hits.append(f"年份:{sorted(set(m))[:5]}")
    if m := _LIMIT.findall(txt):
        hits.append(f"漲跌幅世代:{m[:3]}")
    named = sorted({b for b in bl if len(b) >= 2 and b in txt})
    if named:
        hits.append(f"卷宗專名:{named[:5]}")
    return hits


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan-only", action="store_true", help="只掃描不輸出語料")
    a = ap.parse_args()

    truth = {}
    for f in sorted(TRUTH.glob("batch_*.json")):
        for t in json.loads(f.read_text(encoding="utf-8")):
            truth[(t["code"], t["d0"])] = t
    if not truth:
        raise SystemExit(f"找不到真相檔({TRUTH});先跑 ev58_build_cards")

    clean: dict[str, list[dict]] = {}
    rework: list[dict] = []
    absent = 0
    for (code, d0), t in sorted(truth.items()):
        cdir = NEWS / f"{code}_{d0}"
        mc = cdir / "mechanism_card.json"
        if (cdir / "voided.json").exists():
            continue                                    # 作廢檔本就不進語料
        if not mc.exists():
            absent += 1
            continue
        card = json.loads(mc.read_text(encoding="utf-8"))
        hits = scan(card, _blacklist(cdir, code, d0))
        if hits:
            rework.append({"code": code, "d0": d0, "reasons": hits})
            continue
        # 假名與類別由程式代入,agent 不經手——提示詞就是這麼承諾的。
        card["alias"], card["class"] = t["alias"], (
            "surge" if t["arm"] == "positive" else t.get("neg_kind") or "quiet")
        card["era_code"] = t["era_code"]
        clean.setdefault(t["era_code"], []).append(card)

    n = sum(len(v) for v in clean.values())
    print(f"機制卡 {n + len(rework)} 張已產出(尚缺 {absent} 張未做);"
          f"乾淨 {n}、退回重做 {len(rework)}")
    for r in rework[:10]:
        print(f"  ✗ {r['code']}@{r['d0']}:{'; '.join(r['reasons'])}")
    if len(rework) > 10:
        print(f"  …另 {len(rework) - 10} 張")
    if a.scan_only:
        return

    CORPUS.mkdir(parents=True, exist_ok=True)
    (CORPUS / "_rework.json").write_text(
        json.dumps(rework, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for era, cards in sorted(clean.items()):
        # 按期別分檔,讓「第一趟只准讀蒸餾期別」在檔案層面就是物理的,
        # 而不是叫蒸餾器自己避開某些條目。
        (CORPUS / f"{era}.json").write_text(
            json.dumps(cards, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"  {era}: {len(cards)} 張 → {CORPUS / f'{era}.json'}")


if __name__ == "__main__":
    main()
