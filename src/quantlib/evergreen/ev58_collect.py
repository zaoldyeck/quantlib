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
   (`S`=正例、`N`=配對負例),這在蒸餾階段是**刻意公開**的:
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
#: 四位數字。**不能單憑位數判定是股票代碼**——第一版這麼做,實測會把
#: 「1500 萬元」「2000 人」「3000 片」全部判成洩漏,而機制卡本來就會有金額、
#: 產能、人數。攔到九成正常文字的網不是網,是牆:它會讓幾乎每張卡退回重做、
#: 零張進蒸餾語料,而且發生在燒完全部歸因成本**之後**。
#: 改為兩個條件同時成立才算:①該數字是**實際存在的上市櫃代碼**(查 cache,不是猜);
#: ②它**沒有被量詞或單位跟隨**(有單位的是數量,不是代碼)。
_CODE = re.compile(r"(?<![\d.])(\d{4})(?![\d.])")
#: 量詞與單位——跟在數字後面就代表那是數量。這不是窮舉清單的問題:漏一個單位只會
#: 讓一個數量被誤判成代碼(該案退回重做,可修),而多列一個單位會讓真的代碼漏過去
#: (無聲洩漏,不可修)。**不對稱,所以寧可漏列**。
#: **年/月/日/季刻意不列**:四位數字後面接「年」是日曆年份(2016 年),不是時長
#: ——時長是一兩位數(「3 年內」)。列進去會讓「2016 年的產業循環」這種真洩漏放行,
#: 而那正是上面那條不對稱原則講的「多列一個單位 ⇒ 無聲洩漏」。實測踩過。
_UNIT = re.compile(r"^\s*(?:萬|億|千|百|元|人|片|家|座|條|台|噸|股|張|%|％|"
                   r"個|次|倍|坪|平方|公噸|美元|美金|新台幣|K|M|GW|MW|kg|mm|nm|吋|寸)")
#: 年份。與代碼同一個問題:「2000 人」「1998 元」的數字剛好落在年份區間。
#: 同樣用「後面有沒有跟單位」判定,規則只寫一次(`_is_quantity`)。
_YEAR = re.compile(r"(?<![\d.])((?:19|20)\d{2})(?![\d.])")
#: 漲跌幅世代。年份禁令蓋不到它,但它同樣直接定位年代。
#: 漲跌幅世代。年份禁令蓋不到它,但它同樣直接定位年代。
#: **3.5% 與「減半」也要擋**:2008-10-13~10-24 台股跌幅臨時減半為 3.5%,
#: 寫出這個數字或「跌幅減半」等於把年代釘死到那兩週——比寫「7%」更精確地洩漏。
#: 由 E1 年代語境卡回報、自資料驗證後補入。
_LIMIT = re.compile(
    r"漲跌幅[^。]{0,8}(?:3\.5|7|10)\s*%|(?:3\.5|7|10)\s*%\s*(?:的)?(?:單日)?(?:漲跌幅|上限)"
    r"|跌幅[^。]{0,4}減半|減半[^。]{0,4}跌幅")
#: 民國紀年。`_YEAR` 只認 19xx/20xx,對這批卡片幾乎沒有防禦力——當年的一手材料
#: (重大訊息、年報、公開說明書)**全部以民國紀年書寫**,agent 逐字引用時帶進來的
#: 是「100/03/22」「99 年度」而不是西元年,一個字都不會被舊規則攔到。這不是理論
#: 風險:本批第一張機制卡的來源卡上就有十餘處。
#: 判準:年碼限 8x/9x/1xx(民國 80~149 年),月日各自限合法範圍——「80/20 法則」
#: 因 20 不是合法月份而放行,「3 年」因不足兩位而放行。
_ROC_DATE = re.compile(
    r"(?<![\d.])(?:8\d|9\d|1[0-4]\d)\s*[/年]\s*(?:0?[1-9]|1[0-2])"
    r"(?:\s*[/月]\s*(?:0?[1-9]|[12]\d|3[01]))?")
_ROC_YEAR = re.compile(r"(?<![\d.])(?:8\d|9\d|1[0-4]\d)\s*年度?(?![\d])")
#: 月份。鐵律明文禁的是「年份**或月份**」,而月份同樣定位得到年代區段
#: (「三月那批報導」+ 期別碼 = 直接定位到某個季度)。時長寫法(「三個月」「6 個月」)
#: 因中間有量詞「個」而自然放行,不需要例外清單。
#: 唯一需要例外的是**任指詞**:「任一月營收」「每一月」的「一」屬於前面的「任/每」,
#: 不是一月。這個寫法在證偽條件裡極常見(本批第一案四份 bet 就有兩份),不排除會
#: 讓乾淨的卡片被系統性退回——而規模化的誤報正是本模組 docstring 警告的那種「牆」。
_MONTH = re.compile(r"(?<![任某每逐唯])(?:[一二三四五六七八九]|十[一二]?)月(?![份薪])"
                    r"|(?<![\d.])(?:0?[1-9]|1[0-2])\s*月(?![份薪])")

#: 被**刻意公開**的密封欄位:期別碼與遮罩後的卡片編號。它們的 key 含 "code",
#: 若跟著身分欄位一起進黑名單,會把「機制卡必須寫 `E4`」變成「機制卡一定被退回」
#: ——守護反過來擋住正確答案,而且退回理由看起來跟真洩漏一模一樣。
_SEALED_KEYS = {"era_code", "code_masked"}
#: 身分欄位的 key 特徵。命中者的**值**進黑名單;不命中者仍要往下走
#: (見 `_strings` 的註解)。
#: **「name」刻意不列**:光憑一個 `name` 判不出那是公司還是別的東西——機制卡自己的
#: `novel_features[].name`(特徵名)就會被誤收,而特徵名本來就該同時出現在卷宗與
#: 機制卡上,於是每張卡都因為「寫了自己的特徵名」被退回。改由 `_is_identity_key`
#: 判定:複合的 name 欄位(`name_today`/`company_name`)一律算身分;**光禿禿的
#: `name` 只有在同一個 dict 裡還帶著代碼時才算**(同業條目的形態)。
_IDENTITY_KEYS = ("aliases", "peers", "customers", "suppliers",
                  "company", "ticker", "code")


def _is_identity_key(key: str, siblings: dict) -> bool:
    k = key.lower()
    if k in _SEALED_KEYS:
        return False
    if any(t in k for t in _IDENTITY_KEYS):
        return True
    if "name" not in k:
        return False
    if k != "name":
        return True                       # name_today / name_then / english_name…
    return any("code" in s.lower() or "ticker" in s.lower() for s in siblings)


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


def _strings(x, collect: bool = False) -> list[str]:
    """挖出卷宗裡的身分字串:**只收**身分欄位的值,但**照樣往下走**每一層。

    「只收」與「只走」是兩件事,第一版把它們寫成同一件,結果整個黑名單是空的:
    提示詞規定的 `case_record.json` 形態是 `{"identity_card": {"name_today": ...}}`,
    而 `identity_card` 這個 key 不含任何身分字樣,於是遞迴在最外層就停住,公司名、
    當年名、同業名單一個都沒進黑名單——掃描器照樣回報「乾淨」。這是本模組
    docstring 自己警告的那種無聲失效:洩漏的卡片長得跟乾淨的一模一樣,而現在
    連守門的都說它乾淨。

    修法:遞迴永遠往下,`collect` 旗標一旦在某層被身分 key 打開,該子樹的字串
    全收(同業條目是 `{"code":..., "name":...}` 這種巢狀結構,必須整棵收)。
    """
    if isinstance(x, str):
        return [x] if collect else []
    if isinstance(x, dict):
        return [s for k, v in x.items()
                for s in _strings(v, collect or _is_identity_key(k, x))]
    if isinstance(x, list):
        return [s for i in x for s in _strings(i, collect)]
    return []


def _is_quantity(txt: str, m: re.Match) -> bool:
    """該數字是不是「數量」而非識別碼——判準是後面有沒有跟單位。

    規則只寫一次,代碼與年份共用:兩者是同一個問題(數字剛好落在該區間)。
    """
    return bool(_UNIT.match(txt[m.end():]))


def listed_codes() -> set[str]:
    """實際存在的四碼上市櫃代碼——用來把「代碼」與「數量」分開。

    查 cache 而不是用規則猜:台股代碼的號段沒有一條乾淨的規則,而猜錯的兩個方向
    代價不對稱(見 `_UNIT` 註解)。
    """
    from quantlib.apex import data
    con = data.connect()
    q = con.sql("SELECT DISTINCT company_code FROM daily_quote "
                "WHERE length(company_code) = 4").pl()
    return set(q["company_code"].to_list())


def scan(card: dict, bl: set[str], codes: set[str] | None = None) -> list[str]:
    """回傳命中的洩漏原因;空清單代表乾淨。

    `codes` 為實際上市櫃代碼集合;省略時退化為「只靠卷宗黑名單 + 年份」,
    仍能擋住本案自己的代碼(那是最主要的洩漏面),只是擋不到不相干的第三家公司。
    """
    txt = json.dumps(card, ensure_ascii=False)
    hits = []
    suspect = [m.group(1) for m in _CODE.finditer(txt)
               if (codes is None or m.group(1) in codes) and not _is_quantity(txt, m)]
    if suspect:
        hits.append(f"四位數字代碼:{sorted(set(suspect))[:5]}")
    years = [m.group(1) for m in _YEAR.finditer(txt) if not _is_quantity(txt, m)]
    if years:
        hits.append(f"年份:{sorted(set(years))[:5]}")
    if m := _LIMIT.findall(txt):
        hits.append(f"漲跌幅世代:{m[:3]}")
    if m := _ROC_DATE.findall(txt):
        hits.append(f"民國紀年日期:{sorted(set(m))[:5]}")
    if m := _ROC_YEAR.findall(txt):
        hits.append(f"民國年度:{sorted(set(m))[:5]}")
    if m := _MONTH.findall(txt):
        hits.append(f"月份:{sorted(set(m))[:5]}")
    # 黑名單裡的**純數字**(本案代碼)也要走同一個數量判定,否則「資本額 2330 萬元」
    # 會因為子字串比對而誤報——而黑名單的誤報最毒:它會讓「真的有洩漏」與「剛好
    # 撞到一個數字」在報表上長得一樣,現場久了就不再認真看它。
    named = []
    for b in bl:
        if len(b) < 2 or b not in txt:
            continue
        if b.isdigit() and len(b) == 4:
            if all(_is_quantity(txt, m) for m in re.finditer(re.escape(b), txt)):
                continue
        named.append(b)
    if named:
        hits.append(f"卷宗專名:{sorted(named)[:5]}")
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

    codes = listed_codes()
    print(f"上市櫃四碼代碼 {len(codes):,} 個(用來把代碼與數量分開)")
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
        hits = scan(card, _blacklist(cdir, code, d0), codes)
        if hits:
            rework.append({"code": code, "d0": d0, "reasons": hits})
            continue
        # 假名與類別由程式代入,agent 不經手——提示詞就是這麼承諾的。
        # 負例不再分 near_miss/quiet 兩檔(那兩個帶的邊界沒有出處);硬度改由配對後
        # 的實際報酬分布呈現,見統計表。故只有兩類。
        card["alias"], card["class"] = t["alias"], (
            "surge" if t["arm"] == "positive" else "control")
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
