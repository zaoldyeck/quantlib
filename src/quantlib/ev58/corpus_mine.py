"""EV58 期別語境卡:當年標題語料的詞彙考掘(corpus mine)。

為什麼要這支
------------
語境卡的 ``vocabulary`` 一節,規格明寫「取得方式是去讀當年的實際報導,**不是憑
印象回想**」。憑印象回想的失敗模式很隱蔽:寫出來的詞看起來很像那個年代,但當年
的報導其實用另一個講法,下游拿它去搜就是全空,再把全空誤讀成「當年沒消息」。

本模組把「讀當年的報導」變成可重跑的機械流程:把 ``archive_probe`` 已經抓下來的
當年標題(URL 內嵌刊出日,故日期可被第三者驗證)當語料,做兩件事:

1. ``ngrams`` — 對指定年份區間的標題做中文 n-gram 詞頻,把**當年高頻但今天不這麼
   講**的詞逼出來。詞是從語料長出來的,不是我想出來的。
2. ``grep`` — 給定候選詞,列出當年實際用到它的標題與連結,直接產出 evidence。

不需要 cache.duckdb(純讀 ``_probe_cache`` 的 TSV)。

用法
----
    uv run --project . python -m quantlib.ev58.corpus_mine ngrams --lo 2008-01-01 --hi 2010-12-31 --n 3 --top 200
    uv run --project . python -m quantlib.ev58.corpus_mine grep  --lo 2008-01-01 --hi 2010-12-31 --terms "陸資|中概|三通"
    uv run --project . python -m quantlib.ev58.corpus_mine stats
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from datetime import date as _date, datetime as _dt
from pathlib import Path

from quantlib import paths

CACHE_DIR = paths.OUT / "ev58_news" / "_era_brief" / "_probe_cache"

_CJK = re.compile(r"[一-鿿]+")
_FNAME = re.compile(r"^(\w+?)_(\d{4}-\d{2}-\d{2})\.tsv$")

# 標題語料含大量社會/文化新聞;詞彙考掘只要財經面。命中任一即視為財經標題。
FIN_HINT = (
    "股|盤|指數|外資|法人|融資|融券|漲|跌|營收|獲利|財報|景氣|經濟|金融|銀行|保險|券商|"
    "電子|科技|半導體|晶圓|面板|記憶體|DRAM|IC|LED|太陽能|生技|鋼|塑化|水泥|航運|汽車|"
    "原物料|油價|黃金|匯率|美元|台幣|人民幣|利率|降息|升息|債|基金|ETF|上市|上櫃|興櫃|"
    "投資|市場|訂單|報價|產能|出貨|庫存|裁員|無薪假|失業|GDP|CPI|貿易|出口|進口|ECFA|"
    "陸資|台商|兩岸|MOU|三通|消費券|紓困|救市|國安基金|熱錢|通膨|通縮|升值|貶值|標售|購併"
)
_FIN = re.compile(FIN_HINT)


def _rows(sites: tuple[str, ...] = ("epochtimes", "libertytimes", "pttStock")) -> list[tuple[str, str, str]]:
    """(date, url, title) — 讀所有已快取的當年標題語料,依 URL 去重。

    去重是必要的而非潔癖:PTT 的抓取以「頁碼」為單位,相鄰取樣日的頁區間會重疊,
    同一篇文章因此出現在多個檔案裡。不去重的話詞頻會被重複計數,「當年高頻詞」
    就變成「被我抓了幾次的詞」。PTT 的日期一律取檔名內嵌的發文 epoch(權威),
    不取抓取用的取樣日(那只是頁碼二分搜尋的起點,可能差好幾週)。
    """
    seen: set[str] = set()
    out: list[tuple[str, str, str]] = []
    for p in sorted(CACHE_DIR.glob("*.tsv")):
        m = _FNAME.match(p.name)
        if not m or m.group(1) not in sites:
            continue
        d = m.group(2)
        for ln in p.read_text(errors="replace").splitlines():
            parts = ln.split("\t")
            if len(parts) < 2:
                continue
            if m.group(1) == "pttStock":  # epoch \t url \t title
                if len(parts) < 3:
                    continue
                url, title = parts[1], parts[2]
                d = _dt.utcfromtimestamp(int(parts[0]) + 8 * 3600).strftime("%Y-%m-%d")
            else:  # url \t title
                url, title = parts[0], parts[1]
            title = title.replace(" | 大紀元", "").strip()
            if not title or title.startswith("<ERR") or url in seen:
                continue
            seen.add(url)
            out.append((d, url, title))
    # snaps/ 是入口網首頁快照(標題 \t 連結),密度最高,一併納入
    snaps = CACHE_DIR / "snaps"
    if snaps.exists():
        for p in sorted(snaps.glob("*.tsv")):
            m = _FNAME.match(p.name)
            if not m:
                continue
            for ln in p.read_text(errors="replace").splitlines():
                parts = ln.split("\t")
                if len(parts) < 2 or not parts[0].strip() or parts[1] in seen:
                    continue
                seen.add(parts[1])
                out.append((m.group(2), parts[1], parts[0].strip()))
    return out


def load(lo: str, hi: str, fin_only: bool = True) -> list[tuple[str, str, str]]:
    a, b = _date.fromisoformat(lo), _date.fromisoformat(hi)
    return [
        r
        for r in _rows()
        if a <= _date.fromisoformat(r[0]) <= b and (not fin_only or _FIN.search(r[2]))
    ]


def ngrams(rows: list[tuple[str, str, str]], n: int) -> Counter:
    c: Counter = Counter()
    for _, _, title in rows:
        for chunk in _CJK.findall(title):
            for i in range(len(chunk) - n + 1):
                c[chunk[i : i + n]] += 1
    return c


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["ngrams", "grep", "stats", "dump", "hits", "evidence", "snapev"])
    ap.add_argument("--lo", default="2008-01-01")
    ap.add_argument("--hi", default="2010-12-31")
    ap.add_argument("--n", type=int, default=3)
    ap.add_argument("--top", type=int, default=150)
    ap.add_argument("--min-count", type=int, default=3)
    ap.add_argument("--terms", default=None, help="以 | 分隔的候選詞")
    ap.add_argument("--per-term", type=int, default=2)
    ap.add_argument("--all", action="store_true", help="不做財經標題過濾")
    a = ap.parse_args(argv)

    rows = load(a.lo, a.hi, fin_only=not a.all)
    if a.cmd == "stats":
        by_year: Counter = Counter(r[0][:4] for r in rows)
        print(f"rows={len(rows)}  years={dict(sorted(by_year.items()))}")
        print(f"dates={len({r[0] for r in rows})}")
        return 0
    if a.cmd == "dump":
        for d, u, t in rows:
            print(f"{d}\t{u}\t{t}")
        return 0
    if a.cmd == "snapev":
        # 入口網文章頁常常沒有被存檔(只有首頁快照被存),此時證據應該指向**首頁快照本身**
        # ——它一定存在(是我們自己抓下來的那一頁),且該日該詞確實印在上面。
        from quantlib.ev58.corpus_expand import PORTALS

        snaps = CACHE_DIR / "snaps"
        stamp_re = re.compile(r"/web/(\d{14})/")
        for term in (a.terms or "").split("|"):
            if not term:
                continue
            for f in sorted(snaps.glob("*.tsv")):
                m = _FNAME.match(f.name)
                if not m or m.group(1) not in PORTALS:
                    continue
                text = f.read_text(errors="replace")
                titles = [ln.split("\t")[0] for ln in text.splitlines() if term in ln.split("\t")[0]]
                sm = stamp_re.search(text)
                if not titles or not sm:
                    continue
                home = f"https://web.archive.org/web/{sm.group(1)}/{PORTALS[m.group(1)]}"
                print(f"{term}\t{sm.group(1)[:8]}\t{home}\t{titles[0][:60]}")
        return 0
    if a.cmd == "evidence":
        # 每個候選詞挑幾條當證據:優先 URL 內嵌刊出日的新聞站,標題短者優先(標題長的多是首頁聚合)
        pref = ("epochtimes.com", "libertytimes.com.tw", "cnyes.com", "ithome.com.tw")
        for term in (a.terms or "").split("|"):
            if not term:
                continue
            hit = [r for r in rows if term in r[2]]
            hit.sort(key=lambda r: (0 if any(p in r[1] for p in pref) else 1, len(r[2])))
            for d, u, t in hit[: a.per_term]:
                print(f"{term}\t{d}\t{u}\t{t}")
            if not hit:
                print(f"{term}\tNOHIT")
        return 0
    if a.cmd == "hits":
        # 一次量一批候選詞的命中數與最早/最晚出現日 —— 詞是不是「當年通用」看得出來
        for term in (a.terms or "").split("|"):
            if not term:
                continue
            hit = [r for r in rows if term in r[2]]
            span = f"{min(r[0] for r in hit)}~{max(r[0] for r in hit)}" if hit else "-"
            print(f"{len(hit)}\t{term}\t{span}")
        return 0
    if a.cmd == "grep":
        terms = (a.terms or "").split("|")
        for d, u, t in rows:
            if any(x and x in t for x in terms):
                print(f"{d}\t{u}\t{t}")
        return 0
    c = ngrams(rows, a.n)
    for g, k in c.most_common(a.top):
        if k < a.min_count:
            break
        print(f"{k}\t{g}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
