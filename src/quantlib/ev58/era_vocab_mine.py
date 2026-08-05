"""EV58 期別語境卡:當年用語的探勘與驗證(vocabulary mine / verify)。

為什麼要這支
------------
語境卡最重的一欄是 vocabulary——「今天這樣講、當年那樣講」。它有兩種寫法:

1. 憑印象回想那個年代流行什麼詞 → 寫出來的是**我對那年代的刻板印象**,而且
   永遠只想得到「事後證明有戲」的那幾個(倖存者偏誤),下游拿去搜等於帶著答案
   回到過去。
2. 把當年逐日的新聞標題整包拿下來,**數哪些詞真的高頻**,再回頭找一則實際用到
   它的報導當出處 → 詞是量出來的,連結是驗過的。

本模組做第二種。輸入是 :mod:`quantlib.ev58.cnyes_probe` 抓下來的逐日語料,輸出:

- ``mine``   : 標題字元 n-gram 的頻次榜(依文件頻率過濾),用來**發現**沒想到的詞。
- ``rise``   : 同一詞在兩段期間的頻次對比,用來分辨「這詞是這個年代才有的」還是
               「每個年代都在講」——語境卡要的是前者。
- ``verify`` : 讀一份語境卡,對每個 ``then`` 詞在語料裡找出處(最早一則 + 標題命中
               優先),輸出 TSV。查無命中者即「這個詞當年沒人這樣用」,該刪。

不需要 cache.duckdb(讀本地語料)。

用法
----
    uv run --project . python -m quantlib.ev58.era_vocab_mine mine   --from 2020-01-01 --to 2021-12-31 --n 3 --top 200
    uv run --project . python -m quantlib.ev58.era_vocab_mine rise   --terms 航海王,晶片荒 --a 2020-01-01:2020-12-31 --b 2021-01-01:2021-12-31
    uv run --project . python -m quantlib.ev58.era_vocab_mine verify --brief var/out/ev58_news/_era_brief/E5.json --from 2020-01-01 --to 2021-12-31
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import date as _date
from pathlib import Path

from quantlib.ev58 import cnyes_probe

# 只留中文字;數字、英數與標點會把 n-gram 榜洗成「2月營收月增」這類模板噪音
_HAN = re.compile(r"[一-鿿]+")
# 每天盤後稿的固定模板詞,對「當年說法」零資訊
_STOP = {
    "營收", "月增", "年增", "月減", "年減", "億元", "萬元", "法說", "公司", "今日", "昨日",
    "台股", "美股", "上漲", "下跌", "收盤", "開盤", "盤中", "股價", "個股", "指數",
}


def _titles(a: _date, b: _date, cat: str = cnyes_probe.DEFAULT_CAT):
    for _, r in cnyes_probe.iter_corpus(a, b, cat):
        yield r


def cmd_mine(a: _date, b: _date, n: int, top: int, min_df: int, cat: str) -> None:
    tf: Counter[str] = Counter()
    df: Counter[str] = Counter()
    ndoc = 0
    for r in _titles(a, b, cat):
        ndoc += 1
        seen = set()
        for chunk in _HAN.findall(r["title"]):
            for i in range(len(chunk) - n + 1):
                g = chunk[i : i + n]
                tf[g] += 1
                seen.add(g)
        for g in seen:
            df[g] += 1
    rows = [(g, c, df[g]) for g, c in tf.items() if df[g] >= min_df and g not in _STOP]
    rows.sort(key=lambda x: -x[1])
    print(f"# docs={ndoc} grams={len(tf)} shown={min(top, len(rows))}", file=sys.stderr)
    for g, c, d in rows[:top]:
        print(f"{c}\t{d}\t{g}")


def _count(term: str, a: _date, b: _date, cat: str, title_only: bool) -> tuple[int, int]:
    t = body = 0
    for r in _titles(a, b, cat):
        if term in r["title"]:
            t += 1
        elif not title_only and term in (r["summary"] + " " + r["content"]):
            body += 1
    return t, body


def cmd_rise(terms: list[str], a: tuple[_date, _date], b: tuple[_date, _date], cat: str) -> None:
    print("term\tA_title\tA_body\tB_title\tB_body")
    for t in terms:
        at, ab = _count(t, a[0], a[1], cat, False)
        bt, bb = _count(t, b[0], b[1], cat, False)
        print(f"{t}\t{at}\t{ab}\t{bt}\t{bb}")


def _best_hits(term: str, a: _date, b: _date, cat: str, want: int = 3) -> list[dict]:
    """回傳最多 want 則:標題命中優先(最早的在前),不足才補內文命中。"""
    th: list[dict] = []
    bh: list[dict] = []
    for _, r in cnyes_probe.iter_corpus(a, b, cat):
        if term in r["title"]:
            if len(th) < want:
                th.append(r)
        elif len(bh) < want and term in (r["summary"] + " " + r["content"]):
            bh.append(r)
        if len(th) >= want:
            break
    return (th + bh)[:want]


def cmd_verify(brief: Path, a: _date, b: _date, cat: str) -> int:
    d = json.loads(brief.read_text())
    miss = 0
    print("status\tterm\ttoday\tdate\turl\ttitle")
    for v in d.get("vocabulary", []):
        for term in v.get("then", []):
            hits = _best_hits(term, a, b, cat, want=1)
            if not hits:
                miss += 1
                print(f"MISS\t{term}\t{v.get('today','')}\t\t\t")
                continue
            h = hits[0]
            where = "TITLE" if term in h["title"] else "BODY"
            print(
                f"{where}\t{term}\t{v.get('today','')}\t{h['at'][:10]}\t"
                f"https://news.cnyes.com/news/id/{h['id']}\t{h['title']}"
            )
    print(f"# terms_missing={miss}", file=sys.stderr)
    return 0


_CNYES_ID = re.compile(r"news\.cnyes\.com/news/id/(\d+)")


def cmd_evidence(brief: Path, a: _date, b: _date, cat: str) -> int:
    """把卡片裡每個鉅亨連結拿回本地語料對帳:文章在不在、刊出日對不對、詞在不在。

    這是「連結是真的」之外的第二層檢查——連結活著但刊出日寫錯(把 2021 的稿標成
    2020),下游會照著錯的時間錨去挖,錯得無聲。本地語料有 publishAt,對帳零成本。
    """
    d = json.loads(brief.read_text())
    want: dict[str, dict] = {}  # id -> {claim_date, terms, field}

    def add(url, claim_date, terms, field, want_title=False):
        m = _CNYES_ID.search(url or "")
        if m:
            want.setdefault(
                m.group(1),
                {"claim": claim_date or "", "terms": set(), "field": field, "want_title": False},
            )
            want[m.group(1)]["terms"].update(t for t in terms if t)
            want[m.group(1)]["want_title"] |= want_title

    for v in d.get("vocabulary", []):
        add(v.get("evidence"), v.get("evidence_date"), v.get("then", []), "vocabulary",
            bool(v.get("evidence_in_title")))
        for u in v.get("evidence_extra") or []:
            add(u, None, v.get("then", []), "vocabulary.extra")
    for e in d.get("macro_timeline", []):
        add(e.get("source"), e.get("date"), [], "macro")
        for u in e.get("source_extra") or []:
            add(u, None, [], "macro.extra")
    for s in d.get("sector_context", []):
        add(s.get("source"), None, [], "sector")
        for u in s.get("source_extra") or []:
            add(u, None, [], "sector.extra")
    for u in (d.get("market_rules", {}) or {}).get("source", []) or []:
        add(u, None, [], "market_rules")

    found: dict[str, dict] = {}
    for _, r in cnyes_probe.iter_corpus(a, b, cat):
        sid = str(r["id"])
        if sid in want and sid not in found:
            found[sid] = r
    bad = 0
    print("status\tid\tclaim_date\tcorpus_date\tterms_hit\tfield\ttitle")
    for sid, w in want.items():
        r = found.get(sid)
        if r is None:
            bad += 1
            print(f"NOT_IN_CORPUS\t{sid}\t{w['claim']}\t\t\t{w['field']}\t")
            continue
        cdate = r["at"][:10]
        hay = r["title"] + " " + r["summary"] + " " + r["content"]
        hits = sorted(t for t in w["terms"] if t in hay)
        in_title = [t for t in hits if t in r["title"]]
        status = "OK"
        if w["claim"] and w["claim"] != cdate:
            status = "DATE_MISMATCH"
        elif w["terms"] and not hits:
            status = "NO_TERM"
        elif w.get("want_title") and not in_title:
            # 卡片宣稱「這個詞當年出現在標題上」,就必須真的在標題上
            status = "NOT_IN_TITLE"
        if status != "OK":
            bad += 1
        print(f"{status}\t{sid}\t{w['claim']}\t{cdate}\t{'|'.join(hits)}\t{w['field']}\t{r['title'][:60]}")
    print(f"# cnyes_links={len(want)} bad={bad}", file=sys.stderr)
    return 0


def _span(s: str) -> tuple[_date, _date]:
    x, y = s.split(":")
    return _date.fromisoformat(x), _date.fromisoformat(y)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("mine")
    p.add_argument("--from", dest="a", required=True)
    p.add_argument("--to", dest="b", required=True)
    p.add_argument("--n", type=int, default=3)
    p.add_argument("--top", type=int, default=200)
    p.add_argument("--min-df", type=int, default=15)
    p.add_argument("--cat", default=cnyes_probe.DEFAULT_CAT)

    p = sub.add_parser("rise")
    p.add_argument("--terms", required=True)
    p.add_argument("--a", required=True, help="YYYY-MM-DD:YYYY-MM-DD")
    p.add_argument("--b", required=True)
    p.add_argument("--cat", default=cnyes_probe.DEFAULT_CAT)

    for name in ("verify", "evidence"):
        p = sub.add_parser(name)
        p.add_argument("--brief", required=True)
        p.add_argument("--from", dest="a", required=True)
        p.add_argument("--to", dest="b", required=True)
        p.add_argument("--cat", default=cnyes_probe.DEFAULT_CAT)

    g = ap.parse_args(argv)
    if g.cmd == "evidence":
        return cmd_evidence(Path(g.brief), _date.fromisoformat(g.a), _date.fromisoformat(g.b), g.cat)
    if g.cmd == "mine":
        cmd_mine(_date.fromisoformat(g.a), _date.fromisoformat(g.b), g.n, g.top, g.min_df, g.cat)
        return 0
    if g.cmd == "rise":
        cmd_rise([t for t in g.terms.split(",") if t], _span(g.a), _span(g.b), g.cat)
        return 0
    return cmd_verify(Path(g.brief), _date.fromisoformat(g.a), _date.fromisoformat(g.b), g.cat)


if __name__ == "__main__":
    sys.exit(main())
