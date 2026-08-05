"""EV58 期別語境卡:vocabulary 每個「當年說法」的逐詞查核(vocab audit)。

為什麼要這支
------------
``link_check`` 只保證「這個連結活著,而且頁面上出現了 then 裡的**某一個**詞」。
但一組 then 通常列了三四個詞,只要其中一個命中,整組就綠燈——剩下那幾個沒被驗到
的詞會混著過關。下游拿著沒驗到的詞去搜,搜不到,再把搜不到誤讀成「當年沒消息」,
而錯誤的來源其實是這張卡片。

本模組把顆粒度降到**每一個詞**:對 vocabulary 的每個 then 詞,問兩件事——

1. 它有沒有出現在該條目的任一 evidence 頁面上?(用 link_check 的快取,不重抓)
2. 它有沒有出現在當年標題語料裡?(corpus_mine 的 300+ 取樣日語料)

兩個都沒有 → 這個詞沒有當年出處,應該刪掉或補證據。留白比編造有用。

不需要 cache.duckdb。

用法
----
    uv run --project . python -m quantlib.ev58.vocab_audit --brief var/out/ev58_news/_era_brief/E1.json
    uv run --project . python -m quantlib.ev58.vocab_audit --brief ... --bad-only
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from quantlib.ev58 import corpus_mine
from quantlib.ev58.link_check import _load_cache


def audit(path: Path, lo: str, hi: str) -> list[tuple[str, str, str]]:
    """(狀態, 今天的說法, 當年的詞) — 狀態為 page / corpus / MISSING。"""
    brief = json.loads(path.read_text())
    cache = _load_cache()
    corpus = corpus_mine.load(lo, hi, fin_only=False)
    corpus_text = "\n".join(t for _, _, t in corpus)
    out: list[tuple[str, str, str]] = []
    for v in brief.get("vocabulary", []):
        urls = [v.get("evidence")] + list(v.get("evidence_extra", []) or [])
        found_on_pages: set[str] = set()
        for u in urls:
            row = cache.get(u or "")
            if row:
                found_on_pages.update(x for x in row["terms_found"].split("|") if x)
        for term in v.get("then", []):
            if term in found_on_pages:
                state = "page"
            elif term in corpus_text:
                state = "corpus"
            else:
                state = "MISSING"
            out.append((state, v.get("today", ""), term))
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--brief", required=True)
    ap.add_argument("--lo", default="2008-01-01")
    ap.add_argument("--hi", default="2010-12-31")
    ap.add_argument("--bad-only", action="store_true")
    a = ap.parse_args(argv)
    rows = audit(Path(a.brief), a.lo, a.hi)
    bad = 0
    for state, today, term in rows:
        if state == "MISSING":
            bad += 1
        elif a.bad_only:
            continue
        print(f"{state}\t{term}\t<- {today}")
    print(f"# terms={len(rows)} missing={bad}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
