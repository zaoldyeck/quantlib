"""EV58 期別語境卡:媒體地景的機械體檢(media probe)。

為什麼要這支
------------
``media_landscape`` 一節的用途很具體:下游遇到打不開的連結時,要能分辨「這個站
當年就沒有這條消息」(miss)還是「站還在但當年的文章已經下架」(blocked/dead)。
這兩者的差別是「真的沒有消息」判定的整個地基——把 dead 誤判成 miss,就會得出
「那個年代沒人報導」的錯誤結論。

所以這一節不能用印象寫,要量:對每個站台,先用 Wayback CDX **列出它在該年代實際
存在過的文章網址**,再拿這些網址去**打今天的線上站台**,看還能不能開。

    era_urls  = CDX 在 [from, to] 區間內有存檔的該站文章數(當年有沒有內容)
    live_ok   = 這些網址今天直接開得起來的比例(今天還讀不讀得到)

判定:live_ok 高 → alive;era_urls 有但 live_ok≈0 → 當年有、今天沒了,要走時光機;
CDX 也空 → 該站當年就不在這個網址下。

不需要 cache.duckdb(純網路)。

用法
----
    uv run --project . python -m quantlib.ev58.media_probe --era 2008 2010
    uv run --project . python -m quantlib.ev58.media_probe --era 2008 2010 --only cnyes,udn
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import re
import sys
import urllib.parse
from pathlib import Path

from quantlib import paths
from quantlib.ev58.archive_probe import _get
from quantlib.ev58.link_check import check_one

OUT = paths.OUT / "ev58_news" / "_era_brief" / "_probe_cache" / "media_probe.tsv"
CDX = "https://web.archive.org/cdx/search/cdx"

# (鍵, CDX 網址樣式, 用來確認是「文章頁」而非首頁/圖檔的正規式)
TARGETS: list[tuple[str, str, str]] = [
    ("epochtimes", "epochtimes.com/b5/*", r"/b5/\d+/\d+/\d+/n\d+\.htm"),
    ("libertytimes", "libertytimes.com.tw/*", r"/20\d\d/new/\w+/\d+/today-"),
    ("cnyes", "news.cnyes.com/*", r"cnyes\.com/"),
    ("udn", "udn.com/NEWS/*", r"udn\.com/NEWS/"),
    ("moneydj", "moneydj.com/KMDJ/*", r"moneydj\.com/KMDJ/"),
    ("chinatimes", "news.chinatimes.com/*", r"chinatimes\.com/"),
    ("cna", "cna.com.tw/*", r"cna\.com\.tw/"),
    ("nownews", "nownews.com/news/*", r"nownews\.com/news/"),
    ("ithome", "ithome.com.tw/news/*", r"ithome\.com\.tw/news/\d+"),
    ("yahoo_tw_stock", "tw.stock.yahoo.com/news_content/*", r"news_content"),
    ("wretch", "wretch.cc/blog/*", r"wretch\.cc/blog/"),
    ("ptt_stock", "ptt.cc/bbs/Stock/*", r"/bbs/Stock/M\.\d+"),
    ("digitimes", "digitimes.com.tw/*", r"digitimes\.com\.tw/"),
    ("wearn", "wearn.com/*", r"wearn\.com/"),
]


def era_urls(prefix: str, rx: str, frm: str, to: str, limit: int = 300) -> list[str]:
    q = urllib.parse.urlencode(
        {
            "url": prefix,
            "output": "text",
            "fl": "original",
            "collapse": "urlkey",
            "limit": limit,
            "from": frm,
            "to": to,
            "filter": "statuscode:200",
        }
    )
    pat = re.compile(rx)
    seen: list[str] = []
    for ln in _get(f"{CDX}?{q}", timeout=120).splitlines():
        u = ln.strip()
        if u and pat.search(u) and u not in seen:
            seen.append(u)
    return seen


def probe(key: str, prefix: str, rx: str, frm: str, to: str, sample: int = 6) -> dict:
    try:
        urls = era_urls(prefix, rx, frm, to)
    except Exception as exc:  # noqa: BLE001
        return {"key": key, "era_urls": -1, "live_ok": 0, "sample": 0, "note": f"CDX_ERR {type(exc).__name__}"}
    picks = urls[:: max(1, len(urls) // sample)][:sample] if urls else []
    ok = 0
    codes: list[str] = []
    for u in picks:
        r = check_one(u.replace("http://", "https://") if ":80/" not in u else u, [])
        codes.append(str(r["status"]))
        if str(r["status"]) == "200" and not r["redirect"]:
            ok += 1
    return {
        "key": key,
        "era_urls": len(urls),
        "live_ok": ok,
        "sample": len(picks),
        "note": ",".join(codes),
        "example": picks[0] if picks else "",
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--era", nargs=2, default=["2008", "2010"])
    ap.add_argument("--only", default=None, help="逗號分隔的鍵,只跑這些站")
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args(argv)
    keys = set(a.only.split(",")) if a.only else None
    jobs = [t for t in TARGETS if keys is None or t[0] in keys]
    rows = []
    with cf.ThreadPoolExecutor(max_workers=a.workers) as pool:
        for r in pool.map(lambda t: probe(t[0], t[1], t[2], a.era[0], a.era[1]), jobs):
            rows.append(r)
            print(
                f"{r['key']}\tera_urls={r['era_urls']}\tlive_ok={r['live_ok']}/{r['sample']}"
                f"\tcodes={r['note']}\t{r.get('example','')[:110]}",
                flush=True,
            )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        "".join(
            f"{r['key']}\t{r['era_urls']}\t{r['live_ok']}\t{r['sample']}\t{r['note']}\t{r.get('example','')}\n"
            for r in rows
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
