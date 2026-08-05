"""EV58 期別語境卡:鉅亨網逐日新聞清單的考掘探針(cnyes probe)。

為什麼要這支
------------
語境卡的 vocabulary 欄要的是「**當年實際用過的詞**」,而不是我對那個年代的印象。
兩種取得方式的差距是致命的:

- 搜尋引擎:結果被壓向近期,查 2020 年的題材,前排全是 2023 以後的回顧文。回顧文
  是用結果寫成的,拿它當「當年主流題材」的名單,等於帶著答案回到過去。
- 逐日清單:直接把某一天實際刊出的報導列出來,標題即當年用語,連結即出處,
  刊出日由 API 的 publishAt 保證,不必相信任何人的轉述。

鉅亨網有公開的日期型清單 API,涵蓋 2012 年至今、逐日完整,是 2012 年以後各期別
最好用的一手索引。本模組把它變成可重跑的本地語料:抓一次、存 gz、之後所有
「這個詞當年有沒有人用、哪一天誰用的」都變成本地 grep,零額外請求。

不需要 cache.duckdb(純網路 + 本地語料)。

用法
----
    # 抓一段期間的台股新聞(逐日快取,重跑不重抓;--refresh 強制重抓)
    uv run --project . python -m quantlib.ev58.cnyes_probe pull --from 2020-01-01 --to 2021-12-31

    # 在本地語料裡找「當年誰用了這個詞」(預設全文;--title-only 只掃標題)
    uv run --project . python -m quantlib.ev58.cnyes_probe grep --term 航海王 --from 2021-01-01 --to 2021-12-31

    # 一次驗多個詞,只回每個詞最早出現的那一則(語境卡 evidence 的產生器)
    uv run --project . python -m quantlib.ev58.cnyes_probe first --terms 航海王,缺櫃,晶片荒 --from 2020-01-01 --to 2021-12-31

    # 某一天的全部標題(找轉折日的當天盤後稿)
    uv run --project . python -m quantlib.ev58.cnyes_probe titles --date 2020-03-09

文章網址 = https://news.cnyes.com/news/id/{newsId}
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import gzip
import html as _html
import json
import os
import re
import subprocess
import sys
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path

from quantlib import paths

CACHE_DIR = paths.OUT / "ev58_news" / "_era_brief" / "_probe_cache" / "cnyes"
API = "https://api.cnyes.com/media/api/v1/newslist/category/{cat}"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
_TAG = re.compile(r"<[^>]+>")
DEFAULT_CAT = "tw_stock"


def _epoch(d: _date) -> int:
    return int(datetime(d.year, d.month, d.day).timestamp())


def _curl_json(url: str, timeout: int = 40) -> dict | None:
    """API 端偶發 5xx/限流;curl 比 urllib 少被擋(TLS 指紋),故一律走 curl。"""
    for _ in range(3):
        try:
            proc = subprocess.run(
                ["curl", "-sS", "--compressed", "--max-time", str(timeout), "-A", UA,
                 "-H", "Accept: application/json", url],
                capture_output=True, timeout=timeout + 10,
            )
            return json.loads(proc.stdout.decode("utf-8", "replace"))
        except Exception:  # noqa: BLE001 - 網路/JSON 任何錯都重試
            continue
    return None


def _plain(s: str) -> str:
    return re.sub(r"\s+", " ", _TAG.sub(" ", _html.unescape(s or ""))).strip()


def _day_path(cat: str, d: _date) -> Path:
    return CACHE_DIR / cat / f"{d.isoformat()}.jsonl.gz"


def fetch_day(d: _date, cat: str = DEFAULT_CAT, refresh: bool = False) -> int:
    """抓某日全部頁次並落地。回傳筆數(-1 = 抓取失敗,不寫檔以免把失敗快取成空日)。"""
    path = _day_path(cat, d)
    if path.exists() and not refresh:
        return -2  # 已有
    ts = _epoch(d)
    rows: list[dict] = []
    page = 1
    while True:
        url = f"{API.format(cat=cat)}?startAt={ts}&endAt={ts}&limit=30&page={page}"
        j = _curl_json(url)
        if not j or "items" not in j:
            return -1
        items = j["items"]
        for it in items.get("data", []):
            rows.append(
                {
                    "id": it.get("newsId"),
                    "at": datetime.fromtimestamp(it.get("publishAt", 0)).isoformat(sep=" "),
                    "cat": it.get("categoryName", ""),
                    "title": _plain(it.get("title", "")),
                    "kw": it.get("keyword") or [],
                    "summary": _plain(it.get("summary", ""))[:400],
                    "content": _plain(it.get("content", ""))[:6000],
                }
            )
        if page >= int(items.get("last_page", 1) or 1):
            break
        page += 1
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f".tmp{os.getpid()}")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, path)
    return len(rows)


def load_day(d: _date, cat: str = DEFAULT_CAT) -> list[dict]:
    path = _day_path(cat, d)
    if not path.exists():
        return []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(ln) for ln in fh if ln.strip()]


def _daterange(a: _date, b: _date):
    cur = a
    while cur <= b:
        yield cur
        cur += timedelta(days=1)


def pull(a: _date, b: _date, cat: str = DEFAULT_CAT, workers: int = 8, refresh: bool = False) -> None:
    days = list(_daterange(a, b))
    done = fail = have = 0
    with cf.ThreadPoolExecutor(max_workers=workers) as pool:
        for n in pool.map(lambda d: fetch_day(d, cat, refresh), days):
            if n == -1:
                fail += 1
            elif n == -2:
                have += 1
            else:
                done += 1
    print(f"# pull {a}~{b} cat={cat}: fetched={done} cached={have} failed={fail}", file=sys.stderr)


def iter_corpus(a: _date, b: _date, cat: str = DEFAULT_CAT):
    for d in _daterange(a, b):
        for r in load_day(d, cat):
            yield d, r


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def _common(p):
        p.add_argument("--from", dest="a", required=True)
        p.add_argument("--to", dest="b", required=True)
        p.add_argument("--cat", default=DEFAULT_CAT)

    p = sub.add_parser("pull"); _common(p)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--refresh", action="store_true")

    p = sub.add_parser("grep"); _common(p)
    p.add_argument("--term", required=True)
    p.add_argument("--title-only", action="store_true")
    p.add_argument("--limit", type=int, default=25)

    p = sub.add_parser("first"); _common(p)
    p.add_argument("--terms", required=True, help="逗號分隔")
    p.add_argument("--title-only", action="store_true")
    p.add_argument("--per-term", type=int, default=1)

    p = sub.add_parser("titles")
    p.add_argument("--date", required=True)
    p.add_argument("--cat", default=DEFAULT_CAT)
    p.add_argument("--grep", default=None)

    a = ap.parse_args(argv)

    if a.cmd == "pull":
        pull(_date.fromisoformat(a.a), _date.fromisoformat(a.b), a.cat, a.workers, a.refresh)
        return 0

    if a.cmd == "titles":
        d = _date.fromisoformat(a.date)
        rows = load_day(d, a.cat) or (fetch_day(d, a.cat), load_day(d, a.cat))[1]
        for r in rows:
            if a.grep and a.grep not in r["title"] and a.grep not in r["content"]:
                continue
            print(f"{r['at']}\t{r['id']}\t{r['title']}")
        return 0

    a0, b0 = _date.fromisoformat(a.a), _date.fromisoformat(a.b)
    if a.cmd == "grep":
        n = 0
        for d, r in iter_corpus(a0, b0, a.cat):
            hay = r["title"] if a.title_only else r["title"] + " " + r["summary"] + " " + r["content"]
            if a.term in hay:
                where = "T" if a.term in r["title"] else "B"
                print(f"{r['at']}\t{where}\thttps://news.cnyes.com/news/id/{r['id']}\t{r['title']}")
                n += 1
                if n >= a.limit:
                    break
        print(f"# hits(shown)={n}", file=sys.stderr)
        return 0

    if a.cmd == "first":
        terms = [t for t in a.terms.split(",") if t]
        left = {t: a.per_term for t in terms}
        for d, r in iter_corpus(a0, b0, a.cat):
            hay = r["title"] if a.title_only else r["title"] + " " + r["summary"] + " " + r["content"]
            for t in terms:
                if left[t] > 0 and t in hay:
                    where = "T" if t in r["title"] else "B"
                    print(f"{t}\t{r['at']}\t{where}\thttps://news.cnyes.com/news/id/{r['id']}\t{r['title']}")
                    left[t] -= 1
            if all(v <= 0 for v in left.values()):
                break
        for t, v in left.items():
            if v > 0:
                print(f"{t}\tNONE", file=sys.stderr)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
