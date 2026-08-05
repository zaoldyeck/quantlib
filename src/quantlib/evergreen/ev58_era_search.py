"""EV58 era-brief 支援:**只回傳「那個年代」的報導**的檢索器。

問題:一般搜尋引擎(含 agent 內建 WebSearch)強烈偏好新內容——查「處置股 分盤」回來
的清一色是今年的懶人包。拿今天的文章去寫年代語境卡,等於帶著答案回到過去,正是
era_brief 明令禁止的事。而下游研究員需要的恰恰相反:**當年寫的那一篇**。

作法兩條腿:
1. `ltn`:自由時報站內搜尋支援 `start_time`/`end_time` 真日期區間,直接把年代寫進查詢。
2. `urldate`:對 URL 內嵌日期的站(technews / chinatimes / ettoday / cna / businesstoday),
   打站內搜尋後**用網址上的日期硬篩**——網址日期不受頁面改版影響,是最誠實的年代證據。

Run:
    uv run --project . python -m quantlib.evergreen.ev58_era_search \\
        --q "被動元件 漲價" --from 2018-01-01 --to 2019-12-31
    uv run --project . python -m quantlib.evergreen.ev58_era_search \\
        --qfile q.txt --from 2018-01-01 --to 2019-12-31 --out hits.tsv

不依賴 cache.duckdb。
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# 網址內嵌日期:年代證據裡最硬的一種(頁面可以改版,網址不會)
_URL_DATE_RES = (
    re.compile(r"/(20\d{2})/(\d{2})/(\d{2})/"),  # technews.tw/2018/06/25/
    re.compile(r"/news/(20\d{2})(\d{2})(\d{2})/"),  # ettoday.net/news/20181228/
    re.compile(r"/(?:newspapers|realtimenews|opinion)/(20\d{2})(\d{2})(\d{2})\d*-"),  # chinatimes
    re.compile(r"/news/[a-z]+/(20\d{2})(\d{2})(\d{2})\d{4}\.aspx"),  # cna
    re.compile(r"/post/(20\d{2})(\d{2})(\d{2})\d*/"),  # businesstoday
)
_A_RE = re.compile(r"<a\s[^>]*href=\"([^\"]+)\"[^>]*>", re.I)
_TAG_RE = re.compile(r"<[^>]+>")

# 站內搜尋端點(關鍵字以 {q} 代入,已 urlquote)
SITE_SEARCH = {
    "technews": "https://technews.tw/?s={q}",
    "chinatimes": "https://www.chinatimes.com/search/{q}?page=1&chdtv",
    "ettoday": "https://www.ettoday.net/news_search/doSearch.php?keywords={q}&idx=1&page=1",
    "cna": "https://www.cna.com.tw/search/hysearchws.aspx?q={q}",
    "cnyes": "https://news.cnyes.com/search?keyword={q}",
}


def _curl(url: str, timeout: int = 30) -> str:
    try:
        proc = subprocess.run(
            ["curl", "-sL", "-A", UA, "--max-time", str(timeout), url],
            capture_output=True,
            timeout=timeout + 15,
        )
        return proc.stdout.decode("utf-8", "ignore")
    except Exception:  # noqa: BLE001 - 一個站掛掉不該讓整批搜尋停擺
        return ""


def url_date(url: str) -> str | None:
    for rx in _URL_DATE_RES:
        m = rx.search(url)
        if m:
            y, mo, d = (int(x) for x in m.groups())
            if 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def search_ltn(query: str, d_from: str, d_to: str) -> list[dict]:
    """自由時報站內搜尋:唯一支援真日期區間的來源,年代由伺服器端保證。"""
    url = (
        "https://search.ltn.com.tw/list?keyword="
        + urllib.parse.quote(query)
        + f"&start_time={d_from.replace('-', '')}&end_time={d_to.replace('-', '')}&sort=date&type=all"
    )
    html = _curl(url)
    m = re.search(r'<ul class="list[^"]*"[^>]*>(.*?)</ul>', html, re.S)
    if not m:
        return []
    out: list[dict] = []
    for li in re.findall(r"<li>(.*?)</li>", m.group(1), re.S):
        href = re.search(r'href="(https?://[^"]+)"', li)
        title = re.search(r'title="([^"]+)"', li)
        date = re.search(r"(20\d{2})/(\d{2})/(\d{2})", li)
        if not href:
            continue
        out.append(
            {
                "source": "ltn",
                "date": f"{date.group(1)}-{date.group(2)}-{date.group(3)}" if date else "",
                "title": (title.group(1) if title else "").strip()[:160],
                "url": href.group(1),
                "query": query,
            }
        )
    # 同一篇會同時出現在圖片連結與標題連結
    seen, dedup = set(), []
    for r in out:
        if r["url"] in seen:
            continue
        seen.add(r["url"])
        dedup.append(r)
    return dedup


def search_urldate(site: str, query: str, d_from: str, d_to: str) -> list[dict]:
    """打站內搜尋,只留網址日期落在區間內者。"""
    html = _curl(SITE_SEARCH[site].format(q=urllib.parse.quote(query)))
    out: dict[str, dict] = {}
    for m in _A_RE.finditer(html):
        href = m.group(1)
        if href.startswith("//"):
            href = "https:" + href
        if not href.startswith("http"):
            continue
        d = url_date(href)
        if not d or not (d_from <= d <= d_to):
            continue
        href = href.split("#")[0]
        if href in out:
            continue
        # 標題:抓該連結後面最近的文字
        tail = html[m.end() : m.end() + 400]
        title = _TAG_RE.sub(" ", tail)
        title = re.sub(r"\s+", " ", title).strip()[:120]
        out[href] = {"source": site, "date": d, "title": title, "url": href, "query": query}
    return list(out.values())


def era_search(query: str, d_from: str, d_to: str, sites: list[str] | None = None) -> list[dict]:
    sites = sites or ["ltn", *SITE_SEARCH]
    jobs = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for s in sites:
            if s == "ltn":
                jobs.append(ex.submit(search_ltn, query, d_from, d_to))
            elif s in SITE_SEARCH:
                jobs.append(ex.submit(search_urldate, s, query, d_from, d_to))
        res: list[dict] = []
        for j in jobs:
            res.extend(j.result())
    return sorted(res, key=lambda r: r["date"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--q", action="append", default=[])
    ap.add_argument("--qfile")
    ap.add_argument("--from", dest="d_from", required=True)
    ap.add_argument("--to", dest="d_to", required=True)
    ap.add_argument("--sites", default="")
    ap.add_argument("--out")
    args = ap.parse_args()

    queries = list(args.q)
    if args.qfile:
        queries += [x.strip() for x in Path(args.qfile).read_text(encoding="utf-8").splitlines() if x.strip()]
    if not queries:
        ap.error("需要 --q 或 --qfile")
    sites = [s for s in args.sites.split(",") if s] or None

    rows: list[str] = []
    with ThreadPoolExecutor(max_workers=min(5, len(queries))) as ex:
        for hits in ex.map(lambda q: era_search(q, args.d_from, args.d_to, sites), queries):
            for h in hits:
                rows.append(f"{h['query']}\t{h['source']}\t{h['date']}\t{h['title']}\t{h['url']}")

    text = "\n".join(rows)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text + "\n", encoding="utf-8")
        print(f"{len(rows)} hits -> {args.out}", file=sys.stderr)
    else:
        print(text)


if __name__ == "__main__":
    main()
