"""EV58 era-brief 支援:不靠 agent 內建 WebSearch 的網頁檢索器(直打搜尋引擎)。

為什麼要這支:era_brief 與下游歸因研究員都靠「搜當年的詞、找當年的一手報導」吃飯,
而 agent 內建的 WebSearch 有每個 session 的呼叫上限(用完就整場失能,實測 200 次)。
上限一到,若沒有第二條檢索路徑,整批考掘會被迫停在「查不到」——而那會被誤讀成
「那個年代沒有消息」,正是本專案最想避免的假陰性。

作法:直接 curl 搜尋引擎的 HTML 版結果頁,解析結果連結。Bing 的結果連結多半包在
`/ck/a?...&u=a1<base64url>` 轉址裡,必須解碼還原真實網址(不解碼會拿到一堆 bing.com,
看起來像「搜不到」)。DuckDuckGo 對 curl 回 202 擋機器,故不列入。

Run:
    uv run --project . python -m quantlib.evergreen.ev58_web_search \\
        --q "處置股票 分盤 撮合" --site chinatimes.com --limit 20
    uv run --project . python -m quantlib.evergreen.ev58_web_search \\
        --qfile queries.txt --out var/out/ev58_news/_era_brief/_probe_cache/hits.tsv

不依賴 cache.duckdb。
"""

from __future__ import annotations

import argparse
import base64
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
ENGINES = {
    "bing": "https://www.bing.com/search?q={q}&count=30&setlang=zh-TW&cc=TW",
    "brave": "https://search.brave.com/search?q={q}",
    "mojeek": "https://www.mojeek.com/search?q={q}",
}
# 搜尋引擎自家的資產／導覽網域——留著會淹沒真結果
_ENGINE_HOSTS = re.compile(
    r"(^|\.)(bing\.com|microsoft\.com|msn\.com|microsofttranslator\.com|brave\.com|mojeek\.com"
    r"|duckduckgo\.com|google\.[a-z.]+|go\.microsoft\.com|w3\.org|mastodon\.social)$"
)
_A_RE = re.compile(r"<a\s[^>]*href=\"(https?://[^\"]+)\"[^>]*>(.*?)</a>", re.I | re.S)
_TAG_RE = re.compile(r"<[^>]+>")


def _unwrap_bing(url: str) -> str:
    """Bing 把目標網址 base64url 塞在 `u=a1...`;不還原就只看得到 bing.com。"""
    if "bing.com/ck/a" not in url:
        return url
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    raw = (qs.get("u") or [""])[0]
    if not raw.startswith("a1"):
        return url
    body = raw[2:]
    body += "=" * (-len(body) % 4)
    try:
        return base64.urlsafe_b64decode(body).decode("utf-8", "ignore")
    except Exception:  # noqa: BLE001 - 解不開就退回原網址,不讓一筆壞掉的轉址殺掉整次搜尋
        return url


def fetch(engine: str, query: str, timeout: int = 30) -> list[tuple[str, str]]:
    url = ENGINES[engine].format(q=urllib.parse.quote(query))
    try:
        proc = subprocess.run(
            ["curl", "-sL", "-A", UA, "--max-time", str(timeout), url],
            capture_output=True,
            timeout=timeout + 15,
        )
        html = proc.stdout.decode("utf-8", "ignore")
    except Exception:  # noqa: BLE001
        return []
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for m in _A_RE.finditer(html):
        link = _unwrap_bing(m.group(1))
        host = urllib.parse.urlparse(link).netloc.lower().split(":")[0]
        if _ENGINE_HOSTS.search(host) or not host:
            continue
        title = _TAG_RE.sub("", m.group(2))
        title = re.sub(r"\s+", " ", title).strip()[:160]
        if link in seen:
            continue
        seen.add(link)
        out.append((title, link))
    return out


def search(query: str, engines: list[str] | None = None, limit: int = 25) -> list[dict]:
    """多引擎並打,合併去重(同一網址只留第一個引擎的命中)。"""
    engines = engines or list(ENGINES)
    with ThreadPoolExecutor(max_workers=len(engines)) as ex:
        per = list(ex.map(lambda e: (e, fetch(e, query)), engines))
    merged: dict[str, dict] = {}
    for eng, hits in per:
        for title, link in hits:
            if link not in merged:
                merged[link] = {"engine": eng, "title": title, "url": link, "query": query}
    return list(merged.values())[:limit]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--q", action="append", default=[], help="查詢字串,可重複")
    ap.add_argument("--qfile", help="一行一個查詢")
    ap.add_argument("--site", help="限定網域(等同附加 site:)")
    ap.add_argument("--engines", default=",".join(ENGINES))
    ap.add_argument("--limit", type=int, default=25)
    ap.add_argument("--out", help="TSV 輸出路徑(不給則印到 stdout)")
    args = ap.parse_args()

    queries = list(args.q)
    if args.qfile:
        queries += [x.strip() for x in Path(args.qfile).read_text(encoding="utf-8").splitlines() if x.strip()]
    if not queries:
        ap.error("需要 --q 或 --qfile")
    if args.site:
        queries = [f"{q} site:{args.site}" for q in queries]

    engines = [e for e in args.engines.split(",") if e in ENGINES]
    rows: list[str] = []
    with ThreadPoolExecutor(max_workers=min(6, len(queries))) as ex:
        for hits in ex.map(lambda q: search(q, engines, args.limit), queries):
            for h in hits:
                rows.append(f"{h['query']}\t{h['engine']}\t{h['title']}\t{h['url']}")

    text = "\n".join(rows)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text + "\n", encoding="utf-8")
        print(f"{len(rows)} hits -> {args.out}", file=sys.stderr)
    else:
        print(text)


if __name__ == "__main__":
    main()
