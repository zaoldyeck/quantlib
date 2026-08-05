"""EV58 期別語境卡:限定年代的證據搜尋(era search)。

為什麼要這支
------------
語境卡的每個詞都要附「當年實際用到該詞的報導連結」。一般搜尋引擎會把結果壓向
近期內容,而近期內容是回顧文——回顧文用結果寫成,拿它當「當年用語」的證據等於
帶著答案回到過去。

本模組的做法是**用網址裡的日期當篩子**:只收下網址本身就編碼了刊出日、且該日落
在指定年代區間內的結果。這樣「這篇是不是當年刊的」不必信任站方顯示的日期,第三
者看網址就能驗。搜不到就是搜不到(留白),不降格用回顧文湊數。

兩個後端:

* ``ddg`` — DuckDuckGo HTML 端點,可加站台限定。用來替某個候選詞找當年的新聞。
* ``ptt`` — PTT 看板全文搜尋。PTT 檔名內嵌發文 epoch,日期同樣可驗;它給的是
  **散戶側**的用語(與媒體用語常常不同),是媒體語料照不到的那一半。

不需要 cache.duckdb(純網路)。

用法
----
    uv run --project . python -m quantlib.ev58.era_search ddg --term 無薪假 --lo 2008-01-01 --hi 2010-12-31
    uv run --project . python -m quantlib.ev58.era_search ddg --term 家電下鄉 --site epochtimes.com
    uv run --project . python -m quantlib.ev58.era_search ptt --term 資券相抵 --lo 2008-01-01 --hi 2010-12-31
    uv run --project . python -m quantlib.ev58.era_search batch --terms-file terms.txt   # 每行一個詞
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import html as _html
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import date as _date, datetime as _dt, timezone as _tz
from pathlib import Path

from quantlib import paths

CACHE_DIR = paths.OUT / "ev58_news" / "_era_brief" / "_probe_cache" / "search"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_TAG = re.compile(r"<[^>]+>")
_MON = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]

# 網址內嵌刊出日的常見形態。順序有意義:先試最明確的站台專屬式,再試通用式。
_PAT_EPOCH = re.compile(r"epochtimes\.com/(?:b5|gb)/(\d{1,2})/(\d{1,2})/(\d{1,2})/")
_PAT_LT = re.compile(r"libertytimes\.com\.tw/(20\d\d)/new/([a-z]{3})/(\d{1,2})/")
_PAT_WB = re.compile(r"web\.archive\.org/web/(20\d\d)(\d\d)(\d\d)\d*(?:id_)?/")
_PAT_YMD8 = re.compile(r"[/=_-](20\d\d)(\d\d)(\d\d)(?:\D|$)")
_PAT_YMD_SLASH = re.compile(r"/(20\d\d)/(\d{1,2})/(\d{1,2})(?:/|\D|$)")


def url_date(url: str) -> str | None:
    """從網址推出刊出日(YYYY-MM-DD);推不出來回 None。

    推不出來就丟掉,是本模組的核心紀律:日期不可驗的連結,就不能當「當年用語」
    的證據——它可能是同一個站在 2023 年寫的回顧文。
    """
    m = _PAT_EPOCH.search(url)
    if m:
        y, mo, d = 2000 + int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _fmt(y, mo, d)
    m = _PAT_LT.search(url)
    if m and m.group(2) in _MON:
        return _fmt(int(m.group(1)), _MON.index(m.group(2)) + 1, int(m.group(3)))
    for pat in (_PAT_WB, _PAT_YMD8):
        m = pat.search(url)
        if m:
            return _fmt(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = _PAT_YMD_SLASH.search(url)
    if m:
        return _fmt(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def _fmt(y: int, mo: int, d: int) -> str | None:
    try:
        return _date(y, mo, d).isoformat()
    except ValueError:
        return None


def _get(url: str, timeout: int = 30, tries: int = 3, headers: dict | None = None) -> str:
    last: Exception | None = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})
            with urllib.request.urlopen(req, timeout=timeout) as fh:
                raw = fh.read()
            for enc in ("utf-8", "big5", "cp950"):
                try:
                    return raw.decode(enc)
                except UnicodeDecodeError:
                    continue
            return raw.decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2 * (attempt + 1))
    raise last  # type: ignore[misc]


# lite 版比 html 版耐用得多(html 版實測會回空殼頁);兩種版面的連結樣式都留著。
_DDG_ROW = re.compile(
    r'<a[^>]+class="(?:result__a|result-link)"[^>]+href="([^"]+)"[^>]*>(.*?)</a>', re.S
)


def _unwrap(href: str) -> str:
    """DDG 會把外連包成 /l/?uddg=<encoded>;還原成原始網址才看得到日期。"""
    if "uddg=" in href:
        q = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
        if q.get("uddg"):
            return q["uddg"][0]
    return href if href.startswith("http") else "https:" + href


def ddg(term: str, site: str | None = None, extra: str = "") -> list[tuple[str, str]]:
    """(url, title) — DuckDuckGo HTML 端點。"""
    q = term + (f" site:{site}" if site else "") + (f" {extra}" if extra else "")
    html = _get("https://lite.duckduckgo.com/lite/?q=" + urllib.parse.quote(q))
    out = []
    for m in _DDG_ROW.finditer(html):
        url = _unwrap(_html.unescape(m.group(1)))
        title = re.sub(r"\s+", " ", _html.unescape(_TAG.sub("", m.group(2)))).strip()
        out.append((url, title))
    return out


_BRAVE_A = re.compile(r'<a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>', re.S)
_SKIP_HOST = ("brave.com", "w3.org", "duckduckgo.com", "google.com", "youtube.com")


def brave(term: str, site: str | None = None, extra: str = "") -> list[tuple[str, str]]:
    """(url, title) — Brave Search HTML 端點。

    2026-08 實測:DuckDuckGo(html 與 lite 兩個端點)已對本機直連出驗證碼挑戰、
    Mojeek 回 403、Bing 回無結果的 JS 殼,只有 Brave 與 Yahoo 台灣仍回可解析的
    HTML。保留 ddg 為備援(它日後可能恢復),預設走 brave。
    """
    q = term + (f" site:{site}" if site else "") + (f" {extra}" if extra else "")
    html_txt = _get("https://search.brave.com/search?q=" + urllib.parse.quote(q))
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for m in _BRAVE_A.finditer(html_txt):
        url = _html.unescape(m.group(1))
        if any(h in url for h in _SKIP_HOST) or url in seen:
            continue
        title = re.sub(r"\s+", " ", _html.unescape(_TAG.sub("", m.group(2)))).strip()
        seen.add(url)
        out.append((url, title))
    return out


_PTT_ROW = re.compile(r'href="(/bbs/(\w+)/M\.(\d+)\.A\.\w+\.html)">([^<]*)<')


def ptt(term: str, board: str = "Stock", pages: int = 3) -> list[tuple[str, str, str]]:
    """(date, url, title) — PTT 看板全文搜尋;日期取檔名內嵌 epoch。"""
    out: list[tuple[str, str, str]] = []
    for page in range(1, pages + 1):
        url = f"https://www.ptt.cc/bbs/{board}/search?page={page}&q=" + urllib.parse.quote(term)
        try:
            html = _get(url, headers={"Cookie": "over18=1"})
        except Exception:  # noqa: BLE001
            break
        rows = list(_PTT_ROW.finditer(html))
        if not rows:
            break
        for m in rows:
            stamp = _dt.fromtimestamp(int(m.group(3)) + 8 * 3600, _tz.utc).strftime("%Y-%m-%d")
            out.append((stamp, f"https://www.ptt.cc{m.group(1)}", _html.unescape(m.group(4)).strip()))
    return out


# DDG 單次查詢只回十筆;對每個詞掃一組站台,才能把當年的新聞逼出來。
SITES = [
    None,
    "epochtimes.com",
    "libertytimes.com.tw",
    "web.archive.org",
    "chinatimes.com",
    "cna.com.tw",
    "ithome.com.tw",
    "nownews.com",
    "moneydj.com",
]


def search_era(term: str, lo: str, hi: str, sites: list[str | None] | None = None) -> list[tuple[str, str, str, str]]:
    """(date, url, title, backend) — 只回網址日期落在 [lo, hi] 的結果。"""
    a, b = _date.fromisoformat(lo), _date.fromisoformat(hi)
    seen: set[str] = set()
    hits: list[tuple[str, str, str, str]] = []
    for site in sites if sites is not None else SITES:
        rows: list[tuple[str, str]] = []
        for backend in (brave, ddg):
            try:
                rows = backend(term, site)
            except Exception:  # noqa: BLE001 - 單一後端失敗換下一個,不中斷整個詞
                rows = []
            if rows:
                break
        if not rows:
            continue
        for url, title in rows:
            d = url_date(url)
            if not d or url in seen:
                continue
            if a <= _date.fromisoformat(d) <= b:
                seen.add(url)
                hits.append((d, url, title, f"ddg:{site or 'web'}"))
        time.sleep(1.2)  # DDG 對連發敏感
    try:
        for d, url, title in ptt(term):
            if url in seen or not (a <= _date.fromisoformat(d) <= b):
                continue
            seen.add(url)
            hits.append((d, url, title, "ptt"))
    except Exception:  # noqa: BLE001
        pass
    return sorted(hits)


def _cache_path(term: str, lo: str, hi: str) -> Path:
    key = hashlib.md5(f"{term}|{lo}|{hi}".encode()).hexdigest()[:10]
    return CACHE_DIR / f"{re.sub(r'[^0-9A-Za-z一-鿿]', '_', term)[:24]}_{key}.tsv"


def cached_search(term: str, lo: str, hi: str, refresh: bool = False) -> list[tuple[str, str, str, str]]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_path(term, lo, hi)
    if p.exists() and not refresh:
        return [tuple(ln.split("\t")) for ln in p.read_text().splitlines() if ln.count("\t") == 3]  # type: ignore[misc]
    rows = search_era(term, lo, hi)
    p.write_text("".join("\t".join(r) + "\n" for r in rows))
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["ddg", "ptt", "batch"])
    ap.add_argument("--term", default=None)
    ap.add_argument("--terms-file", default=None)
    ap.add_argument("--site", default=None)
    ap.add_argument("--lo", default="2008-01-01")
    ap.add_argument("--hi", default="2010-12-31")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--refresh", action="store_true")
    a = ap.parse_args(argv)

    if a.cmd == "ptt":
        for d, u, t in ptt(a.term or ""):
            if a.lo <= d <= a.hi:
                print(f"{d}\t{u}\t{t}")
        return 0
    if a.cmd == "ddg":
        sites = [a.site] if a.site else None
        for d, u, t, src in search_era(a.term or "", a.lo, a.hi, sites):  # type: ignore[arg-type]
            print(f"{d}\t{src}\t{u}\t{t}")
        return 0
    terms = [t.strip() for t in Path(a.terms_file).read_text().splitlines() if t.strip()]
    with cf.ThreadPoolExecutor(max_workers=a.workers) as pool:
        for term, rows in zip(terms, pool.map(lambda t: cached_search(t, a.lo, a.hi, a.refresh), terms)):
            for d, u, t, src in rows:
                print(f"{term}\t{d}\t{src}\t{u}\t{t}", flush=True)
            if not rows:
                print(f"{term}\t-\t-\tNOHIT\t-", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
