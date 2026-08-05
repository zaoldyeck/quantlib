"""EV58 期別語境卡:出處連結的機械驗證器(link check)。

為什麼要這支
------------
語境卡的唯一價值在「每一條都有出處」。而出處有三種壞法,肉眼一條條看不完:

1. **連結是假的**——網址長得像真的但站上根本沒這篇(404 / 轉走首頁)。
2. **連結是真的但日期不對**——寫著 2020 年的證據,實際刊出於 2023 年的回顧文;
   回顧文帶結果偏誤,拿它當「當年用語」的證據等於帶著答案回到過去。
3. **連結是真的、日期也對,但頁面根本沒出現那個詞**——詞是憑印象寫的。

本模組把這三件事變成可重跑的機械檢查:抓頁面 → 取標題 → 掃頁內日期 → 掃指定詞,
輸出一張表讓「哪幾條該刪」變成查表而不是憑感覺。

不需要 cache.duckdb(純網路)。

用法
----
    # 驗一份語境卡:每個 vocabulary 條目的 evidence 連結,檢查頁面是否真的出現 then 詞
    uv run --project . python -m quantlib.ev58.link_check --brief var/out/ev58_news/_era_brief/E5.json

    # 驗一批候選連結(stdin 每行 "url<TAB>詞1|詞2")
    printf 'https://x/y\\t航海王\\n' | uv run --project . python -m quantlib.ev58.link_check --stdin

結果快取於 ``var/out/ev58_news/_era_brief/_probe_cache/linkcheck.tsv``(依 URL 去重),
重跑不重抓;``--refresh`` 強制重抓。
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from quantlib import paths

CACHE = paths.OUT / "ev58_news" / "_era_brief" / "_probe_cache" / "linkcheck.tsv"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_TITLE = re.compile(r"<title[^>]*>(.*?)</title>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")
_SCRIPT = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S | re.I)
# 頁內日期:YYYY-MM-DD / YYYY/MM/DD / YYYY年M月D日 三種常見寫法
_DATES = re.compile(r"(20\d\d)[-/年](\d{1,2})[-/月](\d{1,2})")


def _fetch_curl(url: str, timeout: int = 35) -> tuple[int, str]:
    """curl 後援。

    有些站(實測 ctee.com.tw、moneyweekly.com.tw)會擋 urllib 的 TLS/HTTP 指紋,
    連 handshake 都不給,於是 urllib 端看起來像「站掛了」(-1)。若不補這條路徑,
    驗證器會把**活著的正確出處誤判為死連結**,下游照表刪證據——比不驗還糟。
    """
    import subprocess

    try:
        proc = subprocess.run(
            [
                "curl", "-sS", "-L", "--compressed", "--max-time", str(timeout),
                "-w", "\n@@HTTP_CODE@@%{http_code}", "-A", UA,
                "-H", "Accept-Language: zh-TW,zh;q=0.9,en;q=0.8", url,
            ],
            capture_output=True, timeout=timeout + 10,
        )
    except Exception:  # noqa: BLE001
        return -1, "curl-failed"
    raw = proc.stdout
    code = -1
    if b"@@HTTP_CODE@@" in raw:
        raw, _, tail = raw.rpartition(b"\n@@HTTP_CODE@@")
        code = int(tail.strip() or -1)
    return code, _decode(raw)  # 同 _fetch:字集猜錯會讓詞檢查恆為否


_META_CS = re.compile(rb"""charset\s*=\s*["']?\s*([\w-]+)""", re.I)


def _decode(raw: bytes) -> str:
    """依頁面自報的 charset 解碼,再退回逐一嘗試。

    為什麼不能只試 utf-8→big5:本期(2008-2010)的台灣財經站有 Big5,也有 GBK 的
    簡體區塊;解錯字集不會報錯,只會把中文變成亂碼,於是「頁面有沒有出現這個詞」
    的檢查恆為否——驗證器會把**正確的出處判成沒有該詞而刪掉**,比不驗更糟。
    """
    m = _META_CS.search(raw[:4096])
    order = []
    if m:
        cs = m.group(1).decode("ascii", "ignore").lower()
        order.append({"big5": "cp950", "gb2312": "gbk", "gb18030": "gbk"}.get(cs, cs))
    order += ["utf-8", "cp950", "big5", "gbk"]
    for enc in order:
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("utf-8", "replace")


def _fetch(url: str, timeout: int = 35, tries: int = 3) -> tuple[int, str]:
    """回傳 (http_status, html)。連不上以負碼表示,讓「站掛了」與「文章不存在」可分。"""
    last = ""
    for attempt in range(tries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": UA,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as fh:
                raw = fh.read()
                code = fh.getcode()
                final = fh.geturl()
            html = _decode(raw)
            if final != url:
                html = f"<!--REDIRECT {final}-->" + html
            return code, html
        except urllib.error.HTTPError as exc:
            # 403/406/429 是「擋機器人」不是「文章不存在」——換 curl 再問一次
            if exc.code in {401, 403, 406, 429}:
                code, html = _fetch_curl(url, timeout)
                if code == 200:
                    return code, html
            return exc.code, ""
        except Exception as exc:  # noqa: BLE001 - 網路層任何錯都退避重試
            last = f"{type(exc).__name__}"
            time.sleep(2 * (attempt + 1))
    code, html = _fetch_curl(url, timeout)
    if code > 0:
        return code, html
    return -1, last


def check_one(url: str, terms: list[str]) -> dict:
    code, html = _fetch(url)
    text = _TAG.sub(" ", _SCRIPT.sub(" ", html))
    m = _TITLE.search(html)
    title = re.sub(r"\s+", " ", _TAG.sub("", m.group(1))).strip()[:200] if m else ""
    dates = sorted({f"{a}-{int(b):02d}-{int(c):02d}" for a, b, c in _DATES.findall(text[:20000])})
    found = [t for t in terms if t and t in text]
    return {
        "url": url,
        "status": code,
        "title": title,
        "dates": ",".join(dates[:6]),
        "terms_want": "|".join(terms),
        "terms_found": "|".join(found),
        "redirect": "REDIRECT" in html[:60],
    }


COLS = ["url", "status", "title", "dates", "terms_want", "terms_found", "redirect"]


def _load_cache() -> dict[str, dict]:
    if not CACHE.exists():
        return {}
    out: dict[str, dict] = {}
    for ln in CACHE.read_text().splitlines():
        p = ln.split("\t")
        if len(p) == len(COLS):
            out[p[0]] = dict(zip(COLS, p))
    return out


def _save_cache(rows: dict[str, dict]) -> None:
    """合併後原子落地。

    多個期別的驗證會同時跑、共用同一份 TSV;直接整檔覆寫會把並行寫入的別人那批
    洗掉(下次重跑就得重抓)。因此寫入前先重讀磁碟版本合併,再 tmp→replace。
    """
    import os

    CACHE.parent.mkdir(parents=True, exist_ok=True)
    merged = _load_cache()
    merged.update(rows)
    tmp = CACHE.with_suffix(f".tmp{os.getpid()}")
    tmp.write_text(
        "".join(
            "\t".join(str(r.get(c, "")).replace("\t", " ").replace("\n", " ") for c in COLS) + "\n"
            for r in merged.values()
        )
    )
    os.replace(tmp, CACHE)


def check_many(items: list[tuple[str, list[str]]], workers: int = 12, refresh: bool = False) -> list[dict]:
    cache = _load_cache()
    todo = [
        (u, t)
        for u, t in items
        if refresh or u not in cache or str(cache[u]["status"]) in {"-1", ""}
        # 詞集擴充時要重掃(舊快取的 terms_want 不含新詞)
        or any(x and x not in cache[u]["terms_want"].split("|") for x in t)
    ]
    if todo:
        with cf.ThreadPoolExecutor(max_workers=workers) as pool:
            for r in pool.map(lambda it: check_one(it[0], it[1]), todo):
                cache[r["url"]] = r
        _save_cache(cache)
    return [cache[u] for u, _ in items if u in cache]


# 連結後面常緊接中文標點或說明文字;URL 邊界要把全形標點也算進終止字元,
# 否則抓到的網址會拖著「（2012-12-03」這種尾巴,驗起來一律假紅。
_URL = re.compile(r"https?://[^\s,;)　-〿＀-￯]+")


def _is_template(u: str) -> bool:
    """`<id>`、`{YYYYMMDD}` 這類是網址「格式說明」而不是網址本身。

    驗它們只會產生假紅(404),而假紅會稀釋真紅——語境卡的 archive_hint 幾乎
    一定會寫格式樣板,所以這條過濾是必要的,不是美化。
    """
    return any(ch in u for ch in "<>{}")


def _urls_in(text: str) -> list[str]:
    """從一段自由文字撈出可驗的網址(排除模板佔位)。"""
    return [u for raw in _URL.findall(text) if not _is_template(u := raw.rstrip(".,;:)"))]


def _items_from_brief(path: Path) -> list[tuple[str, list[str]]]:
    """從語境卡萃取 (連結, 該連結應該證明的詞)。

    連結不只躺在 ``evidence``/``source``——``note``、``archive_hint`` 裡的第二、
    第三條佐證同樣會被下游點開,漏驗等於漏掉一半的出處。故對每個區塊的所有字串
    值做全掃。
    """
    d = json.loads(path.read_text())
    items: dict[str, set[str]] = {}

    def add(u: str | None, terms: list[str]) -> None:
        if isinstance(u, str) and u.startswith("http") and not _is_template(u):
            items.setdefault(u, set()).update(t for t in terms if t)

    def add_block(block: dict, terms: list[str], primary: tuple[str, ...]) -> None:
        for key in primary:
            val = block.get(key)
            if isinstance(val, str):
                add(val, terms)
            elif isinstance(val, list):
                for x in val:
                    add(x, terms)
        for val in block.values():
            if isinstance(val, str):
                for u in _urls_in(val):
                    add(u, terms)

    for v in d.get("vocabulary", []):
        add_block(v, list(v.get("then", [])), ("evidence", "evidence_extra"))
    for e in d.get("macro_timeline", []):
        add_block(e, [], ("source", "source_extra"))
    for s in d.get("sector_context", []):
        add_block(s, [], ("source", "source_extra"))
    mr = d.get("market_rules", {})
    for key, val in mr.items():
        if isinstance(val, dict):
            add_block(val, [], ("source",))
        elif isinstance(val, list):
            for x in val:
                add(x, [])
        elif isinstance(val, str):
            for u in _urls_in(val):
                add(u, [])
    for m in d.get("media_landscape", []):
        add_block(m, [], ("url",))
    return [(u, sorted(t)) for u, t in items.items()]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--brief", default=None, help="語境卡 JSON 路徑")
    ap.add_argument("--stdin", action="store_true", help="從 stdin 讀 url<TAB>詞1|詞2")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--bad-only", action="store_true", help="只印有問題者")
    a = ap.parse_args(argv)

    if a.stdin:
        items = []
        for ln in sys.stdin.read().splitlines():
            if not ln.strip():
                continue
            p = ln.split("\t")
            items.append((p[0].strip(), p[1].split("|") if len(p) > 1 else []))
    elif a.brief:
        items = _items_from_brief(Path(a.brief))
    else:
        ap.error("need --brief or --stdin")

    rows = check_many(items, workers=a.workers, refresh=a.refresh)
    bad = 0
    for r in rows:
        ok = str(r["status"]) == "200"
        miss = bool(r["terms_want"]) and not r["terms_found"]
        if ok and not miss and a.bad_only:
            continue
        flag = "OK " if ok and not miss else ("DEAD" if not ok else "NOTERM")
        if flag != "OK ":
            bad += 1
        print(f"{flag}\t{r['status']}\t{r['url']}\t{r['dates']}\t{r['terms_found']}\t{r['title'][:90]}")
    print(f"# total={len(rows)} bad={bad}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
