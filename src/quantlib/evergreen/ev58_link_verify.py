"""EV58 era-brief 支援:把「出處連結」逐條機器驗證,不靠人眼相信。

era_brief 的唯一標準是「每一條都要有出處」,而出處只有同時滿足三件事才算數:
連結**活著**(HTTP 200 且不是導頁)、頁面**真的出現那個詞**、頁面**屬於該年代**
(年代靠 URL 內嵌日期或內文年份字串佐證)。憑印象貼連結會讓下游整批朝錯方向查。

輸入 JSONL 或 JSON list,每筆:
    {"url": "...", "term": "當年用語", "era": ["2018", "2019"]}

輸出:同結構加上 `http`(狀態碼)、`term_hits`、`era_hit`、`verdict`
(`ok` | `dead` | `term_missing` | `era_unproven`)。

Run:
    uv run --project . python -m quantlib.evergreen.ev58_link_verify \
        --in var/out/ev58_news/_era_brief/E2_links.json \
        --out var/out/ev58_news/_era_brief/E2_links_verified.json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
TAG_RE = re.compile(r"<[^>]+>")
CHARSET_RE = re.compile(rb"""(?i)charset\s*=\s*["']?\s*([\w-]+)""")
# 台灣舊站(聯合電子報等)是 Big5;以 UTF-8 硬解會整頁變亂碼,
# 於是「詞不在頁面上」的假陽性——這比連結壞掉更毒,因為它看起來像有查過。
_FALLBACK_ENCODINGS = ("utf-8", "big5hkscs", "cp950", "gb18030")
# 年代佐證:頁面自己寫出來的日期。民國年(103/104)在當年的官方文件裡是常態。
_DATE_PATTERNS = (
    re.compile(r"(20\d{2})[-/年\.](\d{1,2})[-/月\.](\d{1,2})"),
    re.compile(r"民國\s*(\d{2,3})\s*年\s*(\d{1,2})\s*月"),
    re.compile(r"\b(1\d{2})\.(\d{1,2})\.(\d{1,2})\b"),  # 103.05.22 式民國年
)
# URL 內嵌日期(最可信的年代證據——不受頁面改版影響)
_URL_DATE_RES = (
    re.compile(r"/(20\d{2})/(\d{1,2})/(\d{1,2})/"),  # technews.tw/2014/02/27/
    re.compile(r"/(?:newspapers|realtimenews|news)/(20\d{2})(\d{2})(\d{2})"),  # chinatimes / ctee
    re.compile(r"/post/(20\d{2})(\d{2})(\d{2})\d*/"),  # businesstoday
    re.compile(r"[?&]date=(20\d{2})(\d{2})(\d{2})"),
)


def _decode(raw: bytes) -> str:
    """先看 meta charset,再依序試;挑「替代字元最少」的解法。"""
    declared = None
    m = CHARSET_RE.search(raw[:4096])
    if m:
        declared = m.group(1).decode("ascii", "ignore").lower()
        if declared in ("big5", "big-5", "ms950"):
            declared = "cp950"
    best, best_bad = "", 10**9
    for enc in ([declared] if declared else []) + list(_FALLBACK_ENCODINGS):
        if not enc:
            continue
        try:
            txt = raw.decode(enc, "replace")
        except LookupError:
            continue
        bad = txt.count("�")
        if bad < best_bad:
            best, best_bad = txt, bad
        if bad == 0:
            break
    return best


def _strip(html: str) -> str:
    html = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    return TAG_RE.sub(" ", html)


def page_dates(url: str, text: str) -> dict:
    """回傳頁面可佐證的日期證據:URL 內嵌日期 + 內文出現的年月日/民國年。"""
    url_date = None
    for rx in _URL_DATE_RES:
        m = rx.search(url)
        if m:
            y, mo, d = (int(x) for x in m.groups())
            url_date = f"{y:04d}-{mo:02d}-{d:02d}"
            break
    years: set[int] = set()
    for rx in _DATE_PATTERNS:
        for m in rx.finditer(text[:200000]):
            y = int(m.group(1))
            years.add(y + 1911 if y < 1000 else y)
    return {"url_date": url_date, "text_years": sorted(y for y in years if 1990 <= y <= 2100)}


def check(item: dict) -> dict:
    """`term` 為單詞;`terms` 為多個同義寫法(任一命中即算)。"""
    url = item["url"]
    terms = [t for t in ([item["term"]] if item.get("term") else []) + list(item.get("terms", [])) if t]
    era = [str(e) for e in item.get("era", [])]
    out = dict(item)
    try:
        proc = subprocess.run(
            ["curl", "-sL", "-A", UA, "--max-time", "40", "-w", "\n@@HTTP:%{http_code}", url],
            capture_output=True,
            timeout=60,
        )
        raw = proc.stdout
    except Exception as exc:  # noqa: BLE001 - 網路層什麼都可能爆
        out |= {"http": 0, "term_hits": 0, "era_hit": False, "verdict": "dead", "err": str(exc)}
        return out

    code = 0
    marker = b"\n@@HTTP:"
    if marker in raw:
        raw, _, tail = raw.rpartition(marker)
        code = int(tail.decode("ascii", "ignore").strip() or 0)
    text = _strip(_decode(raw))
    per_term = {t: text.count(t) for t in terms}
    hits = max(per_term.values()) if per_term else 1
    dates = page_dates(url, text)
    era_years = {int(y[:4]) for y in era if y[:4].isdigit()}
    url_year = int(dates["url_date"][:4]) if dates["url_date"] else None
    era_hit = bool(
        (url_year is not None and url_year in era_years)
        or (era_years & set(dates["text_years"]))
        or any(y in url for y in era)
    )

    if code != 200 or len(text) < 400:
        verdict = "dead"
    elif terms and hits == 0:
        verdict = "term_missing"
    elif era and not era_hit:
        verdict = "era_unproven"
    else:
        verdict = "ok"
    out |= {
        "http": code,
        "term_hits": hits,
        "per_term": per_term,
        "era_hit": era_hit,
        "verdict": verdict,
        **dates,
    }
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    raw = Path(args.inp).read_text(encoding="utf-8").strip()
    items = json.loads(raw) if raw.startswith("[") else [json.loads(x) for x in raw.splitlines() if x.strip()]

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        results = list(ex.map(check, items))

    Path(args.out).write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
    bad = [r for r in results if r["verdict"] != "ok"]
    for r in results:
        print(f"{r['verdict']:14s} http={r['http']:3d} hits={r['term_hits']:3d} {r.get('term', ''):18s} {r['url']}")
    print(f"\n{len(results) - len(bad)}/{len(results)} ok")


if __name__ == "__main__":
    main()
