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


def _strip(html: str) -> str:
    html = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    return TAG_RE.sub(" ", html)


def check(item: dict) -> dict:
    url = item["url"]
    term = item.get("term") or ""
    era = [str(e) for e in item.get("era", [])]
    out = dict(item)
    try:
        proc = subprocess.run(
            ["curl", "-sL", "-A", UA, "--max-time", "40", "-w", "\n@@HTTP:%{http_code}", url],
            capture_output=True,
            timeout=60,
        )
        body = proc.stdout.decode("utf-8", "ignore")
    except Exception as exc:  # noqa: BLE001 - 網路層什麼都可能爆
        out |= {"http": 0, "term_hits": 0, "era_hit": False, "verdict": "dead", "err": str(exc)}
        return out

    code = 0
    if "@@HTTP:" in body:
        body, _, tail = body.rpartition("@@HTTP:")
        code = int(tail.strip() or 0)
    text = _strip(body)
    hits = text.count(term) if term else 1
    # 年代佐證:URL 內嵌日期優先(最可信),否則看內文是否出現該年份
    era_hit = any(y in url for y in era) or any(f"{y}" in text for y in era)

    if code != 200 or len(text) < 400:
        verdict = "dead"
    elif term and hits == 0:
        verdict = "term_missing"
    elif era and not era_hit:
        verdict = "era_unproven"
    else:
        verdict = "ok"
    out |= {"http": code, "term_hits": hits, "era_hit": era_hit, "verdict": verdict}
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
