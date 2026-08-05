"""EV58 era-brief 證據稽核:把整張語境卡的每一條出處逐條實測。

`ev58_era_brief_check` 管的是**格式**(欄位齊不齊、連結活不活);本模組管的是
**證據**——`vocabulary` 每一組宣稱「當年有人這樣講」,那就必須在它給的那一頁上
真的找得到那個詞,而且那一頁要證明得出自己屬於那個年代。兩件事任一不成立,
下游就會拿著一個從沒存在過的詞去搜十五年前的新聞,搜不到再回報「當年沒消息」。

判定(逐條寫進報告,不做四捨五入):
- `ok`            連結活、詞命中、年代有據
- `term_missing`  頁面活著但整頁找不到宣稱的任一寫法
- `era_unproven`  詞在,但頁面拿不出屬於該年代的日期證據(URL 內嵌日期或內文年份)
- `dead`          HTTP 非 200 或內容過短(導頁/空殼)

不需要 cache.duckdb(純網路)。

Run:
    uv run --project . python -m quantlib.evergreen.ev58_era_brief_evidence \\
        --brief var/out/ev58_news/_era_brief/E3.json \\
        --years 2014 2015 \\
        --out var/out/ev58_news/_era_brief/E3_evidence.json
"""

from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from quantlib.evergreen.ev58_link_verify import check


def collect(brief: dict, years: list[str]) -> list[dict]:
    items: list[dict] = []
    for i, v in enumerate(brief.get("vocabulary", [])):
        url = str(v.get("evidence", ""))
        if url.startswith("http"):
            items.append(
                {"slot": f"vocabulary[{i}]", "label": v.get("today", ""), "url": url, "terms": list(v.get("then", [])), "era": years}
            )
    for i, m in enumerate(brief.get("macro_timeline", [])):
        url = str(m.get("source", ""))
        if url.startswith("http"):
            items.append({"slot": f"macro_timeline[{i}]", "label": m.get("date", ""), "url": url, "terms": [], "era": []})
    for i, s in enumerate(brief.get("sector_context", [])):
        url = str(s.get("source", ""))
        if url.startswith("http"):
            items.append({"slot": f"sector_context[{i}]", "label": s.get("sector", ""), "url": url, "terms": [], "era": []})
    for i, url in enumerate(brief.get("market_rules", {}).get("source", [])):
        if str(url).startswith("http"):
            items.append({"slot": f"market_rules.source[{i}]", "label": "", "url": url, "terms": [], "era": []})
    return items


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--brief", required=True)
    ap.add_argument("--years", nargs="+", required=True, help="該期涵蓋的西元年,如 2014 2015")
    ap.add_argument("--out", required=True)
    ap.add_argument("--workers", type=int, default=10)
    args = ap.parse_args()

    brief = json.loads(Path(args.brief).read_text(encoding="utf-8"))
    items = collect(brief, list(args.years))
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        results = list(ex.map(check, items))

    Path(args.out).write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
    for r in sorted(results, key=lambda x: (x["verdict"] == "ok", x["slot"])):
        hit = ",".join(f"{k}:{v}" for k, v in (r.get("per_term") or {}).items() if v) or "-"
        print(f"{r['verdict']:13s} http={r['http']:3d} url_date={str(r.get('url_date')):10s} {r['slot']:22s} {r['label'][:16]:18s} {hit[:60]:62s} {r['url'][:95]}")
    bad = [r for r in results if r["verdict"] != "ok"]
    print(f"\n{len(results) - len(bad)}/{len(results)} ok;不合格 {len(bad)} 條")


if __name__ == "__main__":
    main()
