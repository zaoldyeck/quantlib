"""EV58 era-brief 出廠檢查:交件前把「說得出口的規矩」變成會紅燈的機制。

era_brief 有四條硬規矩,散文寫著沒用,交件前必須逐條機器檢:
1. JSON 合法且必要欄位齊全;
2. `vocabulary` 至少 20 組,且每組都附了連結(附不出連結的不准寫);
3. 卡片裡出現的每個外部連結都活著(HTTP 200,實際 curl 過);
4. 不碰個股——全文不得出現 4 位數股票代號樣式。

Run:
    uv run --project . python -m quantlib.evergreen.ev58_era_brief_check \
        --brief var/out/ev58_news/_era_brief/E2.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from quantlib.evergreen.ev58_link_verify import check

# 中文行文會用「、」「,」串接多個網址,不排除會把兩條黏成一條假死連結。
URL_RE = re.compile(r"https?://[^\s\"'()<>,、,;;:：)】]+")
# 台股代號:獨立 4 位數。年份 1900~2099 一律排除——本卡片滿是年份,
# 而 20xx 段又剛好與鋼鐵類股代號重疊,不排除會被年份淹沒到看不見真陽性。
# 代價:真的寫到 2015~2029 這段代號時會漏掉,故公司「名稱」仍須人工複核。
CODE_RE = re.compile(r"(?<![0-9A-Za-z./%-])(?!19[0-9]{2}|20[0-9]{2})([1-9][0-9]{3})(?![0-9A-Za-z.%-])")
MIN_VOCAB = 20
REQUIRED = ("era_code", "macro_timeline", "vocabulary", "market_rules", "media_landscape", "sector_context", "uncertain")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--brief", required=True)
    ap.add_argument("--skip-links", action="store_true")
    args = ap.parse_args()

    raw = Path(args.brief).read_text(encoding="utf-8")
    brief = json.loads(raw)  # 不合法直接爆,這就是第一道檢查
    fails: list[str] = []

    for key in REQUIRED:
        if key not in brief:
            fails.append(f"缺欄位 {key}")

    vocab = brief.get("vocabulary", [])
    if len(vocab) < MIN_VOCAB:
        fails.append(f"vocabulary 只有 {len(vocab)} 組,規格要求 >= {MIN_VOCAB}")
    for v in vocab:
        if not str(v.get("evidence", "")).startswith("http"):
            fails.append(f"vocabulary 無連結: {v.get('today')}")

    for i, ev in enumerate(brief.get("macro_timeline", [])):
        for f in ("date", "event", "market_impact", "source"):
            if not ev.get(f):
                fails.append(f"macro_timeline[{i}] 缺 {f}")

    # 個股防線:代號樣式(公司名無法機器窮舉,靠人工複核,此處只擋代號)
    for m in CODE_RE.finditer(raw):
        line = raw[: m.start()].count("\n") + 1
        fails.append(f"疑似個股代號 {m.group(1)}(第 {line} 行)")

    urls = sorted(set(URL_RE.findall(raw)))
    print(f"外部連結 {len(urls)} 條;vocabulary {len(vocab)} 組;macro_timeline {len(brief.get('macro_timeline', []))} 條")

    if not args.skip_links:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=10) as ex:
            results = list(ex.map(lambda u: check({"url": u, "term": "", "era": []}), urls))
        for r in results:
            if r["verdict"] != "ok":
                fails.append(f"連結不通({r['verdict']}, http={r['http']}): {r['url']}")

    if fails:
        print("\n".join(f"FAIL  {f}" for f in fails))
        sys.exit(1)
    print("PASS  era-brief 出廠檢查全過")


if __name__ == "__main__":
    main()
