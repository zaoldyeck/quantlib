"""C-foreign_holding_ratio 稽核 05:全史原始檔「檔名日期 vs 內容日期」比對。

- twse: Big5-HKSCS CSV,第一行標題含民國日期,如「115年07月17日 外資及陸資投資持股統計」
- tpex: UTF-8 JSON,tables[0].date 為「115/04/24」民國日期

Reader(src/main/scala/reader/TradingReader.scala:899-901)只從檔名取日期、從不讀內容日期,
所以只要端點回了別天的快照,DB 就會把它當成請求日的資料。

Run: uv run --project research python docs/data_audit/scripts/C-foreign_holding_ratio/05_filename_vs_content_date.py
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

REPO = Path(__file__).resolve().parents[4]
BASE = REPO / "data" / "foreign_holding_ratio"

TWSE_HDR = re.compile(r"(\d{2,3})年(\d{1,2})月(\d{1,2})日")
MINGUO_SLASH = re.compile(r"^(\d{2,3})/(\d{1,2})/(\d{1,2})$")
FNAME = re.compile(r"^(\d{4})_(\d{1,2})_(\d{1,2})\.csv$")


def roc(y: int, m: int, d: int) -> date:
    return date(y + 1911, m, d)


def scan(market: str):
    rows = []
    empty = 0
    noheader = []
    for f in sorted((BASE / market).rglob("*.csv")):
        mm = FNAME.match(f.name)
        if not mm:
            continue
        fdate = date(int(mm.group(1)), int(mm.group(2)), int(mm.group(3)))
        raw = f.read_bytes()
        if len(raw) <= 10:
            empty += 1
            continue
        cdate = None
        if market == "twse":
            head = raw[:400].decode("big5-hkscs", errors="replace")
            m2 = TWSE_HDR.search(head)
            if m2:
                cdate = roc(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
        else:
            try:
                j = json.loads(raw.decode("utf-8", errors="replace"))
                ds = (j.get("tables") or [{}])[0].get("date")
                if ds:
                    m2 = MINGUO_SLASH.match(ds.strip())
                    if m2:
                        cdate = roc(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
            except Exception:  # noqa: BLE001
                pass
        if cdate is None:
            noheader.append(f.name)
            continue
        rows.append((fdate, cdate, f))
    return rows, empty, noheader


def main() -> None:
    for market in ("twse", "tpex"):
        rows, empty, noheader = scan(market)
        bad = [(fd, cd, f) for fd, cd, f in rows if fd != cd]
        print(f"\n===== [{market}] 有內容檔 {len(rows)};空回應(<=10B){empty};無日期標題 {len(noheader)} =====")
        print(f"  檔名日期 != 內容日期:{len(bad)} 檔")
        by_year = Counter(fd.year for fd, _, _ in bad)
        print(f"  錯位年份分佈:{dict(sorted(by_year.items()))}")
        content_years = Counter(cd for _, cd, _ in bad)
        print(f"  錯位檔的『真實內容日』前 10:{content_years.most_common(10)}")
        for fd, cd, f in bad[:40]:
            print(f"    {f.relative_to(REPO)}  檔名={fd}  內容={cd}")
        if len(bad) > 40:
            print(f"    ... 共 {len(bad)} 檔")
        if noheader:
            print(f"  無日期標題樣本:{noheader[:10]}")


if __name__ == "__main__":
    main()
