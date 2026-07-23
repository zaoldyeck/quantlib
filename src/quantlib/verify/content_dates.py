"""內容日 vs 檔名日交叉驗證 —— 抓「錯日汙染」(raw 檔內容其實是別天的資料)。

**動機**:BUG_TRACKER(2026-07-23)的錯日類(#3 daily_quote 2009-12-12 裝 12-18、#6 dtd 23
個日期裝別天、#13 foreign 只認檔名不看內容)是 dimension① 內容正確性缺陷——raw_coverage
只驗「有沒有」不驗「對不對」。TWSE 日報表首行帶民國日期頭(`NN年NN月NN日`),據此比對檔名
編碼日,不一致=該檔內容被寫錯日(下載游標錯位/週六補班檔裝週間資料)。

這類多已被 remediation 重爬修掉(抽驗 5 檔全對),本工具做**全量定案**:掃全日源 raw,
逐檔比對。回報每源 mismatch 數;0 = 該源錯日類已清。

Run: uv run --project . python -m quantlib.verify.content_dates
唯讀,不改資料。
"""
from __future__ import annotations

import re
from datetime import date as Date

from quantlib import paths

_ROC_HEADER = re.compile(r"(\d{2,3})年(\d{1,2})月(\d{1,2})日")  # 民國日期頭(首 300 bytes)
_FNAME = re.compile(r"^(\d{4})_(\d{1,2})_(\d{1,2})$")

#: 帶民國日期頭、可驗內容日的日源(raw 在 <source>/<market>/<year>/Y_M_D.csv)。
_DAILY_SOURCES = [
    "daily_quote", "daily_trading_details", "margin_transactions",
    "foreign_holding_ratio", "market_index", "stock_per_pbr", "sbl_borrowing",
]


def _content_date(raw: bytes) -> Date | None:
    """從首行民國日期頭抽內容日(Big5);無頭回 None。"""
    head = raw[:300].decode("big5", errors="replace")
    m = _ROC_HEADER.search(head)
    if not m:
        return None
    roc_y, mo, d = (int(m.group(i)) for i in (1, 2, 3))
    try:
        return Date(roc_y + 1911, mo, d)  # 民國 + 1911 = 西元
    except ValueError:
        return None


def _filename_date(stem: str) -> Date | None:
    m = _FNAME.match(stem)
    if not m:
        return None
    try:
        return Date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def scan_source(source: str) -> dict:
    """掃一個日源:回 {scanned, checked, mismatches:[(path, fn, ct)], no_header}。"""
    base = paths.RAW / source
    scanned = checked = no_header = 0
    mismatches: list[tuple[str, Date, Date]] = []
    for f in base.rglob("*.csv"):
        if not f.is_file() or f.stat().st_size == 0:  # sentinel 跳過
            continue
        fn = _filename_date(f.stem)
        if fn is None:
            continue
        scanned += 1
        ct = _content_date(f.read_bytes())
        if ct is None:
            no_header += 1
            continue
        checked += 1
        if ct != fn:
            mismatches.append((str(f.relative_to(paths.RAW)), fn, ct))
    return {"source": source, "scanned": scanned, "checked": checked,
            "no_header": no_header, "mismatches": mismatches}


def main() -> None:
    print("=== 內容日 vs 檔名日交叉驗證(錯日汙染偵測;raw 首行民國日期頭)===\n")
    total_mis = 0
    for source in _DAILY_SOURCES:
        r = scan_source(source)
        n = len(r["mismatches"])
        total_mis += n
        status = "✓ 無錯日" if n == 0 else f"❌ {n} 檔錯日"
        print(f"  {r['source']:26} 掃 {r['scanned']:>5} / 驗 {r['checked']:>5}"
              f"(無頭 {r['no_header']:>4}) → {status}")
        for path, fn, ct in r["mismatches"][:10]:
            print(f"      {path}: 檔名 {fn} ≠ 內容 {ct}")
        if n > 10:
            print(f"      ...(+{n - 10} 更多)")
    print(f"\n總計錯日檔:{total_mis}"
          + ("  → 全日源內容日=檔名日,錯日汙染類已清(BUG_TRACKER #3/#6/#13 定案修復)"
             if total_mis == 0 else "  → 需逐檔重爬修正"))


if __name__ == "__main__":
    main()
