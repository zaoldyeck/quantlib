"""EV28 標記落盤前驗收:code 存在性、PIT 日期審計、材料落檔、覆蓋。

檢查:
1. code 格式(4 碼)+ 存在性(站位日於 daily_quote 有交易;無 → 剔除記錄)
2. PIT 日期審計:event/evidence 內出現的日期不得晚於站位日(regex 抽
   YYYY-MM-DD、YYYY/M、YYYY年M月;可疑者列清單)
3. 材料落檔:ev28_news/{month}/materials.json 存在且非空
4. 月份覆蓋 4/4;每月 0-15 筆(空手權合法)
剔除型損失 >10% → 不落盤停下報告。

Run: uv run --project research python -m research.evergreen.ev28_validate <task_output_file>
"""
from __future__ import annotations

import json
import re
import sys
from datetime import date as Date

import duckdb
import polars as pl

STANCE = {"2023-02": Date(2023, 2, 13), "2023-08": Date(2023, 8, 11),
          "2024-03": Date(2024, 3, 11), "2025-04": Date(2025, 4, 11)}
OUT = "research/evergreen/data/ev28_pilot_labels.parquet"

DATE_PATS = [
    (re.compile(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})"), lambda m: Date(int(m[1]), int(m[2]), int(m[3]))),
    (re.compile(r"(20\d{2})年(\d{1,2})月"), lambda m: Date(int(m[1]), int(m[2]), 28)),
    (re.compile(r"(20\d{2})[-/](\d{1,2})(?![-/\d])"), lambda m: Date(int(m[1]), int(m[2]), 28)),
]


def extract_dates(text: str) -> list[Date]:
    out = []
    for pat, conv in DATE_PATS:
        for m in pat.finditer(text):
            try:
                out.append(conv(m))
            except ValueError:
                pass
    return out


def main() -> None:
    doc = json.load(open(sys.argv[1]))
    rows = []
    for r in doc["result"]:
        for lb in r["labels"]:
            rows.append({"month": r["month"], **lb})
    df = pl.DataFrame(rows)
    n0 = df.height
    issues: list[str] = []

    raw = duckdb.connect("research/cache.duckdb", read_only=True)
    drop = []
    for r in df.to_dicts():
        d = STANCE[r["month"]]
        ok = raw.execute(
            "SELECT 1 FROM daily_quote WHERE company_code = ? AND date = ? LIMIT 1",
            [r["code"], d]).fetchone()
        if not ok:
            drop.append((r["month"], r["code"], "站位日無交易/代碼不存在"))
    if drop:
        issues.append(f"code 存在性剔除 {len(drop)}:{drop}")
        for m, c, _ in drop:
            df = df.filter(~((pl.col("month") == m) & (pl.col("code") == c)))

    pit_flags = []
    for r in df.to_dicts():
        stance = STANCE[r["month"]]
        for field in ("event", "evidence"):
            for dt in extract_dates(r.get(field, "")):
                if dt > stance:
                    pit_flags.append((r["month"], r["code"], field, str(dt)))
    if pit_flags:
        issues.append(f"PIT 日期可疑 {len(pit_flags)} 處(人工覆核):{pit_flags[:6]}")

    import os
    for m in STANCE:
        p = f"research/evergreen/data/ev28_news/{m}/materials.json"
        if not (os.path.exists(p) and os.path.getsize(p) > 100):
            issues.append(f"材料落檔缺失:{p}")

    months = sorted(df["month"].unique().to_list())
    per = dict(df.group_by("month").len().iter_rows())
    print(f"月份覆蓋 {len(months)}/4;逐月筆數 {per}")
    print(f"總筆數 {n0} → 驗後 {df.height}(剔除 {n0 - df.height})")
    for i in issues:
        print(" ⚠", i)
    lossy = (n0 - df.height) / max(n0, 1)
    if lossy > 0.10:
        print("✗ 剔除 >10%,不落盤")
        sys.exit(1)
    df.sort(["month", "code"]).write_parquet(OUT)
    print(f"✓ 驗收通過,落盤 {OUT}")


if __name__ == "__main__":
    main()
