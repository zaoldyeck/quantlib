"""EV29 registry_v3 驗收+落盤:code 存在性、PIT 日期審計、覆蓋(通用站位版)。

站位日 = 每月 10 日後首交易日(自動計算)。合併 EV28 pilot 之 2025-04。
Run: uv run --project research python -m research.evergreen.ev29_validate <task_output_file>
"""
from __future__ import annotations

import json
import re
import sys
from datetime import date as Date

import duckdb
import polars as pl
from research import paths

OUT = "research/evergreen/data/registry_v3.parquet"

DATE_PATS = [
    (re.compile(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})"), lambda m: Date(int(m[1]), int(m[2]), int(m[3]))),
    (re.compile(r"(20\d{2})年(\d{1,2})月"), lambda m: Date(int(m[1]), int(m[2]), 28)),
]


def main() -> None:
    doc = json.load(open(sys.argv[1]))
    rows = []
    for r in doc["result"]:
        for lb in r["labels"]:
            rows.append({"month": r["month"], **lb})
    pilot = pl.read_parquet("research/evergreen/data/ev28_pilot_labels.parquet")
    rows += [r for r in pilot.to_dicts() if r["month"] == "2025-04"]
    df = pl.DataFrame(rows).drop("arm", strict=False)
    n0 = df.height

    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    dates = [r[0] for r in raw.execute(
        "SELECT DISTINCT date FROM daily_quote ORDER BY date").fetchall()]
    stance = {}
    for ym in df["month"].unique().to_list():
        y, m = int(ym[:4]), int(ym[5:7])
        stance[ym] = min(d for d in dates if d.year == y and d.month == m and d.day > 10)

    issues, drop = [], []
    for r in df.to_dicts():
        ok = raw.execute(
            "SELECT 1 FROM daily_quote WHERE company_code = ? AND date = ? LIMIT 1",
            [r["code"], stance[r["month"]]]).fetchone()
        if not ok:
            drop.append((r["month"], r["code"]))
    if drop:
        issues.append(f"code 存在性剔除 {len(drop)}:{drop}")
        for m, c in drop:
            df = df.filter(~((pl.col("month") == m) & (pl.col("code") == c)))

    pit = []
    for r in df.to_dicts():
        s = stance[r["month"]]
        for field in ("event", "evidence"):
            for pat, conv in DATE_PATS:
                for mm in pat.finditer(r.get(field, "") or ""):
                    try:
                        if conv(mm) > s:
                            pit.append((r["month"], r["code"], field, str(conv(mm))))
                    except ValueError:
                        pass
    if pit:
        issues.append(f"PIT 日期可疑 {len(pit)} 處:{pit[:8]}")

    months = sorted(df["month"].unique().to_list())
    print(f"月份覆蓋 {len(months)}(2024-10~2026-06 期望 21);逐月:"
          f"{dict(df.group_by('month').len().sort('month').iter_rows())}")
    print(f"總筆數 {n0} → 驗後 {df.height}")
    for i in issues:
        print(" ⚠", i)
    if (n0 - df.height) / max(n0, 1) > 0.10:
        print("✗ 剔除 >10%,不落盤")
        sys.exit(1)
    df.sort(["month", "code"]).write_parquet(OUT)
    print(f"✓ registry_v3 落盤:{df.height} 筆 × {len(months)} 個月")


if __name__ == "__main__":
    main()
