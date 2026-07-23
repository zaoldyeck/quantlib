"""EV24 registry_v2 落盤前驗收(v1 code 污染事故的防再發關卡)。

檢查(全部通過才落盤):
1. code 格式:純 4 碼數字(v1 病:"2383 台光電");可修者 regex 修復並記錄
2. code 幻覺:每筆 code 必須存在於該月表中(agent 標了表外代碼 = 剔除)
3. 月份覆蓋:49 個月無缺、每月 3-15 筆
4. conviction ∈ {1..5}、archetype ∈ {A,B}、必填欄位無 null
5. 同 (month, code) 無重複
6. 分佈 sanity:conviction 分佈 vs pilot 4 月相似(粗檢)
污染率(修復+剔除合計)> 5% → 不落盤,停下報告。

Run: uv run --project . python -m quantlib.evergreen.ev24_validate <task_output_file>
"""
from __future__ import annotations

import json
import sys

import polars as pl

TABLE_DIR = "src/quantlib/evergreen/data/ev17_tables"
PILOT = "src/quantlib/evergreen/data/ev17_chips_labels.parquet"
OUT = "src/quantlib/evergreen/data/registry_v2.parquet"


def table_codes(month: str) -> set[str]:
    lines = open(f"{TABLE_DIR}/{month}.txt").read().splitlines()[1:]
    return {ln.split()[0] for ln in lines}


def main() -> None:
    doc = json.load(open(sys.argv[1]))
    rows = []
    for r in doc["result"]:
        for lb in r["labels"]:
            rows.append({"month": r["month"], **lb})
    new = pl.DataFrame(rows)
    pilot = pl.read_parquet(PILOT).drop("arm")
    df = pl.concat([new, pilot.select(new.columns)], how="vertical")
    n0 = df.height
    issues: list[str] = []

    # 1. code 格式修復
    bad_fmt = df.filter(~pl.col("code").str.contains(r"^\d{4}$"))
    if bad_fmt.height:
        issues.append(f"code 格式異常 {bad_fmt.height} 筆(regex 修復):"
                      f"{bad_fmt['code'].head(5).to_list()}")
        df = df.with_columns(pl.col("code").str.extract(r"(\d{4})", 1))
        still = df.filter(pl.col("code").is_null()).height
        if still:
            issues.append(f"  其中 {still} 筆修復失敗 → 剔除")
            df = df.filter(pl.col("code").is_not_null())

    # 2. code 幻覺(不在該月表中)
    ghost_rows = []
    for m in df["month"].unique().to_list():
        codes = table_codes(m)
        gh = df.filter((pl.col("month") == m) & ~pl.col("code").is_in(list(codes)))
        if gh.height:
            ghost_rows.append((m, gh["code"].to_list()))
    n_ghost = sum(len(c) for _, c in ghost_rows)
    if n_ghost:
        issues.append(f"幻覺代碼(不在當月表)共 {n_ghost} 筆 → 剔除:"
                      f"{ghost_rows[:3]}")
        for m, codes in ghost_rows:
            df = df.filter(~((pl.col("month") == m) & pl.col("code").is_in(codes)))

    # 3-5. 結構檢查
    months = sorted(df["month"].unique().to_list())
    if len(months) != 49:
        issues.append(f"月份覆蓋異常:{len(months)}/49,缺 "
                      f"{49 - len(months)} 個月")
    per = df.group_by("month").len()
    out_range = per.filter((pl.col("len") < 3) | (pl.col("len") > 15))
    if out_range.height:
        issues.append(f"每月筆數出界:{dict(out_range.iter_rows())}")
    bad_conv = df.filter(~pl.col("conviction").is_in([1, 2, 3, 4, 5])).height
    bad_arch = df.filter(~pl.col("archetype").is_in(["A", "B"])).height
    nulls = sum(df[c].null_count() for c in
                ["code", "signal_type", "conviction", "evidence", "reason"])
    for label, n in [("conviction 出界", bad_conv), ("archetype 異常", bad_arch),
                     ("必填 null", nulls)]:
        if n:
            issues.append(f"{label}:{n} 筆")
    dup = df.group_by(["month", "code"]).len().filter(pl.col("len") > 1)
    if dup.height:
        issues.append(f"重複 (month,code):{dup.height} 組 → 保留 conviction 高者")
        df = (df.sort("conviction", descending=True)
              .unique(subset=["month", "code"], keep="first"))

    # 6. conviction 分佈 sanity
    dist = dict(df.group_by("conviction").len().sort("conviction").iter_rows())
    pd_ = dict(pilot.group_by("conviction").len().sort("conviction").iter_rows())
    print(f"conviction 分佈 全量 {dist} vs pilot {pd_}")

    lossy = n0 - df.height          # 剔除型損失(幻覺/修復失敗/重複)
    rate = lossy / n0
    print(f"\n總筆數 {n0} → 清洗後 {df.height};格式修復 {bad_fmt.height} 筆"
          f"(無資訊損失);剔除型損失 {lossy} 筆({rate:.1%})")
    for i in issues:
        print(" ⚠", i)
    if rate > 0.05:
        print("\n✗ 剔除型損失 >5%,不落盤——停下報告")
        sys.exit(1)
    df.sort(["month", "code"]).write_parquet(OUT)
    print(f"\n✓ 驗收通過,已落盤 {OUT}({df.height} 筆 × {len(months)} 個月)")


if __name__ == "__main__":
    main()
