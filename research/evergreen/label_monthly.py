"""Evergreen 月中標記日工具(/evergreen-label command 的配套腳本)。

三個子命令(月份格式 YYYY-MM,可多個):

  prompt   組裝該月標記提示詞(逐字取凍結檔 PROMPT_ev28_labeling.md +
           ev27_phil_inline.txt,{date}/{month} 代入)→ 寫
           data/prompts/{month}.txt 並印站位日
  validate 驗收 agent 輸出 JSON(--input 檔:{"month":..,"labels":[..]}):
           code 站位日存在性、PIT 日期審計、材料落檔、筆數 0-15;
           剔除型損失 >10% 即失敗
  merge    驗收通過後落盤 registry_v3(先備份;month+code 去重,新資料勝)

Run:
  uv run --project research python -m research.evergreen.label_monthly prompt 2026-07
  uv run --project research python -m research.evergreen.label_monthly validate --input out.json
  uv run --project research python -m research.evergreen.label_monthly merge --input out.json
依賴 cache: 是(站位日與 code 存在性查 daily_quote)
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from datetime import date as Date
from pathlib import Path

import duckdb
import polars as pl

EG = Path("research/evergreen")
REGISTRY = EG / "data" / "registry_v3.parquet"
PROMPT_MD = EG / "PROMPT_ev28_labeling.md"
PHIL = EG / "data" / "ev27_phil_inline.txt"
NEWS_DIR = EG / "data" / "ev28_news"
PROMPTS_DIR = EG / "data" / "prompts"

DATE_PATS = [
    (re.compile(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})"),
     lambda m: Date(int(m[1]), int(m[2]), int(m[3]))),
    (re.compile(r"(20\d{2})年(\d{1,2})月"), lambda m: Date(int(m[1]), int(m[2]), 28)),
    (re.compile(r"(20\d{2})[-/](\d{1,2})(?![-/\d])"), lambda m: Date(int(m[1]), int(m[2]), 28)),
]


def cache() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("research/cache.duckdb", read_only=True)


def stance_day(month: str) -> Date:
    """該月站位日 = 每月 10 日後第一個交易日。

    歷史月從 cache 交易曆查;當月標記日當天 cache 尚無該日行情(未收盤),
    fallback 用「10 日後第一個非週末日」估算(颱風/國定假日誤差由
    validate 的 code 存在性檢查兜底)。
    """
    y, m = map(int, month.split("-"))
    row = cache().execute(
        "SELECT min(date) FROM daily_quote WHERE date > ?", [Date(y, m, 10)]
    ).fetchone()
    if row and row[0] is not None:
        d = row[0]
        return d if isinstance(d, Date) else d.date()
    d = Date(y, m, 11)
    while d.weekday() >= 5:
        d = Date.fromordinal(d.toordinal() + 1)
    print(f"  (站位日估算 {d}:cache 尚無該日行情——當月站位日當天屬常態)")
    return d


def build_prompt(month: str) -> tuple[str, Date]:
    """逐字取凍結提示詞組裝;任何偏離凍結檔的需求都必須回到使用者裁決。"""
    d = stance_day(month)
    seg = PROMPT_MD.read_text().split("## 標記提示詞", 1)[1]
    body = seg.split("\n---\n", 1)[0].split("\n\n", 1)[1].strip()
    assert "{哲學全文}" in body, "凍結提示詞結構變動——停止,回報使用者"
    pr = (body.replace("{哲學全文}", PHIL.read_text())
              .replace("{date}", d.isoformat()).replace("{month}", month))
    assert "{date}" not in pr and "{month}" not in pr
    return pr, d


def cmd_prompt(months: list[str]) -> None:
    PROMPTS_DIR.mkdir(parents=True, exist_ok=True)
    for month in months:
        pr, d = build_prompt(month)
        out = PROMPTS_DIR / f"{month}.txt"
        out.write_text(pr)
        print(f"{month} 站位日 {d}:提示詞 {len(pr):,} 字 → {out}")
        if d > Date.today():
            print(f"  ⚠ 站位日未到,今天不可標記 {month}")


def load_labels(path: str) -> tuple[str, pl.DataFrame]:
    doc = json.loads(Path(path).read_text())
    month, labels = doc["month"], doc.get("labels", [])
    cols = ["code", "name", "theme", "signal_type", "event", "evidence",
            "invalidation", "conviction"]
    df = (pl.DataFrame(labels, schema_overrides={"code": pl.Utf8})
          if labels else pl.DataFrame(schema={c: pl.Utf8 for c in cols}))
    return month, df.with_columns(pl.lit(month).alias("month"))


def universe_drop(df: pl.DataFrame, d: Date) -> list[str]:
    """回傳 universe 外(站位日無交易/不存在於上市上櫃)應剔除的 codes。

    validate 與 merge 共用——單一事實源,merge 落盤的一定是剔除後資料。
    """
    con = cache()
    dmax = con.execute("SELECT max(date) FROM daily_quote").fetchone()[0]
    dmax = dmax if isinstance(dmax, Date) else dmax.date()
    d_chk = min(d, dmax)  # 當月站位日尚未入 cache 時,以最近交易日驗存在性
    return [r["code"] for r in df.to_dicts()
            if not con.execute("SELECT 1 FROM daily_quote WHERE company_code=? "
                               "AND date=? LIMIT 1", [r["code"], d_chk]).fetchone()]


def cmd_validate(path: str) -> None:
    month, df = load_labels(path)
    d = stance_day(month)
    n0 = df.height
    issues: list[str] = []
    if n0 > 15:
        sys.exit(f"✗ {month} 標記 {n0} 筆 > 15 上限,不落盤")

    drop = universe_drop(df, d)
    if drop:
        issues.append(f"code 站位日無交易/不存在(興櫃/未上市櫃),剔除:{drop}")
        df = df.filter(~pl.col("code").is_in(drop))

    def _dates(text: str):
        for pat, conv in DATE_PATS:
            for m in pat.finditer(text):
                try:
                    yield conv(m)
                except ValueError:  # 「2023/2024」等跨年寫法誤中 regex
                    continue

    pit = [(r["code"], f, str(dt))
           for r in df.to_dicts() for f in ("event", "evidence")
           for dt in _dates(r.get(f) or "") if dt > d]
    if pit:
        issues.append(f"PIT 日期晚於站位日 {d},人工覆核:{pit[:6]}")

    mat = NEWS_DIR / month / "materials.json"
    if not (mat.exists() and mat.stat().st_size > 100):
        issues.append(f"材料落檔缺失:{mat}(agent 必須存搜尋材料)")

    print(f"{month} 站位日 {d}:{n0} 筆 → 驗後 {df.height}(剔除 {n0 - df.height})")
    for i in issues:
        print(" ⚠", i)
    if n0 and (n0 - df.height) / n0 > 0.10:
        sys.exit("✗ 剔除 >10%,不落盤——檢查 agent 輸出")
    print("✓ 驗收通過(merge 前請人工掃過 ⚠ 項)")


def cmd_merge(path: str) -> None:
    month, df = load_labels(path)
    d = stance_day(month)
    drop = universe_drop(df, d)
    if drop:
        print(f"  merge 前剔除 universe 外 codes:{drop}(原始檔保留全量)")
        df = df.filter(~pl.col("code").is_in(drop))
    cols = ["month", "code", "name", "theme", "signal_type", "event",
            "evidence", "invalidation", "conviction"]
    df = df.select(cols).with_columns(pl.col("conviction").cast(pl.Int64))
    reg = pl.read_parquet(REGISTRY)
    bak = REGISTRY.with_suffix(f".backup_{Date.today():%Y%m%d}.parquet")
    shutil.copy(REGISTRY, bak)
    merged = (pl.concat([reg.filter(pl.col("month") != month), df])
              .sort(["month", "code"]))
    merged.write_parquet(REGISTRY)
    print(f"✓ registry_v3:{reg.height} → {merged.height} 筆"
          f"(月份 {reg['month'].n_unique()} → {merged['month'].n_unique()};"
          f"備份 {bak.name})")
    print(f"  {month}:{df.height} 筆入冊 "
          f"{sorted(df['code'].to_list()) if df.height else '(空手月)'}")


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p1 = sub.add_parser("prompt")
    p1.add_argument("months", nargs="+")
    for name in ("validate", "merge"):
        p = sub.add_parser(name)
        p.add_argument("--input", required=True)
    a = ap.parse_args()
    if a.cmd == "prompt":
        cmd_prompt(a.months)
    elif a.cmd == "validate":
        cmd_validate(a.input)
    else:
        cmd_merge(a.input)


if __name__ == "__main__":
    main()
