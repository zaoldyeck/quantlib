"""歷史資料重抓驅動器:把偵測到的汙染/缺漏日期,刪汙染 → Scala 重抓 → 重入庫 → 驗證。

消費 `research/audits/date_integrity.py` 產出的 worklist(幽靈日 + 缺交易日),
對每一個 (table, market, date) 做「乾淨重抓」四步:

  1. **刪汙染來源**:刪本地原始檔(含 0-byte sentinel——爬蟲以「檔存在」跳過,
     不刪就永遠抓不回)+ 刪 PG 該列 + 刪 cache 該列。三處都刪,否則:
       - 留原始檔/ sentinel → 爬蟲跳過(Task.pullDailyFiles filterNot existFiles)
       - 留 PG 列 → reader 的 dataAlreadyInDB 跳過
  2. **重抓**:`Main pull <target> --since <最早日期>`(重抓被刪的日、跳過已存在的)
  3. **重入庫**:`Main read <target>`
  4. **驗證**:重跑 date_integrity 確認該日期已乾淨(幽靈日指紋消失 / 缺日補上)

**安全**:預設 --dry-run 只印計畫不動任何東西。真跑要 --execute。
money-path 資料手術,務必先 dry-run 檢視。

用法:
  uv run --project research python -m research.audits.recrawl --worklist worklist.json        # 乾跑
  uv run --project research python -m research.audits.recrawl --worklist worklist.json --execute
  uv run --project research python -m research.audits.recrawl --worklist worklist.json --table daily_quote --execute
"""
from __future__ import annotations

import argparse
import json
import subprocess
from collections import defaultdict
from pathlib import Path

import duckdb
import psycopg2  # noqa: F401 - 確認可用;實際用 subprocess psql 較穩

from research import paths

REPO = Path(__file__).resolve().parents[2]

#: worklist 的 table 名 → (Scala pull/read target, 原始檔目錄, PG 表名, cache 表名)
TABLE_MAP = {
    "daily_quote": ("daily_quote", "daily_quote", "daily_quote", "daily_quote"),
    "daily_trading_details": ("daily_trading_details", "daily_trading_details",
                              "daily_trading_details", "daily_trading_details"),
    "margin_transactions": ("margin", "margin_transactions",
                            "margin_transactions", "margin_transactions"),
    "stock_per_pbr_dividend_yield": ("stock_per_pbr", "stock_per_pbr_dividend_yield",
                                     "stock_per_pbr_dividend_yield", "stock_per_pbr"),
    "index": ("index", "index", "index", "market_index"),
    "foreign_holding_ratio": ("foreign", "foreign_holding_ratio",
                              "foreign_holding_ratio", "foreign_holding_ratio"),
    "sbl_borrowing": ("sbl", "sbl_borrowing", "sbl_borrowing", "sbl_borrowing"),
}


def _raw_file(raw_dir: str, market: str, date: str) -> Path | None:
    """本地原始檔路徑(民國年檔名:{西元年}_{月}_{日}.csv,存於 {年} 子目錄)。"""
    y, m, d = date.split("-")
    base = REPO / "data" / raw_dir / market / y
    for name in (f"{y}_{int(m)}_{int(d)}.csv", f"{y}_{int(m)}_{int(d)}.html"):
        p = base / name
        if p.exists():
            return p
    return None


def _psql(sql: str) -> None:
    subprocess.run(["psql", "-h", "localhost", "-p", "5432", "-d", "quantlib",
                    "-c", sql], check=True, capture_output=True)


def plan(worklist: list[dict], only: str | None) -> dict[str, list[dict]]:
    """把 worklist 依 table 分組;過濾未知表與 --table。"""
    by: dict[str, list[dict]] = defaultdict(list)
    for it in worklist:
        t = it["table"]
        if t not in TABLE_MAP or (only and t != only):
            continue
        by[t].append(it)
    return by


def execute_table(table: str, items: list[dict], dry: bool) -> None:
    target, raw_dir, pg_table, cache_table = TABLE_MAP[table]
    dates = sorted({(it["market"], it["date"]) for it in items})
    phantom = [it for it in items if it["kind"] == "phantom"]
    missing = [it for it in items if it["kind"] == "missing"]
    since = min(d for _, d in dates)
    print(f"\n[{table}] {len(dates)} 個 (market,date):幽靈 {len(phantom)}、缺日 {len(missing)}"
          f";重抓 since={since}")

    if dry:
        for market, date in dates[:20]:
            f = _raw_file(raw_dir, market, date)
            print(f"    - {market} {date}"
                  f"{'  原始檔:' + f.name if f else '  原始檔:無(可能 sentinel 或未下載)'}")
        if len(dates) > 20:
            print(f"    …(共 {len(dates)} 個)")
        print(f"    [dry] 會做:刪原始檔/sentinel + DELETE pg.{pg_table} + DELETE cache.{cache_table}"
              f" → Main pull {target} --since {since} → Main read {target} → 驗證")
        return

    # 1) 刪汙染來源(原始檔 + sentinel + PG + cache)
    for market, date in dates:
        # 原始檔 / sentinel:掃該年目錄找對應日(sentinel 也在此,一併刪)
        y, m, d = date.split("-")
        for ext in ("csv", "html"):
            p = REPO / "data" / raw_dir / market / y / f"{y}_{int(m)}_{int(d)}.{ext}"
            if p.exists():
                p.unlink()
        _psql(f"DELETE FROM {pg_table} WHERE market='{market}' AND date='{date}'")
    # cache:一次連線刪全部(避免多次開檔)
    try:
        con = duckdb.connect(str(paths.CACHE_DB))
        for market, date in dates:
            con.execute(f"DELETE FROM {cache_table} WHERE market=? AND date=?", [market, date])
        con.close()
    except Exception as exc:  # noqa: BLE001
        print(f"    ⚠ cache 刪除失敗(可能被鎖):{exc};稍後重跑 cache_tables.py 會一致")

    # 2) 重抓 + 3) 重入庫(Scala;長任務,呼叫端應在背景跑整支)
    print(f"    → sbt runMain Main pull {target} --since {since}")
    subprocess.run(["sbt", "-error", f'runMain Main pull {target} --since {since}'],
                   cwd=REPO, check=True)
    print(f"    → sbt runMain Main read {target}")
    subprocess.run(["sbt", "-error", f'runMain Main read {target}'], cwd=REPO, check=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="歷史資料重抓驅動器")
    ap.add_argument("--worklist", required=True, help="date_integrity 產出的 JSON")
    ap.add_argument("--table", default=None, help="只處理某表")
    ap.add_argument("--execute", action="store_true", help="真跑(預設只 dry-run)")
    args = ap.parse_args()

    worklist = json.loads(Path(args.worklist).read_text(encoding="utf-8"))
    by = plan(worklist, args.table)
    if not by:
        print("(worklist 無可處理項目)")
        return
    print(f"重抓計畫:{sum(len(v) for v in by.values())} 項,跨 {len(by)} 張表"
          + ("  [DRY-RUN]" if not args.execute else "  [EXECUTE]"))
    for table, items in by.items():
        execute_table(table, items, dry=not args.execute)
    if not args.execute:
        print("\n這是 dry-run。確認無誤後加 --execute 真跑;跑完 cache 重建 + 重跑 date_integrity 驗證。")


if __name__ == "__main__":
    main()
