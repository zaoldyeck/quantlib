"""建立 S 策略雲端 VM 用的精簡 cache(只含 S 必要表;日頻表切近 N 年)。

VM 只做 live 決策——s_advisor 看 2 年窗 + 300 日暖機(≈ 近 3 年),不需全 22 年
歷史、不需其他策略(Serenity/Evergreen/期貨)的表。從完整 cache.duckdb 切出 S 用到
的 7 張表:

- **日頻**(daily_quote / daily_trading_details / stock_per_pbr):`date >= --since`
  (預設 2023-01-01,給 2 年決策窗 + 暖機足夠餘裕);
- **月頻/不定期/衍生**(operating_revenue / ex_right_dividend / capital_reduction /
  industry_taxonomy_pit):全量(本就小,且營收 YoY / 除權息還原價需較長歷史)。

產物 `cache_s_slim.duckdb` 直接當 VM 的 `cache.duckdb`。日頻索引一併重建以維持查詢速度。

Run: uv run --project research python -m research.crawl.slim_cache [--since 2023-01-01] [--verify]
依賴:cache.duckdb 為最新。
"""
from __future__ import annotations

import argparse
import os

import duckdb

from research.crawl.sink import CACHE_DB
from research import paths

OUT = os.path.join(os.path.dirname(CACHE_DB), paths.CACHE_SLIM_DB.name)

#: S 決策 + 爬蟲會讀寫的表(唯一依賴集;見 research/apex/data.py 溯源)
DAILY_TABLES = ["daily_quote", "daily_trading_details", "stock_per_pbr"]
FULL_TABLES = ["operating_revenue", "ex_right_dividend", "capital_reduction",
               "industry_taxonomy_pit"]
#: 對齊 cache_tables.py 的索引(查詢速度)
INDEXES = [
    ("idx_dq_code_date", "daily_quote", "company_code, date"),
    ("idx_dq_date", "daily_quote", "date"),
    ("idx_pb_code_date", "stock_per_pbr", "company_code, date"),
    ("idx_cr_code_date", "capital_reduction", "company_code, date"),
]


def build(since: str, out: str = OUT) -> None:
    if os.path.exists(out):
        os.remove(out)
    con = duckdb.connect(out)
    con.execute(f"ATTACH '{CACHE_DB}' AS src (READ_ONLY)")
    try:
        for t in DAILY_TABLES:
            con.execute(f"CREATE TABLE {t} AS SELECT * FROM src.{t} "
                        f"WHERE date >= DATE '{since}'")
        for t in FULL_TABLES:
            con.execute(f"CREATE TABLE {t} AS SELECT * FROM src.{t}")
    finally:
        con.execute("DETACH src")
    for name, tbl, cols in INDEXES:
        con.execute(f"CREATE INDEX {name} ON {tbl}({cols})")
    print(f"精簡 cache → {out}(日頻自 {since})")
    for t in DAILY_TABLES + FULL_TABLES:
        cols = {c[0] for c in con.execute(f"DESCRIBE {t}").fetchall()}
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        span = ""
        for dc in ("date", "effective_date"):
            if dc in cols:
                lo, hi = con.execute(f"SELECT min({dc}), max({dc}) FROM {t}").fetchone()
                span = f"  [{lo} ~ {hi}]"
                break
        else:
            if "year" in cols:
                lo, hi = con.execute(f"SELECT min(year), max(year) FROM {t}").fetchone()
                span = f"  [{lo} ~ {hi}]"
        print(f"  {t:26} {n:>10,} 列{span}")
    con.close()
    print(f"檔案大小:{os.path.getsize(out) / 1e6:.0f} MB(完整 cache {os.path.getsize(CACHE_DB) / 1e9:.1f} GB)")


def verify(out: str = OUT) -> None:
    """把 data 層指向精簡 cache,跑 build_day_plan,確認決策可算出(表足夠)。"""
    from research.apex import data
    from research.trading.live import notify
    from research.trading.live.s_plan import build_day_plan

    data.CACHE_DB = out  # 指向精簡 cache
    con = duckdb.connect(out, read_only=True)
    con.execute(f"SET threads = {max(1, os.cpu_count() or 4)}")
    from research.crawl.sink import CACHE_DB as _rq  # noqa
    rq = os.path.join(os.path.dirname(out), "raw_quarterly.parquet")
    if os.path.exists(rq):
        con.execute(f"CREATE OR REPLACE TEMP VIEW raw_quarterly AS "
                    f"SELECT * FROM read_parquet('{rq}')")
    try:
        plan = build_day_plan(con, {"2408": 1000.0}, notify.today_taipei(), 495500.0)
        print(f"✓ 精簡 cache 可算決策:買 {plan.buys}｜賣 {plan.sells}｜備註 {plan.notes[:1]}")
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="切 S 雲端用精簡 cache")
    ap.add_argument("--since", default="2023-01-01", help="日頻表起始日(預設 2023-01-01)")
    ap.add_argument("--verify", action="store_true", help="切完跑一次 build_day_plan 驗證")
    args = ap.parse_args()
    build(args.since)
    if args.verify:
        verify()


if __name__ == "__main__":
    main()
