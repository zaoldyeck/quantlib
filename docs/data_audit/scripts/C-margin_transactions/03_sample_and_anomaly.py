"""C-margin_transactions ③:抽樣逐欄比對(cache vs PG)+ cache 端異常值掃描。

三段:
1. **抽樣逐欄**:3 個日期 × 5 檔股票,兩邊各拉 7 欄用 pandas `DataFrame.equals` 比。
   (全史指紋比對在 01_parity.py;這裡是給人看的第二種眼睛。)
2. **異常值**:重複主鍵 / 未來日期 / market 值域 / company_code 字元集 / NULL /
   負值 / int32 邊界 / 餘額 > 限額(物理不可能)/ 限額為 0 卻有餘額。
3. **整日內容指紋撞號**:cache 端獨立複驗「整天的資料被複製到別天」。

Run: PYTHONPATH=<repo> uv run --project . python docs/data_audit/scripts/C-margin_transactions/03_sample_and_anomaly.py
依賴:PostgreSQL(第 1 段)、var/cache/cache.duckdb(唯讀)。
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd

from research import paths

PG_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}")

COLS = ["market", "date", "company_code", "margin_balance", "short_balance",
        "margin_quota", "short_quota"]

SAMPLES = [
    ("twse", "2013-06-14", ["2330", "2317", "1101", "3008", "2412"]),
    ("tpex", "2019-11-07", ["6488", "3105", "5483", "8069", "4966"]),
    ("twse", "2026-07-17", ["2330", "2317", "1101", "3008", "2412"]),
]


def sample_check(con: duckdb.DuckDBPyConnection) -> None:
    print("=== ① 抽樣逐欄比對(3 日期 × 5 檔)===")
    for market, day, codes in SAMPLES:
        lst = ",".join(f"'{c}'" for c in codes)
        pg = con.sql(
            "SELECT market, date, company_code, "
            "       margin_balance_of_the_day AS margin_balance, "
            "       short_balance_of_the_day AS short_balance, "
            "       margin_quota, short_quota FROM pg.public.margin_transactions "
            f"WHERE market='{market}' AND date=DATE '{day}' AND company_code IN ({lst}) "
            "ORDER BY company_code").df()
        ca = con.sql(
            f"SELECT {', '.join(COLS)} FROM ca.margin_transactions "
            f"WHERE market='{market}' AND date=DATE '{day}' AND company_code IN ({lst}) "
            "ORDER BY company_code").df()
        same = pg.equals(ca)
        print(f"  {market} {day} n_pg={len(pg)} n_cache={len(ca)} equals={same}")
        if not same:
            print(pg.to_string())
            print(ca.to_string())
        elif market == "twse" and day == "2026-07-17":
            print(ca.to_string())


def anomaly(con: duckdb.DuckDBPyConnection) -> None:
    print("\n=== ② 異常值掃描(cache 全表)===")
    checks = {
        "總列數": "SELECT count(*) FROM cav",
        "重複主鍵(market,date,code)": "SELECT count(*) FROM (SELECT market,date,company_code FROM cav GROUP BY 1,2,3 HAVING count(*)>1)",
        "date 在未來": "SELECT count(*) FROM cav WHERE date > current_date",
        "market 非 twse/tpex": "SELECT count(*) FROM cav WHERE market NOT IN ('twse','tpex')",
        "company_code 非英數": "SELECT count(*) FROM cav WHERE NOT regexp_matches(company_code,'^[0-9A-Za-z]+$')",
        "任一欄 NULL": "SELECT count(*) FROM cav WHERE margin_balance IS NULL OR short_balance IS NULL OR margin_quota IS NULL OR short_quota IS NULL",
        "margin_balance < 0": "SELECT count(*) FROM cav WHERE margin_balance < 0",
        "short_balance < 0": "SELECT count(*) FROM cav WHERE short_balance < 0",
        "margin_quota < 0": "SELECT count(*) FROM cav WHERE margin_quota < 0",
        "short_quota < 0": "SELECT count(*) FROM cav WHERE short_quota < 0",
        "任一欄 |v| > 2.0e9 (int32 邊界)": "SELECT count(*) FROM cav WHERE greatest(abs(margin_balance),abs(short_balance),abs(margin_quota),abs(short_quota)) > 2000000000",
        "margin_balance > margin_quota": "SELECT count(*) FROM cav WHERE margin_balance > margin_quota",
        "short_balance > short_quota": "SELECT count(*) FROM cav WHERE short_balance > short_quota",
        "margin_quota = 0 但 margin_balance > 0": "SELECT count(*) FROM cav WHERE margin_quota = 0 AND margin_balance > 0",
        "short_quota = 0 但 short_balance > 0": "SELECT count(*) FROM cav WHERE short_quota = 0 AND short_balance > 0",
        "四欄全為 0": "SELECT count(*) FROM cav WHERE margin_balance=0 AND short_balance=0 AND margin_quota=0 AND short_quota=0",
    }
    for name, sql in checks.items():
        print(f"  {name}: {con.sql(sql).fetchone()[0]:,}")

    print("\n  值域(min/max):")
    print(con.sql("SELECT min(margin_balance) mb_min, max(margin_balance) mb_max, "
                  "min(short_balance) sb_min, max(short_balance) sb_max, "
                  "min(margin_quota) mq_min, max(margin_quota) mq_max, "
                  "min(short_quota) sq_min, max(short_quota) sq_max FROM cav").df().to_string())

    print("\n  『餘額 > 限額』的分期統計(A 維 BUG#1 的 cache 端複驗):")
    print(con.sql(
        "SELECT market, CASE WHEN date < DATE '2007-06-01' THEN 'A: <2007-06' "
        "                    WHEN date < DATE '2008-09-30' THEN 'B: 2007-06..2008-09' "
        "                    ELSE 'C: >=2008-09-30' END era, "
        "       count(*) n, count(*) FILTER (short_balance > short_quota) sb_gt_sq, "
        "       round(100.0*count(*) FILTER (short_balance > short_quota)/count(*),2) pct, "
        "       round(avg(short_quota),1) avg_sq "
        "FROM cav GROUP BY 1,2 ORDER BY 1,2").df().to_string())

    print("\n  tpex 2007-2009 逐月 avg(short_quota)(斷層應落在 2007-06 與 2008-10):")
    print(con.sql(
        "SELECT strftime(date,'%Y-%m') ym, count(*) n, round(avg(short_quota),1) avg_sq, "
        "       round(avg(margin_quota),1) avg_mq "
        "FROM cav WHERE market='tpex' AND date < DATE '2009-04-01' "
        "GROUP BY 1 ORDER BY 1").df().to_string())


def fingerprint(con: duckdb.DuckDBPyConnection) -> None:
    print("\n=== ③ 整日內容指紋撞號(cache 端獨立複驗『整天資料被複製到別天』)===")
    con.sql("CREATE OR REPLACE VIEW fp AS "
            "SELECT market, date, count(*) n, "
            "       sum(hash(company_code || ':' || margin_balance || ':' || short_balance || ':' "
            "                || margin_quota || ':' || short_quota)::HUGEINT) h "
            "FROM cav GROUP BY 1,2")
    dup = con.sql(
        "SELECT a.market ma, a.date da, b.market mb, b.date db, a.n "
        "FROM fp a JOIN fp b ON a.h = b.h AND a.n = b.n "
        "  AND (a.market, a.date) < (b.market, b.date) ORDER BY 1,2").df()
    print(f"撞號對數: {len(dup)}")
    print(dup.to_string() if len(dup) else "(無)")


def main() -> None:
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql(f"ATTACH '{paths.CACHE_DB}' AS ca (READ_ONLY)")
    con.sql("CREATE VIEW cav AS SELECT * FROM ca.margin_transactions")
    pd.set_option("display.width", 200)
    sample_check(con)
    anomaly(con)
    fingerprint(con)


if __name__ == "__main__":
    main()
