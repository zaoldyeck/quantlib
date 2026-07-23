"""C-margin_transactions ①:cache 與 PostgreSQL 的全史逐欄一致性(非抽樣)。

方法:DuckDB 同時 ATTACH PG(READ_ONLY)與 cache 檔,兩邊都以 (market, date) 分組算
三個指紋——列數、`sum(hash(全部 7 欄)::HUGEINT)`、`bit_xor(hash(全部 7 欄))`——再
FULL JOIN 對照。三個數字同時相等,才算該日逐列逐欄相同(sum 抓值差、bit_xor 抓
順序無關的集合差、count 抓缺多)。

另外印:schema 型別對照、逐年逐市場列數、cache 索引唯一性。

Run: uv run --project . python docs/data_audit/scripts/C-margin_transactions/01_parity.py
依賴:PostgreSQL 在跑;var/cache/cache.duckdb 存在(不寫入,唯讀)。
"""
from __future__ import annotations

import os

import duckdb

from research import paths

PG_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}")

#: cache_tables.py:47 的投影(唯一真源);PG 側必須用同樣的改名才能比對
PG_PROJ = ("SELECT market, date, company_code, "
           "margin_balance_of_the_day AS margin_balance, "
           "short_balance_of_the_day AS short_balance, "
           "margin_quota, short_quota FROM pg.public.margin_transactions")

COLS = "market, date, company_code, margin_balance, short_balance, margin_quota, short_quota"

FP = (f"SELECT market, date, count(*) AS n, "
      f"       sum(hash({COLS})::HUGEINT) AS h_sum, "
      f"       bit_xor(hash({COLS})) AS h_xor "
      f"FROM {{src}} GROUP BY 1, 2")


def main() -> None:
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql(f"ATTACH '{paths.CACHE_DB}' AS ca (READ_ONLY)")

    print("=== schema 對照 ===")
    print("PG:")
    print(con.sql("SELECT column_name, data_type, is_nullable FROM pg.information_schema.columns "
                  "WHERE table_name='margin_transactions' ORDER BY ordinal_position").df().to_string())
    print("cache:")
    print(con.sql("DESCRIBE ca.margin_transactions").df().to_string())

    con.sql(f"CREATE VIEW pgv AS {PG_PROJ}")
    con.sql("CREATE VIEW cav AS SELECT * FROM ca.margin_transactions")

    print("\n=== 全表列數 ===")
    print(con.sql("SELECT (SELECT count(*) FROM pgv) AS pg_rows, "
                  "       (SELECT count(*) FROM cav) AS cache_rows").df().to_string())

    print("\n=== 逐年逐市場列數(不符者列出)===")
    yr = con.sql(
        "WITH p AS (SELECT market, year(date) y, count(*) n FROM pgv GROUP BY 1,2), "
        "     c AS (SELECT market, year(date) y, count(*) n FROM cav GROUP BY 1,2) "
        "SELECT coalesce(p.market,c.market) market, coalesce(p.y,c.y) y, "
        "       p.n pg_n, c.n cache_n FROM p FULL JOIN c USING (market, y) "
        "WHERE p.n IS DISTINCT FROM c.n ORDER BY 1,2").df()
    print(f"不符的年×市場格數: {len(yr)}")
    print(yr.to_string() if len(yr) else "(全部相同)")
    tot = con.sql("WITH p AS (SELECT market, year(date) y FROM pgv), "
                  "c AS (SELECT market, year(date) y FROM cav) "
                  "SELECT count(*) FROM (SELECT DISTINCT market,y FROM p UNION "
                  "SELECT DISTINCT market,y FROM c)").fetchone()[0]
    print(f"總共比對 {tot} 個年×市場格")

    print("\n=== 全史 (market,date) 指紋比對 ===")
    con.sql(f"CREATE VIEW pfp AS {FP.format(src='pgv')}")
    con.sql(f"CREATE VIEW cfp AS {FP.format(src='cav')}")
    j = con.sql(
        "SELECT coalesce(p.market,c.market) market, coalesce(p.date,c.date) date, "
        "       p.n pg_n, c.n cache_n, "
        "       (p.h_sum IS NOT DISTINCT FROM c.h_sum) sum_eq, "
        "       (p.h_xor IS NOT DISTINCT FROM c.h_xor) xor_eq "
        "FROM pfp p FULL JOIN cfp c USING (market, date)")
    con.sql("CREATE VIEW jj AS " + j.sql_query())
    print(con.sql(
        "SELECT count(*) AS days_total, "
        "       count(*) FILTER (pg_n IS NULL) AS only_cache, "
        "       count(*) FILTER (cache_n IS NULL) AS only_pg, "
        "       count(*) FILTER (pg_n IS NOT NULL AND cache_n IS NOT NULL AND "
        "                        (pg_n<>cache_n OR NOT sum_eq OR NOT xor_eq)) AS mismatch "
        "FROM jj").df().to_string())
    bad = con.sql("SELECT * FROM jj WHERE pg_n IS NULL OR cache_n IS NULL OR "
                  "pg_n<>cache_n OR NOT sum_eq OR NOT xor_eq ORDER BY market, date").df()
    print(bad.to_string() if len(bad) else "(零差異)")

    print("\n=== cache 索引 / 主鍵唯一性 ===")
    print(con.sql("SELECT database_name, index_name, is_unique, sql FROM duckdb_indexes() "
                  "WHERE table_name='margin_transactions'").df().to_string())
    print(con.sql("SELECT count(*) AS dup_keys FROM (SELECT market,date,company_code "
                  "FROM cav GROUP BY 1,2,3 HAVING count(*)>1)").df().to_string())


if __name__ == "__main__":
    main()
