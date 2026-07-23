"""C-daily_trading_details / 步驟 1:cache vs PostgreSQL 全史逐欄比對。

做法:兩邊各自對 (market, date) 分組,算出「列數 + 四個數值欄的雜湊和 + 雜湊 XOR」
三個指紋,再 FULL JOIN 比對。雜湊和抓數值差異、XOR 抓順序無關的集合差異、
count 抓筆數差異——三者同時相等才算逐位一致(非抽樣,是全體)。

用法:uv run --project . python docs/data_audit/scripts/C-daily_trading_details/01_parity.py
依賴:var/cache/cache.duckdb 為當前世代;PostgreSQL localhost:5432/quantlib。
"""
from __future__ import annotations

import os

import duckdb

from research import paths

PG_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}",
)

# cache 端欄名 → PG 端欄名(cache 對 trust 欄做了改名)
COLS = [
    ("foreign_investors_difference", "foreign_investors_difference"),
    ("trust_difference", "securities_investment_trust_companies_difference"),
    ("dealers_difference", "dealers_difference"),
    ("total_difference", "total_difference"),
]


def fingerprint(alias: str, table: str, cache_side: bool) -> str:
    cols = [(c if cache_side else p) for c, p in COLS]
    tup = ", ".join(cols)
    return f"""
        SELECT market, date,
               count(*)                                AS n,
               sum(hash({tup})::HUGEINT)               AS hsum,
               bit_xor(hash({tup}))                    AS hxor
        FROM {alias}.{table}
        GROUP BY 1, 2
    """


def main() -> None:
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql(f"ATTACH '{paths.CACHE_DB}' AS ca (READ_ONLY)")

    con.sql(f"CREATE TEMP TABLE fp_pg AS {fingerprint('pg.public', 'daily_trading_details', False)}")
    con.sql(f"CREATE TEMP TABLE fp_ca AS {fingerprint('ca', 'daily_trading_details', True)}")

    print("== 全表列數 ==")
    print(con.sql("""
        SELECT 'pg' AS side, sum(n) AS rows, count(*) AS days FROM fp_pg
        UNION ALL
        SELECT 'cache', sum(n), count(*) FROM fp_ca
    """).df().to_string(index=False))

    print("\n== 共同 (market,date) 上指紋不符者 ==")
    print(con.sql("""
        SELECT p.market, p.date, p.n AS n_pg, c.n AS n_cache,
               p.hsum = c.hsum AS hsum_eq, p.hxor = c.hxor AS hxor_eq
        FROM fp_pg p JOIN fp_ca c USING (market, date)
        WHERE p.n <> c.n OR p.hsum IS DISTINCT FROM c.hsum OR p.hxor IS DISTINCT FROM c.hxor
        ORDER BY 1, 2
    """).df().to_string(index=False))

    print("\n== 只在 cache ==")
    print(con.sql("""
        SELECT c.market, c.date, c.n FROM fp_ca c
        LEFT JOIN fp_pg p USING (market, date) WHERE p.market IS NULL ORDER BY 1,2
    """).df().to_string(index=False))

    print("\n== 只在 PG ==")
    print(con.sql("""
        SELECT p.market, p.date, p.n FROM fp_pg p
        LEFT JOIN fp_ca c USING (market, date) WHERE c.market IS NULL ORDER BY 1,2
    """).df().to_string(index=False))

    print("\n== 逐年列數 ==")
    print(con.sql("""
        SELECT year(date) AS y, market,
               sum(CASE WHEN side='pg' THEN n END)    AS pg,
               sum(CASE WHEN side='cache' THEN n END) AS cache
        FROM (SELECT 'pg' AS side, * FROM fp_pg UNION ALL SELECT 'cache', * FROM fp_ca)
        GROUP BY 1, 2 ORDER BY 1, 2
    """).df().to_string(index=False))


if __name__ == "__main__":
    main()
