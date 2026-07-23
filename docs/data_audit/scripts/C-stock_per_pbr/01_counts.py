"""C-stock_per_pbr 稽核 01:cache vs PG 的整表/逐年/逐日筆數對照。

Run: uv run --project . python docs/data_audit/scripts/C-stock_per_pbr/01_counts.py
需要 cache.duckdb 與本機 PostgreSQL(唯讀 attach)。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")

    print("== 整表 ==")
    print(con.sql("""
      SELECT (SELECT COUNT(*) FROM stock_per_pbr) AS cache_rows,
             (SELECT COUNT(*) FROM pg.public.stock_per_pbr_dividend_yield) AS pg_rows
    """).df().to_string())

    print("\n== 逐年 x market(差異年份)==")
    print(con.sql("""
      WITH c AS (SELECT market, year(date) y, COUNT(*) n FROM stock_per_pbr GROUP BY 1,2),
           p AS (SELECT market, year(date) y, COUNT(*) n
                 FROM pg.public.stock_per_pbr_dividend_yield GROUP BY 1,2)
      SELECT COALESCE(c.market,p.market) market, COALESCE(c.y,p.y) y,
             c.n cache_n, p.n pg_n, COALESCE(c.n,0)-COALESCE(p.n,0) diff
      FROM c FULL OUTER JOIN p ON c.market=p.market AND c.y=p.y
      WHERE COALESCE(c.n,0) <> COALESCE(p.n,0)
      ORDER BY 1,2
    """).df().to_string())

    print("\n== 逐日 x market(差異日;最多 60 行)==")
    print(con.sql("""
      WITH c AS (SELECT market, date, COUNT(*) n FROM stock_per_pbr GROUP BY 1,2),
           p AS (SELECT market, date, COUNT(*) n
                 FROM pg.public.stock_per_pbr_dividend_yield GROUP BY 1,2)
      SELECT COALESCE(c.market,p.market) market, COALESCE(c.date,p.date) date,
             c.n cache_n, p.n pg_n, COALESCE(c.n,0)-COALESCE(p.n,0) diff
      FROM c FULL OUTER JOIN p ON c.market=p.market AND c.date=p.date
      WHERE COALESCE(c.n,0) <> COALESCE(p.n,0)
      ORDER BY 2,1 LIMIT 60
    """).df().to_string())

    print("\n== 差異日總數 ==")
    print(con.sql("""
      WITH c AS (SELECT market, date, COUNT(*) n FROM stock_per_pbr GROUP BY 1,2),
           p AS (SELECT market, date, COUNT(*) n
                 FROM pg.public.stock_per_pbr_dividend_yield GROUP BY 1,2)
      SELECT COUNT(*) n_diff_days, SUM(COALESCE(c.n,0)-COALESCE(p.n,0)) net_rows
      FROM c FULL OUTER JOIN p ON c.market=p.market AND c.date=p.date
      WHERE COALESCE(c.n,0) <> COALESCE(p.n,0)
    """).df().to_string())


if __name__ == "__main__":
    main()
