"""C-capital_reduction 稽核 01:schema 對照 + cache vs PG 全表逐欄逐鍵比對。

Run: uv run --project . python docs/data_audit/scripts/C-capital_reduction/01_schema_and_counts.py
需要 cache.duckdb 與本機 PostgreSQL(唯讀 attach)。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"
COLS = ["market", "date", "company_code",
        "post_reduction_reference_price", "reason_for_capital_reduction"]


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")

    print("== cache schema ==")
    print(con.sql("DESCRIBE capital_reduction").df().to_string())

    print("\n== PG schema ==")
    print(con.sql("""
      SELECT column_name, data_type, is_nullable, ordinal_position
      FROM pg.information_schema.columns
      WHERE table_name = 'capital_reduction' ORDER BY ordinal_position
    """).df().to_string())

    print("\n== 整表筆數 ==")
    print(con.sql("""
      SELECT (SELECT COUNT(*) FROM capital_reduction)           AS cache_rows,
             (SELECT COUNT(*) FROM pg.public.capital_reduction) AS pg_rows
    """).df().to_string())

    print("\n== 逐年 x market ==")
    print(con.sql("""
      WITH c AS (SELECT market, year(date) y, COUNT(*) n FROM capital_reduction GROUP BY 1,2),
           p AS (SELECT market, year(date) y, COUNT(*) n FROM pg.public.capital_reduction GROUP BY 1,2)
      SELECT COALESCE(c.market,p.market) market, COALESCE(c.y,p.y) y,
             COALESCE(c.n,0) cache_n, COALESCE(p.n,0) pg_n,
             COALESCE(c.n,0)-COALESCE(p.n,0) diff
      FROM c FULL OUTER JOIN p ON c.market=p.market AND c.y=p.y
      ORDER BY 1,2
    """).df().to_string())

    sel_c = ", ".join(COLS)
    print("\n== 全列 EXCEPT:cache 有 / PG 無 ==")
    print(con.sql(f"""
      SELECT * FROM (
        SELECT {sel_c} FROM capital_reduction
        EXCEPT
        SELECT {sel_c} FROM pg.public.capital_reduction
      ) ORDER BY market, date, company_code
    """).df().to_string())

    print("\n== 全列 EXCEPT:PG 有 / cache 無 ==")
    print(con.sql(f"""
      SELECT * FROM (
        SELECT {sel_c} FROM pg.public.capital_reduction
        EXCEPT
        SELECT {sel_c} FROM capital_reduction
      ) ORDER BY market, date, company_code
    """).df().to_string())

    print("\n== 共用鍵逐欄 IS DISTINCT FROM 計數 ==")
    print(con.sql("""
      SELECT COUNT(*) AS shared_keys,
             SUM(CASE WHEN c.post_reduction_reference_price
                        IS DISTINCT FROM p.post_reduction_reference_price
                      THEN 1 ELSE 0 END) AS diff_post_ref,
             SUM(CASE WHEN c.reason_for_capital_reduction
                        IS DISTINCT FROM p.reason_for_capital_reduction
                      THEN 1 ELSE 0 END) AS diff_reason
      FROM capital_reduction c
      JOIN pg.public.capital_reduction p
        ON c.market=p.market AND c.date=p.date AND c.company_code=p.company_code
    """).df().to_string())

    print("\n== 鍵層級 EXCEPT(找 key 而非整列差異)==")
    print(con.sql("""
      SELECT 'cache_only' AS side, COUNT(*) n FROM (
        SELECT market,date,company_code FROM capital_reduction
        EXCEPT SELECT market,date,company_code FROM pg.public.capital_reduction)
      UNION ALL
      SELECT 'pg_only', COUNT(*) FROM (
        SELECT market,date,company_code FROM pg.public.capital_reduction
        EXCEPT SELECT market,date,company_code FROM capital_reduction)
    """).df().to_string())


if __name__ == "__main__":
    main()
