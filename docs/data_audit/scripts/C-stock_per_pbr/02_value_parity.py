"""C-stock_per_pbr 稽核 02:cache vs PG 全表逐鍵逐欄值比對(不抽樣)。

鍵 = (market, date, company_code)。比對 price_book_ratio / dividend_yield /
price_to_earning_ratio 三欄,NULL 對 NULL 視為相同。

Run: uv run --project research python docs/data_audit/scripts/C-stock_per_pbr/02_value_parity.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"
COLS = ["price_book_ratio", "dividend_yield", "price_to_earning_ratio"]


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql("SET memory_limit='8GB'")

    con.sql("""
      CREATE OR REPLACE TEMP VIEW p AS
      SELECT market, date, company_code, price_book_ratio, dividend_yield,
             price_to_earning_ratio
      FROM pg.public.stock_per_pbr_dividend_yield
    """)

    print("== 鍵集合差異(cache-only / pg-only)==")
    print(con.sql("""
      SELECT 'cache_only' side, COUNT(*) n FROM (
        SELECT market,date,company_code FROM stock_per_pbr
        EXCEPT SELECT market,date,company_code FROM p)
      UNION ALL
      SELECT 'pg_only', COUNT(*) FROM (
        SELECT market,date,company_code FROM p
        EXCEPT SELECT market,date,company_code FROM stock_per_pbr)
    """).df().to_string())

    print("\n== cache-only 鍵的日期分佈 ==")
    print(con.sql("""
      SELECT market, date, COUNT(*) n FROM (
        SELECT market,date,company_code FROM stock_per_pbr
        EXCEPT SELECT market,date,company_code FROM p)
      GROUP BY 1,2 ORDER BY 2,1 LIMIT 30
    """).df().to_string())

    print("\n== pg-only 鍵的日期分佈 ==")
    print(con.sql("""
      SELECT market, date, COUNT(*) n FROM (
        SELECT market,date,company_code FROM p
        EXCEPT SELECT market,date,company_code FROM stock_per_pbr)
      GROUP BY 1,2 ORDER BY 2,1 LIMIT 30
    """).df().to_string())

    sel = ", ".join(
        f"COUNT(*) FILTER (WHERE c.{c} IS DISTINCT FROM p.{c}) AS d_{c}" for c in COLS)
    print("\n== 共用鍵逐欄不一致筆數 ==")
    print(con.sql(f"""
      SELECT COUNT(*) shared, {sel}
      FROM stock_per_pbr c JOIN p USING (market, date, company_code)
    """).df().to_string())

    for c in COLS:
        df = con.sql(f"""
          SELECT market, date, company_code, c.{c} AS cache_v, p.{c} AS pg_v
          FROM stock_per_pbr c JOIN p USING (market, date, company_code)
          WHERE c.{c} IS DISTINCT FROM p.{c}
          ORDER BY date LIMIT 10
        """).df()
        if len(df):
            print(f"\n-- {c} 不一致樣本 --")
            print(df.to_string())


if __name__ == "__main__":
    main()
