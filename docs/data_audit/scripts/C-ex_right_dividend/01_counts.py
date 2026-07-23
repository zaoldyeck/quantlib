"""C-ex_right_dividend 稽核 01:cache vs PG 的整表/逐年/逐日筆數對照。

cache 同步 SQL(research/cache_tables.py:42)只取 cash_dividend > 0,故直接比整表
筆數必然對不上;本腳本同時輸出「PG 全量」與「PG 過濾後」兩條基準,才能分辨
「同步條件造成的差」與「真的漏/多」。

Run: uv run --project research python docs/data_audit/scripts/C-ex_right_dividend/01_counts.py
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
      SELECT (SELECT COUNT(*) FROM ex_right_dividend) AS cache_rows,
             (SELECT COUNT(*) FROM pg.public.ex_right_dividend) AS pg_rows_all,
             (SELECT COUNT(*) FROM pg.public.ex_right_dividend WHERE cash_dividend > 0)
               AS pg_rows_filtered
    """).df().to_string())

    print("\n== cache 的 cash_dividend 分佈(同步條件應保證 >0)==")
    print(con.sql("""
      SELECT market,
             COUNT(*) n,
             COUNT(*) FILTER (WHERE cash_dividend > 0)  n_pos,
             COUNT(*) FILTER (WHERE cash_dividend = 0)  n_zero,
             COUNT(*) FILTER (WHERE cash_dividend < 0)  n_neg,
             COUNT(*) FILTER (WHERE cash_dividend IS NULL) n_null,
             MIN(date) mn, MAX(date) mx
      FROM ex_right_dividend GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n== PG 的 cash_dividend 分佈 ==")
    print(con.sql("""
      SELECT market,
             COUNT(*) n,
             COUNT(*) FILTER (WHERE cash_dividend > 0)  n_pos,
             COUNT(*) FILTER (WHERE cash_dividend = 0)  n_zero,
             COUNT(*) FILTER (WHERE cash_dividend < 0)  n_neg,
             MIN(date) mn, MAX(date) mx
      FROM pg.public.ex_right_dividend GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n== 逐年 x market(cache vs PG-filtered,只列差異年)==")
    print(con.sql("""
      WITH c AS (SELECT market, year(date) y, COUNT(*) n FROM ex_right_dividend GROUP BY 1,2),
           p AS (SELECT market, year(date) y, COUNT(*) n
                 FROM pg.public.ex_right_dividend WHERE cash_dividend > 0 GROUP BY 1,2)
      SELECT COALESCE(c.market,p.market) market, COALESCE(c.y,p.y) y,
             c.n cache_n, p.n pg_n, COALESCE(c.n,0)-COALESCE(p.n,0) diff
      FROM c FULL OUTER JOIN p ON c.market=p.market AND c.y=p.y
      WHERE COALESCE(c.n,0) <> COALESCE(p.n,0)
      ORDER BY 1,2
    """).df().to_string())

    print("\n== 逐年 x market 全表(對照用)==")
    print(con.sql("""
      WITH c AS (SELECT market, year(date) y, COUNT(*) n FROM ex_right_dividend GROUP BY 1,2),
           p AS (SELECT market, year(date) y, COUNT(*) n
                 FROM pg.public.ex_right_dividend WHERE cash_dividend > 0 GROUP BY 1,2)
      SELECT COALESCE(c.market,p.market) market, COALESCE(c.y,p.y) y,
             c.n cache_n, p.n pg_n
      FROM c FULL OUTER JOIN p ON c.market=p.market AND c.y=p.y
      ORDER BY 1,2
    """).df().to_string())

    print("\n== 逐日差異(cache vs PG-filtered)==")
    print(con.sql("""
      WITH c AS (SELECT market, date, COUNT(*) n FROM ex_right_dividend GROUP BY 1,2),
           p AS (SELECT market, date, COUNT(*) n
                 FROM pg.public.ex_right_dividend WHERE cash_dividend > 0 GROUP BY 1,2)
      SELECT COALESCE(c.market,p.market) market, COALESCE(c.date,p.date) date,
             c.n cache_n, p.n pg_n, COALESCE(c.n,0)-COALESCE(p.n,0) diff
      FROM c FULL OUTER JOIN p ON c.market=p.market AND c.date=p.date
      WHERE COALESCE(c.n,0) <> COALESCE(p.n,0)
      ORDER BY 2,1
    """).df().to_string())


if __name__ == "__main__":
    main()
