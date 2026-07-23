"""C-ex_right_dividend 稽核 05:cache vs PG 全表逐鍵逐欄比對 + 指定隨機抽樣。

比對基準 = PG 過濾 cash_dividend > 0(research/cache_tables.py:42 的同步條件)。
輸出:
  1. 雙向 EXCEPT 找獨有鍵(cache_only / pg_only)。
  2. 共同鍵的 cash_dividend 值差異筆數(IS DISTINCT FROM)。
  3. cache_only 列的來歷判定(是否為 Python 爬蟲 research/crawl 直寫 cache 的新資料)。
  4. 指定抽樣:3 個日期 x 5 檔,逐欄印出兩邊的值。

Run: uv run --project research python docs/data_audit/scripts/C-ex_right_dividend/05_value_parity.py
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"
SEED = 20260722


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql("""
      CREATE OR REPLACE TEMP VIEW p AS
      SELECT market, date, company_code, cash_dividend
      FROM pg.public.ex_right_dividend WHERE cash_dividend > 0
    """)

    print("== 鍵集合雙向差集 ==")
    print(con.sql("""
      SELECT
        (SELECT COUNT(*) FROM (SELECT market,date,company_code FROM ex_right_dividend
                               EXCEPT SELECT market,date,company_code FROM p)) AS cache_only,
        (SELECT COUNT(*) FROM (SELECT market,date,company_code FROM p
                               EXCEPT SELECT market,date,company_code FROM ex_right_dividend)) AS pg_only
    """).df().to_string())

    print("\n== 共同鍵的 cash_dividend 差異筆數 ==")
    print(con.sql("""
      SELECT COUNT(*) AS shared,
             COUNT(*) FILTER (WHERE c.cash_dividend IS DISTINCT FROM p.cash_dividend) AS diff
      FROM ex_right_dividend c JOIN p USING (market, date, company_code)
    """).df().to_string())

    print("\n== 差異樣本(最多 30 筆)==")
    print(con.sql("""
      SELECT c.market, c.date, c.company_code,
             c.cash_dividend AS cache_v, p.cash_dividend AS pg_v
      FROM ex_right_dividend c JOIN p USING (market, date, company_code)
      WHERE c.cash_dividend IS DISTINCT FROM p.cash_dividend
      ORDER BY c.date LIMIT 30
    """).df().to_string())

    print("\n== cache_only 明細(cache 有、PG-filtered 沒有)==")
    print(con.sql("""
      SELECT c.*, pg_all.cash_dividend AS pg_raw_cash, pg_all.right_or_dividend AS pg_kind
      FROM ex_right_dividend c
      LEFT JOIN pg.public.ex_right_dividend pg_all USING (market, date, company_code)
      WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.market=c.market AND p.date=c.date
                          AND p.company_code=c.company_code)
      ORDER BY c.date, c.market, c.company_code
    """).df().to_string())

    print("\n== pg_only 明細(PG-filtered 有、cache 沒有)==")
    print(con.sql("""
      SELECT p.* FROM p
      WHERE NOT EXISTS (SELECT 1 FROM ex_right_dividend c WHERE c.market=p.market
                          AND c.date=p.date AND c.company_code=p.company_code)
      ORDER BY p.date LIMIT 50
    """).df().to_string())

    print("\n== 指定隨機抽樣:3 日 x 5 檔,逐欄比對 ==")
    rng = random.Random(SEED)
    dates = [r[0] for r in con.sql("""
        SELECT DISTINCT date FROM ex_right_dividend
        WHERE date < DATE '2026-01-01'
        GROUP BY date HAVING COUNT(*) >= 5 ORDER BY date
    """).fetchall()]
    picked = rng.sample(dates, 3)
    ok = bad = 0
    for d in picked:
        codes = [r[0] for r in con.sql(
            f"SELECT company_code FROM ex_right_dividend WHERE date=DATE '{d}' "
            f"ORDER BY company_code").fetchall()]
        for c in rng.sample(codes, min(5, len(codes))):
            row = con.sql(f"""
              SELECT c.market, c.date, c.company_code, c.cash_dividend AS cache_cash,
                     p.cash_dividend AS pg_cash
              FROM ex_right_dividend c JOIN p USING (market, date, company_code)
              WHERE c.date = DATE '{d}' AND c.company_code = '{c}'
            """).df()
            same = (not row.empty) and bool((row["cache_cash"] == row["pg_cash"]).all())
            ok, bad = (ok + 1, bad) if same else (ok, bad + 1)
            print(f"  {d} {c}: {'OK ' if same else 'MISMATCH'} {row.to_dict('records')}")
    print(f"抽樣結果:{ok} 同 / {bad} 異")


if __name__ == "__main__":
    main()
