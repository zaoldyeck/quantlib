"""C-capital_reduction 稽核 09:未解釋跳動的分流。

08 找出的「未解釋向上跳」有三種:
  (a) 表裡其實有這檔的減資紀錄,只是日期差幾天(宣告的恢復買賣日碰到颱風假/延後);
  (b) 槓桿反向 ETF / ETN 的反分割(不是減資,交易所不列在 TWTAUU);
  (c) 真的漏抓的減資。
本腳本對每一筆做 ±30 天的減資紀錄比對,並標出 ETF 代號,把 (c) 篩出來。

Run: uv run --project . python docs/data_audit/scripts/C-capital_reduction/09_unexplained_triage.py
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

    con.sql("""
      CREATE OR REPLACE TEMP VIEW jumps AS
      WITH q AS (
        SELECT market, company_code, date, closing_price,
               LAG(date)          OVER (PARTITION BY market, company_code ORDER BY date) prev_date,
               LAG(closing_price) OVER (PARTITION BY market, company_code ORDER BY date) prev_close
        FROM daily_quote WHERE closing_price > 0),
      gaps AS (
        SELECT *, closing_price/prev_close AS ratio, date_diff('day', prev_date, date) gap_days
        FROM q WHERE prev_close IS NOT NULL)
      SELECT * FROM gaps WHERE gap_days BETWEEN 3 AND 60 AND ratio > 1.25
    """)

    print("== 有減資資料的年份、未被同日紀錄解釋的向上跳:分流 ==")
    print(con.sql("""
      SELECT j.market, j.date, j.company_code,
             (SELECT COUNT(*) FROM etf e WHERE e.company_code=j.company_code) is_etf,
             j.prev_date, j.prev_close, j.closing_price, ROUND(j.ratio,3) ratio, j.gap_days,
             (SELECT MIN(cr.date) FROM pg.public.capital_reduction cr
              WHERE cr.market=j.market AND cr.company_code=j.company_code
                AND abs(date_diff('day', cr.date, j.date)) <= 30) near_cr_date,
             (SELECT MIN(cr.post_reduction_reference_price) FROM pg.public.capital_reduction cr
              WHERE cr.market=j.market AND cr.company_code=j.company_code
                AND abs(date_diff('day', cr.date, j.date)) <= 30) near_cr_post
      FROM jumps j
      LEFT JOIN pg.public.capital_reduction cr
        ON cr.market=j.market AND cr.company_code=j.company_code AND cr.date=j.date
      LEFT JOIN pg.public.ex_right_dividend er
        ON er.market=j.market AND er.company_code=j.company_code AND er.date=j.date
      WHERE cr.company_code IS NULL AND er.company_code IS NULL
        AND ((j.market='twse' AND j.date >= DATE '2011-01-25')
          OR (j.market='tpex' AND j.date >= DATE '2013-01-16'))
      ORDER BY j.date
    """).df().to_string())

    print("\n== 剩下的『真嫌疑』:非 ETF、±30 天內無任何減資紀錄 ==")
    print(con.sql("""
      SELECT j.market, j.date, j.company_code,
             j.prev_date, j.prev_close, j.closing_price, ROUND(j.ratio,3) ratio, j.gap_days
      FROM jumps j
      LEFT JOIN pg.public.capital_reduction cr
        ON cr.market=j.market AND cr.company_code=j.company_code AND cr.date=j.date
      LEFT JOIN pg.public.ex_right_dividend er
        ON er.market=j.market AND er.company_code=j.company_code AND er.date=j.date
      WHERE cr.company_code IS NULL AND er.company_code IS NULL
        AND ((j.market='twse' AND j.date >= DATE '2011-01-25')
          OR (j.market='tpex' AND j.date >= DATE '2013-01-16'))
        AND (SELECT COUNT(*) FROM etf e WHERE e.company_code=j.company_code) = 0
        AND NOT EXISTS (SELECT 1 FROM pg.public.capital_reduction cr2
                        WHERE cr2.market=j.market AND cr2.company_code=j.company_code
                          AND abs(date_diff('day', cr2.date, j.date)) <= 30)
      ORDER BY j.date
    """).df().to_string())


if __name__ == "__main__":
    main()
