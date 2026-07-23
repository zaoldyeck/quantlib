"""C-capital_reduction 稽核 08:反向驗證——市場上有減資特徵、表裡卻沒紀錄的事件。

減資的市場特徵:停牌數個交易日 → 復牌當天價格相對停牌前收盤大跳(遠超漲跌幅
上限)。把 daily_quote 裡符合這個特徵的事件全撈出來,扣掉 capital_reduction 與
ex_right_dividend 已解釋的,剩下的就是「疑似漏抓的減資」候選。

先用已知的 666 筆減資校準這個偵測器的召回率,再看未解釋的殘量。

Run: uv run --project research python docs/data_audit/scripts/C-capital_reduction/08_unexplained_jumps.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"

JUMP_SQL = """
WITH q AS (
  SELECT market, company_code, date, closing_price,
         LAG(date)          OVER (PARTITION BY market, company_code ORDER BY date) prev_date,
         LAG(closing_price) OVER (PARTITION BY market, company_code ORDER BY date) prev_close
  FROM daily_quote
  WHERE closing_price > 0
),
gaps AS (
  SELECT *, closing_price/prev_close AS ratio,
         date_diff('day', prev_date, date) AS gap_days
  FROM q WHERE prev_close IS NOT NULL
)
SELECT * FROM gaps
WHERE gap_days BETWEEN 3 AND 60
  AND (ratio > 1.25 OR ratio < 0.75)
"""


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql(f"CREATE OR REPLACE TEMP VIEW jumps AS {JUMP_SQL}")

    print("== 偵測器召回率校準:666 筆已知減資中,有多少被這個特徵抓到 ==")
    print(con.sql("""
      SELECT cr.market, COUNT(*) n,
             SUM(CASE WHEN j.company_code IS NOT NULL THEN 1 ELSE 0 END) detected
      FROM pg.public.capital_reduction cr
      LEFT JOIN jumps j
        ON j.market=cr.market AND j.company_code=cr.company_code AND j.date=cr.date
      GROUP BY 1
    """).df().to_string())

    print("\n== 全部跳動事件數 vs 已被 capital_reduction / ex_right_dividend 解釋 ==")
    print(con.sql("""
      SELECT j.market,
             COUNT(*) total,
             SUM(CASE WHEN cr.company_code IS NOT NULL THEN 1 ELSE 0 END) by_capred,
             SUM(CASE WHEN cr.company_code IS NULL AND er.company_code IS NOT NULL THEN 1 ELSE 0 END) by_exright,
             SUM(CASE WHEN cr.company_code IS NULL AND er.company_code IS NULL THEN 1 ELSE 0 END) unexplained
      FROM jumps j
      LEFT JOIN pg.public.capital_reduction cr
        ON cr.market=j.market AND cr.company_code=j.company_code AND cr.date=j.date
      LEFT JOIN pg.public.ex_right_dividend er
        ON er.market=j.market AND er.company_code=j.company_code AND er.date=j.date
      GROUP BY 1
    """).df().to_string())

    print("\n== 未解釋事件逐年(疑似漏抓減資的上界)==")
    print(con.sql("""
      SELECT j.market, year(j.date) y, COUNT(*) n,
             SUM(CASE WHEN j.ratio > 1.25 THEN 1 ELSE 0 END) up,
             SUM(CASE WHEN j.ratio < 0.75 THEN 1 ELSE 0 END) down
      FROM jumps j
      LEFT JOIN pg.public.capital_reduction cr
        ON cr.market=j.market AND cr.company_code=j.company_code AND cr.date=j.date
      LEFT JOIN pg.public.ex_right_dividend er
        ON er.market=j.market AND er.company_code=j.company_code AND er.date=j.date
      WHERE cr.company_code IS NULL AND er.company_code IS NULL
      GROUP BY 1,2 ORDER BY 1,2
    """).df().to_string())

    print("\n== 未解釋且『向上跳』(減資的典型方向)且發生在有減資資料的年份 ==")
    print(con.sql("""
      SELECT j.market, j.date, j.company_code, j.prev_date, j.prev_close, j.closing_price,
             ROUND(j.ratio,3) ratio, j.gap_days
      FROM jumps j
      LEFT JOIN pg.public.capital_reduction cr
        ON cr.market=j.market AND cr.company_code=j.company_code AND cr.date=j.date
      LEFT JOIN pg.public.ex_right_dividend er
        ON er.market=j.market AND er.company_code=j.company_code AND er.date=j.date
      WHERE cr.company_code IS NULL AND er.company_code IS NULL
        AND j.ratio > 1.25
        AND ((j.market='twse' AND j.date >= DATE '2011-01-25')
          OR (j.market='tpex' AND j.date >= DATE '2013-01-16'))
      ORDER BY j.date
    """).df().to_string())

    print("\n== 減資資料起點之前(twse<2011-01-25 / tpex<2013-01-16)的未解釋向上跳 ==")
    print(con.sql("""
      SELECT j.market, year(j.date) y, COUNT(*) n
      FROM jumps j
      LEFT JOIN pg.public.ex_right_dividend er
        ON er.market=j.market AND er.company_code=j.company_code AND er.date=j.date
      WHERE er.company_code IS NULL AND j.ratio > 1.25
        AND ((j.market='twse' AND j.date < DATE '2011-01-25')
          OR (j.market='tpex' AND j.date < DATE '2013-01-16'))
      GROUP BY 1,2 ORDER BY 1,2
    """).df().to_string())


if __name__ == "__main__":
    main()
