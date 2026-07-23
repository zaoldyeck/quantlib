"""C-capital_reduction 稽核 10:用「流通在外股數驟減」獨立偵測減資,量化覆蓋起點前的缺口。

減資的定義性特徵是股數變少。foreign_holding_ratio.outstanding_shares 是每日快照
(twse 2005-01-03 起、tpex 2010-01-04 起),比價格跳動乾淨得多:
  shares_drop_ratio = shares[t] / shares[t-1] < 0.9  且不是換股/合併
先用 666 筆已知減資校準,再掃 capital_reduction 覆蓋起點之前的區間,估出缺口規模。

Run: uv run --project research python docs/data_audit/scripts/C-capital_reduction/10_shares_outstanding_probe.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"

DROPS = """
CREATE OR REPLACE TEMP VIEW share_drops AS
WITH s AS (
  SELECT market, company_code, date, outstanding_shares,
         LAG(date)              OVER (PARTITION BY market, company_code ORDER BY date) prev_date,
         LAG(outstanding_shares)OVER (PARTITION BY market, company_code ORDER BY date) prev_shares
  FROM foreign_holding_ratio
  WHERE outstanding_shares > 0
)
SELECT market, company_code, date, prev_date, prev_shares, outstanding_shares,
       outstanding_shares::DOUBLE / prev_shares AS keep_ratio
FROM s
WHERE prev_shares IS NOT NULL
  AND outstanding_shares::DOUBLE / prev_shares < 0.90
  AND date_diff('day', prev_date, date) <= 21
"""


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql(DROPS)

    print("== 校準:666 筆已知減資,在 ±7 天內有沒有對應的股數驟減 ==")
    print(con.sql("""
      SELECT cr.market, COUNT(*) AS n,
             SUM(CASE WHEN EXISTS (SELECT 1 FROM share_drops d
                 WHERE d.market=cr.market AND d.company_code=cr.company_code
                   AND abs(date_diff('day', d.date, cr.date)) <= 7) THEN 1 ELSE 0 END) AS matched
      FROM pg.public.capital_reduction cr
      WHERE (cr.market='twse' AND cr.date >= DATE '2005-01-10')
         OR (cr.market='tpex' AND cr.date >= DATE '2010-01-10')
      GROUP BY 1
    """).df().to_string())

    print("\n== 股數驟減事件逐年 x market(全期)==")
    print(con.sql("""
      SELECT market, year(date) AS y, COUNT(*) AS n FROM share_drops GROUP BY 1,2 ORDER BY 1,2
    """).df().to_string())

    print("\n== 覆蓋起點『之後』的股數驟減,有多少被 capital_reduction 解釋(±7 天)==")
    print(con.sql("""
      SELECT d.market, COUNT(*) AS n,
             SUM(CASE WHEN EXISTS (SELECT 1 FROM pg.public.capital_reduction cr
                 WHERE cr.market=d.market AND cr.company_code=d.company_code
                   AND abs(date_diff('day', cr.date, d.date)) <= 7) THEN 1 ELSE 0 END) AS explained
      FROM share_drops d
      WHERE (d.market='twse' AND d.date >= DATE '2011-01-25')
         OR (d.market='tpex' AND d.date >= DATE '2013-01-16')
      GROUP BY 1
    """).df().to_string())

    print("\n== 覆蓋起點『之前』的股數驟減筆數(= 缺口下界)==")
    print(con.sql("""
      SELECT market, COUNT(*) AS n, MIN(date) AS d0, MAX(date) AS d1
      FROM share_drops
      WHERE (market='twse' AND date < DATE '2011-01-25')
         OR (market='tpex' AND date < DATE '2013-01-16')
      GROUP BY 1
    """).df().to_string())

    print("\n-- 覆蓋起點前、掉最多的 25 筆(可看出是不是真減資)--")
    print(con.sql("""
      SELECT d.market, d.date, d.company_code, d.prev_shares, d.outstanding_shares,
             ROUND(d.keep_ratio,4) keep_ratio,
             (SELECT q.closing_price FROM daily_quote q WHERE q.market=d.market
               AND q.company_code=d.company_code AND q.date<d.date AND q.closing_price>0
               ORDER BY q.date DESC LIMIT 1) pre_close,
             (SELECT q.closing_price FROM daily_quote q WHERE q.market=d.market
               AND q.company_code=d.company_code AND q.date>=d.date AND q.closing_price>0
               ORDER BY q.date ASC LIMIT 1) post_close
      FROM share_drops d
      WHERE (d.market='twse' AND d.date < DATE '2011-01-25')
         OR (d.market='tpex' AND d.date < DATE '2013-01-16')
      ORDER BY d.keep_ratio ASC LIMIT 25
    """).df().to_string())


if __name__ == "__main__":
    main()
