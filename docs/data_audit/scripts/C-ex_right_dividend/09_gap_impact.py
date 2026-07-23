"""C-ex_right_dividend 稽核 09:量化兩個缺口對「還原價」的實際傷害。

缺口 A:twse 2024 年除息事件大量缺漏。
  根因假設 = MOPS t108sb27 的月份參數是**公告日期**(04 腳本已證:每個
  YYYY_M.csv 內所有列的公告日期都落在該月,而除權息日往後溢出 1-2 個月),
  但 Task.pullExRightDividend:357-358 從 2024-07 才開始抓 → 2024-06(含)以前
  公告、2024-07 以後除息的事件永遠沒有來源;同時 twse legacy 端點在 2024-06
  已死,legacy 也補不到。

缺口 B:換 MOPS 後股票股利完全沒有還原(語義漂移,見 07 腳本)。

本腳本用 daily_quote 的實際價格落差當「事件真的發生過」的獨立證據。

Run: uv run --project . python docs/data_audit/scripts/C-ex_right_dividend/09_gap_impact.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402
import polars as pl  # noqa: E402
from importlib import import_module  # noqa: E402

from research import paths  # noqa: E402

_m = import_module("04_mops_file_semantics")
_s = import_module("07_semantic_drift")

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.register("mv", _s.mops_events())

    print("== A1. 2024 缺漏公司是否出現在任何 MOPS 月報檔裡 ==")
    print(con.sql("""
      WITH y AS (SELECT market, company_code, year(date) y FROM ex_right_dividend
                 WHERE cash_dividend > 0 GROUP BY 1,2,3),
      miss AS (
        SELECT a.market, a.company_code
        FROM (SELECT market, company_code FROM y WHERE y=2023
              INTERSECT SELECT market, company_code FROM y WHERE y=2025) a
        WHERE NOT EXISTS (SELECT 1 FROM y WHERE y.market=a.market
                            AND y.company_code=a.company_code AND y.y=2024))
      SELECT m.market,
             COUNT(*) n_miss,
             COUNT(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM mv WHERE mv.market=m.market
                  AND mv.company_code=m.company_code
                  AND year(coalesce(mv.ex_div, mv.ex_right))=2024)) n_in_mops_file
      FROM miss m GROUP BY 1
    """).df().to_string())

    print("\n== A2. 缺漏事件的獨立證據:2024 年 6-9 月最大單日跌幅 ==")
    print("   (真的有除息 → 該公司在 2024 夏季某天會有一筆與 2023/2025 股利率相當的跳空)")
    print(con.sql("""
      WITH y AS (SELECT market, company_code, year(date) y FROM ex_right_dividend
                 WHERE cash_dividend > 0 GROUP BY 1,2,3),
      miss AS (
        SELECT a.market, a.company_code
        FROM (SELECT market, company_code FROM y WHERE y=2023
              INTERSECT SELECT market, company_code FROM y WHERE y=2025) a
        WHERE NOT EXISTS (SELECT 1 FROM y WHERE y.market=a.market
                            AND y.company_code=a.company_code AND y.y=2024)),
      px AS (
        SELECT market, company_code, date, closing_price,
               lag(closing_price) OVER (PARTITION BY market, company_code ORDER BY date) prev
        FROM daily_quote WHERE date BETWEEN DATE '2024-06-01' AND DATE '2024-09-30'),
      worst AS (
        SELECT p.market, p.company_code, p.date, p.prev, p.closing_price,
               p.closing_price/nullif(p.prev,0)-1 AS ret,
               row_number() OVER (PARTITION BY p.market, p.company_code
                                  ORDER BY p.closing_price/nullif(p.prev,0)) rn
        FROM px p JOIN miss m USING (market, company_code)
        WHERE p.prev > 0)
      SELECT market,
             COUNT(*) n,
             COUNT(*) FILTER (WHERE ret <= -0.02) n_drop_ge_2pct,
             COUNT(*) FILTER (WHERE ret <= -0.04) n_drop_ge_4pct,
             round(median(ret), 4) med_worst_ret
      FROM worst WHERE rn = 1 GROUP BY 1
    """).df().to_string())

    print("\n   對照組:2024 有除息紀錄的 twse 公司,同期最大單日跌幅分佈")
    print(con.sql("""
      WITH have AS (SELECT DISTINCT market, company_code FROM ex_right_dividend
                    WHERE cash_dividend > 0 AND year(date)=2024),
      px AS (
        SELECT market, company_code, date, closing_price,
               lag(closing_price) OVER (PARTITION BY market, company_code ORDER BY date) prev
        FROM daily_quote WHERE date BETWEEN DATE '2024-06-01' AND DATE '2024-09-30'),
      worst AS (
        SELECT p.market, p.company_code,
               p.closing_price/nullif(p.prev,0)-1 AS ret,
               row_number() OVER (PARTITION BY p.market, p.company_code
                                  ORDER BY p.closing_price/nullif(p.prev,0)) rn
        FROM px p JOIN have h USING (market, company_code) WHERE p.prev > 0)
      SELECT market, COUNT(*) n,
             COUNT(*) FILTER (WHERE ret <= -0.02) n_drop_ge_2pct,
             round(median(ret), 4) med_worst_ret
      FROM worst WHERE rn = 1 GROUP BY 1
    """).df().to_string())

    print("\n== A3. 逐檔樣本(twse 前 12 檔):2023/2025 股利率 vs 2024 夏季最大跌幅 ==")
    print(con.sql("""
      WITH y AS (SELECT market, company_code, year(date) y FROM ex_right_dividend
                 WHERE cash_dividend > 0 GROUP BY 1,2,3),
      miss AS (
        SELECT a.market, a.company_code
        FROM (SELECT market, company_code FROM y WHERE y=2023
              INTERSECT SELECT market, company_code FROM y WHERE y=2025) a
        WHERE NOT EXISTS (SELECT 1 FROM y WHERE y.market=a.market
                            AND y.company_code=a.company_code AND y.y=2024)),
      px AS (
        SELECT market, company_code, date, closing_price,
               lag(closing_price) OVER (PARTITION BY market, company_code ORDER BY date) prev
        FROM daily_quote WHERE date BETWEEN DATE '2024-06-01' AND DATE '2024-09-30'),
      worst AS (
        SELECT p.market, p.company_code, p.date, p.prev, p.closing_price,
               p.closing_price/nullif(p.prev,0)-1 AS ret,
               row_number() OVER (PARTITION BY p.market, p.company_code
                                  ORDER BY p.closing_price/nullif(p.prev,0)) rn
        FROM px p JOIN miss m USING (market, company_code) WHERE p.prev > 0)
      SELECT w.company_code, w.date AS worst_day, w.prev, w.closing_price,
             round(w.ret, 4) worst_ret,
             e23.date d2023, e23.cash_dividend v2023,
             e25.date d2025, e25.cash_dividend v2025
      FROM worst w
      LEFT JOIN ex_right_dividend e23 ON e23.market=w.market
             AND e23.company_code=w.company_code AND year(e23.date)=2023
      LEFT JOIN ex_right_dividend e25 ON e25.market=w.market
             AND e25.company_code=w.company_code AND year(e25.date)=2025
      WHERE w.rn=1 AND w.market='twse'
      ORDER BY w.company_code LIMIT 12
    """).df().to_string())

    print("\n== B. 換 MOPS 後「有股票股利卻沒被還原」的事件數 ==")
    print(con.sql("""
      WITH e AS (
        SELECT market, date, company_code, cash_dividend,
               CASE WHEN closing_price_before_ex_right_ex_dividend = 0
                     AND ex_right_ex_dividend_reference_price = 0
                    THEN 'mops' ELSE 'legacy' END AS src
        FROM pg.public.ex_right_dividend)
      SELECT mv.market, year(mv.ex_right) y,
             COUNT(*) n_stock_events,
             COUNT(*) FILTER (WHERE e.src='mops') n_src_mops,
             COUNT(*) FILTER (WHERE e.src='legacy') n_src_legacy,
             COUNT(*) FILTER (WHERE e.src IS NULL) n_no_db_row,
             round(median(1.0/(1.0+mv.stock_div/10.0)-1), 4) med_dilution
      FROM mv LEFT JOIN e ON e.market=mv.market AND e.date=mv.ex_right
                         AND e.company_code=mv.company_code
      WHERE mv.stock_div > 0 AND mv.ex_right IS NOT NULL
      GROUP BY 1,2 ORDER BY 1,2
    """).df().to_string())

    print("\n== B2. 影響最大的 15 筆(稀釋幅度 x 是否 mops 來源)==")
    print(con.sql("""
      WITH e AS (
        SELECT market, date, company_code, cash_dividend,
               CASE WHEN closing_price_before_ex_right_ex_dividend = 0
                     AND ex_right_ex_dividend_reference_price = 0
                    THEN 'mops' ELSE 'legacy' END AS src
        FROM pg.public.ex_right_dividend)
      SELECT mv.market, mv.ex_right AS date, mv.company_code, mv.stock_div, mv.cash_div,
             e.src, e.cash_dividend AS db_cash,
             round(1.0/(1.0+mv.stock_div/10.0)-1, 4) dilution
      FROM mv LEFT JOIN e ON e.market=mv.market AND e.date=mv.ex_right
                         AND e.company_code=mv.company_code
      WHERE mv.stock_div > 0 AND mv.ex_right IS NOT NULL
        AND (e.src = 'mops' OR e.src IS NULL)
      ORDER BY mv.stock_div DESC LIMIT 15
    """).df().to_string())


if __name__ == "__main__":
    main()
