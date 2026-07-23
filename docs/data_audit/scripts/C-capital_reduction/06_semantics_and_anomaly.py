"""C-capital_reduction 稽核 06:數值異常 + 語意自洽 + 對 daily_quote 的外部驗證。

檢查:
  A. 數值異常:NULL / <=0 / 未來日期 / 重複鍵 / 代號格式 / 減資原因分類。
  B. 語意自洽(用 PG 才有的欄位):
     - closing_price_on_the_last_trading_date 是否等於 daily_quote 在恢復買賣日
       前一個交易日的收盤價(cache 丟掉這欄,prices.py 改用 daily_quote 重建
       pre_close;兩者若不等,還原因子就會錯)。
     - post_reduction_reference_price 是否落在恢復買賣日的當日 low~high 之間
       (參考價 = 開盤競價基準的來源)。
  C. 外部驗證:daily_quote 裡「停牌數日後價格大跳」但 capital_reduction /
     ex_right_dividend 都沒有紀錄的事件 = 疑似漏抓的減資。

Run: uv run --project . python docs/data_audit/scripts/C-capital_reduction/06_semantics_and_anomaly.py
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

    print("== A. cache 數值異常 ==")
    print(con.sql("""
      SELECT COUNT(*) n,
             SUM(CASE WHEN post_reduction_reference_price IS NULL THEN 1 ELSE 0 END) post_null,
             SUM(CASE WHEN post_reduction_reference_price <= 0 THEN 1 ELSE 0 END)    post_le0,
             SUM(CASE WHEN date > CURRENT_DATE THEN 1 ELSE 0 END)                    future_date,
             SUM(CASE WHEN reason_for_capital_reduction IS NULL
                        OR trim(reason_for_capital_reduction)='' THEN 1 ELSE 0 END)  reason_blank,
             SUM(CASE WHEN NOT regexp_matches(company_code,'^[0-9]{4}[0-9A-Z]?$')
                      THEN 1 ELSE 0 END)                                             odd_code
      FROM capital_reduction
    """).df().to_string())

    print("\n-- 未來日期明細 --")
    print(con.sql("SELECT * FROM capital_reduction WHERE date > CURRENT_DATE ORDER BY date")
          .df().to_string())

    print("\n-- 重複鍵 --")
    print(con.sql("""
      SELECT market,date,company_code,COUNT(*) n FROM capital_reduction
      GROUP BY 1,2,3 HAVING COUNT(*)>1
    """).df().to_string())

    print("\n-- 減資原因分類 --")
    print(con.sql("""
      SELECT reason_for_capital_reduction, COUNT(*) n,
             MIN(date) d0, MAX(date) d1
      FROM capital_reduction GROUP BY 1 ORDER BY n DESC
    """).df().to_string())

    print("\n-- post_ref / pre_close 隱含還原因子分佈(PG 欄位)--")
    print(con.sql("""
      SELECT market,
             COUNT(*) n,
             ROUND(MIN(post_reduction_reference_price/closing_price_on_the_last_trading_date),4) f_min,
             ROUND(quantile_cont(post_reduction_reference_price/closing_price_on_the_last_trading_date,0.5),4) f_p50,
             ROUND(MAX(post_reduction_reference_price/closing_price_on_the_last_trading_date),4) f_max,
             SUM(CASE WHEN post_reduction_reference_price/closing_price_on_the_last_trading_date
                        NOT BETWEEN 0.05 AND 5.0 THEN 1 ELSE 0 END) AS dropped_by_prices_guard
      FROM pg.public.capital_reduction GROUP BY 1
    """).df().to_string())

    print("\n-- 被 prices.py 的 0.05<f<5 護欄丟掉的事件 --")
    print(con.sql("""
      SELECT market,date,company_code,company_name,
             closing_price_on_the_last_trading_date pre, post_reduction_reference_price post,
             ROUND(post_reduction_reference_price/closing_price_on_the_last_trading_date,3) f,
             reason_for_capital_reduction
      FROM pg.public.capital_reduction
      WHERE post_reduction_reference_price/closing_price_on_the_last_trading_date
            NOT BETWEEN 0.05 AND 5.0
      ORDER BY date
    """).df().to_string())

    print("\n== B1. pre_close(PG 欄) vs daily_quote 前一交易日收盤 ==")
    print(con.sql("""
      WITH cr AS (
        SELECT market,date,company_code,company_name,
               closing_price_on_the_last_trading_date pre_declared,
               post_reduction_reference_price post_ref
        FROM pg.public.capital_reduction
      ),
      j AS (
        SELECT cr.*, (
          SELECT q.closing_price FROM daily_quote q
          WHERE q.market=cr.market AND q.company_code=cr.company_code
            AND q.date < cr.date AND q.closing_price > 0
          ORDER BY q.date DESC LIMIT 1) AS pre_from_dq,
          (SELECT q.date FROM daily_quote q
          WHERE q.market=cr.market AND q.company_code=cr.company_code
            AND q.date < cr.date AND q.closing_price > 0
          ORDER BY q.date DESC LIMIT 1) AS pre_dq_date
        FROM cr
      )
      SELECT COUNT(*) n,
             SUM(CASE WHEN pre_from_dq IS NULL THEN 1 ELSE 0 END) no_prior_quote,
             SUM(CASE WHEN pre_from_dq IS NOT NULL
                       AND abs(pre_from_dq-pre_declared) < 0.005 THEN 1 ELSE 0 END) exact,
             SUM(CASE WHEN pre_from_dq IS NOT NULL
                       AND abs(pre_from_dq-pre_declared) >= 0.005 THEN 1 ELSE 0 END) mismatch
      FROM j
    """).df().to_string())

    print("\n-- 不一致明細(前 40)--")
    print(con.sql("""
      WITH cr AS (
        SELECT market,date,company_code,company_name,
               closing_price_on_the_last_trading_date pre_declared,
               post_reduction_reference_price post_ref
        FROM pg.public.capital_reduction),
      j AS (
        SELECT cr.*, (
          SELECT q.closing_price FROM daily_quote q
          WHERE q.market=cr.market AND q.company_code=cr.company_code
            AND q.date < cr.date AND q.closing_price > 0
          ORDER BY q.date DESC LIMIT 1) AS pre_from_dq,
          (SELECT q.date FROM daily_quote q
          WHERE q.market=cr.market AND q.company_code=cr.company_code
            AND q.date < cr.date AND q.closing_price > 0
          ORDER BY q.date DESC LIMIT 1) AS pre_dq_date
        FROM cr)
      SELECT market,date,company_code,company_name,pre_declared,pre_from_dq,pre_dq_date,
             ROUND(post_ref/pre_declared,4) f_declared,
             ROUND(post_ref/pre_from_dq,4)  f_dq
      FROM j
      WHERE pre_from_dq IS NOT NULL AND abs(pre_from_dq-pre_declared) >= 0.005
      ORDER BY abs(post_ref/pre_declared - post_ref/pre_from_dq) DESC
      LIMIT 40
    """).df().to_string())

    print("\n== B2. post_ref 是否落在恢復買賣日當天的 low~high ==")
    print(con.sql("""
      WITH j AS (
        SELECT cr.market,cr.date,cr.company_code,cr.company_name,
               cr.post_reduction_reference_price post_ref,
               q.opening_price o, q.lowest_price l, q.highest_price h, q.closing_price c
        FROM pg.public.capital_reduction cr
        LEFT JOIN daily_quote q
          ON q.market=cr.market AND q.company_code=cr.company_code AND q.date=cr.date)
      SELECT COUNT(*) n,
             SUM(CASE WHEN o IS NULL THEN 1 ELSE 0 END) no_quote_that_day,
             SUM(CASE WHEN o IS NOT NULL AND post_ref BETWEEN l*0.9 AND h*1.1 THEN 1 ELSE 0 END) in_band,
             SUM(CASE WHEN o IS NOT NULL AND NOT (post_ref BETWEEN l*0.9 AND h*1.1) THEN 1 ELSE 0 END) out_band
      FROM j
    """).df().to_string())

    print("\n-- 落在區間外的明細(前 30)--")
    print(con.sql("""
      WITH j AS (
        SELECT cr.market,cr.date,cr.company_code,cr.company_name,
               cr.post_reduction_reference_price post_ref,
               cr.closing_price_on_the_last_trading_date pre,
               q.opening_price o, q.lowest_price l, q.highest_price h, q.closing_price c
        FROM pg.public.capital_reduction cr
        LEFT JOIN daily_quote q
          ON q.market=cr.market AND q.company_code=cr.company_code AND q.date=cr.date)
      SELECT * FROM j
      WHERE o IS NOT NULL AND NOT (post_ref BETWEEN l*0.9 AND h*1.1)
      ORDER BY date LIMIT 30
    """).df().to_string())


if __name__ == "__main__":
    main()
