"""C-ex_right_dividend 稽核 11:用 TWSE BWIBBU(stock_per_pbr)獨立還原 2024 缺漏的除息日與金額。

原理:TWSE「個股日本益比、殖利率及股價淨值比」的殖利率 = 最近一次已配發的每股現金
股利 / 當日收盤價,而且**在除息當日換成新一年度的股利**。所以
    implied_div(t) = dividend_yield(t)/100 × closing_price(t)
是一條階梯函數,階梯的跳點就是除息日、跳到的高度就是該年度現金股利。
這條線來自完全獨立的另一支 TWSE 端點,可以當 ex_right_dividend 的外部對照。

流程:
  (1) 對照組驗證:在 2024 有 ex_right_dividend 紀錄的公司,用本法還原的日期/金額
      是否對得上(方法可信度)。
  (2) 套用到 309 檔「2023、2025 有、2024 沒有」的公司,輸出還原出的 2024 除息日與金額。

Run: uv run --project . python docs/data_audit/scripts/C-ex_right_dividend/11_recover_missing_from_bwibbu.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    con.sql("""
      CREATE OR REPLACE TEMP VIEW imp AS
      SELECT p.company_code, p.date,
             p.dividend_yield/100.0 * q.closing_price AS implied_div
      FROM stock_per_pbr p
      JOIN daily_quote q USING (market, date, company_code)
      WHERE p.market='twse' AND p.dividend_yield IS NOT NULL
        AND q.closing_price > 0
        AND p.date BETWEEN DATE '2023-11-01' AND DATE '2025-03-31'
    """)

    # 階梯偵測:同代號、依日期,implied_div 相對前一交易日變動 > 5%(且絕對值 > 0.05)
    con.sql("""
      CREATE OR REPLACE TEMP VIEW steps AS
      WITH s AS (
        SELECT company_code, date, implied_div,
               lag(implied_div) OVER (PARTITION BY company_code ORDER BY date) prev_div
        FROM imp)
      SELECT company_code, date, prev_div, implied_div
      FROM s
      WHERE prev_div IS NOT NULL
        AND abs(implied_div - prev_div) > 0.05
        AND abs(implied_div - prev_div) / nullif(prev_div, 0) > 0.05
        AND date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
    """)

    print("== (1) 對照組:2024 有紀錄的公司,還原日期是否命中 ==")
    print(con.sql("""
      WITH rec AS (SELECT company_code, date, cash_dividend FROM ex_right_dividend
                   WHERE market='twse' AND cash_dividend > 0 AND year(date)=2024),
      m AS (
        SELECT r.company_code, r.date AS rec_date, r.cash_dividend,
               min(abs(datediff('day', s.date, r.date))) AS min_gap
        FROM rec r LEFT JOIN steps s USING (company_code)
        GROUP BY 1,2,3)
      SELECT COUNT(*) n_records,
             COUNT(*) FILTER (WHERE min_gap = 0) hit_same_day,
             COUNT(*) FILTER (WHERE min_gap <= 2) hit_within_2d,
             COUNT(*) FILTER (WHERE min_gap IS NULL) no_step_found
      FROM m
    """).df().to_string())

    print("\n== (2) miss 組:BWIBBU 還原的 2024 除息日 + 金額 ==")
    print(con.sql("""
      WITH y AS (SELECT company_code, year(date) y FROM ex_right_dividend
                 WHERE market='twse' AND cash_dividend > 0 GROUP BY 1,2),
      miss AS (
        SELECT a.company_code FROM
          (SELECT company_code FROM y WHERE y=2023
           INTERSECT SELECT company_code FROM y WHERE y=2025) a
        WHERE a.company_code NOT IN (SELECT company_code FROM y WHERE y=2024))
      SELECT COUNT(DISTINCT company_code) n_companies_with_step,
             COUNT(*) n_steps
      FROM steps WHERE company_code IN (SELECT company_code FROM miss)
    """).df().to_string())

    print("\n  逐檔(取 2024 年變動最大的那一步,前 40 檔)")
    print(con.sql("""
      WITH y AS (SELECT company_code, year(date) y FROM ex_right_dividend
                 WHERE market='twse' AND cash_dividend > 0 GROUP BY 1,2),
      miss AS (
        SELECT a.company_code FROM
          (SELECT company_code FROM y WHERE y=2023
           INTERSECT SELECT company_code FROM y WHERE y=2025) a
        WHERE a.company_code NOT IN (SELECT company_code FROM y WHERE y=2024)),
      best AS (
        SELECT s.*, row_number() OVER (PARTITION BY s.company_code
                                       ORDER BY abs(s.implied_div - s.prev_div) DESC) rn
        FROM steps s WHERE s.company_code IN (SELECT company_code FROM miss))
      SELECT b.company_code, b.date AS recovered_ex_date,
             round(b.prev_div, 4) prev_year_div, round(b.implied_div, 4) new_div,
             e23.cash_dividend v2023, e25.cash_dividend v2025
      FROM best b
      LEFT JOIN ex_right_dividend e23 ON e23.market='twse'
             AND e23.company_code=b.company_code AND year(e23.date)=2023
      LEFT JOIN ex_right_dividend e25 ON e25.market='twse'
             AND e25.company_code=b.company_code AND year(e25.date)=2025
      WHERE b.rn=1 ORDER BY b.date LIMIT 40
    """).df().to_string())

    print("\n  還原除息日的月份分佈")
    print(con.sql("""
      WITH y AS (SELECT company_code, year(date) y FROM ex_right_dividend
                 WHERE market='twse' AND cash_dividend > 0 GROUP BY 1,2),
      miss AS (
        SELECT a.company_code FROM
          (SELECT company_code FROM y WHERE y=2023
           INTERSECT SELECT company_code FROM y WHERE y=2025) a
        WHERE a.company_code NOT IN (SELECT company_code FROM y WHERE y=2024)),
      best AS (
        SELECT s.*, row_number() OVER (PARTITION BY s.company_code
                                       ORDER BY abs(s.implied_div - s.prev_div) DESC) rn
        FROM steps s WHERE s.company_code IN (SELECT company_code FROM miss))
      SELECT month(date) m, COUNT(*) n FROM best WHERE rn=1 GROUP BY 1 ORDER BY 1
    """).df().to_string())


if __name__ == "__main__":
    main()
