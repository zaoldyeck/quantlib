"""C-ex_right_dividend 稽核 13:結案數字(缺口規模、影響面、cache/PG 落差)。

把 01~12 的結論壓成報告要引用的幾個數字,一次跑完可重現。

Run: uv run --project research python docs/data_audit/scripts/C-ex_right_dividend/13_final_tally.py
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

    print("== 公司列 vs ETF 列:逐年 x market(PG)==")
    print(con.sql("""
      SELECT market, year(date) y,
             COUNT(*) FILTER (WHERE company_code NOT LIKE '00%') company_rows,
             COUNT(*) FILTER (WHERE company_code LIKE '00%')     etf_rows
      FROM pg.public.ex_right_dividend WHERE date >= DATE '2021-01-01'
      GROUP BY 1,2 ORDER BY 1,2
    """).df().to_string())

    print("\n== twse 2024 下半年缺口:逐月 公司列 (2023/2024/2025 對照) ==")
    print(con.sql("""
      PIVOT (SELECT month(date) m, year(date) y, COUNT(*) n
             FROM pg.public.ex_right_dividend
             WHERE market='twse' AND company_code NOT LIKE '00%'
               AND year(date) BETWEEN 2023 AND 2025 GROUP BY 1,2)
      ON y IN (2023, 2024, 2025) USING first(n) GROUP BY m ORDER BY m
    """).df().to_string())

    print("\n== ETF 配息最後一筆日期 x market ==")
    print(con.sql("""
      SELECT market, max(date) last_etf_div, COUNT(*) FILTER (WHERE date >= DATE '2024-07-01') n_after_2024h2
      FROM pg.public.ex_right_dividend WHERE company_code LIKE '00%' GROUP BY 1
    """).df().to_string())

    print("\n== 目前仍在交易、但配息紀錄早已斷掉的 ETF 檔數 ==")
    print(con.sql("""
      WITH q AS (SELECT market, company_code, max(date) last_px
                 FROM daily_quote WHERE company_code LIKE '00%' GROUP BY 1,2),
           e AS (SELECT market, company_code, max(date) last_div
                 FROM pg.public.ex_right_dividend WHERE company_code LIKE '00%' GROUP BY 1,2)
      SELECT q.market,
             COUNT(*) n_etf_trading_now,
             COUNT(*) FILTER (WHERE e.last_div IS NULL) never_had_div,
             COUNT(*) FILTER (WHERE e.last_div < DATE '2024-07-01') stopped_before_2024h2
      FROM q LEFT JOIN e USING (market, company_code)
      WHERE q.last_px >= DATE '2026-07-01' GROUP BY 1
    """).df().to_string())

    print("\n== cache 相對 PG 的落差(雙寫入路徑)==")
    print(con.sql("""
      WITH p AS (SELECT market, date, company_code, cash_dividend
                 FROM pg.public.ex_right_dividend WHERE cash_dividend > 0)
      SELECT
        (SELECT COUNT(*) FROM (SELECT market,date,company_code FROM ex_right_dividend
                               EXCEPT SELECT market,date,company_code FROM p)) cache_only,
        (SELECT COUNT(*) FROM (SELECT market,date,company_code FROM p
                               EXCEPT SELECT market,date,company_code FROM ex_right_dividend)) pg_only,
        (SELECT max(date) FROM ex_right_dividend) cache_max,
        (SELECT max(date) FROM pg.public.ex_right_dividend) pg_max,
        (SELECT COUNT(*) FROM ex_right_dividend WHERE cash_dividend = 0) cache_zero_rows
    """).df().to_string())

    print("\n== twse 換源當口的『整段空白』(6/22~7/14 逐年對照)==")
    print(con.sql("""
      SELECT year(date) y, COUNT(*) n, COUNT(DISTINCT date) n_days
      FROM pg.public.ex_right_dividend
      WHERE market='twse'
        AND (month(date)=6 AND day(date) >= 22 OR month(date)=7 AND day(date) <= 14)
        AND year(date) BETWEEN 2021 AND 2026
      GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n== twse legacy 最後一天 / MOPS 第一天 ==")
    print(con.sql("""
      SELECT date, COUNT(*) n FROM pg.public.ex_right_dividend
      WHERE market='twse' AND date BETWEEN DATE '2024-06-15' AND DATE '2024-07-20'
      GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n== 受影響的名單樣本:2024 缺漏 x 市值代表性(用 2025 現金股利當代理)==")
    print(con.sql("""
      WITH y AS (SELECT company_code, year(date) y FROM ex_right_dividend
                 WHERE market='twse' AND cash_dividend > 0 GROUP BY 1,2),
      miss AS (SELECT a.company_code FROM
        (SELECT company_code FROM y WHERE y=2023
         INTERSECT SELECT company_code FROM y WHERE y=2025) a
        WHERE a.company_code NOT IN (SELECT company_code FROM y WHERE y=2024))
      SELECT m.company_code, e.date d2025, e.cash_dividend v2025,
             q.closing_price px_2024_07_01,
             round(e.cash_dividend / nullif(q.closing_price,0), 4) approx_yield
      FROM miss m
      JOIN ex_right_dividend e ON e.market='twse' AND e.company_code=m.company_code
                              AND year(e.date)=2025
      LEFT JOIN daily_quote q ON q.market='twse' AND q.company_code=m.company_code
                             AND q.date=DATE '2024-07-01'
      WHERE m.company_code IN ('1101','2303','2317','2412','2882','2454','2881','2886',
                               '2891','1216','2002','1301','1303','2207','2801','2884')
      ORDER BY m.company_code
    """).df().to_string())


if __name__ == "__main__":
    main()
