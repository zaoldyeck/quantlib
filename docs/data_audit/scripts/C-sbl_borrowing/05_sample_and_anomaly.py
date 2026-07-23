"""C-sbl_borrowing ⑤:抽樣逐欄比對(cache vs PG vs 原始檔)+ 全表異常值掃描。

① 抽樣:3 個日期 × 5 檔股票,cache 與 PG 的每一欄逐格比對(pandas `DataFrame.equals`),
   另外挑一格對原始 CSV 逐欄核對(確認欄位對位:借券區塊是第 8~13 欄,不是融券的 2~7)。
② 異常值:NULL / 負值 / 未來日期 / market 值域 / company_code 字元集 / 重複主鍵 /
   恆等式(當日餘額 = 前日餘額 + 當日賣出 − 當日還券 + 當日調整)/ 限額關係 /
   單日列數異常低。

Run: PYTHONPATH=<repo> uv run --project . python docs/data_audit/scripts/C-sbl_borrowing/05_sample_and_anomaly.py
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd

from research import paths

PG_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}")

COLS = ("market, date, company_code, prev_day_balance, daily_sold, daily_returned, "
        "daily_adjustment, daily_balance, next_day_limit")

SAMPLES = [
    ("twse", "2016-04-06", ["0050", "2330", "2317", "1101", "2412"]),
    ("tpex", "2019-11-07", ["6488", "3105", "5483", "8069", "4966"]),
    ("twse", "2026-07-17", ["2330", "2317", "2454", "3008", "2412"]),
]


def main() -> None:
    pd.set_option("display.width", 260)
    pd.set_option("display.max_columns", 40)
    pd.set_option("display.max_rows", 200)

    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql(f"ATTACH '{paths.CACHE_DB}' AS ca (READ_ONLY)")

    print("=== ① 抽樣逐欄比對(cache vs PG)===")
    for market, d, codes in SAMPLES:
        lst = ", ".join(f"'{c}'" for c in codes)
        w = (f"WHERE market='{market}' AND date=DATE '{d}' AND company_code IN ({lst}) "
             "ORDER BY company_code")
        a = con.sql(f"SELECT {COLS} FROM pg.public.sbl_borrowing {w}").df()
        b = con.sql(f"SELECT {COLS} FROM ca.sbl_borrowing {w}").df()
        print(f"\n--- {market} {d} ({len(a)} 列)---")
        print(a.to_string())
        print(f"cache 與 PG DataFrame.equals => {a.equals(b)}")

    con2 = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    print("\n\n=== ② 全表異常值掃描(cache)===")
    print(con2.sql("""
    SELECT count(*) AS n_rows,
           count(*) FILTER (market IS NULL OR date IS NULL OR company_code IS NULL
                            OR prev_day_balance IS NULL OR daily_sold IS NULL
                            OR daily_returned IS NULL OR daily_adjustment IS NULL
                            OR daily_balance IS NULL OR next_day_limit IS NULL) AS n_null,
           count(*) FILTER (market NOT IN ('twse','tpex')) AS n_bad_market,
           count(*) FILTER (NOT regexp_matches(company_code, '^[0-9][0-9A-Za-z]*$')) AS n_bad_code,
           count(*) FILTER (date > current_date) AS n_future,
           count(*) FILTER (prev_day_balance < 0) AS n_neg_prev,
           count(*) FILTER (daily_sold < 0) AS n_neg_sold,
           count(*) FILTER (daily_returned < 0) AS n_neg_ret,
           count(*) FILTER (daily_adjustment < 0) AS n_neg_adj,
           count(*) FILTER (daily_balance < 0) AS n_neg_bal,
           count(*) FILTER (next_day_limit < 0) AS n_neg_limit,
           count(*) FILTER (daily_balance = 0 AND prev_day_balance = 0 AND daily_sold = 0
                            AND daily_returned = 0 AND daily_adjustment = 0
                            AND next_day_limit = 0) AS n_all_zero
    FROM sbl_borrowing""").df().T.to_string())

    print("\n--- 恆等式 daily_balance = prev + sold - returned + adj ---")
    print(con2.sql("""
    SELECT count(*) AS n_total,
           count(*) FILTER (daily_balance
                <> prev_day_balance + daily_sold - daily_returned + daily_adjustment) AS n_viol
    FROM sbl_borrowing""").df().to_string())
    print(con2.sql("""
    SELECT market, year(date) AS y, count(*) AS n_viol FROM sbl_borrowing
    WHERE daily_balance <> prev_day_balance + daily_sold - daily_returned + daily_adjustment
    GROUP BY 1,2 ORDER BY 1,2""").df().to_string())
    print("違反樣本(全部):")
    print(con2.sql("""
    SELECT *, prev_day_balance + daily_sold - daily_returned + daily_adjustment AS calc
    FROM sbl_borrowing
    WHERE daily_balance <> prev_day_balance + daily_sold - daily_returned + daily_adjustment
    ORDER BY date LIMIT 10""").df().to_string())

    print("\n--- 限額關係:daily_balance vs next_day_limit ---")
    print(con2.sql("""
    SELECT count(*) AS n,
           count(*) FILTER (daily_balance > next_day_limit) AS bal_gt_limit,
           round(100.0*count(*) FILTER (daily_balance > next_day_limit)/count(*), 2) AS pct,
           count(*) FILTER (next_day_limit = 0 AND daily_balance > 0) AS limit0_bal_pos
    FROM sbl_borrowing""").df().to_string())

    print("\n--- 值域極端(前 5 大 daily_balance)---")
    print(con2.sql("""
    SELECT market, date, company_code, daily_balance, next_day_limit
    FROM sbl_borrowing ORDER BY daily_balance DESC LIMIT 5""").df().to_string())

    print("\n--- 單日列數異常(前後 21 日滾動中位數 70% 門檻)---")
    print(con2.sql("""
    WITH d AS (SELECT market, date, count(*) AS n FROM sbl_borrowing GROUP BY 1,2),
         m AS (SELECT *, median(n) OVER (PARTITION BY market ORDER BY date
                 ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) AS med FROM d)
    SELECT market, date, dayname(date) AS wd, n, med, round(n/med, 3) AS ratio
    FROM m WHERE n < 0.7*med ORDER BY ratio LIMIT 30""").df().to_string())

    print("\n--- 逐年列數 / 每日平均檔數 ---")
    print(con2.sql("""
    SELECT market, year(date) AS y, count(*) AS n_rows, count(DISTINCT date) AS n_days,
           round(count(*)*1.0/count(DISTINCT date), 1) AS avg_codes,
           count(DISTINCT company_code) AS n_codes
    FROM sbl_borrowing GROUP BY 1,2 ORDER BY 1,2""").df().to_string())

    print("\n--- 覆蓋率:sbl 的檔數 vs 同日 daily_quote 檔數 ---")
    print(con2.sql("""
    WITH s AS (SELECT market, date, count(*) AS ns FROM sbl_borrowing GROUP BY 1,2),
         q AS (SELECT market, date, count(*) AS nq FROM daily_quote GROUP BY 1,2)
    SELECT market, min(ns-nq) AS min_diff, max(ns-nq) AS max_diff,
           round(avg(ns-nq),1) AS avg_diff, count(*) AS n_days,
           count(*) FILTER (ns > nq) AS n_sbl_more
    FROM s JOIN q USING (market, date) GROUP BY 1 ORDER BY 1""").df().to_string())


if __name__ == "__main__":
    main()
