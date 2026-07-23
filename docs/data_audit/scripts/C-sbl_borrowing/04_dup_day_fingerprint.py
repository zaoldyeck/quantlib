"""C-sbl_borrowing ④:cache 端獨立複驗——整日內容指紋撞號 + 幽靈日 + 連續性斷裂。

三件事,全部只用 `var/cache/cache.duckdb` 自己的資料(不看原始檔),所以可以和
③(原始檔的檔名/內容日期核對)互相印證:

  ① 整日指紋撞號:(market, date) 的 `sum(hash(code||prev||sold||ret||adj||bal||limit))`
     若與另一天完全相同 → 那一天存的是別天的複製品。
  ② 幽靈日:週末/國定假日卻有整天資料。
  ③ 連續性斷裂:`prev_day_balance[t]` 應等於同一檔股票上一個有資料日的
     `daily_balance[t-1]`。大量斷裂 = 該日資料不屬於這個時序位置。
     這是判斷「這一天到底是哪一天」的最強證據,因為它不依賴任何外部日曆。

Run: PYTHONPATH=<repo> uv run --project . python docs/data_audit/scripts/C-sbl_borrowing/04_dup_day_fingerprint.py
"""
from __future__ import annotations

import duckdb
import pandas as pd

from research import paths

FP = """
SELECT market, date, count(*) AS n,
       sum(hash(company_code || '|' || prev_day_balance || '|' || daily_sold || '|' ||
                daily_returned || '|' || daily_adjustment || '|' || daily_balance || '|' ||
                next_day_limit)::HUGEINT) AS h
FROM sbl_borrowing GROUP BY 1, 2
"""

#: 只看餘額(限額每日重算,休市日照樣更新,會讓完整指紋錯過部分複製日)
FP_BAL = """
SELECT market, date, count(*) AS n,
       sum(hash(company_code || '|' || daily_balance)::HUGEINT) AS h
FROM sbl_borrowing GROUP BY 1, 2
"""


def main() -> None:
    pd.set_option("display.width", 250)
    pd.set_option("display.max_rows", 300)
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    for label, fp in (("全欄指紋", FP), ("只看 (code, daily_balance)", FP_BAL)):
        print(f"\n=== ① 整日指紋撞號 — {label} ===")
        d = con.sql(f"""
        WITH f AS ({fp})
        SELECT a.market, a.date AS date_a, b.date AS date_b, a.n
        FROM f a JOIN f b ON a.market=b.market AND a.h=b.h AND a.n=b.n AND a.date<b.date
        ORDER BY 1,2""").df()
        print(f"撞號對數: {len(d)}")
        print(d.to_string() if len(d) else "(無)")

    print("\n=== ② 幽靈日:週末 / daily_quote 該日 0 列 ===")
    g = con.sql("""
    WITH s AS (SELECT market, date, count(*) AS n FROM sbl_borrowing GROUP BY 1,2),
         dq AS (SELECT market, date FROM daily_quote GROUP BY 1,2),
         mt AS (SELECT market, date FROM margin_transactions GROUP BY 1,2)
    SELECT s.market, s.date, dayname(s.date) AS wd, s.n,
           (dq.date IS NOT NULL) AS dq_has, (mt.date IS NOT NULL) AS margin_has
    FROM s LEFT JOIN dq USING (market, date) LEFT JOIN mt USING (market, date)
    WHERE isodow(s.date) >= 6 OR dq.date IS NULL
    ORDER BY 1,2""").df()
    print(f"總數: {len(g)}")
    print(g.to_string() if len(g) else "(無)")

    print("\n=== ③ prev_day_balance 連續性:逐日斷裂比例(最差 40 天)===")
    b = con.sql("""
    WITH s AS (
      SELECT market, date, company_code, prev_day_balance, daily_balance,
             lag(daily_balance) OVER (PARTITION BY market, company_code ORDER BY date) AS prev_bal_actual,
             lag(date)          OVER (PARTITION BY market, company_code ORDER BY date) AS prev_date
      FROM sbl_borrowing)
    SELECT market, date, dayname(date) AS wd, count(*) AS n,
           count(*) FILTER (prev_bal_actual IS NOT NULL AND prev_day_balance <> prev_bal_actual) AS n_break,
           round(100.0 * count(*) FILTER (prev_bal_actual IS NOT NULL AND prev_day_balance <> prev_bal_actual)
                 / nullif(count(*) FILTER (prev_bal_actual IS NOT NULL), 0), 2) AS pct_break
    FROM s GROUP BY 1,2,3 HAVING n > 100 ORDER BY pct_break DESC NULLS LAST LIMIT 40""").df()
    print(b.to_string())

    print("\n=== ③b 全史斷裂率(基準線)===")
    print(con.sql("""
    WITH s AS (
      SELECT market, date, company_code, prev_day_balance,
             lag(daily_balance) OVER (PARTITION BY market, company_code ORDER BY date) AS prev_bal_actual
      FROM sbl_borrowing)
    SELECT market, count(*) FILTER (prev_bal_actual IS NOT NULL) AS n_cmp,
           count(*) FILTER (prev_bal_actual IS NOT NULL AND prev_day_balance <> prev_bal_actual) AS n_break,
           round(100.0*count(*) FILTER (prev_bal_actual IS NOT NULL AND prev_day_balance <> prev_bal_actual)
                 / count(*) FILTER (prev_bal_actual IS NOT NULL), 3) AS pct
    FROM s GROUP BY 1 ORDER BY 1""").df().to_string())

    print("\n=== ④ 跨市場同 (date, company_code) 重複(轉上市/上櫃當日)===")
    print(con.sql("""
    SELECT date, company_code, count(*) AS n, string_agg(market, ',' ORDER BY market) AS mkts,
           string_agg(daily_balance::VARCHAR, ',' ORDER BY market) AS bals
    FROM sbl_borrowing GROUP BY 1,2 HAVING count(*) > 1 ORDER BY 1""").df().to_string())


if __name__ == "__main__":
    main()
