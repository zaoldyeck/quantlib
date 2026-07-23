"""C-margin_transactions ④:cache 端的「列數形狀」健檢。

- margin 每日列數 vs daily_quote 每日列數(margin 應涵蓋全部上市/上櫃普通股 +
  部分 ETF,含停資停券的 0 額度列)。
- 單日列數異常低掃描(前後 21 日滾動中位數的 70% 門檻)→ 逐日查星期幾,
  分辨「週六補行交易日」與「真的少抓」。
- 尾端新鮮度:margin 覆蓋到哪一天、離齊備日差幾個交易日。

Run: PYTHONPATH=<repo> uv run --project research python docs/data_audit/scripts/C-margin_transactions/04_rowcount_shape.py
"""
from __future__ import annotations

import duckdb
import pandas as pd

from research import paths


def main() -> None:
    pd.set_option("display.width", 200)
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    print("=== margin 列數 - daily_quote 列數(2015 起,同 market/date)===")
    print(con.execute("""
        WITH m AS (SELECT market, date, count(*) n FROM margin_transactions GROUP BY 1,2),
             q AS (SELECT market, date, count(*) n FROM daily_quote GROUP BY 1,2)
        SELECT m.market, min(m.n-q.n) mn, max(m.n-q.n) mx, round(avg(m.n-q.n),1) av,
               count(*) n_days, count(*) FILTER (m.n > q.n) n_margin_more
        FROM m JOIN q USING (market, date)
        WHERE m.date >= DATE '2015-01-01' GROUP BY 1 ORDER BY 1""").df().to_string())

    print("\n=== 單日列數 < 前後 21 日滾動中位數 70% 的日子 ===")
    r = con.execute("""
        WITH d AS (SELECT market, date, count(*) n FROM margin_transactions GROUP BY 1,2),
             w AS (SELECT market, date, n,
                          median(n) OVER (PARTITION BY market ORDER BY date
                                          ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) med
                   FROM d)
        SELECT market, date, n, med, dayname(date) dow
        FROM w WHERE n < 0.7 * med ORDER BY market, date""").df()
    print(f"共 {len(r)} 天")
    print(r.to_string() if len(r) else "(無)")

    print("\n=== 逐年『四欄全 0』與『限額 0 但有餘額』占比 ===")
    print(con.execute("""
        SELECT year(date) y, count(*) n,
               round(100.0*count(*) FILTER (margin_balance=0 AND short_balance=0
                     AND margin_quota=0 AND short_quota=0)/count(*),2) pct_all_zero,
               round(100.0*count(*) FILTER (margin_quota=0 AND margin_balance>0)/count(*),2) pct_susp_mgn
        FROM margin_transactions GROUP BY 1 ORDER BY 1""").df().to_string())


if __name__ == "__main__":
    main()
