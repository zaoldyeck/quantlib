"""C-stock_per_pbr 稽核 08:逐日列數與前一日的落差(找部分公告/幽靈日)。

檔名≠內容(04)只抓得到「標題日期不符」的錯位;若某日內容雖是當日、卻只公告一半,
標題會是對的。本腳本用「相對前一交易日列數變動 > 10%」當偵測器補這個死角。

Run: uv run --project research python docs/data_audit/scripts/C-stock_per_pbr/08_daycount_jumps.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    for mkt in ("twse", "tpex"):
        print(f"== {mkt}:相對前一交易日列數變動 > 10% ==")
        print(con.sql(f"""
          WITH d AS (SELECT date, COUNT(*) n FROM stock_per_pbr WHERE market='{mkt}' GROUP BY 1),
               s AS (SELECT date, n, lag(n) OVER (ORDER BY date) prev FROM d)
          SELECT date, prev, n, ROUND(n::DOUBLE/prev - 1, 4) chg FROM s
          WHERE prev IS NOT NULL AND abs(n::DOUBLE/prev - 1) > 0.10
          ORDER BY date
        """).df().to_string())
        print()

    print("== 兩市場:與前一交易日「三個數值欄逐檔完全相同」的日(疑似重複內容)==")
    for mkt in ("twse", "tpex"):
        print(f"-- {mkt} --")
        print(con.sql(f"""
          WITH d AS (SELECT DISTINCT date FROM stock_per_pbr WHERE market='{mkt}'),
               s AS (SELECT date, lag(date) OVER (ORDER BY date) prev FROM d)
          SELECT s.date, s.prev, COUNT(*) n_common,
                 COUNT(*) FILTER (WHERE a.price_book_ratio IS NOT DISTINCT FROM b.price_book_ratio
                                    AND a.dividend_yield IS NOT DISTINCT FROM b.dividend_yield
                                    AND a.price_to_earning_ratio IS NOT DISTINCT FROM b.price_to_earning_ratio) n_same
          FROM s
          JOIN stock_per_pbr a ON a.market='{mkt}' AND a.date = s.date
          JOIN stock_per_pbr b ON b.market='{mkt}' AND b.date = s.prev
                              AND b.company_code = a.company_code
          GROUP BY 1,2 HAVING n_same = n_common AND n_common > 50
          ORDER BY 1
        """).df().to_string())
        print()


if __name__ == "__main__":
    main()
