"""C-stock_per_pbr 稽核 06:數值欄異常掃描 + 缺漏日清單。

檢查:負/零 PB、負 PE、極端 PE/PB、殖利率 > 30%、未來日期、重複鍵、
company_code 格式、全 NULL 列。並列出「有報價卻無 per_pbr」的缺漏日(含星期)。

Run: uv run --project research python docs/data_audit/scripts/C-stock_per_pbr/06_value_anomaly.py
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

WD = ["一", "二", "三", "四", "五", "六", "日"]


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    print("== NULL / 極端值總覽 ==")
    print(con.sql("""
      SELECT COUNT(*) n,
        COUNT(*) FILTER (WHERE price_book_ratio IS NULL) pb_null,
        COUNT(*) FILTER (WHERE dividend_yield IS NULL) dy_null,
        COUNT(*) FILTER (WHERE price_to_earning_ratio IS NULL) pe_null,
        COUNT(*) FILTER (WHERE price_book_ratio IS NULL AND dividend_yield IS NULL
                           AND price_to_earning_ratio IS NULL) all_null,
        COUNT(*) FILTER (WHERE price_book_ratio < 0) pb_neg,
        COUNT(*) FILTER (WHERE price_book_ratio = 0) pb_zero,
        COUNT(*) FILTER (WHERE price_book_ratio > 100) pb_gt100,
        COUNT(*) FILTER (WHERE price_to_earning_ratio < 0) pe_neg,
        COUNT(*) FILTER (WHERE price_to_earning_ratio = 0) pe_zero,
        COUNT(*) FILTER (WHERE price_to_earning_ratio > 1000) pe_gt1000,
        COUNT(*) FILTER (WHERE dividend_yield < 0) dy_neg,
        COUNT(*) FILTER (WHERE dividend_yield > 30) dy_gt30,
        COUNT(*) FILTER (WHERE date > current_date) future
      FROM stock_per_pbr
    """).df().T.to_string())

    print("\n== 極端值樣本 ==")
    for label, cond in [("pb>100", "price_book_ratio > 100"),
                        ("pb=0", "price_book_ratio = 0"),
                        ("pe>1000", "price_to_earning_ratio > 1000"),
                        ("dy>30", "dividend_yield > 30")]:
        df = con.sql(f"""
          SELECT market, date, company_code, price_book_ratio pb, dividend_yield dy,
                 price_to_earning_ratio pe FROM stock_per_pbr
          WHERE {cond} ORDER BY date DESC LIMIT 5
        """).df()
        print(f"-- {label}: --")
        print(df.to_string() if len(df) else "(無)")

    print("\n== 重複鍵 / 代號格式 ==")
    print(con.sql("""
      SELECT (SELECT COUNT(*) FROM (SELECT market,date,company_code FROM stock_per_pbr
              GROUP BY ALL HAVING COUNT(*)>1)) dup_keys,
             (SELECT COUNT(DISTINCT company_code) FROM stock_per_pbr
              WHERE NOT regexp_matches(company_code,'^[0-9]{4}[0-9A-Z]?$')) odd_codes
    """).df().to_string())
    print(con.sql("""
      SELECT company_code, COUNT(*) n FROM stock_per_pbr
      WHERE NOT regexp_matches(company_code,'^[0-9]{4}[0-9A-Z]?$')
      GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).df().to_string())

    print("\n== 缺漏日:daily_quote 有、stock_per_pbr 無(含星期)==")
    for mkt in ("twse", "tpex"):
        df = con.sql(f"""
          SELECT date, n FROM (
            SELECT date, COUNT(*) n FROM daily_quote WHERE market='{mkt}' GROUP BY 1) q
          WHERE date >= (SELECT MIN(date) FROM stock_per_pbr WHERE market='{mkt}')
            AND date NOT IN (SELECT DISTINCT date FROM stock_per_pbr WHERE market='{mkt}')
          ORDER BY date
        """).df()
        print(f"-- {mkt}: {len(df)} 天 --")
        for _, r in df.iterrows():
            d: date = r["date"].date()
            print(f"   {d} ({WD[d.weekday()]})  daily_quote {int(r['n'])} 檔")


if __name__ == "__main__":
    main()
