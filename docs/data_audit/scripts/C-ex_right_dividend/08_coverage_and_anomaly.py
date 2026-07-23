"""C-ex_right_dividend 稽核 08:覆蓋缺口 + 異常值掃描。

ex_right_dividend 是事件表(不是每日一列),所以「日期缺口」要用兩個角度看:
  (a) 除權息日落在非交易日 → 抓錯或解析錯;
  (b) 某段期間應該有事件卻整段沒有 → 端點死掉沒補(用「跨年出現度」偵測:
      2023 與 2025 都有除息、2024 卻沒有的公司,幾乎必然是 2024 漏抓)。

異常值:負現金股利、股利 >= 除權息前收盤價、未來日期、重複鍵、代號格式。

Run: uv run --project . python docs/data_audit/scripts/C-ex_right_dividend/08_coverage_and_anomaly.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402
from quantlib.data_calendar import is_trading_day  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"
TODAY = "2026-07-22"


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")

    print("== (a) 除權息日不在 daily_quote 交易日集合的列 ==")
    df = con.sql("""
      SELECT c.market, c.date, COUNT(*) n,
             string_agg(c.company_code, ',' ORDER BY c.company_code) codes
      FROM ex_right_dividend c
      WHERE NOT EXISTS (SELECT 1 FROM daily_quote q
                        WHERE q.market=c.market AND q.date=c.date)
      GROUP BY 1,2 ORDER BY 2,1
    """).df()
    print(df.to_string())
    print("\n  → 用 src/quantlib/data_calendar.is_trading_day 複核(0-byte sentinel 休市日曆):")
    for _, r in df.iterrows():
        d = r["date"].date()
        flag = "未來" if str(d) > TODAY else ("交易日" if is_trading_day(d) else "休市")
        print(f"    {r['market']} {d} n={r['n']:>3} → {flag}")

    print("\n== (b) 跨年出現度:2023 與 2025 都有除息、2024 卻完全沒有的公司 ==")
    print(con.sql("""
      WITH y AS (SELECT market, company_code, year(date) y FROM ex_right_dividend
                 WHERE cash_dividend > 0 GROUP BY 1,2,3)
      SELECT market, COUNT(*) n_companies
      FROM (SELECT market, company_code FROM y WHERE y=2023
            INTERSECT SELECT market, company_code FROM y WHERE y=2025) a
      WHERE NOT EXISTS (SELECT 1 FROM y WHERE y.market=a.market
                          AND y.company_code=a.company_code AND y.y=2024)
      GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n  對照組:2022 與 2024 都有、2023 沒有(基準漏失率)")
    print(con.sql("""
      WITH y AS (SELECT market, company_code, year(date) y FROM ex_right_dividend
                 WHERE cash_dividend > 0 GROUP BY 1,2,3)
      SELECT market, COUNT(*) n_companies
      FROM (SELECT market, company_code FROM y WHERE y=2022
            INTERSECT SELECT market, company_code FROM y WHERE y=2024) a
      WHERE NOT EXISTS (SELECT 1 FROM y WHERE y.market=a.market
                          AND y.company_code=a.company_code AND y.y=2023)
      GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n  對照組:2024 與 2026 都有、2025 沒有")
    print(con.sql("""
      WITH y AS (SELECT market, company_code, year(date) y FROM ex_right_dividend
                 WHERE cash_dividend > 0 GROUP BY 1,2,3)
      SELECT market, COUNT(*) n_companies
      FROM (SELECT market, company_code FROM y WHERE y=2024
            INTERSECT SELECT market, company_code FROM y WHERE y=2026) a
      WHERE NOT EXISTS (SELECT 1 FROM y WHERE y.market=a.market
                          AND y.company_code=a.company_code AND y.y=2025)
      GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n  2024 漏失公司的樣本(twse,前 30 檔,附 2023/2025 除息日與金額)")
    print(con.sql("""
      WITH y AS (SELECT market, company_code, year(date) y FROM ex_right_dividend
                 WHERE cash_dividend > 0 GROUP BY 1,2,3),
      miss AS (
        SELECT a.market, a.company_code
        FROM (SELECT market, company_code FROM y WHERE y=2023
              INTERSECT SELECT market, company_code FROM y WHERE y=2025) a
        WHERE NOT EXISTS (SELECT 1 FROM y WHERE y.market=a.market
                            AND y.company_code=a.company_code AND y.y=2024))
      SELECT m.company_code,
             max(CASE WHEN year(e.date)=2023 THEN e.date END) d2023,
             max(CASE WHEN year(e.date)=2023 THEN e.cash_dividend END) v2023,
             max(CASE WHEN year(e.date)=2025 THEN e.date END) d2025,
             max(CASE WHEN year(e.date)=2025 THEN e.cash_dividend END) v2025
      FROM miss m JOIN ex_right_dividend e
        ON e.market=m.market AND e.company_code=m.company_code
      WHERE m.market='twse' AND year(e.date) IN (2023, 2025)
      GROUP BY 1 ORDER BY 1 LIMIT 30
    """).df().to_string())

    print("\n== 異常值掃描(cache)==")
    print(con.sql(f"""
      SELECT COUNT(*) n,
             COUNT(*) FILTER (WHERE cash_dividend < 0)  neg,
             COUNT(*) FILTER (WHERE cash_dividend = 0)  zero,
             COUNT(*) FILTER (WHERE cash_dividend IS NULL) n_null,
             COUNT(*) FILTER (WHERE date > DATE '{TODAY}') future,
             COUNT(*) FILTER (WHERE NOT regexp_matches(company_code, '^[0-9]{{4}}[0-9A-Z]?$')) odd_code,
             COUNT(*) FILTER (WHERE cash_dividend > 100) gt100
      FROM ex_right_dividend
    """).df().to_string())

    print("\n  重複鍵")
    print(con.sql("""
      SELECT COUNT(*) dup_keys FROM (
        SELECT market, date, company_code FROM ex_right_dividend
        GROUP BY 1,2,3 HAVING COUNT(*) > 1)
    """).df().to_string())

    print("\n  未來日期分佈(逐月)")
    print(con.sql(f"""
      SELECT market, date, COUNT(*) n FROM ex_right_dividend
      WHERE date > DATE '{TODAY}' GROUP BY 1,2 ORDER BY 2,1
    """).df().to_string())

    print("\n  cash_dividend >= 除權息前收盤價(PG,prices.py:_build_factor_table 會丟掉這種)")
    print(con.sql("""
      SELECT market, date, company_code, company_name, cash_dividend,
             closing_price_before_ex_right_ex_dividend pre_close,
             ex_right_ex_dividend_reference_price ref_px, right_or_dividend
      FROM pg.public.ex_right_dividend
      WHERE closing_price_before_ex_right_ex_dividend > 0
        AND cash_dividend >= closing_price_before_ex_right_ex_dividend
      ORDER BY date
    """).df().to_string())

    print("\n  cash_dividend 前 15 大(cache)")
    print(con.sql("""
      SELECT * FROM ex_right_dividend ORDER BY cash_dividend DESC LIMIT 15
    """).df().to_string())


if __name__ == "__main__":
    main()
