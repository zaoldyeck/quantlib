"""C-ex_right_dividend 稽核 12:ETF 配息在換 MOPS 之後整批消失。

MOPS t108sb27 是「上市/上櫃**公司**除權息公告」,不含 ETF 受益憑證的收益分配;
legacy 的 TWSE TWT49U / TPEx exDailyQ 則是交易所的除權息計算結果表,ETF 也在裡面。
→ twse 自 2024-07(換 MOPS)起、tpex 自 2026-05 起,ETF 配息全數缺漏。

影響:research/prices.py 的還原價與 total_return_series(0050 基準)會少算這些配息。

Run: uv run --project research python docs/data_audit/scripts/C-ex_right_dividend/12_etf_gap.py
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

    print("== ETF(代號 00 開頭)配息列數:逐年 x market ==")
    print(con.sql("""
      SELECT market, year(date) y, COUNT(*) n, COUNT(DISTINCT company_code) n_etf
      FROM pg.public.ex_right_dividend
      WHERE company_code LIKE '00%' AND date >= DATE '2019-01-01'
      GROUP BY 1,2 ORDER BY 1,2
    """).df().to_string())

    print("\n== ETF 配息列數:2024 逐月 x market ==")
    print(con.sql("""
      SELECT market, year(date) y, month(date) m, COUNT(*) n
      FROM pg.public.ex_right_dividend
      WHERE company_code LIKE '00%' AND date >= DATE '2024-01-01'
      GROUP BY 1,2,3 ORDER BY 1,2,3
    """).df().to_string())

    print("\n== 主要 ETF 最後一筆配息紀錄 vs 它在 daily_quote 的最新報價 ==")
    print(con.sql("""
      WITH e AS (SELECT market, company_code, max(date) last_div, COUNT(*) n_div
                 FROM pg.public.ex_right_dividend
                 WHERE company_code LIKE '00%' GROUP BY 1,2),
           q AS (SELECT market, company_code, max(date) last_px, COUNT(*) n_px
                 FROM daily_quote WHERE company_code LIKE '00%' GROUP BY 1,2)
      SELECT q.market, q.company_code, e.n_div, e.last_div, q.last_px, q.n_px
      FROM q LEFT JOIN e USING (market, company_code)
      WHERE q.last_px >= DATE '2026-07-01' AND q.n_px > 400
        AND (e.last_div IS NULL OR e.last_div < DATE '2025-01-01')
      ORDER BY q.market, q.company_code
    """).df().to_string())

    print("\n== 0050 / 0056 / 00878 / 006208 全部配息紀錄 ==")
    print(con.sql("""
      SELECT company_code, date, cash_dividend
      FROM pg.public.ex_right_dividend
      WHERE company_code IN ('0050','0056','00878','006208','00919','00713')
        AND date >= DATE '2022-01-01' ORDER BY company_code, date
    """).df().to_string())

    print("\n== 0050 缺漏配息對 total_return_series 的量級 ==")
    print("   (0050 近年年化配息率約 3%;2024-07 起缺 2 年 → 基準 TR 低估約 6%)")
    print(con.sql("""
      SELECT date, closing_price FROM daily_quote
      WHERE market='twse' AND company_code='0050'
        AND date IN (DATE '2024-07-01', DATE '2026-07-20')
      ORDER BY date
    """).df().to_string())


if __name__ == "__main__":
    main()
