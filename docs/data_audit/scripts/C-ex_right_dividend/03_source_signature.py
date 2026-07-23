"""C-ex_right_dividend 稽核 03:分辨每一列的來源(legacy 端點 vs MOPS 月報)。

TradingReader.parseMopsRows(src/main/scala/reader/TradingReader.scala:364-396)對
MOPS 月報把「除權息前收盤價 / 參考價 / 漲跌停 / 開盤參考價 / 除息參考價」六個價格欄
一律填 0(MOPS 沒有這些欄位);legacy TWT49U / exDailyQ 則有真值。
→ pre_close = 0 即可當作「這列來自 MOPS」的指紋。

用途:2024 年 twse 筆數腰斬要先確認是「legacy 端點死掉、MOPS 沒補齊」還是別的原因。

Run: uv run --project research python docs/data_audit/scripts/C-ex_right_dividend/03_source_signature.py
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
    con.sql("""
      CREATE OR REPLACE TEMP VIEW e AS
      SELECT market, date, company_code, company_name, cash_dividend,
             right_or_dividend,
             closing_price_before_ex_right_ex_dividend AS pre_close,
             ex_right_ex_dividend_reference_price AS ref_px,
             limit_up, limit_down, opening_reference_price,
             ex_dividend_reference_price,
             CASE WHEN closing_price_before_ex_right_ex_dividend = 0
                   AND ex_right_ex_dividend_reference_price = 0
                  THEN 'mops' ELSE 'legacy' END AS src
      FROM pg.public.ex_right_dividend
    """)

    print("== 來源指紋 x 年 x market ==")
    print(con.sql("""
      PIVOT (SELECT market, year(date) y, src, COUNT(*) n FROM e GROUP BY 1,2,3)
      ON src USING first(n) GROUP BY market, y ORDER BY market, y
    """).df().to_string())

    print("\n== 2024-2026 逐月來源指紋 ==")
    print(con.sql("""
      PIVOT (SELECT market, year(date) y, month(date) m, src, COUNT(*) n
             FROM e WHERE year(date) >= 2024 GROUP BY 1,2,3,4)
      ON src USING first(n) GROUP BY market, y, m ORDER BY market, y, m
    """).df().to_string())

    print("\n== right_or_dividend 值域 x 來源 ==")
    print(con.sql("""
      SELECT src, right_or_dividend, COUNT(*) n,
             COUNT(*) FILTER (WHERE cash_dividend > 0) n_cash_pos
      FROM e GROUP BY 1,2 ORDER BY 1, 3 DESC
    """).df().to_string())

    print("\n== cash_dividend <= 0 的列(cache 會被 >0 濾掉)==")
    print(con.sql("""
      SELECT market, date, company_code, company_name, cash_dividend,
             right_or_dividend, pre_close, ref_px, src
      FROM e WHERE cash_dividend <= 0 ORDER BY cash_dividend, date
    """).df().to_string())


if __name__ == "__main__":
    main()
