"""C-foreign_holding_ratio 稽核 08:tpex 2010 假資料的汙染量化。

已證(05):tpex/2010 的 361 個原始檔內容日期全是 2026-04-24(1 檔 2026-05-12),
即 TPEx `insti/qfii` 端點對它沒有的歷史日期回「當下最新快照」,爬蟲以請求日存檔、
Reader 只讀檔名日期(TradingReader.scala:899-901)→ 整個 2010 年變成同一份 2026 快照。

本腳本量化:
  (1) 該年 884 檔中,有多少在 2010 年根本還沒上櫃(daily_quote/tpex 查無報價)
  (2) 這份 2010 快照與真正的 2026-04-24 快照是否逐檔相同(證明來源)
  (3) 若有策略用 tpex 2010 的 foreign_held_ratio,拿到的是 16 年後的數字(前視)

Run: uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/08_tpex2010_impact.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    print("== tpex 2010 規模 ==")
    print(con.sql("""
      SELECT COUNT(*) rows_2010, COUNT(DISTINCT date) n_days, COUNT(DISTINCT company_code) n_codes,
             (SELECT COUNT(*) FROM foreign_holding_ratio) total_rows
      FROM foreign_holding_ratio WHERE market='tpex' AND year(date)=2010
    """).df().to_string())

    print("\n== 2010 出現的代號:在 2010 年有沒有 tpex 報價 ==")
    print(con.sql("""
      WITH c AS (SELECT DISTINCT company_code FROM foreign_holding_ratio
                 WHERE market='tpex' AND year(date)=2010),
           q AS (SELECT DISTINCT company_code FROM daily_quote
                 WHERE market='tpex' AND year(date)=2010)
      SELECT COUNT(*) codes_2010,
             SUM(CASE WHEN q.company_code IS NULL THEN 1 ELSE 0 END) never_quoted_in_2010
      FROM c LEFT JOIN q USING (company_code)
    """).df().to_string())

    print("\n== 這些『2010 年不存在』代號的首次報價年份(前 15)==")
    print(con.sql("""
      WITH c AS (SELECT DISTINCT company_code FROM foreign_holding_ratio
                 WHERE market='tpex' AND year(date)=2010),
           q AS (SELECT company_code, MIN(date) first_quote FROM daily_quote
                 WHERE market='tpex' GROUP BY 1)
      SELECT year(q.first_quote) first_year, COUNT(*) n
      FROM c LEFT JOIN q USING (company_code)
      GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n== 2010 快照 vs 真 2026-04-24 快照(逐檔三欄比對)==")
    print(con.sql("""
      SELECT COUNT(*) common,
             SUM(CASE WHEN a.outstanding_shares=b.outstanding_shares
                       AND a.foreign_held_shares=b.foreign_held_shares
                       AND a.foreign_held_ratio =b.foreign_held_ratio THEN 1 ELSE 0 END) identical
      FROM foreign_holding_ratio a
      JOIN foreign_holding_ratio b
        ON b.market='tpex' AND b.date=DATE '2026-04-24' AND b.company_code=a.company_code
      WHERE a.market='tpex' AND a.date=DATE '2010-06-15'
    """).df().to_string())

    print("\n== 直觀樣本:5347 (2011 起真資料 vs 2010 假資料) ==")
    print(con.sql("""
      SELECT date, outstanding_shares, foreign_held_shares, foreign_held_ratio
      FROM foreign_holding_ratio
      WHERE market='tpex' AND company_code='5347'
        AND date IN (DATE '2010-01-04', DATE '2010-06-15', DATE '2010-12-31',
                     DATE '2011-01-04', DATE '2026-04-24')
      ORDER BY date
    """).df().to_string())

    print("\n== tpex 真資料起點 ==")
    print(con.sql("""
      SELECT MIN(date) first_real_tpex FROM foreign_holding_ratio
      WHERE market='tpex' AND date >= DATE '2011-01-01'
    """).df().to_string())


if __name__ == "__main__":
    main()
