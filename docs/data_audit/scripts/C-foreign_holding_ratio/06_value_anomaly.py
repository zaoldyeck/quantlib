"""C-foreign_holding_ratio 稽核 06:數值欄異常掃描 + 內部恆等式檢查。

交易所定義(TPEx 欄名自帶公式;TWSE 同義):
  A = 發行股數 outstanding_shares
  C = 外資持有股數 foreign_held_shares
  F = 法令投資上限比率(%) foreign_limit_ratio
  B = 尚可投資股數 = A*F/100 - C  → foreign_remaining_shares
  D = 尚可投資比率 = B/A*100      → foreign_remaining_ratio
  E = 持股比率     = C/A*100      → foreign_held_ratio

Run: uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/06_value_anomaly.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    print("== 基本異常計數 ==")
    print(con.sql("""
      SELECT COUNT(*) n,
        SUM(CASE WHEN outstanding_shares IS NULL OR foreign_held_shares IS NULL
                   OR foreign_held_ratio IS NULL THEN 1 ELSE 0 END) n_nulls,
        SUM(CASE WHEN outstanding_shares <= 0 THEN 1 ELSE 0 END) out_le0,
        SUM(CASE WHEN foreign_held_shares < 0 THEN 1 ELSE 0 END) held_neg,
        SUM(CASE WHEN foreign_remaining_shares < 0 THEN 1 ELSE 0 END) remain_neg,
        SUM(CASE WHEN foreign_held_ratio < 0 OR foreign_held_ratio > 100 THEN 1 ELSE 0 END) held_ratio_oob,
        SUM(CASE WHEN foreign_remaining_ratio < 0 OR foreign_remaining_ratio > 100 THEN 1 ELSE 0 END) remain_ratio_oob,
        SUM(CASE WHEN foreign_limit_ratio < 0 OR foreign_limit_ratio > 100 THEN 1 ELSE 0 END) limit_oob,
        SUM(CASE WHEN foreign_held_shares > outstanding_shares THEN 1 ELSE 0 END) held_gt_out,
        SUM(CASE WHEN date > current_date THEN 1 ELSE 0 END) future_date
      FROM foreign_holding_ratio
    """).df().to_string())

    print("\n== 重複鍵 / 代號格式 ==")
    print(con.sql("""
      SELECT (SELECT COUNT(*) FROM (SELECT market,date,company_code FROM foreign_holding_ratio
              GROUP BY 1,2,3 HAVING COUNT(*)>1)) dup_keys,
             (SELECT COUNT(*) FROM foreign_holding_ratio
              WHERE NOT regexp_matches(company_code,'^[0-9]{4}[0-9A-Z]{0,2}$')) odd_codes
    """).df().to_string())
    print(con.sql("""
      SELECT company_code, COUNT(*) n FROM foreign_holding_ratio
      WHERE NOT regexp_matches(company_code,'^[0-9]{4}[0-9A-Z]{0,2}$')
      GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """).df().to_string())

    print("\n== 恆等式 E = C/A*100(容差 0.01pp)==")
    print(con.sql("""
      SELECT market, COUNT(*) n,
             SUM(CASE WHEN abs(foreign_held_ratio - foreign_held_shares*100.0/outstanding_shares) > 0.01
                      THEN 1 ELSE 0 END) bad_E
      FROM foreign_holding_ratio WHERE outstanding_shares > 0 GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n== 恆等式 B = A*F/100 - C(容差 1 股)==")
    print(con.sql("""
      SELECT market, COUNT(*) n,
             SUM(CASE WHEN abs(foreign_remaining_shares - (outstanding_shares*foreign_limit_ratio/100.0
                                                           - foreign_held_shares)) > 1
                      THEN 1 ELSE 0 END) bad_B
      FROM foreign_holding_ratio WHERE outstanding_shares > 0 GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n== foreign_limit_ratio 取值分佈 ==")
    print(con.sql("""
      SELECT market, foreign_limit_ratio, COUNT(*) n FROM foreign_holding_ratio
      GROUP BY 1,2 ORDER BY 1, 3 DESC LIMIT 20
    """).df().to_string())

    print("\n== foreign_held_ratio 分位數(逐 market)==")
    print(con.sql("""
      SELECT market, MIN(foreign_held_ratio) p0,
             quantile_cont(foreign_held_ratio,0.5) p50,
             quantile_cont(foreign_held_ratio,0.99) p99,
             MAX(foreign_held_ratio) p100,
             SUM(CASE WHEN foreign_held_ratio=0 THEN 1 ELSE 0 END) zero_rows
      FROM foreign_holding_ratio GROUP BY 1 ORDER BY 1
    """).df().to_string())

    print("\n== 單日跳動:同代號 foreign_held_ratio 相鄰交易日變動 > 20pp 的前 15 筆 ==")
    print(con.sql("""
      WITH s AS (
        SELECT market, company_code, date, foreign_held_ratio,
               LAG(foreign_held_ratio) OVER (PARTITION BY market,company_code ORDER BY date) prev,
               LAG(date) OVER (PARTITION BY market,company_code ORDER BY date) prev_date
        FROM foreign_holding_ratio)
      SELECT market, company_code, prev_date, date, prev, foreign_held_ratio,
             foreign_held_ratio-prev AS chg
      FROM s WHERE prev IS NOT NULL AND date - prev_date <= 5
        AND abs(foreign_held_ratio-prev) > 20
      ORDER BY abs(foreign_held_ratio-prev) DESC LIMIT 15
    """).df().to_string())
    print(con.sql("""
      WITH s AS (
        SELECT market, company_code, date, foreign_held_ratio,
               LAG(foreign_held_ratio) OVER (PARTITION BY market,company_code ORDER BY date) prev,
               LAG(date) OVER (PARTITION BY market,company_code ORDER BY date) prev_date
        FROM foreign_holding_ratio)
      SELECT COUNT(*) n_jump_gt20 FROM s
      WHERE prev IS NOT NULL AND date - prev_date <= 5 AND abs(foreign_held_ratio-prev) > 20
    """).df().to_string())


if __name__ == "__main__":
    main()
