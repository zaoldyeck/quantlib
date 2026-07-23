"""C-foreign_holding_ratio 稽核 09:用「共識交易日曆」列出真正漏抓的日期。

單一表的日期集合都可能自帶洞(daily_quote/twse 已知有 0-byte 假休市),
所以母體用共識法:一個日期只要在 daily_quote(twse)/daily_quote(tpex)/market_index/
margin_transactions/daily_trading_details/stock_per_pbr/taifex_futures_daily
七個來源中**至少 2 個**有資料,就算交易日。

Run: uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/09_missing_trading_days.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402
from quantlib.data_calendar import is_trading_day  # noqa: E402

CONSENSUS = """
CREATE OR REPLACE TEMP VIEW cal AS
WITH src AS (
  SELECT DISTINCT date, 'dq_twse' s FROM daily_quote WHERE market='twse'
  UNION ALL SELECT DISTINCT date, 'dq_tpex' FROM daily_quote WHERE market='tpex'
  UNION ALL SELECT DISTINCT date, 'index'   FROM market_index
  UNION ALL SELECT DISTINCT date, 'margin'  FROM margin_transactions
  UNION ALL SELECT DISTINCT date, 'dtd'     FROM daily_trading_details
  UNION ALL SELECT DISTINCT date, 'spp'     FROM stock_per_pbr
  UNION ALL SELECT DISTINCT date, 'taifex'  FROM taifex_futures_daily
)
SELECT date, COUNT(*) n_src, string_agg(s, ',' ORDER BY s) srcs
FROM src GROUP BY 1 HAVING COUNT(*) >= 2
"""

# 起點:ForeignHoldingRatioSetting 的 firstDate(twse)/ 實證真資料起點(tpex,見 08)
START = {"twse": "2005-01-03", "tpex": "2011-01-03"}


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql(CONSENSUS)
    print("共識交易日總數:", con.sql("SELECT COUNT(*) FROM cal").fetchone()[0])

    for mkt in ("twse", "tpex"):
        rows = con.execute(f"""
          SELECT c.date, c.n_src, c.srcs
          FROM cal c
          LEFT JOIN (SELECT DISTINCT date FROM foreign_holding_ratio WHERE market='{mkt}') f
                 ON f.date = c.date
          WHERE c.date >= DATE '{START[mkt]}' AND f.date IS NULL
          ORDER BY 1
        """).fetchall()
        print(f"\n===== [{mkt}] 共識交易日但 foreign_holding_ratio 無資料:{len(rows)} 天 =====")
        for d, n, s in rows:
            print(f"    {d} {d.strftime('%a')} n_src={n} ({s}) py_is_trading_day={is_trading_day(d)}")

        extra = con.execute(f"""
          SELECT f.date, f.n
          FROM (SELECT date, COUNT(*) n FROM foreign_holding_ratio WHERE market='{mkt}' GROUP BY 1) f
          LEFT JOIN cal c ON c.date=f.date
          WHERE c.date IS NULL AND f.date >= DATE '{START[mkt]}'
          ORDER BY 1
        """).fetchall()
        print(f"  [{mkt}] 本表有、共識日曆判為非交易日:{len(extra)} 天")
        for d, n in extra[:30]:
            print(f"    {d} {d.strftime('%a')} rows={n}")


if __name__ == "__main__":
    main()
