"""C-foreign_holding_ratio 稽核 03:日期覆蓋缺口(對 daily_quote 母體 + 休市日曆)。

三路交叉:
  (1) 同市場 daily_quote 有報價、foreign_holding_ratio 沒資料的日期 → 疑似漏抓
  (2) research/data_calendar.py::is_trading_day(0-byte sentinel 休市日曆,含颱風假)複核
  (3) 反向:本表有、daily_quote 沒有的日期(幽靈日)
另附逐年「日數 x 每日列數」概況,判斷 tpex 2010 列數異常。

Run: uv run --project research python docs/data_audit/scripts/C-foreign_holding_ratio/03_coverage_gaps.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402
from research.data_calendar import is_trading_day  # noqa: E402

START = {"twse": "2005-01-03", "tpex": "2010-01-04"}  # ForeignHoldingRatioSetting 的起始日


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    print("== 逐年 x market:日數 / 列數 / 每日中位列數 ==")
    print(con.sql("""
      WITH d AS (SELECT market, date, COUNT(*) n FROM foreign_holding_ratio GROUP BY 1,2)
      SELECT market, year(date) y, COUNT(*) n_days, SUM(n) n_rows,
             MEDIAN(n) med_rows, MIN(n) min_rows, MAX(n) max_rows
      FROM d GROUP BY 1,2 ORDER BY 1,2
    """).df().to_string())

    for mkt in ("twse", "tpex"):
        print(f"\n== [{mkt}] daily_quote 有、foreign_holding_ratio 沒有的日期 ==")
        rows = con.sql(f"""
          WITH q AS (SELECT DISTINCT date FROM daily_quote
                     WHERE market='{mkt}' AND date >= DATE '{START[mkt]}'),
               f AS (SELECT DISTINCT date FROM foreign_holding_ratio WHERE market='{mkt}')
          SELECT q.date,
                 (SELECT COUNT(*) FROM daily_quote dq
                  WHERE dq.market='{mkt}' AND dq.date=q.date) AS quote_rows
          FROM q LEFT JOIN f ON q.date=f.date
          WHERE f.date IS NULL ORDER BY 1
        """).fetchall()
        print(f"  缺 {len(rows)} 天")
        for d, qn in rows:
            print(f"    {d} {d.strftime('%a')} quote_rows={qn} is_trading_day={is_trading_day(d)}")

        print(f"\n== [{mkt}] foreign_holding_ratio 有、daily_quote 沒有的日期(幽靈日)==")
        rows = con.sql(f"""
          WITH q AS (SELECT DISTINCT date FROM daily_quote WHERE market='{mkt}'),
               f AS (SELECT date, COUNT(*) n FROM foreign_holding_ratio
                     WHERE market='{mkt}' GROUP BY 1)
          SELECT f.date, f.n FROM f LEFT JOIN q ON q.date=f.date
          WHERE q.date IS NULL ORDER BY 1
        """).fetchall()
        print(f"  {len(rows)} 天")
        for d, n in rows[:40]:
            print(f"    {d} {d.strftime('%a')} fhr_rows={n} is_trading_day={is_trading_day(d)}")

    print("\n== 連續日期缺口(相鄰兩個有資料日的間隔 > 5 天)==")
    for mkt in ("twse", "tpex"):
        rows = con.sql(f"""
          WITH d AS (SELECT DISTINCT date FROM foreign_holding_ratio WHERE market='{mkt}'),
               l AS (SELECT date, LAG(date) OVER (ORDER BY date) prev FROM d)
          SELECT prev, date, date - prev AS gap_days FROM l
          WHERE prev IS NOT NULL AND date - prev > 5 ORDER BY 3 DESC
        """).fetchall()
        print(f"  [{mkt}] {len(rows)} 段")
        for prev, cur, gap in rows[:30]:
            print(f"    {prev} → {cur}  gap={gap}d")


if __name__ == "__main__":
    main()
