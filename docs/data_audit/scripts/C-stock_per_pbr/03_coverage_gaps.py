"""C-stock_per_pbr 稽核 03:日期覆蓋缺口。

三種比對:
1. cache 的 stock_per_pbr 日期 vs cache 的 daily_quote 日期(同市場;daily_quote
   是本專案事實上的交易日母體)。
2. 對 `quantlib.data_calendar.is_trading_day`(sentinel 休市日曆)逐日檢查。
3. 每日列數異常低的日子(可能是部分公告)。

Run: uv run --project . python docs/data_audit/scripts/C-stock_per_pbr/03_coverage_gaps.py
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402
from quantlib.data_calendar import is_trading_day  # noqa: E402


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    print("== 1) stock_per_pbr 缺、daily_quote 有的交易日 ==")
    for mkt in ("twse", "tpex"):
        df = con.sql(f"""
          SELECT date, n FROM (
            SELECT date, COUNT(*) n FROM daily_quote WHERE market='{mkt}' GROUP BY 1) q
          WHERE date >= (SELECT MIN(date) FROM stock_per_pbr WHERE market='{mkt}')
            AND date NOT IN (SELECT DISTINCT date FROM stock_per_pbr WHERE market='{mkt}')
          ORDER BY date
        """).df()
        print(f"\n-- {mkt}: {len(df)} 天 --")
        print(df.to_string() if len(df) else "(無)")

    print("\n== 2) stock_per_pbr 有、daily_quote 沒有的日 ==")
    for mkt in ("twse", "tpex"):
        df = con.sql(f"""
          SELECT DISTINCT date FROM stock_per_pbr WHERE market='{mkt}'
            AND date NOT IN (SELECT DISTINCT date FROM daily_quote WHERE market='{mkt}')
          ORDER BY date
        """).df()
        print(f"-- {mkt}: {len(df)} 天 -- {df['date'].tolist()[:20]}")

    print("\n== 3) 對 sentinel 休市日曆:是交易日卻沒有 stock_per_pbr 的日 ==")
    for mkt in ("twse", "tpex"):
        rows = con.sql(
            f"SELECT MIN(date), MAX(date) FROM stock_per_pbr WHERE market='{mkt}'").fetchone()
        have = {r[0] for r in con.sql(
            f"SELECT DISTINCT date FROM stock_per_pbr WHERE market='{mkt}'").fetchall()}
        d, end, miss = rows[0], rows[1], []
        while d <= end:
            if is_trading_day(d) and d not in have:
                miss.append(d)
            d += timedelta(days=1)
        print(f"-- {mkt}: {len(miss)} 天 --")
        print(miss if len(miss) <= 60 else f"{miss[:30]} ... {miss[-30:]}")

    print("\n== 4) 每日列數異常低(< 該市場中位數 50%)==")
    for mkt in ("twse", "tpex"):
        df = con.sql(f"""
          WITH d AS (SELECT date, COUNT(*) n FROM stock_per_pbr WHERE market='{mkt}' GROUP BY 1),
               m AS (SELECT median(n) med FROM d)
          SELECT date, n, (SELECT med FROM m) med FROM d
          WHERE n < (SELECT med FROM m) * 0.5 ORDER BY date
        """).df()
        print(f"-- {mkt}: {len(df)} 天 --")
        print(df.to_string() if len(df) else "(無)")


if __name__ == "__main__":
    main()
