"""C-margin_transactions ⑤:純日期序列的連續性(涵蓋沒有證人表的早期區間)。

02_gaps.py 用「其他日頻表當證人」找缺日,但 twse margin 起於 2001-01-02、
daily_quote 起於 2004-02-11——2001~2004 這 3 年沒有任何證人。這裡改用兩個
不依賴其他表的方法:

1. **相鄰交易日間隔**:列出間隔 > 4 天(排除週末)的斷口,人工對照國定假日。
2. **平日空洞**:margin 沒有、且不是週末、且 `is_trading_day` 為真的日子
   (`is_trading_day` 讀 daily_quote 的 0-byte sentinel;2004 之前沒有 sentinel,
   一律回 True,所以這段的輸出會包含所有國定假日,需人工判讀)。

Run: PYTHONPATH=<repo> uv run --project . python docs/data_audit/scripts/C-margin_transactions/05_calendar_gaps.py
"""
from __future__ import annotations

import duckdb
import pandas as pd

from research import paths
from quantlib.data_calendar import is_trading_day


def main() -> None:
    pd.set_option("display.width", 200)
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    print("=== 相鄰交易日間隔 > 4 天的斷口 ===")
    df = con.execute("""
        WITH d AS (SELECT DISTINCT market, date FROM margin_transactions),
             s AS (SELECT market, date, lag(date) OVER (PARTITION BY market ORDER BY date) prev
                   FROM d)
        SELECT market, prev, date, date - prev AS gap_days, dayname(prev) prev_dow, dayname(date) dow
        FROM s WHERE date - prev > 4 ORDER BY market, prev""").df()
    print(f"共 {len(df)} 個")
    print(df.to_string())

    print("\n=== 平日空洞(margin 無、非週末、is_trading_day=True)===")
    for market in ("twse", "tpex"):
        dmin, dmax = con.execute(
            "SELECT min(date), max(date) FROM margin_transactions WHERE market=?",
            [market]).fetchone()
        have = {r[0] for r in con.execute(
            "SELECT DISTINCT date FROM margin_transactions WHERE market=?",
            [market]).fetchall()}
        allw = con.execute(
            "SELECT unnest(generate_series(?::DATE, ?::DATE, INTERVAL 1 DAY))::DATE d",
            [dmin, dmax]).fetchall()
        miss = [d for (d,) in allw
                if d.weekday() < 5 and d not in have and is_trading_day(d)]
        by_year: dict[int, list] = {}
        for d in miss:
            by_year.setdefault(d.year, []).append(d)
        print(f"  [{market}] {dmin}~{dmax} 平日空洞 {len(miss)} 天;逐年:")
        for y in sorted(by_year):
            ds = by_year[y]
            head = ", ".join(str(x) for x in ds[:12])
            print(f"    {y}: {len(ds)} 天  {head}{' …' if len(ds) > 12 else ''}")


if __name__ == "__main__":
    main()
