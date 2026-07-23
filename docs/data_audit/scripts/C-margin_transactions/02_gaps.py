"""C-margin_transactions ②:cache 端的日期覆蓋缺口(缺日 / 幽靈日)。

兩種缺口各用互相獨立的方法認定,避免單一日曆源自己就是錯的:

- **缺日**:margin 沒有該 (market,date),但其他 6 張日頻表有(證人投票),再用
  `research.data_calendar.is_trading_day`(讀 0-byte sentinel)複核。**注意日曆本身
  有毒**:C-daily_quote 已查出 twse 2021-08-18 / 2025-08-15 / 2026-04-29 /
  2026-05-28 四天是真交易日卻留了假 sentinel,故此處以「證人數」為主判據、
  `is_trading_day` 僅作旁證。
- **幽靈日**:margin 有資料,但 daily_quote 當日該市場 0 列 → 休市日卻有數字。
  **必須限縮在 daily_quote 該市場自己有覆蓋的區間內**(twse daily_quote 起
  2004-02-11、tpex 起 2007-07-02),否則報價表自己的起點會被誤判成幾百個幽靈日。

Run: PYTHONPATH=<repo> uv run --project research python docs/data_audit/scripts/C-margin_transactions/02_gaps.py
依賴:var/cache/cache.duckdb(唯讀)、data/daily_quote/twse/*/ 的 sentinel 檔。
"""
from __future__ import annotations

from collections import Counter

import duckdb

from research import paths
from research.data_calendar import (is_trading_day, latest_complete_trading_day,
                                    stale_tables)

WITNESS = ["daily_quote", "daily_trading_details", "stock_per_pbr",
           "market_index", "sbl_borrowing", "foreign_holding_ratio"]


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    print("=== cache 新鮮度 ===")
    print("latest_complete_trading_day:", latest_complete_trading_day())
    print("stale_tables():", stale_tables())

    rng = con.execute("SELECT market, min(date), max(date), count(DISTINCT date), count(*) "
                      "FROM margin_transactions GROUP BY 1 ORDER BY 1").fetchall()
    print("\n=== margin 起訖 ===")
    for r in rng:
        print(" ", r)
    print("daily_quote 起訖:",
          con.execute("SELECT market, min(date), max(date) FROM daily_quote "
                      "GROUP BY 1 ORDER BY 1").fetchall())

    union = " UNION ALL ".join(
        f"SELECT market, date, '{t}' AS src FROM {t}" for t in WITNESS)
    con.execute(f"CREATE OR REPLACE TEMP VIEW wit AS {union}")

    for market, dmin, dmax, _, _ in rng:
        print(f"\n=== [{market}] 缺日候選(證人有、margin 無;限 margin 起訖區間內)===")
        rows = con.execute(
            "SELECT w.date, count(DISTINCT w.src) AS n_witness, "
            "       string_agg(DISTINCT w.src, ',' ORDER BY w.src) AS witnesses "
            "FROM wit w WHERE w.market = ? AND w.date BETWEEN ? AND ? "
            "  AND NOT EXISTS (SELECT 1 FROM margin_transactions m "
            "                  WHERE m.market = w.market AND m.date = w.date) "
            "GROUP BY 1 ORDER BY 1", [market, dmin, dmax]).fetchall()
        strong = [r for r in rows if r[1] >= 2]
        weak = [r for r in rows if r[1] < 2]
        print(f"候選 {len(rows)} 天 → 證人 ≥2 的 {len(strong)} 天(判真缺漏)、"
              f"證人 =1 的 {len(weak)} 天")
        for d, n, w in strong:
            print(f"  真缺漏 {d} witnesses={n} is_trading_day={is_trading_day(d)} [{w}]")
        if weak:
            print("  單一證人日的證人分布:",
                  Counter(w for _, _, w in weak),
                  " is_trading_day=True 的有:",
                  [str(d) for d, _, _ in weak if is_trading_day(d)])

        # 幽靈日:限縮在 daily_quote 該市場自己有覆蓋的區間內
        qmin, qmax = con.execute(
            "SELECT min(date), max(date) FROM daily_quote WHERE market = ?",
            [market]).fetchone()
        lo, hi = max(dmin, qmin), min(dmax, qmax)
        print(f"=== [{market}] 幽靈日(margin 有、daily_quote 0 列;限 {lo}~{hi})===")
        g = con.execute(
            "SELECT m.date, count(*) AS n_rows FROM margin_transactions m "
            "WHERE m.market = ? AND m.date BETWEEN ? AND ? AND NOT EXISTS ("
            "  SELECT 1 FROM daily_quote q WHERE q.market = m.market AND q.date = m.date) "
            "GROUP BY 1 ORDER BY 1", [market, lo, hi]).fetchall()
        for d, n in g:
            other = con.execute(
                "SELECT count(DISTINCT src) FROM wit WHERE market=? AND date=?",
                [market, d]).fetchone()[0]
            print(f"  {d} margin_rows={n} is_trading_day={is_trading_day(d)} "
                  f"其他證人表數={other}")
        if not g:
            print("  (無)")

    print("\n=== 逐年交易日數(cache)===")
    print(con.execute(
        "SELECT year(date) y, "
        "  count(DISTINCT date) FILTER (market='twse') twse_days, "
        "  count(DISTINCT date) FILTER (market='tpex') tpex_days "
        "FROM margin_transactions GROUP BY 1 ORDER BY 1").df().to_string())


if __name__ == "__main__":
    main()
