"""C-sbl_borrowing ②:覆蓋缺口——哪些交易日整天沒有借券資料。

三種互相獨立的判準,交集才算「真的漏抓」:
  A. 證人投票:同一 (market, date) 其他日頻表(daily_quote / daily_trading_details /
     stock_per_pbr / market_index / margin_transactions / foreign_holding_ratio)
     有資料,而 sbl 沒有。
  B. 純日曆:`research.data_calendar.is_trading_day`(讀 daily_quote 的 0-byte
     sentinel,颱風假才判得出來)說是交易日,而 sbl 沒有。
  C. 原始檔形態:`data/sbl_borrowing/<market>/<year>/` 底下該日檔案的大小/內容——
     交易所親口回「無資料」 vs 抓失敗留下的殘檔,大小分布不同。

Run: PYTHONPATH=<repo> uv run --project research python docs/data_audit/scripts/C-sbl_borrowing/02_gaps.py
依賴:var/cache/cache.duckdb 為最新(cache_tables.py)。
"""
from __future__ import annotations

import datetime as dt
import os

import duckdb
import pandas as pd

from research import paths
from research.data_calendar import is_trading_day

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))))
RAW = os.path.join(REPO, "data", "sbl_borrowing")

WITNESS = ["daily_quote", "daily_trading_details", "stock_per_pbr",
           "margin_transactions", "foreign_holding_ratio"]


def main() -> None:
    pd.set_option("display.width", 250)
    pd.set_option("display.max_rows", 400)
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    lo, hi = con.sql("SELECT min(date), max(date) FROM sbl_borrowing").fetchone()
    print(f"sbl 覆蓋區間: {lo} ~ {hi}")
    print(con.sql("SELECT market, count(*) AS n_rows, count(DISTINCT date) AS n_days, "
                  "min(date) AS lo, max(date) AS hi FROM sbl_borrowing GROUP BY 1 ORDER BY 1")
          .df().to_string())

    # ---- A. 證人投票 -------------------------------------------------------
    print("\n=== A. 證人投票(其他表有資料、sbl 沒有)===")
    parts = " UNION ALL ".join(
        f"SELECT market, date, '{t}' AS src FROM {t} WHERE date BETWEEN DATE '{lo}' AND DATE '{hi}'"
        for t in WITNESS)
    q = f"""
    WITH w AS (SELECT market, date, src FROM ({parts}) GROUP BY 1,2,3),
         wd AS (SELECT market, date, count(*) n_wit, string_agg(src, ',' ORDER BY src) wits
                FROM w GROUP BY 1,2),
         s AS (SELECT DISTINCT market, date FROM sbl_borrowing)
    SELECT wd.market, wd.date, wd.n_wit, wd.wits
    FROM wd LEFT JOIN s USING (market, date)
    WHERE s.date IS NULL
    ORDER BY wd.n_wit DESC, wd.market, wd.date
    """
    a = con.sql(q).df()
    print(f"總數: {len(a)}")
    print(a.groupby(["market", "n_wit"]).size().to_string())
    print(a[a.n_wit >= 2].to_string() if len(a) else "(無)")

    # ---- B. 純日曆 ---------------------------------------------------------
    print("\n=== B. 純日曆(is_trading_day=True 但 sbl 無列)===")
    have = {(m, d) for m, d in con.sql(
        "SELECT DISTINCT market, date FROM sbl_borrowing").fetchall()}
    rows = []
    for market in ("twse", "tpex"):
        d = lo
        while d <= hi:
            if d.weekday() < 5 and (market, d) not in have and is_trading_day(d):
                rows.append((market, d))
            d += dt.timedelta(days=1)
    b = pd.DataFrame(rows, columns=["market", "date"])
    print(f"總數: {len(b)}")
    print(b.groupby(["market", b.date.map(lambda x: x.year)]).size().to_string()
          if len(b) else "(無)")

    # ---- C. 原始檔形態 ------------------------------------------------------
    print("\n=== C. 這些日子的原始檔長什麼樣 ===")
    out = []
    for market, d in rows:
        f = os.path.join(RAW, market, str(d.year), f"{d.year}_{d.month}_{d.day}.csv")
        if not os.path.exists(f):
            out.append((market, d, -1, "(檔案不存在)"))
            continue
        sz = os.path.getsize(f)
        head = ""
        if sz:
            with open(f, "rb") as fh:
                raw = fh.read(300)
            try:
                head = raw.decode("big5-hkscs", errors="replace").replace("\r\n", " | ")[:160]
            except Exception:  # noqa: BLE001
                head = repr(raw[:120])
        out.append((market, d, sz, head))
    c = pd.DataFrame(out, columns=["market", "date", "bytes", "head"])
    print(c.groupby(["market", "bytes"]).size().to_string() if len(c) else "(無)")
    print("\n每種 size 各一個樣本:")
    if len(c):
        print(c.drop_duplicates(subset=["market", "bytes"]).to_string())
    print("\n全部(市場, 日期, bytes):")
    print(c[["market", "date", "bytes"]].to_string() if len(c) else "(無)")

    # ---- 對照:sbl 有資料但 daily_quote 沒有(幽靈日)-------------------------
    print("\n=== D. sbl 有資料但同市場 daily_quote 該日 0 列(幽靈日候選)===")
    g = con.sql(f"""
    WITH s AS (SELECT market, date, count(*) n FROM sbl_borrowing GROUP BY 1,2),
         dq AS (SELECT market, date FROM daily_quote
                WHERE date BETWEEN DATE '{lo}' AND DATE '{hi}' GROUP BY 1,2)
    SELECT s.market, s.date, s.n FROM s LEFT JOIN dq USING (market, date)
    WHERE dq.date IS NULL ORDER BY 1,2""").df()
    print(f"總數: {len(g)}")
    print(g.to_string() if len(g) else "(無)")

    # ---- 每年每市場的日數 vs daily_quote ------------------------------------
    print("\n=== E. 逐年:sbl 有幾天 vs daily_quote 有幾天 ===")
    print(con.sql(f"""
    WITH s AS (SELECT market, year(date) y, count(DISTINCT date) d FROM sbl_borrowing GROUP BY 1,2),
         q AS (SELECT market, year(date) y, count(DISTINCT date) d FROM daily_quote
               WHERE date BETWEEN DATE '{lo}' AND DATE '{hi}' GROUP BY 1,2)
    SELECT coalesce(s.market,q.market) market, coalesce(s.y,q.y) y,
           s.d sbl_days, q.d dq_days, coalesce(s.d,0)-coalesce(q.d,0) diff
    FROM s FULL JOIN q USING (market, y) ORDER BY 1,2""").df().to_string())


if __name__ == "__main__":
    main()
