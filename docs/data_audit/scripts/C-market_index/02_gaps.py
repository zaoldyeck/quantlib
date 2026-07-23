"""C-market_index #2: 覆蓋缺口——cache 的 market_index 少了哪些交易日?

三種互相獨立的方法:
  A 證人投票:同一份 cache 裡其他 6 張日頻表在該 (market,date) 有資料,market_index 沒有。
  B 純日曆:quantlib.data_calendar.is_trading_day(讀 0-byte sentinel,含颱風假)為 True 的平日。
  C 原始檔形態:data/index/<market>/<year>/<Y>_<M>_<D>.csv 存不存在 / 幾 bytes。
另外反向掃「幽靈日」:market_index 有資料但全市場其他表都沒有。

Run: PYTHONPATH=. uv run --project . python docs/data_audit/scripts/C-market_index/02_gaps.py
需要:var/cache/cache.duckdb(只讀)+ data/ 原始檔目錄。
"""
from datetime import date, timedelta
from pathlib import Path

import duckdb
from research import paths
from quantlib.data_calendar import is_trading_day

ROOT = Path(__file__).resolve().parents[4]
con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

OTHER = ["daily_quote", "daily_trading_details", "stock_per_pbr",
         "margin_transactions", "sbl_borrowing", "foreign_holding_ratio"]

print("== 0. 各表覆蓋區間(cache) ==")
for t in ["market_index"] + OTHER:
    r = con.sql(f"SELECT market, min(date), max(date), count(DISTINCT date) "
                f"FROM {t} GROUP BY 1 ORDER BY 1").fetchall()
    for m, lo, hi, nd in r:
        print(f"  {t:24} {m:5} {lo} → {hi}  ({nd:,} 天)")

mi_range = {m: (lo, hi) for m, lo, hi in
            con.sql("SELECT market, min(date), max(date) FROM market_index GROUP BY 1").fetchall()}

print("\n== A. 證人投票:其他表有、market_index 沒有(限縮在 market_index 自己的覆蓋區間內)==")
for mkt, (lo, hi) in sorted(mi_range.items()):
    union = " UNION ALL ".join(
        f"SELECT DISTINCT date, '{t}' AS src FROM {t} WHERE market='{mkt}' "
        f"AND date BETWEEN DATE '{lo}' AND DATE '{hi}'"
        for t in OTHER)
    q = f"""
    WITH w AS ({union}),
         agg AS (SELECT date, count(DISTINCT src) n_wit, string_agg(DISTINCT src, ',') wit
                 FROM w GROUP BY 1),
         mi AS (SELECT DISTINCT date FROM market_index WHERE market='{mkt}')
    SELECT agg.date, agg.n_wit, agg.wit FROM agg
    LEFT JOIN mi ON mi.date=agg.date
    WHERE mi.date IS NULL
    ORDER BY 1"""
    rows = con.sql(q).fetchall()
    print(f"  [{mkt}] {len(rows)} 天有證人但 market_index 無資料")
    for d, n, w in rows:
        f = ROOT / "data" / "index" / mkt / str(d.year) / f"{d.year}_{d.month}_{d.day}.csv"
        sz = f.stat().st_size if f.exists() else "NO_FILE"
        print(f"    {d} 證人={n} ({w})  raw={sz}")

print("\n== B. 純日曆:is_trading_day=True 的平日但 market_index 無資料 ==")
for mkt, (lo, hi) in sorted(mi_range.items()):
    have = {r[0] for r in con.sql(
        f"SELECT DISTINCT date FROM market_index WHERE market='{mkt}'").fetchall()}
    miss = []
    d = lo
    while d <= hi:
        if d not in have and is_trading_day(d):
            miss.append(d)
        d += timedelta(days=1)
    print(f"  [{mkt}] {len(miss)} 天")
    for d in miss:
        f = ROOT / "data" / "index" / mkt / str(d.year) / f"{d.year}_{d.month}_{d.day}.csv"
        sz = f.stat().st_size if f.exists() else "NO_FILE"
        print(f"    {d} ({d.strftime('%a')})  raw={sz}")

print("\n== C. 幽靈日:market_index 有資料但同市場其他 6 表全部 0 列 ==")
for mkt, (lo, hi) in sorted(mi_range.items()):
    union = " UNION ALL ".join(
        f"SELECT DISTINCT date, '{t}' AS src FROM {t} WHERE market='{mkt}'" for t in OTHER)
    # 只在其他表自己也有覆蓋的區間內判定,否則起點差異會製造假陽性
    cov = con.sql(f"""SELECT min(mn), max(mx) FROM (
        {' UNION ALL '.join(f"SELECT min(date) mn, max(date) mx FROM {t} WHERE market='{mkt}'" for t in OTHER)})
        """).fetchone()
    q = f"""
    WITH w AS ({union}),
         agg AS (SELECT date, count(DISTINCT src) n FROM w GROUP BY 1),
         mi AS (SELECT date, count(*) n FROM market_index WHERE market='{mkt}' GROUP BY 1)
    SELECT mi.date, mi.n FROM mi LEFT JOIN agg ON agg.date=mi.date
    WHERE agg.date IS NULL AND mi.date BETWEEN DATE '{cov[0]}' AND DATE '{cov[1]}'
    ORDER BY 1"""
    rows = con.sql(q).fetchall()
    print(f"  [{mkt}] {len(rows)} 天(其他表覆蓋區間 {cov[0]}~{cov[1]})")
    for d, n in rows:
        print(f"    {d} ({d.strftime('%a')})  market_index {n} 列  is_trading_day={is_trading_day(d)}")

print("\n== D. 半殘日:當日列數 < 前後 21 日滾動中位數的 80% ==")
q = """
WITH d AS (SELECT market, date, count(*) n FROM market_index GROUP BY 1,2),
     r AS (SELECT *, median(n) OVER (PARTITION BY market ORDER BY date
                 ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) med FROM d)
SELECT market, date, n, med FROM r WHERE n < med*0.8 ORDER BY market, date"""
for r in con.sql(q).fetchall():
    print(f"  {r[0]} {r[1]} ({r[1].strftime('%a')})  n={r[2]}  med={r[3]:.0f}")
