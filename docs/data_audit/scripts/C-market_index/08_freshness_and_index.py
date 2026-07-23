"""C-market_index #8: cache 新鮮度、索引/唯一性、以及 TAIEX 線的實害量化。

Run: PYTHONPATH=. uv run --project research python docs/data_audit/scripts/C-market_index/08_freshness_and_index.py
"""
import os
from datetime import date

import duckdb
from research import paths
from research.data_calendar import latest_complete_trading_day, stale_tables

con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

print("== ① 新鮮度 ==")
print("  cache.duckdb mtime:", __import__("time").ctime(os.path.getmtime(paths.CACHE_DB)))
print("  latest_complete_trading_day():", latest_complete_trading_day())
print("  stale_tables():", stale_tables())
print("  market_index max(date) by market:",
      con.sql("SELECT market, max(date) FROM market_index GROUP BY 1").fetchall())

print("\n== ② cache 索引 / 唯一性 ==")
print(con.sql("SELECT index_name, is_unique, sql FROM duckdb_indexes() "
              "WHERE table_name='market_index'").df().to_string())
print("  重複 (market,date,name):",
      con.sql("SELECT count(*) FROM (SELECT market,date,name FROM market_index "
              "GROUP BY 1,2,3 HAVING count(*)>1)").fetchone()[0])

print("\n== ③ TAIEX 線髒污盤點 ==")
TAIEX = "發行量加權股價指數"
PHANTOM = ["2015-08-29", "2016-05-26", "2017-08-02", "2018-08-04",
           "2018-09-15", "2018-10-03", "2019-07-05", "2019-09-25"]
SRC = {"2015-08-29": "2015-12-18", "2016-05-26": "2016-01-18", "2017-08-02": "2017-12-18",
       "2018-08-04": "2018-07-24", "2018-09-15": "2017-03-17", "2018-10-03": "2018-06-15",
       "2019-07-05": "2019-07-16", "2019-09-25": "2019-09-02"}
for d in PHANTOM:
    r = con.sql(f"SELECT close, change FROM market_index WHERE market='twse' "
                f"AND name='{TAIEX}' AND date=DATE '{d}'").fetchone()
    s = con.sql(f"SELECT close, change FROM market_index WHERE market='twse' "
                f"AND name='{TAIEX}' AND date=DATE '{SRC[d]}'").fetchone()
    wd = date.fromisoformat(d).strftime("%a")
    print(f"  {d} ({wd}) 存的 close={r[0]} change={r[1]}   ← 來源日 {SRC[d]} close={s[0]} change={s[1]}")
n_ret = con.sql(f"SELECT count(*) FROM market_index WHERE market='twse' AND name='{TAIEX}'").fetchone()[0]
print(f"  TAIEX 共 {n_ret} 天 → 8 個幽靈日各汙染『當天 + 隔天』2 個日報酬 = 最多 16/{n_ret-1} 個報酬失真")

print("\n== ④ 缺 TAIEX 的日子 ==")
print("  2019-04-29 的 TAIEX 存在哪個名字下:",
      con.sql("SELECT name, close, change FROM market_index WHERE market='twse' "
              "AND date=DATE '2019-04-29' AND name LIKE '%加權股價指數'").fetchall())
print("  接續驗證 2019-04-26 close + 2019-04-29 change:",
      con.sql("SELECT close FROM market_index WHERE market='twse' "
              f"AND name='{TAIEX}' AND date=DATE '2019-04-26'").fetchone())

print("\n== ⑤ tpex 主線『櫃買指數』覆蓋 ==")
print("  ", con.sql("SELECT min(date), max(date), count(*) FROM market_index "
                    "WHERE market='tpex' AND name='櫃買指數'").fetchone())
print("  tpex 有資料天數:",
      con.sql("SELECT count(DISTINCT date) FROM market_index WHERE market='tpex'").fetchone()[0])
print("  櫃買指數 日報酬 |ret| 前 8:")
for r in con.sql("""
WITH t AS (SELECT date, close, lag(close) OVER (ORDER BY date) pc,
                  lag(date) OVER (ORDER BY date) pd
           FROM market_index WHERE market='tpex' AND name='櫃買指數' AND close IS NOT NULL)
SELECT date, pd, pc, close, round((close/pc-1)*100,2) FROM t
WHERE pc IS NOT NULL ORDER BY abs(close/pc-1) DESC LIMIT 8""").fetchall():
    print("   ", r)

print("\n== ⑥ 『四個舊 roster 日』對現行指數名造成的洞 ==")
for d, ref in (("2019-07-05", "2019-07-04"), ("2019-10-30", "2019-10-29"),
               ("2026-02-26", "2026-02-25"), ("2026-03-11", "2026-03-10")):
    q = f"""
    WITH ref AS (SELECT DISTINCT name FROM market_index WHERE market='twse' AND date=DATE '{ref}'),
         day AS (SELECT DISTINCT name FROM market_index WHERE market='twse' AND date=DATE '{d}')
    SELECT (SELECT count(*) FROM ref), (SELECT count(*) FROM day),
           (SELECT count(*) FROM ref ANTI JOIN day USING(name)) missing_vs_ref,
           (SELECT count(*) FROM day ANTI JOIN ref USING(name)) legacy_only"""
    print(f"  {d} vs {ref}: {con.sql(q).fetchone()}  (ref 名稱數, 當日名稱數, 當日缺的現行名, 當日獨有的舊名)")
