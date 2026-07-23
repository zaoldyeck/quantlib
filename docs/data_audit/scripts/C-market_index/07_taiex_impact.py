"""C-market_index #7: 消費端實害——TAIEX(發行量加權股價指數)序列本身有多髒?

repo 裡幾乎所有 market_index 的消費者只拿這一條線(regime 判斷、下跌市過濾、
期貨基差),所以「這張表能不能信」實務上約等於「這條線能不能信」。
本腳本:
  ① TAIEX 覆蓋率(有幾天缺)、是否混入非交易日
  ② 由 cache 直接算日報酬,列出 |ret| 最大的日子 → 幽靈日會跳出來
  ③ 檢查 name 過濾少了 market 條件會不會撈到 tpex 的列
  ④ 舊/新指數名切換(2019-04-29)造成的序列斷點清單

Run: PYTHONPATH=. uv run --project research python docs/data_audit/scripts/C-market_index/07_taiex_impact.py
"""
import duckdb
from research import paths

con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
TAIEX = "發行量加權股價指數"

print("== ① TAIEX 覆蓋 ==")
print("  ", con.sql(f"SELECT market, min(date), max(date), count(*) FROM market_index "
                    f"WHERE name='{TAIEX}' GROUP BY 1").fetchall())
print("  twse market_index 有資料的天數:",
      con.sql("SELECT count(DISTINCT date) FROM market_index WHERE market='twse'").fetchone()[0])
print("  其中缺 TAIEX 的日子:",
      con.sql(f"SELECT date FROM (SELECT DISTINCT date FROM market_index WHERE market='twse') d "
              f"WHERE NOT EXISTS (SELECT 1 FROM market_index m WHERE m.market='twse' "
              f"AND m.name='{TAIEX}' AND m.date=d.date) ORDER BY 1").fetchall())
print("  TAIEX 落在非交易日(daily_quote twse 當天 <100 檔)的列:")
for r in con.sql(f"""
    WITH t AS (SELECT date, close FROM market_index WHERE market='twse' AND name='{TAIEX}'),
         q AS (SELECT date, count(*) n FROM daily_quote WHERE market='twse' GROUP BY 1)
    SELECT t.date, t.close, coalesce(q.n,0) FROM t LEFT JOIN q USING(date)
    WHERE coalesce(q.n,0) < 100 ORDER BY 1""").fetchall():
    print(f"    {r[0]} ({r[0].strftime('%a')})  close={r[1]}  daily_quote={r[2]} 檔")

print("\n== ② TAIEX 日報酬 |ret| 前 15 名 ==")
q = f"""
WITH t AS (SELECT date, close, lag(close) OVER (ORDER BY date) pc,
                  lag(date) OVER (ORDER BY date) pd
           FROM market_index WHERE market='twse' AND name='{TAIEX}' AND close IS NOT NULL)
SELECT date, pd, pc, close, round((close/pc-1)*100, 2) ret_pct
FROM t WHERE pc IS NOT NULL ORDER BY abs(close/pc-1) DESC LIMIT 15"""
for r in con.sql(q).fetchall():
    print(f"    {r[0]} (前一交易日 {r[1]})  {r[2]} → {r[3]}  {r[4]:+.2f}%")

print("\n  台股單日漲跌幅上限 ±10%(2015-06-01 起;之前 ±7%)——超過即不可能:")
print("   ", con.sql(f"""
WITH t AS (SELECT date, close, lag(close) OVER (ORDER BY date) pc
           FROM market_index WHERE market='twse' AND name='{TAIEX}' AND close IS NOT NULL)
SELECT count(*) FROM t WHERE pc IS NOT NULL AND abs(close/pc-1) > 0.10""").fetchone()[0], "列 |ret|>10%")

print("\n== ③ name 過濾沒加 market 條件會不會撈到 tpex? ==")
print("  ", con.sql(f"SELECT market, count(*) FROM market_index WHERE name='{TAIEX}' GROUP BY 1").fetchall())
print("  name LIKE '%發行量加權股價指數%' 的所有名稱:",
      con.sql("SELECT DISTINCT market, name FROM market_index "
              "WHERE name LIKE '%發行量加權股價指數%'").fetchall())

print("\n== ④ 2019-04-29 舊/新指數名切換造成的序列斷點 ==")
q = """
WITH f AS (SELECT name, min(date) fd, max(date) ld, count(*) n
           FROM market_index WHERE market='twse' GROUP BY 1)
SELECT name, fd, ld, n FROM f
WHERE ld >= DATE '2019-04-29' AND fd < DATE '2019-04-29' AND n < 2600
ORDER BY n, name"""
rows = con.sql(q).fetchall()
print(f"  『2019-04-29 之前就存在、之後只零星出現』的舊名 {len(rows)} 檔:")
for r in rows[:60]:
    print(f"    {r[0]:28} {r[1]} → {r[2]}  共 {r[3]} 天")

print("\n  新名(2019-04-29 首見)在 2026-02-26 / 2026-03-11 的缺洞:")
q2 = """
WITH newnames AS (SELECT DISTINCT name FROM market_index
                  WHERE market='twse' AND date=DATE '2026-02-25')
SELECT count(*) FROM newnames n
WHERE NOT EXISTS (SELECT 1 FROM market_index m WHERE m.market='twse'
                  AND m.date=DATE '2026-02-26' AND m.name=n.name)"""
print("   ", con.sql(q2).fetchone()[0], "檔在 2026-02-26 查無資料(2026-02-25 有 267 檔)")
