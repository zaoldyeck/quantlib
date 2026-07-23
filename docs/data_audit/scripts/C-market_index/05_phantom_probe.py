"""C-market_index #5: 幽靈日(整天是別天的資料)——放寬到「交集名稱全等」的撞號法,
並逐一解釋 ④ 語意一致性掃描裡每個「單日 ≥20 列不一致」的日期。

#4 的嚴格指紋(整天 name 集合 + 值都相同)只抓到 6 對,漏掉『部分複製』的情形
(來源日與汙染日的指數清單不同)。這裡改成:兩個日期在**共同指數名**上的
(close, change) 全部相同且共同名稱數 ≥ 50 → 判撞號。

Run: PYTHONPATH=. uv run --project research python docs/data_audit/scripts/C-market_index/05_phantom_probe.py
"""
import duckdb
from research import paths

con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

print("== A. 交集名稱全等的日期對(twse)==")
q = """
WITH d AS (SELECT market, date, name, close, change FROM market_index WHERE market='twse'),
     j AS (
       SELECT a.date d1, b.date d2,
              count(*) n_common,
              count(*) FILTER (WHERE a.close IS NOT DISTINCT FROM b.close
                               AND a.change IS NOT DISTINCT FROM b.change) n_same
       FROM d a JOIN d b ON a.name=b.name AND a.date < b.date
       WHERE a.date >= DATE '2015-01-01'
       GROUP BY 1,2)
SELECT d1, d2, n_common, n_same FROM j
WHERE n_common >= 50 AND n_same = n_common ORDER BY d2"""
for r in con.sql(q).fetchall():
    print("   ", r)

print("\n== B. 每個受污染日 vs 來源日的列數 ==")
PAIRS = [("2015-12-18", "2015-08-29"), ("2016-01-18", "2016-05-26"),
         ("2017-12-18", "2017-08-02"), ("2018-07-24", "2018-08-04"),
         ("2017-03-17", "2018-09-15"), ("2018-06-15", "2018-10-03"),
         ("2019-07-16", "2019-07-05"), ("2019-09-02", "2019-09-25")]
for src, bad in PAIRS:
    r = con.sql(f"""
      WITH a AS (SELECT name,close,change FROM market_index WHERE market='twse' AND date=DATE '{src}'),
           b AS (SELECT name,close,change FROM market_index WHERE market='twse' AND date=DATE '{bad}')
      SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b),
             (SELECT count(*) FROM a JOIN b USING(name)),
             (SELECT count(*) FROM a JOIN b USING(name)
              WHERE a.close IS NOT DISTINCT FROM b.close AND a.change IS NOT DISTINCT FROM b.change)
    """).fetchone()
    print(f"  來源 {src} n={r[0]:>4}  受汙染 {bad} n={r[1]:>4}  共同名稱={r[2]:>4}  值全同={r[3]:>4}")

print("\n== C. 逐日不一致列數(全表,含 <20 的)——前 40 名 ==")
q = """
WITH s AS (SELECT market, name, date, close, change,
                  lag(close) OVER (PARTITION BY market,name ORDER BY date) pc,
                  lag(date)  OVER (PARTITION BY market,name ORDER BY date) pd
           FROM market_index WHERE close IS NOT NULL)
SELECT market, date, count(*) n_bad, max(pd) prev_date
FROM s WHERE pc IS NOT NULL AND abs(close-pc-change) > 0.02
GROUP BY 1,2 ORDER BY 3 DESC LIMIT 40"""
for r in con.sql(q).fetchall():
    print(f"  {r[0]} {r[1]} ({r[1].strftime('%a')})  不一致 {r[2]:>4} 列   前一筆日期(max)={r[3]}")

print("\n== D. 2019-07-31 / 2026-03-02 個案 ==")
for d in ("2019-07-31", "2026-03-02"):
    q = f"""
    WITH s AS (SELECT market,name,date,close,change,
                      lag(close) OVER (PARTITION BY market,name ORDER BY date) pc,
                      lag(date)  OVER (PARTITION BY market,name ORDER BY date) pd
               FROM market_index WHERE market='twse' AND close IS NOT NULL)
    SELECT name, pd, pc, close, change, close-pc-change diff
    FROM s WHERE date=DATE '{d}' AND abs(close-pc-change) > 0.02 ORDER BY abs(diff) DESC LIMIT 6"""
    print(f"  [{d}]")
    for r in con.sql(q).fetchall():
        print("   ", r)
    n = con.sql(f"SELECT count(*) FROM market_index WHERE market='twse' AND date=DATE '{d}'").fetchone()[0]
    print(f"    當日總列數={n}")
