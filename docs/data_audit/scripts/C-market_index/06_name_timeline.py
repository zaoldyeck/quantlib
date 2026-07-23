"""C-market_index #6: 指數「名稱首見日」與半殘日內容檢查。

問題:幽靈日 2019-07-05 有 135 列(鄰日只有 ~94 列),其中 87 列與 2019-07-16 全等,
另外 48 列是哪來的?若那些指數名在 2019-07-05 之前從未出現、之後也要等到 7/31 才出現,
就是**前視汙染**(該日在真實世界還不存在的指數,在庫裡有值)。
同理檢查半殘日 twse 2026-02-26 / 2026-03-11 到底是「truncate」還是「換了一組指數」。

Run: PYTHONPATH=. uv run --project research python docs/data_audit/scripts/C-market_index/06_name_timeline.py
"""
import duckdb
from research import paths

con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

print("== A. 2019-07-05(幽靈日)的 135 列拆解 ==")
q = """
WITH d AS (SELECT name FROM market_index WHERE market='twse' AND date=DATE '2019-07-05'),
     src AS (SELECT name FROM market_index WHERE market='twse' AND date=DATE '2019-07-16')
SELECT (SELECT count(*) FROM d) n_0705,
       (SELECT count(*) FROM d SEMI JOIN src USING(name)) n_in_0716,
       (SELECT count(*) FROM d ANTI JOIN src USING(name)) n_not_in_0716"""
print("  ", con.sql(q).fetchone())

print("\n  不在 2019-07-16 的那批,它們的『全史首見日』分佈:")
q = """
WITH extra AS (
  SELECT name FROM market_index WHERE market='twse' AND date=DATE '2019-07-05'
  EXCEPT SELECT name FROM market_index WHERE market='twse' AND date=DATE '2019-07-16'),
first_seen AS (SELECT name, min(date) fd FROM market_index WHERE market='twse' GROUP BY 1),
second AS (SELECT name, min(date) sd FROM market_index
           WHERE market='twse' AND date > DATE '2019-07-05' GROUP BY 1)
SELECT e.name, f.fd first_seen, s.sd next_seen
FROM extra e JOIN first_seen f USING(name) LEFT JOIN second s USING(name)
ORDER BY f.fd, e.name"""
rows = con.sql(q).fetchall()
n_first_on_0705 = sum(1 for r in rows if str(r[1]) == "2019-07-05")
print(f"    共 {len(rows)} 檔,其中 {n_first_on_0705} 檔的全史首見日就是 2019-07-05(= 前視/幽靈)")
for r in rows[:45]:
    print("     ", r)

print("\n== B. 半殘日 twse 2026-02-26 / 2026-03-11 ==")
for d, prev in (("2026-02-26", "2026-02-25"), ("2026-03-11", "2026-03-10")):
    q = f"""
    WITH a AS (SELECT name FROM market_index WHERE market='twse' AND date=DATE '{prev}'),
         b AS (SELECT name FROM market_index WHERE market='twse' AND date=DATE '{d}')
    SELECT (SELECT count(*) FROM a) n_prev, (SELECT count(*) FROM b) n_day,
           (SELECT count(*) FROM b SEMI JOIN a USING(name)) n_overlap,
           (SELECT count(*) FROM b ANTI JOIN a USING(name)) n_new"""
    print(f"  [{d}] vs {prev}: {con.sql(q).fetchone()}")
    q2 = f"""
    SELECT name FROM market_index WHERE market='twse' AND date=DATE '{d}'
    EXCEPT SELECT name FROM market_index WHERE market='twse' AND date=DATE '{prev}'"""
    ex = [r[0] for r in con.sql(q2).fetchall()]
    print(f"    {d} 有但 {prev} 沒有的名稱({len(ex)}):{ex[:10]}")
    # 是否整段是別天的複製?
    q3 = f"""
    WITH b AS (SELECT name, close, change FROM market_index WHERE market='twse' AND date=DATE '{d}'),
         o AS (SELECT date, name, close, change FROM market_index WHERE market='twse'
               AND date BETWEEN DATE '{d}'::DATE - 400 AND DATE '{d}'::DATE + 60 AND date <> DATE '{d}')
    SELECT o.date, count(*) n_common,
           count(*) FILTER (WHERE o.close IS NOT DISTINCT FROM b.close
                            AND o.change IS NOT DISTINCT FROM b.change) n_same
    FROM o JOIN b USING(name) GROUP BY 1 HAVING n_same >= 20 ORDER BY n_same DESC LIMIT 5"""
    print(f"    與鄰近日期的值撞號(n_same≥20):{con.sql(q3).fetchall()}")

print("\n== C. 逐年『當年新出現的指數名』數(看是否有異常尖峰)==")
q = """
WITH f AS (SELECT market, name, min(date) fd FROM market_index GROUP BY 1,2)
SELECT market, year(fd) y, count(*) n_new FROM f GROUP BY 1,2 ORDER BY 1,2"""
for r in con.sql(q).fetchall():
    print(f"  {r[0]:5} {r[1]}  新增 {r[2]:>4} 檔")

print("\n== D. 只出現過 1 天的指數名(疑似幽靈/來源端亂碼)==")
q = """
SELECT market, name, min(date) d, count(*) n FROM market_index
GROUP BY 1,2 HAVING count(*) = 1 ORDER BY 1,3"""
rows = con.sql(q).fetchall()
print(f"  共 {len(rows)} 個")
for r in rows[:40]:
    print("   ", r)
