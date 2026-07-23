"""C-market_index #1: DuckDB cache `market_index` vs PostgreSQL `public."index"` 全史一致性。

不抽樣:對每個 (market, date) 分組計算三重指紋(列數 + sum(hash) + bit_xor(hash)),
兩邊必須逐格相等。PG 側套用與 research/cache_tables.py:46 完全相同的投影/改名。

Run: PYTHONPATH=. uv run --project research python docs/data_audit/scripts/C-market_index/01_parity.py
需要:PostgreSQL 在跑 + var/cache/cache.duckdb 存在(不依賴 cache 新鮮度)。
"""
import os
import duckdb
from research import paths

PG_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}",
)

# 與 research/cache_tables.py:46 的投影一字不差
PG_PROJ = ('SELECT market, date, name, close, change, "change(%)" AS change_pct '
           'FROM pg.public."index"')

FP_COLS = "market, date, name, close, change, change_pct"

con = duckdb.connect(":memory:")
con.sql("INSTALL postgres; LOAD postgres;")
con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
con.sql(f"ATTACH '{paths.CACHE_DB}' AS ca (READ_ONLY)")

con.sql(f"CREATE VIEW pg_mi AS {PG_PROJ}")
con.sql("CREATE VIEW ca_mi AS SELECT * FROM ca.market_index")

print("== 1. 總列數 ==")
pg_n = con.sql("SELECT count(*) FROM pg_mi").fetchone()[0]
ca_n = con.sql("SELECT count(*) FROM ca_mi").fetchone()[0]
print(f"  pg_rows={pg_n:,}  cache_rows={ca_n:,}  diff={pg_n - ca_n}")

print("\n== 2. 逐年 × 市場列數 ==")
q = """
WITH p AS (SELECT market, year(date) y, count(*) n FROM pg_mi GROUP BY 1,2),
     c AS (SELECT market, year(date) y, count(*) n FROM ca_mi GROUP BY 1,2)
SELECT coalesce(p.market,c.market) market, coalesce(p.y,c.y) y,
       coalesce(p.n,0) pg_n, coalesce(c.n,0) ca_n
FROM p FULL OUTER JOIN c ON p.market=c.market AND p.y=c.y
ORDER BY 1,2
"""
rows = con.sql(q).fetchall()
bad = [r for r in rows if r[2] != r[3]]
print(f"  比對 {len(rows)} 個年×市場格,不符 {len(bad)} 格")
for r in bad:
    print("   ", r)
if not bad:
    for r in rows:
        print(f"    {r[0]:5} {r[1]}  {r[2]:>8,}")

print("\n== 3. 全史 (market,date) 三重指紋 ==")
fp = f"""
WITH p AS (
  SELECT market, date, count(*) n,
         sum(hash({FP_COLS})::HUGEINT) s,
         bit_xor(hash({FP_COLS})) x
  FROM pg_mi GROUP BY 1,2),
c AS (
  SELECT market, date, count(*) n,
         sum(hash({FP_COLS})::HUGEINT) s,
         bit_xor(hash({FP_COLS})) x
  FROM ca_mi GROUP BY 1,2)
SELECT
  count(*) FILTER (WHERE p.market IS NOT NULL AND c.market IS NOT NULL) n_both,
  count(*) FILTER (WHERE c.market IS NULL) only_pg,
  count(*) FILTER (WHERE p.market IS NULL) only_cache,
  count(*) FILTER (WHERE p.market IS NOT NULL AND c.market IS NOT NULL
                     AND (p.n<>c.n OR p.s IS DISTINCT FROM c.s OR p.x IS DISTINCT FROM c.x)) mismatch
FROM p FULL OUTER JOIN c ON p.market=c.market AND p.date=c.date
"""
both, only_pg, only_cache, mismatch = con.sql(fp).fetchone()
print(f"  days_both={both}  only_pg={only_pg}  only_cache={only_cache}  mismatch={mismatch}")

if mismatch or only_pg or only_cache:
    detail = fp.replace(
        "SELECT\n  count(*)", "SELECT p.market, p.date, c.market, c.date, p.n, c.n, p.s, c.s\n  -- count(*)"
    )
    print("  >>> 有差異,列出前 20 筆:")
    d = con.sql(f"""
    WITH p AS (SELECT market,date,count(*) n, sum(hash({FP_COLS})::HUGEINT) s FROM pg_mi GROUP BY 1,2),
         c AS (SELECT market,date,count(*) n, sum(hash({FP_COLS})::HUGEINT) s FROM ca_mi GROUP BY 1,2)
    SELECT coalesce(p.market,c.market) m, coalesce(p.date,c.date) d, p.n pg_n, c.n ca_n,
           (p.s = c.s) same_fp
    FROM p FULL OUTER JOIN c ON p.market=c.market AND p.date=c.date
    WHERE p.n IS DISTINCT FROM c.n OR p.s IS DISTINCT FROM c.s
    ORDER BY 2 LIMIT 20""").fetchall()
    for r in d:
        print("   ", r)

print("\n== 4. NULL 分佈一致性(close 是 Option) ==")
for side, v in (("pg", "pg_mi"), ("cache", "ca_mi")):
    r = con.sql(f"SELECT count(*) FILTER (WHERE close IS NULL), "
                f"count(*) FILTER (WHERE change IS NULL), "
                f"count(*) FILTER (WHERE change_pct IS NULL) FROM {v}").fetchone()
    print(f"  {side:6} close_null={r[0]}  change_null={r[1]}  pct_null={r[2]}")

print("\n== 5. 投影 SQL 字面比對(cache_tables.py vs db.py) ==")
import re
root = paths.REPO_ROOT if hasattr(paths, "REPO_ROOT") else "."
ct = open(f"{root}/research/cache_tables.py").read()
dbp = open(f"{root}/research/db.py").read()
norm = lambda s: re.sub(r"\s+", " ", s).strip()
m1 = re.search(r"\(\"market_index\",\s*(.*?)\),\n", ct, re.S)
ct_sql = norm(m1.group(1).strip().strip("'"))
m2 = re.search(r"CREATE OR REPLACE VIEW market_index AS '\s*\n(.*?)FROM pg\.public\.\"index\"'", dbp, re.S)
db_sql = norm(m2.group(1).replace("'", "")) + ' FROM pg.public."index"'
print(f"  cache_tables: {ct_sql}")
print(f"  db.py       : {db_sql}")
print(f"  相同: {norm(ct_sql).replace(' ','') == norm(db_sql).replace(' ','')}")
