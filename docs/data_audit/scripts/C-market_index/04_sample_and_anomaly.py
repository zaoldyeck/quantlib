"""C-market_index #4: 抽樣逐欄比對 + cache 端異常值掃描 + 整日內容指紋撞號。

① 抽樣:3 個日期 × 5 個指數,cache vs PG 逐欄 DataFrame.equals。
② 異常掃描:重複主鍵、未來日期、market 值域、close ≤ 0、change_pct 極端值、
   NULL close 但 change=0(A 維 BUG#3 的 cache 端重現)、名稱失真(報酬報酬)。
③ 整日內容指紋撞號:同一市場兩個不同日期,整天的 (name, close, change) 完全相同
   → TWSE 靜默 fallback 的幽靈日(A 維 BUG#1 的 cache 端獨立重現)。
④ 語意一致性:|close_t − close_(t−1) − change_t| > 0.02 的比例。

Run: PYTHONPATH=. uv run --project research python docs/data_audit/scripts/C-market_index/04_sample_and_anomaly.py
"""
import os
import duckdb
import pandas as pd
from research import paths

PG_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}")

con = duckdb.connect(":memory:")
con.sql("INSTALL postgres; LOAD postgres;")
con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
con.sql(f"ATTACH '{paths.CACHE_DB}' AS ca (READ_ONLY)")
con.sql('CREATE VIEW pg_mi AS SELECT market, date, name, close, change, '
        '"change(%)" AS change_pct FROM pg.public."index"')
con.sql("CREATE VIEW ca_mi AS SELECT * FROM ca.market_index")

print("== ① 抽樣逐欄比對(3 日期 × 5 指數)==")
SAMPLES = [
    ("twse", "2013-06-14", ["發行量加權股價指數", "水泥類指數", "半導體類指數",
                            "金融保險類指數", "電子類指數"]),
    ("tpex", "2019-11-07", ["櫃買指數", "光電業類指數", "生技醫療類指數",
                            "半導體業類指數", "電子零組件類指數"]),
    ("twse", "2026-07-17", ["發行量加權股價指數", "電子類指數", "臺灣50指數",
                            "未含金融保險股指數", "臺灣高股息指數"]),
]
for mkt, d, names in SAMPLES:
    inlist = ",".join(f"'{n}'" for n in names)
    w = f"WHERE market='{mkt}' AND date=DATE '{d}' AND name IN ({inlist})"
    p = con.sql(f"SELECT * FROM pg_mi {w} ORDER BY name").df()
    c = con.sql(f"SELECT * FROM ca_mi {w} ORDER BY name").df()
    print(f"  {mkt} {d}: pg {len(p)} 列 / cache {len(c)} 列  equals={p.equals(c)}")
    if len(p) and not p.equals(c):
        print(p.to_string()); print(c.to_string())
    elif len(p) < len(names):
        print(f"    (只找到 {len(p)}/{len(names)} 個指數名)")
        print("    cache 該日名稱樣本:",
              [r[0] for r in con.sql(f"SELECT DISTINCT name FROM ca_mi WHERE market='{mkt}' "
                                     f"AND date=DATE '{d}' LIMIT 12").fetchall()])

print("\n== ② cache 異常值掃描 ==")
checks = {
    "重複主鍵 (market,date,name)":
        "SELECT count(*) FROM (SELECT market,date,name FROM ca_mi GROUP BY 1,2,3 HAVING count(*)>1)",
    "date 在未來(> today)": "SELECT count(*) FROM ca_mi WHERE date > current_date",
    "market 非 twse/tpex": "SELECT count(*) FROM ca_mi WHERE market NOT IN ('twse','tpex')",
    "close < 0": "SELECT count(*) FROM ca_mi WHERE close < 0",
    "close = 0": "SELECT count(*) FROM ca_mi WHERE close = 0",
    "close IS NULL": "SELECT count(*) FROM ca_mi WHERE close IS NULL",
    "close NULL 但 change=0 且 pct=0(假平盤)":
        "SELECT count(*) FROM ca_mi WHERE close IS NULL AND change=0 AND change_pct=0",
    "|change_pct| > 20": "SELECT count(*) FROM ca_mi WHERE abs(change_pct) > 20",
    "|change_pct| > 10": "SELECT count(*) FROM ca_mi WHERE abs(change_pct) > 10",
    "close 非 NULL 但 change=0 且 pct=0":
        "SELECT count(*) FROM ca_mi WHERE close IS NOT NULL AND change=0 AND change_pct=0",
    "name 含『報酬報酬』": "SELECT count(*) FROM ca_mi WHERE name LIKE '%報酬報酬%'",
    "name = 'null' 字面": "SELECT count(*) FROM ca_mi WHERE name='null'",
    "name 為空字串": "SELECT count(*) FROM ca_mi WHERE trim(name)=''",
}
for label, q in checks.items():
    print(f"  {label:44} {con.sql(q).fetchone()[0]:>8,}")

print("\n  |change_pct| > 10 的代表樣本:")
for r in con.sql("SELECT market,date,name,close,change,change_pct FROM ca_mi "
                 "WHERE abs(change_pct)>10 ORDER BY abs(change_pct) DESC LIMIT 10").fetchall():
    print("   ", r)

print("\n  close 最大/最小(非 NULL):")
print("   ", con.sql("SELECT min(close), max(close) FROM ca_mi").fetchone())
print("  最小 close 樣本:")
for r in con.sql("SELECT market,date,name,close FROM ca_mi WHERE close IS NOT NULL "
                 "ORDER BY close LIMIT 5").fetchall():
    print("   ", r)

print("\n== ③ 整日內容指紋撞號(同市場、不同日期、整天資料完全相同)==")
q = """
WITH fp AS (
  SELECT market, date, count(*) n,
         sum(hash(name || '|' || coalesce(close,-1) || '|' || change)::HUGEINT) s
  FROM ca_mi GROUP BY 1,2)
SELECT a.market, a.date d1, b.date d2, a.n
FROM fp a JOIN fp b ON a.market=b.market AND a.s=b.s AND a.n=b.n AND a.date<b.date
ORDER BY 1,2"""
dups = con.sql(q).fetchall()
print(f"  撞號 {len(dups)} 對")
for r in dups:
    print("   ", r)

print("\n== ④ 語意一致性 |close_t − close_(t−1) − change_t| > 0.02 ==")
q = """
WITH s AS (SELECT market, name, date, close, change,
                  lag(close) OVER (PARTITION BY market,name ORDER BY date) pc
           FROM ca_mi WHERE close IS NOT NULL)
SELECT count(*) chk, count(*) FILTER (WHERE abs(close-pc-change) > 0.02) bad
FROM s WHERE pc IS NOT NULL"""
chk, bad = con.sql(q).fetchone()
print(f"  可檢查 {chk:,} 列,不一致 {bad:,} 列 ({bad/chk*100:.3f}%)")
q2 = """
WITH s AS (SELECT market, name, date, close, change,
                  lag(close) OVER (PARTITION BY market,name ORDER BY date) pc
           FROM ca_mi WHERE close IS NOT NULL)
SELECT market, date, count(*) n_bad, count(*) FILTER (WHERE TRUE) tot
FROM s WHERE pc IS NOT NULL AND abs(close-pc-change) > 0.02
GROUP BY 1,2 HAVING count(*) >= 20 ORDER BY 3 DESC"""
print("  單日不一致 ≥20 列的日期:")
for r in con.sql(q2).fetchall():
    print("   ", r)
