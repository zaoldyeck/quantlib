"""C-cf_progressive_raw: per-company coverage holes + tradability cross-check.

Run: uv run --project research python docs/data_audit/scripts/C-cf_progressive_raw/gaps.py
Reads var/cache/cache.duckdb read-only (needs daily_quote for tradability).
"""
import duckdb, sys, pathlib
import pandas as pd
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[4]))
from research import paths
pd.set_option("display.width", 260); pd.set_option("display.max_rows", 300)
con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
T = "cf_progressive_raw"

print("== quarter sequence contiguity 2009Q4..2026Q1 ==")
print(con.sql(f"""
WITH have AS (SELECT DISTINCT year*10+quarter yq FROM {T}),
     want AS (SELECT y.y*10+q.q yq FROM (SELECT UNNEST(range(2009,2027)) y) y,
                                        (SELECT UNNEST(range(1,5)) q) q
              WHERE y.y*10+q.q BETWEEN 20094 AND 20261)
SELECT COUNT(*) missing_quarters, LIST(yq) FROM want WHERE yq NOT IN (SELECT yq FROM have)""").df().to_string())

print("\n== 2026Q1 vs neighbours: how many *tradable* companies are missing? ==")
print(con.sql(f"""
WITH q1 AS (SELECT DISTINCT company_code FROM {T} WHERE year=2026 AND quarter=1),
     prev AS (SELECT DISTINCT company_code FROM {T} WHERE year=2025 AND quarter=4),
     traded AS (SELECT company_code, COUNT(*) nd, SUM(trade_value) tv FROM daily_quote
                WHERE date >= DATE '2026-01-01' AND date < DATE '2026-07-01' GROUP BY 1)
SELECT COUNT(*) missing_vs_2025Q4,
       COUNT(*) FILTER (WHERE t.nd >= 80) missing_and_traded,
       ROUND(SUM(t.tv) FILTER (WHERE t.nd >= 80)/1e8, 1) traded_value_e8
FROM prev LEFT JOIN traded t USING (company_code)
WHERE prev.company_code NOT IN (SELECT company_code FROM q1)""").df().to_string())

print("\n== 2026Q1 missing sample (largest by 2026H1 turnover) ==")
print(con.sql(f"""
WITH q1 AS (SELECT DISTINCT company_code FROM {T} WHERE year=2026 AND quarter=1),
     prev AS (SELECT DISTINCT company_code FROM {T} WHERE year=2025 AND quarter=4),
     traded AS (SELECT company_code, COUNT(*) nd, SUM(trade_value) tv FROM daily_quote
                WHERE date >= DATE '2026-01-01' AND date < DATE '2026-07-01' GROUP BY 1)
SELECT prev.company_code, t.nd, ROUND(t.tv/1e8,1) tv_e8 FROM prev LEFT JOIN traded t USING (company_code)
WHERE prev.company_code NOT IN (SELECT company_code FROM q1) AND t.nd >= 80
ORDER BY t.tv DESC LIMIT 25""").df().to_string())

print("\n== internal holes: company has quarter Y-1 and Y+1 but not Y (2013+, tradable only) ==")
print(con.sql(f"""
WITH seq AS (SELECT company_code, year*4+quarter-1 t FROM (SELECT DISTINCT company_code, year, quarter FROM {T})),
     bounds AS (SELECT company_code, MIN(t) lo, MAX(t) hi FROM seq GROUP BY 1),
     want AS (SELECT b.company_code, UNNEST(range(b.lo, b.hi+1)) t FROM bounds b),
     hole AS (SELECT w.company_code, w.t FROM want w LEFT JOIN seq s USING (company_code, t) WHERE s.t IS NULL)
SELECT t/4 AS year, (t%4)+1 AS quarter, COUNT(*) n_companies
FROM hole WHERE t/4 >= 2013 GROUP BY 1,2 ORDER BY 1,2""").df().to_string())

print("\n== of those holes, how many companies actually traded that year (>=150 days)? ==")
print(con.sql(f"""
WITH seq AS (SELECT company_code, year*4+quarter-1 t FROM (SELECT DISTINCT company_code, year, quarter FROM {T})),
     bounds AS (SELECT company_code, MIN(t) lo, MAX(t) hi FROM seq GROUP BY 1),
     want AS (SELECT b.company_code, UNNEST(range(b.lo, b.hi+1)) t FROM bounds b),
     hole AS (SELECT w.company_code, w.t, w.t/4 y FROM want w LEFT JOIN seq s USING (company_code, t) WHERE s.t IS NULL),
     td AS (SELECT company_code, YEAR(date) y, COUNT(*) nd FROM daily_quote GROUP BY 1,2)
SELECT h.y AS year, (h.t%4)+1 AS quarter, COUNT(*) n_holes,
       COUNT(*) FILTER (WHERE td.nd >= 150) n_tradable
FROM hole h LEFT JOIN td ON td.company_code=h.company_code AND td.y=h.y
WHERE h.y >= 2013 GROUP BY 1,2 ORDER BY 1,2""").df().to_string())

print("\n== the semi-annual-only cohort: companies present only in Q2/Q4 of a year — traded? ==")
print(con.sql(f"""
WITH per AS (SELECT company_code, year, LIST(DISTINCT quarter ORDER BY quarter) qs FROM {T} GROUP BY 1,2),
     td AS (SELECT company_code, YEAR(date) y, COUNT(*) nd FROM daily_quote GROUP BY 1,2)
SELECT per.year, COUNT(*) n_semiannual, COUNT(*) FILTER (WHERE td.nd >= 150) n_tradable
FROM per LEFT JOIN td ON td.company_code=per.company_code AND td.y=per.year
WHERE per.qs = [2,4] GROUP BY 1 ORDER BY 1""").df().to_string())
