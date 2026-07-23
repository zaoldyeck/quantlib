"""C-cf_progressive_raw: accounting-identity + value-domain anomaly scan.

Run: uv run --project . python docs/data_audit/scripts/C-cf_progressive_raw/anomaly.py
Reads var/cache/cache.duckdb read-only.
"""
import duckdb, sys, pathlib
import pandas as pd
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[4]))
from research import paths
pd.set_option("display.width", 260); pd.set_option("display.max_rows", 200)
con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
T = "cf_progressive_raw"

con.sql(f"""CREATE OR REPLACE TEMP VIEW w AS
SELECT year, quarter, company_code,
  MAX(value) FILTER (WHERE title IN ('營業活動之淨現金流入（流出）','營業活動之淨現金流入(流出)')) cfo,
  MAX(value) FILTER (WHERE title IN ('投資活動之淨現金流入（流出）','投資活動之淨現金流入(流出)')) cfi,
  MAX(value) FILTER (WHERE title IN ('籌資活動之淨現金流入（流出）','籌資活動之淨現金流入(流出)')) cff,
  MAX(value) FILTER (WHERE title IN ('匯率變動對現金及約當現金之影響','匯率變動對現金及約當現金之影響數')) fx,
  MAX(value) FILTER (WHERE title IN ('本期現金及約當現金增加（減少）數','本期現金及約當現金增加(減少)數')) dcash,
  MAX(value) FILTER (WHERE title IN ('期初現金及約當現金餘額')) c0,
  MAX(value) FILTER (WHERE title IN ('期末現金及約當現金餘額')) c1
FROM {T} GROUP BY 1,2,3""")

print("== identity A: c0 + dcash = c1 (tol 1) ==")
print(con.sql("""SELECT year, COUNT(*) n,
  COUNT(*) FILTER (WHERE c0 IS NULL OR c1 IS NULL OR dcash IS NULL) missing,
  COUNT(*) FILTER (WHERE abs(c0+dcash-c1) > 1) bad
FROM w GROUP BY 1 ORDER BY 1""").df().to_string())

print("\n== identity B: cfo+cfi+cff+fx = dcash (tol 1) ==")
print(con.sql("""SELECT year, COUNT(*) n,
  COUNT(*) FILTER (WHERE cfo IS NULL OR cfi IS NULL OR cff IS NULL OR dcash IS NULL) missing,
  COUNT(*) FILTER (WHERE abs(cfo+cfi+cff+COALESCE(fx,0)-dcash) > 1) bad
FROM w GROUP BY 1 ORDER BY 1""").df().to_string())

print("\n== identity B worst offenders ==")
print(con.sql("""SELECT year, quarter, company_code, cfo, cfi, cff, fx, dcash,
  (cfo+cfi+cff+COALESCE(fx,0)-dcash) diff
FROM w WHERE cfo IS NOT NULL AND cfi IS NOT NULL AND cff IS NOT NULL AND dcash IS NOT NULL
  AND abs(cfo+cfi+cff+COALESCE(fx,0)-dcash) > 1
ORDER BY abs(cfo+cfi+cff+COALESCE(fx,0)-dcash) DESC LIMIT 15""").df().to_string())

print("\n== 期末現金 negative (impossible) ==")
print(con.sql("SELECT COUNT(*) n FROM w WHERE c1 < 0").df().to_string())
print(con.sql("SELECT year,quarter,company_code,c0,c1 FROM w WHERE c1 < 0 ORDER BY c1 LIMIT 10").df().to_string())

print("\n== unit-scale check: median |期末現金| by year (thousands NTD expected) ==")
print(con.sql("SELECT year, COUNT(*) n, MEDIAN(c1) med_c1, MEDIAN(abs(cfo)) med_cfo FROM w GROUP BY 1 ORDER BY 1").df().to_string())

print("\n== company_code format ==")
print(con.sql(f"""SELECT CASE WHEN regexp_matches(company_code,'^[0-9]{{4}}$') THEN '4-digit'
  WHEN regexp_matches(company_code,'^[0-9]{{6}}$') THEN '6-digit'
  WHEN regexp_matches(company_code,'^[0-9]{{5}}$') THEN '5-digit'
  ELSE 'other' END fmt, COUNT(DISTINCT company_code) ncode, COUNT(*) n
FROM {T} GROUP BY 1 ORDER BY 3 DESC""").df().to_string())
print(con.sql(f"SELECT DISTINCT company_code FROM {T} WHERE NOT regexp_matches(company_code,'^[0-9]{{4}}$') ORDER BY 1 LIMIT 20").df().to_string())

print("\n== year/quarter domain, future periods (today 2026-07-22, latest closed quarter = 2026Q2) ==")
print(con.sql(f"SELECT MIN(year) ylo, MAX(year) yhi, MIN(quarter) qlo, MAX(quarter) qhi FROM {T}").df().to_string())

print("\n== extreme values ==")
print(con.sql(f"SELECT MIN(value) mn, MAX(value) mx, COUNT(*) FILTER (WHERE abs(value)>1e12) huge FROM {T}").df().to_string())
print(con.sql(f"SELECT year,quarter,company_code,title,value FROM {T} ORDER BY abs(value) DESC LIMIT 8").df().to_string())
