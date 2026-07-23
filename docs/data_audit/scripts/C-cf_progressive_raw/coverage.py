"""C-cf_progressive_raw: coverage / gaps / title-vocabulary / anomaly scan.

Run: uv run --project research python docs/data_audit/scripts/C-cf_progressive_raw/coverage.py
Reads var/cache/cache.duckdb read-only.
"""
import duckdb, sys, pathlib
import pandas as pd
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[4]))
from research import paths
pd.set_option("display.width", 250); pd.set_option("display.max_rows", 400)

con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
T = "cf_progressive_raw"

print("== per-quarter coverage ==")
print(con.sql(f"SELECT year, quarter, COUNT(*) n, COUNT(DISTINCT company_code) nc, "
              f"COUNT(DISTINCT title) nt FROM {T} GROUP BY 1,2 ORDER BY 1,2").df().to_string())

print("\n== CFO title coverage (raw_quarterly.CF_TITLES) ==")
CFO = ["營業活動之淨現金流入（流出）", "營業活動之淨現金流入(流出)"]
lst = ",".join(repr(t) for t in CFO)
print(con.sql(f"""
SELECT year, quarter, COUNT(DISTINCT company_code) nc,
       COUNT(DISTINCT company_code) FILTER (WHERE title IN ({lst})) nc_cfo,
       ROUND(100.0*COUNT(DISTINCT company_code) FILTER (WHERE title IN ({lst}))
             / COUNT(DISTINCT company_code), 1) pct
FROM {T} GROUP BY 1,2 ORDER BY 1,2""").df().to_string())

print("\n== top titles containing 營業活動 (any year) ==")
print(con.sql(f"SELECT title, COUNT(*) n, COUNT(DISTINCT company_code) nc, "
              f"MIN(year*10+quarter) lo, MAX(year*10+quarter) hi FROM {T} "
              f"WHERE title LIKE '%營業活動%' GROUP BY 1 ORDER BY 2 DESC LIMIT 25").df().to_string())

print("\n== top 40 titles overall ==")
print(con.sql(f"SELECT title, COUNT(*) n, MIN(year*10+quarter) lo, MAX(year*10+quarter) hi "
              f"FROM {T} GROUP BY 1 ORDER BY 2 DESC LIMIT 40").df().to_string())
