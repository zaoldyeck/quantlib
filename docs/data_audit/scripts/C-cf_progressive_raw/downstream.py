"""C-cf_progressive_raw: quantify lag-diff contamination caused by missing quarters.

cfo_q in src/quantlib/strat_lab/raw_quarterly.py is YTD_t - YTD_{t-1} with
shift(1).over(company_code, year) and NO continuity guard. A missing intermediate
quarter therefore silently produces a MULTI-quarter value labelled as one quarter,
and cfo_ttm (rolling_sum 4) then spans >4 calendar quarters.

Run: uv run --project . python docs/data_audit/scripts/C-cf_progressive_raw/downstream.py
"""
import duckdb, sys, pathlib
import pandas as pd
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[4]))
from research import paths
pd.set_option("display.width", 250); pd.set_option("display.max_rows", 120)
con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

con.sql("""CREATE OR REPLACE TEMP VIEW cfo AS
SELECT year, quarter, company_code,
       MAX(value) FILTER (WHERE title IN ('營業活動之淨現金流入（流出）','營業活動之淨現金流入(流出)')) v
FROM cf_progressive_raw GROUP BY 1,2,3""")

print("== rows whose cfo_q silently spans >1 quarter (prev quarter of same year absent) ==")
print(con.sql("""
WITH s AS (SELECT year, quarter, company_code, v,
                  LAG(quarter) OVER (PARTITION BY company_code, year ORDER BY quarter) pq
           FROM cfo),
     td AS (SELECT company_code, YEAR(date) y, COUNT(*) nd, SUM(trade_value) tv FROM daily_quote GROUP BY 1,2)
SELECT s.year, COUNT(*) n_bad, COUNT(*) FILTER (WHERE td.nd>=150) n_bad_tradable,
       ROUND(SUM(td.tv) FILTER (WHERE td.nd>=150)/1e8,1) tradable_turnover_e8
FROM s LEFT JOIN td ON td.company_code=s.company_code AND td.y=s.year
WHERE s.quarter > 1 AND s.pq IS NOT NULL AND s.pq <> s.quarter - 1
GROUP BY 1 ORDER BY 1""").df().to_string())

print("\n== 2025 detail: which tradable companies, which quarter ==")
print(con.sql("""
WITH s AS (SELECT year, quarter, company_code, v,
                  LAG(quarter) OVER (PARTITION BY company_code, year ORDER BY quarter) pq
           FROM cfo),
     td AS (SELECT company_code, YEAR(date) y, COUNT(*) nd, SUM(trade_value) tv FROM daily_quote GROUP BY 1,2)
SELECT s.year, s.quarter, s.pq AS prev_quarter_present, s.company_code, td.nd, ROUND(td.tv/1e8,1) tv_e8
FROM s JOIN td ON td.company_code=s.company_code AND td.y=s.year
WHERE s.quarter > 1 AND s.pq IS NOT NULL AND s.pq <> s.quarter - 1 AND s.year=2025 AND td.nd>=150
ORDER BY td.tv DESC LIMIT 30""").df().to_string())
