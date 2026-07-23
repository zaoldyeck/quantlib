"""Is the stored dividend_yield point-in-time?

implied_DPS = dividend_yield/100 * close_of_that_day
Compare with the most recent actual cash dividend from ex_right_dividend
(<= observation date, within 400 days). If dividend_yield were computed with a
FUTURE dividend, implied_DPS would systematically exceed the PIT dividend.
"""
import os

import duckdb

con = duckdb.connect()
con.sql("SET threads=8")
con.sql("INSTALL postgres")
con.sql("LOAD postgres")
con.sql(f"ATTACH 'host=localhost port=5432 dbname=quantlib user={os.environ.get('USER','zaoldyeck')}' AS pg (TYPE POSTGRES, READ_ONLY)")

con.sql("""
CREATE TABLE obs AS
SELECT s.market, s.date, s.company_code, s.dividend_yield dy, q.closing_price AS px,
       s.dividend_yield/100.0*q.closing_price AS implied_dps
FROM pg.public.stock_per_pbr_dividend_yield s
JOIN pg.public.daily_quote q
  ON q.market=s.market AND q.date=s.date AND q.company_code=s.company_code
WHERE s.dividend_yield > 0 AND q.closing_price > 0
""")
print("obs rows:", con.sql("SELECT count(*) FROM obs").fetchone())

con.sql("""
CREATE TABLE xd AS
SELECT market, date, company_code, cash_dividend
FROM pg.public.ex_right_dividend WHERE cash_dividend > 0
""")

con.sql("""
CREATE TABLE j AS
SELECT o.*, x.cash_dividend pit_dps, x.date xd_date
FROM obs o
ASOF LEFT JOIN xd x
  ON o.market = x.market AND o.company_code = x.company_code AND o.date >= x.date
""")

print("\nmarket, year, n, matched_pit, pct_implied_within_5pct_of_pit, median_ratio")
q = """
SELECT market, year(date) yr, count(*) n,
  count(pit_dps) n_pit,
  round(100.0*sum(CASE WHEN pit_dps IS NOT NULL AND date - xd_date <= 400
                       AND abs(implied_dps - pit_dps) <= 0.05*pit_dps THEN 1 ELSE 0 END)
        / nullif(sum(CASE WHEN pit_dps IS NOT NULL AND date - xd_date <= 400 THEN 1 ELSE 0 END),0), 1) pct_match,
  round(median(CASE WHEN pit_dps IS NOT NULL AND date - xd_date <= 400 THEN implied_dps/pit_dps END), 3) med_ratio
FROM j GROUP BY 1,2 ORDER BY 1,2
"""
for r in con.sql(q).fetchall():
    print(r)
