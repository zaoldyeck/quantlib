"""Compare independent raw parse vs PostgreSQL stock_per_pbr_dividend_yield."""
import os

import duckdb

SCR = "/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad"
con = duckdb.connect()
con.sql("SET threads=8")
con.sql("SET memory_limit='8GB'")
con.sql("INSTALL postgres")
con.sql("LOAD postgres")
con.sql(f"ATTACH 'host=localhost port=5432 dbname=quantlib user={os.environ.get('USER','zaoldyeck')}' AS pg (TYPE POSTGRES, READ_ONLY)")

con.sql(f"""
CREATE TABLE raw AS
SELECT market, date, company_code, company_name, pe, pb, dy, ncols
FROM read_parquet('{SCR}/Aperpbr_raw.parquet') WHERE kind='row'
""")
con.sql("""
CREATE TABLE db AS
SELECT market, date, company_code, company_name,
       price_to_earning_ratio AS pe, price_book_ratio AS pb, dividend_yield AS dy
FROM pg.public.stock_per_pbr_dividend_yield
""")

print("raw rows:", con.sql("SELECT count(*) FROM raw").fetchone())
print("db  rows:", con.sql("SELECT count(*) FROM db").fetchone())

con.sql("""
CREATE TABLE raw_d AS
SELECT * EXCLUDE rn FROM (
  SELECT *, row_number() OVER (PARTITION BY market, date, company_code ORDER BY company_name) rn
  FROM raw
) WHERE rn=1
""")
print("raw dedup rows:", con.sql("SELECT count(*) FROM raw_d").fetchone())

print("\n=== dates in raw but missing in db ===")
print(con.sql("""
SELECT r.market, count(*) AS n_dates, min(r.date), max(r.date)
FROM (SELECT DISTINCT market, date FROM raw_d) r
LEFT JOIN (SELECT DISTINCT market, date FROM db) d USING (market, date)
WHERE d.date IS NULL GROUP BY 1
""").fetchall())

print("\n=== dates in db but not in raw ===")
print(con.sql("""
SELECT d.market, count(*), min(d.date), max(d.date)
FROM (SELECT DISTINCT market, date FROM db) d
LEFT JOIN (SELECT DISTINCT market, date FROM raw_d) r USING (market, date)
WHERE r.date IS NULL GROUP BY 1
""").fetchall())

con.sql("""
CREATE TABLE common_dates AS
SELECT market, date FROM (SELECT DISTINCT market, date FROM raw_d)
INTERSECT SELECT market, date FROM (SELECT DISTINCT market, date FROM db)
""")

print("\n=== rows in raw missing from db (common dates) ===")
print(con.sql("""
SELECT r.market, count(*) FROM raw_d r JOIN common_dates c USING (market, date)
LEFT JOIN db d USING (market, date, company_code)
WHERE d.company_code IS NULL GROUP BY 1
""").fetchall())

print("\n=== rows in db missing from raw (common dates) ===")
print(con.sql("""
SELECT d.market, count(*) FROM db d JOIN common_dates c USING (market, date)
LEFT JOIN raw_d r USING (market, date, company_code)
WHERE r.company_code IS NULL GROUP BY 1
""").fetchall())

print("\n=== value mismatches by market/year ===")
q = """
WITH j AS (
  SELECT r.market, r.date, r.ncols,
         r.pe rpe, d.pe dpe, r.pb rpb, d.pb dpb, r.dy rdy, d.dy ddy,
         r.company_name rnm, d.company_name dnm
  FROM raw_d r JOIN common_dates c USING (market, date)
  JOIN db d USING (market, date, company_code)
)
SELECT market, year(date) yr, count(*) n,
  sum(CASE WHEN rpe IS DISTINCT FROM dpe THEN 1 ELSE 0 END) pe_bad,
  sum(CASE WHEN rpb IS DISTINCT FROM dpb THEN 1 ELSE 0 END) pb_bad,
  sum(CASE WHEN rdy IS DISTINCT FROM ddy THEN 1 ELSE 0 END) dy_bad,
  sum(CASE WHEN rnm IS DISTINCT FROM dnm THEN 1 ELSE 0 END) nm_bad
FROM j GROUP BY 1,2 ORDER BY 1,2
"""
for row in con.sql(q).fetchall():
    print(row)

con.sql("""
CREATE TABLE bad AS
SELECT r.market, r.date, r.company_code, r.ncols, r.company_name rnm, d.company_name dnm,
       r.pe rpe, d.pe dpe, r.pb rpb, d.pb dpb, r.dy rdy, d.dy ddy
FROM raw_d r JOIN common_dates c USING (market, date)
JOIN db d USING (market, date, company_code)
WHERE (r.pe IS DISTINCT FROM d.pe) OR (r.pb IS DISTINCT FROM d.pb)
   OR (r.dy IS DISTINCT FROM d.dy) OR (r.company_name IS DISTINCT FROM d.company_name)
""")
print("\nbad count:", con.sql("SELECT count(*) FROM bad").fetchone())
for row in con.sql("SELECT * FROM bad LIMIT 25").fetchall():
    print(row)
con.sql(f"COPY bad TO '{SCR}/Aperpbr_bad.parquet' (FORMAT PARQUET)")
