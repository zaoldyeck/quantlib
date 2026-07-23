"""C-cf_progressive_raw: DuckDB cache vs PostgreSQL parity + integrity.

Run: uv run --project research python docs/data_audit/scripts/C-cf_progressive_raw/parity.py
Needs: var/cache/cache.duckdb current (research/cache_tables.py); PG `quantlib` up.
Read-only on both sides. Compares every (year,quarter) bucket with count /
distinct-code / distinct-title / SUM / MIN / MAX / row-content BIT_XOR(HASH).
"""
import duckdb, os, sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[4]))
from research import paths

PG = os.environ.get("QL_PG_DSN", f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER','zaoldyeck')}")
con = duckdb.connect()
con.sql("INSTALL postgres; LOAD postgres;")
con.sql(f"ATTACH '{paths.CACHE_DB}' AS c (READ_ONLY)")
con.sql(f"ATTACH '{PG}' AS pg (TYPE postgres, READ_ONLY)")

CACHE = "c.cf_progressive_raw"
# exactly the cache_tables.py projection
PGV = ("(SELECT market, year, quarter, company_code, title, value "
       "FROM pg.public.cash_flows_progressive WHERE market='tw')")

print("== totals ==")
print(con.sql(f"SELECT (SELECT COUNT(*) FROM {CACHE}) cache_n, (SELECT COUNT(*) FROM {PGV}) pg_filtered, "
              "(SELECT COUNT(*) FROM pg.public.cash_flows_progressive) pg_all").df().to_string())

agg = ("SELECT year, quarter, COUNT(*) n, COUNT(DISTINCT company_code) nc, COUNT(DISTINCT title) nt, "
       "SUM(value) sv, MIN(value) mn, MAX(value) mx, "
       "BIT_XOR(HASH(market||'|'||company_code||'|'||title||'|'||CAST(value AS VARCHAR))) hx "
       "FROM {t} GROUP BY 1,2")
d = con.sql(f"""
WITH a AS ({agg.format(t=CACHE)}), b AS ({agg.format(t=PGV)})
SELECT COALESCE(a.year,b.year) y, COALESCE(a.quarter,b.quarter) q,
       a.n an, b.n bn, a.nc anc, b.nc bnc, a.nt ant, b.nt bnt,
       a.sv asv, b.sv bsv, a.mn amn, b.mn bmn, a.mx amx, b.mx bmx, a.hx ahx, b.hx bhx
FROM a FULL OUTER JOIN b ON a.year=b.year AND a.quarter=b.quarter
WHERE a.n IS DISTINCT FROM b.n OR a.nc IS DISTINCT FROM b.nc OR a.nt IS DISTINCT FROM b.nt
   OR a.sv IS DISTINCT FROM b.sv OR a.mn IS DISTINCT FROM b.mn OR a.mx IS DISTINCT FROM b.mx
   OR a.hx IS DISTINCT FROM b.hx
ORDER BY 1,2""").df()
nq = con.sql(f"SELECT COUNT(*) FROM (SELECT DISTINCT year, quarter FROM {CACHE})").fetchone()[0]
print(f"\n== per-quarter checksum: {nq} quarters compared, {len(d)} mismatching ==")
if len(d):
    print(d.to_string())

print("\n== cache NULL / NaN / Inf ==")
print(con.sql(f"SELECT COUNT(*) FILTER (WHERE market IS NULL) m, COUNT(*) FILTER (WHERE year IS NULL) y, "
              "COUNT(*) FILTER (WHERE quarter IS NULL) q, COUNT(*) FILTER (WHERE company_code IS NULL) c, "
              "COUNT(*) FILTER (WHERE title IS NULL) t, COUNT(*) FILTER (WHERE value IS NULL) v, "
              "COUNT(*) FILTER (WHERE isnan(value)) nan, COUNT(*) FILTER (WHERE isinf(value)) inf "
              f"FROM {CACHE}").df().to_string())

print("\n== duplicate keys ==")
print("  (year,quarter,code,title):", con.sql(
    f"SELECT COUNT(*) FROM (SELECT year,quarter,company_code,title FROM {CACHE} "
    "GROUP BY 1,2,3,4 HAVING COUNT(*)>1)").fetchone()[0])

print("\n== schema ==")
print(con.sql(f"DESCRIBE {CACHE}").df().to_string())
print(con.sql("SELECT column_name, data_type FROM pg.information_schema.columns "
              "WHERE table_name='cash_flows_progressive' ORDER BY ordinal_position").df().to_string())
