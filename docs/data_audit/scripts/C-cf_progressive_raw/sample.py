"""C-cf_progressive_raw: random-sample column-by-column cache vs PG comparison.

Run: uv run --project research python docs/data_audit/scripts/C-cf_progressive_raw/sample.py
Deterministic (seed 20260722): 3 random quarters x 5 random companies each,
every column compared with pandas.DataFrame.equals.
"""
import duckdb, os, sys, pathlib, random
import pandas as pd
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[4]))
from research import paths

PG = os.environ.get("QL_PG_DSN", f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER','zaoldyeck')}")
con = duckdb.connect()
con.sql("INSTALL postgres; LOAD postgres;")
con.sql(f"ATTACH '{paths.CACHE_DB}' AS c (READ_ONLY)")
con.sql(f"ATTACH '{PG}' AS pg (TYPE postgres, READ_ONLY)")

rng = random.Random(20260722)
qs = [tuple(r) for r in con.sql("SELECT DISTINCT year, quarter FROM c.cf_progressive_raw ORDER BY 1,2").df().values]
picked = rng.sample(qs, 3)
total = 0
ok = True
for (y, q) in picked:
    codes = con.sql(f"SELECT DISTINCT company_code FROM c.cf_progressive_raw WHERE year={y} AND quarter={q} ORDER BY 1").df()["company_code"].tolist()
    sel = rng.sample(codes, 5)
    inlist = ",".join(repr(c) for c in sel)
    cols = "market, year, quarter, company_code, title, value"
    a = con.sql(f"SELECT {cols} FROM c.cf_progressive_raw WHERE year={y} AND quarter={q} AND company_code IN ({inlist}) ORDER BY company_code, title").df()
    b = con.sql(f"SELECT {cols} FROM pg.public.cash_flows_progressive WHERE market='tw' AND year={y} AND quarter={q} AND company_code IN ({inlist}) ORDER BY company_code, title").df()
    same = a.equals(b)
    ok &= same
    total += len(a)
    print(f"{y}Q{q} codes={sel} rows={len(a)} pg_rows={len(b)} equals={same}")
print(f"\nTOTAL rows compared: {total}  ALL-EQUAL: {ok}")
