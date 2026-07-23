"""Find which company_codes are in DB but not in independent raw parse (and vice
versa) for the two mismatching HTML periods, then check whether each DB-only
code physically appears anywhere in the raw file bytes."""
import subprocess
from rowcount_parity import parse_html, ROOT
import os

CASES = [
    ("tpex", "individual", 2001, 6, "tpex/2001/2001_6_i.html"),
    ("twse", "individual", 2005, 1, "twse/2005/2005_1_i.html"),
]


def db_codes(market, typ, year, month):
    sql = (f"SELECT company_code||'|'||company_name FROM operating_revenue WHERE market='{market}' "
           f"AND type='{typ}' AND year={year} AND month={month} ORDER BY company_code;")
    out = subprocess.check_output(
        ["psql", "-h", "localhost", "-p", "5432", "-d", "quantlib", "-tA", "-c", sql]
    ).decode()
    return [x for x in out.splitlines() if x.strip()]


for market, typ, year, month, rel in CASES:
    path = os.path.join(ROOT, rel)
    raw_codes = parse_html(path)
    raw_set = set(raw_codes)
    dbrows = db_codes(market, typ, year, month)
    db_set = set(x.split("|")[0] for x in dbrows)
    db_only = sorted(db_set - raw_set)
    raw_only = sorted(raw_set - db_set)
    print(f"\n===== {market} {typ} {year}-{month} =====")
    print(f"raw distinct={len(raw_set)}  db distinct={len(db_set)}")
    print("DB-only codes (in DB, not in my raw parse):", db_only)
    print("raw-only codes (in my parse, not in DB):", raw_only)
    # For each DB-only code, does it appear in raw file bytes at all?
    raw_bytes = open(path, "rb").read().decode("big5-hkscs", "replace")
    for code in db_only:
        name = [x.split("|",1)[1] for x in dbrows if x.split("|")[0] == code]
        present = code in raw_bytes
        print(f"   DB-only {code} ({name}): appears in raw file bytes? {present}")
