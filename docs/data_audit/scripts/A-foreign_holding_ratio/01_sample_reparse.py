"""Independent re-parse of foreign_holding_ratio raw files, compared field-by-field to PostgreSQL.

Deliberately does NOT import any project parsing code (the Scala reader is the unit under test).
Run: uv run --project research python <this file>
"""
import csv
import io
import json
import os
import subprocess
import sys
from collections import Counter

REPO = "/Users/zaoldyeck/Documents/scala/quantlib"


def pg(sql):
    out = subprocess.run(
        ["psql", "-h", "localhost", "-p", "5432", "-d", "quantlib", "-t", "-A", "-F", "\t", "-c", sql],
        capture_output=True, text=True, check=True).stdout
    return [l.split("\t") for l in out.strip().split("\n") if l.strip()]


def clean(s):
    return s.replace(",", "").replace("%", "").replace(" ", "").replace("=", "").strip().strip('"')


def parse_twse(path):
    """Return {code: (name, out_sh, rem_sh, held_sh, rem_r, held_r, limit_r)} + header date str."""
    raw = open(path, "rb").read()
    if not raw:
        return None, {}
    txt = raw.decode("big5hkscs", errors="replace")
    lines = txt.splitlines()
    hdr_date = lines[0].strip().strip('"') if lines else ""
    rows = {}
    rdr = csv.reader(io.StringIO(txt.replace("=", "")))
    for row in rdr:
        if len(row) < 10:
            continue
        code = clean(row[0])
        if not code or not code[0].isdigit():
            continue
        if not all(c.isdigit() or c.isupper() for c in code):
            continue
        try:
            rows[code] = (
                clean(row[1]),
                int(clean(row[3])), int(clean(row[4])), int(clean(row[5])),
                float(clean(row[6])), float(clean(row[7])), float(clean(row[8])),
            )
        except ValueError:
            continue
    return hdr_date, rows


def parse_tpex(path):
    raw = open(path, "rb").read()
    if not raw:
        return None, {}
    j = json.loads(raw.decode("utf-8"))
    t = j["tables"][0]
    rows = {}
    for r in t["data"]:
        if len(r) < 9:
            continue
        code = clean(r[1])
        if not code or not code[0].isdigit():
            continue
        try:
            rows[code] = (
                clean(r[2]),
                int(clean(r[3])), int(clean(r[4])), int(clean(r[5])),
                float(clean(r[6])), float(clean(r[7])), float(clean(r[8])),
            )
        except ValueError:
            continue
    return t.get("date"), rows


def db_rows(market, date):
    q = ("SELECT company_code, company_name, outstanding_shares, foreign_remaining_shares, "
         "foreign_held_shares, foreign_remaining_ratio, foreign_held_ratio, foreign_limit_ratio "
         f"FROM foreign_holding_ratio WHERE market='{market}' AND date='{date}'")
    out = {}
    for r in pg(q):
        out[r[0]] = (r[1].strip(), int(r[2]), int(r[3]), int(r[4]), float(r[5]), float(r[6]), float(r[7]))
    return out


def compare(market, fname):
    y, m, d = os.path.basename(fname)[:-4].split("_")
    date = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    hdr, raw = (parse_twse(fname) if market == "twse" else parse_tpex(fname))
    db = db_rows(market, date)
    only_raw = sorted(set(raw) - set(db))
    only_db = sorted(set(db) - set(raw))
    diffs = []
    for c in sorted(set(raw) & set(db)):
        if raw[c] != db[c]:
            diffs.append((c, raw[c], db[c]))
    print(f"{market} {date} hdr={hdr!r} raw={len(raw)} db={len(db)} "
          f"missing_in_db={len(only_raw)} extra_in_db={len(only_db)} value_diffs={len(diffs)}")
    if only_raw[:6]:
        print("   raw-only:", only_raw[:6])
    if only_db[:6]:
        print("   db-only :", only_db[:6])
    for c, a, b in diffs[:6]:
        print("   DIFF", c, "raw=", a, "db=", b)
    return len(only_raw), len(only_db), len(diffs)


SAMPLES = [
    ("twse", "2005/2005_1_3.csv"), ("twse", "2005/2005_7_1.csv"),
    ("twse", "2007/2007_3_1.csv"),
    ("twse", "2009/2009_12_31.csv"), ("twse", "2010/2010_1_4.csv"),
    ("twse", "2015/2015_6_1.csv"), ("twse", "2018/2018_9_3.csv"),
    ("twse", "2023/2023_5_15.csv"),
    ("twse", "2026/2026_7_1.csv"), ("twse", "2026/2026_7_17.csv"),
    ("tpex", "2011/2011_1_4.csv"), ("tpex", "2013/2013_6_3.csv"),
    ("tpex", "2015/2015_6_1.csv"), ("tpex", "2018/2018_9_3.csv"),
    ("tpex", "2023/2023_5_15.csv"),
    ("tpex", "2026/2026_7_1.csv"), ("tpex", "2026/2026_7_17.csv"),
]

if __name__ == "__main__":
    tot = Counter()
    for mkt, rel in SAMPLES:
        p = os.path.join(REPO, "data/foreign_holding_ratio", mkt, rel)
        if not os.path.exists(p):
            print("MISSING FILE", p)
            continue
        a, b, c = compare(mkt, p)
        tot["missing_in_db"] += a
        tot["extra_in_db"] += b
        tot["value_diffs"] += c
    print("TOTAL", dict(tot))
