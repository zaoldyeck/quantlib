"""Full-corpus independent re-parse of ALL foreign_holding_ratio raw files vs PostgreSQL.

Per (market, filename-date):
  - independent parse row count + column-wise checksums
  - content-date extracted from the file itself (TWSE header 民國 / TPEx JSON "date")
  - compared to PG aggregates for the same (market, date)
"""
import csv
import io
import json
import os
import re
import subprocess
from collections import Counter

sys_path = None
REPO = "/Users/zaoldyeck/Documents/scala/quantlib"
BASE = os.path.join(REPO, "data/foreign_holding_ratio")


def clean(s):
    return s.replace(",", "").replace("%", "").replace(" ", "").replace("=", "").strip().strip('"')


def ok_code(c):
    return bool(c) and c[0].isdigit() and all(ch.isdigit() or ch.isupper() for ch in c)


def parse_twse(path):
    raw = open(path, "rb").read()
    if not raw:
        return None, []
    txt = raw.decode("big5hkscs", errors="replace")
    lines = txt.splitlines()
    cd = None
    if lines:
        m = re.match(r'^"?(\d{2,3})年(\d{2})月(\d{2})日', lines[0].strip())
        if m:
            cd = f"{int(m.group(1)) + 1911:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    out = []
    for row in csv.reader(io.StringIO(txt.replace("=", ""))):
        if len(row) < 10:
            continue
        code = clean(row[0])
        if not ok_code(code):
            continue
        try:
            out.append((code, clean(row[1]), int(clean(row[3])), int(clean(row[4])), int(clean(row[5])),
                        float(clean(row[6])), float(clean(row[7])), float(clean(row[8]))))
        except ValueError:
            continue
    return cd, out


def parse_tpex(path):
    raw = open(path, "rb").read()
    if not raw:
        return None, []
    j = json.loads(raw.decode("utf-8"))
    t = j["tables"][0]
    d = t.get("date")
    cd = None
    if d and re.match(r"^\d{3}/\d{2}/\d{2}$", d):
        cd = f"{int(d[:3]) + 1911:04d}-{d[4:6]}-{d[7:9]}"
    out = []
    for r in t["data"]:
        if len(r) < 9:
            continue
        code = clean(r[1])
        if not ok_code(code):
            continue
        try:
            out.append((code, clean(r[2]), int(clean(r[3])), int(clean(r[4])), int(clean(r[5])),
                        float(clean(r[6])), float(clean(r[7])), float(clean(r[8]))))
        except ValueError:
            continue
    return cd, out


def sig(rows):
    # dedupe by code like the reader does (distinctBy market,date,code) -> keep FIRST
    seen = {}
    for r in rows:
        if r[0] not in seen:
            seen[r[0]] = r
    v = list(seen.values())
    return (len(v),
            sum(x[2] for x in v), sum(x[3] for x in v), sum(x[4] for x in v),
            round(sum(x[5] for x in v), 2), round(sum(x[6] for x in v), 2), round(sum(x[7] for x in v), 2))


def pg_agg():
    sql = ("SELECT market, date, count(*), sum(outstanding_shares), sum(foreign_remaining_shares), "
           "sum(foreign_held_shares), round(sum(foreign_remaining_ratio)::numeric,2), "
           "round(sum(foreign_held_ratio)::numeric,2), round(sum(foreign_limit_ratio)::numeric,2) "
           "FROM foreign_holding_ratio GROUP BY market, date")
    out = subprocess.run(["psql", "-h", "localhost", "-p", "5432", "-d", "quantlib", "-t", "-A", "-F", "\t",
                          "-c", sql], capture_output=True, text=True, check=True).stdout
    res = {}
    for line in out.strip().split("\n"):
        p = line.split("\t")
        res[(p[0], p[1])] = (int(p[2]), int(p[3]), int(p[4]), int(p[5]),
                             round(float(p[6]), 2), round(float(p[7]), 2), round(float(p[8]), 2))
    return res


def main():
    db = pg_agg()
    stats = Counter()
    date_mismatch = []
    count_mismatch = []
    value_mismatch = []
    missing_in_db = []
    files = []
    for mkt in ("twse", "tpex"):
        for yr in sorted(os.listdir(os.path.join(BASE, mkt))):
            d = os.path.join(BASE, mkt, yr)
            if not os.path.isdir(d):
                continue
            for f in sorted(os.listdir(d)):
                if f.endswith(".csv"):
                    files.append((mkt, os.path.join(d, f)))
    print("raw files:", len(files))
    for mkt, path in files:
        y, m, dd = os.path.basename(path)[:-4].split("_")
        fdate = f"{int(y):04d}-{int(m):02d}-{int(dd):02d}"
        cd, rows = (parse_twse(path) if mkt == "twse" else parse_tpex(path))
        if not rows:
            stats["empty_or_nodata"] += 1
            if (mkt, fdate) in db:
                stats["EMPTY_BUT_IN_DB"] += 1
            continue
        stats["parsed"] += 1
        if cd and cd != fdate:
            date_mismatch.append((mkt, fdate, cd, len(rows)))
        s = sig(rows)
        k = (mkt, fdate)
        if k not in db:
            missing_in_db.append((mkt, fdate, s[0]))
            continue
        if db[k][0] != s[0]:
            count_mismatch.append((mkt, fdate, s[0], db[k][0]))
        elif db[k] != s:
            value_mismatch.append((mkt, fdate, s, db[k]))
    print("stats", dict(stats))
    print("\nDATE MISMATCH (content-date != filename-date):", len(date_mismatch))
    by = Counter((m, c) for m, f, c, n in date_mismatch)
    for k, v in by.most_common(20):
        print("   ", k, v)
    print("\nROW-COUNT MISMATCH:", len(count_mismatch))
    for x in count_mismatch[:20]:
        print("   ", x)
    print("\nVALUE CHECKSUM MISMATCH:", len(value_mismatch))
    for x in value_mismatch[:20]:
        print("   ", x)
    print("\nPARSED-BUT-NOT-IN-DB:", len(missing_in_db))
    for x in missing_in_db[:20]:
        print("   ", x)
    dbonly = sorted(set(db) - {(m, f"{int(os.path.basename(p)[:-4].split('_')[0]):04d}-"
                               f"{int(os.path.basename(p)[:-4].split('_')[1]):02d}-"
                               f"{int(os.path.basename(p)[:-4].split('_')[2]):02d}") for m, p in files})
    print("\nIN-DB-BUT-NO-RAW-FILE:", len(dbonly), dbonly[:20])


main()
