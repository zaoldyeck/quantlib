"""Probe: duplicate codes within a file, and rows dropped by stockCode regex. Throwaway."""
import csv, json, re, glob, io, os
from collections import Counter

BASE = "/Users/zaoldyeck/Documents/scala/quantlib/data/sbl_borrowing"
STOCKCODE = re.compile(r"[0-9][0-9A-Z]*$")
HEADERISH = {"股票", "代號", "名稱", ""}

def clean(s):
    return s.replace("=", "").replace('"', "").replace(",", "").replace("%", "").replace(" ", "").strip()

def rows_twse(path):
    with open(path, "rb") as f:
        raw = f.read().decode("big5hkscs", errors="replace")
    return list(csv.reader(io.StringIO(raw)))

def rows_tpex(path):
    with open(path, "rb") as f:
        j = json.load(f)
    t = j.get("tables")
    return t[0].get("data", []) if t else []

dup_files = []            # files with duplicate stock codes among valid rows
dropped_by_regex = Counter()  # col0 values that have >=14 cols but fail regex (potential real securities)

def scan(path, rowfn, market):
    try:
        rows = rowfn(path)
    except Exception:
        return
    seen = {}
    for row in rows:
        cells = [clean(c) for c in row]
        if len(cells) < 14:
            continue
        c0 = cells[0]
        if STOCKCODE.match(c0):
            if c0 in seen and seen[c0] != tuple(cells[8:14]):
                dup_files.append((market, os.path.basename(path), c0, seen[c0], tuple(cells[8:14])))
            seen[c0] = tuple(cells[8:14])
        else:
            # has >=14 cols but col0 not a stock code — is it a real security or a note/header row?
            if c0 not in HEADERISH and not any(k in c0 for k in ["說明", "備註", "資料", "統計"]):
                dropped_by_regex[c0] += 1

years = ["2016", "2019", "2022", "2026"]
n = 0
for market, rowfn in [("twse", rows_twse), ("tpex", rows_tpex)]:
    for y in years:
        for path in glob.glob(f"{BASE}/{market}/{y}/*.csv"):
            scan(path, rowfn, market)
            n += 1

print(f"scanned {n} files")
print(f"\nDUPLICATE stock codes with DIFFERING 借券 values within a file: {len(dup_files)}")
for d in dup_files[:10]:
    print("   ", d)
print(f"\nRows with >=14 cols whose col0 FAILS stockCode regex (potential dropped securities): {len(dropped_by_regex)} distinct")
for val, cnt in dropped_by_regex.most_common(20):
    print(f"   {repr(val)}: {cnt}")
