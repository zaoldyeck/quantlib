"""Probe: negative signs, short-column drops, 備註 (note) field. Throwaway."""
import csv, json, re, glob, io, os
from collections import Counter

BASE = "/Users/zaoldyeck/Documents/scala/quantlib/data/sbl_borrowing"
STOCKCODE = re.compile(r"[0-9][0-9A-Z]*$")

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
    if not t: return []
    return t[0].get("data", [])

short_col_rows = []          # stock-code rows with <14 cols (would be dropped)
neg_examples = []            # negative values in cols 8-13
note_counter = Counter()     # 備註 raw values (col 14)
neg_col_counter = Counter()

def scan(path, rowfn, market):
    try:
        rows = rowfn(path)
    except Exception as e:
        return
    for row in rows:
        cells = [clean(c) for c in row]
        if not cells or not STOCKCODE.match(cells[0]):
            continue
        if len(cells) < 14:
            short_col_rows.append((market, os.path.basename(path), cells[0], len(cells)))
            continue
        # note col (index 14 if present) — raw before clean for the note text
        if len(row) > 14:
            note_raw = row[14].strip()
            if note_raw:
                note_counter[note_raw] += 1
        for idx in range(8, 14):
            try:
                v = int(cells[idx])
                if v < 0:
                    neg_col_counter[idx] += 1
                    if len(neg_examples) < 20:
                        neg_examples.append((market, os.path.basename(path), cells[0], idx, v))
            except ValueError:
                pass

# scan a broad sample: all files in 2016, 2019, 2022, 2026 for both markets
years = ["2016", "2019", "2022", "2026"]
nfiles = 0
for market, rowfn in [("twse", rows_twse), ("tpex", rows_tpex)]:
    for y in years:
        for path in glob.glob(f"{BASE}/{market}/{y}/*.csv"):
            scan(path, rowfn, market)
            nfiles += 1

print(f"scanned {nfiles} files across years {years}, both markets\n")
print(f"SHORT-COLUMN stock rows (would be dropped by size>=14): {len(short_col_rows)}")
for r in short_col_rows[:10]:
    print("   ", r)
print(f"\nNEGATIVE values by column index (8=prev 9=sold 10=ret 11=adj 12=bal 13=limit):")
for idx in sorted(neg_col_counter):
    print(f"   col{idx}: {neg_col_counter[idx]} negative values")
print("  examples:")
for e in neg_examples[:12]:
    print("   ", e)
print(f"\n備註 (note col 14) distinct raw values (top 15):")
for val, cnt in note_counter.most_common(15):
    print(f"   {repr(val)}: {cnt}")
