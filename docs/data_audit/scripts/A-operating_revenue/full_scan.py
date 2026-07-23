"""Full schema-drift + encoding scan across ALL operating_revenue raw files.
For each file, decode with the SAME encoding rule the reader uses, then report
the distribution of column counts of data rows. Flags:
  - CSV data rows whose col count is NOT in {11, 14} (reader case 11 / case _).
    case _ needs >=14 cols or transferValues(7) throws -> import failure.
  - any UnicodeDecodeError / replacement chars (encoding boundary wrong).
  - HTML data rows (10-cell) count sanity.
Reader rules: CSV enc = UTF-8 if year>2012 else Big5-HKSCS; HTML always Big5-HKSCS.
"""
import csv
import glob
import os
import re
import collections

ROOT = "/Users/zaoldyeck/Documents/scala/quantlib/data/operating_revenue"

csv_colcount = collections.Counter()
html_ok = 0
enc_fail = []
weird_csv = []       # files with data-row col counts outside {11,14}
repl_char = []       # files with U+FFFD after decode (mojibake)
fname_re = re.compile(r"(\d+)_(\d+)_(\w)")

files = sorted(glob.glob(os.path.join(ROOT, "*", "*", "*")))
n_csv = n_html = 0
for path in files:
    base = os.path.basename(path)
    m = fname_re.match(base)
    if not m:
        continue
    year = int(m.group(1))
    if path.endswith(".csv"):
        n_csv += 1
        enc = "utf-8" if year > 2012 else "big5-hkscs"
        try:
            raw = open(path, "rb").read()
            text = raw.decode(enc)
        except UnicodeDecodeError as e:
            enc_fail.append((path, enc, str(e)[:60]))
            continue
        if "�" in text:
            repl_char.append((path, enc))
        rows = list(csv.reader(text.splitlines()))
        body = rows[1:]  # header dropped
        cc = collections.Counter(len(r) for r in body if r)
        for k, v in cc.items():
            csv_colcount[k] += v
        offbook = {k: v for k, v in cc.items() if k not in (11, 14)}
        # a lone stray count of 1 (blank tail line) is harmless; flag real ones
        real_off = {k: v for k, v in offbook.items() if k not in (0, 1)}
        if real_off:
            weird_csv.append((os.path.relpath(path, ROOT), dict(cc)))
    elif path.endswith(".html"):
        n_html += 1
        enc = "big5-hkscs"
        raw = open(path, "rb").read()
        try:
            raw.decode(enc, "strict")
            html_ok += 1
        except UnicodeDecodeError as e:
            enc_fail.append((path, enc, str(e)[:70]))

print(f"scanned: {n_csv} csv, {n_html} html")
print(f"\nCSV data-row column-count distribution (all files): {dict(csv_colcount)}")
print(f"\nCSV files with col counts outside {{11,14}} (excluding 0/1 blank tails): {len(weird_csv)}")
for p, cc in weird_csv[:40]:
    print("   ", p, cc)
print(f"\nEncoding decode FAILURES ({len(enc_fail)}):")
for p, enc, e in enc_fail[:40]:
    print("   ", os.path.relpath(p, ROOT), enc, e)
print(f"\nFiles with U+FFFD replacement chars after decode ({len(repl_char)}):")
for p, enc in repl_char[:40]:
    print("   ", os.path.relpath(p, ROOT), enc)
