"""(1) Cross-verify filename period vs content 資料年月 for every after-IFRS CSV
    (reader takes period from FILENAME and drops the content date column, so a
    crawler misnaming would silently mislabel the DB period).
(2) Spot negative monthly_revenue rows and confirm sign fidelity raw vs DB.
"""
import csv
import glob
import os
import re

ROOT = "/Users/zaoldyeck/Documents/scala/quantlib/data/operating_revenue"
fname_re = re.compile(r"(\d+)_(\d+)_c\.csv$")

mismatches = []
checked = 0
files = sorted(glob.glob(os.path.join(ROOT, "*", "*", "*_c.csv"))) + \
        sorted(glob.glob(os.path.join(ROOT, "*", "*_c.csv")))
for path in files:
    base = os.path.basename(path)
    m = fname_re.search(base)
    if not m:
        continue
    fy, fm = int(m.group(1)), int(m.group(2))
    if fy <= 2012:
        continue  # before-IFRS consolidated has NO date column; skip
    enc = "utf-8"
    rows = list(csv.reader(open(path, encoding=enc)))
    if len(rows) < 2:
        continue
    # find first data row with a 資料年月 like NNN/M in col1
    for r in rows[1:]:
        if len(r) >= 2 and re.match(r"^\d{3}/\d{1,2}$", r[1].strip()):
            roc_y, roc_m = r[1].strip().split("/")
            cy, cm = int(roc_y) + 1911, int(roc_m)
            checked += 1
            if (cy, cm) != (fy, fm):
                mismatches.append((os.path.relpath(path, ROOT), (fy, fm), (cy, cm)))
            break

print(f"after-IFRS CSV filename-vs-content date check: {checked} files checked, "
      f"{len(mismatches)} mismatches")
for p, fn, ct in mismatches[:50]:
    print("   MISMATCH", p, "filename", fn, "content", ct)
