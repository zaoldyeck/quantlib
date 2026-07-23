"""Independent SBL parse vs PG. Throwaway audit probe (does NOT call Scala reader)."""
import csv, json, re, subprocess, io

BASE = "/Users/zaoldyeck/Documents/scala/quantlib/data/sbl_borrowing"
STOCKCODE = re.compile(r"[0-9][0-9A-Z]*$")

def clean(s):
    return s.replace("=", "").replace('"', "").replace(",", "").replace("%", "").replace(" ", "").strip()

def parse_twse(path):
    """Big5 CSV -> dict code -> (name, prev, sold, ret, adj, bal, limit) using cols 8-13."""
    out = {}
    with open(path, "rb") as f:
        raw = f.read().decode("big5hkscs", errors="replace")
    rdr = csv.reader(io.StringIO(raw))
    for row in rdr:
        cells = [clean(c) for c in row]
        if len(cells) < 14:
            continue
        if not STOCKCODE.match(cells[0]):
            continue
        try:
            out[cells[0]] = (cells[1], int(cells[8]), int(cells[9]), int(cells[10]),
                             int(cells[11]), int(cells[12]), int(cells[13]))
        except ValueError:
            pass
    return out

def parse_tpex(path):
    with open(path, "rb") as f:
        j = json.load(f)
    out = {}
    data = j["tables"][0]["data"]
    for row in data:
        cells = [clean(c) for c in row]
        if len(cells) < 14:
            continue
        if not STOCKCODE.match(cells[0]):
            continue
        try:
            out[cells[0]] = (cells[1], int(cells[8]), int(cells[9]), int(cells[10]),
                             int(cells[11]), int(cells[12]), int(cells[13]))
        except ValueError:
            pass
    return out

def pg(market, date, codes):
    codelist = ",".join(f"'{c}'" for c in codes)
    sql = (f"SELECT company_code, company_name, prev_day_balance, daily_sold, daily_returned, "
           f"daily_adjustment, daily_balance, next_day_limit FROM sbl_borrowing "
           f"WHERE market='{market}' AND date='{date}' AND company_code IN ({codelist}) "
           f"ORDER BY company_code;")
    r = subprocess.run(["psql","-h","localhost","-p","5432","-d","quantlib","-tAF","|","-c",sql],
                       capture_output=True, text=True)
    out = {}
    for line in r.stdout.strip().splitlines():
        p = line.split("|")
        out[p[0]] = (p[1], int(p[2]), int(p[3]), int(p[4]), int(p[5]), int(p[6]), int(p[7]))
    return out

CASES = [
    ("twse", "2016-01-11", f"{BASE}/twse/2016/2016_1_11.csv", parse_twse),
    ("twse", "2022-06-15", f"{BASE}/twse/2022/2022_6_15.csv", parse_twse),
    ("twse", "2026-07-08", f"{BASE}/twse/2026/2026_7_8.csv", parse_twse),
    ("tpex", "2016-01-11", f"{BASE}/tpex/2016/2016_1_11.csv", parse_tpex),
    ("tpex", "2022-06-15", f"{BASE}/tpex/2022/2022_6_15.csv", parse_tpex),
    ("tpex", "2026-07-08", f"{BASE}/tpex/2026/2026_7_8.csv", parse_tpex),
]

total_mismatch = 0
for market, date, path, parser in CASES:
    parsed = parser(path)
    # sample: first 5 codes + some non-zero-balance codes
    codes = list(parsed.keys())
    nonzero = [c for c in codes if parsed[c][5] > 0][:8]
    sample = sorted(set(codes[:5] + nonzero + codes[-3:]))
    dbvals = pg(market, date, sample)
    print(f"\n===== {market} {date}  raw_rows={len(parsed)}  sample={len(sample)} =====")
    mism = 0
    for c in sample:
        praw = parsed.get(c)
        pdb = dbvals.get(c)
        if pdb is None:
            print(f"  MISSING IN DB: {c} raw={praw}")
            mism += 1
            continue
        # compare numeric tuple (name may differ in whitespace)
        if praw[1:] != pdb[1:]:
            print(f"  MISMATCH {c}: raw={praw} db={pdb}")
            mism += 1
    if mism == 0:
        print(f"  OK all {len(sample)} sampled codes match (name+6 numeric cols)")
        # show one example
        ex = sample[0]
        print(f"    e.g. {ex}: raw={parsed[ex]} db={dbvals[ex]}")
    total_mismatch += mism

# also: full-file count parity for one case
print("\n===== FULL-FILE COUNT PARITY =====")
for market, date, path, parser in CASES:
    parsed = parser(path)
    r = subprocess.run(["psql","-h","localhost","-p","5432","-d","quantlib","-tAc",
        f"SELECT count(*) FROM sbl_borrowing WHERE market='{market}' AND date='{date}';"],
        capture_output=True, text=True)
    dbcount = int(r.stdout.strip())
    print(f"  {market} {date}: raw_valid_rows={len(parsed)}  db_rows={dbcount}  diff={len(parsed)-dbcount}")

print(f"\nTOTAL MISMATCH = {total_mismatch}")
