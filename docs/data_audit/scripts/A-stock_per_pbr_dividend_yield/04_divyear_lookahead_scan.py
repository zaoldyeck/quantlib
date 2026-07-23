"""Full scan: per-file count of rows whose 股利年度 is in the FUTURE vs the file date."""
from __future__ import annotations

import csv
import io
import re
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

ROOT = Path("/Users/zaoldyeck/Documents/scala/quantlib/data/stock_per_pbr_dividend_yield")
FNAME = re.compile(r"^(\d+)_(\d+)_(\d+)\.csv$")
CODE = re.compile(r"^[0-9][0-9A-Z]*$")


def norm(s):
    return s.replace("=", "").strip().strip('"').replace(" ", "")


def scan(args):
    market, path = args
    p = Path(path)
    m = FNAME.match(p.name)
    y, mo = int(m.group(1)), int(m.group(2))
    roc = y - 1911
    raw = p.read_bytes()
    if len(raw) < 100:
        return None
    txt = raw.decode("big5hkscs", errors="replace").replace("=", "")
    rows = list(csv.reader(io.StringIO(txt)))
    hdr_i = None
    for i, r in enumerate(rows):
        if r and norm(r[0]) in ("證券代號", "股票代號"):
            hdr_i = i
            break
    if hdr_i is None:
        return None
    hdr = [norm(c) for c in rows[hdr_i]]
    if "股利年度" not in hdr:
        return (market, f"{y}-{mo:02d}", 0, 0, 0)
    i_y = hdr.index("股利年度")
    i_yld = next(i for i, c in enumerate(hdr) if c.startswith("殖利率"))
    n = fut = fut_nz = 0
    for r in rows[hdr_i + 1:]:
        if not r or not CODE.match(norm(r[0])) or len(r) <= max(i_y, i_yld):
            continue
        n += 1
        try:
            dvy = int(norm(r[i_y]))
        except ValueError:
            continue
        if dvy > roc:
            fut += 1
            try:
                if float(norm(r[i_yld]).replace(",", "")) != 0.0:
                    fut_nz += 1
            except ValueError:
                pass
    return (market, f"{y}-{mo:02d}", n, fut, fut_nz)


def main():
    jobs = [(mk, str(f)) for mk in ("twse", "tpex") for f in sorted((ROOT / mk).rglob("*.csv"))]
    agg = defaultdict(lambda: [0, 0, 0, 0])
    with ProcessPoolExecutor() as ex:
        for res in ex.map(scan, jobs, chunksize=32):
            if res is None:
                continue
            mk, ym, n, fut, fnz = res
            a = agg[(mk, ym)]
            a[0] += n
            a[1] += fut
            a[2] += fnz
            a[3] += 1
    print("market,year_month,files,rows,future_divyear_rows,future_with_nonzero_yield")
    for (mk, ym), a in sorted(agg.items()):
        if a[1] or ym.endswith(("-01", "-07")):
            print(f"{mk},{ym},{a[3]},{a[0]},{a[1]},{a[2]}")
    print("\n--- totals with future div-year ---")
    tot = defaultdict(lambda: [0, 0, 0])
    for (mk, ym), a in agg.items():
        t = tot[mk]
        t[0] += a[0]
        t[1] += a[1]
        t[2] += a[2]
    for mk, t in tot.items():
        print(mk, "rows=", t[0], "future=", t[1], "future_nonzero_yield=", t[2])


if __name__ == "__main__":
    main()
