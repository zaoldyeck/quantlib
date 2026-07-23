"""Cross-verify filename date vs in-file content date for stock_per_pbr_dividend_yield.

TWSE title line:  "113年06月21日 個股日本益比、殖利率及股價淨值比"
TPEx line 2:      資料日期:115/04/21
Also censuses header-less / tiny files and duplicate payloads across dates.
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path("/Users/zaoldyeck/Documents/scala/quantlib/data/stock_per_pbr_dividend_yield")
FNAME = re.compile(r"^(\d+)_(\d+)_(\d+)\.csv$")
TWSE_TITLE = re.compile(r"(\d{2,3})年(\d{1,2})月(\d{1,2})日")
TPEX_DATE = re.compile(r"資料日期[:：]\s*(\d{2,3})/(\d{1,2})/(\d{1,2})")

mismatch = []
nodate = []
nohdr = Counter()
nohdr_examples = defaultdict(list)
payload = defaultdict(list)   # (market, sha1 of data section) -> [dates]
sizes = Counter()

for mk in ("twse", "tpex"):
    for f in sorted((ROOT / mk).rglob("*.csv")):
        m = FNAME.match(f.name)
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        raw = f.read_bytes()
        sizes[(mk, len(raw))] += 1 if len(raw) < 20 else 0
        if len(raw) < 20:
            nohdr[(mk, len(raw))] += 1
            if len(nohdr_examples[(mk, len(raw))]) < 3:
                nohdr_examples[(mk, len(raw))].append((str(f), raw[:20]))
            continue
        txt = raw.decode("big5hkscs", errors="replace")
        head = txt[:400]
        if mk == "twse":
            g = TWSE_TITLE.search(head)
        else:
            g = TPEX_DATE.search(head)
        if not g:
            nodate.append(str(f))
            continue
        ry, rm, rd = int(g.group(1)), int(g.group(2)), int(g.group(3))
        if (ry + 1911, rm, rd) != (y, mo, d):
            mismatch.append((str(f), f"file={y}-{mo:02d}-{d:02d}", f"content=ROC{ry}/{rm:02d}/{rd:02d} -> {ry+1911}-{rm:02d}-{rd:02d}"))
        # payload hash over data lines only (strip the date-bearing title)
        lines = [l for l in txt.splitlines() if re.match(r'^"?=?"?\d', l)]
        h = hashlib.sha1("\n".join(lines).encode()).hexdigest()
        payload[(mk, h)].append(f"{y}-{mo:02d}-{d:02d}")

print("=== files < 20 bytes (sentinels) ===")
for k, v in sorted(nohdr.items()):
    print(k, v, nohdr_examples[k][:1])
print("\n=== files with no parseable content date ===", len(nodate))
for p in nodate[:20]:
    print("  ", p)
print("\n=== filename-date vs content-date MISMATCH ===", len(mismatch))
for r in mismatch[:60]:
    print("  ", r)

print("\n=== identical data payload shared by >1 date ===")
dups = {k: v for k, v in payload.items() if len(v) > 1}
print("groups:", len(dups), " files involved:", sum(len(v) for v in dups.values()))
per_mk = Counter(k[0] for k in dups)
print(per_mk)
shown = 0
for k, v in sorted(dups.items(), key=lambda kv: kv[1][0]):
    print("  ", k[0], sorted(v))
    shown += 1
    if shown >= 40:
        print("   ... truncated")
        break
