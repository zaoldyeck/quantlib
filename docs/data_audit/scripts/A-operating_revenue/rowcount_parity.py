"""Independent re-parse of operating_revenue raw files -> distinct company_code
count per (market,type,year,month), WITHOUT calling the Scala reader.
Compares to PG operating_revenue counts. Mirrors reader semantics:
  - HTML: rows with exactly 10 cells whose head != header -> data. code=cell[0].
  - CSV(before-IFRS, 11 data cols): drop header(10 cols); strip '='; code=cell[0].
  - CSV(after-IFRS, 14 cols): drop header; code=cell[2].
  - dedupe by company_code (reader does distinctBy(market, companyCode)).
"""
import csv
import os
import re
import subprocess
import sys
from html.parser import HTMLParser

ROOT = "/Users/zaoldyeck/Documents/scala/quantlib/data/operating_revenue"


class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.cur = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.cur = []
        elif tag in ("td", "th") and self.cur is not None:
            self.cell = []

    def handle_endtag(self, tag):
        if tag == "tr" and self.cur is not None:
            self.rows.append(self.cur)
            self.cur = None
        elif tag in ("td", "th") and self.cell is not None:
            self.cur.append("".join(self.cell).strip())
            self.cell = None

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)


def parse_html(path):
    raw = open(path, "rb").read().decode("big5-hkscs", "replace")
    p = TableParser()
    p.feed(raw)
    codes = []
    for r in p.rows:
        # reader: size==10 && head != "公司 代號" (Jsoup) == "公司代號" here
        if len(r) == 10 and r[0].replace(" ", "") != "公司代號":
            codes.append(r[0])
    return codes


def parse_csv(path, year):
    enc = "utf-8" if year > 2012 else "big5-hkscs"
    rows = list(csv.reader(open(path, encoding=enc)))
    body = rows[1:]  # reader drops header via .tail
    codes = []
    for r in body:
        if len(r) == 11:  # before-IFRS consolidated
            code = r[0].replace("=", "").replace('"', "").strip()
        else:  # after-IFRS (14) etc.
            code = r[2].replace("=", "").replace('"', "").strip()
        codes.append(code)
    return codes


def dedupe(codes):
    seen = set()
    out = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def db_count(market, typ, year, month):
    sql = (
        f"SELECT count(*) FROM operating_revenue WHERE market='{market}' "
        f"AND type='{typ}' AND year={year} AND month={month};"
    )
    out = subprocess.check_output(
        ["psql", "-h", "localhost", "-p", "5432", "-d", "quantlib", "-tA", "-c", sql]
    )
    return int(out.strip())


SAMPLES = [
    # (market, type, year, month, relpath)
    ("twse", "individual", 2001, 6, "twse/2001/2001_6_i.html"),
    ("tpex", "individual", 2001, 6, "tpex/2001/2001_6_i.html"),
    ("twse", "individual", 2005, 1, "twse/2005/2005_1_i.html"),
    ("twse", "consolidated", 2005, 1, "twse/2005/2005_1_c.csv"),
    ("twse", "consolidated", 2008, 1, "twse/2008/2008_1_c.csv"),
    ("tpex", "consolidated", 2008, 1, "tpex/2008/2008_1_c.csv"),
    ("twse", "consolidated", 2012, 1, "twse/2012/2012_1_c.csv"),
    ("twse", "consolidated", 2013, 1, "twse/2013/2013_1_c.csv"),
    ("tpex", "consolidated", 2013, 1, "tpex/2013/2013_1_c.csv"),
    ("twse", "consolidated", 2024, 1, "twse/2024/2024_1_c.csv"),
    ("tpex", "consolidated", 2024, 1, "tpex/2024/2024_1_c.csv"),
    ("twse", "consolidated", 2026, 1, "twse/2026/2026_1_c.csv"),
]

print(f"{'market':5} {'type':12} {'y-m':8} {'rawN':>6} {'distinct':>8} {'dbN':>6}  {'status'}")
bad = 0
for market, typ, year, month, rel in SAMPLES:
    path = os.path.join(ROOT, rel)
    if not os.path.exists(path):
        print(market, typ, f"{year}-{month}", "MISSING FILE", rel)
        continue
    if path.endswith(".html"):
        codes = parse_html(path)
    else:
        codes = parse_csv(path, year)
    dd = dedupe(codes)
    dbn = db_count(market, typ, year, month)
    ok = "OK" if len(dd) == dbn else "*** MISMATCH ***"
    if len(dd) != dbn:
        bad += 1
    dupes = len(codes) - len(dd)
    print(f"{market:5} {typ:12} {year}-{month:<5} {len(codes):>6} {len(dd):>8} {dbn:>6}  {ok}"
          + (f"  (raw had {dupes} dup-codes)" if dupes else ""))

print("\nMISMATCHES:", bad)
