"""Independent (re-implemented) parse of every stock_per_pbr_dividend_yield raw CSV.

Deliberately does NOT call TradingReader — that is the audited subject.
Column mapping is driven by the CSV's own header row (Chinese column names),
not by column count, so it is immune to the fall-through bug class.

Output: scratchpad/raw_parsed.parquet with
  market, date, company_code, company_name, pe, pb, dy, ncols, hdr_era
"""
from __future__ import annotations

import csv
import io
import re
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import polars as pl

ROOT = Path("/Users/zaoldyeck/Documents/scala/quantlib/data/stock_per_pbr_dividend_yield")
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/raw_parsed.parquet")

FNAME = re.compile(r"^(\d+)_(\d+)_(\d+)\.csv$")
CODE = re.compile(r"^[0-9][0-9A-Z]*$")

# Chinese header labels -> canonical field
PE_LABELS = {"本益比"}
PB_LABELS = {"股價淨值比"}
DY_LABELS = {"殖利率(%)", "殖利率(%)"}
CODE_LABELS = {"證券代號", "股票代號"}
NAME_LABELS = {"證券名稱", "名稱", "公司名稱"}


def norm(s: str) -> str:
    return s.replace("=", "").strip().strip('"').replace(" ", "").replace("　", "")


def to_f(s: str):
    """Mirror Scala String.toDoubleOption after ' ' and ',' stripping."""
    s = s.replace(" ", "").replace(",", "").replace("　", "")
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_file(args):
    market, path = args
    p = Path(path)
    m = FNAME.match(p.name)
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    date = f"{y:04d}-{mo:02d}-{d:02d}"
    raw = p.read_bytes()
    if not raw:
        return []
    txt = raw.decode("big5hkscs", errors="replace").replace("=", "")
    rows = list(csv.reader(io.StringIO(txt)))
    # locate header
    hdr_idx = None
    for i, r in enumerate(rows):
        if r and norm(r[0]) in CODE_LABELS:
            hdr_idx = i
            break
    if hdr_idx is None:
        return [("__NOHEADER__", market, date, "", "", None, None, None, 0)]
    hdr = [norm(c) for c in rows[hdr_idx]]
    era = "|".join(hdr)
    try:
        i_code = next(i for i, c in enumerate(hdr) if c in CODE_LABELS)
        i_name = next(i for i, c in enumerate(hdr) if c in NAME_LABELS)
        i_pe = next(i for i, c in enumerate(hdr) if c in PE_LABELS)
        i_pb = next(i for i, c in enumerate(hdr) if c in PB_LABELS)
        i_dy = next(i for i, c in enumerate(hdr) if c.startswith("殖利率"))
    except StopIteration:
        return [("__BADHEADER__", market, date, era, "", None, None, None, 0)]

    out = []
    for r in rows[hdr_idx + 1:]:
        if not r:
            continue
        c0 = norm(r[0])
        if not CODE.match(c0):
            continue
        if len(r) <= max(i_pe, i_pb, i_dy, i_name):
            out.append(("__SHORTROW__", market, date, c0, era, None, None, None, len(r)))
            continue
        out.append((
            "row", market, date, c0,
            norm(r[i_name]),
            to_f(r[i_pe]), to_f(r[i_pb]), to_f(r[i_dy]),
            len(r),
        ))
    return out


def main():
    jobs = []
    for mk in ("twse", "tpex"):
        for f in sorted((ROOT / mk).rglob("*.csv")):
            jobs.append((mk, str(f)))
    print(f"files={len(jobs)}", flush=True)
    recs = []
    with ProcessPoolExecutor() as ex:
        for i, res in enumerate(ex.map(parse_file, jobs, chunksize=32)):
            recs.extend(res)
            if i % 2000 == 0:
                print(f"  {i}/{len(jobs)} rows={len(recs)}", flush=True)
    df = pl.DataFrame(
        recs,
        schema=["kind", "market", "date", "company_code", "company_name",
                "pe", "pb", "dy", "ncols"],
        orient="row",
    ).with_columns(pl.col("date").str.to_date())
    df.write_parquet(OUT)
    print(df.shape)
    print(df.group_by("kind").len())
    print(df.filter(pl.col("kind") == "row").group_by(["market", "ncols"]).len().sort(["market", "ncols"]))


if __name__ == "__main__":
    main()
