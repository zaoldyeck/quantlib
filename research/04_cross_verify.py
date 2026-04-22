"""Cross-verify filename date vs CSV content date for index/ directory.

The bug: some local index CSVs have the wrong content (filename says
2012_5_31.csv but the file header says 106年03月03日 = 2017-03-03).
This script reads every CSV's first line to extract the content date,
compares against the filename-encoded date, and prints mismatches.

Usage:
    uv run python 04_cross_verify.py
"""
from __future__ import annotations
import os
import re
import sys
from pathlib import Path


ROC_HEADER_RE = re.compile(r"(\d+)年(\d+)月(\d+)日")


def read_content_date(path: Path) -> tuple[int, int, int] | None:
    """Try to read the ROC date from the first line (BIG5)."""
    try:
        with open(path, "rb") as f:
            raw = f.read(200)
        try:
            line = raw.decode("big5", errors="replace")
        except Exception:
            return None
        m = ROC_HEADER_RE.search(line)
        if not m:
            return None
        roc_y, mo, d = (int(m.group(i)) for i in (1, 2, 3))
        # ROC year + 1911 = CE year
        return (roc_y + 1911, mo, d)
    except OSError:
        return None


def parse_filename_date(name: str) -> tuple[int, int, int] | None:
    """Filename pattern: YYYY_M_D.csv"""
    m = re.match(r"^(\d{4})_(\d+)_(\d+)\.csv$", name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def main():
    root = Path("data/index")
    if not root.is_dir():
        print(f"Not found: {root}", file=sys.stderr)
        sys.exit(1)

    total = 0
    mismatches = []
    for csv in root.rglob("*.csv"):
        total += 1
        fn_date = parse_filename_date(csv.name)
        if fn_date is None:
            continue
        content_date = read_content_date(csv)
        if content_date is None:
            continue
        if fn_date != content_date:
            mismatches.append((csv, fn_date, content_date))

    print(f"Scanned {total} CSV files under {root}")
    print(f"Found {len(mismatches)} mismatches (filename date != content date):")
    for path, fn_d, ct_d in mismatches[:50]:
        print(f"  {path}")
        print(f"    filename: {fn_d[0]:04d}-{fn_d[1]:02d}-{fn_d[2]:02d}")
        print(f"    content:  {ct_d[0]:04d}-{ct_d[1]:02d}-{ct_d[2]:02d}")
    if len(mismatches) > 50:
        print(f"  ... (+{len(mismatches)-50} more)")


if __name__ == "__main__":
    main()
