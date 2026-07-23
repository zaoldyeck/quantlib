"""A-sbl_borrowing ①:全 6,266 個原始檔的版型普查(不解析值,只看骨架)。

問的問題:欄位順序有沒有隨年份無聲漂移?TWSE 的 Big5 標頭簽章有幾種?TPEx JSON
的 `fields` 陣列有幾種?`tables` 會不會不只一張?每檔的欄數分佈?哪些檔是
0-byte sentinel(休市日)或空回應?

Run: uv run --project research python docs/data_audit/scripts/A-sbl_borrowing/01_format_scan.py
不依賴 cache / PG,只讀 data/sbl_borrowing/**。
"""
from __future__ import annotations

import collections
import csv
import io
import json
import re
from pathlib import Path

from research import paths

RAW = paths.RAW / "sbl_borrowing"
FNAME = re.compile(r"^(\d{4})_(\d{1,2})_(\d{1,2})\.csv$")


def twse_rows(path: Path):
    """複製 QuantlibCSVReader 的兩條特規:整行 replace('=','') + 含 "" 但無 ,"" 跳行。"""
    raw = path.read_bytes().decode("big5hkscs", errors="replace")
    out = []
    for line in raw.splitlines():
        if '""' in line and ',""' not in line:
            continue
        out.append(next(csv.reader([line.replace("=", "")])) if line.strip() else [])
    return out


def main() -> None:
    twse_sig = collections.Counter()
    tpex_sig = collections.Counter()
    tpex_tables_n = collections.Counter()
    tpex_keys = collections.Counter()
    sizes = collections.Counter()
    empty = collections.defaultdict(list)
    widths = collections.defaultdict(collections.Counter)
    n_files = collections.Counter()
    first_sig_date = {}

    for market in ("twse", "tpex"):
        for path in sorted((RAW / market).rglob("*.csv")):
            m = FNAME.match(path.name)
            if not m:
                print(f"[BADNAME] {path}")
                continue
            date = f"{int(m[1]):04d}-{int(m[2]):02d}-{int(m[3]):02d}"
            n_files[market] += 1
            sz = path.stat().st_size
            sizes[(market, "0" if sz == 0 else "<200" if sz < 200 else ">=200")] += 1
            if sz < 200:
                empty[market].append((date, sz))
                continue
            if market == "tpex":
                obj = json.loads(path.read_text("utf-8"))
                tpex_keys[tuple(sorted(obj.keys()))] += 1
                tables = obj.get("tables", [])
                tpex_tables_n[len(tables)] += 1
                for t in tables:
                    sig = tuple(t.get("fields", []))
                    tpex_sig[sig] += 1
                    first_sig_date.setdefault(("tpex", sig), date)
                    for row in t.get("data", []):
                        widths["tpex"][len(row)] += 1
            else:
                rows = twse_rows(path)
                hdr = next((r for r in rows if r and r[0].strip() == "代號"), None)
                sig = tuple(x.strip() for x in hdr) if hdr else ("<NO-HEADER>",)
                twse_sig[sig] += 1
                first_sig_date.setdefault(("twse", sig), date)
                for row in rows:
                    if row and re.fullmatch(r"[0-9][0-9A-Za-z]*", row[0].strip()):
                        widths["twse"][len(row)] += 1

    print("== 檔數 ==", dict(n_files))
    print("== 大小分佈 ==", dict(sizes))
    for mk in ("twse", "tpex"):
        print(f"== {mk} 空/迷你檔 {len(empty[mk])} 個 ==", empty[mk][:12])
    print("\n== TWSE 標頭簽章 ==")
    for sig, n in twse_sig.most_common():
        print(f"  n={n:5d} first={first_sig_date.get(('twse', sig))} {sig}")
    print("\n== TPEx fields 簽章 ==")
    for sig, n in tpex_sig.most_common():
        print(f"  n={n:5d} first={first_sig_date.get(('tpex', sig))} {sig}")
    print("\n== TPEx top-level keys ==", tpex_keys.most_common())
    print("== TPEx tables 張數 ==", tpex_tables_n.most_common())
    print("== 資料列欄數分佈 ==", {k: dict(v) for k, v in widths.items()})


if __name__ == "__main__":
    main()
