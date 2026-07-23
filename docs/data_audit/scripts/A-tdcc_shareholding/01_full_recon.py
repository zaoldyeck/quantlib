#!/usr/bin/env python3
"""A-tdcc_shareholding 稽核 01:原始檔全量重讀 → PG 逐格對帳。

獨立解析器,**不呼叫** TradingReader(那是受測對象)。做四件事:
  1. 掃描 29 個原始檔的標頭簽章 / 欄位數 / 內容日期 / tier 值域(抓版型漂移)
  2. 建立 (data_date, company_code, holding_tier) → (人數, 股數, 比例) 的全量字典
  3. 與 PG tdcc_shareholding 全表逐格比對(only_raw / only_db / 每欄不符數)
  4. 檢查同 data_date 的多份下載檔內容是否互相矛盾(TDCC 端點只給當週,
     一個 data_date 會落在好幾個檔名)

不依賴 cache.duckdb。需要本機 PostgreSQL。

Run:
    python3 docs/data_audit/scripts/A-tdcc_shareholding/01_full_recon.py
"""
from __future__ import annotations

import collections
import csv
import glob
import os
import subprocess
import sys
import tempfile

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
RAW_GLOB = os.path.join(REPO, "data", "tdcc_shareholding", "weekly", "*", "*.csv")
EXPECTED_HEADER = ("資料日期", "證券代號", "持股分級", "人數", "股數", "占集保庫存數比例%")


def parse_raw() -> tuple[dict, dict, list]:
    """回傳 (key -> value, key -> 首見檔名, 每檔摘要)。"""
    raw: dict[tuple, tuple] = {}
    src: dict[tuple, str] = {}
    per_file = []
    conflicts = []
    for path in sorted(glob.glob(RAW_GLOB)):
        name = os.path.basename(path)
        # TDCC opendata 端點吐 UTF-8 with BOM;utf-8-sig 才讀得到第一個欄名
        with open(path, encoding="utf-8-sig", newline="") as fh:
            rdr = csv.reader(fh)
            header = tuple(next(rdr))
            rows = [r for r in rdr if r]
        widths = collections.Counter(len(r) for r in rows)
        dates = collections.Counter(r[0] for r in rows)
        tiers = sorted({int(r[2]) for r in rows})
        per_file.append(
            dict(file=name, header_ok=(header == EXPECTED_HEADER), n_rows=len(rows),
                 widths=dict(widths), dates=dict(dates), tiers=tiers,
                 n_codes=len({r[1].strip() for r in rows}))
        )
        seen_in_file = set()
        for r in rows:
            d = f"{r[0][:4]}-{r[0][4:6]}-{r[0][6:]}"
            key = (d, r[1].strip(), int(r[2]))
            val = (int(r[3].replace(",", "")), int(r[4].replace(",", "")), float(r[5]))
            if key in seen_in_file:
                conflicts.append(("WITHIN-FILE DUP", name, key))
            seen_in_file.add(key)
            if key in raw and raw[key] != val:
                conflicts.append(("CROSS-FILE CONFLICT", key, raw[key], src[key], val, name))
            raw[key] = val
            src[key] = name
    return raw, per_file, conflicts


def load_pg() -> dict[tuple, tuple]:
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tf:
        out = tf.name
    sql = (
        "\\copy (SELECT data_date, company_code, holding_tier, num_holders, num_shares, "
        f"pct_of_outstanding FROM tdcc_shareholding) TO '{out}' CSV HEADER"
    )
    subprocess.run(["psql", "-h", "localhost", "-p", "5432", "-d", "quantlib", "-c", sql],
                   check=True, stdout=subprocess.DEVNULL)
    pg = {}
    with open(out, newline="") as fh:
        for r in csv.DictReader(fh):
            pg[(r["data_date"], r["company_code"], int(r["holding_tier"]))] = (
                int(r["num_holders"]), int(r["num_shares"]), float(r["pct_of_outstanding"]))
    os.unlink(out)
    return pg


def main() -> int:
    raw, per_file, conflicts = parse_raw()
    print(f"raw files                : {len(per_file)}")
    bad_header = [f["file"] for f in per_file if not f["header_ok"]]
    print(f"header signature drift   : {len(bad_header)} {bad_header[:5]}")
    widths = collections.Counter(w for f in per_file for w in f["widths"])
    print(f"column-count variants    : {dict(widths)}  (期望 {{6: n_files}})")
    multi_date = [f["file"] for f in per_file if len(f["dates"]) != 1]
    print(f"files with >1 data_date  : {len(multi_date)} {multi_date}")
    tier_sets = {tuple(f["tiers"]) for f in per_file}
    print(f"tier value sets          : {tier_sets}")
    print(f"raw key conflicts        : {len(conflicts)} {conflicts[:3]}")

    pg = load_pg()
    print(f"\nraw distinct keys        : {len(raw)}")
    print(f"pg  rows                 : {len(pg)}")
    only_raw = set(raw) - set(pg)
    only_db = set(pg) - set(raw)
    print(f"only in raw (漏入庫)      : {len(only_raw)} {sorted(only_raw)[:3]}")
    print(f"only in db  (無來源)      : {len(only_db)} {sorted(only_db)[:3]}")

    mismatch = collections.defaultdict(list)
    for k in set(raw) & set(pg):
        r, p = raw[k], pg[k]
        if r[0] != p[0]:
            mismatch["num_holders"].append((k, r[0], p[0]))
        if r[1] != p[1]:
            mismatch["num_shares"].append((k, r[1], p[1]))
        if abs(r[2] - p[2]) > 1e-9:
            mismatch["pct_of_outstanding"].append((k, r[2], p[2]))
    for col in ("num_holders", "num_shares", "pct_of_outstanding"):
        lst = mismatch[col]
        print(f"cell mismatch {col:<20}: {len(lst)} {lst[:3]}")

    ok = (not bad_header and not multi_date and not conflicts
          and not only_raw and not only_db and not any(mismatch.values()))
    print("\nRESULT:", "PASS — 原始檔與 PG 逐格一致" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
