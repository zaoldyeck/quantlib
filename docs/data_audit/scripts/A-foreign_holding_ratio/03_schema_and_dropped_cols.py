"""A-foreign_holding_ratio 佐證腳本 3:欄位版型掃描 + 未入庫欄位盤點。

回答三件事:
1. TWSE CSV 全史有幾種標頭簽章?index 0-8 的語意在各時代是否一致(欄位錯位風險)?
2. TPEx JSON 全史有幾種 fields 簽章?有沒有列寬 < 9 會被 reader 丟掉?
3. 原始檔有、schema 沒接的欄位各有多少非空(白白丟掉的資訊)?

Run: python3 docs/data_audit/scripts/A-foreign_holding_ratio/03_schema_and_dropped_cols.py
不依賴 cache.duckdb;直接讀 data/foreign_holding_ratio/。
"""
import csv
import io
import json
import os
from collections import Counter

BASE = "data/foreign_holding_ratio"


def twse_header_signatures():
    hdrs = Counter()
    span = {}
    for yr in sorted(os.listdir(f"{BASE}/twse")):
        for f in sorted(os.listdir(f"{BASE}/twse/{yr}")):
            raw = open(f"{BASE}/twse/{yr}/{f}", "rb").read()
            if not raw:
                continue
            rows = list(csv.reader(io.StringIO(raw.decode("big5hkscs", "replace").replace("=", ""))))
            if len(rows) < 2:
                continue
            h = tuple(x.strip() for x in rows[1])
            hdrs[h] += 1
            span.setdefault(h, [f[:-4], f[:-4]])[1] = f[:-4]
    for h, c in hdrs.most_common():
        print(f"{c} files  span={span[h]}")
        for i, name in enumerate(h):
            print(f"    [{i}] {name}")


def tpex_field_signatures():
    fields = Counter()
    widths = Counter()
    for yr in sorted(os.listdir(f"{BASE}/tpex")):
        for f in sorted(os.listdir(f"{BASE}/tpex/{yr}")):
            raw = open(f"{BASE}/tpex/{yr}/{f}", "rb").read()
            if not raw:
                continue
            t = json.loads(raw.decode())["tables"][0]
            fields[tuple(t.get("fields", []))] += 1
            for r in t["data"]:
                widths[len(r)] += 1
    print("row widths:", dict(widths), " (<9 would be dropped by the reader)")
    for k, v in fields.most_common():
        print(v, "x", k)


def dropped_columns():
    lim = Counter()
    reason = Counter()
    for yr in sorted(os.listdir(f"{BASE}/twse")):
        for f in sorted(os.listdir(f"{BASE}/twse/{yr}")):
            raw = open(f"{BASE}/twse/{yr}/{f}", "rb").read()
            if not raw:
                continue
            rows = list(csv.reader(io.StringIO(raw.decode("big5hkscs", "replace").replace("=", ""))))
            if len(rows) < 3 or len(rows[1]) < 13:
                continue  # only the post-2009 layout carries 陸資上限 at [9]
            for r in rows[2:]:
                if len(r) < 13 or not r[0].strip() or not r[0].strip()[0].isdigit():
                    continue
                lim["same" if r[8].strip() == r[9].strip() else "DIFF"] += 1
                reason[r[10].strip() or "<empty>"] += 1
    print("TWSE 共用上限[8] vs 陸資上限[9]:", dict(lim))
    print("TWSE 與前日異動原因[10]:", reason.most_common(6))
    note = Counter()
    for yr in sorted(os.listdir(f"{BASE}/tpex")):
        for f in sorted(os.listdir(f"{BASE}/tpex/{yr}")):
            raw = open(f"{BASE}/tpex/{yr}/{f}", "rb").read()
            if not raw:
                continue
            for r in json.loads(raw.decode())["tables"][0]["data"]:
                note[r[9].strip() or "<empty>"] += 1
    print("TPEx 備註[9]:", note.most_common(8))


if __name__ == "__main__":
    print("=== TWSE header signatures ===")
    twse_header_signatures()
    print("\n=== TPEx JSON field signatures ===")
    tpex_field_signatures()
    print("\n=== dropped columns ===")
    dropped_columns()
