"""A-margin_transactions #4 — TPEx 尾三欄(券限額 / 資券相抵 / 備註)的版型斷代。

TPEx 原始檔的標頭 20 欄全史零漂移(見 01_header_scan.py),但**資料列**的尾三欄
換過兩次位置。判定工具是交易所自己印在檔裡的一致性式子:

    券使用率(%) = 券餘額 / 券限額 × 100

拿 idx9(資限額)與 idx17 分別去套,誰命中就是誰。結果(2026-07-22):

  era A  2007-01-02 ~ 2007-05-31   idx17=券限額 idx18=(空) idx19=資券相抵
         idx17 命中率 100%  → reader 讀 values(18) 拿到空字串 → 資券相抵 全被寫成 0
  era B  2007-06-01 ~ 2008-09-26   idx17=資券相抵 idx18=(空) idx19=備註  ← 券限額根本沒印
         idx17 命中率 0%,idx9 命中率 100%(= 真正的券限額其實等於資限額)
         → reader 把「資券相抵」寫進 short_quota,offsetting 寫 0
  era C  2008-09-30 ~ 迄今          idx17=券限額 idx18=資券相抵 idx19=備註(與標頭一致)
         → reader 正確

旁證(不必信使用率式子也能判死):era B 有 65,821 / 135,679 列(48.5%)出現
「券餘額 > 券限額」——餘額不可能超過自己的限額。era A 與 2009+ 皆為 0 / 0.25%。

run: uv run --project research python docs/data_audit/scripts/A-margin_transactions/04_tpex_tail_eras.py
不依賴 cache.duckdb。
"""

import collections
import csv
import glob
import io
import os
import re

BASE = "data/margin_transactions/tpex"
STOCK = re.compile(r"^[0-9][0-9A-Z]*$")

# 版型斷代的取樣日(含兩個邊界的前後日)
PROBE_DAYS = ["2007_1_10", "2007_3_5", "2007_4_2", "2007_5_31", "2007_6_1", "2007_6_5",
              "2007_12_5", "2008_3_5", "2008_9_5", "2008_9_26", "2008_9_30", "2008_10_6",
              "2009_3_5", "2013_3_15", "2015_1_5", "2020_7_15", "2026_7_15"]


def fields(line: str):
    line = line.rstrip("\r")
    if '""' in line and ',""' not in line:
        return None
    try:
        return next(csv.reader(io.StringIO(line.replace("=", ""))))
    except Exception:
        return None


def f2(s: str):
    s = s.strip().replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def probe(day: str) -> None:
    y = day.split("_")[0]
    p = f"{BASE}/{y}/{day}.csv"
    if not os.path.exists(p) or os.path.getsize(p) == 0:
        print(f"{day}: (no file / sentinel)")
        return
    hits: collections.Counter[int] = collections.Counter()
    n = 0
    for line in open(p, "rb").read().decode("big5hkscs", "replace").split("\n"):
        r = fields(line)
        if not r or len(r) != 20 or not STOCK.match(r[0].strip()):
            continue
        bal, rate = f2(r[14]), f2(r[16])
        if bal is None or rate is None or rate == 0:
            continue
        n += 1
        for idx in (9, 17):
            q = f2(r[idx])
            if q and abs(bal / q * 100 - rate) < 0.05:
                hits[idx] += 1
    print(f"{day}: n={n:4d}  idx9(資限額) hit={hits[9]:4d}   idx17 hit={hits[17]:4d}")


def scan_boundaries() -> None:
    """逐檔判斷「idx18 空 + idx19 數字」(era A) / 「idx19 為備註空白」(era B) / 正常(era C)。"""
    def isnum(s: str) -> bool:
        s = s.strip().replace(",", "")
        return s != "" and re.fullmatch(r"-?\d+", s) is not None

    prev = None
    for year in sorted(os.listdir(BASE)):
        files = sorted(glob.glob(os.path.join(BASE, year, "*.csv")),
                       key=lambda p: [int(x) for x in os.path.basename(p)[:-4].split("_")])
        for f in files:
            if os.path.getsize(f) == 0:
                continue
            c: collections.Counter[str] = collections.Counter()
            for line in open(f, "rb").read().decode("big5hkscs", "replace").split("\n"):
                r = fields(line)
                if not r or len(r) != 20 or not STOCK.match(r[0].strip()):
                    continue
                a, b = r[18].strip(), r[19].strip()
                if a == "" and isnum(b):
                    c["A_tail_swapped"] += 1
                elif isnum(a):
                    c["C_normal"] += 1
                elif a == "" and b != "" and not isnum(b):
                    c["B_no_short_quota"] += 1
            if not c:
                continue
            kind = c.most_common(1)[0][0]
            if kind != prev:
                print("  layout ->", os.path.basename(f), kind, dict(c))
                prev = kind


if __name__ == "__main__":
    print("=== 券使用率 = 券餘額/X*100 命中率(X=idx9 或 idx17)")
    for d in PROBE_DAYS:
        probe(d)
    print()
    print("=== 尾三欄版型切換點(全 tpex 檔逐日掃描)")
    scan_boundaries()
