"""A-margin_transactions #1 — 標頭欄序漂移掃描(全 16,468 檔)。

問題:融資融券原始檔的欄位有沒有在某一年無聲增減/換位?
做法:把每個檔的標頭列(TWSE「股票代號…」/ TPEx「代號…」)壓成簽章字串,統計相異簽章。

結果(2026-07-22):
  twse 2 個簽章 —— 欄序/欄數完全相同,只有 2024-10 起改名
       (股票代號→代號、限額→次一營業日限額);
  tpex 1 個簽章 —— 全史零漂移。
→ 標頭本身不會漂,但「標頭不漂 ≠ 資料不漂」:見 04_tpex_tail_eras.py,
  TPEx 2007-06~2008-09 標頭仍寫「券限額」,實際資料卻換成了「資券相抵」。

run: uv run --project . python docs/data_audit/scripts/A-margin_transactions/01_header_scan.py
不依賴 cache.duckdb。
"""

import collections
import csv
import glob
import io
import os

BASE = "data/margin_transactions"


def fields(line: str):
    """複製 util/QuantlibCSVReader.scala 的兩條前處理:跳過含 `""` 但無 `,""` 的列、剝掉 `=`。"""
    line = line.rstrip("\r")
    if '""' in line and ',""' not in line:
        return None
    try:
        return next(csv.reader(io.StringIO(line.replace("=", ""))))
    except Exception:
        return None


def main() -> None:
    for market, key in (("twse", "股票代號"), ("tpex", "代號")):
        sig: collections.Counter[str] = collections.Counter()
        first: dict[str, str] = {}
        for year in sorted(os.listdir(os.path.join(BASE, market))):
            for p in sorted(glob.glob(os.path.join(BASE, market, year, "*.csv"))):
                if os.path.getsize(p) == 0:
                    continue
                for line in open(p, "rb").read().decode("big5hkscs", "replace").split("\n"):
                    r = fields(line)
                    if not r:
                        continue
                    h = [x.strip() for x in r]
                    if h and (h[0] == key or (market == "twse" and h[0] == "代號" and len(h) > 14)):
                        s = "|".join(h)
                        sig[s] += 1
                        first.setdefault(s, os.path.basename(p))
                        break
        print("===", market, "distinct header signatures:", len(sig))
        for s, c in sig.most_common():
            print(f"   n={c:6d} first={first[s]}  {s}")


if __name__ == "__main__":
    main()
