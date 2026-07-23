"""A-margin_transactions #3 — 全量原始檔 vs PostgreSQL 逐欄比對(16,468 檔 / 8,341,265 列)。

用一份獨立解析器(自己讀 CSV、依標頭中文名定位欄位)重讀全部原始檔,
再與 margin_transactions 的同一 (market, date, company_code) 逐欄比對 14 個欄位。
同時模擬「現行 Scala reader 的列過濾條件」會收下幾列,用來抓出
『DB 現在有、但用今天的程式重跑會消失』的列。

輸出:docs/data_audit/scripts/A-margin_transactions/recon.csv,每檔一列:
  n_raw / n_db            獨立解析 vs DB 的股票檔數
  scala_ok / n_data_rows  現行 reader 收下的列數 vs 實際資料列數(差額 = 會被靜靜丟掉的列)
  only_raw / only_db      單邊獨有的股票代號數
  d_<欄位>                該欄位不符的列數

結果(2026-07-22):
  only_raw = only_db = 0(每一列都對得上,無漏匯、無多匯)
  14 欄中 12 欄零差異;差異只有兩袋:
    d_offsetting  12,944 列(tpex 2007-01-02~2007-03-30)—— 見 findings BUG#1
    d_margin_quota   400 列(tpex 2008-01-25)—— 原始檔當天 idx8/idx9 對調,
                     reader 的 Try(values(9)).getOrElse(values(8)) 接住了,DB 是對的
  現行 reader 會丟掉 466,788 列(tpex 2011-01-03~2014-10-30,代號有補空白)—— 見 findings BUG#3

run: uv run --project research python docs/data_audit/scripts/A-margin_transactions/03_recon.py
需要 PostgreSQL(psql),不需要 cache.duckdb。約 6 分鐘。
"""

import collections
import csv
import glob
import io
import os
import re
import subprocess
import sys

BASE = "data/margin_transactions"
OUT = os.path.dirname(os.path.abspath(__file__))
STOCK = re.compile(r"^[0-9][0-9A-Z]*$")

# 原始檔索引 -> DB 欄位(順序同 NAMES)
TWSE_MAP = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
TPEX_MAP = [1, 3, 4, 5, 2, 6, 9, 12, 11, 13, 10, 14, 17, 18]
NAMES = ["company_name", "margin_purchase", "margin_sales", "cash_redemption",
         "margin_bal_prev", "margin_bal_day", "margin_quota", "short_covering",
         "short_sale", "stock_redemption", "short_bal_prev", "short_bal_day",
         "short_quota", "offsetting"]


def parse_file(path: str, market: str):
    """獨立解析。回傳 (code -> 欄位陣列, 現行 Scala filter 會收下的列數, 資料列數, 欄數異常列數)。"""
    want = 17 if market == "twse" else 20
    raw = open(path, "rb").read().decode("big5hkscs", "replace")
    out, scala_ok, total, badlen = {}, 0, 0, 0
    for rawline in raw.split("\n"):
        line = rawline.rstrip("\r")
        if not line.strip():
            continue
        if '""' in line and ',""' not in line:      # QuantlibCSVReader.scala:21
            continue
        line = line.replace("=", "")                 # QuantlibCSVReader.scala:23
        try:
            r = next(csv.reader(io.StringIO(line)))
        except Exception:
            continue
        if not r or not STOCK.match(r[0].strip()):
            continue
        total += 1
        if len(r) != want:
            badlen += 1
            continue
        # 現行 reader 是對「未正規化的 head」做 matches(),代號補空白就會被丟掉
        if STOCK.match(r[0]):
            scala_ok += 1
        out[r[0].strip()] = [x.replace(" ", "").replace(",", "") for x in r]
    return out, scala_ok, total, badlen


def to_i(s: str):
    try:
        return int(s)
    except Exception:
        return None


def load_db():
    q = ("COPY (SELECT market,date,company_code,company_name,margin_purchase,margin_sales,"
         "cash_redemption,margin_balance_of_previous_day,margin_balance_of_the_day,margin_quota,"
         "short_covering,short_sale,stock_redemption,short_balance_of_previous_day,"
         "short_balance_of_the_day,short_quota,offsetting_of_margin_purchases_and_short_sales "
         "FROM margin_transactions) TO STDOUT WITH (FORMAT csv)")
    p = subprocess.Popen(["psql", "-h", "localhost", "-p", "5432", "-d", "quantlib", "-c", q],
                         stdout=subprocess.PIPE, text=True, bufsize=1 << 20)
    db = collections.defaultdict(dict)
    for row in csv.reader(p.stdout):
        db[(row[0], row[1])][row[2]] = row[3:]
    p.wait()
    return db


def main() -> None:
    db = load_db()
    print("db days:", len(db), file=sys.stderr)
    rows_out = []
    for market in ("twse", "tpex"):
        mapping = TWSE_MAP if market == "twse" else TPEX_MAP
        for year in sorted(os.listdir(os.path.join(BASE, market))):
            for f in sorted(glob.glob(os.path.join(BASE, market, year, "*.csv"))):
                y, m, d = (int(x) for x in os.path.basename(f)[:-4].split("_"))
                date = f"{y}-{m:02d}-{d:02d}"
                size = os.path.getsize(f)
                raw, scala_ok, total, badlen = ({}, 0, 0, 0) if size == 0 else parse_file(f, market)
                dbd = db.get((market, date), {})
                diffs: collections.Counter[str] = collections.Counter()
                for code in set(raw) & set(dbd):
                    v, g = raw[code], dbd[code]
                    for i, (ri, nm) in enumerate(zip(mapping, NAMES)):
                        rv = v[ri]
                        if nm == "company_name":
                            if rv != g[0]:
                                diffs[nm] += 1
                            continue
                        iv = to_i(rv)
                        if iv is None:
                            iv = 0
                        if iv != int(g[i]):
                            diffs[nm] += 1
                rows_out.append(dict(market=market, date=date, size=size, n_raw=len(raw),
                                     n_db=len(dbd), scala_ok=scala_ok, n_data_rows=total,
                                     badlen=badlen, only_raw=len(set(raw) - set(dbd)),
                                     only_db=len(set(dbd) - set(raw)),
                                     **{("d_" + k): v for k, v in diffs.items()}))
    keys = (["market", "date", "size", "n_raw", "n_db", "scala_ok", "n_data_rows", "badlen",
             "only_raw", "only_db"] + ["d_" + n for n in NAMES])
    path = os.path.join(OUT, "recon.csv")
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        for r in rows_out:
            w.writerow({k: r.get(k, 0) for k in keys})
    print("wrote", path, len(rows_out))

    # 摘要
    tot: collections.Counter[str] = collections.Counter()
    drop = 0
    for r in rows_out:
        drop += r["n_data_rows"] - r["scala_ok"]
        for n in NAMES:
            tot["d_" + n] += r.get("d_" + n, 0)
        tot["only_raw"] += r["only_raw"]
        tot["only_db"] += r["only_db"]
    print("column mismatches:", {k: v for k, v in tot.items() if v})
    print("rows current Scala reader would silently drop:", drop)


if __name__ == "__main__":
    main()
