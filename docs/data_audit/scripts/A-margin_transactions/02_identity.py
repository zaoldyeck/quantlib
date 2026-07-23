"""A-margin_transactions #2 — 用「餘額恆等式」獨立驗證欄位對應(不看標頭、不呼叫受測程式)。

恆等式(融資/融券各一條,交易所原始檔必然成立):
    前日餘額 + 買進 - 賣出 - 現金償還 = 今日餘額            (融資)
    前券餘額 + 券賣 - 券買 - 券償     = 券餘額               (融券,方向相反)

只要恆等式在某組索引上 100% 成立,那組索引就是真正的
(prev, buy, sell, redeem, today);這是不依賴中文標頭的欄位定位法,
也就同時證明 TradingReader.readMarginTransactions 的映射對不對。

被測索引(= TradingReader 用的索引):
  twse: 融資 (5,2,3,4,6)  融券 (11,9,8,10,12)
  tpex: 融資 (2,3,4,5,6)  融券 (10,11,12,13,14)

結果(2026-07-22,每年隨機抽 6 檔):
  twse 88,577 列 × 2 條恆等式,0 不符;tpex 49,792 列 × 2 條,0 不符。

run: uv run --project research python docs/data_audit/scripts/A-margin_transactions/02_identity.py
不依賴 cache.duckdb。
"""

import collections
import csv
import glob
import io
import os
import random
import re

BASE = "data/margin_transactions"
STOCK = re.compile(r"^[0-9][0-9A-Z]*$")


def num(s: str):
    s = s.strip().replace(",", "").replace(" ", "")
    if s in ("", "--", "-"):
        return None
    try:
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return None


def rows(path: str, market: str):
    raw = open(path, "rb").read().decode("big5hkscs", "replace")
    want = 17 if market == "twse" else 20
    out = []
    for r in csv.reader(io.StringIO(raw)):
        if len(r) != want:
            continue
        if not STOCK.match(r[0].strip().replace('"', "").replace(" ", "")):
            continue
        out.append([x.replace(" ", "").replace(",", "") for x in r])
    return out


def check(market: str, years) -> None:
    tot: collections.Counter[str] = collections.Counter()
    bad = []
    for y in years:
        files = [f for f in sorted(glob.glob(os.path.join(BASE, market, str(y), "*.csv")))
                 if os.path.getsize(f) > 0]
        random.seed(y)
        for f in random.sample(files, min(6, len(files))):
            for v in rows(f, market):
                if market == "twse":
                    m = (num(v[5]), num(v[2]), num(v[3]), num(v[4]), num(v[6]))
                    s = (num(v[11]), num(v[9]), num(v[8]), num(v[10]), num(v[12]))
                else:
                    m = (num(v[2]), num(v[3]), num(v[4]), num(v[5]), num(v[6]))
                    s = (num(v[10]), num(v[11]), num(v[12]), num(v[13]), num(v[14]))
                for tag, (p, b, sl, rd, t) in (("margin", m), ("short", s)):
                    if None in (p, b, sl, rd, t):
                        tot[tag + "_null"] += 1
                        continue
                    tot[tag + "_n"] += 1
                    if p + b - sl - rd != t:
                        tot[tag + "_mismatch"] += 1
                        if len(bad) < 5:
                            bad.append((os.path.basename(f), tag, v[0], p, b, sl, rd, t))
    print(market, dict(tot))
    for b in bad:
        print("   sample mismatch", b)


if __name__ == "__main__":
    check("twse", range(2001, 2027))
    check("tpex", range(2007, 2027))
