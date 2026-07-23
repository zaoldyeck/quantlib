"""C-sbl_borrowing ③:原始檔「檔名日期 vs 內容自報日期」全量核對。

為什麼要查:`TradingReader.readSblBorrowing`(src/main/scala/reader/TradingReader.scala:834)
的日期**只從檔名解析**(`LocalDate.of(y,m,d)`),完全不看檔案自己的日期欄——
所以只要交易所在某次請求回了別天的資料,那筆資料就會被靜靜掛到錯誤的日期上。
`research/audits/04_cross_verify.py` 只掃 `data/index`,從未掃過 sbl。

判讀:
  * TWSE CSV(Big5)第一列 `"105年04月06日 信用額度總量管制餘額表"` → 民國日期。
  * TPEx JSON 頂層 `"date":"20230608"` 與 tables[0].date `"112/06/08"` → 兩者都取。

Run: PYTHONPATH=<repo> uv run --project research python docs/data_audit/scripts/C-sbl_borrowing/03_content_date_verify.py
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re

import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))))
RAW = os.path.join(REPO, "data", "sbl_borrowing")

ROC = re.compile(r"(\d+)年(\d+)月(\d+)日")
FN = re.compile(r"^(\d{4})_(\d+)_(\d+)\.csv$")


def content_date(path: str) -> tuple[dt.date | None, str]:
    """回傳 (檔案自報日期, 形態標籤)。"""
    sz = os.path.getsize(path)
    if sz == 0:
        return None, "0-byte sentinel"
    with open(path, "rb") as fh:
        head = fh.read(400)
    if head[:1] == b"{":
        try:
            obj = json.loads(open(path, encoding="utf-8").read())
        except Exception:  # noqa: BLE001
            return None, "json-parse-fail"
        d = obj.get("date")
        n = 0
        tables = obj.get("tables") or []
        if tables:
            n = tables[0].get("totalCount", 0)
        if not d:
            return None, f"json-no-date(totalCount={n})"
        return dt.date(int(d[:4]), int(d[4:6]), int(d[6:8])), f"json(totalCount={n})"
    txt = head.decode("big5-hkscs", errors="replace")
    m = ROC.search(txt)
    if not m:
        return None, f"csv-no-roc-date({sz}B)"
    y, mo, d = (int(m.group(i)) for i in (1, 2, 3))
    return dt.date(y + 1911, mo, d), "csv"


def main() -> None:
    pd.set_option("display.width", 250)
    pd.set_option("display.max_rows", 400)
    rows = []
    for market in ("twse", "tpex"):
        base = os.path.join(RAW, market)
        for year in sorted(os.listdir(base)):
            ydir = os.path.join(base, year)
            if not os.path.isdir(ydir):
                continue
            for name in sorted(os.listdir(ydir)):
                m = FN.match(name)
                if not m:
                    continue
                fnd = dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                path = os.path.join(ydir, name)
                cd, kind = content_date(path)
                rows.append((market, fnd, cd, kind, os.path.getsize(path), path))
    df = pd.DataFrame(rows, columns=["market", "fn_date", "content_date", "kind",
                                     "bytes", "path"])
    print(f"掃描 {len(df)} 個檔案")
    print(df.groupby(["market", "kind"]).size().to_string())

    print("\n=== 檔名日期 != 內容日期 ===")
    bad = df[(df.content_date.notna()) & (df.fn_date != df.content_date)].copy()
    bad["lag_days"] = [(c - f).days for f, c in zip(bad.fn_date, bad.content_date)]
    bad["weekday"] = [f.strftime("%a") for f in bad.fn_date]
    print(f"總數: {len(bad)}")
    print(bad[["market", "fn_date", "weekday", "content_date", "lag_days", "bytes"]]
          .sort_values(["market", "fn_date"]).to_string() if len(bad) else "(無)")

    print("\n=== 內容日期撞號(同一內容日期被存成多個檔名)===")
    ok = df[df.content_date.notna()]
    dup = (ok.groupby(["market", "content_date"]).agg(
        n=("fn_date", "size"), fns=("fn_date", lambda s: sorted(s)))
        .query("n > 1"))
    print(f"總數: {len(dup)}")
    print(dup.to_string() if len(dup) else "(無)")

    print("\n=== 0-byte sentinel 一覽(逐年)===")
    s = df[df.kind == "0-byte sentinel"]
    print(f"總數: {len(s)}")
    print(s.assign(y=s.fn_date.map(lambda d: d.year),
                   wd=s.fn_date.map(lambda d: d.strftime("%a")))
          .groupby(["market", "y", "wd"]).size().to_string() if len(s) else "(無)")
    print("\n平日(週一~五)的 0-byte sentinel:")
    sw = s[s.fn_date.map(lambda d: d.weekday() < 5)]
    print(sw[["market", "fn_date"]].assign(
        wd=sw.fn_date.map(lambda d: d.strftime("%a"))).to_string() if len(sw) else "(無)")

    print("\n=== 其他非正常形態 ===")
    other = df[~df.kind.isin(["csv", "0-byte sentinel"]) & ~df.kind.str.startswith("json(")]
    print(other.to_string() if len(other) else "(無)")

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "content_date_scan.csv")
    df.to_csv(out, index=False)
    print(f"\n完整結果寫到 {out}")


if __name__ == "__main__":
    main()
