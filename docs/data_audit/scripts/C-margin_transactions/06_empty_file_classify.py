"""C-margin_transactions ⑥:用原始檔的「無資料回應形態」分辨休市與漏抓。

02/05 的證人法在 2001~2004(無任何其他表覆蓋)失效。這裡改用第三種、完全獨立
的證據:**交易所對「該日無資料」的回應長什麼樣**。實測 TWSE 有四種小檔:

| bytes | 內容 | 語義 |
|---|---|---|
| 818 | 完整標頭 + 彙總空表(舊版) | 交易所明說「這天沒有」→ 休市 |
| 26 | Big5「很抱歉,沒有符合條件的資料!」(新版) | 同上 |
| 0 | 0-byte sentinel(爬蟲自寫) | 同上 |
| **4** | 只有 CRLF/空回應 | **抓失敗**,不是休市 |

TPEx 側則是 871 / 993 / 994 bytes 的「共0筆」表頭回應,無法單靠大小分辨,
故 TPEx 仍以證人法為準(02_gaps.py)。

本腳本對每個「DB 無列」的日期印出原始檔大小分類,讓「休市 vs 漏抓」有第三方證據。

Run: PYTHONPATH=<repo> uv run --project research python docs/data_audit/scripts/C-margin_transactions/06_empty_file_classify.py
"""
from __future__ import annotations

from collections import Counter
from datetime import timedelta

import duckdb

from research import paths

RAW = paths.RAW / "margin_transactions"
#: TWSE 的「交易所明說無資料」回應大小(實測全 corpus 只有這幾種小檔)
TWSE_NODATA = {0, 26, 818}


def path_of(market: str, d) -> "object":
    return RAW / market / str(d.year) / f"{d.year}_{d.month}_{d.day}.csv"


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    for market in ("twse", "tpex"):
        dmin, dmax = con.execute(
            "SELECT min(date), max(date) FROM margin_transactions WHERE market=?",
            [market]).fetchone()
        have = {r[0] for r in con.execute(
            "SELECT DISTINCT date FROM margin_transactions WHERE market=?",
            [market]).fetchall()}
        sizes = Counter()
        suspects = []
        d = dmin
        while d <= dmax:
            if d.weekday() < 5 and d not in have:
                p = path_of(market, d)
                s = p.stat().st_size if p.exists() else -1
                sizes[s] += 1
                if market == "twse" and s not in TWSE_NODATA:
                    suspects.append((d, s))
            d += timedelta(days=1)
        print(f"=== [{market}] {dmin}~{dmax} 「DB 無列的平日」原始檔大小分布 ===")
        for s, n in sorted(sizes.items()):
            tag = ("(檔案不存在)" if s < 0 else
                   "(交易所明說無資料 → 休市)" if market == "twse" and s in TWSE_NODATA
                   else "(需人工判讀)")
            print(f"  {s:>6} bytes × {n:>4} 天 {tag}")
        if market == "twse":
            print(f"  → 非『交易所明說無資料』形態的日子({len(suspects)} 天,判為漏抓):")
            for d, s in suspects:
                print(f"     {d} {d.strftime('%a')} size={s}")
        d = dmin

    print("\n=== 全 corpus 的 4-byte(空回應)檔清單 vs DB 是否有列 ===")
    for market in ("twse", "tpex"):
        for p in sorted((RAW / market).rglob("*.csv")):
            if p.stat().st_size == 4:
                y, m, dd = p.stem.split("_")
                n = con.execute(
                    "SELECT count(*) FROM margin_transactions WHERE market=? AND date=?",
                    [market, f"{y}-{int(m):02d}-{int(dd):02d}"]).fetchone()[0]
                print(f"  {market} {y}-{int(m):02d}-{int(dd):02d} db_rows={n}")


if __name__ == "__main__":
    main()
