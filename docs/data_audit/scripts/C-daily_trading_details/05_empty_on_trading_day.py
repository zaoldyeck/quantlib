"""C-daily_trading_details / 步驟 5:從原始檔那一側獨立確認缺口。

TWSE/TPEx 對「非交易日」的請求會回空 body,爬蟲把它原樣落檔(TWSE 是 2 bytes 的
CRLF、TPEx 是只有表頭的 512 bytes),而 `Detail.getDatesOfExistFiles` 只要檔案存在
且開頭不是 `<html>` 就當作「這天做完了」→ `Task.pullDailyFiles` 的 `filterNot(existFiles)`
從此永遠跳過。所以:**空檔 × 該日確實開市 = 永久缺漏**。

「該日確實開市」用兩個互相獨立的證人:`research.data_calendar.is_trading_day`
(daily_quote 的 0-byte sentinel 日曆,颱風假也涵蓋)與 cache 內 daily_quote 的列數。

用法:PYTHONPATH=<repo> uv run --project research python \
      docs/data_audit/scripts/C-daily_trading_details/05_empty_on_trading_day.py
"""
from __future__ import annotations

import re
from datetime import date as Date
from pathlib import Path

import duckdb

from research import paths
from research.data_calendar import is_trading_day

EMPTY_MAX_BYTES = 600  # TWSE 空回應 2 bytes;TPEx「只有表頭」約 512 bytes


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    hits = []
    n_empty = 0
    for mkt in ("twse", "tpex"):
        for p in sorted(Path(f"data/daily_trading_details/{mkt}").rglob("*.csv")):
            if p.stat().st_size > EMPTY_MAX_BYTES:
                continue
            m = re.match(r"(\d+)_(\d+)_(\d+)\.csv", p.name)
            if not m:
                continue
            d = Date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            n_empty += 1
            if not is_trading_day(d):
                continue
            n_dtd = con.sql(
                f"SELECT count(*) FROM daily_trading_details WHERE market='{mkt}' AND date=DATE '{d}'"
            ).fetchone()[0]
            if n_dtd:
                continue
            n_q = con.sql(
                f"SELECT count(*) FROM daily_quote WHERE market='{mkt}' AND date=DATE '{d}'"
            ).fetchone()[0]
            hits.append((mkt, d.isoformat(), p.stat().st_size, n_q, str(p)))

    print(f"空檔總數:{n_empty}")
    print(f"其中落在交易日且 DB 無資料者:{len(hits)}")
    for mkt, d, size, n_q, path in hits:
        print(f"  {mkt} {d}  raw={size}B  daily_quote={n_q}  {path}")


if __name__ == "__main__":
    main()
