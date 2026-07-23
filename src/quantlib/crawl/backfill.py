"""日頻源歷史回補:逐交易日 fetch_day + 封存 + upsert cache,resumable。

用於 2026 才加、歷史未回補的源(insider_holding 等)——update.py 的日頻刷新只從
「最新已收日」往前補、且空表 guard 不從零下載,故歷史缺料要用本工具全範圍回補。

resumable:以 `archive.has_raw` 判該 (源,市場,日) 是否已抓過(fetch_day 內部已封存
raw),已抓即跳過——中斷後重跑只補未抓的。禮貌 sleep 避免 MOPS anti-bot。

Run: uv run python -m quantlib.crawl.backfill --source insider_holding --from 2007-01-01
依賴 cache:是(upsert 直寫)。
"""
from __future__ import annotations

import argparse
import importlib
import time
from datetime import date as Date, timedelta

from quantlib.crawl import archive
from quantlib.crawl.sink import Sink
from quantlib.data_calendar import is_trading_day, latest_complete_trading_day

#: 各源封存的副檔名(has_raw 判已抓用;預設 csv)。
_RAW_EXT = {"insider_holding": "html"}


def backfill(source_name: str, start: Date, end: Date | None = None,
             sleep: float = 0.3) -> None:
    mod = importlib.import_module(f"quantlib.crawl.sources.{source_name}")
    end = end or latest_complete_trading_day()
    dcol = getattr(mod, "DATE_COL", "date")
    ext = _RAW_EXT.get(source_name, "csv")
    markets = getattr(mod, "MARKETS", ("twse", "tpex"))
    fetched = skipped = rows = errs = 0
    print(f"[backfill] {source_name} {start} ~ {end}(resumable via has_raw)", flush=True)
    # 並發友善:fetch(網路,無鎖)與 sleep 期間不持 cache 寫鎖;只在有資料 upsert 時
    # **每日短開短關** Sink(瞬間持鎖 ms 級)→ 讀者(db.connect 讀重試)在 fetch 空檔即可
    # 取得鎖。長寫者不再一把鎖住 cache 數小時。
    for market in markets:
        d = start
        while d <= end:
            if is_trading_day(d):
                if archive.has_raw(source_name, market, d, ext=ext):
                    skipped += 1
                else:
                    try:
                        df = mod.fetch_day(market, d)   # 網路 + 內部先封存 raw;無 cache 鎖
                        fetched += 1
                        if df is not None and not df.is_empty():
                            with Sink() as sink:        # 短開短關:upsert 完立即釋放寫鎖
                                rows += sink.upsert_day(mod.TABLE, market, d, df, date_col=dcol)
                    except Exception as exc:  # noqa: BLE001 - 單日失敗不擋整源
                        errs += 1
                        if errs <= 5:
                            print(f"  ⚠ {market} {d}: {type(exc).__name__}: {str(exc)[:60]}", flush=True)
                    time.sleep(sleep)
                    if fetched % 500 == 0:
                        print(f"  ...{market} {d} 抓 {fetched} 跳 {skipped} 累計 {rows} 列 ({errs} 錯)", flush=True)
            d += timedelta(days=1)
    print(f"[backfill] 完成 {source_name}:抓 {fetched} 日、跳 {skipped} 已存、upsert {rows} 列、{errs} 錯", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="日頻源歷史回補(resumable)")
    ap.add_argument("--source", required=True, help="源模組名(如 insider_holding)")
    ap.add_argument("--from", dest="start", required=True, help="起始日 YYYY-MM-DD")
    ap.add_argument("--to", dest="end", default=None, help="結束日(預設最新齊備日)")
    ap.add_argument("--sleep", type=float, default=0.3, help="每日 fetch 間隔秒(禮貌)")
    args = ap.parse_args()
    backfill(args.source, Date.fromisoformat(args.start),
             Date.fromisoformat(args.end) if args.end else None, args.sleep)


if __name__ == "__main__":
    main()
