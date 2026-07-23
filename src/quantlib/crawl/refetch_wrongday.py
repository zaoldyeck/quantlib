"""重爬「錯日汙染」檔:內容日≠檔名日的 raw 檔,逐檔重爬修正 raw + cache。

`verify.content_dates` 揪出的檔名日≠內容日檔(某次下載游標錯位,把 A 日資料存到 B 日
檔名),其 cache 列裝著別天的數字(策略讀該日 = 讀到錯值)。修法:對每個錯日檔呼叫源的
`fetch_day(market, 檔名日)`——它重抓、內容日守衛驗證(不對即 SchemaDrift 拒絕)、位元保真
覆蓋 raw;成功即 upsert cache 換掉錯列。TWSE 對歷史日多能回正確資料(實測 spb/dtd 皆取回)。

resumable:content_dates 每次重掃當前 raw,已修的不再列出。禮貌 sleep 避免 anti-bot。

Run: uv run --project . python -m quantlib.crawl.refetch_wrongday
依賴 cache:是(upsert 直寫)。
"""
from __future__ import annotations

import importlib
import time

from quantlib.crawl import parse
from quantlib.crawl.sink import Sink
from quantlib.verify.content_dates import _DAILY_SOURCES, scan_source

#: cache 表名 → 源模組名(市場指數表名與模組名不同)。
_SRC_MODULE = {"market_index": "index"}


def refetch(sleep: float = 0.3) -> dict:
    fixed = still_wrong = nodata = errs = 0
    for source in _DAILY_SOURCES:
        r = scan_source(source)
        if not r["mismatches"]:
            continue
        modname = _SRC_MODULE.get(source, source)
        mod = importlib.import_module(f"quantlib.crawl.sources.{modname}")
        date_col = getattr(mod, "DATE_COL", "date")
        print(f"[refetch] {source}: {len(r['mismatches'])} 個錯日檔")
        for path, fn, ct in r["mismatches"]:
            market = path.split("/")[1]  # <source>/<market>/<year>/<file>
            try:
                df = mod.fetch_day(market, fn)  # 重抓 + 內容日守衛 + 覆蓋 raw
                if df is None or df.is_empty():
                    nodata += 1  # 非交易日/無資料 → raw 已覆蓋為無資料回應,cache 不受汙染
                    print(f"  · {market} {fn}: 無資料(非交易日?),raw 已更新")
                    continue
                with Sink() as sink:  # 短開短關:upsert 換掉該日錯列
                    n = sink.upsert_day(mod.TABLE, market, fn, df, date_col=date_col)
                fixed += 1
                print(f"  ✓ {market} {fn}: 修正 {n} 列(原內容={ct})")
            except parse.SchemaDrift:
                still_wrong += 1
                print(f"  ❌ {market} {fn}: TWSE 仍回非請求日資料,raw 已更新但拒入 cache")
            except Exception as exc:  # noqa: BLE001 - 單檔失敗不擋整批
                errs += 1
                print(f"  ⚠ {market} {fn}: {type(exc).__name__}: {str(exc)[:60]}")
            time.sleep(sleep)
    print(f"\n[refetch] 完成:修正 {fixed}、TWSE 仍錯 {still_wrong}、無資料 {nodata}、其他錯 {errs}")
    return {"fixed": fixed, "still_wrong": still_wrong, "nodata": nodata, "errs": errs}


def sentinel_unrecoverable(min_gap_years: int = 3) -> int:
    """把「端點無法重爬」的錯日 raw 改 0-byte sentinel(只留正確資料)。

    判準:content_dates 揪出的錯日檔,其內容日**遠晚於**檔名日(≥min_gap_years 年)=
    請求舊日期端點回傳最新資料(該日資料源本不存在,如 foreign 2004/2010 TPEx——TPEx
    外資 2011 才有)。這種重爬只會再拿到最新垃圾,唯一正解是 sentinel 標記無資料。
    近距離錯位(可能真能重爬)不動,交給 refetch()。
    """
    from quantlib import paths
    from quantlib.verify.content_dates import _DAILY_SOURCES, scan_source
    n = 0
    for source in _DAILY_SOURCES:
        for path, fn, ct in scan_source(source)["mismatches"]:
            if ct.year - fn.year < min_gap_years:  # 近距離錯位 → 可能可重爬,不 sentinel
                continue
            raw = paths.RAW / path
            if raw.exists() and raw.stat().st_size > 0:
                raw.write_bytes(b"")
                n += 1
    print(f"[sentinel] {n} 個無法重爬的錯日 raw → 0-byte sentinel(標記無資料)")
    return n


if __name__ == "__main__":
    import sys
    if "--sentinel-unrecoverable" in sys.argv:
        sentinel_unrecoverable()
    else:
        refetch()
