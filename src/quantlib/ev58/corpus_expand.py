"""EV58 期別語境卡:當年標題語料的批次擴充(corpus expand)。

為什麼要這支
------------
``archive_probe`` 一次只處理一個站台一個日期;而詞彙考掘要的是「一個年代」的語料
密度——單日幾十條標題長不出可信的詞頻,更看不出哪些詞是當年通用、哪些只是某天的
偶然用字。本模組把「站台 × 日期」的矩陣一次跑完並落檔快取,讓語料可以逐步長大、
中斷後零損失重跑。

三種語料的取得成本與性質不同,故分開:

* ``portal`` — 財經入口網首頁的 Wayback 快照。**單位成本最低、台股用語密度最高**
  (一頁 30~350 條當日標題),且連出去的文章 URL 多半自帶刊出日(如 cnyes 的
  ``/Content/YYYYMMDD/``),日期可被第三者驗證。
* ``epochtimes`` — 逐篇標題,涵蓋面廣但財經佔比低。
* ``ptt`` — 散戶側用語(當年鄉民怎麼講),與媒體用語常常不同,是「今天的詞搜不到
  當年消息」的另一個來源。

不需要 cache.duckdb(純網路 + 讀寫 ``_probe_cache``)。

用法
----
    uv run --project . python -m quantlib.ev58.corpus_expand portal --lo 2008-01-01 --hi 2010-12-31 --every 45
    uv run --project . python -m quantlib.ev58.corpus_expand ptt    --lo 2008-01-01 --hi 2010-12-31 --every 60
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import sys
from datetime import date as _date, timedelta
from pathlib import Path

from quantlib import paths
from quantlib.ev58 import archive_probe as ap

SNAP_DIR = paths.OUT / "ev58_news" / "_era_brief" / "_probe_cache" / "snaps"

# 2008-2010 年活著的台股財經入口網(首頁快照 = 當日標題牆)。
PORTALS: dict[str, str] = {
    "cnyes": "http://www.cnyes.com/",
    "yahoo": "http://tw.stock.yahoo.com/",
    "udnmoney": "http://money.udn.com/",
    "moneydj": "http://www.moneydj.com/",
    "chinatimes": "http://news.chinatimes.com/",
    "wearn": "http://www.wearn.com/",
    "nownews": "http://www.nownews.com/finance/",
}


def _dates(lo: str, hi: str, every: int) -> list[_date]:
    a, b = _date.fromisoformat(lo), _date.fromisoformat(hi)
    out, cur = [], a
    while cur <= b:
        out.append(cur)
        cur += timedelta(days=every)
    return out


def _portal_one(job: tuple[str, str, _date]) -> str:
    key, url, d = job
    SNAP_DIR.mkdir(parents=True, exist_ok=True)
    cache = SNAP_DIR / f"{key}_{d:%Y-%m-%d}.tsv"
    if cache.exists() and cache.stat().st_size > 0:
        return f"skip {key} {d}"
    try:
        rows = ap.snapshot_headlines(url, d)
    except Exception as exc:  # noqa: BLE001 - 單格失敗不該中斷整個矩陣
        cache.write_text("")
        return f"ERR  {key} {d} {type(exc).__name__}"
    cache.write_text("".join(f"{t}\t{h}\n" for t, h in rows))
    return f"ok   {key} {d} n={len(rows)}"


def _ptt_one(d: _date) -> str:
    try:
        rows = ap.ptt_titles("Stock", d, pages=6)
    except Exception as exc:  # noqa: BLE001
        return f"ERR  ptt {d} {type(exc).__name__}"
    return f"ok   ptt {d} n={len(rows)}"


def main(argv: list[str] | None = None) -> int:
    a_ = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    a_.add_argument("cmd", choices=["portal", "ptt", "epochtimes"])
    a_.add_argument("--lo", default="2008-01-01")
    a_.add_argument("--hi", default="2010-12-31")
    a_.add_argument("--every", type=int, default=45, help="每 N 天取一個樣本日")
    a_.add_argument("--sites", default=None, help="portal: 以逗號分隔的站台鍵")
    a_.add_argument("--workers", type=int, default=4, help="Wayback 對併發敏感,預設保守")
    a = a_.parse_args(argv)

    days = _dates(a.lo, a.hi, a.every)
    if a.cmd == "portal":
        keys = a.sites.split(",") if a.sites else list(PORTALS)
        jobs = [(k, PORTALS[k], d) for k in keys for d in days]
        with cf.ThreadPoolExecutor(max_workers=a.workers) as pool:
            for line in pool.map(_portal_one, jobs):
                print(line, flush=True)
        return 0
    if a.cmd == "ptt":
        for d in days:  # PTT 有頻率限制,序列跑
            print(_ptt_one(d), flush=True)
        return 0
    with cf.ThreadPoolExecutor(max_workers=a.workers) as pool:
        for line in pool.map(
            lambda d: f"ok epochtimes {d} n={len(ap.titles('epochtimes', d))}", days
        ):
            print(line, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
