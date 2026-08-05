"""EV58 era-brief 支援:直接向 TWSE 取加權指數日線,並與 cache 對帳。

為什麼不直接信 cache:交易所是事實的唯一權威,`market_index` 只是投影。實測
(2026-08-06)投影會壞——`market_index` 的 TAIEX 收盤在 18 個交易日與官方不符,
且錯值與相鄰日湊成「暴跌後暴漲」的假事件(例:2016-05-26 cache 記 7811.18,
官方 8394.12,於是憑空長出 -7.0% / +8.4% 的一對假單日行情)。era_brief 的
macro_timeline 一旦收錄這種假轉折,下游數十位研究員會去考掘一場沒發生過的股災。

錯誤源頭在**封存的原始檔本身**(`data/market_index/twse/<y>/<y>_<m>_<d>.csv`
內就已經是錯值),不是 parser——屬 CLAUDE.md 已知病徵「TWSE partial/stale
daily publish」。故重建 cache 修不掉,要靠對帳找出來、重抓那幾天。

依賴 cache:是(用來對帳;不需最新資料)。

Run:
    uv run --project . python -m quantlib.evergreen.ev58_taiex_authority \
        --start 2015-06-01 --end 2017-12-31
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
from dataclasses import dataclass
from datetime import date

import polars as pl

from quantlib.db import connect
from quantlib.evergreen.ev58_era_index_pivots import TAIEX, big_days, zigzag

API = "https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date={ym}01&response=json"
UA = {"User-Agent": "Mozilla/5.0 (quantlib ev58 era-brief audit)"}


@dataclass(frozen=True)
class Mismatch:
    date: str
    cache_close: float | None
    official_close: float


def _months(start: date, end: date) -> list[str]:
    out, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append(f"{y}{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _roc_to_iso(s: str) -> str:
    y, m, d = s.split("/")
    return f"{int(y) + 1911:04d}-{int(m):02d}-{int(d):02d}"


def fetch_official(start: date, end: date, pause: float = 1.0) -> pl.DataFrame:
    """逐月抓官方 TAIEX 歷史日線(開高低收)。"""
    rows: list[dict[str, object]] = []
    for ym in _months(start, end):
        req = urllib.request.Request(API.format(ym=ym), headers=UA)
        with urllib.request.urlopen(req, timeout=30) as fh:
            payload = json.load(fh)
        if payload.get("stat") != "OK":
            raise RuntimeError(f"TWSE {ym} 回應非 OK: {payload.get('stat')}")
        for r in payload.get("data", []):
            iso = _roc_to_iso(r[0])
            if start.isoformat() <= iso <= end.isoformat():
                rows.append(
                    {
                        "date": iso,
                        "open": float(r[1].replace(",", "")),
                        "high": float(r[2].replace(",", "")),
                        "low": float(r[3].replace(",", "")),
                        "close": float(r[4].replace(",", "")),
                    }
                )
        time.sleep(pause)  # 官方站點禮貌間隔
    return pl.DataFrame(rows).sort("date")


def reconcile(official: pl.DataFrame, start: str, end: str) -> list[Mismatch]:
    con = connect()
    cache = con.execute(
        """
        SELECT CAST(date AS VARCHAR) AS date, close
        FROM market_index
        WHERE market='twse' AND name=? AND date BETWEEN ? AND ?
        """,
        [TAIEX, start, end],
    ).pl()
    joined = official.join(cache, on="date", how="left", suffix="_cache")
    bad = joined.filter(
        pl.col("close_cache").is_null()
        | ((pl.col("close_cache") - pl.col("close")).abs() > 0.01)
    )
    return [
        Mismatch(r["date"], r["close_cache"], r["close"])
        for r in bad.iter_rows(named=True)
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--threshold", type=float, default=0.03)
    args = ap.parse_args()

    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    off = fetch_official(start, end)
    print(f"# TWSE 官方 TAIEX {args.start}~{args.end}  rows={len(off)}")
    hi = off.filter(pl.col("close") == pl.col("close").max()).row(0, named=True)
    lo = off.filter(pl.col("close") == pl.col("close").min()).row(0, named=True)
    print(
        f"open={off['close'][0]:.2f}({off['date'][0]}) "
        f"close={off['close'][-1]:.2f}({off['date'][-1]}) "
        f"high={hi['close']:.2f}({hi['date']}) low={lo['close']:.2f}({lo['date']})"
    )

    print("\n## cache 對帳(官方為準)")
    bad = reconcile(off, args.start, args.end)
    if not bad:
        print("全數一致")
    for m in bad:
        got = "缺列" if m.cache_close is None else f"{m.cache_close:.2f}"
        print(f"{m.date}  cache={got}  官方={m.official_close:.2f}")

    print(f"\n## zigzag pivots (>= {args.threshold * 100:.0f}%,官方收盤)")
    for p in zigzag(off.select("date", "close"), args.threshold):
        print(f"{p.date}  {p.kind:6s} {p.close:9.2f}  leg {p.move_pct * 100:+6.1f}%")

    print("\n## 單日最大波動 top 20(官方收盤)")
    for r in big_days(off.select("date", "close"), n=20).iter_rows(named=True):
        print(f"{r['date']}  {r['close']:9.2f}  {r['ret'] * 100:+5.2f}%")


if __name__ == "__main__":
    main()
