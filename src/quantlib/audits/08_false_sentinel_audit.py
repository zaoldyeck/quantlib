"""偽 sentinel 稽核:某源說「這天沒開市」,但同一天別的源都有資料。

## 為什麼這個特別毒

0-byte sentinel 在本專案有雙重身分:它既是「問過了、真的沒東西」的紀錄,**也是我們的
休市日曆**(`quantlib.data_calendar.is_trading_day` 直接讀它——颱風假無法從星期幾推得)。
所以一個寫錯的 sentinel 不只是少一天資料:它會讓那一天在**全系統**眼中不存在。

實測(2026-08-06,由指數自洽稽核順藤摸出來):`daily_quote/twse` 在
**2021-08-18、2025-08-15、2026-04-29、2026-05-28** 四天是 0-byte sentinel,而同一天
`daily_trading_details`(1086~1320 檔)、`margin_transactions`(1075~1271 檔)、
`market_index`(175~272 檔)全都有完整資料。市場明明開著。實測那四天現在重抓,
TWSE 一抓就給(1137 / 1354 / 1361 列)——當年寫下的是**端點暫時性失敗**,不是休市。

後果具體:`daily_quote` 是 `prices.fetch_adjusted_panel` 的來源,**所有 NAV 模擬、
所有回測都經過它**;而那四天同時被交易日曆判為非交易日,於是連「少了一天」都不會
有人發現——缺口與日曆互相背書。

## 判準:兩個證人,而且查的是 **cache 覆蓋**不是 sentinel

某源在某日 **cache 沒有資料**,而**同日 ≥2 個其他日頻源有** ⇒ 該日缺漏。
用兩個證人而非一個,是因為單一證人可能自己就是壞的(實測:指數也曾在無交易的
星期六長出 93-133 列幽靈)。兩個獨立源同時有資料,市場開著就是事實。

**為什麼判準是 cache 覆蓋而不是「有沒有 sentinel」**(這是第一版的錯,當場踩到):
第一版掃 raw 目錄裡的 0-byte 檔。但 raw 一旦被重抓覆蓋,sentinel 就消失了——而
**cache 未必跟著補上**。於是稽核會回報「乾淨」,實際上那幾天仍然不在 cache 裡。
症狀比原本更糟:缺口還在,但偵測器已經看不見它。cache 覆蓋是消費者真正讀到的東西,
它才是該被稽核的對象;sentinel 只降級為「成因說明」。

Run:
  uv run --project . python -m quantlib.audits.08_false_sentinel_audit
  uv run --project . python -m quantlib.audits.08_false_sentinel_audit --fix
依賴 cache: 是。`--fix` 會連外重抓。
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date as Date

from quantlib import paths

#: 日頻源 → (raw 目錄名, 爬蟲模組名, cache 表名)。**三個名字未必相同**,必須逐一
#: 指名而不是靠猜:實測 `market_index` 表由 `crawl.sources.index` 產出、raw 落在
#: `market_index/`;`stock_per_pbr` 表的 raw 目錄叫 `stock_per_pbr_dividend_yield`。
#: 猜錯的代價是重抓時 ModuleNotFoundError,而那發生在稽核已經印完報告之後。
#:
#: **互為證人的必須是獨立端點**:同一個端點拆出來的兩張表一起壞掉是常態,
#: 那樣的「兩個證人」等於一個。
DAILY = {
    "daily_quote": ("daily_quote", "daily_quote", "daily_quote"),
    "daily_trading_details": ("daily_trading_details", "daily_trading_details",
                              "daily_trading_details"),
    "margin_transactions": ("margin_transactions", "margin_transactions",
                            "margin_transactions"),
    "market_index": ("market_index", "index", "market_index"),
    "stock_per_pbr": ("stock_per_pbr_dividend_yield", "stock_per_pbr", "stock_per_pbr"),
}
WITNESSES = 2


def _sentinels(source: str, market: str) -> set[Date]:
    """該源該市場所有 0-byte sentinel 的日期。"""
    out: set[Date] = set()
    base = paths.RAW / source / market
    if not base.exists():
        return out
    for f in base.glob("*/*.csv"):
        if f.stat().st_size != 0:
            continue
        try:
            y, m, d = (int(x) for x in f.stem.split("_")[:3])
            out.add(Date(y, m, d))
        except ValueError:
            continue
    return out


def audit(con, market: str = "twse") -> dict[str, list[Date]]:
    have: dict[str, set[Date]] = {}
    for src, (_, _, tbl) in DAILY.items():
        q = con.sql(f"SELECT DISTINCT date FROM {tbl} WHERE market = '{market}'").pl()
        have[src] = set(q["date"].to_list())

    bad: dict[str, list[Date]] = {}
    for src in DAILY:
        witnesses: dict[Date, int] = defaultdict(int)
        for other, days in have.items():
            if other == src:
                continue
            for d in days:
                witnesses[d] += 1
        sent = _sentinels(DAILY[src][0], market)
        # 起點取**該源自己**的最早一天:各源的歷史深度不同(dtd 與 index 都是 2008
        # 才開始,而 daily_quote 回溯到 2004)。不設起點的話,「該源還沒開始」會被
        # 誤報成上千個缺口,真正的零星缺口就淹沒在裡面——`monthly_coverage` 早就
        # 用同一個做法,這裡漏掉了。
        if not have[src]:
            continue
        since = min(have[src])
        missing = sorted(d for d, n in witnesses.items()
                         if n >= WITNESSES and d >= since and d not in have[src])
        if missing:
            bad[src] = missing
            # sentinel 只是成因之一,不是判準——標出來供處置參考
            bad[f"{src}::sentinel_backed"] = sorted(d for d in missing if d in sent)
    return {k: v for k, v in bad.items() if v}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", default="twse")
    ap.add_argument("--fix", action="store_true", help="重抓偽 sentinel 那幾天並寫回 cache")
    a = ap.parse_args()

    from quantlib.apex import data
    con = data.connect()
    bad = audit(con, a.market)
    if not bad:
        print(f"✓ {a.market}:沒有偽 sentinel(每個休市標記都有 ≥{WITNESSES} 個源同意)")
        return
    for src, days in sorted(bad.items()):
        if src.endswith("::sentinel_backed"):
            print(f"    其中 {len(days)} 天目前仍有 0-byte sentinel(那同時是交易日曆的"
                  f"來源,所以這些天在全系統眼中不存在):"
                  + "、".join(str(d) for d in days[:8]))
            continue
        print(f"✗ {src}/{a.market}:cache 缺 {len(days)} 個交易日 —— "
              + "、".join(str(d) for d in days[:12])
              + (f" …另 {len(days) - 12} 天" if len(days) > 12 else ""))
    print(f"\n這些日子有 ≥{WITNESSES} 個其他日頻源有資料,市場確實開著。")
    if not a.fix:
        print("\n(只稽核。要重抓加 --fix)")
        return

    from importlib import import_module
    from quantlib.crawl.sink import Sink
    con.close()
    sink = Sink()
    try:
        for src, days in sorted(bad.items()):
            if src.endswith("::sentinel_backed"):
                continue
            _, modname, table = DAILY[src]
            mod = import_module(f"quantlib.crawl.sources.{modname}")
            for d in days:
                try:
                    df = mod.fetch_day(a.market, d)      # 內含原始檔原子落地
                except Exception as exc:                 # noqa: BLE001
                    print(f"  {src} {d}: 抓取失敗 {type(exc).__name__}: {exc}")
                    continue
                if df is None or df.is_empty():
                    print(f"  {src} {d}: 端點仍回空——這天可能真的休市,保留 sentinel")
                    continue
                print(f"  {src} {d}: {sink.upsert(table, df, mod.KEY_COLS)} 列")
    finally:
        sink.close()
    print("\n重抓完成。請重跑本稽核確認。")


if __name__ == "__main__":
    main()
