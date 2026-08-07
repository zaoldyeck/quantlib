"""還原價漏還原的公司行動掃描:單日還原報酬超過當年漲跌幅上限 = 有事沒還原。

## 為什麼這條判準不需要任何參數

台股個股單日漲跌幅有**制度上限**:2015-06-01 之前 7%,之後 10%(交易所公告,界限
不是選擇)。真實交易產生的單日報酬**不可能**超過它。所以:

    |還原後單日報酬| > 當日制度上限  ⇒  那一天有公司行動沒被還原

沒有門檻要挑、沒有敏感度要掃——**物理界限直接當偵測器**。這正是專案鐵律說的
「拿不出證據就不要加那個參數,把決定權留給不變式或物理界限」。

## 這支是怎麼被找出來的(2026-08-06)

EV58 最小量測試的階段 B agent 在做 `5468@2011-08-11` 時回報:「這檔暴漲是假的,
+97.8% 裡有 +77 個百分點是減資換股比例」。自驗確認,而且比單一案子嚴重得多:

    capital_reduction 最早紀錄:twse 2012、tpex 2013
    raw 封存最早:  twse 2020、tpex 2020

**蒸餾期 2008-2011 完全沒有減資資料**,而那是樣本的 65%(7% 漲跌幅世代)。減資沒被
還原 ⇒ 還原價機械跳空 ⇒ 被算成暴漲。這與 CLAUDE.md 已記載的「純配股幽靈崩跌」
(FC1)是同一個機制的另一面。

## 兩種處置,先量再修

- **能補資料**:去抓 2008-2011 的減資公告,補進 cache 後重建還原價。
- **補不到**:受影響的「檔 × 日」必須從樣本剔除,而不是假裝資料是對的。
  剔除的依據就是本掃描的輸出——**具名、可重跑、可稽核**。

Run:
  uv run --project . python -m quantlib.audits.09_unadjusted_action_scan
  uv run --project . python -m quantlib.audits.09_unadjusted_action_scan --market tpex
依賴 cache: 是。
"""
from __future__ import annotations

import argparse
from datetime import date as Date

import polars as pl

from quantlib import prices
from quantlib.apex import data

C = "company_code"
#: 台股個股單日漲跌幅上限與其生效日——交易所公告,**界限不是選擇**。
LIMIT_CHANGE = Date(2015, 6, 1)
LIMIT_BEFORE, LIMIT_AFTER = 0.07, 0.10
#: 容差:還原因子與收盤價各自四捨五入,合成報酬的誤差可達千分之幾。取 0.005
#: (半個百分點)讓正常的進位誤差不會誤報,而任何真實的公司行動跳空都遠大於它。
TOL = 0.005


def scan(con, market: str, start: str, end: str) -> pl.DataFrame:
    px = prices.fetch_adjusted_panel(con, start, end, market=market)
    if px.is_empty():
        return px
    # **母體必須與樣本一致**:EV57 只收四碼普通股。不過濾的話,權證(五碼)會主宰
    # 結果——它們本來就沒有相同的漲跌幅約束,實測 tpex 2008 的極端違反全是 7xxxx。
    # 第一版沒過濾,148,779 筆裡絕大多數是權證,那個數字沒有意義。
    d = (px.select([C, "date", "close"]).filter(pl.col("close") > 0)
         .filter(pl.col(C).str.contains(r"^[0-9]{4}$")).sort([C, "date"])
         .with_columns((pl.col("close") / pl.col("close").shift(1).over(C) - 1).alias("r"))
         .drop_nulls("r"))
    return (d.with_columns(
        pl.when(pl.col("date") < LIMIT_CHANGE).then(pl.lit(LIMIT_BEFORE))
          .otherwise(pl.lit(LIMIT_AFTER)).alias("limit"))
        .filter(pl.col("r").abs() > pl.col("limit") + TOL)
        .with_columns((pl.col("r").abs() - pl.col("limit")).alias("excess"))
        .select([C, "date", "close", "r", "limit", "excess"])
        .sort("excess", descending=True))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", default=None, help="twse | tpex(預設兩個都掃)")
    ap.add_argument("--start", default="2008-01-01")
    ap.add_argument("--end", default="2021-12-31")
    ap.add_argument("--top", type=int, default=15)
    a = ap.parse_args()

    con = data.connect()
    frames = []
    for m in ([a.market] if a.market else ("twse", "tpex")):
        f = scan(con, m, a.start, a.end)
        if not f.is_empty():
            frames.append(f.with_columns(pl.lit(m).alias("market")))
    if not frames:
        print("✓ 無單日還原報酬超過制度上限者")
        return
    bad = pl.concat(frames)

    print(f"✗ {bad.height:,} 個「檔 × 日」的還原報酬超過當日制度上限"
          f"({a.start} ~ {a.end})\n")
    per_year = (bad.with_columns(pl.col("date").dt.year().alias("y"))
                .group_by(["y", "market"]).agg(pl.len().alias("n"),
                                               pl.col(C).n_unique().alias("codes"))
                .sort(["y", "market"]))
    print("逐年逐市場:")
    print(per_year)
    print(f"\n最極端 {a.top} 筆:")
    print(bad.head(a.top))
    print("\n判準沒有可調參數:單日漲跌幅上限是交易所制度,真實交易不可能超過它。"
          "\n超過即代表該日有公司行動未被還原——減資、面額變更、合併換股、股票分割等。")


if __name__ == "__main__":
    main()
