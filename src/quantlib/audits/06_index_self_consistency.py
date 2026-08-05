"""指數自洽稽核:同一列的收盤價與漲跌點數必須對得起來。

## 為什麼這條檢查抓得到別人抓不到的錯

TWSE 的指數檔每一列自帶三個彼此相依的數字:`close`、`change`、`change_pct`。
於是有一條**不需要任何外部權威**就成立的恆等式:

    close(今日) == close(昨日) + change(今日)

違反它,代表那一列的原始資料本身壞掉——不是 parser 的錯,重建 cache 修不掉。
實測樣本(2026-08-06 由 EV58 年代語境卡的支援工作追出來):

    2016-05-25  close 8396.20
    2016-05-26  close 7811.18  change +49.17  change_pct +0.63%

8396.20 + 49.17 = 8445.37,不是 7811.18;而 7811.18 相對前日是 −6.97%,不是 +0.63%。
`data/market_index/twse/2016/2016_5_26.csv` 封存的原始檔裡就已經是 7,811.18,
屬 CLAUDE.md 已知病徵「TWSE partial/stale daily publish」。

## 為什麼要在意

`market_index` 是 **regime 分類器的輸入**(EV57 的樣本分層、任何以大盤為基準的分析)。
一個壞掉的收盤價會同時製造一根假暴跌與一根假暴漲(錯值與相鄰日各湊一對),而那對
假行情長得跟真的股災一模一樣——下游會據此把某個月標成「崩跌 regime」,或讓年代
語境卡收錄一場沒發生過的股災,再讓數十位研究員去考掘它。

## 判準

`|close - (prev_close + change)| > TOL` 即違反。`TOL` 取 0.02:指數本身取到小數第二位,
兩個報價各自的四捨五入誤差合計不超過 0.01,留一倍餘裕。**這不是可調參數**——放寬它
只會讓真正的錯誤混進來,收緊它會把正常的進位誤差當成錯。

停牌、休市不影響:恆等式比較的是**資料裡相鄰的兩列**,不是日曆相鄰。指數改制或
基期調整會整段違反,那種情況會以連續大量違反的形態出現,與零星壞值可分辨。

Run:
  uv run --project . python -m quantlib.audits.06_index_self_consistency
  uv run --project . python -m quantlib.audits.06_index_self_consistency --name 發行量加權股價指數
依賴 cache: 是。
"""
from __future__ import annotations

import argparse

import polars as pl

from quantlib.apex import data

#: 指數報價取到小數第二位,兩個報價的進位誤差合計 ≤0.01,留一倍餘裕。
#: 放寬會讓真錯混進來,收緊會把正常進位誤差當錯——兩邊都不是可調的偏好。
TOL = 0.02


def audit(con, market: str | None = None, name: str | None = None) -> pl.DataFrame:
    where = ["change IS NOT NULL", "close IS NOT NULL"]
    if market:
        where.append(f"market = '{market}'")
    if name:
        where.append(f"name = '{name}'")
    df = con.sql(f"SELECT market, name, date, close, change, change_pct "
                 f"FROM market_index WHERE {' AND '.join(where)} "
                 f"ORDER BY market, name, date").pl()
    if df.is_empty():
        return df
    return (df.with_columns(
        pl.col("close").shift(1).over(["market", "name"]).alias("prev_close"))
        .with_columns((pl.col("prev_close") + pl.col("change")).alias("expected"))
        .with_columns((pl.col("close") - pl.col("expected")).abs().alias("gap"))
        .filter(pl.col("prev_close").is_not_null() & (pl.col("gap") > TOL))
        .with_columns(
            # 收盤與前收的實際變化率,對照該列自報的 change_pct——兩者背離時,
            # 幾乎總是 close 那一格壞掉(change 與 change_pct 互相自洽)。
            ((pl.col("close") / pl.col("prev_close") - 1) * 100).round(2).alias("implied_pct"))
        .select(["market", "name", "date", "prev_close", "close", "expected",
                 "change", "change_pct", "implied_pct", "gap"]))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", default=None)
    ap.add_argument("--name", default=None, help="只查某個指數(如 發行量加權股價指數)")
    ap.add_argument("--top", type=int, default=25)
    a = ap.parse_args()

    con = data.connect()
    bad = audit(con, a.market, a.name)
    if bad.is_empty():
        print("✓ 所有指數列的 close == prev_close + change,無自洽違反")
        return

    per = (bad.group_by(["market", "name"]).agg(pl.len().alias("n"))
           .sort("n", descending=True))
    total = con.sql("SELECT count(*) FROM market_index WHERE change IS NOT NULL").pl().item()
    print(f"✗ {bad.height} 列違反自洽(全表 {total:,} 列,{bad.height / total:.3%})\n")
    print("按指數:")
    print(per.head(20))
    print(f"\n最嚴重 {a.top} 列(gap 由大到小):")
    print(bad.sort("gap", descending=True).head(a.top))
    print("\n處置:違反者的原始封存檔本身即錯值(rebuild 修不掉),需重抓該日 raw 再重建。"
          "\n     `implied_pct` 與 `change_pct` 背離者,壞的是 close 那一格。")


if __name__ == "__main__":
    main()
