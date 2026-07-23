"""Phase 3.4 goal-④:S 逐筆 MAE/MFE 分析——止損/止盈位置的分佈證據(John Sweeney 法)。

MAE(Maximum Adverse Excursion)= 持有期間相對進場的最大不利波動;MFE = 最大有利波動。
用途:把「止損放哪/要不要止盈」從拍腦袋變成證據——
- 若贏家很少見 MAE ≤ -X%,則 -X% 絕對停損能砍輸家而少傷贏家(存在分離點);
- 若贏家常先 MAE 破 -X% 再噴,則絕對停損必殺贏家(今晨 s_structure 實測 abs20 全指標劣化,
  本檔給出「為什麼」的分佈層證據);
- 輸家的 MFE 分佈 = 止盈證據:輸家若常先給 +Y% 紙上獲利再死,+Y% 止盈可救。

近似聲明:以**收盤價**相對進場日收盤算 MAE/MFE(與引擎 trail 的收盤峰值基準一致;
盤中極值會更深,但截面比較與分離點判定不受影響)。reuse prep_cached + run_s_full(canonical)。

Run: uv run --project . python -m quantlib.strat_lab.s_maemfe
依賴 cache:是(乾淨世代)。
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.strategy_s import C, DS, prep_cached, run_s_full


def trade_excursions(panel: pl.DataFrame, trades: pl.DataFrame) -> pl.DataFrame:
    """每筆已平倉交易 → (ret_net, days_held, mae, mfe);基準 = 進場日 close。"""
    closed = trades.filter(pl.col("exit_reason") != "open").with_row_index("tid")
    px = panel.select([C, "date", "close"])
    j = (closed.select(["tid", C, "entry_date", "exit_date", "ret_net", "days_held"])
         .join(px, on=C, how="inner")
         .filter((pl.col("date") >= pl.col("entry_date")) & (pl.col("date") <= pl.col("exit_date"))))
    base = (j.filter(pl.col("date") == pl.col("entry_date"))
            .select(["tid", pl.col("close").alias("base")]))
    exc = (j.join(base, on="tid", how="inner")
           .with_columns((pl.col("close") / pl.col("base") - 1).alias("exc"))
           .group_by("tid")
           .agg(pl.col("exc").min().alias("mae"), pl.col("exc").max().alias("mfe"),
                pl.col("ret_net").first(), pl.col("days_held").first()))
    return exc


def _pct(s: pl.Series, qs=(0.05, 0.10, 0.25, 0.50, 0.75, 0.90)) -> str:
    return "  ".join(f"P{int(q*100):02d} {s.quantile(q):+.1%}" for q in qs)


def main() -> None:
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    _, trades = run_s_full(panel, feat, elig, DS)
    exc = trade_excursions(panel, trades)
    win = exc.filter(pl.col("ret_net") > 0)
    los = exc.filter(pl.col("ret_net") <= 0)
    print(f"=== S 逐筆 MAE/MFE(已平倉 {exc.height} 筆:贏 {win.height}/輸 {los.height};收盤近似)===\n")
    print(f"  贏家 MAE 分佈:{_pct(win['mae'])}")
    print(f"  輸家 MAE 分佈:{_pct(los['mae'])}")
    print(f"  贏家 MFE 分佈:{_pct(win['mfe'])}")
    print(f"  輸家 MFE 分佈:{_pct(los['mfe'])}\n")

    print("=== 絕對停損反事實:各停損位會殺掉多少贏家 vs 攔到多少輸家 ===")
    print(f"  {'停損位':>8}{'贏家中槍率':>12}{'輸家攔截率':>12}{'贏家中槍的平均最終報酬':>24}")
    for x in (0.05, 0.10, 0.15, 0.20, 0.25, 0.30):
        wk = win.filter(pl.col("mae") <= -x)
        lk = los.filter(pl.col("mae") <= -x)
        wr = wk.height / max(win.height, 1)
        lr = lk.height / max(los.height, 1)
        wret = wk["ret_net"].mean() if wk.height else float("nan")
        print(f"  {-x:>+7.0%}{wr:>12.1%}{lr:>12.1%}{wret:>+23.1%}")
    print("  判讀:若某位「輸家攔截率 >> 贏家中槍率」且中槍贏家最終報酬不高 = 有分離點;"
          "反之(贏家常先深回檔再噴)= 絕對停損必傷,今晨 abs20 全劣化的分佈根因。\n")

    print("=== 止盈反事實:輸家死前給過多少紙上獲利 ===")
    for y in (0.10, 0.20, 0.30, 0.40, 0.60):
        lsave = los.filter(pl.col("mfe") >= y).height / max(los.height, 1)
        wcap = win.filter(pl.col("ret_net") >= y).height / max(win.height, 1)
        print(f"  +{y:.0%} 止盈:可救輸家 {lsave:.1%}(曾到過此獲利)| 會封頂贏家 {wcap:.1%}(最終報酬≥此)")
    print("  判讀:救到的輸家比例若遠低於被封頂的贏家比例 = 止盈負期望(S 右尾肥,截尾即砍引擎)。")


if __name__ == "__main__":
    main()
