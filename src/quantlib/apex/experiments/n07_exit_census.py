"""N07 — 出場理由普查 + 營收新鮮度的右尾機率(下一輪該挖哪裡的定位儀)。

**為什麼先做這個**:N06 的診斷欄意外顯示 trailing stop 只決定 0.6% 的出場——也就是
前一輪花力氣調的是一條幾乎不觸發的閘。**不知道哪條規則在管事,就是在瞎調。**
本檔把 S 的每一筆出場按理由歸類、算出各理由的筆數佔比、報酬貢獻與持有天數,
先定位「誰真的在決定 S 的績效」,再決定下一刀往哪切。

同時量第二件事:**進場時的營收新鮮度 → 右尾機率**。S 的池是「營收公布 7 日內」,
但 1 日內與 7 日內是否同質從未量過;若火箭機率隨新鮮度衰減,池門檻或權重就有依據。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n07_exit_census
"""
from __future__ import annotations

import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.strategy_s import prep_cached, run_s_full

C = "company_code"
START = "2015-01-01"
OUT = paths.OUT / "apex" / "n07_exit_census"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    nav, tr = run_s_full(panel, feat, elig, START)
    tr.write_parquet(OUT / "trades.parquet")

    closed = tr.filter(pl.col("exit_reason") != "open")
    print("=" * 82)
    print(f"【1】出場理由普查(已平倉 {closed.height} 筆 / 全部 {tr.height} 筆)")
    print("=" * 82)
    tot = closed.height
    t = (closed.group_by("exit_reason").agg([
        pl.len().alias("n"),
        pl.col("ret_net").mean().alias("平均報酬"),
        pl.col("ret_net").median().alias("中位報酬"),
        (pl.col("ret_net") > 0).mean().alias("勝率"),
        pl.col("days_held").median().alias("中位天數"),
        pl.col("ret_net").sum().alias("報酬總和"),
    ]).with_columns([
        (pl.col("n") / tot).round(4).alias("筆數佔比"),
        (pl.col("報酬總和") / closed["ret_net"].sum()).round(4).alias("報酬貢獻佔比"),
    ]).sort("n", descending=True))
    with pl.Config(tbl_rows=-1, tbl_width_chars=170, float_precision=4):
        print(t.select(["exit_reason", "n", "筆數佔比", "報酬貢獻佔比", "平均報酬",
                        "中位報酬", "勝率", "中位天數"]))

    print("\n" + "=" * 82)
    print("【2】報酬集中度:S 的獲利有多少來自最賺的少數幾筆")
    print("=" * 82)
    r = closed.sort("ret_net", descending=True)["ret_net"].to_numpy()
    pos = r[r > 0]
    for k in (1, 5, 10, 20):
        n = max(1, int(len(r) * k / 100))
        print(f"  最賺的 {k:2d}% ({n:3d} 筆) 佔全部正報酬總和的 "
              f"{r[:n].sum() / pos.sum():6.1%}")
    print(f"  正報酬筆數佔比 {len(pos) / len(r):.1%};"
          f"報酬 ≥ +50% 的筆數 {int((r >= 0.5).sum())}({(r >= 0.5).mean():.1%})")

    print("\n" + "=" * 82)
    print("【3】進場時營收新鮮度 → 結果分佈(池門檻 fresh ≤ 7 是否同質)")
    print("=" * 82)
    fr = feat.select(["date", C, "rev_fresh_days"])
    d = (closed.join(fr, left_on=["entry_date", C], right_on=["date", C], how="left")
         .drop_nulls("rev_fresh_days"))
    cut = d.select("entry_date").unique().sort("entry_date")["entry_date"]
    mid = cut[int(len(cut) * 0.75)]
    for tag, sub in (("IS", d.filter(pl.col("entry_date") <= mid)),
                     ("OOS", d.filter(pl.col("entry_date") > mid))):
        t2 = (sub.group_by("rev_fresh_days").agg([
            pl.len().alias("n"),
            pl.col("ret_net").mean().round(4).alias("平均報酬"),
            (pl.col("ret_net") >= 0.5).mean().round(4).alias("P_報酬≥50%"),
            (pl.col("ret_net") > 0).mean().round(3).alias("勝率"),
        ]).sort("rev_fresh_days"))
        print(f"\n--- {tag} ---")
        with pl.Config(tbl_rows=-1):
            print(t2)
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
