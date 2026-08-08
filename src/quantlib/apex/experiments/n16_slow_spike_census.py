"""N16 — 慢速暴漲股普查(川湖類):250 日 +200% 的結構重估股,S 蓋到多少?

**預註冊**:`ledger/batches.md` §N16(2026-08-08),預測 P1-P3 先於執行。

**與 F05 的分工**:F05(2026-07-10)已對**快速暴漲**(60 日 +80%)做過 case-control,
結論:第一前兆 = 高已實現波動,但轉化雙敗(高波是雙尾放大器)。本批普查 F05 的死角
——**慢速暴漲**(250 交易日內最高收盤 ≥ +200%),即 N15 川湖案的母體:
1. 這類有多少?與快速暴漲重疊多少(重疊 = S 理論上可經爆發段捕捉)?
2. 依進場日營收狀態四象限分類(eligible 截面百分位,門檻 70):
   HH 高年增高加速 / HL 高年增低加速(川湖持續型)/ LH 轉機加速型 / LL 無基本面。
3. S 的覆蓋漏斗:曾在池 → 最佳名次 ≤5 → 實際交易;cfo 閘全擋的比例。

**PIT**:前瞻極值用 shift(-1) 起算(不含當日),分類特徵取 T 當日(當日收盤已知)。
視窗不完整的起點一律剔除(slow 需 T ≤ 資料尾端 −370 日曆天),避免右尾截斷低估。

依賴 cache: 是(prep_cached)。
run: uv run --project . python -m quantlib.apex.experiments.n16_slow_spike_census
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data
from quantlib.apex.strategy_s import WREL, prep_cached, score_pool

C = "company_code"
H_FAST, TH_FAST, COOL_FAST, PAD_FAST = 60, float(np.log(1.8)), 120, 90
H_SLOW, TH_SLOW, COOL_SLOW, PAD_SLOW = 250, float(np.log(3.0)), 250, 370
PCT_TH = 70.0                  # 四象限門檻(預註冊)
TRADES = paths.OUT / "apex" / "n07_exit_census" / "trades.parquet"
OUT = paths.OUT / "apex" / "n16_slow_spike"


def build_base(panel: pl.DataFrame, feat: pl.DataFrame, elig: pl.DataFrame) -> pl.DataFrame:
    """eligible 逐日截面 + 前瞻極值 + 營收狀態百分位。"""
    lg = pl.col("close").log()
    p = (panel.select(["date", C, "close"]).sort([C, "date"])
         .with_columns(lg.alias("lg"))
         .with_columns(pl.col("lg").shift(-1).over(C).alias("lg1"))
         .with_columns([
             (pl.col("lg1").reverse().rolling_max(H_FAST, min_periods=1).reverse()
              .over(C) - pl.col("lg")).alias("mg60"),
             (pl.col("lg1").reverse().rolling_max(H_SLOW, min_periods=1).reverse()
              .over(C) - pl.col("lg")).alias("mg250"),
             pl.int_range(pl.len()).over(C).alias("bar"),
         ]))
    base = (p.join(elig.filter(pl.col("eligible")).select(["date", C, "adv20"]),
                   on=["date", C], how="semi")
            .join(elig.select(["date", C, "adv20"]), on=["date", C], how="left")
            .join(feat.select(["date", C, "rev_yoy", "rev_yoy_accel"]),
                  on=["date", C], how="left"))
    return base.with_columns([
        (pl.col("rev_yoy").rank() / pl.col("rev_yoy").count() * 100)
        .over("date").alias("pct_yoy"),
        (pl.col("rev_yoy_accel").rank() / pl.col("rev_yoy_accel").count() * 100)
        .over("date").alias("pct_accel"),
    ])


def episodes(base: pl.DataFrame, mg: str, th: float, cool: int, last_ok) -> pl.DataFrame:
    """貪婪冷卻抽取:同碼取合格日,與前一個保留起點相距 ≥ cool 根 bar 才收。"""
    q = (base.filter((pl.col(mg) >= th) & (pl.col("date") <= last_ok))
         .select(["date", C, "bar", mg, "pct_yoy", "pct_accel", "adv20", "close"])
         .sort([C, "bar"]))
    rows = []
    for _, g in q.group_by(C, maintain_order=True):
        bars = g["bar"].to_numpy()
        keep, last = [], -10**9
        for i, b in enumerate(bars):
            if b >= last + cool:
                keep.append(i)
                last = b
        rows.append(g[keep])
    ep = pl.concat(rows) if rows else q.head(0)
    return (ep.rename({"date": "T", mg: "lg_gain"})
            .with_columns((pl.col("lg_gain").exp() - 1).alias("gain"))
            .with_row_index("eid"))


def quadrant() -> pl.Expr:
    return (pl.when(pl.col("pct_yoy").is_null() | pl.col("pct_accel").is_null())
            .then(pl.lit("NA 無營收資料"))
            .when((pl.col("pct_yoy") >= PCT_TH) & (pl.col("pct_accel") >= PCT_TH))
            .then(pl.lit("HH 高年增高加速"))
            .when(pl.col("pct_yoy") >= PCT_TH).then(pl.lit("HL 持續型(川湖)"))
            .when(pl.col("pct_accel") >= PCT_TH).then(pl.lit("LH 轉機加速型"))
            .otherwise(pl.lit("LL 無基本面")).alias("類"))


def coverage(ep: pl.DataFrame, pool: pl.DataFrame, pre: pl.DataFrame,
             trades: pl.DataFrame, pad_days: int) -> pl.DataFrame:
    """每個 episode 在 [T, T+pad] 內:池日數 / 最佳名次 / 資格日數(cfo 前)/ 是否被 S 交易。"""
    w = ep.select(["eid", C, "T"]).with_columns(
        pl.col("T").dt.offset_by(f"{pad_days}d").alias("Tend"))
    pj = (w.join(pool.select(["date", C, "rk"]), on=C)
          .filter((pl.col("date") >= pl.col("T")) & (pl.col("date") <= pl.col("Tend")))
          .group_by("eid").agg([pl.len().alias("池日"), pl.col("rk").min().alias("最佳名次")]))
    qj = (w.join(pre.select(["date", C]), on=C)
          .filter((pl.col("date") >= pl.col("T")) & (pl.col("date") <= pl.col("Tend")))
          .group_by("eid").agg(pl.len().alias("資格日")))
    tj = (w.join(trades.select(["entry_date", C]), on=C)
          .filter((pl.col("entry_date") >= pl.col("T"))
                  & (pl.col("entry_date") <= pl.col("Tend")))
          .group_by("eid").agg(pl.len().alias("S筆數")))
    return (ep.join(pj, on="eid", how="left").join(qj, on="eid", how="left")
            .join(tj, on="eid", how="left")
            .with_columns([pl.col("池日").fill_null(0), pl.col("資格日").fill_null(0),
                           pl.col("S筆數").fill_null(0)]))


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    last = panel["date"].max()
    base = build_base(panel, feat, elig)

    slow = episodes(base, "mg250", TH_SLOW, COOL_SLOW,
                    last - pl.duration(days=PAD_SLOW))
    fast = episodes(base, "mg60", TH_FAST, COOL_FAST,
                    last - pl.duration(days=PAD_FAST))
    slow = slow.with_columns(quadrant())
    fast = fast.with_columns(quadrant())
    slow.write_parquet(OUT / "slow_episodes.parquet")
    fast.write_parquet(OUT / "fast_episodes.parquet")

    print("=" * 88)
    print(f"【1】census:慢速(250 日 ≥+200%)= {slow.height} 起 | "
          f"快速(60 日 ≥+80%,F05 同定義)= {fast.height} 起")
    print("=" * 88)
    yr = (slow.group_by(pl.col("T").dt.year().alias("年")).agg(pl.len().alias("慢速"))
          .join(fast.group_by(pl.col("T").dt.year().alias("年")).agg(pl.len().alias("快速")),
                on="年", how="full", coalesce=True).sort("年"))
    with pl.Config(tbl_rows=-1):
        print(yr)
    ks = slow.filter(pl.col(C) == "2059")
    print(f"\n2059 川湖的慢速 episode:{ks.select(['T', 'gain', '類']).to_dicts()}")

    # 慢速 × 快速重疊:慢速窗內有無快速爆發段(有 = S 理論上可經爆發捕捉)
    ov = (slow.select(["eid", C, "T"])
          .with_columns(pl.col("T").dt.offset_by(f"{PAD_SLOW}d").alias("Tend"))
          .join(fast.select([C, pl.col("T").alias("Tf")]), on=C)
          .filter((pl.col("Tf") >= pl.col("T")) & (pl.col("Tf") <= pl.col("Tend")))
          .select("eid").unique())
    slow = slow.with_columns(pl.col("eid").is_in(ov["eid"]).alias("含快速爆發"))
    print(f"慢速中含快速爆發段者:{ov.height}/{slow.height} = {ov.height / slow.height:.1%}")

    print("\n" + "=" * 88)
    print("【2】慢速暴漲的營收狀態四象限(T 日,eligible 截面百分位,門檻 70)")
    print("=" * 88)
    pool = (score_pool(feat, elig)
            .with_columns(pl.col("score").rank("ordinal", descending=True)
                          .over("date").alias("rk")))
    pre = (feat.filter(pl.col("rev_fresh_days") <= 7)
           .join(elig.filter(pl.col("eligible")).select(["date", C]),
                 on=["date", C], how="semi")
           .drop_nulls(subset=list(WREL))
           .filter(pl.all_horizontal([pl.col(c).is_finite() for c in WREL])))
    trades = pl.read_parquet(TRADES)
    cov = coverage(slow, pool, pre, trades, PAD_SLOW)
    cov.write_parquet(OUT / "slow_coverage.parquet")

    tab = (cov.group_by("類").agg([
        pl.len().alias("n"),
        (pl.len() / slow.height * 100).round(1).alias("佔比%"),
        (pl.col("gain").median() * 100).round(0).alias("中位漲幅%"),
        (pl.col("含快速爆發").mean() * 100).round(0).alias("含爆發%"),
        ((pl.col("池日") > 0).mean() * 100).round(0).alias("曾在池%"),
        ((pl.col("最佳名次") <= 5).mean() * 100).round(0).alias("名次≤5%"),
        ((pl.col("S筆數") > 0).mean() * 100).round(0).alias("S交易%"),
        (((pl.col("資格日") > 0) & (pl.col("池日") == 0)).mean() * 100)
        .round(0).alias("cfo全擋%"),
    ]).sort("n", descending=True))
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=170):
        print(tab)

    print("\n--- HL 持續型(川湖類)漲幅前 20(供肉眼核對)---")
    with pl.Config(tbl_rows=-1, tbl_width_chars=150, float_precision=2):
        print(cov.filter(pl.col("類") == "HL 持續型(川湖)")
              .sort("gain", descending=True).head(20)
              .select([C, "T", "gain", "pct_yoy", "pct_accel",
                       "池日", "最佳名次", "S筆數", "含快速爆發"]))

    print("\n" + "=" * 88)
    print("【3】對照:快速暴漲(F05 母體)的四象限與 S 覆蓋")
    print("=" * 88)
    covf = coverage(fast, pool, pre, trades, PAD_FAST)
    tabf = (covf.group_by("類").agg([
        pl.len().alias("n"), (pl.len() / fast.height * 100).round(1).alias("佔比%"),
        (pl.col("gain").median() * 100).round(0).alias("中位漲幅%"),
        ((pl.col("池日") > 0).mean() * 100).round(0).alias("曾在池%"),
        ((pl.col("S筆數") > 0).mean() * 100).round(0).alias("S交易%"),
    ]).sort("n", descending=True))
    with pl.Config(tbl_rows=-1, tbl_width_chars=150):
        print(tabf)
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
