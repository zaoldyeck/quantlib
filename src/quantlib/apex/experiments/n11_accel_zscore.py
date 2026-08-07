"""N11 — 營收加速的自身標準化(把「標準化才能比」從出場搬到核心訊號)。

**假設**:S 的頭號因子 `rev_yoy_accel`(月營收 3 個月年增 − 12 個月年增)目前是**截面
比大小**。但一家營收本來就忽上忽下的公司,+5pp 的加速可能只是它的常態雜訊;一家穩定
的公司出現同樣的 +5pp,是罕見事件。**用同一把尺比兩者,和用固定 35% 停損比不同波動的
股票是同一個錯誤**——N04 已量到後者是 8.1σ 到 37σ 的不一致。

作法:對每家公司,用它自己**過去 12 次營收公布**的加速值算離散度,把當期加速除以它,
得到「這次加速對這家公司而言有多罕見」的 z 分數。純屬標準化,不引入任何新資料。

**流程**:先用 N08 的條件篩選(S 池內、火箭機率當量尺)看它有沒有增量,有才跑端到端。
沒有就結案——**不為了「試過了」而燒一輪回測**。

**PIT**:release 序列由 `rev_fresh_days` 的重置點界定(公布間該值逐日遞增,新公布時
回落);離散度只用「含當期在內、往前 12 次」的已公布值,無未來資訊。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n11_accel_zscore
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib import paths
from quantlib.apex import data, metrics
from quantlib.apex.experiments.n08_conditional_factor_screen import (
    CRASH, ROCKET, forward_outcomes, _tstat,
)
from quantlib.apex.strategy_s import WREL, canonical_score, prep_cached, run_s_full

C = "company_code"
FULL = "2015-01-01"
SPLIT = "2021-07-01"
N_REL = 12          # 離散度視窗:過去 12 次營收公布(約一年)
MIN_REL = 6         # 不足 6 次不給 z(不足就 null,不猜)
OUT = paths.OUT / "apex" / "n11_accel_z"


def accel_z(feat: pl.DataFrame) -> pl.DataFrame:
    """(date, company_code, accel_z) — 當期加速 ÷ 該公司過去 12 次公布的加速標準差。"""
    f = (feat.select(["date", C, "rev_fresh_days", "rev_yoy_accel"])
         .drop_nulls(["rev_fresh_days", "rev_yoy_accel"])
         .sort([C, "date"]))
    # 新公布 = fresh_days 相對前一交易日下降(公布間逐日遞增)
    f = f.with_columns(
        (pl.col("rev_fresh_days") < pl.col("rev_fresh_days").shift(1).over(C))
        .fill_null(True).alias("is_new"))
    rel = (f.filter("is_new").sort([C, "date"])
           .with_columns([
               pl.col("rev_yoy_accel").rolling_std(N_REL, min_periods=MIN_REL)
               .over(C).alias("sd"),
               pl.col("rev_yoy_accel").rolling_mean(N_REL, min_periods=MIN_REL)
               .over(C).alias("mu"),
           ])
           .with_columns(
               pl.when(pl.col("sd") > 0)
               .then((pl.col("rev_yoy_accel") - pl.col("mu")) / pl.col("sd"))
               .otherwise(None).alias("accel_z"))
           .select([C, "date", "accel_z"]).drop_nulls("accel_z"))
    # 攤回每日:每次公布的 z 沿用到下一次公布
    return (f.select(["date", C]).sort("date")
            .join_asof(rel.sort("date"), on="date", by=C, strategy="backward")
            .drop_nulls("accel_z").sort([C, "date"]))


def screen(pool: pl.DataFrame, col: str, tag: str) -> None:
    d = (pool.drop_nulls([col, "maxgain60", "fwd21"]).filter(pl.col(col).is_finite())
         .filter(pl.len().over("date") >= 10))
    d = d.with_columns(((pl.col(col).rank("ordinal") / pl.len()).over("date") * 5)
                       .ceil().clip(1, 5).cast(pl.Int32).alias("q"))
    hi, lo = d.filter(pl.col("q") == 5), d.filter(pl.col("q") == 1)
    p_hi, p_lo = float((hi["maxgain60"] >= ROCKET).mean()), float((lo["maxgain60"] >= ROCKET).mean())
    c_hi, c_lo = float((hi["maxdraw60"] <= CRASH).mean()), float((lo["maxdraw60"] <= CRASH).mean())
    ic = (d.group_by("date").agg(
        pl.corr(pl.col(col).rank(), pl.col("fwd21").rank()).alias("ic"))["ic"].to_numpy())
    lift = p_hi / p_lo if p_lo > 0 else float("nan")
    clift = c_hi / c_lo if c_lo > 0 else float("nan")
    print(f"  {tag:4s} {col:16s} n={d.height:>7,}  火箭 {p_hi:.4f}/{p_lo:.4f}={lift:5.2f}x  "
          f"崩跌 {c_hi:.4f}/{c_lo:.4f}={clift:5.2f}x  賠率={lift / clift:5.2f}  "
          f"IC={np.nanmean(ic):+.4f} t={_tstat(ic):+5.2f}")


def _row(label: str, nav, trades) -> dict:
    st = metrics.summarize(nav, trades)
    return {"arm": label, "cagr": st["cagr"], "sharpe": st["sharpe"],
            "sortino": st["sortino"], "mdd": st["mdd"], "calmar": st["calmar"],
            "n_trades": st.get("n_trades", 0)}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    z = accel_z(feat)
    print(f"accel_z 覆蓋 {z.height:,} 列 / {z[C].n_unique():,} 檔")

    from quantlib.apex.strategy_s import score_pool
    fo = forward_outcomes(panel)
    days = fo.select("date").unique().sort("date")["date"].to_list()
    pool = (score_pool(feat, elig)
            .filter(pl.col("date") >= pl.lit(FULL).str.to_date())
            .filter(pl.col("date") <= days[-61])
            .join(fo, on=["date", C], how="inner")
            .join(z, on=["date", C], how="left"))
    cut = pool.select("date").unique().sort("date")["date"].to_list()
    mid = cut[int(len(cut) * 0.75)]
    print("\n" + "=" * 82)
    print("【1】條件篩選:accel_z 對照原始 rev_yoy_accel(S 池內,火箭機率量尺)")
    print("=" * 82)
    for tag, sub in (("IS", pool.filter(pl.col("date") <= mid)),
                     ("OOS", pool.filter(pl.col("date") > mid))):
        screen(sub, "rev_yoy_accel", tag)
        screen(sub, "accel_z", tag)

    print("\n" + "=" * 82)
    print("【2】端到端:accel_z 取代 / 併入 rev_yoy_accel")
    print("=" * 82)
    zz = z.rename({"accel_z": "_z"})

    def mk(mode: str):
        def f(df: pl.DataFrame) -> pl.DataFrame:
            d = df.join(zz, on=["date", C], how="left")
            if mode == "base":
                return canonical_score(d)
            # z 缺值(公布次數不足)→ 退回原始 accel 的截面百分位,不剔除該股
            d = d.with_columns(pl.col("_z").alias("accel_z_f"))
            if mode == "replace":
                w = {k: v for k, v in WREL.items() if k != "rev_yoy_accel"}
                d = d.drop_nulls("accel_z_f")
                return canonical_score(d, {**w, "accel_z_f": 1.0})
            if mode == "add":
                d = d.drop_nulls("accel_z_f")
                return canonical_score(d, {**WREL, "accel_z_f": 0.5})
            raise ValueError(mode)
        return f

    rows_full, rows_a, rows_b = [], [], []
    for label, mode in (("A0 現行", "base"), ("Z1 取代 accel", "replace"),
                        ("Z2 加 accel_z 0.5", "add")):
        nav, tr = run_s_full(panel, feat, elig, FULL, _score_fn=mk(mode))
        rows_full.append(_row(label, nav, tr))
        cutd = pl.lit(SPLIT).str.to_date()
        na = nav.filter(pl.col("date") < cutd)
        rows_a.append(_row(label, na.with_columns(pl.col("nav") / pl.col("nav").first()),
                           tr.filter(pl.col("entry_date") < cutd)))
        nb, trb = run_s_full(panel, feat, elig, SPLIT, _score_fn=mk(mode))
        rows_b.append(_row(label, nb, trb))
        print(f"  {label} 完成")

    for tag, rows in (("全窗", rows_full), ("前窗", rows_a), ("後窗", rows_b)):
        t = pl.DataFrame(rows)
        base = t.filter(pl.col("arm").str.starts_with("A0"))
        t = t.with_columns([
            ((pl.col("cagr") - base["cagr"][0]) * 100).round(2).alias("ΔCAGR_pp"),
            (pl.col("sharpe") - base["sharpe"][0]).round(3).alias("ΔSharpe")])
        print(f"\n--- {tag} ---")
        with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=170, float_precision=4):
            print(t)
        t.write_parquet(OUT / f"arms_{tag}.parquet")
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
