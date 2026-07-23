"""G06 — 時間序列模型・訊號層第一波(預註冊見 batches.md G06 段)。

閉式統計族(全向量化):ar1_pred / mr_z20 / vamom;訊號層評估同 G04
(fwd10 top-k 超額 + RankIC),對照 = G04 lambdarank 與裸 mom20。

Run: uv run --project . python -m quantlib.apex.experiments.g06_timeseries
依賴 cache: 是
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.assemble import build_features

C = "company_code"


def main() -> None:
    con = data.connect()
    panel, feat, elig = build_features(con, "2014-06-01", "2026-07-19",
                                       warmup_days=300)
    px = (panel.sort([C, "date"])
          .with_columns((pl.col("close") / pl.col("close").shift(1) - 1)
                        .over(C).alias("r"))
          .with_columns([
              # AR(1) 一步預測:ρ̂(252) × r_t(rolling 相關 × 當日報酬)
              (pl.rolling_corr(pl.col("r"), pl.col("r").shift(1), window_size=252)
               .over(C) * pl.col("r")).alias("ar1_pred"),
              # 均值回歸 z:−(close/MA20 − 1)/σ20
              (-((pl.col("close") / pl.col("close").rolling_mean(20) - 1)
                 / pl.col("r").rolling_std(20).clip(1e-6, None)))
              .over(C).alias("mr_z20"),
              # 波動調整動能:mom20 / EWMA-vol(λ=0.94)
              ((pl.col("close") / pl.col("close").shift(20) - 1)
               / (pl.col("r").pow(2).ewm_mean(alpha=0.06).sqrt() * np.sqrt(252))
               .clip(1e-6, None)).over(C).alias("vamom"),
              (pl.col("close") / pl.col("close").shift(20) - 1)
              .over(C).alias("mom20"),
              # 標籤 fwd10(t+1 → t+11 close)
              (pl.col("close").shift(-11) / pl.col("close").shift(-1) - 1)
              .over(C).alias("fwd10"),
          ])
          .select(["date", C, "ar1_pred", "mr_z20", "vamom", "mom20", "fwd10"]))
    ds = (px.join(elig.filter(pl.col("eligible")).select(["date", C]),
                  on=["date", C], how="semi")
          .filter(pl.col("date") >= pl.date(2019, 1, 2))
          .filter(pl.col("fwd10").is_not_null() & pl.col("fwd10").is_not_nan()))
    print(f"面板:{ds.height:,} 列;{ds['date'].min()} ~ {ds['date'].max()}")
    print(f"對照:G04 lambdarank top-5 +1.93%/10d(t12.0);"
          f"判讀=時序族要勝裸 mom20 才有『建模增量』\n")
    print(f"{'因子':10s} {'RankIC':>8s} {'t':>6s} {'top-5/10d':>10s} {'t':>6s} "
          f"{'top-20/10d':>10s} {'t':>6s}")
    for f in ("ar1_pred", "mr_z20", "vamom", "mom20"):
        d = ds.filter(pl.col(f).is_not_null() & pl.col(f).is_not_nan())
        daily = (d.group_by("date").agg(
            pl.corr(f, "fwd10", method="spearman").alias("ic"))
            .drop_nulls().filter(pl.col("ic").is_not_nan()))
        ic = daily["ic"].to_numpy()
        t_ic = ic.mean() / ic.std(ddof=1) * np.sqrt(len(ic))
        line = f"{f:10s} {ic.mean():+8.4f} {t_ic:6.1f}"
        for k in (5, 20):
            top = (d.with_columns(pl.col(f).rank(descending=True)
                                  .over("date").alias("rk"))
                   .filter(pl.col("rk") <= k)
                   .group_by("date").agg(pl.col("fwd10").mean().alias("t"))
                   .join(d.group_by("date").agg(pl.col("fwd10").mean().alias("a")),
                         on="date"))
            ex = (top["t"] - top["a"]).to_numpy()
            tt = ex.mean() / ex.std(ddof=1) * np.sqrt(len(ex))
            line += f" {ex.mean():+10.2%} {tt:6.1f}"
        print(line)


if __name__ == "__main__":
    main()
