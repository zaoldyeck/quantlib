"""G04b — horizon 第二波:fwd{10,42,63} 排名訊號 vs fwd21(預註冊見 G04 段)。

同 G04 管線(全市場、lambdarank、756/63/26 walk-forward);評估 = 各標籤
以「自身 horizon 的 top-k 超額」計,並換算年化(×252/N)跨 horizon 可比。
policy 標籤已於第一波證偽,不再跑。

Run: uv run --project research python -m research.apex.experiments.g04b_horizon
依賴 cache: 是。輸出:ledger/g04_scores_fwd{10,42,63}.parquet + stdout。
"""
from __future__ import annotations

from datetime import date as Date

import duckdb
import lightgbm as lgb
import numpy as np
import polars as pl

from research.apex import data
from research.apex.assemble import build_features
from research.apex.experiments.g04_signal_rank import (BASE_PARAMS, C,
                                                       EMBARGO, OOS_START,
                                                       REFIT_EVERY, TRAIN_W)

HORIZONS = (10, 42, 63)


def main() -> None:
    con = data.connect()
    panel, feat, elig = build_features(con, "2014-06-01", "2026-07-15",
                                       warmup_days=300)
    raw = duckdb.connect("var/cache/cache.duckdb", read_only=True)
    mkt = (raw.execute("SELECT date, close FROM market_index "
                       "WHERE name = '發行量加權股價指數' ORDER BY date").pl()
           .with_columns((pl.col("close") / pl.col("close").shift(1) - 1)
                         .alias("mkt_ret")).select(["date", "mkt_ret"]))
    px = (panel.sort([C, "date"])
          .with_columns((pl.col("close") / pl.col("close").shift(1) - 1)
                        .over(C).alias("ret"))
          .join(mkt, on="date", how="left")
          .with_columns([
              (pl.col("close") / pl.col("close").shift(20) - 1).over(C).alias("mom20"),
              (pl.col("close") / pl.col("close").shift(60) - 1).over(C).alias("mom60"),
              (pl.col("close") / pl.col("close").shift(252) - 1).over(C).alias("mom252"),
              pl.col("ret").rolling_std(20).over(C).alias("vol20"),
              pl.col("ret").rolling_std(60).over(C).alias("vol60"),
              (pl.col("volume").cast(pl.Float64).rolling_mean(5)
               / pl.col("volume").cast(pl.Float64).rolling_mean(60))
              .over(C).alias("vsurge"),
              pl.col("trade_value").cast(pl.Float64).rolling_median(20)
              .over(C).log1p().alias("log_adv"),
              pl.col("close").log1p().alias("log_px"),
              pl.when(pl.col("mkt_ret") < 0)
              .then(pl.col("ret") - pl.col("mkt_ret")).otherwise(None).alias("_ex"),
              pl.when(pl.col("mkt_ret") < 0)
              .then((pl.col("ret") > 0).cast(pl.Float64)).otherwise(None).alias("_w"),
          ])
          .with_columns([
              pl.col("_ex").rolling_mean(60, min_samples=10).over(C).alias("dm_rs60"),
              pl.col("_w").rolling_mean(60, min_samples=10).over(C).alias("dm_win60"),
          ])
          .select(["date", C, "close", "mom20", "mom60", "mom252", "vol20",
                   "vol60", "vsurge", "log_adv", "log_px", "dm_rs60", "dm_win60"]))

    lab_parts = []
    for _, g in px.select(["date", C, "close"]).sort([C, "date"]).group_by(
            C, maintain_order=True):
        closes = g["close"].to_numpy().astype(np.float64)
        n = len(closes)
        cols = {"date": g["date"], C: g[C]}
        for h in HORIZONS:
            f = np.full(n, np.nan)
            if n > h + 1:
                f[:-(h + 1)] = closes[h + 1:] / closes[1:n - h] - 1.0
            cols[f"fwd{h}"] = f
        lab_parts.append(pl.DataFrame(cols))
    labels = pl.concat(lab_parts).with_columns(
        [pl.col(f"fwd{h}").fill_nan(None) for h in HORIZONS])

    ds = (feat.join(px.drop("close"), on=["date", C], how="inner")
          .join(labels, on=["date", C], how="inner")
          .join(elig.filter(pl.col("eligible")).select(["date", C]),
                on=["date", C], how="semi"))
    LABELS = [f"fwd{h}" for h in HORIZONS]
    AXES = [c for c in ds.columns
            if c not in ("date", C, *LABELS)
            and ds[c].dtype in (pl.Float64, pl.Float32, pl.Int64)]
    print(f"面板:{ds.height:,} 列 × {len(AXES)} 特徵")

    def relevance(pct):
        return (pl.when(pct >= 0.9).then(3).when(pct >= 0.7).then(2)
                .when(pct >= 0.4).then(1).otherwise(0))

    print(f"\n{'標籤':7s} {'top-5 超額/期':>13s} {'t':>6s} {'年化超額':>9s} "
          f"{'top-20/期':>10s} {'t':>6s} {'年化':>8s}")
    print("(對照 fwd21:top-5 +2.01%/21d t9.6 → 年化 ~+27%;top-20 +1.42 t11.3 → ~+18%)")
    for h in HORIZONS:
        LABEL = f"fwd{h}"
        d = (ds.filter(pl.col(LABEL).is_not_null())
             .with_columns(((pl.col(LABEL).rank() / pl.len()).over("date"))
                           .alias("pct"))
             .with_columns(relevance(pl.col("pct")).alias("rel"))
             .sort(["date", C]))
        dates_all = d.select("date").unique().sort("date")["date"].to_list()
        preds = []
        for ri in range(TRAIN_W + EMBARGO, len(dates_all), REFIT_EVERY):
            pr_lo = dates_all[ri]
            if pr_lo < OOS_START:
                continue
            tr_lo = dates_all[max(0, ri - EMBARGO - TRAIN_W)]
            tr_hi = dates_all[ri - EMBARGO]
            val_lo = dates_all[ri - EMBARGO - 126]
            pr_hi = dates_all[min(ri + REFIT_EVERY, len(dates_all)) - 1]
            trn = d.filter((pl.col("date") >= tr_lo) & (pl.col("date") < val_lo))
            val = d.filter((pl.col("date") >= val_lo) & (pl.col("date") < tr_hi))
            pr = d.filter((pl.col("date") >= pr_lo) & (pl.col("date") <= pr_hi))
            if not (trn.height and val.height and pr.height):
                continue
            m = lgb.LGBMRanker(objective="lambdarank", eval_at=[10], **BASE_PARAMS)
            gt = trn.group_by("date", maintain_order=True).len()["len"].to_list()
            gv = val.group_by("date", maintain_order=True).len()["len"].to_list()
            m.fit(trn.select(AXES).to_numpy(), trn["rel"].to_numpy(), group=gt,
                  eval_set=[(val.select(AXES).to_numpy(), val["rel"].to_numpy())],
                  eval_group=[gv],
                  callbacks=[lgb.early_stopping(50, verbose=False)])
            preds.append(pr.select(["date", C, LABEL]).with_columns(
                pl.Series("pred", m.predict(pr.select(AXES).to_numpy()))))
        sc = pl.concat(preds)
        sc.write_parquet(f"research/apex/ledger/g04_scores_{LABEL}.parquet")

        line = f"{LABEL:7s}"
        for k in (5, 20):
            top = (sc.with_columns(pl.col("pred").rank(descending=True)
                                   .over("date").alias("rk"))
                   .filter(pl.col("rk") <= k)
                   .group_by("date").agg(pl.col(LABEL).mean().alias("t"))
                   .join(sc.group_by("date").agg(pl.col(LABEL).mean().alias("a")),
                         on="date"))
            ex = (top["t"] - top["a"]).to_numpy()
            tt = ex.mean() / ex.std(ddof=1) * np.sqrt(len(ex))
            ann = (1 + ex.mean()) ** (252 / h) - 1
            line += f" {ex.mean():+.2%} (t{tt:5.1f}) 年化{ann:+.0%} |"
        print(line)


if __name__ == "__main__":
    main()
