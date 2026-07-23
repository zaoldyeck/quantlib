"""G04c — 基本面特徵擴充 × fwd10 排名訊號(預註冊見 batches.md G04c 段)。

新增:pe/pb(日頻)+ 季報 PIT 12 軸(毛利/營益/淨利率、roa、週轉、流動比、
負債比、F-Score、d_roa/d_gm YoY)+ rev_seq(月營收環比)。
其餘管線與 G04 逐字同構(lambdarank、756/63/26、OOS 2019+)。

Run: uv run --project . python -m quantlib.apex.experiments.g04c_fundamentals
依賴 cache: 是。輸出:ledger/g04c_scores_fwd10.parquet + stdout(對照 G04 基準)。
"""
from __future__ import annotations

import duckdb
import lightgbm as lgb
import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.assemble import build_features
from quantlib.apex.experiments.g04_signal_rank import (BASE_PARAMS, C,
                                                       EMBARGO, OOS_START,
                                                       REFIT_EVERY, TRAIN_W)

QCOLS = ["gross_margin_ttm", "operating_margin_q", "net_margin_q", "roa_ttm",
         "asset_turnover_ttm", "current_ratio", "lt_debt_ratio", "f_score_raw",
         "d_roa_yoy", "d_gross_margin_yoy"]


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

    # ── 基本面擴充 ──
    val = (raw.execute(
        "SELECT date, company_code, price_to_earning_ratio AS pe, "
        "price_book_ratio AS pb FROM stock_per_pbr").pl()
        .unique(subset=["date", C], keep="first"))
    q = (pl.read_parquet("src/quantlib/apex/../raw_quarterly.parquet")
         if False else pl.read_parquet("research/raw_quarterly.parquet"))
    q = (q.sort([C, "year", "quarter"])
         .with_columns(
             pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
             .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
             .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
             .otherwise(pl.date(pl.col("year") + 1, 3, 31)).alias("q_avail"))
         .select([C, "q_avail", *QCOLS]).sort("q_avail"))
    rev = (data.load_monthly_revenue(con, "2026-07-15")
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue").rolling_sum(3)
                / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
               .over(C).alias("rev_seq"),
           ]).select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))

    lab_parts = []
    for _, g in px.select(["date", C, "close"]).sort([C, "date"]).group_by(
            C, maintain_order=True):
        closes = g["close"].to_numpy().astype(np.float64)
        n = len(closes)
        f = np.full(n, np.nan)
        if n > 11:
            f[:-11] = closes[11:] / closes[1:n - 10] - 1.0
        lab_parts.append(pl.DataFrame({"date": g["date"], C: g[C], "fwd10": f}))
    labels = pl.concat(lab_parts).with_columns(pl.col("fwd10").fill_nan(None))

    ds = (feat.join(px.drop("close"), on=["date", C], how="inner")
          .join(val, on=["date", C], how="left")
          .sort("date")
          .join_asof(q, left_on="date", right_on="q_avail", by=C,
                     strategy="backward", tolerance="150d")
          .join_asof(rev, left_on="date", right_on="avail", by=C,
                     strategy="backward", tolerance="70d")
          .join(labels, on=["date", C], how="inner")
          .join(elig.filter(pl.col("eligible")).select(["date", C]),
                on=["date", C], how="semi"))
    AXES = [c for c in ds.columns
            if c not in ("date", C, "fwd10", "q_avail", "avail")
            and ds[c].dtype in (pl.Float64, pl.Float32, pl.Int64)]
    print(f"面板:{ds.height:,} 列 × {len(AXES)} 特徵(G04 為 25)")

    def relevance(pct):
        return (pl.when(pct >= 0.9).then(3).when(pct >= 0.7).then(2)
                .when(pct >= 0.4).then(1).otherwise(0))

    d = (ds.filter(pl.col("fwd10").is_not_null())
         .with_columns(((pl.col("fwd10").rank() / pl.len()).over("date"))
                       .alias("pct"))
         .with_columns(relevance(pl.col("pct")).alias("rel"))
         .sort(["date", C]))
    dates_all = d.select("date").unique().sort("date")["date"].to_list()
    preds, imps = [], []
    for ri in range(TRAIN_W + EMBARGO, len(dates_all), REFIT_EVERY):
        pr_lo = dates_all[ri]
        if pr_lo < OOS_START:
            continue
        tr_lo = dates_all[max(0, ri - EMBARGO - TRAIN_W)]
        tr_hi = dates_all[ri - EMBARGO]
        val_lo = dates_all[ri - EMBARGO - 126]
        pr_hi = dates_all[min(ri + REFIT_EVERY, len(dates_all)) - 1]
        trn = d.filter((pl.col("date") >= tr_lo) & (pl.col("date") < val_lo))
        va = d.filter((pl.col("date") >= val_lo) & (pl.col("date") < tr_hi))
        pr = d.filter((pl.col("date") >= pr_lo) & (pl.col("date") <= pr_hi))
        if not (trn.height and va.height and pr.height):
            continue
        m = lgb.LGBMRanker(objective="lambdarank", eval_at=[10], **BASE_PARAMS)
        gt = trn.group_by("date", maintain_order=True).len()["len"].to_list()
        gv = va.group_by("date", maintain_order=True).len()["len"].to_list()
        m.fit(trn.select(AXES).to_numpy(), trn["rel"].to_numpy(), group=gt,
              eval_set=[(va.select(AXES).to_numpy(), va["rel"].to_numpy())],
              eval_group=[gv],
              callbacks=[lgb.early_stopping(50, verbose=False)])
        preds.append(pr.select(["date", C, "fwd10"]).with_columns(
            pl.Series("pred", m.predict(pr.select(AXES).to_numpy()))))
        imps.append(m.feature_importances_)
    sc = pl.concat(preds)
    sc.write_parquet("src/quantlib/apex/ledger/g04c_scores_fwd10.parquet")

    imp = np.mean(imps, axis=0)
    order = np.argsort(imp)[::-1]
    print("\nimportance top-12:",
          [(AXES[i], round(float(imp[i]), 1)) for i in order[:12]])
    fund = [a for a in AXES if a in ("pe", "pb", "rev_seq", "dy",
                                     "cfo_ni_ratio_ttm", *QCOLS)]
    fshare = sum(imp[AXES.index(a)] for a in fund) / imp.sum()
    print(f"基本面特徵 importance 佔比:{fshare:.0%}")

    print(f"\n{'':6s}{'top-k 超額/10日':>14s} {'t':>6s}(G04 基準:top-5 +1.93 t12.0 / top-20 +1.39 t14.1)")
    for k in (5, 20):
        top = (sc.with_columns(pl.col("pred").rank(descending=True)
                               .over("date").alias("rk"))
               .filter(pl.col("rk") <= k)
               .group_by("date").agg(pl.col("fwd10").mean().alias("t"))
               .join(sc.group_by("date").agg(pl.col("fwd10").mean().alias("a")),
                     on="date"))
        ex = (top["t"] - top["a"]).to_numpy()
        tt = ex.mean() / ex.std(ddof=1) * np.sqrt(len(ex))
        print(f"top-{k:2d} {ex.mean():+.2%} (t {tt:5.1f})")


if __name__ == "__main__":
    main()
