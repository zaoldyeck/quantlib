"""G04 — 全市場排名學習・純訊號層驗證(預註冊見 ledger/batches.md G04 段)。

第一波:標籤 {fwd21_rank, policy21_rank}(policy = 經 trail35/time30/lts15
結算的交易報酬),全市場橫斷面,lambdarank walk-forward(756/63/26),
純訊號層評估(RankIC / 十分位 / top-decile 超額)。不做組合回測。

Run: uv run --project . python -m quantlib.apex.experiments.g04_signal_rank
依賴 cache: 是(需最新)。輸出:ledger/g04_scores_{label}.parquet + stdout。
"""
from __future__ import annotations

from datetime import date as Date

import duckdb
import lightgbm as lgb
import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.assemble import build_features

C = "company_code"
TRAIN_W, REFIT_EVERY, EMBARGO = 756, 63, 26
OOS_START = Date(2019, 1, 2)
BASE_PARAMS = dict(
    n_estimators=600, learning_rate=0.05, num_leaves=31, min_child_samples=200,
    colsample_bytree=0.8, subsample=0.8, subsample_freq=1,
    random_state=42, verbose=-1, n_jobs=-1,
)
TRAIL, TSTOP, LTS = 0.35, 30, 15


def policy_return(closes: np.ndarray) -> np.ndarray:
    """每個 t:t+1 close 進場,trail/time/lts 先到先出,出場 close 結算報酬。

    向量化:sliding window(TSTOP+1)沿路徑掃描;末尾不足窗者以可得路徑結算。
    """
    n = len(closes)
    out = np.full(n, np.nan, dtype=np.float64)
    if n < 3:
        return out
    w = TSTOP + 1
    pad = np.concatenate([closes, np.full(w, closes[-1])])
    win = np.lib.stride_tricks.sliding_window_view(pad, w)[1:n + 1]  # t+1 起
    entry = win[:, 0:1]
    peak = np.maximum.accumulate(win, axis=1)
    trail_hit = win <= peak * (1.0 - TRAIL)
    lts_hit = (win < entry) & (np.arange(w)[None, :] >= LTS)
    hit = trail_hit | lts_hit
    first = np.where(hit.any(axis=1), hit.argmax(axis=1), w - 1)
    exit_px = win[np.arange(len(win)), first]
    out[:len(win)] = exit_px / entry[:, 0] - 1.0
    return out


def main() -> None:
    con = data.connect()
    panel, feat, elig = build_features(con, "2014-06-01", "2026-07-15",
                                       warmup_days=300)
    raw = duckdb.connect("var/cache/cache.duckdb", read_only=True)
    # 擴充軸:多窗動能/波動/量增/log 價格與 ADV + dm 因子
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

    # 標籤:fwd21 與 policy(逐檔 numpy)
    lab_parts = []
    for _, g in px.select(["date", C, "close"]).sort([C, "date"]).group_by(
            C, maintain_order=True):
        closes = g["close"].to_numpy().astype(np.float64)
        n = len(closes)
        fwd21 = np.full(n, np.nan)
        if n > 22:
            fwd21[:-22] = closes[22:] / closes[1:n - 21] - 1.0  # t+1→t+22
        pol = policy_return(closes)
        lab_parts.append(pl.DataFrame({
            "date": g["date"], C: g[C],
            "fwd21": fwd21, "policy21": pol}))
    labels = pl.concat(lab_parts).with_columns([
        pl.col("fwd21").fill_nan(None), pl.col("policy21").fill_nan(None)])

    ds = (feat.join(px.drop("close"), on=["date", C], how="inner")
          .join(labels, on=["date", C], how="inner")
          .join(elig.filter(pl.col("eligible")).select(["date", C]),
                on=["date", C], how="semi"))
    AXES = [c for c in ds.columns
            if c not in ("date", C, "fwd21", "policy21")
            and ds[c].dtype in (pl.Float64, pl.Float32, pl.Int64)]
    print(f"面板:{ds.height:,} 列 × {len(AXES)} 特徵;"
          f"{ds['date'].min()} ~ {ds['date'].max()}")

    def relevance(pct):
        return (pl.when(pct >= 0.9).then(3).when(pct >= 0.7).then(2)
                .when(pct >= 0.4).then(1).otherwise(0))

    results = {}
    for LABEL in ("fwd21", "policy21"):
        d = (ds.drop_nulls(subset=[LABEL])
             .with_columns(((pl.col(LABEL).rank() / pl.len()).over("date"))
                           .alias("pct"))
             .with_columns(relevance(pl.col("pct")).alias("rel"))
             .sort(["date", C]))
        dates_all = d.select("date").unique().sort("date")["date"].to_list()
        preds = []
        n_refit = 0
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
            m = lgb.LGBMRanker(objective="lambdarank", eval_at=[10],
                               **BASE_PARAMS)
            gt = trn.group_by("date", maintain_order=True).len()["len"].to_list()
            gv = val.group_by("date", maintain_order=True).len()["len"].to_list()
            m.fit(trn.select(AXES).to_numpy(), trn["rel"].to_numpy(), group=gt,
                  eval_set=[(val.select(AXES).to_numpy(), val["rel"].to_numpy())],
                  eval_group=[gv],
                  callbacks=[lgb.early_stopping(50, verbose=False)])
            cols = list(dict.fromkeys(["date", C, "fwd21", LABEL, "pct"]))
            preds.append(pr.select(cols).with_columns(
                pl.Series("pred", m.predict(pr.select(AXES).to_numpy()))))
            n_refit += 1
        sc = pl.concat(preds)
        sc.write_parquet(f"src/quantlib/apex/ledger/g04_scores_{LABEL}.parquet")

        # 訊號層評估
        daily = (sc.group_by("date").agg([
            pl.corr("pred", "pct", method="spearman").alias("ric"),
            pl.len().alias("n")]).drop_nulls()
            .filter(pl.col("ric").is_not_nan()).sort("date"))
        ric = daily["ric"].to_numpy()
        t_ric = ric.mean() / ric.std(ddof=1) * np.sqrt(len(ric))
        sc_ev = sc.filter(pl.col("fwd21").is_not_null()
                          & pl.col("fwd21").is_not_nan())
        dec = (sc_ev.with_columns(
                   ((pl.col("pred").rank() / pl.len() * 10).ceil()
                    .clip(1, 10)).over("date").alias("dec"))
               .group_by("dec").agg(pl.col("fwd21").mean()).sort("dec"))
        top = (sc_ev.with_columns(((pl.col("pred").rank(descending=True))
                                   .over("date")).alias("rk")))
        topdec = (top.filter(pl.col("rk") <= pl.col("rk").max().over("date") * 0.1)
                  .group_by("date").agg(pl.col("fwd21").mean().alias("t"))
                  .join(sc_ev.group_by("date").agg(pl.col("fwd21").mean().alias("a")),
                        on="date"))
        ex = (topdec["t"] - topdec["a"]).to_numpy()
        t_ex = ex.mean() / ex.std(ddof=1) * np.sqrt(len(ex))
        results[LABEL] = dict(n_refit=n_refit, ric=float(ric.mean()),
                              t_ric=float(t_ric),
                              topdec_ex21=float(ex.mean()), t_ex=float(t_ex))
        print(f"\n=== 標籤 {LABEL}({n_refit} refits,OOS {daily['date'].min()}"
              f"~{daily['date'].max()})===")
        print(f"RankIC 均值 {ric.mean():+.4f}(t {t_ric:.1f});"
              f"top-decile fwd21 超額 {ex.mean():+.2%}/次(t {t_ex:.1f})")
        with pl.Config(tbl_rows=10):
            print(dec)

    print("\n=== 判準(預註冊):RankIC>0.03 且 t>4;top-decile 超額月化>1% ===")
    for k, v in results.items():
        ok = v["ric"] > 0.03 and v["t_ric"] > 4 and v["topdec_ex21"] > 0.01
        print(f"{k}: {'✓ 通過' if ok else '✗ 未過'} {v}")


if __name__ == "__main__":
    main()
