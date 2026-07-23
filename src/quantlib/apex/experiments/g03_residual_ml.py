"""G03 — 殘差學習 + 特徵擴充 + expanding window(ML 線最後攻勢)。

疊加式三變體(全 lambdarank NDCG@5):
  a) geo-as-feature:19 軸 + S 幾何分數 + cfo_gate flag
  b) a + 特徵擴充(~40 軸)
  c) b + expanding 訓練窗
預註冊見 ledger/batches.md G03。

Run: uv run --project . python -m quantlib.apex.experiments.g03_residual_ml
"""
from __future__ import annotations

import time
from datetime import date as Date

import lightgbm as lgb
import numpy as np
import polars as pl

from quantlib.apex import data, ledger
from quantlib.apex.assemble import FEATURE_COLS
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.apex.experiments.g01_ml_ranker import (
    C, END, S_WTS, W3_START, kpi, paired, prep, run_book, s_score)
from quantlib.apex.experiments.g02_lambdarank import (
    BASE_PARAMS, relevance, top5_spread)

TRAIN_W, REFIT_EVERY, EMBARGO = 756, 63, 26


def build_dataset_ext(panel, feat):
    """G03 樣本:19 軸 + geo_score + cfo_gate + 擴充特徵 + label/fwd_ret。"""
    # 擴充價量特徵(panel 端)
    ext = (panel.sort([C, "date"])
           .with_columns([
               (pl.col("close") / pl.col("close").shift(20) - 1).over(C).alias("mom_20"),
               (pl.col("close") / pl.col("close").shift(60) - 1).over(C).alias("mom_60"),
               (pl.col("close") / pl.col("close").shift(252) - 1).over(C).alias("mom_252"),
               (pl.col("close") / pl.col("close").shift(1) - 1).over(C)
               .rolling_std(20).over(C).alias("vol_20"),
               (pl.col("close") / pl.col("close").shift(1) - 1).over(C)
               .rolling_std(60).over(C).alias("vol_60"),
               (pl.col("volume").cast(pl.Float64).rolling_mean(5)
                / (pl.col("volume").cast(pl.Float64).rolling_mean(60) + 1))
               .over(C).alias("volume_surge_60"),
               ((pl.col("close").rolling_max(60) - pl.col("close").rolling_min(60))
                / (pl.col("close").rolling_mean(60) + 1e-9))
               .over(C).alias("consolidation_60"),
               pl.col("trade_value").cast(pl.Float64).rolling_median(20)
               .over(C).log1p().alias("log_adv20"),
               pl.col("raw_close").log1p().alias("log_price"),
               pl.int_range(pl.len()).over(C).alias("bars_listed"),
               (pl.col("close") / pl.col("close").rolling_max(252)).over(C)
               .diff(20).over(C).alias("d_high52w_20"),
               (pl.col("close").shift(-21) / pl.col("close") - 1).over(C).alias("fwd_ret"),
           ])
           .select(["date", C, "mom_20", "mom_60", "mom_252", "vol_20", "vol_60",
                    "volume_surge_60", "consolidation_60", "log_adv20", "log_price",
                    "bars_listed", "d_high52w_20", "fwd_ret"]))
    # rev 擴充(feat 端已有 rev_yoy/accel/seq/rel;加 lags 與連續加速)
    feat = (feat.sort([C, "date"])
            .with_columns([
                pl.col("rev_yoy").shift(21).over(C).alias("rev_yoy_lag1m"),
                (pl.col("rev_yoy_accel") > 0).cast(pl.Int8).alias("_acc_pos"),
            ])
            .with_columns(
                pl.col("_acc_pos").rolling_sum(63).over(C).alias("accel_streak_3m"))
            .drop("_acc_pos"))
    elig = (data.eligibility(panel, min_adv=5_000_000.0)
            .filter(pl.col("eligible")).select(["date", C]))
    # geo_score + cfo_gate(在 fresh cohort 內計)
    pool = (feat.filter(pl.col("rev_fresh_days") <= 7)
            .join(elig, on=["date", C], how="semi"))
    geo = None
    for c_, wt in S_WTS.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        geo = term if geo is None else geo * term
    pool = pool.with_columns([
        geo.alias("geo_score"),
        (pl.col("cfo_ni_ratio_ttm")
         >= pl.col("cfo_ni_ratio_ttm").median().over("date"))
        .cast(pl.Int8).alias("cfo_gate"),
        pl.len().over("date").alias("cohort_size"),
        pl.col("date").dt.month().alias("month"),
    ])
    ds = (pool.join(ext, on=["date", C], how="left")
          .with_columns((pl.col("fwd_ret").rank() / pl.col("fwd_ret").count())
                        .over("date").alias("label"))
          .with_columns(relevance(pl.col("label")).alias("rel"))
          .sort("date"))
    base_axes = [c for c in FEATURE_COLS if c != "rev_fresh_days"] + [
        "rev_fresh_days", "rev_seq", "accel_rel", "volume_surge_60",
        "consolidation_60"]
    ax_a = base_axes + ["geo_score", "cfo_gate"]
    ax_b = ax_a + ["mom_20", "mom_60", "mom_252", "vol_20", "vol_60", "log_adv20",
                   "log_price", "bars_listed", "d_high52w_20", "rev_yoy_lag1m",
                   "accel_streak_3m", "cohort_size", "month"]
    return ds, {"a_geo": ax_a, "b_ext": ax_b}


def walk_forward(ds, axes, panel, *, expanding=False):
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    scores, imps, n_refit = [], [], 0
    for ri in range(TRAIN_W + EMBARGO, len(dates_all), REFIT_EVERY):
        refit_d = dates_all[ri]
        tr_lo = dates_all[0] if expanding else dates_all[max(0, ri - EMBARGO - TRAIN_W)]
        tr_hi = dates_all[ri - EMBARGO]
        val_lo = dates_all[ri - EMBARGO - 126]
        pr_hi = dates_all[min(ri + REFIT_EVERY, len(dates_all)) - 1]
        tr_all = (ds.filter((pl.col("date") >= tr_lo) & (pl.col("date") < tr_hi))
                  .drop_nulls(subset=["label"]).sort("date"))
        pr = ds.filter((pl.col("date") >= refit_d) & (pl.col("date") <= pr_hi)
                       ).sort("date")
        if tr_all.height < 3000 or pr.height == 0:
            continue
        trn = tr_all.filter(pl.col("date") < val_lo)
        val = tr_all.filter(pl.col("date") >= val_lo)
        if trn.height < 2000 or val.height < 200:
            continue
        m = lgb.LGBMRanker(objective="lambdarank", eval_at=[5], **BASE_PARAMS)
        gt = trn.group_by("date", maintain_order=True).len()["len"].to_list()
        gv = val.group_by("date", maintain_order=True).len()["len"].to_list()
        m.fit(trn.select(axes).to_numpy(), trn["rel"].to_numpy(), group=gt,
              eval_set=[(val.select(axes).to_numpy(), val["rel"].to_numpy())],
              eval_group=[gv], callbacks=[lgb.early_stopping(50, verbose=False)])
        pred = m.predict(pr.select(axes).to_numpy(), num_iteration=m.best_iteration_)
        scores.append(pr.select(["date", C, "fwd_ret"])
                      .with_columns(pl.Series("raw_score", pred))
                      .with_columns((pl.col("raw_score").rank() / pl.len())
                                    .over("date").alias("score")))
        imps.append(m.feature_importances_ / m.feature_importances_.sum())
        n_refit += 1
    sc = pl.concat(scores)
    imp = pl.DataFrame({"axis": axes, "gain": np.mean(imps, axis=0).round(4)}
                       ).sort("gain", descending=True)
    return sc, imp, n_refit


def main() -> None:
    t0 = time.time()
    con, panel, feat = prep()
    ds, axmap = build_dataset_ext(panel, feat)
    print(f"prep {time.time()-t0:.0f}s;樣本 {ds.height:,}")

    variants = [("a_geo", axmap["a_geo"], False),
                ("b_ext", axmap["b_ext"], False),
                ("c_expand", axmap["b_ext"], True)]
    results = {}
    for name, axes, expanding in variants:
        t1 = time.time()
        sc, imp, n_refit = walk_forward(ds, axes, panel, expanding=expanding)
        spr, spr_t, n_d = top5_spread(sc)
        sc.select(["date", C, "score"]).write_parquet(
            f"src/quantlib/apex/ledger/g03_scores_{name}.parquet")
        print(f"\n[{name}] refits {n_refit}  axes {len(axes)}  "
              f"top5-spread +{spr:.3%}(t {spr_t:.1f})  ({time.time()-t1:.0f}s)")
        print(f"  importance top6:{imp.head(6)['axis'].to_list()}")
        results[name] = sc

    rows, navs = [], {}
    for name, sc in results.items():
        for tag, start in [("W3", W3_START), ("fullOOS", sc["date"].min().isoformat())]:
            nav = run_book(panel, feat, sc.select(["date", C, "score"]), start)
            navs[(name, tag)] = nav
            k = kpi(nav)
            rows.append({"obj": name, "window": tag,
                         **{kk: round(vv, 3) for kk, vv in k.items()}})
            ledger.log_trial(family="g_line", name=f"g03_{name}_{tag}",
                             hypothesis="殘差學習/特徵擴充/expanding",
                             config={"variant": name}, window=f"{start}..{END}",
                             metrics={kk: float(vv) for kk, vv in k.items()},
                             batch="G03", curve=nav)
    for tag in ["W3", "fullOOS"]:
        st = (W3_START if tag == "W3"
              else min(v["date"].min() for (o, tg), v in navs.items()
                       if tg == "fullOOS").isoformat())
        nav_s = run_book(panel, feat, s_score(feat, panel, st), st)
        navs[("S", tag)] = nav_s
        k = kpi(nav_s)
        rows.append({"obj": "S", "window": tag,
                     **{kk: round(vv, 3) for kk, vv in k.items()}})
    print()
    print(pl.DataFrame(rows).sort(["window", "p5"], descending=[False, True]))
    for name in results:
        for tag in ["W3", "fullOOS"]:
            d = paired(navs[(name, tag)], navs[("S", tag)])
            print(f"配對 {name} − S({tag}):{d['mean']:+.2%}/年"
                  f"  CI [{d['lo']:+.2%}, {d['hi']:+.2%}]")
    print(f"\ntotal {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
