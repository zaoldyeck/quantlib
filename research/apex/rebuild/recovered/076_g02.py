"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T17:24:13.847Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/g02_lambdarank.py)
涵蓋 trials(6):g02_lambdarank_W3, g02_lambdarank_fullOOS, g02_rankmse_W3, g02_rankmse_fullOOS, g02_raw_W3, g02_raw_fullOOS
"""
"""G02 — LambdaRank cohort 排名器(修 G01:真交易日曆 + top-k 目標)。

三 objective 對照:lambdarank(NDCG@5,主)/ raw-reg(fwd_ret 直接回歸)/
rank-MSE(G01 式,對照)。預註冊見 ledger/batches.md G02。

Run: uv run --project research python -m research.apex.experiments.g02_lambdarank
"""
from __future__ import annotations

import time
from datetime import date as Date

import lightgbm as lgb
import numpy as np
import polars as pl

from research.apex import data, ledger
from research.apex.assemble import entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import (
    C, END, S_WTS, W3_START, build_dataset, kpi, paired, prep, run_book, s_score)

TRAIN_W, REFIT_EVERY, EMBARGO = 756, 63, 26   # 真交易日
BASE_PARAMS = dict(
    n_estimators=600, learning_rate=0.05, num_leaves=31, min_child_samples=200,
    colsample_bytree=0.8, subsample=0.8, subsample_freq=1,
    random_state=42, verbose=-1, n_jobs=-1,
)


def relevance(label_pct: pl.Expr) -> pl.Expr:
    return (pl.when(label_pct >= 0.9).then(3)
            .when(label_pct >= 0.7).then(2)
            .when(label_pct >= 0.4).then(1)
            .otherwise(0))


def fit_predict(objective, trn, val, pr, axes):
    """單一 refit:訓練 + 對預測期打分(輸出 cohort 內 rank-pct)。"""
    Xt, Xv, Xp = (d.select(axes).to_numpy() for d in (trn, val, pr))
    if objective == "lambdarank":
        m = lgb.LGBMRanker(objective="lambdarank", eval_at=[5], **BASE_PARAMS)
        gt = trn.group_by("date", maintain_order=True).len()["len"].to_list()
        gv = val.group_by("date", maintain_order=True).len()["len"].to_list()
        m.fit(Xt, trn["rel"].to_numpy(), group=gt,
              eval_set=[(Xv, val["rel"].to_numpy())], eval_group=[gv],
              callbacks=[lgb.early_stopping(50, verbose=False)])
    else:
        y = "fwd_ret" if objective == "raw" else "label"
        m = lgb.LGBMRegressor(objective="regression", **BASE_PARAMS)
        m.fit(Xt, trn[y].to_numpy(), eval_set=[(Xv, val[y].to_numpy())],
              callbacks=[lgb.early_stopping(50, verbose=False)])
    pred = m.predict(Xp, num_iteration=m.best_iteration_)
    out = (pr.select(["date", C, "fwd_ret"])
           .with_columns(pl.Series("raw_score", pred))
           .with_columns((pl.col("raw_score").rank() / pl.len())
                         .over("date").alias("score")))
    return out, m.feature_importances_ / m.feature_importances_.sum()


def walk_forward(ds, axes, panel, objective):
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    scores, imps = [], []
    n_refit = 0
    for ri in range(TRAIN_W + EMBARGO, len(dates_all), REFIT_EVERY):
        refit_d = dates_all[ri]
        tr_lo = dates_all[max(0, ri - EMBARGO - TRAIN_W)]
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
        out, imp = fit_predict(objective, trn, val, pr, axes)
        scores.append(out)
        imps.append(imp)
        n_refit += 1
    sc = pl.concat(scores)
    imp = pl.DataFrame({"axis": axes, "gain": np.mean(imps, axis=0).round(4)}
                       ).sort("gain", descending=True)
    return sc, imp, n_refit


def top5_spread(sc: pl.DataFrame) -> tuple[float, float, int]:
    """OOS 每 cohort 日:模型 top-5 mean fwd21 − cohort mean fwd21。"""
    g = (sc.drop_nulls(subset=["fwd_ret"])
         .with_columns(pl.col("score").rank(descending=True).over("date").alias("rk"))
         .group_by("date")
         .agg([pl.col("fwd_ret").filter(pl.col("rk") <= 5).mean().alias("top5"),
               pl.col("fwd_ret").mean().alias("all"), pl.len().alias("n")])
         .filter(pl.col("n") >= 8)
         .with_columns((pl.col("top5") - pl.col("all")).alias("spr"))
         .drop_nulls(subset=["spr"]))
    v = g["spr"].to_numpy()
    return float(v.mean()), float(v.mean() / (v.std() / np.sqrt(len(v)))), len(v)


def main() -> None:
    t0 = time.time()
    con, panel, feat = prep()
    ds, axes = build_dataset(panel, feat)
    ds = ds.join(
        (panel.sort([C, "date"])
         .with_columns((pl.col("close").shift(-21) / pl.col("close") - 1)
                       .over(C).alias("fwd_ret"))
         .select(["date", C, "fwd_ret"])),
        on=["date", C], how="left").with_columns(relevance(pl.col("label")).alias("rel"))
    print(f"prep {time.time()-t0:.0f}s;樣本 {ds.height:,}")

    results = {}
    for obj in ["lambdarank", "raw", "rankmse"]:
        t1 = time.time()
        sc, imp, n_refit = walk_forward(ds, axes, panel, obj)
        spr, spr_t, n_d = top5_spread(sc)
        sc.select(["date", C, "score"]).write_parquet(
            f"research/apex/ledger/g02_scores_{obj}.parquet")
        print(f"\n[{obj}] refits {n_refit}  OOS {sc['date'].min()} → {sc['date'].max()}"
              f"  ({time.time()-t1:.0f}s)")
        print(f"  top5-spread(fwd21):{spr:+.3%}/次  t {spr_t:.1f}  n {n_d}")
        print(f"  importance top5:{imp.head(5)['axis'].to_list()}")
        results[obj] = sc
    print()

    rows, navs = [], {}
    for obj, sc in results.items():
        for tag, start in [("W3", W3_START), ("fullOOS", sc["date"].min().isoformat())]:
            nav = run_book(panel, feat, sc.select(["date", C, "score"]), start)
            navs[(obj, tag)] = nav
            k = kpi(nav)
            rows.append({"obj": obj, "window": tag,
                         **{kk: round(vv, 3) for kk, vv in k.items()}})
            ledger.log_trial(family="g_line", name=f"g02_{obj}_{tag}",
                             hypothesis="LambdaRank top-k 目標 vs 迴歸",
                             config={"objective": obj}, window=f"{start}..{END}",
                             metrics={kk: float(vv) for kk, vv in k.items()},
                             batch="G02", curve=nav)
    for tag, start in [("W3", W3_START), ("fullOOS", None)]:
        st = start or min(v["date"].min() for (o, tg), v in navs.items()
                          if tg == "fullOOS").isoformat()
        nav_s = run_book(panel, feat, s_score(feat, panel, st), st)
        navs[("S", tag)] = nav_s
        k = kpi(nav_s)
        rows.append({"obj": "S", "window": tag,
                     **{kk: round(vv, 3) for kk, vv in k.items()}})
    print(pl.DataFrame(rows).sort(["window", "p5"], descending=[False, True]))
    for obj in results:
        for tag in ["W3", "fullOOS"]:
            d = paired(navs[(obj, tag)], navs[("S", tag)])
            print(f"配對 {obj} − S({tag}):{d['mean']:+.2%}/年"
                  f"  CI [{d['lo']:+.2%}, {d['hi']:+.2%}]")
    print(f"\ntotal {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()

