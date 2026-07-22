# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T17:21:10.836Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/g01_ml_ranker.py)
# 涵蓋 trials(4):g01_ML_W3, g01_ML_fullOOS, g01_S_W3, g01_S_fullOOS
"""G01 — LightGBM cohort 排名器(walk-forward 全 OOS)。

保留 apex_revcycle_S 的事件框架(fresh≤7 池、adv5、n5/mn2、五出場),
僅將六軸幾何 rank 替換為 LightGBM 預測分數。預註冊見 ledger/batches.md G01。

- 樣本:每日 fresh cohort;特徵 19 軸(NaN 原生處理,不 drop);
  標籤 = fwd 21 交易日報酬的 cohort 內 rank-pct。
- Walk-forward:每 63 交易日 refit、訓練窗 756 交易日、embargo 26 交易日,
  refit 日後資料零接觸 → 整條 NAV 全 OOS。

Run: uv run --project research python -m research.apex.experiments.g01_ml_ranker
"""
from __future__ import annotations

import time
from datetime import date as Date

import lightgbm as lgb
import numpy as np
import polars as pl

from research.apex import data, ledger
from research.apex.assemble import FEATURE_COLS, build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
PREP_START, END = "2012-01-01", "2026-07-09"
TRAIN_W, REFIT_EVERY, EMBARGO = 756, 63, 26
FWD = 21
W3_START = "2023-07-10"
S_WTS = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
         "rev_seq": 0.5, "accel_rel": 0.5}
LGB_PARAMS = dict(
    objective="regression", n_estimators=600, learning_rate=0.05, num_leaves=31,
    min_child_samples=200, colsample_bytree=0.8, subsample=0.8, subsample_freq=1,
    random_state=42, verbose=-1, n_jobs=-1,
)


def prep():
    con = data.connect()
    panel, feat, _ = build_features(con, PREP_START, END)
    rev = (data.load_monthly_revenue(con, END)
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue").rolling_sum(3)
                / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1)
               .over(C).alias("rev_seq"),
           ])
           .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance="70d")
            .sort([C, "date"]))
    tax = con.sql(
        "SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
        "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
    fx = (feat.select(["date", C, "rev_yoy_accel"]).drop_nulls().sort("date")
          .join_asof(tax.sort("effective_date"), left_on="date",
                     right_on="effective_date", by=C, strategy="backward")
          .drop_nulls(subset=["industry"]))
    ind_med = fx.group_by(["date", "industry"]).agg(
        pl.col("rev_yoy_accel").median().alias("m"))
    rel = (fx.join(ind_med, on=["date", "industry"], how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("m")).alias("accel_rel"))
           .select(["date", C, "accel_rel"]))
    feat = feat.join(rel, on=["date", C], how="left")
    extra = (panel.sort([C, "date"])
             .with_columns([
                 (pl.col("volume").cast(pl.Float64).rolling_mean(5)
                  / (pl.col("volume").cast(pl.Float64).rolling_mean(60) + 1))
                 .over(C).alias("volume_surge_60"),
                 ((pl.col("close").rolling_max(60) - pl.col("close").rolling_min(60))
                  / (pl.col("close").rolling_mean(60) + 1e-9))
                 .over(C).alias("consolidation_60"),
             ])
             .select(["date", C, "volume_surge_60", "consolidation_60"]))
    feat = feat.join(extra, on=["date", C], how="left")
    return con, panel, feat


def build_dataset(panel, feat):
    """fresh cohort 樣本:(date, code, features..., label=fwd21 cohort rank-pct)。"""
    fwd = (panel.sort([C, "date"])
           .with_columns((pl.col("close").shift(-FWD) / pl.col("close") - 1)
                         .over(C).alias("fwd_ret"))
           .select(["date", C, "fwd_ret"]))
    elig = (data.eligibility(panel, min_adv=5_000_000.0)
            .filter(pl.col("eligible")).select(["date", C]))
    axes = [c for c in FEATURE_COLS if c != "rev_fresh_days"] + [
        "rev_fresh_days", "rev_seq", "accel_rel", "volume_surge_60", "consolidation_60"]
    ds = (feat.filter(pl.col("rev_fresh_days") <= 7)
          .join(elig, on=["date", C], how="semi")
          .join(fwd, on=["date", C], how="left")
          .with_columns((pl.col("fwd_ret").rank() / pl.col("fwd_ret").count())
                        .over("date").alias("label"))
          .select(["date", C, *axes, "label"])
          .sort("date"))
    return ds, axes


def walk_forward(ds, axes):
    """每 REFIT_EVERY 交易日 refit;回傳 OOS 分數流 + importance。"""
    dates = ds.select("date").unique().sort("date")["date"].to_list()
    didx = {d: i for i, d in enumerate(dates)}
    first_refit = TRAIN_W + EMBARGO
    scores, ics, imps = [], [], []
    n_refit = 0
    for ri in range(first_refit, len(dates), REFIT_EVERY):
        refit_d = dates[ri]
        tr_lo, tr_hi = dates[max(0, ri - EMBARGO - TRAIN_W)], dates[ri - EMBARGO]
        pr_hi = dates[min(ri + REFIT_EVERY, len(dates)) - 1]
        tr = ds.filter((pl.col("date") >= tr_lo) & (pl.col("date") < tr_hi)
                       ).drop_nulls(subset=["label"])
        pr = ds.filter((pl.col("date") >= refit_d) & (pl.col("date") <= pr_hi))
        if tr.height < 3000 or pr.height == 0:
            continue
        # 時間切分早停:訓練窗最後 63 交易日當 val
        val_lo = dates[ri - EMBARGO - 63]
        trn = tr.filter(pl.col("date") < val_lo)
        val = tr.filter(pl.col("date") >= val_lo)
        m = lgb.LGBMRegressor(**LGB_PARAMS)
        m.fit(trn.select(axes).to_numpy(), trn["label"].to_numpy(),
              eval_set=[(val.select(axes).to_numpy(), val["label"].to_numpy())],
              callbacks=[lgb.early_stopping(50, verbose=False)])
        pred = m.predict(pr.select(axes).to_numpy(),
                         num_iteration=m.best_iteration_)
        out = pr.select(["date", C, "label"]).with_columns(
            pl.Series("score", pred))
        scores.append(out.select(["date", C, "score"]))
        # OOS 日度 rank IC(僅 label 已實現且 cohort ≥ 8 檔的日子)
        ic_df = (out.drop_nulls(subset=["label"])
                 .group_by("date").agg([
                     pl.corr(pl.col("score").rank(), pl.col("label").rank())
                     .alias("ic"), pl.len().alias("n")])
                 .filter(pl.col("n") >= 8))
        ics.append(ic_df.select(["date", "ic"]))
        imps.append(m.feature_importances_ / m.feature_importances_.sum())
        n_refit += 1
    sc = pl.concat(scores)
    ic = pl.concat(ics).drop_nulls()
    imp = pl.DataFrame({"axis": axes, "gain": np.mean(imps, axis=0).round(4)}
                       ).sort("gain", descending=True)
    return sc, ic, imp, n_refit


def kpi(nav, n_boot=2000, block=21, seed=42):
    v = nav.sort("date")["nav"].to_numpy()
    d = nav.sort("date")["date"].to_numpy()
    r = v[1:] / v[:-1] - 1
    t = len(r)
    yrs = (d[-1] - d[0]).astype("timedelta64[D]").astype(float) / 365.25
    cagr = (v[-1] / v[0]) ** (1 / yrs) - 1
    rng = np.random.default_rng(seed)
    nb = int(np.ceil(t / block))
    starts = rng.integers(0, t, size=(n_boot, nb))
    idx = (starts[:, :, None] + np.arange(block)[None, None, :]) % t
    boot = np.prod(1.0 + r[idx.reshape(n_boot, -1)[:, :t]],
                   axis=1) ** (252.0 / t) - 1.0
    dd = v / np.maximum.accumulate(v) - 1
    return {"cagr": cagr, "p5": float(np.percentile(boot, 5)),
            "martin": cagr / float(np.sqrt(np.mean(dd ** 2))),
            "mdd": float(dd.min())}


def run_book(panel, feat, sc, start):
    e, _ = entries_and_flags(
        sc.filter(pl.col("date") >= pl.lit(start).str.to_date()), 5, 10**9)
    f = (feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C])
         .filter(pl.col("date") >= pl.lit(start).str.to_date()))
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                      loser_time_stop=15),
                   start=Date.fromisoformat(start))
    return res.nav.select(["date", "nav"]).sort("date")


def s_score(feat, panel, start):
    elig = (data.eligibility(panel, min_adv=5_000_000.0)
            .filter(pl.col("eligible")).select(["date", C]))
    df = (feat.filter(pl.col("rev_fresh_days") <= 7)
          .join(elig, on=["date", C], how="semi")
          .drop_nulls(subset=list(S_WTS))
          .filter(pl.col("cfo_ni_ratio_ttm")
                  >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    expr = None
    for c_, wt in S_WTS.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    return (df.with_columns(expr.alias("score"))
            .select(["date", C, "score"])
            .filter(pl.col("date") >= pl.lit(start).str.to_date()))


def paired(nav_a, nav_b, block=21, n_boot=4000, seed=42):
    j = nav_a.join(nav_b, on="date", suffix="_b").sort("date")
    ra = np.log(j["nav"].to_numpy()[1:] / j["nav"].to_numpy()[:-1])
    rb = np.log(j["nav_b"].to_numpy()[1:] / j["nav_b"].to_numpy()[:-1])
    d = ra - rb
    t = len(d)
    rng = np.random.default_rng(seed)
    nb = int(np.ceil(t / block))
    starts = rng.integers(0, t, size=(n_boot, nb))
    idx = (starts[:, :, None] + np.arange(block)[None, None, :]) % t
    ann = d[idx.reshape(n_boot, -1)[:, :t]].mean(axis=1) * 252
    return {"mean": float(d.mean() * 252), "lo": float(np.percentile(ann, 2.5)),
            "hi": float(np.percentile(ann, 97.5))}


def main() -> None:
    t0 = time.time()
    con, panel, feat = prep()
    ds, axes = build_dataset(panel, feat)
    print(f"prep {time.time()-t0:.0f}s;樣本 {ds.height:,}(label 覆蓋 "
          f"{ds.drop_nulls(subset=['label']).height:,})")
    sc, ic, imp, n_refit = walk_forward(ds, axes)
    sc.write_parquet("research/apex/ledger/g01_scores.parquet")
    ic_v = ic["ic"].to_numpy()
    ic_mean = float(ic_v.mean())
    ic_t = float(ic_mean / (ic_v.std() / np.sqrt(len(ic_v))))
    print(f"walk-forward:{n_refit} refits,{time.time()-t0:.0f}s")
    print(f"OOS rank IC:mean {ic_mean:.4f}  t {ic_t:.1f}  n_days {len(ic_v)}")
    print("\nFeature importance(gain,跨 refit 平均):")
    with pl.Config(tbl_rows=19):
        print(imp)

    oos_start = sc["date"].min().isoformat()
    rows = []
    navs = {}
    for tag, start in [("W3", W3_START), ("fullOOS", oos_start)]:
        nav_ml = run_book(panel, feat, sc, start)
        nav_s = run_book(panel, feat, s_score(feat, panel, start), start)
        navs[(tag, "ml")] = nav_ml
        navs[(tag, "s")] = nav_s
        for nm, nv in [("ML", nav_ml), ("S", nav_s)]:
            k = kpi(nv)
            rows.append({"window": tag, "book": nm,
                         **{kk: round(vv, 3) for kk, vv in k.items()}})
            ledger.log_trial(
                family="g_line", name=f"g01_{nm}_{tag}",
                hypothesis="LightGBM cohort 排名器 vs 幾何 rank",
                config={"train_w": TRAIN_W, "refit": REFIT_EVERY,
                        "embargo": EMBARGO, "axes": len(axes)},
                window=f"{start}..{END}",
                metrics={kk: float(vv) for kk, vv in k.items()},
                batch="G01", curve=nv)
    print(pl.DataFrame(rows))
    for tag in ["W3", "fullOOS"]:
        d = paired(navs[(tag, "ml")], navs[(tag, "s")])
        print(f"配對 ML − S({tag}):{d['mean']:+.2%}/年  "
              f"CI [{d['lo']:+.2%}, {d['hi']:+.2%}]")
    print(f"\ntotal {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()

