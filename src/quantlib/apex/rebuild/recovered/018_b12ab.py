# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T01:04:35.923Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/src/quantlib/apex/experiments/b12_new_families2.py)
# 涵蓋 trials(8):b12ab_tpex_only, b12ab_twse_only, b12c_minprice20, b12d_fscore_gate, b12e_revmonthly_abs20, b12f_4axis_mom, b12g_fresh_breakout, b12h_revmonthly_d15
"""B12 — 新家族探索 II(8 trials × 6 家族;預註冊見 ledger/batches.md)。

Run: uv run --project . python -m quantlib.apex.experiments.b12_new_families2
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from quantlib.apex import data
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExitSpec, PortSpec

C = "company_code"
DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "B12"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
FGATE = [pl.col("cfo_ni_ratio_ttm").is_not_null()]  # 佔位不用

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)

# f_score(季頻 PIT)併入 feat 供 d 用
rq = (
    pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
    .sort([C, "year", "quarter"])
    .with_columns(
        pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
        .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
        .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
        .otherwise(pl.date(pl.col("year") + 1, 3, 31)).alias("q_avail")
    )
    .select([C, "q_avail", pl.col("f_score_raw").cast(pl.Float64)])
    .drop_nulls().sort("q_avail")
)
feat = (feat.sort("date")
        .join_asof(rq, left_on="date", right_on="q_avail", by=C,
                   strategy="backward", tolerance="150d")
        .sort([C, "date"]))

markets = panel.select(["date", C, "market"]).unique(subset=["date", C], keep="first")
feat_m = feat.join(markets, on=["date", C], how="left")

td = panel.select(pl.col("date").unique().sort()).with_columns(
    [pl.col("date").dt.day().alias("dom"), pl.col("date").dt.month().alias("m"),
     pl.col("date").dt.year().alias("y")])
DAY11 = td.filter(pl.col("dom").is_between(11, 24)).group_by(["y", "m"]).agg(
    pl.col("date").min()).get_column("date")
DAY15 = td.filter(pl.col("dom").is_between(15, 28)).group_by(["y", "m"]).agg(
    pl.col("date").min()).get_column("date")


def W(df):
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def go(name, hypothesis, family, entries, flags, *, topn=20, trail=0.25, tstop=30,
       abs_stop=None, max_new=5):
    return run_trial(
        name=name, hypothesis=hypothesis, family=family, batch=BATCH,
        panel=panel, entries=entries, exit_flags=flags, bench=bench,
        window=WINDOW, start=START,
        config={"topn": topn, "trail": trail, "time_stop": tstop, "abs_stop": abs_stop},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop, abs_stop=abs_stop),
        verbose=False,
    )


def revcycle(feat_src, *, require, fresh=5, stale=26, extra_entry_filter=None,
             weights=None):
    pool = feat_src.filter(pl.col("rev_fresh_days") <= fresh)
    sc = W(blend_score(pool, elig, weights or TRI, require=require))
    if extra_entry_filter is not None:
        sc = sc.join(extra_entry_filter, on=["date", C], how="semi")
    flags = W(feat.filter(pl.col("rev_fresh_days") >= stale).select(["date", C]))
    e, _ = entries_and_flags(sc, 20, 10**9)
    return e, flags


runs = []
# a/b universe 切分
for mkt in ("twse", "tpex"):
    fm = feat_m.filter(pl.col("market") == mkt).drop("market")
    e, f = revcycle(fm, require=GATE)
    runs.append(go(f"b12ab_{mkt}_only", f"{mkt} 單市場 rev-cycle", "universe", e, f))

# c min_price 20(以 elig 之外再加價格門檻:panel raw_close ≥ 20 決策日)
px20 = panel.filter(pl.col("raw_close") >= 20).select(["date", C])
e, f = revcycle(feat, require=GATE, extra_entry_filter=px20)
runs.append(go("b12c_minprice20", "低價股剔除", "eligibility", e, f))

# d 閘替換:f_score ≥ 5
e, f = revcycle(feat, require=[pl.col("f_score_raw") >= 5])
runs.append(go("b12d_fscore_gate", "F-Score 閘替換 cfo 閘", "gate_design", e, f))

# e rev_monthly_n20 + abs20
sc5 = W(blend_score(feat, elig, TRI, require=GATE)).filter(pl.col("date").is_in(DAY11.implode()))
r5 = sc5.with_columns(pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
e5 = r5.filter(pl.col("rk") <= 20).select(["date", C, "score"])
f5 = (panel.select(["date", C]).filter(pl.col("date").is_in(DAY11.implode()))
      .join(e5.select(["date", C]), on=["date", C], how="anti"))
runs.append(go("b12e_revmonthly_abs20", "月頻最簡版 + abs20", "rev_monthly",
               e5, f5, tstop=None, abs_stop=0.20, max_new=20))

# f 軸擴充 TRI + 0.5 mom126
e, f = revcycle(feat, require=GATE,
                weights={"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0,
                         "mom_126_5": 0.5})
runs.append(go("b12f_4axis_mom", "TRI + 0.5×mom126", "axes", e, f))

# g fresh ∩ donchian 觸發
trig = feat.filter(pl.col("donchian_60") > 1.0).select(["date", C])
e, f = revcycle(feat, require=GATE, extra_entry_filter=trig)
runs.append(go("b12g_fresh_breakout", "揭露新鮮 ∩ 突破事件", "entry_trigger", e, f))

# h rev_monthly 決策日 ≥15(執行延遲)
sc8 = W(blend_score(feat, elig, TRI, require=GATE)).filter(pl.col("date").is_in(DAY15.implode()))
r8 = sc8.with_columns(pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
e8 = r8.filter(pl.col("rk") <= 20).select(["date", C, "score"])
f8 = (panel.select(["date", C]).filter(pl.col("date").is_in(DAY15.implode()))
      .join(e8.select(["date", C]), on=["date", C], how="anti"))
runs.append(go("b12h_revmonthly_d15", "月頻延遲到 15 日(執行延遲探測)", "rev_monthly",
               e8, f8, tstop=None, max_new=20))

cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar", "n_trades"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=120):
    print(cmp)
print("\n晉級門檻:frontier over v1s26(30.8/1.70/−30.9)或 CAGR≥27 ∧ Sharpe≥1.60 ∧ MDD≥−30")
print(f"total {time.time()-t0:.1f}s")

