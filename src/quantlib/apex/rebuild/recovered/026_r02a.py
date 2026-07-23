# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:03:58.659Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/src/quantlib/apex/experiments/r02_regime_momentum.py)
# 涵蓋 trials(10):r02a_52wh_tb, r02b_tb_halt, r02c_tb_derisk, r02d_tb_derisk_t25, r02e_tb_crashstate, r02f_mom61_derisk, r02g_v3_mom_5050, r02h_v3_mom_7030, r02i_tb_derisk_top5, r02j_tb_derisk_t35
"""R02 — tie-break × regime 開關 × 雙線合成(10 trials;預註冊見 batches.md R-LINE)。

Run: uv run --project . python -m quantlib.apex.experiments.r02_regime_momentum
"""
from __future__ import annotations

import time
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import blend_score, build_features, run_trial
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec

C = "company_code"
DEV_START, DEV_END = "2019-01-02", "2025-06-30"
BATCH = "R02"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)

mom61 = (panel.sort([C, "date"])
         .with_columns((pl.col("close").shift(21) / pl.col("close").shift(126) - 1)
                       .over(C).alias("mom_6_1"))
         .select(["date", C, "mom_6_1"]))
adv = data.eligibility(panel).select(["date", C, "adv20"])
feat = (feat.join(mom61, on=["date", C], how="left")
        .join(adv, on=["date", C], how="left")
        .with_columns(pl.col("adv20").rank("ordinal", descending=True).over("date").alias("adv_rk")))

td = panel.select(pl.col("date").unique().sort()).with_columns(
    [pl.col("date").dt.month().alias("m"), pl.col("date").dt.year().alias("y")])
DAY1 = td.group_by(["y", "m"]).agg(pl.col("date").min()).get_column("date")

# regime 序列
b = bench.sort("date").with_columns(pl.col("nav").rolling_mean(200).alias("ma200"))
below_ma = b.filter(pl.col("nav") <= pl.col("ma200")).select("date")
r = b.with_columns((pl.col("nav") / pl.col("nav").shift(20) - 1).alias("r20"))
rr = r["r20"].to_numpy()
state = np.zeros(len(rr), dtype=bool)
on = False
for i, x in enumerate(rr):
    if not np.isnan(x):
        if not on and x < -0.10:
            on = True
        elif on and x > -0.03:
            on = False
    state[i] = on
crash_days = b.select("date").with_columns(pl.Series("c", state)).filter(pl.col("c")).select("date")
all_codes = panel.select(pl.col(C).unique())
print(f"MA200-below days: {below_ma.height} | crash-state days: {crash_days.height}")


def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def monthly_book(weights, *, topn, extra=None):
    sc = W_(blend_score(feat, elig, weights)).filter(pl.col("date").is_in(DAY1.implode()))
    if extra is not None:
        sc = sc.join(extra, on=["date", C], how="semi")
    r_ = sc.with_columns(pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
    e = r_.filter(pl.col("rk") <= topn).select(["date", C, "score"])
    f = (panel.select(["date", C]).filter(pl.col("date").is_in(DAY1.implode()))
         .join(e.select(["date", C]), on=["date", C], how="anti"))
    return e, f


def apply_regime(e, f, *, halt_dates=None, derisk_dates=None):
    if derisk_dates is not None:
        f = pl.concat([f, derisk_dates.join(all_codes, how="cross").select(["date", C])]).unique()
        e = e.join(derisk_dates, on="date", how="anti")
    if halt_dates is not None:
        e = e.join(halt_dates, on="date", how="anti")
    return e, f


def go(name, hypothesis, family, e, f, *, topn, trail=0.30):
    return run_trial(
        name=name, hypothesis=hypothesis, family=family, batch=BATCH,
        panel=panel, entries=e, exit_flags=f, bench=bench, window=WINDOW, start=START,
        config={"topn": topn, "trail": trail},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=topn),
        exit_spec=ExitSpec(trailing_stop=trail),
        verbose=False,
    )


TB = {"high_52w": 1.0, "mom_6_1": 0.05}
runs = []
e0, f0 = monthly_book(TB, topn=10)
runs.append(go("r02a_52wh_tb", "tie-break 修復", "mom_monthly", e0, f0, topn=10))
e, f = apply_regime(e0, f0, halt_dates=below_ma)
runs.append(go("r02b_tb_halt", "tb + MA200 停新倉", "mom_regime", e, f, topn=10))
e, f = apply_regime(e0, f0, derisk_dates=below_ma)
runs.append(go("r02c_tb_derisk", "tb + MA200 全出", "mom_regime", e, f, topn=10))
runs.append(go("r02d_tb_derisk_t25", "c + trail25", "mom_regime", e, f, topn=10, trail=0.25))
runs.append(go("r02j_tb_derisk_t35", "c + trail35", "mom_regime", e, f, topn=10, trail=0.35))
e, f = apply_regime(e0, f0, derisk_dates=crash_days)
runs.append(go("r02e_tb_crashstate", "tb + 崩盤狀態機全出", "mom_regime", e, f, topn=10))

adv100 = W_(feat.filter(pl.col("adv_rk") <= 100).select(["date", C]))
e, f = monthly_book({"mom_6_1": 1.0}, topn=10, extra=adv100)
e, f = apply_regime(e, f, derisk_dates=below_ma)
runs.append(go("r02f_mom61_derisk", "流動性動能 + derisk", "mom_regime", e, f, topn=10))

e5, f5 = monthly_book(TB, topn=5)
e5, f5 = apply_regime(e5, f5, derisk_dates=below_ma)
runs.append(go("r02i_tb_derisk_top5", "集中 top5 + derisk", "mom_regime", e5, f5, topn=5))

# 雙線合成:v3(R01 T0128 曲線)⊕ r02c
v3_nav = ledger.load_curve("T0128")
c_nav = ledger.load_curve(runs[2]["trial_id"])


def blend(a, b_, wa):
    j = (a.select(["date", pl.col("nav").alias("na")])
         .join(b_.select(["date", pl.col("nav").alias("nb")]), on="date", how="inner")
         .sort("date")
         .with_columns([
             (pl.col("na") / pl.col("na").shift(1) - 1).fill_null(0.0).alias("ra"),
             (pl.col("nb") / pl.col("nb").shift(1) - 1).fill_null(0.0).alias("rb")])
         .with_columns(((1 + wa * pl.col("ra") + (1 - wa) * pl.col("rb")).cum_prod()).alias("nav")))
    return j.select(["date", "nav"])


for nm, wa in [("r02g_v3_mom_5050", 0.5), ("r02h_v3_mom_7030", 0.7)]:
    nav = blend(v3_nav, c_nav, wa)
    s = metrics.perf_stats(nav)
    tid = ledger.log_trial(family="dual_line", name=nm, hypothesis=f"v3 {wa:.0%} ⊕ gated-mom",
                           config={"w_v3": wa, "books": ["T0128", runs[2]["trial_id"]]},
                           window=WINDOW, metrics=s, batch=BATCH, curve=nav)
    runs.append({"trial_id": tid, "name": nm, **s})

cmp = pl.DataFrame(
    [{k: r.get(k) for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=12, tbl_width_chars=115):
    print(cmp)
print("\nv3 對照(T0128):42.9%/1.75/−26.6 | 晉級:CAGR≥30 ∧ Sharpe≥1.2 ∧ MDD≥−40")
print(f"total {time.time()-t0:.1f}s")

