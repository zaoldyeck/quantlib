"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:33:55.881Z(工具 Bash)
涵蓋 trials(13):matchwin_r06e_vs_00631L, matchwin_t35_vs_00631L, r08a_t35_revlevel, r08b_t40, r08c_t35_fresh6, r08d_t35_momw40, r08e_t35_stale28, r08f_t35_maxnew8, r08g_t35_n7, r08h_t35_pos15, 正2全史同窗_R3, 正2全史同窗_r08a, 正2全史同窗_v6
"""
"""R08 — 最終精煉批(8 trials)+ t35 版正2全史同窗補跑。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DS, DE = "2019-01-02", "2025-06-30"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DS, DE)
bench = data.benchmark_nav(con, DS, DE)

def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())

def go(name, *, exps=None, topn=8, trail=0.35, fresh=5, stale=26, max_new=5):
    w = exps or W4
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    cols = list(w)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=cols))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = W_(df.with_columns(expr.alias("score")).select(["date", C, "score"]))
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = W_(feat.filter(pl.col("rev_fresh_days") >= stale).select(["date", C]))
    return run_trial(
        name=name, hypothesis="R08 最終精煉", family="r08", batch="R08",
        panel=panel, entries=e, exit_flags=f, bench=bench, window=f"{DS}..{DE}",
        start=Date.fromisoformat(DS),
        config={"topn": topn, "trail": trail, "fresh": fresh, "stale": stale},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=30), verbose=False)

runs = [
    go("r08a_t35_revlevel", exps=dict(W4) | {"rev_yoy": 0.5}),
    go("r08b_t40", trail=0.40),
    go("r08c_t35_fresh6", fresh=6),
    go("r08d_t35_momw40", exps=dict(W4) | {"mom_126_5": 0.4}),
    go("r08e_t35_stale28", stale=28),
    go("r08f_t35_maxnew8", max_new=8),
    go("r08g_t35_n7", topn=7),
    go("r08h_t35_pos15", exps=dict(W4) | {"close_pos_20": 1.5}),
]
cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=105):
    print(cmp)
print("對照 r07f(geo-n8-t35):60.9/1.72/−38.7 | 晉級:CAGR≥62.9 或(Sharpe≥1.80∧CAGR≥60.9)\n")

# t35 版正2全史同窗
p2, f2, e2 = build_features(con, "2014-11-03", "2026-07-07")
pool = f2.filter(pl.col("rev_fresh_days") <= 5)
df = (pool.join(e2.filter(pl.col("eligible")).select(["date", C]), on=["date", C], how="semi")
      .drop_nulls(subset=list(W4)))
for cond in GATE:
    df = df.filter(cond)
expr = None
for c_, wt in W4.items():
    term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
    expr = term if expr is None else expr * term
sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
    pl.col("date") >= pl.lit("2014-11-03").str.to_date())
e, _ = entries_and_flags(sc, 8, 10**9)
fl = f2.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
    pl.col("date") >= pl.lit("2014-11-03").str.to_date())
res = simulate(p2, e, exit_flags=fl, exec_spec=ExecSpec(),
               port_spec=PortSpec(n_slots=8, max_new_per_day=5),
               exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30),
               start=Date(2014, 11, 3))
s = metrics.perf_stats(res.nav)
ledger.log_trial(family="fullspan", name="matchwin_t35_vs_00631L", hypothesis="t35 全史同窗",
                 config={"geo": True, "topn": 8, "trail": 0.35},
                 window="2014-11-03..2026-07-07", metrics=s, batch="R08", curve=res.nav)
yt = metrics.yearly_table(res.nav)
print(f"geo-n8-t35 正2全史同窗:CAGR {s['cagr']:+.1%} | Sharpe {s['sharpe']:.2f} | "
      f"MDD {s['mdd']:+.1%} | {s['final_nav_ratio']:.0f}x(正2:+37.7/1.08/−55.1/42x)")
print("逐年:", "  ".join(f"{y}:{r*100:+.0f}%" for y, r in zip(yt["year"], yt["ret"])))
print(f"total {time.time()-t0:.1f}s")
