"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:05:59.813Z(工具 Bash)
涵蓋 trials(6):r19a_n6mn2f6, r19b_n5mn3f6, r19c_n5mn2f6, r19d_n6mn3f6_t40, r19e_n6mn3f6_s60, r19f_n6mn3f7
"""
"""R19 — 最後突擊(6 交互)+ 勝者完整 battery。"""
import time
import numpy as np
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics, validate
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}
t0 = time.time()
con = data.connect()

def prep(ws, we):
    panel, feat, _ = build_features(con, ws, we)
    rev = (data.load_monthly_revenue(con, we)
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue").rolling_sum(3)
                / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1).over(C).alias("rev_seq"),
           ])
           .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    return panel, (feat.sort("date")
                   .join_asof(rev, left_on="date", right_on="avail", by=C,
                              strategy="backward", tolerance="70d")
                   .sort([C, "date"]))

DS, DE = "2019-01-02", "2025-06-30"
panel, feat = prep(DS, DE)
E5 = data.eligibility(panel, min_adv=5_000_000.0)
bench = data.benchmark_nav(con, DS, DE)

def go(panel_, feat_, el, ws, *, topn=6, fresh=6, trail=0.35, max_new=3, w=None, seed=None):
    w = w or W5
    pool = feat_.filter(pl.col("rev_fresh_days") <= fresh)
    df = (pool.join(el.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    if seed is not None:
        sc = sc.with_columns(pl.Series("score", seed.random(sc.height)))
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat_.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel_, e, exit_flags=f, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new),
                    exit_spec=ExitSpec(trailing_stop=trail, time_stop=30),
                    start=Date.fromisoformat(ws))

def trial(name, res):
    s = metrics.summarize(res.nav, res.trades, bench)
    tid = ledger.log_trial(family="mod_line", name=name, hypothesis="R19 終突擊",
                           config={"name": name}, window=f"{DS}..{DE}", metrics=s,
                           batch="R19", curve=res.nav)
    return {"trial_id": tid, "name": name, **{k: s[k] for k in ("cagr", "sharpe", "mdd")}}

runs = [
    trial("r19a_n6mn2f6", go(panel, feat, E5, DS, max_new=2)),
    trial("r19b_n5mn3f6", go(panel, feat, E5, DS, topn=5)),
    trial("r19c_n5mn2f6", go(panel, feat, E5, DS, topn=5, max_new=2)),
    trial("r19d_n6mn3f6_t40", go(panel, feat, E5, DS, trail=0.40)),
    trial("r19e_n6mn3f6_s60", go(panel, feat, E5, DS, w=dict(W5) | {"rev_seq": 0.6})),
    trial("r19f_n6mn3f7", go(panel, feat, E5, DS, fresh=7)),
]
cmp = pl.DataFrame(runs).sort("cagr", descending=True)
with pl.Config(tbl_rows=8, tbl_width_chars=100):
    print(cmp)

# 勝者 battery(取 cagr 最高者 config 重跑已存曲線)
best = cmp.row(0, named=True)
print(f"\n── {best['name']} battery ──")
nav = ledger.load_curve(best["trial_id"])
s = metrics.perf_stats(nav)
bs = validate.block_bootstrap_cagr(nav)
trials = ledger.all_trials()
n = trials.height
rng = np.random.default_rng(97)
kw = {"r19a_n6mn2f6": {"max_new": 2}, "r19b_n5mn3f6": {"topn": 5},
      "r19c_n5mn2f6": {"topn": 5, "max_new": 2}, "r19d_n6mn3f6_t40": {"trail": 0.40},
      "r19e_n6mn3f6_s60": {"w": dict(W5) | {"rev_seq": 0.6}}, "r19f_n6mn3f7": {"fresh": 7}}[best["name"]]
perm = [metrics.perf_stats(go(panel, feat, E5, DS, seed=rng, **kw).nav)["cagr"] for _ in range(100)]
p = float((np.array(perm) >= s["cagr"]).mean())
# 舊時代披露
OW, OE = "2012-01-02", "2018-12-28"
p_old, f_old = prep(OW, OE)
E5o = data.eligibility(p_old, min_adv=5_000_000.0)
so = metrics.perf_stats(go(p_old, f_old, E5o, OW, **kw).nav)
print(f"bootstrap CI[{bs['ci_lo']:+.1%},{bs['ci_hi']:+.1%}] | perm p={p:.3f}(null 中位 {np.median(perm):+.1%})"
      f" | N={n} | 舊時代披露:{so['cagr']:+.1%}/{so['sharpe']:.2f}")
print(f"total {time.time()-t0:.0f}s")
