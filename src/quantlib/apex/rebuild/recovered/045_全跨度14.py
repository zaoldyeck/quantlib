"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T10:13:09.937Z(工具 Bash)
涵蓋 trials(10):全跨度14.5y_R3, 全跨度14.5y_S, 全跨度14.5y_v6, 正2全史同窗_R3, 正2全史同窗_r08a, 正2全史同窗_v6, 現代era_R3, 現代era_S, 現代era_r08a, 現代era_v6
"""
"""v6 battery 補跑 + 連續窗。"""
import os, time, json
import numpy as np
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics, validate
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DS, DE = "2019-01-02", "2025-06-30"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
t0 = time.time()
con = data.connect()

def prep(ws, we):
    panel, feat, elig = build_features(con, ws, we)
    rev = (data.load_monthly_revenue(con, we)
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue").rolling_sum(3)
                / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1).over(C).alias("rev_seq"),
           ])
           .select([C, "avail", "rev_seq"]).drop_nulls().sort("avail"))
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C, strategy="backward", tolerance="70d")
            .sort([C, "date"]))
    return panel, feat, elig

W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}

def run(panel, feat, elig, ws, seed=None):
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(W5)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in W5.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    if seed is not None:
        sc = sc.with_columns(pl.Series("score", seed.random(sc.height)))
    e, _ = entries_and_flags(sc, 8, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=8, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30),
                    start=Date.fromisoformat(ws))

panel, feat, elig = prep(DS, DE)
nav = ledger.load_curve("T0229")
s = metrics.perf_stats(nav)
bs = validate.block_bootstrap_cagr(nav)
trials = ledger.all_trials()
n = trials.height
dev_ids = trials.filter(pl.col("window").str.starts_with("2019-01-02"))["trial_id"].to_list()
curves = [pl.read_parquet(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))
          for t in dev_ids if os.path.exists(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))]
vsr = validate.sr_variance_from_curves(curves)
dsr = validate.deflated_sharpe(nav, n_trials=n, sr_var_across_trials=vsr)
common = None
for c in curves:
    ds_ = set(c["date"].to_list())
    common = ds_ if common is None else (common & ds_)
mat = np.stack([validate.daily_returns(c.filter(pl.col("date").is_in(pl.Series(sorted(common)).implode())))
                for c in curves], axis=1)
pbo = validate.pbo_cscv(mat, s=16)
rng = np.random.default_rng(71)
perm = [metrics.perf_stats(run(panel, feat, elig, DS, seed=rng).nav)["cagr"] for _ in range(200)]
p = float((np.array(perm) >= s["cagr"]).mean())
print(f"battery:bootstrap CI[{bs['ci_lo']:+.1%},{bs['ci_hi']:+.1%}] DSR {dsr['dsr']:.4f}(N={n}) "
      f"PBO {pbo['pbo']:.3f} perm p={p:.4f}(null 中位 {np.median(perm):+.1%})")

for ws, we, tag, target in [("2019-01-02", "2026-07-07", "現代era", 0.559),
                             ("2014-11-03", "2026-07-07", "正2全史同窗", 0.377),
                             ("2012-01-02", "2026-07-07", "全跨度14.5y", None)]:
    p2, f2, e2 = prep(ws, we)
    r = run(p2, f2, e2, ws)
    sm = metrics.perf_stats(r.nav)
    ledger.log_trial(family="fullspan", name=f"{tag}_v6", hypothesis="v6 連續窗",
                     config={"W5": True}, window=f"{ws}..{we}", metrics=sm,
                     batch="R12-CERT", curve=r.nav)
    print(f"{tag}: {sm['cagr']:+.1%}/{sm['sharpe']:.2f}/{sm['mdd']:+.1%} | {sm['final_nav_ratio']:.0f}x "
          f"{'🏆>正2' if target and sm['cagr'] > target else ''}")
    if tag == "全跨度14.5y":
        yt = metrics.yearly_table(r.nav)
        print("逐年:", "  ".join(f"{y}:{v*100:+.0f}%" for y, v in zip(yt["year"], yt["ret"])))
print(f"total {time.time()-t0:.0f}s")
