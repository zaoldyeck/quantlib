"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T10:11:54.905Z(工具 Bash)
涵蓋 trials(8):v6_dev, 正2全史同窗_R3, 正2全史同窗_r08a, 正2全史同窗_v6, 現代era_R3, 現代era_S, 現代era_r08a, 現代era_v6
"""
"""v6 完整認證:擾動 grid → battery → 連續窗 vs 正2。"""
import os, time
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

def run(panel, feat, elig, ws, *, fresh=5, stale=26, topn=8, trail=0.35, tstop=30,
        momw=0.5, seqw=0.5, accw=1.0, seed=None):
    w = {"rev_yoy_accel": accw, "high_52w": 1.0, "close_pos_20": 1.0,
         "mom_126_5": momw, "rev_seq": seqw}
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
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
    f = feat.filter(pl.col("rev_fresh_days") >= stale).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
                    start=Date.fromisoformat(ws))

panel, feat, elig = prep(DS, DE)
res = run(panel, feat, elig, DS)
s = metrics.perf_stats(res.nav)
tid = ledger.log_trial(family="rev_cycle_v6", name="v6_dev", hypothesis="v6 認證",
                       config={"W5": True}, window=f"{DS}..{DE}",
                       metrics=s, batch="R12-CERT", curve=res.nav)
print(f"{tid} v6 dev: {s['cagr']:+.2%}/{s['sharpe']:.3f}/{s['mdd']:+.2%}")

rows = [{"v": "base", "cagr": s["cagr"], "sharpe": s["sharpe"], "mdd": s["mdd"]}]
for nm, kw in [("fresh4", {"fresh": 4}), ("fresh6", {"fresh": 6}),
               ("stale21", {"stale": 21}), ("stale31", {"stale": 31}),
               ("n6", {"topn": 6}), ("n10", {"topn": 10}),
               ("trail30", {"trail": 0.30}), ("trail40", {"trail": 0.40}),
               ("time24", {"tstop": 24}), ("time36", {"tstop": 36}),
               ("momw40", {"momw": 0.4}), ("momw60", {"momw": 0.6}),
               ("seqw40", {"seqw": 0.4}), ("seqw60", {"seqw": 0.6}),
               ("accw80", {"accw": 0.8}), ("accw120", {"accw": 1.2})]:
    ss = metrics.perf_stats(run(panel, feat, elig, DS, **kw).nav)
    rows.append({"v": nm, "cagr": ss["cagr"], "sharpe": ss["sharpe"], "mdd": ss["mdd"]})
    print(f"  {nm:8s} {ss['cagr']:+.1%}/{ss['sharpe']:.2f}/{ss['mdd']:+.1%}")
pt = pl.DataFrame(rows)
spread = float(pt["cagr"].max() - pt["cagr"].min())
okn = pt.filter((pl.col("cagr") >= 0.30) & (pl.col("mdd") >= -0.45) & (pl.col("sharpe") >= 1.2)).height
print(f"擾動 spread {spread:.1%}(R 判準 <20%)| R-gates {okn}/{pt.height}")

# battery
bs = validate.block_bootstrap_cagr(res.nav)
trials = ledger.all_trials()
n = trials.height
dev_ids = trials.filter(pl.col("window").str.starts_with("2019-01-02"))["trial_id"].to_list()
curves = [pl.read_parquet(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))
          for t in dev_ids if os.path.exists(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))]
vsr = validate.sr_variance_from_curves(curves)
dsr = validate.deflated_sharpe(res.nav, n_trials=n, sr_var_across_trials=vsr)
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

# 連續窗
for ws, we, tag, target in [("2019-01-02", "2026-07-07", "現代era", 0.559),
                             ("2014-11-03", "2026-07-07", "正2全史同窗", 0.377),
                             ("2012-01-02", "2026-07-07", "全跨度", None)]:
    p2, f2, e2 = prep(ws, we)
    r = run(p2, f2, e2, ws)
    sm = metrics.perf_stats(r.nav)
    ledger.log_trial(family="fullspan", name=f"{tag}_v6", hypothesis="v6 連續窗",
                     config={"W5": True}, window=f"{ws}..{we}", metrics=sm,
                     batch="R12-CERT", curve=r.nav)
    print(f"{tag}: {sm['cagr']:+.1%}/{sm['sharpe']:.2f}/{sm['mdd']:+.1%} | {sm['final_nav_ratio']:.0f}x "
          f"{'🏆>正2' if target and sm['cagr'] > target else ''}")
    if tag == "全跨度":
        yt = metrics.yearly_table(r.nav)
        print("逐年:", "  ".join(f"{y}:{v*100:+.0f}%" for y, v in zip(yt["year"], yt["ret"])))
print(f"total {time.time()-t0:.0f}s")
