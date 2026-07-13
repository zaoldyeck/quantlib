"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:08:12.494Z(工具 Bash)
涵蓋 trials(10):全跨度14.5y_R3, 全跨度14.5y_S, 全跨度14.5y_v6, 正2全史同窗_R3, 正2全史同窗_r08a, 正2全史同窗_v6, 現代era_R3, 現代era_S, 現代era_r08a, 現代era_v6
"""
"""R3 完整認證:擾動 grid + DSR/PBO + 連續窗 + 解剖。"""
import os, time
import numpy as np
import polars as pl
from datetime import date as Date
from research.apex import data, ledger, metrics, validate
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

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

def go(panel_, feat_, ws, *, adv=5_000_000.0, topn=5, fresh=6, trail=0.35, max_new=2,
       tstop=30, w=None):
    w = w or W5
    el = data.eligibility(panel_, min_adv=adv)
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
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat_.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel_, e, exit_flags=f, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new),
                    exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
                    start=Date.fromisoformat(ws))

DS, DE = "2019-01-02", "2025-06-30"
panel, feat = prep(DS, DE)

# 擾動 grid
rows = []
for nm, kw in [("base", {}), ("n4", {"topn": 4}), ("n6", {"topn": 6}),
               ("mn1", {"max_new": 1}), ("mn3", {"max_new": 3}),
               ("fresh5", {"fresh": 5}), ("fresh7", {"fresh": 7}),
               ("trail30", {"trail": 0.30}), ("trail40", {"trail": 0.40}),
               ("stale21…time24", {"tstop": 24}), ("time36", {"tstop": 36}),
               ("adv4M", {"adv": 4_000_000.0}), ("adv6M", {"adv": 6_000_000.0}),
               ("seq40", {"w": dict(W5) | {"rev_seq": 0.4}}),
               ("seq60", {"w": dict(W5) | {"rev_seq": 0.6}})]:
    s = metrics.perf_stats(go(panel, feat, DS, **kw).nav)
    rows.append({"v": nm, **{k: s[k] for k in ("cagr", "sharpe", "mdd")}})
    print(f"  {nm:14s} {s['cagr']:+.1%}/{s['sharpe']:.2f}/{s['mdd']:+.1%}")
pt = pl.DataFrame(rows)
print(f"擾動 spread {float(pt['cagr'].max()-pt['cagr'].min()):.1%} | "
      f"全過 R-gates {pt.filter((pl.col('cagr')>=0.30)&(pl.col('mdd')>=-0.45)&(pl.col('sharpe')>=1.2)).height}/{pt.height}")

# DSR/PBO
nav = ledger.load_curve("T0279")
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
print(f"DSR {dsr['dsr']:.4f}(N={n}) | PBO {pbo['pbo']:.3f}")

# 解剖 + 連續窗
res = go(panel, feat, DS)
s = metrics.summarize(res.nav, res.trades, None)
print(f"解剖:trades {s['n_trades']} win {s['win_rate']:.0%} PF {s['profit_factor']:.2f} "
      f"medHold {s['med_days_held']:.0f}d turnover {s['turnover_ann']:.1f}x 前後曝險 {s['exposure']:.0%}")
for ws, we, tag, target in [("2019-01-02", "2026-07-07", "現代era", 0.559),
                             ("2014-11-03", "2026-07-07", "正2全史同窗", 0.377),
                             ("2012-01-02", "2026-07-07", "全跨度14.5y", None)]:
    p2, f2 = prep(ws, we)
    r = go(p2, f2, ws)
    sm = metrics.perf_stats(r.nav)
    ledger.log_trial(family="fullspan", name=f"{tag}_R3", hypothesis="R3 連續窗",
                     config={"R3": True}, window=f"{ws}..{we}", metrics=sm,
                     batch="R19-CERT", curve=r.nav)
    print(f"{tag}: {sm['cagr']:+.1%}/{sm['sharpe']:.2f}/{sm['mdd']:+.1%} | {sm['final_nav_ratio']:.0f}x "
          f"{'🏆>正2' if target and sm['cagr'] > target else ''}")
    if tag == "全跨度14.5y":
        yt = metrics.yearly_table(r.nav)
        print("逐年:", "  ".join(f"{y}:{v*100:+.0f}%" for y, v in zip(yt["year"], yt["ret"])))
print(f"total {time.time()-t0:.0f}s")
