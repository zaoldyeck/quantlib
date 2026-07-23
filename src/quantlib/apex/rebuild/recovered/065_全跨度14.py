"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T13:04:28.123Z(工具 Bash)
涵蓋 trials(7):全跨度14.5y_R3, 全跨度14.5y_S, 全跨度14.5y_v6, 現代era_R3, 現代era_S, 現代era_r08a, 現代era_v6
"""
"""apex_revcycle_S 認證:舊時代 + 擾動(lts 軸)+ bootstrap/perm + 連續窗。"""
import time
import numpy as np
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics, validate
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
WREL = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
        "rev_seq": 0.5, "accel_rel": 0.5}
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
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C, strategy="backward", tolerance="70d")
            .sort([C, "date"]))
    tax = con.sql("SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
                  "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
    fx = (feat.select(["date", C, "rev_yoy_accel"]).drop_nulls().sort("date")
          .join_asof(tax.sort("effective_date"), left_on="date", right_on="effective_date",
                     by=C, strategy="backward").drop_nulls(subset=["industry"]))
    ind_med = fx.group_by(["date", "industry"]).agg(pl.col("rev_yoy_accel").median().alias("m"))
    rel = (fx.join(ind_med, on=["date", "industry"], how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("m")).alias("accel_rel"))
           .select(["date", C, "accel_rel"]))
    return panel, feat.join(rel, on=["date", C], how="left")

def go(panel_, feat_, ws, *, fresh=7, topn=5, trail=0.35, tstop=30, lts=15, seed=None):
    el = data.eligibility(panel_, min_adv=5_000_000.0)
    pool = feat_.filter(pl.col("rev_fresh_days") <= fresh)
    df = (pool.join(el.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(WREL)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in WREL.items():
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
                    port_spec=PortSpec(n_slots=topn, max_new_per_day=2),
                    exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop,
                                       loser_time_stop=lts),
                    start=Date.fromisoformat(ws))

# 舊時代
OW, OE = "2012-01-02", "2018-12-28"
p_old, f_old = prep(OW, OE)
so = metrics.perf_stats(go(p_old, f_old, OW).nav)
print(f"S 舊時代 2012-18:{so['cagr']:+.1%}/{so['sharpe']:.2f}/Sortino {so['sortino']:.2f}"
      f"(n5+rel 43.4、R3 51.3)")

# dev 擾動(lts 12/18 ± 其他)
DS, DE = "2019-01-02", "2025-06-30"
panel, feat = prep(DS, DE)
for nm, kw in [("lts12", {"lts": 12}), ("lts18", {"lts": 18}),
               ("fresh6", {"fresh": 6}), ("n4", {"topn": 4}), ("n6", {"topn": 6}),
               ("trail30", {"trail": 0.30}), ("trail40", {"trail": 0.40})]:
    s = metrics.perf_stats(go(panel, feat, DS, **kw).nav)
    print(f"  {nm:8s} {s['cagr']:+.1%}/So {s['sortino']:.2f}/MDD {s['mdd']:+.1%}")

# bootstrap + perm
res = go(panel, feat, DS)
s = metrics.perf_stats(res.nav)
bs = validate.block_bootstrap_cagr(res.nav)
rng = np.random.default_rng(89)
perm = [metrics.perf_stats(go(panel, feat, DS, seed=rng).nav)["sortino"] for _ in range(100)]
p = float((np.array(perm) >= s["sortino"]).mean())
print(f"bootstrap CI[{bs['ci_lo']:+.1%},{bs['ci_hi']:+.1%}] | Sortino-perm p={p:.3f}"
      f"(null 中位 {np.median(perm):.2f})")

# 連續窗
for ws, we, tag in [("2019-01-02", "2026-07-07", "現代era"),
                     ("2012-01-02", "2026-07-07", "全跨度14.5y")]:
    p2, f2 = prep(ws, we)
    r = go(p2, f2, ws)
    sm = metrics.perf_stats(r.nav)
    ledger.log_trial(family="fullspan", name=f"{tag}_S", hypothesis="S 連續窗",
                     config={"S": True}, window=f"{ws}..{we}", metrics=sm,
                     batch="R27-CERT", curve=r.nav)
    print(f"{tag}: {sm['cagr']:+.1%}/Sh {sm['sharpe']:.2f}/So {sm['sortino']:.2f}/"
          f"MDD {sm['mdd']:+.1%} | {sm['final_nav_ratio']:.0f}x")
print(f"total {time.time()-t0:.0f}s")
