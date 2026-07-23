"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:56:27.946Z(工具 Bash)
涵蓋 trials(2):r26b_skew, r26c_negdvol
"""
"""R26 — n5+rel 完整認證(舊時代/battery)+ Sortino 導向新軸 2 試。"""
import os, time
import numpy as np
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics, validate
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}
WREL = dict(W5) | {"accel_rel": 0.5}
t0 = time.time()
con = data.connect()

def prep(ws, we, extra_axes=False):
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
    feat = feat.join(rel, on=["date", C], how="left")
    if extra_axes:
        ax = (panel.sort([C, "date"])
              .with_columns((pl.col("close") / pl.col("close").shift(1) - 1).over(C).alias("ret"))
              .with_columns([
                  pl.col("ret").rolling_skew(60).over(C).alias("skew60"),
                  (-(pl.col("ret").clip(upper_bound=0.0).pow(2).rolling_mean(60).sqrt()))
                  .over(C).alias("neg_dvol"),
              ])
              .select(["date", C, "skew60", "neg_dvol"]))
        feat = feat.join(ax, on=["date", C], how="left")
    return panel, feat

def go(panel_, feat_, ws, *, w, seed=None):
    el = data.eligibility(panel_, min_adv=5_000_000.0)
    pool = feat_.filter(pl.col("rev_fresh_days") <= 7)
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
    e, _ = entries_and_flags(sc, 5, 10**9)
    f = feat_.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel_, e, exit_flags=f, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                    exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30),
                    start=Date.fromisoformat(ws))

# 舊時代確認
OW, OE = "2012-01-02", "2018-12-28"
p_old, f_old = prep(OW, OE)
so = metrics.perf_stats(go(p_old, f_old, OW, w=WREL).nav)
print(f"n5+rel 舊時代 2012-18:{so['cagr']:+.1%}/{so['sharpe']:.2f}/Sortino {so['sortino']:.2f} "
      f"({'✅≥45%' if so['cagr'] >= 0.45 else '❌'};R3 同段 51.3)")

# battery(dev)
DS, DE = "2019-01-02", "2025-06-30"
panel, feat = prep(DS, DE, extra_axes=True)
nav = ledger.load_curve("T0311")
bs = validate.block_bootstrap_cagr(nav)
n = ledger.all_trials().height
rng = np.random.default_rng(83)
perm = [metrics.perf_stats(go(panel, feat, DS, w=WREL, seed=rng).nav)["cagr"] for _ in range(100)]
p = float((np.array(perm) >= 1.217).mean())
print(f"battery:bootstrap CI[{bs['ci_lo']:+.1%},{bs['ci_hi']:+.1%}] | perm p={p:.3f} | N={n}")

# Sortino 導向新軸
bench = data.benchmark_nav(con, DS, DE)
for name, w in [("r26b_skew", dict(WREL) | {"skew60": 0.25}),
                ("r26c_negdvol", dict(WREL) | {"neg_dvol": 0.25})]:
    res = go(panel, feat, DS, w=w)
    s = metrics.summarize(res.nav, res.trades, bench)
    ledger.log_trial(family="mod_line", name=name, hypothesis="Sortino 導向軸",
                     config={"name": name}, window=f"{DS}..{DE}", metrics=s,
                     batch="R26", curve=res.nav)
    print(f"{name}: {s['cagr']:+.1%}/{s['sharpe']:.2f}/Sortino {s['sortino']:.2f}/MDD {s['mdd']:+.1%}")
print(f"total {time.time()-t0:.0f}s")
