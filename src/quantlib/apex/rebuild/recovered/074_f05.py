"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T17:15:25.351Z(工具 Bash)
涵蓋 trials(3):f05_CONT_precursor, f05_S+consol, f05_S_base
"""
"""F05b/c — 前兆軸轉化:S+consolidation 配對;前兆容器獨立引擎。"""
import time
import numpy as np
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
S_WTS = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
         "rev_seq": 0.5, "accel_rel": 0.5}
DS, DE = "2023-07-10", "2026-07-09"
t0 = time.time()
con = data.connect()
panel, feat, _ = build_features(con, DS, DE)
rev = (data.load_monthly_revenue(con, DE)
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
extra = (panel.sort([C, "date"])
         .with_columns(
             ((pl.col("close").rolling_max(60) - pl.col("close").rolling_min(60))
              / (pl.col("close").rolling_mean(60) + 1e-9)).over(C).alias("consolidation_60"))
         .select(["date", C, "consolidation_60"]))
feat = feat.join(extra, on=["date", C], how="left")
E5 = (data.eligibility(panel, min_adv=5_000_000.0)
      .filter(pl.col("eligible")).select(["date", C]))

def kpi(nav, n_boot=2000, block=21, seed=42):
    v = nav.sort("date")["nav"].to_numpy(); d = nav.sort("date")["date"].to_numpy()
    r = v[1:] / v[:-1] - 1; t = len(r)
    yrs = (d[-1] - d[0]).astype("timedelta64[D]").astype(float) / 365.25
    cagr = (v[-1] / v[0]) ** (1 / yrs) - 1
    rng = np.random.default_rng(seed)
    nb = int(np.ceil(t / block))
    starts = rng.integers(0, t, size=(n_boot, nb))
    idx = (starts[:, :, None] + np.arange(block)[None, None, :]) % t
    boot = np.prod(1.0 + r[idx.reshape(n_boot, -1)[:, :t]], axis=1) ** (252.0 / t) - 1.0
    dd = v / np.maximum.accumulate(v) - 1
    return {"cagr": cagr, "p5": float(np.percentile(boot, 5)),
            "martin": cagr / float(np.sqrt(np.mean(dd**2))), "mdd": float(dd.min())}

def go(name, wts, *, cohort=True, gate=True):
    pool = feat.filter(pl.col("rev_fresh_days") <= 7) if cohort else feat
    df = pool.join(E5, on=["date", C], how="semi").drop_nulls(subset=list(wts))
    if gate:
        df = df.filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date"))
    expr = None
    for c_, wt in wts.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    e, _ = entries_and_flags(sc, 5, 10**9)
    f = None
    if cohort:
        f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
            pl.col("date") >= pl.lit(DS).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15),
                   start=Date.fromisoformat(DS))
    k = kpi(res.nav)
    ledger.log_trial(family="f_line", name=f"f05_{name}", hypothesis="暴漲前兆軸轉化",
                     config={"wts": list(wts), "cohort": cohort}, window=f"{DS}..{DE}",
                     metrics={kk: float(vv) for kk, vv in k.items()}, batch="F05", curve=res.nav)
    return res.nav.select(["date", "nav"]), {"name": name, **{kk: round(vv, 3) for kk, vv in k.items()}}

nav_s, r_s = go("S_base", S_WTS)
nav_b, r_b = go("S+consol", S_WTS | {"consolidation_60": 0.5})
nav_c, r_c = go("CONT_precursor", {"consolidation_60": 1.0, "mom_126_5": 1.0, "rev_yoy": 0.5},
                cohort=False, gate=False)
print(pl.DataFrame([r_s, r_b, r_c]))

j = nav_b.join(nav_s, on="date", suffix="_s")
rb = np.log(j["nav"].to_numpy()[1:] / j["nav"].to_numpy()[:-1])
rs = np.log(j["nav_s"].to_numpy()[1:] / j["nav_s"].to_numpy()[:-1])
d = rb - rs; t_ = len(d)
rng = np.random.default_rng(42); nb = int(np.ceil(t_/21))
starts = rng.integers(0, t_, size=(4000, nb))
idx = (starts[:, :, None] + np.arange(21)[None, None, :]) % t_
ann = d[idx.reshape(4000, -1)[:, :t_]].mean(axis=1) * 252
print(f"配對 S+consol − S:{d.mean()*252:+.2%}/年  CI [{np.percentile(ann,2.5):+.2%}, {np.percentile(ann,97.5):+.2%}]")
print(f"total {time.time()-t0:.0f}s")
