"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T16:56:23.538Z(工具 Bash)
涵蓋 trials(3):f03_S+up_fullspan, f03_S-rel_fullspan, f03_S_fullspan
"""
"""F03 — 認證:S vs S-rel vs S+up 配對 bootstrap + seed 敏感度 + 擾動 + 窗外披露。"""
import time
import numpy as np
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
BASE = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
        "rev_seq": 0.5, "accel_rel": 0.5}
CFGS = {
    "S": BASE,
    "S-rel": {a: w for a, w in BASE.items() if a != "accel_rel"},
    "S+up": BASE | {"updays_20": 0.5},
    "S+up03": BASE | {"updays_20": 0.3},
    "S+up07": BASE | {"updays_20": 0.7},
}
t0 = time.time()
con = data.connect()

def prep(ds, de):
    panel, feat, _ = build_features(con, ds, de)
    rev = (data.load_monthly_revenue(con, de)
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
    E5 = (data.eligibility(panel, min_adv=5_000_000.0)
          .filter(pl.col("eligible")).select(["date", C]))
    return panel, feat, E5

def run(panel, feat, E5, wts, start):
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(E5, on=["date", C], how="semi").drop_nulls(subset=list(wts))
          .filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    expr = None
    for c_, wt in wts.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(start).str.to_date())
    e, _ = entries_and_flags(sc, 5, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(start).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15),
                   start=Date.fromisoformat(start))
    return res.nav.select(["date", "nav"]).sort("date")

def p5_multi(nav, seeds=(42, 7, 99, 2024, 31415), block=21, n_boot=2000):
    v = nav["nav"].to_numpy(); r = v[1:] / v[:-1] - 1; t = len(r)
    out = []
    for seed in seeds:
        rng = np.random.default_rng(seed)
        nb = int(np.ceil(t / block))
        starts = rng.integers(0, t, size=(n_boot, nb))
        idx = (starts[:, :, None] + np.arange(block)[None, None, :]) % t
        boot = np.prod(1.0 + r[idx.reshape(n_boot, -1)[:, :t]], axis=1) ** (252.0 / t) - 1.0
        out.append(np.percentile(boot, 5))
    return np.array(out)

def paired_diff(nav_a, nav_b, block=21, n_boot=4000, seed=42):
    """log 日報酬差的 block bootstrap 年化差 CI(a − b)。"""
    j = nav_a.join(nav_b, on="date", suffix="_b").sort("date")
    ra = np.log(j["nav"].to_numpy()[1:] / j["nav"].to_numpy()[:-1])
    rb = np.log(j["nav_b"].to_numpy()[1:] / j["nav_b"].to_numpy()[:-1])
    d = ra - rb; t = len(d)
    rng = np.random.default_rng(seed)
    nb = int(np.ceil(t / block))
    starts = rng.integers(0, t, size=(n_boot, nb))
    idx = (starts[:, :, None] + np.arange(block)[None, None, :]) % t
    ann = d[idx.reshape(n_boot, -1)[:, :t]].mean(axis=1) * 252
    return {"mean": float(d.mean() * 252), "lo": float(np.percentile(ann, 2.5)),
            "hi": float(np.percentile(ann, 97.5)),
            "p_neg": float((ann <= 0).mean())}

# --- W3 窗:seed 敏感度 + 配對 ---
panel3, feat3, E53 = prep("2023-07-10", "2026-07-09")
navs3 = {k: run(panel3, feat3, E53, w, "2023-07-10") for k, w in CFGS.items()}
print(f"W3 sims {time.time()-t0:.0f}s")
for k in CFGS:
    p5s = p5_multi(navs3[k])
    print(f"{k:8s} P5 across seeds: {p5s.round(3)}  mean {p5s.mean():.3f}")
for cand in ["S-rel", "S+up"]:
    d = paired_diff(navs3[cand], navs3["S"])
    print(f"配對 {cand} − S:年化差 {d['mean']:+.2%}  CI [{d['lo']:+.2%}, {d['hi']:+.2%}]  P(≤0) {d['p_neg']:.2f}")

# --- 窗外披露:全期 sim 切片 ---
panelF, featF, E5F = prep("2012-01-01", "2026-07-09")
def seg_cagr(nav, a, b):
    s = nav.filter((pl.col("date") >= a) & (pl.col("date") < b))
    v = s["nav"].to_numpy()
    yrs = (s["date"][-1] - s["date"][0]).days / 365.25
    return (v[-1] / v[0]) ** (1 / yrs) - 1
print(f"\n窗外披露(全期連續模擬切片):")
for k in ["S", "S-rel", "S+up"]:
    nv = run(panelF, featF, E5F, CFGS[k], "2012-07-02")
    old = seg_cagr(nv, Date(2012, 7, 2), Date(2019, 1, 1))
    mid = seg_cagr(nv, Date(2019, 1, 1), Date(2023, 7, 10))
    w3 = seg_cagr(nv, Date(2023, 7, 10), Date(2026, 7, 10))
    ledger.log_trial(family="f_line", name=f"f03_{k}_fullspan", hypothesis="W3 認證窗外披露",
                     config={"wts": list(CFGS[k])}, window="2012-07-02..2026-07-09",
                     metrics={"old_era": float(old), "mid": float(mid), "w3": float(w3)},
                     batch="F03", curve=nv)
    print(f"{k:8s} 舊時代12-18 {old:+.1%}  中期19-23H1 {mid:+.1%}  W3 {w3:+.1%}")
print(f"\ntotal {time.time()-t0:.0f}s")
