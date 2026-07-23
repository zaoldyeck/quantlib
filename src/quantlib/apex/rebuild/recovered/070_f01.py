"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T16:53:49.871Z(工具 Bash)
涵蓋 trials(16):f01_AOI+donchian_60, f01_AOI+dy, f01_AOI+frn_60, f01_AOI+fvg_20, f01_AOI+hvn_dist, f01_AOI+lowvol_60, f01_AOI+range_pos_60, f01_AOI+rev_yoy, f01_AOI+updays_20, f01_BASE_S, f01_LOO-accel_rel, f01_LOO-close_pos_20, f01_LOO-high_52w, f01_LOO-mom_126_5, f01_LOO-rev_seq, f01_LOO-rev_yoy_accel
"""
"""F01 — 3 年窗(2023-07-10 → 2026-07-09)因子重掃描:S 基座 + LOO×6 + AOI×9。"""
import time
import numpy as np
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DS, DE = "2023-07-10", "2026-07-09"
BASE = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
        "rev_seq": 0.5, "accel_rel": 0.5}
AOI = ["hvn_dist", "range_pos_60", "updays_20", "fvg_20", "donchian_60",
       "rev_yoy", "frn_60", "dy", "lowvol_60"]
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
E5 = (data.eligibility(panel, min_adv=5_000_000.0)
      .filter(pl.col("eligible")).select(["date", C]))
print(f"prep {time.time()-t0:.0f}s")

def kpi(nav, n_boot=2000, block=21, seed=42):
    nav = nav.sort("date")
    v = nav["nav"].to_numpy(); d = nav["date"].to_numpy()
    r = v[1:] / v[:-1] - 1; t = len(r)
    yrs = (d[-1] - d[0]).astype("timedelta64[D]").astype(float) / 365.25
    cagr = (v[-1] / v[0]) ** (1 / yrs) - 1
    rng = np.random.default_rng(seed)
    nb = int(np.ceil(t / block))
    starts = rng.integers(0, t, size=(n_boot, nb))
    idx = (starts[:, :, None] + np.arange(block)[None, None, :]) % t
    boot = np.prod(1.0 + r[idx.reshape(n_boot, -1)[:, :t]], axis=1) ** (252.0 / t) - 1.0
    runmax = np.maximum.accumulate(v); dd = v / runmax - 1
    ulcer = float(np.sqrt(np.mean(dd ** 2)))
    dr = r[r < 0]
    sortino = r.mean() / (np.sqrt((dr**2).sum() / len(r)) + 1e-12) * np.sqrt(252)
    return {"cagr": cagr, "p5": float(np.percentile(boot, 5)),
            "martin": cagr / ulcer, "mdd": float(dd.min()), "sortino": sortino}

def go(name, wts):
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(E5, on=["date", C], how="semi").drop_nulls(subset=list(wts))
          .filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    expr = None
    for c_, wt in wts.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    e, _ = entries_and_flags(sc, 5, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15),
                   start=Date.fromisoformat(DS))
    k = kpi(res.nav)
    ledger.log_trial(family="f_line", name=f"f01_{name}", hypothesis="W3 因子重掃",
                     config={"wts": {a: w for a, w in wts.items()}}, window=f"{DS}..{DE}",
                     metrics={kk: float(vv) for kk, vv in k.items()}, batch="F01", curve=res.nav)
    return {"name": name, **{kk: round(float(vv), 3) for kk, vv in k.items()}}

rows = [go("BASE_S", BASE)]
for ax in BASE:
    rows.append(go(f"LOO-{ax}", {a: w for a, w in BASE.items() if a != ax}))
for ax in AOI:
    rows.append(go(f"AOI+{ax}", BASE | {ax: 0.5}))
out = pl.DataFrame(rows).sort("p5", descending=True)
with pl.Config(tbl_rows=20, tbl_width_chars=110):
    print(out)
print(f"total {time.time()-t0:.0f}s")
