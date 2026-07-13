"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T16:54:59.509Z(工具 Bash)
涵蓋 trials(40):f02_CONT-brk, f02_CONT-mom, f02_S+up-n4-t30, f02_S+up-n4-t35, f02_S+up-n4-t40, f02_S+up-n5-t30, f02_S+up-n5-t35, f02_S+up-n5-t40, f02_S+up-n6-t30, f02_S+up-n6-t35, f02_S+up-n6-t40, f02_S-n4-t30, f02_S-n4-t35, f02_S-n4-t40, f02_S-n5-t30, f02_S-n5-t35, f02_S-n5-t40, f02_S-n6-t30, f02_S-n6-t35, f02_S-n6-t40 …
"""
"""F02 — W3 結構網格:4 結構 × N{4,5,6} × trail{30,35,40} + fresh 掃 + 容器翻案。"""
import time
import numpy as np
import polars as pl
from datetime import date as Date
from research.apex import data, ledger
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DS, DE = "2023-07-10", "2026-07-09"
BASE = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
        "rev_seq": 0.5, "accel_rel": 0.5}
STRUCTS = {
    "S": BASE,
    "S-rel": {a: w for a, w in BASE.items() if a != "accel_rel"},
    "S+up": BASE | {"updays_20": 0.5},
    "S-rel+up": {a: w for a, w in BASE.items() if a != "accel_rel"} | {"updays_20": 0.5},
}
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
    return {"cagr": cagr, "p5": float(np.percentile(boot, 5)),
            "martin": cagr / ulcer, "mdd": float(dd.min())}

def go(name, wts, *, n=5, trail=0.35, fresh=7, cohort=True, batch="F02"):
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh) if cohort else feat
    df = pool.join(E5, on=["date", C], how="semi").drop_nulls(subset=list(wts))
    if cohort:
        df = df.filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date"))
    expr = None
    for c_, wt in wts.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    e, _ = entries_and_flags(sc, n, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date()) if cohort else pl.DataFrame(
        {"date": [], C: []}, schema={"date": pl.Date, C: pl.String})
    res = simulate(panel, e, exit_flags=(f if cohort else None), exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=n, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=trail, time_stop=30, loser_time_stop=15),
                   start=Date.fromisoformat(DS))
    k = kpi(res.nav)
    ledger.log_trial(family="f_line", name=f"f02_{name}", hypothesis="W3 結構網格",
                     config={"wts": list(wts), "n": n, "trail": trail, "fresh": fresh,
                             "cohort": cohort},
                     window=f"{DS}..{DE}",
                     metrics={kk: float(vv) for kk, vv in k.items()}, batch=batch, curve=res.nav)
    return {"name": name, "n": n, "trail": trail, "fresh": fresh,
            **{kk: round(float(vv), 3) for kk, vv in k.items()}}

rows = []
for sname, wts in STRUCTS.items():
    for n in [4, 5, 6]:
        for trail in [0.30, 0.35, 0.40]:
            rows.append(go(f"{sname}-n{n}-t{int(trail*100)}", wts, n=n, trail=trail))
out = pl.DataFrame(rows).sort("p5", descending=True)
with pl.Config(tbl_rows=12, tbl_width_chars=110):
    print(out.head(12))

# fresh 掃(top 結構上)
top = out.row(0, named=True)
wts_top = STRUCTS[top["name"].rsplit("-n", 1)[0]]
rows2 = [go(f"fresh{fr}-{top['name']}", wts_top, n=top["n"], trail=top["trail"], fresh=fr)
         for fr in [5, 10]]
print(pl.DataFrame(rows2))

# 容器翻案(非 revcycle,每日全池)
rows3 = [
    go("CONT-mom", {"high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}, cohort=False),
    go("CONT-brk", {"high_52w": 1.0, "close_pos_20": 1.0, "updays_20": 0.5}, cohort=False),
]
print(pl.DataFrame(rows3))
print(f"total {time.time()-t0:.0f}s")
