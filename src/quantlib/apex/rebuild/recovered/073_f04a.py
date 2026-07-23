"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T17:06:27.614Z(工具 Bash)
涵蓋 trials(2):f04a_qtr_standalone, f04b_S_plus_gm
"""
"""F04 — 季報揭露事件池:a) 獨立引擎 b) 疊加 S 第七軸。3 年窗 + 全期披露。"""
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
t0 = time.time()
con = data.connect()

def quarterly_events():
    """季報事件表:(code, avail, gm_d, opm_d, ni_d)。avail = deadline 隔日。"""
    q = con.sql("SELECT company_code, year, quarter, gross_margin_q, operating_margin_q, "
                "ni_q, rev_q FROM raw_quarterly").pl()
    q = (q.sort([C, "year", "quarter"])
         .with_columns([
             (pl.col("gross_margin_q") - pl.col("gross_margin_q").shift(4)).over(C).alias("gm_d"),
             (pl.col("operating_margin_q") - pl.col("operating_margin_q").shift(4)).over(C).alias("opm_d"),
             ((pl.col("ni_q") - pl.col("ni_q").shift(4))
              / (pl.col("rev_q").abs() + 1e3)).over(C).alias("ni_d"),
         ])
         .with_columns(
             pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 16))
             .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 15))
             .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 15))
             .otherwise(pl.date(pl.col("year") + 1, 4, 1)).alias("avail"))
         .select([C, "avail", "gm_d", "opm_d", "ni_d"])
         .drop_nulls().sort("avail"))
    return q

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

    qe = quarterly_events()
    # fq_fresh_days:交易日距(avail 映射至其後首個交易日 index)
    dates = feat.select("date").unique().sort("date").with_row_index("didx")
    feat = feat.join(dates, on="date", how="left")
    qe2 = (qe.sort("avail")
           .join_asof(dates.rename({"date": "avail_td", "didx": "aidx"}).sort("avail_td"),
                      left_on="avail", right_on="avail_td", strategy="forward")
           .drop_nulls(subset=["aidx"]).sort("avail"))
    feat = (feat.sort("date")
            .join_asof(qe2.select([C, "avail", "gm_d", "opm_d", "ni_d", "aidx"]),
                       left_on="date", right_on="avail", by=C, strategy="backward",
                       tolerance="130d")
            .with_columns((pl.col("didx") - pl.col("aidx")).alias("fq_fresh"))
            .sort([C, "date"]))
    E5 = (data.eligibility(panel, min_adv=5_000_000.0)
          .filter(pl.col("eligible")).select(["date", C]))
    return panel, feat, E5

def kpi(nav, n_boot=2000, block=21, seed=42):
    v = nav.sort("date")["nav"].to_numpy()
    d = nav.sort("date")["date"].to_numpy()
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

def sim(panel, feat, E5, sc_df, start, *, n=5, trail=0.35, tstop=30, stale_expr=None):
    e, _ = entries_and_flags(sc_df, n, 10**9)
    f = (feat.filter(stale_expr).select(["date", C])
         .filter(pl.col("date") >= pl.lit(start).str.to_date()))
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=n, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop, loser_time_stop=15),
                   start=Date.fromisoformat(start))
    return res.nav.select(["date", "nav"]).sort("date")

def build_score(feat, E5, wts, start, *, fresh_col="rev_fresh_days", fresh_max=7, gate=True):
    pool = feat.filter(pl.col(fresh_col) <= fresh_max)
    df = pool.join(E5, on=["date", C], how="semi").drop_nulls(subset=list(wts))
    if gate:
        df = df.filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date"))
    expr = None
    for c_, wt in wts.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    return (df.with_columns(expr.alias("score")).select(["date", C, "score"])
            .filter(pl.col("date") >= pl.lit(start).str.to_date()))

DS3 = "2023-07-10"
panel, feat, E5 = prep(DS3, "2026-07-09")
print(f"prep {time.time()-t0:.0f}s;季報事件覆蓋:",
      feat.filter(pl.col("fq_fresh").is_between(0, 10)).height, "row-days")

QA = {"gm_d": 1.0, "opm_d": 1.0, "ni_d": 0.5, "high_52w": 1.0, "close_pos_20": 1.0}
rows = []
# F04a 獨立引擎
sc_a = build_score(feat, E5, QA, DS3, fresh_col="fq_fresh", fresh_max=10)
nav_a = sim(panel, feat, E5, sc_a, DS3, tstop=45,
            stale_expr=pl.col("fq_fresh") >= 40)
ka = kpi(nav_a); rows.append({"name": "F04a 季報獨立", **{k: round(v,3) for k,v in ka.items()}})
# S 基準 + F04b 疊加
sc_s = build_score(feat, E5, S_WTS, DS3)
nav_s = sim(panel, feat, E5, sc_s, DS3, stale_expr=pl.col("rev_fresh_days") >= 26)
ks = kpi(nav_s); rows.append({"name": "S 基準", **{k: round(v,3) for k,v in ks.items()}})
sc_b = build_score(feat, E5, S_WTS | {"gm_d": 0.5}, DS3)
nav_b = sim(panel, feat, E5, sc_b, DS3, stale_expr=pl.col("rev_fresh_days") >= 26)
kb = kpi(nav_b); rows.append({"name": "F04b S+gm軸", **{k: round(v,3) for k,v in kb.items()}})
print(pl.DataFrame(rows))

# 相關性(F04a vs S)與配對(F04b vs S)
j = nav_a.join(nav_s, on="date", suffix="_s")
ra = np.log(j["nav"].to_numpy()[1:] / j["nav"].to_numpy()[:-1])
rs = np.log(j["nav_s"].to_numpy()[1:] / j["nav_s"].to_numpy()[:-1])
print(f"F04a↔S 日報酬相關:{np.corrcoef(ra, rs)[0,1]:.2f}")
jb = nav_b.join(nav_s, on="date", suffix="_s")
rb = np.log(jb["nav"].to_numpy()[1:] / jb["nav"].to_numpy()[:-1])
rs2 = np.log(jb["nav_s"].to_numpy()[1:] / jb["nav_s"].to_numpy()[:-1])
d = rb - rs2; t_ = len(d)
rng = np.random.default_rng(42); nb = int(np.ceil(t_/21))
starts = rng.integers(0, t_, size=(4000, nb))
idx = (starts[:, :, None] + np.arange(21)[None, None, :]) % t_
ann = d[idx.reshape(4000, -1)[:, :t_]].mean(axis=1) * 252
print(f"配對 F04b − S:年化差 {d.mean()*252:+.2%}  CI [{np.percentile(ann,2.5):+.2%}, {np.percentile(ann,97.5):+.2%}]")

for nm, nv, kk in [("f04a_qtr_standalone", nav_a, ka), ("f04b_S_plus_gm", nav_b, kb)]:
    ledger.log_trial(family="f_line", name=nm, hypothesis="季報揭露事件池(利潤率層新資訊)",
                     config={}, window=f"{DS3}..2026-07-09",
                     metrics={k2: float(v2) for k2, v2 in kk.items()}, batch="F04", curve=nv)
print(f"total {time.time()-t0:.0f}s")
