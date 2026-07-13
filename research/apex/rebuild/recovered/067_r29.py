"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T13:24:39.137Z(工具 Bash)
涵蓋 trials(2):r29_S_nextday, r29_S_sameday
"""
"""R29 — same-day 門檻出場 @ S(預註冊:P5 > 74.4 或 Martin > 16.4 ∧ P5 ≥ 72)。"""
import time
import numpy as np
import polars as pl
from datetime import date as Date
from research.apex import data, ledger, metrics
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
WREL = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
        "rev_seq": 0.5, "accel_rel": 0.5}
t0 = time.time()
con = data.connect()
DS, DE = "2019-01-02", "2025-06-30"
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
E5 = data.eligibility(panel, min_adv=5_000_000.0)
bench = data.benchmark_nav(con, DS, DE)

def kpi_v3(nav, n_boot=2000, block=21, seed=42):
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
            "ulcer": ulcer, "martin": cagr / ulcer, "mdd": float(dd.min())}

def go(name, *, same_day):
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(E5.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(WREL)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in WREL.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    e, _ = entries_and_flags(sc, 5, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                      loser_time_stop=15, same_day_exit=same_day),
                   start=Date.fromisoformat(DS))
    k = kpi_v3(res.nav)
    s = metrics.summarize(res.nav, res.trades, bench)
    ledger.log_trial(family="mod_line", name=f"r29_{name}", hypothesis="same-day 門檻出場",
                     config={"same_day": same_day}, window=f"{DS}..{DE}",
                     metrics=s | {"p5": k["p5"], "martin": k["martin"]},
                     batch="R29", curve=res.nav)
    return {"name": name, **{kk: round(vv, 4) for kk, vv in k.items()},
            "sortino": round(s["sortino"], 2)}

runs = [go("S_nextday", same_day=False), go("S_sameday", same_day=True)]
cmp = pl.DataFrame(runs)
with pl.Config(tbl_width_chars=115):
    print(cmp)
print("\nS 基準:P5 74.4 / Martin 16.4 / CAGR 120.9 / MDD −32.6 / Sortino 4.19")
print(f"total {time.time()-t0:.1f}s")
