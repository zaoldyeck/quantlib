"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:40:16.703Z(工具 Bash)
涵蓋 trials(6):r23a_n4_rel05, r23b_n4_rel10, r23c_n4_relrepl, r23d_n5_rel05_f7, r23e_n5_rel075, r23f_n4_rel05_f6
"""
"""R23 — 同業相對加速 × n4 堆疊(6 trials;晉級 ≥128 或 2.55∧120;晉級者跑舊時代)。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

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
    feat = (feat.sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C, strategy="backward", tolerance="70d")
            .sort([C, "date"]))
    tax = con.sql("SELECT company_code, effective_date, industry FROM industry_taxonomy_pit "
                  "WHERE industry IS NOT NULL ORDER BY effective_date").pl()
    fx = (feat.select(["date", C, "rev_yoy_accel"]).drop_nulls()
          .sort("date")
          .join_asof(tax.sort("effective_date"), left_on="date", right_on="effective_date",
                     by=C, strategy="backward")
          .drop_nulls(subset=["industry"]))
    ind_med = fx.group_by(["date", "industry"]).agg(pl.col("rev_yoy_accel").median().alias("m"))
    rel = (fx.join(ind_med, on=["date", "industry"], how="left")
           .with_columns((pl.col("rev_yoy_accel") - pl.col("m")).alias("accel_rel"))
           .select(["date", C, "accel_rel"]))
    return panel, feat.join(rel, on=["date", C], how="left")

def go(panel_, feat_, ws, *, topn, fresh, w, log_name=None):
    el = data.eligibility(panel_, min_adv=5_000_000.0)
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
    res = simulate(panel_, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=topn, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30),
                   start=Date.fromisoformat(ws))
    return res

DS, DE = "2019-01-02", "2025-06-30"
panel, feat = prep(DS, DE)
bench = data.benchmark_nav(con, DS, DE)
W4s = dict(W5) | {"rev_seq": 0.4}

def trial(name, **kw):
    res = go(panel, feat, DS, **kw)
    s = metrics.summarize(res.nav, res.trades, bench)
    ledger.log_trial(family="mod_line", name=name, hypothesis="rel×n4 收割",
                     config={"name": name}, window=f"{DS}..{DE}", metrics=s,
                     batch="R23", curve=res.nav)
    return {"name": name, **{k: s[k] for k in ("cagr", "sharpe", "mdd")}}

runs = [
    trial("r23a_n4_rel05", topn=4, fresh=7, w=dict(W4s) | {"accel_rel": 0.5}),
    trial("r23b_n4_rel10", topn=4, fresh=7, w=dict(W4s) | {"accel_rel": 1.0}),
    trial("r23c_n4_relrepl", topn=4, fresh=7,
          w={k: v for k, v in W4s.items() if k != "rev_yoy_accel"} | {"accel_rel": 1.0}),
    trial("r23d_n5_rel05_f7", topn=5, fresh=7, w=dict(W5) | {"accel_rel": 0.5}),
    trial("r23e_n5_rel075", topn=5, fresh=6, w=dict(W5) | {"accel_rel": 0.75}),
    trial("r23f_n4_rel05_f6", topn=4, fresh=6, w=dict(W4s) | {"accel_rel": 0.5}),
]
cmp = pl.DataFrame(runs).sort("cagr", descending=True)
with pl.Config(tbl_rows=8, tbl_width_chars=100):
    print(cmp)

best = cmp.row(0, named=True)
if best["cagr"] >= 1.28 or (best["sharpe"] >= 2.55 and best["cagr"] >= 1.20):
    OW, OE = "2012-01-02", "2018-12-28"
    p_old, f_old = prep(OW, OE)
    kws = {"r23a_n4_rel05": {"topn": 4, "fresh": 7, "w": dict(W4s) | {"accel_rel": 0.5}},
           "r23b_n4_rel10": {"topn": 4, "fresh": 7, "w": dict(W4s) | {"accel_rel": 1.0}},
           "r23c_n4_relrepl": {"topn": 4, "fresh": 7,
                               "w": {k: v for k, v in W4s.items() if k != "rev_yoy_accel"} | {"accel_rel": 1.0}},
           "r23d_n5_rel05_f7": {"topn": 5, "fresh": 7, "w": dict(W5) | {"accel_rel": 0.5}},
           "r23e_n5_rel075": {"topn": 5, "fresh": 6, "w": dict(W5) | {"accel_rel": 0.75}},
           "r23f_n4_rel05_f6": {"topn": 4, "fresh": 6, "w": dict(W4s) | {"accel_rel": 0.5}}}[best["name"]]
    so = metrics.perf_stats(go(p_old, f_old, OW, **kws).nav)
    print(f"\n晉級者 {best['name']} 舊時代:{so['cagr']:+.1%}/{so['sharpe']:.2f} "
          f"({'✅≥45%' if so['cagr'] >= 0.45 else '❌'})")
print("\n晉級 ≥128 或(2.55∧120)| n4 cell 對照 126.1/2.40")
print(f"total {time.time()-t0:.1f}s")
