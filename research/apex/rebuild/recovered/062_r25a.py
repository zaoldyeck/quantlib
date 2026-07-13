"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:48:46.427Z(工具 Bash)
涵蓋 trials(7):r25a_R3_abs20, r25b_R3_abs25, r25c_rel_abs20, r25d_rel_abs25, r25e_rel_trail30, r25f_rel_halt, r25g_rel_abs25_halt
"""
"""R3 回撤解剖 + R25 防禦重訪(Pareto 判準:MDD 改善≥3pp 且 CAGR 犧牲≤2pp,
或 MDD 改善≥5pp 且犧牲≤4pp;Sharpe 降幅 ≤0.05)。"""
import time
import numpy as np
import polars as pl
from datetime import date as Date
from research.apex import data, ledger, metrics
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}
t0 = time.time()

# ── 回撤解剖(R3 曲線)──────────────────────────────────────────────
nav = ledger.load_curve("T0279").sort("date")
v = nav["nav"].to_numpy(); d = nav["date"].to_list()
runmax = np.maximum.accumulate(v)
dd = v / runmax - 1
print("R3 前 5 大回撤段:")
segs = []
i = 0
while i < len(dd):
    if dd[i] < -0.10:
        j = i
        trough, ti = dd[i], i
        while j < len(dd) and dd[j] < -0.02:
            if dd[j] < trough:
                trough, ti = dd[j], j
            j += 1
        segs.append((trough, d[ti], d[i], d[min(j, len(dd)-1)]))
        i = j
    else:
        i += 1
for trough, tdt, sdt, edt in sorted(segs)[:5]:
    print(f"  {trough:+.1%} 谷底 {tdt}(段 {sdt} → {edt})")

# ── R25 防禦重訪 ─────────────────────────────────────────────────────
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

# 崩盤狀態(0050 20d<-10% / 恢復 -3%)只停新倉
b = bench.sort("date").with_columns((pl.col("nav") / pl.col("nav").shift(20) - 1).alias("r20"))
rr = b["r20"].to_numpy()
state = np.zeros(len(rr), dtype=bool); on = False
for i, x in enumerate(rr):
    if not np.isnan(x):
        if not on and x < -0.10: on = True
        elif on and x > -0.03: on = False
    state[i] = on
halt = b.select("date").with_columns(pl.Series("h", state)).filter(pl.col("h")).select("date")

WREL = dict(W5) | {"accel_rel": 0.5}

def go(name, *, w, fresh, abs_stop=None, trail=0.35, use_halt=False):
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    df = (pool.join(E5.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    e, _ = entries_and_flags(sc, 5, 10**9)
    if use_halt:
        e = e.join(halt, on="date", how="anti")
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=trail, time_stop=30, abs_stop=abs_stop),
                   start=Date.fromisoformat(DS))
    s = metrics.summarize(res.nav, res.trades, bench)
    ledger.log_trial(family="mod_line", name=name, hypothesis="防禦重訪@新基座",
                     config={"name": name}, window=f"{DS}..{DE}", metrics=s,
                     batch="R25", curve=res.nav)
    return {"name": name, **{k: s[k] for k in ("cagr", "sharpe", "mdd")}}

runs = [
    go("r25a_R3_abs20", w=W5, fresh=6, abs_stop=0.20),
    go("r25b_R3_abs25", w=W5, fresh=6, abs_stop=0.25),
    go("r25c_rel_abs20", w=WREL, fresh=7, abs_stop=0.20),
    go("r25d_rel_abs25", w=WREL, fresh=7, abs_stop=0.25),
    go("r25e_rel_trail30", w=WREL, fresh=7, trail=0.30),
    go("r25f_rel_halt", w=WREL, fresh=7, use_halt=True),
    go("r25g_rel_abs25_halt", w=WREL, fresh=7, abs_stop=0.25, use_halt=True),
]
cmp = pl.DataFrame(runs).sort("mdd", descending=True)
with pl.Config(tbl_rows=9, tbl_width_chars=100):
    print(cmp)
print("\n對照:R3 113.7/2.44/−33.3 | n5+rel 121.7/2.55/−35.6")
print("Pareto 判準:MDD 改善≥3pp∧犧牲≤2pp,或 ≥5pp∧≤4pp;Sharpe 降 ≤0.05")
print(f"total {time.time()-t0:.1f}s")
