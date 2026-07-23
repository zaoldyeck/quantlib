"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:04:23.777Z(工具 Bash)
涵蓋 trials(5):r18a_n5, r18b_n4, r18c_n5_t40, r18d_n6_fresh4, r18e_n6_maxnew3
"""
"""R18 — adv5 深池集中極限(5 trials;晉級 ≥95 或 2.35∧90;目標 100)。"""
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
E5 = data.eligibility(panel, min_adv=5_000_000.0)
bench = data.benchmark_nav(con, DS, DE)

def go(name, *, topn=6, fresh=5, trail=0.35, max_new=5):
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    df = (pool.join(E5.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(W5)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in W5.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(DS).str.to_date())
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new),
                   exit_spec=ExitSpec(trailing_stop=trail, time_stop=30),
                   start=Date.fromisoformat(DS))
    s = metrics.summarize(res.nav, res.trades, bench)
    tid = ledger.log_trial(family="mod_line", name=name, hypothesis="深池集中極限",
                           config={"name": name}, window=f"{DS}..{DE}", metrics=s,
                           batch="R18", curve=res.nav)
    return {"trial_id": tid, "name": name, **{k: s[k] for k in ("cagr", "sharpe", "mdd")}}

runs = [
    go("r18a_n5"), go("r18b_n4", topn=4), go("r18c_n5_t40", topn=5, trail=0.40),
    go("r18d_n6_fresh4", fresh=4), go("r18e_n6_maxnew3", max_new=3),
]
runs[0]["name"] = "r18a_n5"; runs[0] = runs[0]
cmp = pl.DataFrame(runs).sort("cagr", descending=True)
with pl.Config(tbl_rows=7, tbl_width_chars=100):
    print(cmp)
print("\nadv5_n6 基準:90.7/2.18/−35.5 | 晉級 ≥95 或(2.35∧90)| 目標 ≥100")
print(f"total {time.time()-t0:.1f}s")
