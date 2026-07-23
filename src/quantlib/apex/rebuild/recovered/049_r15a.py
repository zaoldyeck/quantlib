"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:00:54.784Z(工具 Bash)
涵蓋 trials(6):r15a_adv10_stack, r15b_adv5, r15c_adv10_stack_t40, r15d_adv10_n6, r15e_adv10_stack_seq60, r15f_fullcycle_entry
"""
"""R15 — Pareto 疊加 × 更深流動性 × 全週期補倉(6 trials;預註冊:晉級 ≥85 或 2.2∧80)。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, metrics
from quantlib.apex.assemble import build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec

C = "company_code"
DS, DE = "2019-01-02", "2025-06-30"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}
W5a8 = {**W5, "rev_yoy_accel": 0.8}
t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DS, DE)
E10 = data.eligibility(panel, min_adv=10_000_000.0)
E5 = data.eligibility(panel, min_adv=5_000_000.0)
bench = data.benchmark_nav(con, DS, DE)

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

def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())

def go(name, w, *, fresh=5, tstop=30, trail=0.35, el=None, topn=8, entry_fresh_max=None):
    el = el if el is not None else elig
    fmax = entry_fresh_max if entry_fresh_max is not None else fresh
    pool = feat.filter(pl.col("rev_fresh_days") <= fmax)
    df = (pool.join(el.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = W_(df.with_columns(expr.alias("score")).select(["date", C, "score"]))
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    return run_trial(name=name, hypothesis="R15 Pareto 疊加", family="r15", batch="R15",
                     panel=panel, entries=e, exit_flags=f, bench=bench,
                     window=f"{DS}..{DE}", start=Date.fromisoformat(DS),
                     config={"name": name},
                     port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
                     exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop), verbose=False)

runs = [
    go("r15a_adv10_stack", W5a8, fresh=6, tstop=24, el=E10),
    go("r15b_adv5", W5, el=E5),
    go("r15c_adv10_stack_t40", W5a8, fresh=6, tstop=24, trail=0.40, el=E10),
    go("r15d_adv10_n6", W5, el=E10, topn=6),
    go("r15e_adv10_stack_seq60", dict(W5a8) | {"rev_seq": 0.6}, fresh=6, tstop=24, el=E10),
    go("r15f_fullcycle_entry", W5, entry_fresh_max=25),
]
cmp = pl.DataFrame([{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd"]} for r in runs]).sort("cagr", descending=True)
with pl.Config(tbl_rows=8, tbl_width_chars=100):
    print(cmp)
print("\nR2 基準:79.9/2.09/−32.9 | 晉級:≥85 或(Sharpe≥2.2∧≥80)| 目標 ≥100")
print(f"total {time.time()-t0:.1f}s")
