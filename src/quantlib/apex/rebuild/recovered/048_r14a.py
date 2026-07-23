"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T11:59:27.499Z(工具 Bash)
涵蓋 trials(8):r14a_surge, r14b_samom, r14c_surge_swap, r14d_gridstack, r14e_a_plus_d, r14f_sixaxis, r14g_adv10, r14h_surge_adv10
"""
"""R14 — 驚奇軸 × 流動性 × 堆疊(8 trials,R2 scaffold)。"""
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
t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DS, DE)
elig10 = data.eligibility(panel, min_adv=10_000_000.0).filter(
    pl.col("date") >= pl.lit(DS).str.to_date())
bench = data.benchmark_nav(con, DS, DE)

rev = (data.load_monthly_revenue(con, DE)
       .sort([C, "year", "month"])
       .with_columns([
           pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("avail"),
           (pl.col("monthly_revenue").rolling_sum(3)
            / pl.col("monthly_revenue").rolling_sum(3).shift(3) - 1).over(C).alias("rev_seq"),
           (pl.col("monthly_revenue") / pl.col("monthly_revenue").shift(1).rolling_mean(3) - 1)
           .over(C).alias("rev_surge"),
           (pl.col("monthly_revenue") / pl.col("monthly_revenue").shift(1)).over(C).alias("_mom_r"),
       ])
       .with_columns(
           (pl.col("_mom_r")
            / pl.col("_mom_r").shift(12).rolling_median(3, min_samples=2).over(C) - 1)
           .alias("sa_mom")  # 當月MoM ÷ 過去2-3年同月MoM中位(shift12 rolling3 → 12/24/36月前)
       )
       .select([C, "avail", "rev_seq", "rev_surge", "sa_mom"]).sort("avail"))
feat = (feat.sort("date")
        .join_asof(rev, left_on="date", right_on="avail", by=C, strategy="backward", tolerance="70d")
        .sort([C, "date"]))

def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())

def go(name, w, *, fresh=5, tstop=30, trail=0.35, el=None):
    el = el if el is not None else elig
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    df = (pool.join(el.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = W_(df.with_columns(expr.alias("score")).select(["date", C, "score"]))
    e, _ = entries_and_flags(sc, 8, 10**9)
    f = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    return run_trial(name=name, hypothesis="R14 CAGR100 攻堅", family="r14", batch="R14",
                     panel=panel, entries=e, exit_flags=f, bench=bench,
                     window=f"{DS}..{DE}", start=Date.fromisoformat(DS),
                     config={"name": name},
                     port_spec=PortSpec(n_slots=8, max_new_per_day=5),
                     exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop), verbose=False)

W5a8 = {**W5, "rev_yoy_accel": 0.8}
runs = [
    go("r14a_surge", dict(W5) | {"rev_surge": 0.5}),
    go("r14b_samom", dict(W5) | {"sa_mom": 0.5}),
    go("r14c_surge_swap", {k: v for k, v in W5.items() if k != "mom_126_5"} | {"rev_surge": 0.5}),
    go("r14d_gridstack", W5a8, fresh=6, tstop=24),
    go("r14e_a_plus_d", dict(W5a8) | {"rev_surge": 0.5}, fresh=6, tstop=24),
    go("r14f_sixaxis", dict(W5) | {"rev_surge": 0.5, "sa_mom": 0.5}),
    go("r14g_adv10", W5, el=elig10),
    go("r14h_surge_adv10", dict(W5) | {"rev_surge": 0.5}, el=elig10),
]
cmp = pl.DataFrame([{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd"]} for r in runs]).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=100):
    print(cmp)
print("\nR2 基準:79.9/2.09/−32.9 | 晉級:≥85% 或(Sharpe≥2.2∧≥80)| 目標:≥100%")
print(f"total {time.time()-t0:.1f}s")
