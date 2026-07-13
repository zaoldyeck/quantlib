"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T10:09:45.448Z(工具 Bash)
涵蓋 trials(6):r12a_tier_weight, r12b_score_weight, r12c_seq_axis, r12d_seq_replace, r12e_requal_exit, r12f_second_wave
"""
"""R12 — 加權 × 環比 × 再資格出場 × 二波池(6 trials)。"""
import time
import polars as pl
from datetime import date as Date
from research.apex import data, metrics
from research.apex.assemble import build_features, entries_and_flags, run_trial
from research.apex.engine import ExecSpec, ExitSpec, PortSpec

C = "company_code"
DS, DE = "2019-01-02", "2025-06-30"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}
t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DS, DE)
bench = data.benchmark_nav(con, DS, DE)

# 環比動能:3 月營收和 / 前 3 月和 − 1(PIT snap)
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

# 自揭露日累積報酬(供二波池)
rel_px = (feat.filter(pl.col("rev_fresh_days") == pl.col("rev_fresh_days").min().over([C]))
          )  # 太繞;改用 close/close.shift(fresh_days) 近似:
p2 = (panel.sort([C, "date"]).select(["date", C, "close"]))
feat = feat.join(p2, on=["date", C], how="left")
feat = feat.sort([C, "date"]).with_columns(
    (pl.col("close") / pl.col("close").shift(5).over(C) - 1).alias("ret5")  # 揭露≈5日內 → 用5日報酬近似自揭露累積
)

def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())

def geo_sc(pool, w):
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    return df.with_columns(expr.alias("score"))

def go(name, *, w=None, weights_mode=None, pool_expr=None, requal_exit=False,
       trail=0.35, tstop=30):
    w = w or W4
    pool = feat.filter(pool_expr if pool_expr is not None else (pl.col("rev_fresh_days") <= 5))
    sc = W_(geo_sc(pool, w).select(["date", C, "score"]))
    ranked = sc.with_columns(pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
    e = ranked.filter(pl.col("rk") <= 8).select(["date", C, "score", "rk"])
    if weights_mode == "tier":
        e = e.with_columns(
            pl.when(pl.col("rk") <= 2).then(0.19)
            .when(pl.col("rk") <= 6).then(0.125)
            .otherwise(0.06).alias("weight")).drop("rk")
    elif weights_mode == "score":
        e = (e.join(sc, on=["date", C, "score"], how="left")
             .with_columns((pl.col("score") / pl.col("score").sum().over("date")).clip(0, 0.25).alias("weight"))
             .drop("rk"))
    else:
        e = e.drop("rk")
    if requal_exit:
        fresh_all = W_(feat.filter(pl.col("rev_fresh_days") <= 5).select(["date", C]))
        top24 = ranked.filter(pl.col("rk") <= 24).select(["date", C])
        f = pl.concat([fresh_all.join(top24, on=["date", C], how="anti"),
                       W_(feat.filter(pl.col("rev_fresh_days") >= 35).select(["date", C]))]).unique()
        tstop = 45
    else:
        f = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    return run_trial(name=name, hypothesis="R12", family="r12", batch="R12",
                     panel=panel, entries=e, exit_flags=f, bench=bench,
                     window=f"{DS}..{DE}", start=Date.fromisoformat(DS),
                     config={"name": name},
                     port_spec=PortSpec(n_slots=8, max_new_per_day=5),
                     exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop), verbose=False)

runs = [
    go("r12a_tier_weight", weights_mode="tier"),
    go("r12b_score_weight", weights_mode="score"),
    go("r12c_seq_axis", w=dict(W4) | {"rev_seq": 0.5}),
    go("r12d_seq_replace", w={"rev_seq": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}),
    go("r12e_requal_exit", requal_exit=True),
    go("r12f_second_wave", pool_expr=(pl.col("rev_fresh_days") <= 5)
       | ((pl.col("rev_fresh_days") <= 12) & (pl.col("ret5") > 0.03))),
]
cmp = pl.DataFrame([{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd"]} for r in runs]).sort("cagr", descending=True)
with pl.Config(tbl_rows=8, tbl_width_chars=100):
    print(cmp)
print("\n旗艦:60.9/1.72/−38.7 | 晉級:CAGR≥62.9 或(Sharpe≥1.82∧CAGR≥60.9)")
print(f"total {time.time()-t0:.1f}s")
