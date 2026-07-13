"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T09:50:01.136Z(工具 Bash)
涵蓋 trials(3):r11a_pead_axis25, r11b_pead_axis50, r11c_pead_gate
"""
"""R11 — 個股級 PEAD 持續性軸(FinLab 個股頁方法論移植;預註冊:
晉級 = CAGR≥62.9 或 Sharpe≥1.82∧CAGR≥60.9,MDD≥−45)。"""
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

# 個股級 PEAD 持續性:每月揭露日(10日 snap)→ 該股 fwd21 報酬 → 過去 6 次滾動均值(shift 1 = PIT)
td = pl.DataFrame({"td": panel["date"].unique().sort()}).sort("td")
rel = (data.load_monthly_revenue(con, DE)
       .with_columns(pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("d0"))
       .select([C, "d0"]).unique()
       .sort("d0")
       .join_asof(td, left_on="d0", right_on="td", strategy="forward")
       .rename({"td": "rel_day"}).drop_nulls(subset=["rel_day"]))
px = (panel.sort([C, "date"])
      .with_columns((pl.col("close").shift(-22) / pl.col("close").shift(-1) - 1).over(C).alias("cyc_ret"))
      .select(["date", C, "cyc_ret"]))
pead = (rel.join(px, left_on=["rel_day", C], right_on=["date", C], how="inner")
        .sort([C, "rel_day"])
        .with_columns(pl.col("cyc_ret").rolling_mean(6).shift(1).over(C).alias("pead_persist"))
        .drop_nulls(subset=["pead_persist"])
        .select([C, pl.col("rel_day").alias("avail"), "pead_persist"])
        .sort("avail"))
feat = (feat.sort("date")
        .join_asof(pead, left_on="date", right_on="avail", by=C, strategy="backward", tolerance="70d")
        .sort([C, "date"]))
cov = feat.filter(pl.col("rev_fresh_days") <= 5)["pead_persist"].is_not_null().mean()
print(f"pead_persist 覆蓋率(fresh 池): {cov:.1%}")

def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())

def go(name, *, exps=None, require=None):
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    w = exps or W4
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(w)))
    for cond in GATE + (require or []):
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = W_(df.with_columns(expr.alias("score")).select(["date", C, "score"]))
    e, _ = entries_and_flags(sc, 8, 10**9)
    f = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    return run_trial(name=name, hypothesis="個股級 PEAD 持續性(FinLab 個股頁移植)",
                     family="r11", batch="R11", panel=panel, entries=e, exit_flags=f,
                     bench=bench, window=f"{DS}..{DE}", start=Date.fromisoformat(DS),
                     config={"name": name},
                     port_spec=PortSpec(n_slots=8, max_new_per_day=5),
                     exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30), verbose=False)

runs = [
    go("r11a_pead_axis25", exps=dict(W4) | {"pead_persist": 0.25}),
    go("r11b_pead_axis50", exps=dict(W4) | {"pead_persist": 0.5}),
    go("r11c_pead_gate", require=[pl.col("pead_persist") > 0]),
]
cmp = pl.DataFrame([{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd"]} for r in runs]).sort("cagr", descending=True)
with pl.Config(tbl_width_chars=100):
    print(cmp)
print("\n旗艦:60.9/1.72/−38.7 | 晉級:CAGR≥62.9 或(Sharpe≥1.82∧CAGR≥60.9)")
print(f"total {time.time()-t0:.1f}s")
