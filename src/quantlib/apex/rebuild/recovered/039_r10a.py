"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T09:48:39.628Z(工具 Bash)
涵蓋 trials(6):r10a_cap_t30, r10b_cap_stale21, r10c_momw60_t35, r10d_accel125, r10e_h52w125, r10f_pos75
"""
"""R10 — 最終乾涸確認批(6 微組合;預註冊:晉級 = CAGR≥62.9 或 Sharpe≥1.82∧CAGR≥60.9)。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, metrics
from quantlib.apex.assemble import build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec

C = "company_code"
DS, DE = "2019-01-02", "2025-06-30"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}
t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DS, DE)
bench = data.benchmark_nav(con, DS, DE)

def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())

def go(name, *, exps=None, require=None, trail=0.35, stale=26):
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    w = exps or W4
    cols = list(w)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=cols))
    for cond in GATE + (require or []):
        df = df.filter(cond)
    expr = None
    for c_, wt in w.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = W_(df.with_columns(expr.alias("score")).select(["date", C, "score"]))
    e, _ = entries_and_flags(sc, 8, 10**9)
    f = W_(feat.filter(pl.col("rev_fresh_days") >= stale).select(["date", C]))
    return run_trial(name=name, hypothesis="R10 乾涸確認", family="r10", batch="R10",
                     panel=panel, entries=e, exit_flags=f, bench=bench,
                     window=f"{DS}..{DE}", start=Date.fromisoformat(DS),
                     config={"name": name, "trail": trail, "stale": stale},
                     port_spec=PortSpec(n_slots=8, max_new_per_day=5),
                     exit_spec=ExitSpec(trailing_stop=trail, time_stop=30), verbose=False)

CAP = [pl.col("rev_yoy") <= 150]
runs = [
    go("r10a_cap_t30", require=CAP, trail=0.30),
    go("r10b_cap_stale21", require=CAP, stale=21),
    go("r10c_momw60_t35", exps=dict(W4) | {"mom_126_5": 0.6}),
    go("r10d_accel125", exps=dict(W4) | {"rev_yoy_accel": 1.25}),
    go("r10e_h52w125", exps=dict(W4) | {"high_52w": 1.25}),
    go("r10f_pos75", exps=dict(W4) | {"close_pos_20": 0.75}),
]
cmp = pl.DataFrame([{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd"]} for r in runs]).sort("cagr", descending=True)
with pl.Config(tbl_rows=8, tbl_width_chars=100):
    print(cmp)
print("\n旗艦:60.9/1.72/−38.7 | 晉級:CAGR≥62.9 或(Sharpe≥1.82∧CAGR≥60.9)")
print(f"total {time.time()-t0:.1f}s")
