"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T10:16:22.412Z(工具 Bash)
涵蓋 trials(8):r13a_rsv60_05, r13b_rsv60_10, r13c_rsv60_repl, r13d_rsv120_05, r13e_hi120_gate, r13f_donch_025, r13g_rsv_lowvol, r13h_rsv50_05
"""
"""R13 — RSV 家族 × v6 scaffold(8 trials)。"""
import time
import polars as pl
from datetime import date as Date
from research.apex import data, metrics
from research.apex.assemble import build_features, entries_and_flags, run_trial
from research.apex.engine import ExecSpec, ExitSpec, PortSpec

C = "company_code"
DS, DE = "2019-01-02", "2025-06-30"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5, "rev_seq": 0.5}
t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DS, DE)
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

# RSV 變體(盤中高低極值)+ 120日 close 高位
ext = (panel.sort([C, "date"])
       .with_columns([
           ((pl.col("close") - pl.col("low").rolling_min(50))
            / (pl.col("high").rolling_max(50) - pl.col("low").rolling_min(50) + 1e-12)).over(C).alias("rsv50"),
           ((pl.col("close") - pl.col("low").rolling_min(120))
            / (pl.col("high").rolling_max(120) - pl.col("low").rolling_min(120) + 1e-12)).over(C).alias("rsv120"),
           (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("hi120"),
       ])
       .select(["date", C, "rsv50", "rsv120", "hi120"]))
feat = feat.join(ext, on=["date", C], how="left")
# rsv60 ≡ range_pos_60(已在 FEATURE_COLS)

def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())

def go(name, w, *, require=None):
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
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
    return run_trial(name=name, hypothesis="RSV 家族(使用者指示)", family="r13", batch="R13",
                     panel=panel, entries=e, exit_flags=f, bench=bench,
                     window=f"{DS}..{DE}", start=Date.fromisoformat(DS),
                     config={"name": name},
                     port_spec=PortSpec(n_slots=8, max_new_per_day=5),
                     exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30), verbose=False)

W5_no52 = {k: v for k, v in W5.items() if k != "high_52w"}
runs = [
    go("r13a_rsv60_05", dict(W5) | {"range_pos_60": 0.5}),
    go("r13b_rsv60_10", dict(W5) | {"range_pos_60": 1.0}),
    go("r13c_rsv60_repl", dict(W5_no52) | {"range_pos_60": 1.0}),
    go("r13d_rsv120_05", dict(W5) | {"rsv120": 0.5}),
    go("r13e_hi120_gate", W5, require=[pl.col("hi120") >= 0.95]),
    go("r13f_donch_025", dict(W5) | {"donchian_60": 0.25}),
    go("r13g_rsv_lowvol", dict(W5) | {"range_pos_60": 0.5, "lowvol_60": 0.25}),
    go("r13h_rsv50_05", dict(W5) | {"rsv50": 0.5}),
]
cmp = pl.DataFrame([{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd"]} for r in runs]).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=100):
    print(cmp)
print("\nv6 基準:79.9/2.09/−32.9 | 晉級:CAGR≥81.9 或(Sharpe≥2.19∧CAGR≥79.9)")
print(f"total {time.time()-t0:.1f}s")
