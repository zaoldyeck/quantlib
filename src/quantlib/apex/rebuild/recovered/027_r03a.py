"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:05:35.759Z(工具 Bash)
涵蓋 trials(8):r03a_n16, r03b_n12, r03c_n10, r03d_n8, r03e_n16_t30, r03f_n10_t30, r03g_momw100, r03h_n10_momw75
"""
"""R03 — 冠軍集中化階梯(dev 2019-2025H1)。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, metrics
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec

C = "company_code"
DS, DE = "2019-01-02", "2025-06-30"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DS, DE)
bench = data.benchmark_nav(con, DS, DE)

def W_(df):
    return df.filter(pl.col("date") >= pl.lit(DS).str.to_date())

def go(name, topn, trail=0.25, mom_w=0.5):
    w = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": mom_w}
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    sc = W_(blend_score(pool, elig, w, require=GATE))
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    return run_trial(
        name=name, hypothesis="冠軍集中化", family="concentration", batch="R03",
        panel=panel, entries=e, exit_flags=f, bench=bench, window=f"{DS}..{DE}",
        start=Date.fromisoformat(DS),
        config={"topn": topn, "trail": trail, "mom_w": mom_w},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=30), verbose=False)

runs = [
    go("r03a_n16", 16), go("r03b_n12", 12), go("r03c_n10", 10), go("r03d_n8", 8),
    go("r03e_n16_t30", 16, trail=0.30), go("r03f_n10_t30", 10, trail=0.30),
    go("r03g_momw100", 20, mom_w=1.0), go("r03h_n10_momw75", 10, mom_w=0.75),
]
cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar", "n_trades"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=115):
    print(cmp)
print("\nv3-modern 對照:42.9%/1.75/−26.6 | 晉級:R gates ∧ CAGR ≥ 47.9%")
print(f"total {time.time()-t0:.1f}s")
