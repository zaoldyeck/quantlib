"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:11:38.607Z(工具 Bash)
涵蓋 trials(9):r03d_n8, r04a_acc10_cap20, r04b_acc20_cap20, r04c_acc30_cap20, r04d_acc20_cap12, r04e_acc20_cap40, r04f_acc20_h90, r04g_acc20_p55, r04h_h95_acc0
"""
"""R04 — 門檻制變動持股數(8 trials,dev 2019-2025H1)。"""
import time
import polars as pl
from datetime import date as Date
from quantlib.apex import data, metrics
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
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

def go(name, *, thresholds: list, cap: int):
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    sc = W_(blend_score(pool, elig, W4, require=GATE + thresholds))
    e, _ = entries_and_flags(sc, cap, 10**9)   # 池內全數為候選,score 排序,cap 防稀釋
    f = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    return run_trial(
        name=name, hypothesis="門檻制變動持股(使用者提議)", family="threshold_variable_n",
        batch="R04", panel=panel, entries=e, exit_flags=f, bench=bench,
        window=f"{DS}..{DE}", start=Date.fromisoformat(DS),
        config={"thresholds": name, "cap": cap},
        port_spec=PortSpec(n_slots=cap, max_new_per_day=cap),
        exit_spec=ExitSpec(trailing_stop=0.25, time_stop=30), verbose=False)

A = pl.col("rev_yoy_accel")
H = pl.col("high_52w")
P = pl.col("close_pos_20")
runs = [
    go("r04a_acc10_cap20", thresholds=[A > 10], cap=20),
    go("r04b_acc20_cap20", thresholds=[A > 20], cap=20),
    go("r04c_acc30_cap20", thresholds=[A > 30], cap=20),
    go("r04d_acc20_cap12", thresholds=[A > 20], cap=12),
    go("r04e_acc20_cap40", thresholds=[A > 20], cap=40),
    go("r04f_acc20_h90", thresholds=[A > 20, H > 0.90], cap=20),
    go("r04g_acc20_p55", thresholds=[A > 20, P > 0.55], cap=20),
    go("r04h_h95_acc0", thresholds=[H > 0.95, A > 0], cap=20),
]
rows = []
for r in runs:
    yt = metrics.yearly_table(pl.read_parquet(
        f"src/quantlib/apex/ledger/curves/{r['trial_id']}.parquet"))
    rows.append({**{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "exposure"]},
                 "pos_seg": int((yt["ret"] > 0).sum())})
cmp = pl.DataFrame(rows).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=120):
    print(cmp)
print("\n對照:v3-n20 modern 42.9/1.75/−26.6 | r03d_n8 52.2/1.58/−38.6")
print("晉級:CAGR≥45 ∧ Sharpe≥1.55 ∧ MDD≥−40 ∧ 年段正≥5/7")
print(f"total {time.time()-t0:.1f}s")
