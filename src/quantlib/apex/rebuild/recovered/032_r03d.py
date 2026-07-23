"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:29:29.405Z(工具 Bash)
涵蓋 trials(9):r03d_n8, r06a_geo_n8, r06b_geo_n6, r06c_n6, r06d_n5, r06e_geo_n8_t30, r06f_n6_t30, r06g_geo_n5, r06h_n8_t30
"""
"""R06 — 深集中 × 幾何(8 trials,dev 2019-2025H1)。"""
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

def go(name, *, topn, geometric=False, trail=0.25):
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    if geometric:
        cols = list(W4)
        df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                        on=["date", C], how="semi").drop_nulls(subset=cols))
        for cond in GATE:
            df = df.filter(cond)
        expr = None
        for c_, wt in W4.items():
            term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
            expr = term if expr is None else expr * term
        sc = W_(df.with_columns(expr.alias("score")).select(["date", C, "score"]))
    else:
        sc = W_(blend_score(pool, elig, W4, require=GATE))
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = W_(feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]))
    return run_trial(
        name=name, hypothesis="深集中×幾何攻正2", family="r06", batch="R06",
        panel=panel, entries=e, exit_flags=f, bench=bench, window=f"{DS}..{DE}",
        start=Date.fromisoformat(DS),
        config={"topn": topn, "geo": geometric, "trail": trail},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=30), verbose=False)

runs = [
    go("r06a_geo_n8", topn=8, geometric=True),
    go("r06b_geo_n6", topn=6, geometric=True),
    go("r06c_n6", topn=6),
    go("r06d_n5", topn=5),
    go("r06e_geo_n8_t30", topn=8, geometric=True, trail=0.30),
    go("r06f_n6_t30", topn=6, trail=0.30),
    go("r06g_geo_n5", topn=5, geometric=True),
    go("r06h_n8_t30", topn=8, trail=0.30),
]
cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar"]} for r in runs]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=110):
    print(cmp)
print("\n晉級:CAGR≥55 ∧ MDD≥−45 ∧ Sharpe≥1.5 | 對照 r03d_n8 52.2/1.58/−38.6 | 正2 dev窗 35.0")
print(f"total {time.time()-t0:.1f}s")
