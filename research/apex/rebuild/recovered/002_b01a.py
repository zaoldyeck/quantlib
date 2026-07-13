"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T00:07:49.965Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/b01_assembly.py)
涵蓋 trials(8):b01a_rev_accel_solo, b01b_high52w_solo, b01c_rev_x_pos, b01d_tri_axis, b01e_tri_cfo_gate, b01f_tri_n20, b01g_fresh_event, b01h_rev_x_hvn
"""
"""B01 — 策略原型組裝批(8 trials;預註冊見 ledger/batches.md)。

Run: uv run --project research python -m research.apex.experiments.b01_assembly
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from research.apex.engine import ExitSpec, PortSpec

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "B01"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)
C = "company_code"

TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)
print(f"features ready in {time.time()-t0:.1f}s\n")


def W(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def go(name, hypothesis, weights, *, require=None, topn=10, flags_score=None, exit_spec=None):
    sc = W(blend_score(feat, elig, weights, require=require))
    entries, flags = entries_and_flags(sc, topn, topn * 4)
    if flags_score is not None:
        _, flags = entries_and_flags(W(flags_score), topn, topn * 4)
    return run_trial(
        name=name, hypothesis=hypothesis, family="assembly", batch=BATCH,
        panel=panel, entries=entries, exit_flags=flags, bench=bench,
        window=WINDOW, start=START,
        config={"weights": weights, "topn": topn, "exit_rank": topn * 4,
                "trailing": 0.25, "require": str(require)},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=3),
        exit_spec=exit_spec or ExitSpec(trailing_stop=0.25),
    )


runs = []
runs.append(go("b01a_rev_accel_solo", "單因子對照:營收加速", {"rev_yoy_accel": 1.0}))
runs.append(go("b01b_high52w_solo", "單因子對照:52 週高點距離", {"high_52w": 1.0}))
runs.append(go("b01c_rev_x_pos", "兩軸:營收加速 × 價格位置", {"rev_yoy_accel": 1.0, "high_52w": 1.0}))
runs.append(go("b01d_tri_axis", "三軸正交組合", TRI))
runs.append(
    go("b01e_tri_cfo_gate", "三軸 + 盈餘含金量閘", TRI,
       require=[pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")])
)
runs.append(go("b01f_tri_n20", "三軸 N=20 分散版", TRI, topn=20))
runs.append(
    go("b01g_fresh_event", "三軸 + 營收揭露 7 日內才進場", TRI,
       require=[pl.col("rev_fresh_days") <= 7],
       flags_score=blend_score(feat, elig, TRI))
)
runs.append(go("b01h_rev_x_hvn", "價格位置代表換 hvn_dist",
               {"rev_yoy_accel": 1.0, "hvn_dist": 1.0, "close_pos_20": 1.0}))

cmp = pl.DataFrame(
    [
        {k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar",
                            "n_trades", "win_rate", "turnover_ann", "excess_cagr"]}
        for r in runs
    ]
).sort("cagr", descending=True)
print(cmp)
print(f"\ntotal {time.time()-t0:.1f}s")

