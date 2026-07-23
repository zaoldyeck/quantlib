"""V02 — b06_revcycle_top20 validation(val 動用第 2 次;預註冊見 ledger/batches.md)。

凍結 config:fresh≤5 cohort 內 TRI rank + cfo p50 閘、N=20、stale≥22 signal exit、
time_stop 30、trail 25%、max_new 5。
Run: uv run --project . python -m quantlib.apex.experiments.v02_validate_revcycle
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from quantlib.apex import data
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec

BATCH = "V02"
C = "company_code"
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]

WINDOWS = {"dev": ("2012-01-02", "2023-12-29"), "val": ("2024-01-02", "2025-06-30")}

t0 = time.time()
con = data.connect()
runs = []
for wname, (ws, we) in WINDOWS.items():
    panel, feat, elig = build_features(con, ws, we)
    bench = data.benchmark_nav(con, ws, we)
    fresh_pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    sc = blend_score(fresh_pool, elig, TRI, require=GATE).filter(
        pl.col("date") >= pl.lit(ws).str.to_date()
    )
    stale = feat.filter(pl.col("rev_fresh_days") >= 22).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date()
    )
    entries, _ = entries_and_flags(sc, 20, 10**9)
    for fill in ("next_open", "next_close"):
        runs.append(
            run_trial(
                name=f"v02_{wname}_{fill}",
                hypothesis="revcycle_top20 凍結 config validation + fill 雙測",
                family="validation", batch=BATCH,
                panel=panel, entries=entries, exit_flags=stale, bench=bench,
                window=f"{ws}..{we}", start=Date.fromisoformat(ws),
                config={"frozen": "b06_revcycle_top20", "fill": fill},
                exec_spec=ExecSpec(fill_at=fill),
                port_spec=PortSpec(n_slots=20, max_new_per_day=5),
                exit_spec=ExitSpec(trailing_stop=0.25, time_stop=30),
                verbose=(wname == "val" and fill == "next_open"),
            )
        )

cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar", "n_trades", "win_rate"]} for r in runs]
)
with pl.Config(tbl_width_chars=125):
    print(cmp)
print(f"\ntotal {time.time()-t0:.1f}s")
