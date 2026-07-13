"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T00:22:33.064Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/v01_validate_champion.py)
涵蓋 trials(4):v01_dev_next_close, v01_dev_next_open, v01_val_next_close, v01_val_next_open
"""
"""V01 — b02f 冠軍候選 validation 確認(預註冊見 ledger/batches.md)。

config 凍結:TRI 等權 + cfo p50 閘 + N20 + exit_rank 80 + trail 35% + max_new 3。
Runs:dev/open(sanity)、dev/close、val/open、val/close。

Run: uv run --project research python -m research.apex.experiments.v01_validate_champion
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from research.apex.engine import ExecSpec, ExitSpec, PortSpec

BATCH = "V01"
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]

WINDOWS = {
    "dev": ("2012-01-02", "2023-12-29"),
    "val": ("2024-01-02", "2025-06-30"),
}

t0 = time.time()
con = data.connect()
runs = []
for wname, (ws, we) in WINDOWS.items():
    panel, feat, elig = build_features(con, ws, we)
    bench = data.benchmark_nav(con, ws, we)
    sc = blend_score(feat, elig, TRI, require=GATE).filter(
        pl.col("date") >= pl.lit(ws).str.to_date()
    )
    entries, flags = entries_and_flags(sc, 20, 80)
    for fill in ("next_open", "next_close"):
        runs.append(
            run_trial(
                name=f"v01_{wname}_{fill}",
                hypothesis="b02f 凍結 config 的 validation 確認 + fill 慣例雙測",
                family="validation", batch=BATCH,
                panel=panel, entries=entries, exit_flags=flags, bench=bench,
                window=f"{ws}..{we}", start=Date.fromisoformat(ws),
                config={"frozen": "b02f", "fill": fill},
                exec_spec=ExecSpec(fill_at=fill),
                port_spec=PortSpec(n_slots=20, max_new_per_day=3),
                exit_spec=ExitSpec(trailing_stop=0.35),
                verbose=(wname == "val"),
            )
        )

cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar", "n_trades"]} for r in runs]
)
with pl.Config(tbl_width_chars=120):
    print(cmp)
print(f"\ntotal {time.time()-t0:.1f}s")

