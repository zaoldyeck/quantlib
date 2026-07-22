# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T00:21:05.473Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/b03_combine_push.py)
# 涵蓋 trials(10):b03a_t35_mh10, b03b_t45, b03c_no_trail, b03d_w211, b03e_w121, b03f_w112, b03g_dualgate, b03h_gate_p25, b03i_gate_p75, b03j_maxnew5
"""B03 — 合體與極限推進(10 trials;預註冊見 ledger/batches.md)。

Run: uv run --project research python -m research.apex.experiments.b03_combine_push
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from research.apex.engine import ExitSpec, PortSpec

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "B03"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)

TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}


def gate(q: float) -> list[pl.Expr]:
    return [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").quantile(q).over("date")]


t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)
print(f"features ready in {time.time()-t0:.1f}s\n")


def W(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def go(name, hypothesis, weights, *, require, topn=20, exit_rank_mult=4,
       min_hold=1, trailing=0.35, max_new=3):
    sc = W(blend_score(feat, elig, weights, require=require))
    entries, flags = entries_and_flags(sc, topn, topn * exit_rank_mult)
    return run_trial(
        name=name, hypothesis=hypothesis, family="assembly", batch=BATCH,
        panel=panel, entries=entries, exit_flags=flags, bench=bench,
        window=WINDOW, start=START,
        config={"weights": weights, "topn": topn, "exit_rank": topn * exit_rank_mult,
                "trailing": trailing, "min_hold": min_hold, "gate": str(require),
                "max_new": max_new},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=max_new, min_hold_days=min_hold),
        exit_spec=ExitSpec(trailing_stop=trailing),
        verbose=False,
    )


G50 = gate(0.50)
runs = [
    go("b03a_t35_mh10", "trail35 × minhold10 疊加", TRI, require=G50, min_hold=10),
    go("b03b_t45", "trailing 45%", TRI, require=G50, trailing=0.45),
    go("b03c_no_trail", "無 trailing(純 signal-death)", TRI, require=G50, trailing=None),
    go("b03d_w211", "權重 accel 重 2:1:1", {"rev_yoy_accel": 2.0, "high_52w": 1.0, "close_pos_20": 1.0}, require=G50),
    go("b03e_w121", "權重 52wH 重 1:2:1", {"rev_yoy_accel": 1.0, "high_52w": 2.0, "close_pos_20": 1.0}, require=G50),
    go("b03f_w112", "權重吸收重 1:1:2", {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 2.0}, require=G50),
    go("b03g_dualgate", "雙閘 cfo + dy>0", TRI, require=G50 + [pl.col("dy") > 0]),
    go("b03h_gate_p25", "cfo 閘鬆到 p25", TRI, require=gate(0.25)),
    go("b03i_gate_p75", "cfo 閘緊到 p75", TRI, require=gate(0.75)),
    go("b03j_maxnew5", "max_new_per_day 5", TRI, require=G50, max_new=5),
]

cmp = pl.DataFrame(
    [
        {k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar",
                            "n_trades", "turnover_ann"]}
        for r in runs
    ]
).sort("cagr", descending=True)
with pl.Config(tbl_rows=12, tbl_width_chars=120):
    print(cmp)
print(f"\ntotal {time.time()-t0:.1f}s")

