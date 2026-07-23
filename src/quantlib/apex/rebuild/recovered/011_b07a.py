# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T00:44:30.146Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/src/quantlib/apex/experiments/b07_revcycle_v2.py)
# 涵蓋 trials(6):b07a_v2_base, b07b_requal2n, b07c_requal4n, b07d_fresh6, b07e_n16, b07f_n24
"""B07 — apex_revcycle_v2:事件錨定再資格出場(6 trials;預註冊見 ledger/batches.md)。

Run: uv run --project . python -m quantlib.apex.experiments.b07_revcycle_v2
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from quantlib.apex import data
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from quantlib.apex.engine import ExitSpec, PortSpec

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "B07"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)
C = "company_code"
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}

#: cfo 閘 + 覆蓋率 pass-through(當日 cohort cfo 覆蓋 <30% 時放行)
GATE_PT = [
    pl.when(pl.col("cfo_ni_ratio_ttm").is_not_null().mean().over("date") < 0.30)
    .then(True)
    .otherwise(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date"))
]

t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)


def W(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def build_v2(fresh: int, requal_mult: int, topn: int):
    """回傳 (entries, flags):v2 = 揭露日再資格 + 資料斷供保險絲。"""
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    sc = W(blend_score(pool, elig, TRI, require=GATE_PT))
    entries, _ = entries_and_flags(sc, topn, 10**9)
    top_req = sc.with_columns(
        pl.col("score").rank("ordinal", descending=True).over("date").alias("rk")
    ).filter(pl.col("rk") <= topn * requal_mult).select(["date", C])
    fresh_all = W(feat.filter(pl.col("rev_fresh_days") <= fresh).select(["date", C]))
    requal_fail = fresh_all.join(top_req, on=["date", C], how="anti")
    lapse = W(feat.filter(pl.col("rev_fresh_days") >= 35).select(["date", C]))
    flags = pl.concat([requal_fail, lapse]).unique()
    return entries, flags


def go(name, hypothesis, *, fresh=5, requal_mult=3, topn=20, trail=0.25, tstop=45):
    entries, flags = build_v2(fresh, requal_mult, topn)
    return run_trial(
        name=name, hypothesis=hypothesis, family="rev_cycle_v2", batch=BATCH,
        panel=panel, entries=entries, exit_flags=flags, bench=bench,
        window=WINDOW, start=START,
        config={"fresh": fresh, "requal_mult": requal_mult, "topn": topn,
                "trail": trail, "time_stop": tstop, "gate": "cfo_p50_passthrough"},
        port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
        exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
        verbose=False,
    )


runs = [
    go("b07a_v2_base", "v2 base:requal 3N"),
    go("b07b_requal2n", "requal 2N(嚴)", requal_mult=2),
    go("b07c_requal4n", "requal 4N(鬆)", requal_mult=4),
    go("b07d_fresh6", "fresh 6d", fresh=6),
    go("b07e_n16", "N=16", topn=16),
    go("b07f_n24", "N=24", topn=24),
]

cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar", "exposure", "n_trades"]} for r in runs]
).sort("sharpe", descending=True)
with pl.Config(tbl_rows=8, tbl_width_chars=125):
    print(cmp)
print("\nv1 base: cagr 28.4% sharpe 1.72 mdd -23.5% | 判準:CAGR≥27 Sharpe≥1.65 MDD≥-26")
print(f"total {time.time()-t0:.1f}s")

