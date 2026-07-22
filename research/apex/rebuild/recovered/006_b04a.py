# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T00:25:28.670Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/b04_crash_defense.py)
# 涵蓋 trials(8):b04a_abs15, b04b_abs20, b04c_abs25, b04d_brake_v1, b04e_brake_v2, b04f_abs20_brake, b04g_halt_v1, b04h_ma200_halt
"""B04 — 出場堆疊補完:崩盤防禦(8 trials;預註冊見 ledger/batches.md)。

Run: uv run --project research python -m research.apex.experiments.b04_crash_defense
"""
from __future__ import annotations

import time
from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data
from research.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from research.apex.engine import ExitSpec, PortSpec

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "B04"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)
C = "company_code"

TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]


def crash_state(bench: pl.DataFrame, lookback: int, trigger: float, resume: float) -> pl.DataFrame:
    """遲滯型崩盤狀態機:(date, crash bool)。trigger/resume 皆為 lookback 日報酬門檻。"""
    b = bench.sort("date").with_columns(
        (pl.col("nav") / pl.col("nav").shift(lookback) - 1).alias("r")
    )
    r = b["r"].to_numpy()
    state = np.zeros(len(r), dtype=bool)
    on = False
    for i in range(len(r)):
        if not np.isnan(r[i]):
            if not on and r[i] < trigger:
                on = True
            elif on and r[i] > resume:
                on = False
        state[i] = on
    return b.select("date").with_columns(pl.Series("crash", state))


t0 = time.time()
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)
all_codes = panel.select(pl.col(C).unique())

cs_v1 = crash_state(bench, 20, -0.12, -0.05)
cs_v2 = crash_state(bench, 10, -0.08, -0.02)
ma200_off = (
    bench.sort("date")
    .with_columns((pl.col("nav") <= pl.col("nav").rolling_mean(200)).alias("off"))
    .filter(pl.col("off"))
    .select("date")
)
print(f"features {time.time()-t0:.1f}s | crash days v1 {cs_v1['crash'].sum()} "
      f"v2 {cs_v2['crash'].sum()} | ma200-off {ma200_off.height}\n")


def W(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


def go(name, hypothesis, *, abs_stop=None, brake: pl.DataFrame | None = None,
       halt_only: pl.DataFrame | None = None):
    """brake:crash 狀態 frame → 停新倉 + 全面 exit flag;halt_only:只停新倉。"""
    sc = W(blend_score(feat, elig, TRI, require=GATE))
    entries, flags = entries_and_flags(sc, 20, 80)
    halt_dates = None
    if brake is not None:
        halt_dates = brake.filter(pl.col("crash")).select("date")
        crash_flags = halt_dates.join(all_codes, how="cross")
        flags = pl.concat([flags, crash_flags.select(["date", C])]).unique()
    if halt_only is not None:
        halt_dates = halt_only
    if halt_dates is not None:
        entries = entries.join(halt_dates, on="date", how="anti")
    return run_trial(
        name=name, hypothesis=hypothesis, family="crash_defense", batch=BATCH,
        panel=panel, entries=entries, exit_flags=flags, bench=bench,
        window=WINDOW, start=START,
        config={"abs_stop": abs_stop, "brake": brake is not None,
                "halt_only": halt_only is not None},
        port_spec=PortSpec(n_slots=20, max_new_per_day=3),
        exit_spec=ExitSpec(trailing_stop=0.35, abs_stop=abs_stop),
        verbose=False,
    )


runs = [
    go("b04a_abs15", "絕對停損 15%", abs_stop=0.15),
    go("b04b_abs20", "絕對停損 20%", abs_stop=0.20),
    go("b04c_abs25", "絕對停損 25%", abs_stop=0.25),
    go("b04d_brake_v1", "市場剎車 20d<-12%(全面出場+停新倉)", brake=cs_v1),
    go("b04e_brake_v2", "市場剎車 10d<-8% 靈敏版", brake=cs_v2),
    go("b04f_abs20_brake", "abs20 + 剎車 v1", abs_stop=0.20, brake=cs_v1),
    go("b04g_halt_v1", "剎車只停新倉(不強制出場)", halt_only=cs_v1.filter(pl.col("crash")).select("date")),
    go("b04h_ma200_halt", "MA200 停新倉(t35 config 重測)", halt_only=ma200_off),
]

cmp = pl.DataFrame(
    [{k: r[k] for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar", "n_trades", "exposure"]} for r in runs]
).sort("calmar", descending=True)
with pl.Config(tbl_rows=10, tbl_width_chars=125):
    print(cmp)
print(f"\nbaseline b02f: cagr 31.2% sharpe 1.58 mdd -29.2% calmar 1.07")
print(f"total {time.time()-t0:.1f}s")

