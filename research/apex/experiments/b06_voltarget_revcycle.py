"""B06 — 目標波動 overlay + 營收週期節奏(10 trials;預註冊見 ledger/batches.md)。

vol overlay:exposure_t = clip(target/σ_ewma(t-1), 0.2, 1.0),日更;
調整成本 = |Δexposure| × 單邊摩擦(0.1425%+0.1% 滑價 ≈ 0.24%…用 0.28% 半回合含稅攤提)。
Run: uv run --project research python -m research.apex.experiments.b06_voltarget_revcycle
"""
from __future__ import annotations

import time
from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data, ledger, metrics
from research.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from research.apex.engine import ExitSpec, PortSpec

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "B06"
WINDOW = f"{DEV_START}..{DEV_END}"
START = Date.fromisoformat(DEV_START)
C = "company_code"

TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
HALF_TURN_COST = 0.0028  # 半回合摩擦(手續費+滑價+稅攤提)

t0 = time.time()
con = data.connect()


def vol_overlay(nav: pl.DataFrame, target_ann: float, ewma_span: int) -> pl.DataFrame:
    """對既有 NAV 曲線施加 vol-target overlay(t-1 資訊決定 t 曝險)。"""
    d = nav.sort("date")
    r = (d["nav"].to_numpy()[1:] / d["nav"].to_numpy()[:-1]) - 1.0
    lam = 2.0 / (ewma_span + 1.0)
    var = np.empty_like(r)
    v = r[: min(20, len(r))].var()
    for i, x in enumerate(r):
        var[i] = v
        v = (1 - lam) * v + lam * x * x
    sigma_ann = np.sqrt(var * 252)
    expo = np.clip(target_ann / np.maximum(sigma_ann, 1e-9), 0.2, 1.0)
    expo_prev = np.concatenate([[expo[0]], expo[:-1]])
    scaled = expo_prev * r - np.abs(np.diff(np.concatenate([[expo[0]], expo_prev]))) * HALF_TURN_COST
    nav_new = np.concatenate([[1.0], np.cumprod(1 + scaled)])
    return d.select("date").with_columns(pl.Series("nav", nav_new * 1.0))


def log_curve(name, hypothesis, nav, config, bench):
    summ = metrics.perf_stats(nav)
    b = bench.sort("date")
    summ["bench_cagr"] = float((b["nav"][-1] / b["nav"][0]) ** (1 / summ["years"]) - 1)
    summ["excess_cagr"] = summ["cagr"] - summ["bench_cagr"]
    tid = ledger.log_trial(family="vol_overlay", name=name, hypothesis=hypothesis,
                           config=config, window=WINDOW, metrics=summ, batch=BATCH, curve=nav)
    return {"trial_id": tid, "name": name, **summ}


# ── vol overlay 網格(基於 T0038 dev 曲線)─────────────────────────────
base = ledger.load_curve("T0038")
bench = data.benchmark_nav(con, DEV_START, DEV_END)
runs = []
for tv in (0.12, 0.15, 0.18, 0.21):
    for span in (20, 60):
        nav = vol_overlay(base, tv, span)
        runs.append(log_curve(f"b06_vt{int(tv*100)}_e{span}",
                              f"vol target {tv:.0%} ewma{span}", nav,
                              {"target": tv, "ewma": span, "base": "T0038"}, bench))

# ── rev-cycle 節奏系統 ────────────────────────────────────────────────
panel, feat, elig = build_features(con, DEV_START, DEV_END)


def W(df):
    return df.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())


fresh_pool = feat.filter(pl.col("rev_fresh_days") <= 5)
sc_fresh = W(blend_score(fresh_pool, elig, TRI, require=GATE))
stale_flags = W(feat.filter(pl.col("rev_fresh_days") >= 22).select(["date", C]))

for topn in (10, 20):
    entries, _ = entries_and_flags(sc_fresh, topn, 10**9)
    runs.append(
        run_trial(
            name=f"b06_revcycle_top{topn}",
            hypothesis="fresh cohort 內 rank、持有一個揭露週期",
            family="rev_cycle", batch=BATCH,
            panel=panel, entries=entries, exit_flags=stale_flags, bench=bench,
            window=WINDOW, start=START,
            config={"topn": topn, "fresh": 5, "stale_exit": 22, "time_stop": 30},
            port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
            exit_spec=ExitSpec(trailing_stop=0.25, time_stop=30),
            verbose=False,
        )
    )

cmp = pl.DataFrame(
    [{k: r.get(k) for k in ["trial_id", "name", "cagr", "sharpe", "mdd", "calmar"]} for r in runs]
).sort("sharpe", descending=True)
with pl.Config(tbl_rows=12, tbl_width_chars=110):
    print(cmp)
print("\nbaseline b02f dev: cagr 31.2% sharpe 1.58 mdd -29.2%")
print(f"total {time.time()-t0:.1f}s")
