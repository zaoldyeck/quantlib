# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T00:35:44.142Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/src/quantlib/apex/experiments/p01_battery_revcycle.py)
# 涵蓋 trials(4):v02_dev_next_close, v02_dev_next_open, v02_val_next_close, v02_val_next_open
"""P01 — apex_revcycle_v1 完整驗證 battery(預註冊見 ledger/batches.md)。

A. bootstrap CI / DSR / PBO(用既有 curves)
B. 參數 ±20% 一次一參數擾動(10 runs)
C. MC permutation 200 次(cohort 內隨機選股 null)
D. 壓測 2008-2011(frozen config,不調參)

Run: uv run --project . python -m quantlib.apex.experiments.p01_battery_revcycle
"""
from __future__ import annotations

import json
import os
import time
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data, ledger, metrics, validate
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DEV_START, DEV_END = "2012-01-02", "2023-12-29"
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
CHAMPION_CURVE = "T0068"   # v02_dev_next_open

t0 = time.time()
verdict: dict[str, tuple[bool, str]] = {}

# ── A1. bootstrap ────────────────────────────────────────────────────────
champ = ledger.load_curve(CHAMPION_CURVE)
bs = validate.block_bootstrap_cagr(champ)
verdict["bootstrap"] = (bs["ci_lo"] > 0.10, f"CAGR 95% CI [{bs['ci_lo']:+.1%}, {bs['ci_hi']:+.1%}] 下界>10%?")

# ── A2. DSR(N = ledger 全 trial 數;V[SR] 取全部 dev 曲線)───────────────
trials = ledger.all_trials()
n_trials = trials.height
dev_ids = trials.filter(pl.col("window").str.starts_with(DEV_START))["trial_id"].to_list()
curves = []
for tid in dev_ids:
    p = os.path.join(ledger.CURVES_DIR, f"{tid}.parquet")
    if os.path.exists(p):
        curves.append(pl.read_parquet(p))
vsr = validate.sr_variance_from_curves(curves)
dsr = validate.deflated_sharpe(champ, n_trials=n_trials, sr_var_across_trials=vsr)
verdict["dsr"] = (dsr["dsr"] > 0.95, f"DSR {dsr['dsr']:.4f}(N={n_trials}, V[SR]={vsr:.2e}, z={dsr['z']:.2f})")

# ── A3. PBO / CSCV(全部 dev 曲線對齊交集日)──────────────────────────────
common = None
for c in curves:
    ds = set(c["date"].to_list())
    common = ds if common is None else (common & ds)
common = sorted(common)
mat = np.stack([
    validate.daily_returns(c.filter(pl.col("date").is_in(pl.Series(common).implode())))
    for c in curves
], axis=1)
pbo = validate.pbo_cscv(mat, s=16)
verdict["pbo"] = (pbo["pbo"] < 0.5, f"PBO {pbo['pbo']:.3f}(configs={pbo['n_configs']}, combos={pbo['n_combos']})")
print(f"[A] done {time.time()-t0:.0f}s :: bootstrap {bs} | dsr {dsr['dsr']:.4f} | pbo {pbo['pbo']:.3f}")

# ── B. 參數擾動 ──────────────────────────────────────────────────────────
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)


def run_cfg(fresh=5, stale=22, topn=20, trail=0.25, tstop=30, entries_override=None):
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    sc = blend_score(pool, elig, TRI, require=GATE).filter(
        pl.col("date") >= pl.lit(DEV_START).str.to_date())
    flags = feat.filter(pl.col("rev_fresh_days") >= stale).select(["date", C]).filter(
        pl.col("date") >= pl.lit(DEV_START).str.to_date())
    entries = entries_override if entries_override is not None else entries_and_flags(sc, topn, 10**9)[0]
    res = simulate(panel, entries, exit_flags=flags, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
                   exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
                   start=Date.fromisoformat(DEV_START))
    s = metrics.perf_stats(res.nav)
    return s, sc, res


perturb = [
    ("base", {}), ("fresh4", {"fresh": 4}), ("fresh6", {"fresh": 6}),
    ("stale18", {"stale": 18}), ("stale26", {"stale": 26}),
    ("n16", {"topn": 16}), ("n24", {"topn": 24}),
    ("trail20", {"trail": 0.20}), ("trail30", {"trail": 0.30}),
    ("time24", {"tstop": 24}), ("time36", {"tstop": 36}),
]
rows = []
for nm, kw in perturb:
    s, _, _ = run_cfg(**kw)
    rows.append({"variant": nm, "cagr": s["cagr"], "sharpe": s["sharpe"], "mdd": s["mdd"]})
    print(f"  perturb {nm:8s} cagr {s['cagr']:+.1%} sharpe {s['sharpe']:.2f} mdd {s['mdd']:+.1%}")
pt = pl.DataFrame(rows)
spread = float(pt["cagr"].max() - pt["cagr"].min())
gates_ok = pt.filter((pl.col("cagr") >= 0.15) & (pl.col("mdd") >= -0.35) & (pl.col("sharpe") >= 1.0)).height
verdict["perturb"] = (spread < 0.15 and gates_ok >= round(0.8 * pt.height),
                      f"spread {spread:.1%} <15%? gates {gates_ok}/{pt.height} ≥80%?")
print(f"[B] done {time.time()-t0:.0f}s")

# ── C. permutation(cohort 內隨機 N 檔,200 次)───────────────────────────
_, sc_real, res_real = run_cfg()
real_cagr = metrics.perf_stats(res_real.nav)["cagr"]
rng = np.random.default_rng(7)
perm_cagrs = []
flags_frozen = feat.filter(pl.col("rev_fresh_days") >= 22).select(["date", C]).filter(
    pl.col("date") >= pl.lit(DEV_START).str.to_date())
for i in range(200):
    rnd = sc_real.with_columns(pl.Series("score", rng.random(sc_real.height)))
    e_rnd, _ = entries_and_flags(rnd, 20, 10**9)
    res = simulate(panel, e_rnd, exit_flags=flags_frozen, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=20, max_new_per_day=5),
                   exit_spec=ExitSpec(trailing_stop=0.25, time_stop=30),
                   start=Date.fromisoformat(DEV_START))
    perm_cagrs.append(metrics.perf_stats(res.nav)["cagr"])
    if (i + 1) % 50 == 0:
        print(f"  perm {i+1}/200 ({time.time()-t0:.0f}s)")
perm = np.array(perm_cagrs)
p_val = float((perm >= real_cagr).mean())
verdict["permutation"] = (p_val < 0.05,
                          f"p={p_val:.4f}(null median {np.median(perm):+.1%}, real {real_cagr:+.1%})")
print(f"[C] done {time.time()-t0:.0f}s")

# ── D. 壓測 2008-2011 ───────────────────────────────────────────────────
sp, sf, se = build_features(con, "2008-01-02", "2011-12-30")
pool = sf.filter(pl.col("rev_fresh_days") <= 5)
scs = blend_score(pool, se, TRI, require=GATE).filter(pl.col("date") >= pl.lit("2008-01-02").str.to_date())
fls = sf.filter(pl.col("rev_fresh_days") >= 22).select(["date", C])
es, _ = entries_and_flags(scs, 20, 10**9)
res_stress = simulate(sp, es, exit_flags=fls, exec_spec=ExecSpec(),
                      port_spec=PortSpec(n_slots=20, max_new_per_day=5),
                      exit_spec=ExitSpec(trailing_stop=0.25, time_stop=30),
                      start=Date(2008, 1, 2))
ss = metrics.perf_stats(res_stress.nav)
yt = metrics.yearly_table(res_stress.nav)
worst_year = float(yt["ret"].min())
verdict["stress_gfc"] = (ss["mdd"] > -0.45 and worst_year > -0.35,
                         f"2008-11 CAGR {ss['cagr']:+.1%} MDD {ss['mdd']:+.1%} 最差年 {worst_year:+.1%}")
print(yt)

# ── 總結 ─────────────────────────────────────────────────────────────────
print("\n════ P01 BATTERY VERDICT ════")
all_pass = True
for k, (ok, msg) in verdict.items():
    print(f"  {'✅' if ok else '❌'} {k:12s} {msg}")
    all_pass &= ok
print(f"\n{'🏆 ALL PASS → 晉級 final holdout' if all_pass else '⛔ 未全過 → 回 Phase 2'}")

with open(os.path.join(ledger.LEDGER_DIR, "p01_battery.json"), "w", encoding="utf-8") as f:
    json.dump({k: {"pass": bool(ok), "detail": msg} for k, (ok, msg) in verdict.items()},
              f, ensure_ascii=False, indent=2)
print(f"total {time.time()-t0:.0f}s")

