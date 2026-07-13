"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T00:52:44.864Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/p02_battery_v1s26.py)
涵蓋 trials(2):p02_fill_close_dev, p02_fill_close_val
"""
"""P02 — apex_revcycle_v1s26 完整 battery(預註冊見 ledger/batches.md B10 §3)。

含修正版壓測:cfo 閘帶覆蓋率 pass-through(<30% 放行),先驗證其於 dev 與普通閘等價。
Run: uv run --project research python -m research.apex.experiments.p02_battery_v1s26
"""
from __future__ import annotations

import json
import os
import time
from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data, ledger, metrics, validate
from research.apex.assemble import blend_score, build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DEV_START, DEV_END = "2012-01-02", "2023-12-29"
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
GATE_PT = [
    pl.when(pl.col("cfo_ni_ratio_ttm").is_not_null().mean().over("date") < 0.30)
    .then(True)
    .otherwise(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date"))
]
CHAMP = "T0078"
STALE = 26

t0 = time.time()
verdict: dict[str, tuple[bool, str]] = {}
champ = ledger.load_curve(CHAMP)

# A1 bootstrap
bs = validate.block_bootstrap_cagr(champ)
verdict["bootstrap"] = (bs["ci_lo"] > 0.10, f"CAGR 95% CI [{bs['ci_lo']:+.1%}, {bs['ci_hi']:+.1%}]")

# A2 DSR
trials = ledger.all_trials()
n_trials = trials.height
dev_ids = trials.filter(pl.col("window").str.starts_with(DEV_START))["trial_id"].to_list()
curves = [pl.read_parquet(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))
          for t in dev_ids if os.path.exists(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))]
vsr = validate.sr_variance_from_curves(curves)
dsr = validate.deflated_sharpe(champ, n_trials=n_trials, sr_var_across_trials=vsr)
verdict["dsr"] = (dsr["dsr"] > 0.95, f"DSR {dsr['dsr']:.4f}(N={n_trials})")

# A3 PBO
common = None
for c in curves:
    ds = set(c["date"].to_list())
    common = ds if common is None else (common & ds)
common = sorted(common)
mat = np.stack([validate.daily_returns(c.filter(pl.col("date").is_in(pl.Series(common).implode())))
                for c in curves], axis=1)
pbo = validate.pbo_cscv(mat, s=16)
verdict["pbo"] = (pbo["pbo"] < 0.5, f"PBO {pbo['pbo']:.3f}(configs={pbo['n_configs']})")
print(f"[A] {time.time()-t0:.0f}s bootstrap/dsr/pbo done")

# 準備引擎
con = data.connect()
panel, feat, elig = build_features(con, DEV_START, DEV_END)


def run(feat_, elig_, panel_, ws, *, gate, entries_override=None, seed_score=None):
    pool = feat_.filter(pl.col("rev_fresh_days") <= 5)
    sc = blend_score(pool, elig_, TRI, require=gate).filter(pl.col("date") >= pl.lit(ws).str.to_date())
    if seed_score is not None:
        sc = sc.with_columns(pl.Series("score", seed_score.random(sc.height)))
    flags = feat_.filter(pl.col("rev_fresh_days") >= STALE).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, 20, 10**9)
    if entries_override is not None:
        e = entries_override
    return simulate(panel_, e, exit_flags=flags, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=20, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=0.25, time_stop=30),
                    start=Date.fromisoformat(ws))


# gate 等價性驗證(dev 上 pass-through 應與普通閘同結果)
res_pt = run(feat, elig, panel, DEV_START, gate=GATE_PT)
s_pt = metrics.perf_stats(res_pt.nav)
base_cagr = metrics.perf_stats(champ)["cagr"]
gate_equiv = abs(s_pt["cagr"] - base_cagr) < 1e-9
print(f"gate pass-through dev 等價:{gate_equiv}({s_pt['cagr']:+.4%} vs {base_cagr:+.4%})")

# C permutation 200
rng = np.random.default_rng(11)
perm = []
for i in range(200):
    r = run(feat, elig, panel, DEV_START, gate=GATE, seed_score=rng)
    perm.append(metrics.perf_stats(r.nav)["cagr"])
    if (i + 1) % 50 == 0:
        print(f"  perm {i+1}/200 ({time.time()-t0:.0f}s)")
perm = np.array(perm)
p_val = float((perm >= base_cagr).mean())
verdict["permutation"] = (p_val < 0.05, f"p={p_val:.4f}(null 中位 {np.median(perm):+.1%} vs {base_cagr:+.1%})")

# D 修正版壓測(pass-through 閘)
det = []
ok_all = True
for ws, we in [("2008-01-02", "2009-12-31"), ("2010-01-04", "2011-12-30")]:
    sp, sf, se = build_features(con, ws, we)
    r = run(sf, se, sp, ws, gate=GATE_PT)
    ss = metrics.perf_stats(r.nav)
    yt = metrics.yearly_table(r.nav)
    worst = float(yt["ret"].min())
    expo = float((r.nav["invested"] / r.nav["nav"]).mean())
    seg_ok = ss["mdd"] > -0.45 and worst > -0.35 and expo > 0.3
    ok_all &= seg_ok
    det.append(f"{ws[:4]}-{we[:4]}: CAGR {ss['cagr']:+.1%} MDD {ss['mdd']:+.1%} "
               f"最差年 {worst:+.1%} 曝險 {expo:.0%}")
    print("  stress", det[-1])
verdict["stress"] = (ok_all, " | ".join(det))

print("\n════ P02 BATTERY VERDICT(v1s26)════")
all_pass = True
for k, (ok, msg) in verdict.items():
    print(f"  {'✅' if ok else '❌'} {k:12s} {msg}")
    all_pass &= ok
print("  ✅ perturb      spread 12.8%(B10 Gate1 已過,records T0078)")
print(f"\n{'🏆 ALL PASS → 收斂階段(3 連乾涸批)後 holdout' if all_pass else '⛔ 未全過'}")

with open(os.path.join(ledger.LEDGER_DIR, "p02_battery.json"), "w", encoding="utf-8") as f:
    json.dump({k: {"pass": bool(ok), "detail": m} for k, (ok, m) in verdict.items()} |
              {"perturb": {"pass": True, "detail": "spread 12.8% (B10)"},
               "gate_pt_equiv": bool(gate_equiv)},
              f, ensure_ascii=False, indent=2)
print(f"total {time.time()-t0:.0f}s")

