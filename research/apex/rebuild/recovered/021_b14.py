"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T01:19:51.974Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/b14_v4_gauntlet.py)
涵蓋 trials(2):b14_v4_dev, b14_v4_val_synth
"""
"""B14 — v4(v3 + next_mid 分批執行)gauntlet(預註冊見 ledger/batches.md)。

G0 合成近似品質(dev)→ G1 dev+±20% → G2 val(既有曲線合成)→ G3 fill 雙測
→ G4 battery → G5 壓測披露。
Run: uv run --project research python -m research.apex.experiments.b14_v4_gauntlet
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
VAL_START, VAL_END = "2024-01-02", "2025-06-30"
BATCH = "B14"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
GATE_PT = [
    pl.when(pl.col("cfo_ni_ratio_ttm").is_not_null().mean().over("date") < 0.30)
    .then(True)
    .otherwise(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date"))
]

t0 = time.time()
con = data.connect()
results: dict = {}


def weights(mom_w=0.5):
    return {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": mom_w}


def run(feat, elig, panel, ws, *, fresh=5, stale=26, topn=20, trail=0.25, tstop=30,
        mom_w=0.5, fill="next_mid", gate=None, seed=None):
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    sc = blend_score(pool, elig, weights(mom_w), require=gate or GATE).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    if seed is not None:
        sc = sc.with_columns(pl.Series("score", seed.random(sc.height)))
    flags = feat.filter(pl.col("rev_fresh_days") >= stale).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    return simulate(panel, e, exit_flags=flags, exec_spec=ExecSpec(fill_at=fill),
                    port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
                    start=Date.fromisoformat(ws))


def synth5050(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
    j = (a.select(["date", pl.col("nav").alias("na")])
         .join(b.select(["date", pl.col("nav").alias("nb")]), on="date", how="inner")
         .sort("date")
         .with_columns([
             (pl.col("na") / pl.col("na").shift(1) - 1).fill_null(0.0).alias("ra"),
             (pl.col("nb") / pl.col("nb").shift(1) - 1).fill_null(0.0).alias("rb"),
         ])
         .with_columns(((1 + 0.5 * pl.col("ra") + 0.5 * pl.col("rb")).cum_prod()).alias("nav")))
    return j.select(["date", "nav"])


panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)

# ── G0:合成近似品質(dev)────────────────────────────────────────────
res_mid = run(feat, elig, panel, DEV_START)
s_mid = metrics.summarize(res_mid.nav, res_mid.trades, bench)
nav_open = run(feat, elig, panel, DEV_START, fill="next_open").nav
nav_close = run(feat, elig, panel, DEV_START, fill="next_close").nav
s_syn = metrics.perf_stats(synth5050(nav_open, nav_close))
g0 = abs(s_mid["cagr"] - s_syn["cagr"]) < 0.01
results["G0_approx"] = {"pass": g0, "mid_cagr": s_mid["cagr"], "synth_cagr": s_syn["cagr"]}
print(f"G0 近似:mid {s_mid['cagr']:+.2%} vs 合成 {s_syn['cagr']:+.2%} → {'✅' if g0 else '❌'}")

# ── G1:dev gates + ±20% ─────────────────────────────────────────────
yt = metrics.yearly_table(res_mid.nav)
pos_years = int((yt["ret"] > 0).sum())
tid = ledger.log_trial(family="rev_cycle_v4", name="b14_v4_dev", hypothesis="v4 mid 執行",
                       config={"fill": "next_mid", "mom_w": 0.5, "stale": 26},
                       window=f"{DEV_START}..{DEV_END}", metrics=s_mid, batch=BATCH,
                       curve=res_mid.nav)
print(f"{tid} v4 dev: cagr {s_mid['cagr']:+.2%} sharpe {s_mid['sharpe']:.3f} "
      f"mdd {s_mid['mdd']:+.2%} 正年 {pos_years}/12")
rows = [{"variant": "base", "cagr": s_mid["cagr"], "sharpe": s_mid["sharpe"], "mdd": s_mid["mdd"]}]
for nm, kw in [("fresh4", {"fresh": 4}), ("fresh6", {"fresh": 6}),
               ("stale21", {"stale": 21}), ("stale31", {"stale": 31}),
               ("n16", {"topn": 16}), ("n24", {"topn": 24}),
               ("trail20", {"trail": 0.20}), ("trail30", {"trail": 0.30}),
               ("time24", {"tstop": 24}), ("time36", {"tstop": 36}),
               ("momw40", {"mom_w": 0.4}), ("momw60", {"mom_w": 0.6})]:
    ss = metrics.perf_stats(run(feat, elig, panel, DEV_START, **kw).nav)
    rows.append({"variant": nm, "cagr": ss["cagr"], "sharpe": ss["sharpe"], "mdd": ss["mdd"]})
    print(f"  {nm:8s} cagr {ss['cagr']:+.1%} sharpe {ss['sharpe']:.2f} mdd {ss['mdd']:+.1%}")
pt = pl.DataFrame(rows)
spread = float(pt["cagr"].max() - pt["cagr"].min())
okn = pt.filter((pl.col("cagr") >= 0.15) & (pl.col("mdd") >= -0.35) & (pl.col("sharpe") >= 1.0)).height
g1 = (g0 and spread < 0.15 and okn == pt.height and s_mid["cagr"] >= 0.15
      and s_mid["mdd"] >= -0.35 and s_mid["sharpe"] >= 1.0 and pos_years >= 9
      and s_mid["n_trades"] >= 100)
results["G1_perturb"] = {"pass": g1, "spread": spread, "gates": f"{okn}/{pt.height}"}
print(f"G1:{'✅' if g1 else '❌'} spread {spread:.1%} gates {okn}/{pt.height}\n")

if g1:
    # ── G2:val(既有曲線合成)─────────────────────────────────────────
    vp, vf, ve = build_features(con, VAL_START, VAL_END)
    val_open = ledger.load_curve("T0101")
    val_close = run(vf, ve, vp, VAL_START, fill="next_close").nav   # 已量測數字的重現
    nav_val = synth5050(val_open, val_close)
    sv = metrics.perf_stats(nav_val)
    vyt = metrics.yearly_table(nav_val)
    h1 = float(vyt.filter(pl.col("year") == 2025)["ret"][0])
    need = 0.6 * s_mid["sharpe"]
    g2 = sv["sharpe"] >= need and sv["cagr"] >= 0.15 and h1 >= -0.08
    ledger.log_trial(family="validation", name="b14_v4_val_synth", hypothesis="v4 val 合成",
                     config={"synth": ["T0101", "val_close_repro"]},
                     window=f"{VAL_START}..{VAL_END}", metrics=sv, batch=BATCH, curve=nav_val)
    results["G2_val"] = {"pass": g2, "cagr": sv["cagr"], "sharpe": sv["sharpe"],
                         "mdd": sv["mdd"], "h1": h1, "need_sharpe": need}
    print(f"v4 val(合成): cagr {sv['cagr']:+.2%} sharpe {sv['sharpe']:.3f} "
          f"mdd {sv['mdd']:+.2%} 2025H1 {h1:+.1%}(需 Sharpe≥{need:.2f})→ G2:{'✅' if g2 else '❌'}\n")

    if g2:
        # ── G3:fill 雙測(mid vs open / mid vs close)────────────────
        s_open_dev = metrics.perf_stats(nav_open)
        s_close_dev = metrics.perf_stats(nav_close)
        sv_open = metrics.perf_stats(val_open)
        sv_close = metrics.perf_stats(val_close)
        decays = {
            "dev_vs_open": s_mid["cagr"] - s_open_dev["cagr"],
            "dev_vs_close": s_mid["cagr"] - s_close_dev["cagr"],
            "val_vs_open": sv["cagr"] - sv_open["cagr"],
            "val_vs_close": sv["cagr"] - sv_close["cagr"],
        }
        g3 = all(d <= 0.08 for d in decays.values())
        results["G3_fill"] = {"pass": g3} | decays
        print("fill 雙測:", {k: f"{v*100:+.1f}pp" for k, v in decays.items()},
              f"→ G3:{'✅' if g3 else '❌'}\n")

        if g3:
            # ── G4:battery ───────────────────────────────────────────
            bs = validate.block_bootstrap_cagr(res_mid.nav)
            trials = ledger.all_trials()
            n_trials = trials.height
            dev_ids = trials.filter(pl.col("window").str.starts_with(DEV_START))["trial_id"].to_list()
            curves = [pl.read_parquet(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))
                      for t in dev_ids if os.path.exists(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))]
            vsr = validate.sr_variance_from_curves(curves)
            dsr = validate.deflated_sharpe(res_mid.nav, n_trials=n_trials, sr_var_across_trials=vsr)
            common = None
            for c in curves:
                ds = set(c["date"].to_list())
                common = ds if common is None else (common & ds)
            common = sorted(common)
            mat = np.stack([validate.daily_returns(
                c.filter(pl.col("date").is_in(pl.Series(common).implode()))) for c in curves], axis=1)
            pbo = validate.pbo_cscv(mat, s=16)
            rng = np.random.default_rng(37)
            perm = []
            for i in range(200):
                r = run(feat, elig, panel, DEV_START, seed=rng)
                perm.append(metrics.perf_stats(r.nav)["cagr"])
                if (i + 1) % 50 == 0:
                    print(f"  perm {i+1}/200 ({time.time()-t0:.0f}s)")
            p_val = float((np.array(perm) >= s_mid["cagr"]).mean())
            g4 = bs["ci_lo"] > 0.10 and dsr["dsr"] > 0.95 and pbo["pbo"] < 0.5 and p_val < 0.05
            results["G4_battery"] = {"pass": g4, "bootstrap_lo": bs["ci_lo"], "dsr": dsr["dsr"],
                                     "n_trials": n_trials, "pbo": pbo["pbo"], "perm_p": p_val}
            print(f"battery:CI下界 {bs['ci_lo']:+.1%} DSR {dsr['dsr']:.4f}(N={n_trials})"
                  f" PBO {pbo['pbo']:.3f} perm p={p_val:.4f} → G4:{'✅' if g4 else '❌'}\n")

            # ── G5:壓測披露 ─────────────────────────────────────────
            for ws_, we_ in [("2008-01-02", "2009-12-31"), ("2010-01-04", "2011-12-30")]:
                sp, sf, se = build_features(con, ws_, we_)
                r = run(sf, se, sp, ws_, gate=GATE_PT)
                ss = metrics.perf_stats(r.nav)
                yy = metrics.yearly_table(r.nav)
                print(f"stress {ws_[:4]}-{we_[:4]}: cagr {ss['cagr']:+.1%} mdd {ss['mdd']:+.1%} "
                      f"最差年 {float(yy['ret'].min()):+.1%}")
            if g4:
                print("\n🏆🏆 v4 全憲法通過 → 冠軍確立,進入收斂三連批")

with open(os.path.join(ledger.LEDGER_DIR, "b14_gauntlet.json"), "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2, default=float)
print(f"\ntotal {time.time()-t0:.0f}s")

