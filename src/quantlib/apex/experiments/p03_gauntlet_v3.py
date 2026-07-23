"""P03 — apex_revcycle_v3(= v1s26 + 0.5×mom126 軸)完整憲法 gauntlet。

序:G1 dev+±20%(12 變體)→ G2 val(touch #4)→ G3 fill 雙測 → G4 battery
(permutation/bootstrap/DSR/PBO)→ G5 壓測(只評估,披露)。一關敗即停。
Run: uv run --project . python -m quantlib.apex.experiments.p03_gauntlet_v3
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
VAL_START, VAL_END = "2024-01-02", "2025-06-30"
BATCH = "P04"
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
        mom_w=0.5, fill="next_open", gate=None, seed=None):
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


panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)

# ── G1:dev + ±20% ───────────────────────────────────────────────────────
res = run(feat, elig, panel, DEV_START)
s = metrics.summarize(res.nav, res.trades, bench)
yt = metrics.yearly_table(res.nav)
pos_years = int((yt["ret"] > 0).sum())
tid = ledger.log_trial(family="rev_cycle_v3", name="p03_v3_dev", hypothesis="v3 gauntlet",
                       config={"mom_w": 0.5, "stale": 26}, window=f"{DEV_START}..{DEV_END}",
                       metrics=s, batch=BATCH, curve=res.nav)
print(f"{tid} v3 dev: cagr {s['cagr']:+.2%} sharpe {s['sharpe']:.3f} mdd {s['mdd']:+.2%} 正年 {pos_years}/12")
rows = [{"variant": "base", "cagr": s["cagr"], "sharpe": s["sharpe"], "mdd": s["mdd"]}]
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
g1 = (spread < 0.15 and okn == pt.height and s["cagr"] >= 0.15 and s["mdd"] >= -0.35
      and s["sharpe"] >= 1.0 and pos_years >= 9 and s["n_trades"] >= 100)
results["G1_perturb"] = {"pass": g1, "spread": spread, "gates": f"{okn}/{pt.height}"}
print(f"G1:{'✅' if g1 else '❌'} spread {spread:.1%} gates {okn}/{pt.height}\n")

if g1:
    # ── G2:val(touch #4)──────────────────────────────────────────────
    vp, vf, ve = build_features(con, VAL_START, VAL_END)
    vbench = data.benchmark_nav(con, VAL_START, VAL_END)
    vres = run(vf, ve, vp, VAL_START)
    vs = metrics.summarize(vres.nav, vres.trades, vbench)
    vyt = metrics.yearly_table(vres.nav)
    h1 = float(vyt.filter(pl.col("year") == 2025)["ret"][0])
    need = 0.6 * s["sharpe"]
    g2 = vs["sharpe"] >= need and vs["cagr"] >= 0.15 and h1 >= -0.08
    vtid = ledger.log_trial(family="validation", name="p03_v3_val", hypothesis="v3 val",
                            config={"mom_w": 0.5}, window=f"{VAL_START}..{VAL_END}",
                            metrics=vs, batch=BATCH, curve=vres.nav)
    results["G2_val"] = {"pass": g2, "cagr": vs["cagr"], "sharpe": vs["sharpe"],
                         "mdd": vs["mdd"], "h1_2025": h1}
    print(f"{vtid} val: cagr {vs['cagr']:+.2%} sharpe {vs['sharpe']:.3f} mdd {vs['mdd']:+.2%} "
          f"2025H1 {h1:+.1%} → G2:{'✅' if g2 else '❌'}\n")

    if g2:
        # ── G3:fill 雙測 ────────────────────────────────────────────────
        dc = metrics.perf_stats(run(feat, elig, panel, DEV_START, fill="next_close").nav)
        vc = metrics.perf_stats(run(vf, ve, vp, VAL_START, fill="next_close").nav)
        d_dec, v_dec = s["cagr"] - dc["cagr"], vs["cagr"] - vc["cagr"]
        g3 = d_dec <= 0.08 and v_dec <= 0.08
        results["G3_fill"] = {"pass": g3, "dev_decay": d_dec, "val_decay": v_dec,
                              "val_close_sharpe": vc["sharpe"]}
        print(f"fill 雙測:dev 衰減 {d_dec*100:.1f}pp val 衰減 {v_dec*100:.1f}pp → G3:{'✅' if g3 else '❌'}\n")

        if g3:
            # ── G4:battery ─────────────────────────────────────────────
            champ = res.nav
            bs = validate.block_bootstrap_cagr(champ)
            trials = ledger.all_trials()
            n_trials = trials.height
            dev_ids = trials.filter(pl.col("window").str.starts_with(DEV_START))["trial_id"].to_list()
            curves = [pl.read_parquet(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))
                      for t in dev_ids if os.path.exists(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))]
            vsr = validate.sr_variance_from_curves(curves)
            dsr = validate.deflated_sharpe(champ, n_trials=n_trials, sr_var_across_trials=vsr)
            common = None
            for c in curves:
                ds = set(c["date"].to_list())
                common = ds if common is None else (common & ds)
            common = sorted(common)
            mat = np.stack([validate.daily_returns(c.filter(pl.col("date").is_in(pl.Series(common).implode())))
                            for c in curves], axis=1)
            pbo = validate.pbo_cscv(mat, s=16)
            rng = np.random.default_rng(23)
            perm = []
            for i in range(200):
                r = run(feat, elig, panel, DEV_START, seed=rng)
                perm.append(metrics.perf_stats(r.nav)["cagr"])
                if (i + 1) % 50 == 0:
                    print(f"  perm {i+1}/200 ({time.time()-t0:.0f}s)")
            p_val = float((np.array(perm) >= s["cagr"]).mean())
            g4 = bs["ci_lo"] > 0.10 and dsr["dsr"] > 0.95 and pbo["pbo"] < 0.5 and p_val < 0.05
            results["G4_battery"] = {"pass": g4, "bootstrap_lo": bs["ci_lo"],
                                     "dsr": dsr["dsr"], "n_trials": n_trials,
                                     "pbo": pbo["pbo"], "perm_p": p_val,
                                     "perm_null_median": float(np.median(perm))}
            print(f"battery:CI下界 {bs['ci_lo']:+.1%} DSR {dsr['dsr']:.4f}(N={n_trials})"
                  f" PBO {pbo['pbo']:.3f} perm p={p_val:.4f} → G4:{'✅' if g4 else '❌'}\n")

            # ── G5:壓測(只評估)────────────────────────────────────────
            det = []
            for ws_, we_ in [("2008-01-02", "2009-12-31"), ("2010-01-04", "2011-12-30")]:
                sp, sf, se = build_features(con, ws_, we_)
                r = run(sf, se, sp, ws_, gate=GATE_PT)
                ss = metrics.perf_stats(r.nav)
                yy = metrics.yearly_table(r.nav)
                det.append({"window": f"{ws_[:4]}-{we_[:4]}", "cagr": ss["cagr"],
                            "mdd": ss["mdd"], "worst_year": float(yy["ret"].min())})
                print(f"stress {ws_[:4]}-{we_[:4]}: cagr {ss['cagr']:+.1%} mdd {ss['mdd']:+.1%} "
                      f"最差年 {float(yy['ret'].min()):+.1%}")
            results["G5_stress_disclosure"] = det

            if g4:
                print("\n🏆🏆 v3 全憲法通過 → 收斂階段(3 連乾涸批)後 final holdout")

with open(os.path.join(ledger.LEDGER_DIR, "p03_gauntlet.json"), "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2, default=float)
print(f"\ntotal {time.time()-t0:.0f}s")
