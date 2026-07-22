# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T01:12:59.309Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/b13_monthly_gauntlet.py)
# 涵蓋 trials(2):b13_d11_dev, b13_d11_val
"""B13 — 月頻家族 gauntlet(primary d11 → 必要時 secondary d15;預註冊見 batches.md)。

Run: uv run --project research python -m research.apex.experiments.b13_monthly_gauntlet
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
BATCH = "B13"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}

t0 = time.time()
con = data.connect()
results: dict = {}

CTX: dict = {}


def ctx(ws, we):
    key = (ws, we)
    if key not in CTX:
        panel, feat, elig = build_features(con, ws, we)
        td = panel.select(pl.col("date").unique().sort()).with_columns(
            [pl.col("date").dt.day().alias("dom"), pl.col("date").dt.month().alias("m"),
             pl.col("date").dt.year().alias("y")])
        CTX[key] = (panel, feat, elig, td, data.benchmark_nav(con, ws, we))
    return CTX[key]


def decision_days(td: pl.DataFrame, lo: int) -> pl.Series:
    return (td.filter(pl.col("dom") >= lo).group_by(["y", "m"])
            .agg(pl.col("date").min()).get_column("date"))


def run(ws, we, *, day_lo=11, topn=20, trail=0.25, mom_w=0.5, fill="next_open"):
    panel, feat, elig, td, _ = ctx(ws, we)
    w = dict(W4) | {"mom_126_5": mom_w}
    days = decision_days(td, day_lo)
    sc = (blend_score(feat, elig, w, require=GATE)
          .filter(pl.col("date") >= pl.lit(ws).str.to_date())
          .filter(pl.col("date").is_in(days.implode())))
    r = sc.with_columns(pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
    e = r.filter(pl.col("rk") <= topn).select(["date", C, "score"])
    f = (panel.select(["date", C]).filter(pl.col("date").is_in(days.implode()))
         .join(e.select(["date", C]), on=["date", C], how="anti"))
    return simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(fill_at=fill),
                    port_spec=PortSpec(n_slots=topn, max_new_per_day=topn),
                    exit_spec=ExitSpec(trailing_stop=trail),
                    start=Date.fromisoformat(ws))


def gauntlet(tag: str, day_lo: int, allow_val: bool) -> dict:
    out: dict = {"tag": tag}
    _, _, _, _, bench = ctx(DEV_START, DEV_END)
    res = run(DEV_START, DEV_END, day_lo=day_lo)
    s = metrics.summarize(res.nav, res.trades, bench)
    yt = metrics.yearly_table(res.nav)
    pos_years = int((yt["ret"] > 0).sum())
    tid = ledger.log_trial(family="rev_monthly", name=f"b13_{tag}_dev",
                           hypothesis="月頻 gauntlet", config={"day_lo": day_lo, **W4},
                           window=f"{DEV_START}..{DEV_END}", metrics=s, batch=BATCH,
                           curve=res.nav)
    print(f"{tid} {tag} dev: cagr {s['cagr']:+.2%} sharpe {s['sharpe']:.3f} "
          f"mdd {s['mdd']:+.2%} 正年 {pos_years}/12 trades {s['n_trades']}")
    rows = [{"variant": "base", "cagr": s["cagr"], "sharpe": s["sharpe"], "mdd": s["mdd"]}]
    for nm, kw in [("n16", {"topn": 16}), ("n24", {"topn": 24}),
                   ("trail20", {"trail": 0.20}), ("trail30", {"trail": 0.30}),
                   ("momw40", {"mom_w": 0.4}), ("momw60", {"mom_w": 0.6}),
                   ("day+1", {"day_lo": day_lo + 1}), ("day+2", {"day_lo": day_lo + 2})]:
        ss = metrics.perf_stats(run(DEV_START, DEV_END, **({"day_lo": day_lo} | kw)).nav)
        rows.append({"variant": nm, "cagr": ss["cagr"], "sharpe": ss["sharpe"], "mdd": ss["mdd"]})
        print(f"  {nm:8s} cagr {ss['cagr']:+.1%} sharpe {ss['sharpe']:.2f} mdd {ss['mdd']:+.1%}")
    pt = pl.DataFrame(rows)
    spread = float(pt["cagr"].max() - pt["cagr"].min())
    okn = pt.filter((pl.col("cagr") >= 0.15) & (pl.col("mdd") >= -0.35)
                    & (pl.col("sharpe") >= 1.0)).height
    g1 = (spread < 0.15 and okn == pt.height and s["cagr"] >= 0.15 and s["mdd"] >= -0.35
          and s["sharpe"] >= 1.0 and pos_years >= 9 and s["n_trades"] >= 100)
    out["G1"] = {"pass": g1, "spread": spread, "gates": f"{okn}/{pt.height}",
                 "dev": {k: s[k] for k in ("cagr", "sharpe", "mdd")}}
    print(f"{tag} G1:{'✅' if g1 else '❌'} spread {spread:.1%} gates {okn}/{pt.height}\n")
    if not (g1 and allow_val):
        return out

    _, _, _, _, vbench = ctx(VAL_START, VAL_END)
    vres = run(VAL_START, VAL_END, day_lo=day_lo)
    vs = metrics.summarize(vres.nav, vres.trades, vbench)
    vyt = metrics.yearly_table(vres.nav)
    h1 = float(vyt.filter(pl.col("year") == 2025)["ret"][0])
    need = 0.6 * s["sharpe"]
    g2 = vs["sharpe"] >= need and vs["cagr"] >= 0.15 and h1 >= -0.08
    vtid = ledger.log_trial(family="validation", name=f"b13_{tag}_val", hypothesis="月頻 val",
                            config={"day_lo": day_lo}, window=f"{VAL_START}..{VAL_END}",
                            metrics=vs, batch=BATCH, curve=vres.nav)
    out["G2"] = {"pass": g2, "val": {k: vs[k] for k in ("cagr", "sharpe", "mdd")}, "h1": h1}
    print(f"{vtid} {tag} val: cagr {vs['cagr']:+.2%} sharpe {vs['sharpe']:.3f} "
          f"mdd {vs['mdd']:+.2%} 2025H1 {h1:+.1%} → G2:{'✅' if g2 else '❌'}\n")
    if not g2:
        return out

    dc = metrics.perf_stats(run(DEV_START, DEV_END, day_lo=day_lo, fill="next_close").nav)
    vc = metrics.perf_stats(run(VAL_START, VAL_END, day_lo=day_lo, fill="next_close").nav)
    d_dec, v_dec = s["cagr"] - dc["cagr"], vs["cagr"] - vc["cagr"]
    g3 = d_dec <= 0.08 and v_dec <= 0.08
    out["G3"] = {"pass": g3, "dev_decay": d_dec, "val_decay": v_dec,
                 "val_close": {k: vc[k] for k in ("cagr", "sharpe", "mdd")}}
    print(f"{tag} fill 雙測:dev {d_dec*100:+.1f}pp val {v_dec*100:+.1f}pp → G3:{'✅' if g3 else '❌'}\n")
    if not g3:
        return out

    # G4 battery
    bs = validate.block_bootstrap_cagr(res.nav)
    trials = ledger.all_trials()
    n_trials = trials.height
    dev_ids = trials.filter(pl.col("window").str.starts_with(DEV_START))["trial_id"].to_list()
    curves = [pl.read_parquet(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))
              for t in dev_ids if os.path.exists(os.path.join(ledger.CURVES_DIR, f"{t}.parquet"))]
    vsr = validate.sr_variance_from_curves(curves)
    dsr = validate.deflated_sharpe(res.nav, n_trials=n_trials, sr_var_across_trials=vsr)
    common = None
    for c in curves:
        ds = set(c["date"].to_list())
        common = ds if common is None else (common & ds)
    common = sorted(common)
    mat = np.stack([validate.daily_returns(c.filter(pl.col("date").is_in(pl.Series(common).implode())))
                    for c in curves], axis=1)
    pbo = validate.pbo_cscv(mat, s=16)
    rng = np.random.default_rng(31)
    panel, feat, elig, td, _ = ctx(DEV_START, DEV_END)
    days = decision_days(td, day_lo)
    sc_all = (blend_score(feat, elig, W4, require=GATE)
              .filter(pl.col("date") >= pl.lit(DEV_START).str.to_date())
              .filter(pl.col("date").is_in(days.implode())))
    all_days = panel.select(["date", C]).filter(pl.col("date").is_in(days.implode()))
    perm = []
    for i in range(200):
        rnd = sc_all.with_columns(pl.Series("score", rng.random(sc_all.height)))
        rr = rnd.with_columns(pl.col("score").rank("ordinal", descending=True).over("date").alias("rk"))
        e = rr.filter(pl.col("rk") <= 20).select(["date", C, "score"])
        f = all_days.join(e.select(["date", C]), on=["date", C], how="anti")
        r = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                     port_spec=PortSpec(n_slots=20, max_new_per_day=20),
                     exit_spec=ExitSpec(trailing_stop=0.25), start=Date.fromisoformat(DEV_START))
        perm.append(metrics.perf_stats(r.nav)["cagr"])
        if (i + 1) % 50 == 0:
            print(f"  perm {i+1}/200 ({time.time()-t0:.0f}s)")
    p_val = float((np.array(perm) >= s["cagr"]).mean())
    g4 = bs["ci_lo"] > 0.10 and dsr["dsr"] > 0.95 and pbo["pbo"] < 0.5 and p_val < 0.05
    out["G4"] = {"pass": g4, "bootstrap_lo": bs["ci_lo"], "dsr": dsr["dsr"],
                 "n_trials": n_trials, "pbo": pbo["pbo"], "perm_p": p_val,
                 "perm_null_median": float(np.median(perm))}
    print(f"{tag} battery:CI下界 {bs['ci_lo']:+.1%} DSR {dsr['dsr']:.4f}(N={n_trials})"
          f" PBO {pbo['pbo']:.3f} perm p={p_val:.4f} → G4:{'✅' if g4 else '❌'}")
    return out


primary = gauntlet("d11", 11, allow_val=True)
results["primary_d11"] = primary
need_secondary = (primary.get("G1", {}).get("pass") and primary.get("G2", {}).get("pass")
                  and not primary.get("G3", {}).get("pass", False))
if not primary.get("G1", {}).get("pass"):
    print("primary G1 敗 → secondary 不啟動(prereg:僅 G3-only 敗才啟動)")
if need_secondary:
    print("\n── primary 僅敗 G3 → 啟動 secondary d15 ──\n")
    results["secondary_d15"] = gauntlet("d15", 15, allow_val=True)

with open(os.path.join(ledger.LEDGER_DIR, "b13_gauntlet.json"), "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2, default=float)
print(f"\ntotal {time.time()-t0:.0f}s")

