"""B09 — blend 重審,門檻回歸憲章標準(預註冊見 ledger/batches.md)。

Run: uv run --project . python -m quantlib.apex.experiments.b09_blend_charter
"""
from __future__ import annotations

import time
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data, ledger, metrics
from quantlib.apex.assemble import blend_score, build_features, entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DEV_START, DEV_END = "2012-01-02", "2023-12-29"
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
BATCH = "B09"
V1 = {"cagr": 0.283965, "sharpe": 1.718887, "mdd": -0.234949}

t0 = time.time()


def blend5050(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
    j = (a.select(["date", pl.col("nav").alias("na")])
         .join(b.select(["date", pl.col("nav").alias("nb")]), on="date", how="inner")
         .sort("date")
         .with_columns([
             (pl.col("na") / pl.col("na").shift(1) - 1).fill_null(0.0).alias("ra"),
             (pl.col("nb") / pl.col("nb").shift(1) - 1).fill_null(0.0).alias("rb"),
         ])
         .with_columns(((1 + 0.5 * pl.col("ra") + 0.5 * pl.col("rb")).cum_prod()).alias("nav")))
    return j.select(["date", "nav"])


dev_a = ledger.load_curve("T0038")
dev_b = ledger.load_curve("T0068")
nav_dev = blend5050(dev_a, dev_b)
s = metrics.perf_stats(nav_dev)

# Gate1:憲章 frontier improvement over v1 + dev gates
frontier_a = (s["cagr"] >= V1["cagr"] + 0.01 and s["mdd"] >= V1["mdd"] - 0.02
              and s["sharpe"] >= V1["sharpe"] - 0.10)
frontier_b = (s["mdd"] >= V1["mdd"] + 0.02 and s["cagr"] >= V1["cagr"] - 0.01)
frontier_c = (s["sharpe"] >= V1["sharpe"] + 0.15 and s["cagr"] >= V1["cagr"] - 0.01
              and s["mdd"] >= V1["mdd"] - 0.01)
dev_gates = s["cagr"] >= 0.15 and s["mdd"] >= -0.35 and s["sharpe"] >= 1.0
g1 = (frontier_a or frontier_b or frontier_c) and dev_gates
print(f"blend dev: cagr {s['cagr']:+.2%} sharpe {s['sharpe']:.3f} mdd {s['mdd']:+.2%}")
print(f"Gate1(憲章 frontier {'a' if frontier_a else 'b' if frontier_b else 'c' if frontier_c else '無'} + gates):{'✅' if g1 else '❌'}\n")

if g1:
    tid = ledger.log_trial(family="dual_rhythm", name="b09_blend5050_dev",
                           hypothesis="雙節奏 50/50(憲章門檻重審)",
                           config={"books": ["T0038", "T0068"], "w": 0.5},
                           window=f"{DEV_START}..{DEV_END}", metrics=s, batch=BATCH,
                           curve=nav_dev)
    con = data.connect()
    panel, feat, elig = build_features(con, DEV_START, DEV_END)

    def run_v1(fresh=5, stale=22, topn=20, trail=0.25, tstop=30):
        pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
        sc = blend_score(pool, elig, TRI, require=GATE).filter(
            pl.col("date") >= pl.lit(DEV_START).str.to_date())
        flags = feat.filter(pl.col("rev_fresh_days") >= stale).select(["date", C]).filter(
            pl.col("date") >= pl.lit(DEV_START).str.to_date())
        e, _ = entries_and_flags(sc, topn, 10**9)
        return simulate(panel, e, exit_flags=flags, exec_spec=ExecSpec(),
                        port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
                        exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
                        start=Date.fromisoformat(DEV_START)).nav

    rows = [{"variant": "base", **{k: s[k] for k in ("cagr", "sharpe", "mdd")}}]
    for nm, kw in [("fresh4", {"fresh": 4}), ("fresh6", {"fresh": 6}),
                   ("stale18", {"stale": 18}), ("stale26", {"stale": 26}),
                   ("n16", {"topn": 16}), ("n24", {"topn": 24}),
                   ("trail20", {"trail": 0.20}), ("trail30", {"trail": 0.30}),
                   ("time24", {"tstop": 24}), ("time36", {"tstop": 36})]:
        ss = metrics.perf_stats(blend5050(dev_a, run_v1(**kw)))
        rows.append({"variant": f"v1_{nm}", **{k: ss[k] for k in ("cagr", "sharpe", "mdd")}})
        print(f"  perturb v1_{nm:8s} cagr {ss['cagr']:+.1%} sharpe {ss['sharpe']:.2f} mdd {ss['mdd']:+.1%}")
    for nm, tid_ in [("b02f_t25", "T0018"), ("b02f_t45", "T0029")]:
        ss = metrics.perf_stats(blend5050(ledger.load_curve(tid_), dev_b))
        rows.append({"variant": nm, **{k: ss[k] for k in ("cagr", "sharpe", "mdd")}})
        print(f"  perturb {nm:11s} cagr {ss['cagr']:+.1%} sharpe {ss['sharpe']:.2f} mdd {ss['mdd']:+.1%}")
    pt = pl.DataFrame(rows)
    spread = float(pt["cagr"].max() - pt["cagr"].min())
    ok = pt.filter((pl.col("cagr") >= 0.15) & (pl.col("mdd") >= -0.35) & (pl.col("sharpe") >= 1.0)).height
    g2 = spread < 0.15 and ok == pt.height
    print(f"\nGate2 擾動:{'✅' if g2 else '❌'} spread {spread:.1%} gates {ok}/{pt.height}\n")

    if g2:
        nav_val = blend5050(ledger.load_curve("T0040"), ledger.load_curve("T0070"))
        sv = metrics.perf_stats(nav_val)
        yt = metrics.yearly_table(nav_val)
        h1 = float(yt.filter(pl.col("year") == 2025)["ret"][0])
        need_sharpe = 0.6 * s["sharpe"]
        g3 = sv["sharpe"] >= need_sharpe and sv["cagr"] >= 0.15 and h1 >= -0.08
        print(f"blend val: cagr {sv['cagr']:+.2%} sharpe {sv['sharpe']:.3f} mdd {sv['mdd']:+.2%} | 2025H1 {h1:+.1%}")
        print(f"Gate3 val(Sharpe≥{need_sharpe:.2f} CAGR≥15% H1≥-8%):{'✅' if g3 else '❌'}")
        ledger.log_trial(family="dual_rhythm", name="b09_blend5050_val",
                         hypothesis="blend val(既有曲線合成)",
                         config={"books": ["T0040", "T0070"], "w": 0.5},
                         window="2024-01-02..2025-06-30", metrics=sv, batch=BATCH,
                         curve=nav_val)
        if g3:
            print("\n🏆 三關全過 → P02 battery")

print(f"\ntotal {time.time()-t0:.1f}s")
