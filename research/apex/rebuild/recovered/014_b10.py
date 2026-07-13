"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T00:51:18.683Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/b10_v1s26.py)
涵蓋 trials(2):b10_v1s26_dev, b10_v1s26_val
"""
"""B10 — v1s26 單一候選:dev + ±20% 網格(Gate1);過了才開 val(Gate2)。

Run: uv run --project research python -m research.apex.experiments.b10_v1s26
"""
from __future__ import annotations

import time
from datetime import date as Date

import polars as pl

from research.apex import data, ledger, metrics
from research.apex.assemble import blend_score, build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DEV_START, DEV_END = "2012-01-02", "2023-12-29"
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
BATCH = "B10"

t0 = time.time()
con = data.connect()


def run_cfg(ws, we, *, fresh=5, stale=26, topn=20, trail=0.25, tstop=30, fill="next_open",
            feat=None, elig=None, panel=None):
    pool = feat.filter(pl.col("rev_fresh_days") <= fresh)
    sc = blend_score(pool, elig, TRI, require=GATE).filter(pl.col("date") >= pl.lit(ws).str.to_date())
    flags = feat.filter(pl.col("rev_fresh_days") >= stale).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    return simulate(panel, e, exit_flags=flags, exec_spec=ExecSpec(fill_at=fill),
                    port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=trail, time_stop=tstop),
                    start=Date.fromisoformat(ws))


panel, feat, elig = build_features(con, DEV_START, DEV_END)
bench = data.benchmark_nav(con, DEV_START, DEV_END)

res = run_cfg(DEV_START, DEV_END, feat=feat, elig=elig, panel=panel)
s = metrics.summarize(res.nav, res.trades, bench)
tid = ledger.log_trial(family="rev_cycle", name="b10_v1s26_dev", hypothesis="stale 平台化",
                       config={"stale": 26, "base": "v1"}, window=f"{DEV_START}..{DEV_END}",
                       metrics=s, batch=BATCH, curve=res.nav)
print(f"{tid} v1s26 dev: cagr {s['cagr']:+.2%} sharpe {s['sharpe']:.3f} mdd {s['mdd']:+.2%} "
      f"trades {s['n_trades']}")
yt = metrics.yearly_table(res.nav)
pos_years = int((yt["ret"] > 0).sum())
base_gates = (s["cagr"] >= 0.15 and s["mdd"] >= -0.35 and s["sharpe"] >= 1.0
              and pos_years >= 9 and s["n_trades"] >= 100)
print(f"dev gates(含正年 {pos_years}/12):{'✅' if base_gates else '❌'}")

rows = [{"variant": "base", "cagr": s["cagr"], "sharpe": s["sharpe"], "mdd": s["mdd"]}]
for nm, kw in [("fresh4", {"fresh": 4}), ("fresh6", {"fresh": 6}),
               ("stale21", {"stale": 21}), ("stale31", {"stale": 31}),
               ("n16", {"topn": 16}), ("n24", {"topn": 24}),
               ("trail20", {"trail": 0.20}), ("trail30", {"trail": 0.30}),
               ("time24", {"tstop": 24}), ("time36", {"tstop": 36})]:
    ss = metrics.perf_stats(run_cfg(DEV_START, DEV_END, feat=feat, elig=elig, panel=panel, **kw).nav)
    rows.append({"variant": nm, "cagr": ss["cagr"], "sharpe": ss["sharpe"], "mdd": ss["mdd"]})
    print(f"  perturb {nm:8s} cagr {ss['cagr']:+.1%} sharpe {ss['sharpe']:.2f} mdd {ss['mdd']:+.1%}")
pt = pl.DataFrame(rows)
spread = float(pt["cagr"].max() - pt["cagr"].min())
ok = pt.filter((pl.col("cagr") >= 0.15) & (pl.col("mdd") >= -0.35) & (pl.col("sharpe") >= 1.0)).height
g1 = spread < 0.15 and ok == pt.height and base_gates
print(f"\nGate1:{'✅' if g1 else '❌'} spread {spread:.1%}(<15%)gates {ok}/{pt.height}")

if g1:
    vp, vf, ve = build_features(con, "2024-01-02", "2025-06-30")
    vbench = data.benchmark_nav(con, "2024-01-02", "2025-06-30")
    vres = run_cfg("2024-01-02", "2025-06-30", feat=vf, elig=ve, panel=vp)
    vs = metrics.summarize(vres.nav, vres.trades, vbench)
    vyt = metrics.yearly_table(vres.nav)
    h1 = float(vyt.filter(pl.col("year") == 2025)["ret"][0])
    need = 0.6 * s["sharpe"]
    g2 = vs["sharpe"] >= need and vs["cagr"] >= 0.15 and h1 >= -0.08
    vtid = ledger.log_trial(family="validation", name="b10_v1s26_val", hypothesis="v1s26 val",
                            config={"stale": 26}, window="2024-01-02..2025-06-30",
                            metrics=vs, batch=BATCH, curve=vres.nav)
    print(f"\n{vtid} val: cagr {vs['cagr']:+.2%} sharpe {vs['sharpe']:.3f} mdd {vs['mdd']:+.2%} "
          f"2025H1 {h1:+.1%}")
    print(f"Gate2 val(Sharpe≥{need:.2f}):{'✅ → P02 battery' if g2 else '❌ 出局'}")

print(f"\ntotal {time.time()-t0:.1f}s")

