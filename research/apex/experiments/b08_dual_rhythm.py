"""B08 — 雙節奏 blend(b02f ⊕ revcycle_v1, 50/50;預註冊見 ledger/batches.md)。

判準依序:dev → 擾動 → val(既有曲線合成,零新參數)。
Run: uv run --project research python -m research.apex.experiments.b08_dual_rhythm
"""
from __future__ import annotations

import time
from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data, ledger, metrics
from research.apex.assemble import blend_score, build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
DEV_START, DEV_END = "2012-01-02", "2023-12-29"
TRI = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0}
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
BATCH = "B08"

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


def stats(nav, tag):
    s = metrics.perf_stats(nav)
    print(f"{tag:24s} cagr {s['cagr']:+.1%} sharpe {s['sharpe']:.2f} mdd {s['mdd']:+.1%}")
    return s


# ── Gate 1:dev blend ───────────────────────────────────────────────────
dev_a = ledger.load_curve("T0038")   # b02f dev
dev_b = ledger.load_curve("T0068")   # revcycle v1 dev
ra, rb = (dev_a["nav"].to_numpy(), dev_b["nav"].to_numpy())
corr = float(np.corrcoef(ra[1:] / ra[:-1] - 1, rb[1:] / rb[:-1] - 1)[0, 1])
print(f"兩書日報酬相關性:{corr:.3f}")
nav_dev = blend5050(dev_a, dev_b)
s_dev = stats(nav_dev, "blend dev")
g1 = s_dev["sharpe"] >= 1.75 and s_dev["cagr"] >= 0.28 and s_dev["mdd"] >= -0.24
print(f"Gate1 dev:{'✅' if g1 else '❌'}(需 Sharpe≥1.75 CAGR≥28% MDD≥-24%)\n")

bench_dev = None
if g1:
    con = data.connect()
    bench_dev = data.benchmark_nav(con, DEV_START, DEV_END)
    tid = ledger.log_trial(family="dual_rhythm", name="b08a_blend5050_dev",
                           hypothesis="雙節奏 50/50", config={"books": ["T0038", "T0068"]},
                           window=f"{DEV_START}..{DEV_END}",
                           metrics=s_dev, batch=BATCH, curve=nav_dev)
    print(f"logged {tid}")

    # ── Gate 2:擾動(v1 側 10 變體 × 固定 b02f;b02f 側 2 變體 × 固定 v1)──
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

    variants = []
    for nm, kw in [("fresh4", {"fresh": 4}), ("fresh6", {"fresh": 6}),
                   ("stale18", {"stale": 18}), ("stale26", {"stale": 26}),
                   ("n16", {"topn": 16}), ("n24", {"topn": 24}),
                   ("trail20", {"trail": 0.20}), ("trail30", {"trail": 0.30}),
                   ("time24", {"tstop": 24}), ("time36", {"tstop": 36})]:
        nav = blend5050(dev_a, run_v1(**kw))
        s = metrics.perf_stats(nav)
        variants.append({"variant": f"v1_{nm}", **{k: s[k] for k in ("cagr", "sharpe", "mdd")}})
        print(f"  perturb v1_{nm:8s} cagr {s['cagr']:+.1%} sharpe {s['sharpe']:.2f} mdd {s['mdd']:+.1%}")
    for nm, tid_ in [("b02f_t25", "T0018"), ("b02f_t45", "T0029")]:
        nav = blend5050(ledger.load_curve(tid_), dev_b)
        s = metrics.perf_stats(nav)
        variants.append({"variant": nm, **{k: s[k] for k in ("cagr", "sharpe", "mdd")}})
        print(f"  perturb {nm:11s} cagr {s['cagr']:+.1%} sharpe {s['sharpe']:.2f} mdd {s['mdd']:+.1%}")
    pt = pl.DataFrame(variants)
    allc = pt["cagr"].to_list() + [s_dev["cagr"]]
    spread = max(allc) - min(allc)
    gates_ok = pt.filter((pl.col("cagr") >= 0.15) & (pl.col("mdd") >= -0.35)
                         & (pl.col("sharpe") >= 1.0)).height
    g2 = spread < 0.15 and gates_ok == pt.height
    print(f"Gate2 擾動:{'✅' if g2 else '❌'} spread {spread:.1%}(<15%)gates {gates_ok}/{pt.height}\n")

    # ── Gate 3:val blend(既有曲線;val 動用第 3 次)────────────────────
    if g2:
        nav_val = blend5050(ledger.load_curve("T0040"), ledger.load_curve("T0070"))
        s_val = stats(nav_val, "blend val")
        yt = metrics.yearly_table(nav_val)
        print(yt)
        h1 = float(yt.filter(pl.col("year") == 2025)["ret"][0])
        g3 = s_val["sharpe"] >= 1.05 and s_val["cagr"] >= 0.15 and h1 >= -0.08
        print(f"Gate3 val:{'✅' if g3 else '❌'}(Sharpe≥1.05 CAGR≥15% 2025H1≥-8%;實際 {h1:+.1%})")
        ledger.log_trial(family="dual_rhythm", name="b08a_blend5050_val",
                         hypothesis="雙節奏 50/50 val", config={"books": ["T0040", "T0070"]},
                         window="2024-01-02..2025-06-30", metrics=s_val, batch=BATCH, curve=nav_val)

print(f"\ntotal {time.time()-t0:.1f}s")
