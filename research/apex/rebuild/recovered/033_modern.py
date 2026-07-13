"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:30:17.005Z(工具 Bash)
涵蓋 trials(14):modern_r06e_geo_n8_t30, modern_r07d_revlevel, modern_r07f_t35, oos_r03d_n8, oos_r03h_n10_momw75, oos_r06e_geo_n8_t30, oos_r07d_revlevel, oos_r07f_t35, oos_r08a_t35_revlevel, r06e_geo_n8_t30, 現代era_R3, 現代era_S, 現代era_r08a, 現代era_v6
"""
"""r06e OOS 熱年(touch #3)+ 現代era連續對決正2。"""
import polars as pl
import numpy as np
from datetime import date as Date
from research.apex import data, ledger, metrics
from research.apex.assemble import blend_score, build_features, entries_and_flags, run_trial
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research import prices

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W4 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5}
con = data.connect()

def run_geo(ws, we, topn=8, trail=0.30):
    panel, feat, elig = build_features(con, ws, we)
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    cols = list(W4)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=cols))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in W4.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, topn, 10**9)
    f = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=topn, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=trail, time_stop=30),
                    start=Date.fromisoformat(ws)), panel

# OOS 熱年
res, _ = run_geo("2025-07-01", "2026-07-07")
bench_h = data.benchmark_nav(con, "2025-07-01", "2026-07-07")
s = metrics.summarize(res.nav, res.trades, bench_h)
tid = ledger.log_trial(family="oos_hot_year", name="oos_r06e_geo_n8_t30",
                       hypothesis="R06 晉級者 OOS(touch #3)", config={"geo": True, "topn": 8, "trail": 0.30},
                       window="2025-07-01..2026-07-07", metrics=s, batch="R06-OOS", curve=res.nav)
tot = s["final_nav_ratio"] - 1
print(f"{tid} OOS 熱年:總報酬 {tot:+.1%} | Sharpe {s['sharpe']:.2f} | MDD {s['mdd']:+.1%} "
      f"| {'✅ 升級(≥+72.2%∧MDD≥−30%)' if tot >= 0.722 and s['mdd'] >= -0.30 else '❌'}")

# 現代era連續對決(2019 → 2026-07)
res2, _ = run_geo("2019-01-02", "2026-07-07")
s2 = metrics.perf_stats(res2.nav)
tid2 = ledger.log_trial(family="fullspan", name="modern_r06e_geo_n8_t30",
                        hypothesis="現代era連續 vs 正2", config={"geo": True, "topn": 8, "trail": 0.30},
                        window="2019-01-02..2026-07-07", metrics=s2, batch="R06-OOS", curve=res2.nav)
print(f"{tid2} 現代era連續:CAGR {s2['cagr']:+.1%} | Sharpe {s2['sharpe']:.2f} | MDD {s2['mdd']:+.1%} "
      f"| 終值 {s2['final_nav_ratio']:.1f}x")
print(f"正2 同窗:+55.9% / 1.33 / −55.1% / 28.1x → {'🏆 超越' if s2['cagr'] > 0.559 else '❌ 未及'}")
yt = metrics.yearly_table(res2.nav)
print("逐年:", "  ".join(f"{y}:{r*100:+.0f}%" for y, r in zip(yt["year"], yt["ret"])))
