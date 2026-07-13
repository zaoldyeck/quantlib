"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T08:34:48.898Z(工具 Bash)
涵蓋 trials(14):oos_r03d_n8, oos_r03h_n10_momw75, oos_r06e_geo_n8_t30, oos_r07d_revlevel, oos_r07f_t35, oos_r08a_t35_revlevel, r08a_t35_revlevel, 正2全史同窗_R3, 正2全史同窗_r08a, 正2全史同窗_v6, 現代era_R3, 現代era_S, 現代era_r08a, 現代era_v6
"""
"""最終 OOS touch(#6/6):r08a_t35_revlevel 熱年終審 + 現代era連續。"""
import polars as pl
from datetime import date as Date
from research.apex import data, ledger, metrics
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

C = "company_code"
GATE = [pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")]
W5 = {"rev_yoy_accel": 1.0, "high_52w": 1.0, "close_pos_20": 1.0, "mom_126_5": 0.5,
      "rev_yoy": 0.5}
con = data.connect()

def run(ws, we):
    panel, feat, elig = build_features(con, ws, we)
    pool = feat.filter(pl.col("rev_fresh_days") <= 5)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi").drop_nulls(subset=list(W5)))
    for cond in GATE:
        df = df.filter(cond)
    expr = None
    for c_, wt in W5.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = df.with_columns(expr.alias("score")).select(["date", C, "score"]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    e, _ = entries_and_flags(sc, 8, 10**9)
    fl = feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C]).filter(
        pl.col("date") >= pl.lit(ws).str.to_date())
    return simulate(panel, e, exit_flags=fl, exec_spec=ExecSpec(),
                    port_spec=PortSpec(n_slots=8, max_new_per_day=5),
                    exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30),
                    start=Date.fromisoformat(ws))

res = run("2025-07-01", "2026-07-07")
s = metrics.perf_stats(res.nav)
tot = s["final_nav_ratio"] - 1
ledger.log_trial(family="oos_hot_year", name="oos_r08a_t35_revlevel",
                 hypothesis="最終 OOS touch #6", config={"W5": True, "trail": 0.35},
                 window="2025-07-01..2026-07-07", metrics=s, batch="R08-OOS", curve=res.nav)
print(f"OOS 熱年:{tot:+.1%} | Sharpe {s['sharpe']:.2f} | MDD {s['mdd']:+.1%} | "
      f"{'✅ 升級為最終旗艦' if tot >= 0.723 and s['mdd'] >= -0.30 else '❌ r07f 保持旗艦'}")

for ws, we, tag, target in [("2019-01-02", "2026-07-07", "現代era", 0.559),
                             ("2014-11-03", "2026-07-07", "正2全史同窗", 0.377)]:
    m = run(ws, we)
    sm = metrics.perf_stats(m.nav)
    ledger.log_trial(family="fullspan", name=f"{tag}_r08a", hypothesis="r08a 連續窗",
                     config={"W5": True}, window=f"{ws}..{we}", metrics=sm,
                     batch="R08-OOS", curve=m.nav)
    print(f"{tag}:CAGR {sm['cagr']:+.1%} | Sharpe {sm['sharpe']:.2f} | MDD {sm['mdd']:+.1%} | "
          f"{sm['final_nav_ratio']:.0f}x {'🏆>正2' if sm['cagr'] > target else '❌'}")
