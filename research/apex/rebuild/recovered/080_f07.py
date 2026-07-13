"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T19:48:49.904Z(工具 Bash)
涵蓋 trials(10):f07_stale26, f07_stale28, f07_stale30, f07_stale32, f07b_stale26_modern, f07b_stale28_modern, 現代era_R3, 現代era_S, 現代era_r08a, 現代era_v6
"""
"""F07b — stale 鄰域(27/29)+ 現代 era(2019 起)確認。"""
import polars as pl
from datetime import date as Date
from research.apex import data, ledger
from research.apex.assemble import build_features, entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import C, S_WTS, prep, kpi, paired
from research.apex.experiments.g01_ml_ranker import W3_START

def run_all(panel, feat, start, stales):
    elig = (data.eligibility(panel, min_adv=5_000_000.0)
            .filter(pl.col("eligible")).select(["date", C]))
    pool = (feat.filter(pl.col("rev_fresh_days") <= 7)
            .join(elig, on=["date", C], how="semi").drop_nulls(subset=list(S_WTS))
            .filter(pl.col("cfo_ni_ratio_ttm")
                    >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    geo = None
    for c_, wt in S_WTS.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        geo = term if geo is None else geo * term
    sc = (pool.with_columns(geo.alias("score")).select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(start).str.to_date()))
    e, _ = entries_and_flags(sc, 5, 10**9)
    out = {}
    for stale in stales:
        f = (feat.filter(pl.col("rev_fresh_days") >= stale).select(["date", C])
             .filter(pl.col("date") >= pl.lit(start).str.to_date()))
        res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                       exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                          loser_time_stop=15),
                       start=Date.fromisoformat(start))
        out[stale] = res.nav.select(["date", "nav"]).sort("date")
    return out

# W3 鄰域 27/29
con, panel3, feat3 = prep()
navs3 = run_all(panel3, feat3, W3_START, [27, 29])
print("W3 鄰域:")
for s, nv in navs3.items():
    k = kpi(nv)
    print(f"  stale{s}: CAGR {k['cagr']:+.1%}  P5 {k['p5']:+.1%}")

# 現代 era 2019 起(prep 全期版)
panelF, featF, _ = build_features(data.connect(), "2019-01-02", "2026-07-09")
# rev 軸重建(同 g01 prep 內邏輯,直接複用 prep 較繁,改用 g01 prep 於長窗:
from research.apex.experiments import g01_ml_ranker as g
import importlib
g_prep_start_orig = g.PREP_START
g.PREP_START = "2019-01-02"
conF, panelF, featF = g.prep()
g.PREP_START = g_prep_start_orig
navsF = run_all(panelF, featF, "2019-01-02", [26, 28])
print("\n現代 era 2019 起:")
for s, nv in navsF.items():
    k = kpi(nv)
    print(f"  stale{s}: CAGR {k['cagr']:+.1%}  P5 {k['p5']:+.1%}  MDD {k['mdd']:.1%}")
    ledger.log_trial(family="f_line", name=f"f07b_stale{s}_modern", hypothesis="跨揭露續抱全窗",
                     config={"stale": s}, window="2019-01-02..2026-07-09",
                     metrics={kk: float(vv) for kk, vv in k.items()}, batch="F07", curve=nv)
d = paired(navsF[28], navsF[26])
print(f"配對 stale28 − 26(現代era):{d['mean']:+.2%}/年  CI [{d['lo']:+.2%}, {d['hi']:+.2%}]")
