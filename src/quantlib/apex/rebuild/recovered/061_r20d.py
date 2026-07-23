"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T12:41:08.850Z(工具 Bash)
涵蓋 trials(3):r20d_n4_s40_f7, r23d_n5_rel05_f7, r24_ensemble3
"""
"""R24 — config 集成(三書等權,曲線合成;預註冊:frontier (c) vs R3 =
Sharpe ≥ 2.59 ∧ CAGR ≥ 112.7 ∧ MDD ≥ −34.3;或 (a) CAGR ≥114.7 ∧ MDD ≥ −35.3)。"""
import polars as pl
import numpy as np
from quantlib.apex import ledger, metrics

def blend(navs, ws):
    j = None
    for i, nv in enumerate(navs):
        d = nv.sort("date").select(["date", pl.col("nav").alias(f"n{i}")])
        j = d if j is None else j.join(d, on="date", how="inner")
    j = j.sort("date")
    for i in range(len(navs)):
        j = j.with_columns((pl.col(f"n{i}") / pl.col(f"n{i}").shift(1) - 1).fill_null(0.0).alias(f"r{i}"))
    expr = None
    for i, w in enumerate(ws):
        t = w * pl.col(f"r{i}")
        expr = t if expr is None else expr + t
    return j.with_columns(((1 + expr).cum_prod()).alias("nav")).select(["date", "nav"])

# 找三個 cell 的曲線 trial_id
t = ledger.all_trials()
import json
def find(batch, name):
    r = t.filter((pl.col("batch") == batch) & (pl.col("name") == name))
    return r["trial_id"][-1] if r.height else None

ids = {
    "R3_n5": "T0279",
    "n4_stack": find("R20", "r20d_n4_s40_f7"),
    "n5_rel": find("R23", "r23d_n5_rel05_f7"),
}
print("cells:", ids)
navs = [ledger.load_curve(v) for v in ids.values()]

# 相關性
rets = [metrics_daily := None]
rs = []
for nv in navs:
    v = nv.sort("date")["nav"].to_numpy()
    rs.append(v[1:] / v[:-1] - 1)
L = min(len(r) for r in rs)
M = np.corrcoef([r[-L:] for r in rs])
print("pick-level 曲線相關性:", np.round(M, 3).tolist())

nav_ens = blend(navs, [1/3, 1/3, 1/3])
s = metrics.perf_stats(nav_ens)
print(f"\n三書等權集成:CAGR {s['cagr']:+.1%} | Sharpe {s['sharpe']:.3f} | MDD {s['mdd']:+.1%}")
ok_c = s["sharpe"] >= 2.59 and s["cagr"] >= 1.127 and s["mdd"] >= -0.343
ok_a = s["cagr"] >= 1.147 and s["mdd"] >= -0.353 and s["sharpe"] >= 2.34
print(f"frontier (c):{'✅' if ok_c else '❌'} | frontier (a):{'✅' if ok_a else '❌'}(vs R3 113.7/2.44/−33.3)")
tid = ledger.log_trial(family="mod_line", name="r24_ensemble3", hypothesis="config 集成",
                       config={"cells": ids, "w": "equal"}, window="2019-01-02..2025-06-30",
                       metrics=s, batch="R24", curve=nav_ens)
print("logged:", tid)
