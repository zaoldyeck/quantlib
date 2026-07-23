"""EV53c — 波動率目標(vol-targeting)為 Evergreen 套 MDD ≤ 20% 硬帽。

EV53b 回撤斷路器證實:反應式(跌了才砍)在深崩窗殺在谷底、錯過 V 反彈,
報酬代價慘重。vol-targeting 是預應式:崩盤前波動率先飆 → 提前降曝險,且
不歸零、不殺在谷底。曝險 = min(cap, σ_target / σ_realized(過去));σ_realized
以基座 NAV 日報酬 rolling std 因果估計(僅用過去)。cap=1.0(不加槓桿,
維持長多不槓桿性質);曝險 <1 時餘額為現金(使用者已確認允許空倉)。

選擇紀律同 EV53:train 選(MDD≤20% + max P5),OOS 只驗不選。
Part A=三折 walk-forward;Part B=live 窗 refit 產候選。

Run: uv run --project . python -m quantlib.evergreen.ev53c_voltarget [--smoke]
依賴 cache: 是
"""
from __future__ import annotations

import itertools
import json
import sys

import numpy as np
import pandas as pd
import polars as pl

from quantlib.evergreen.ev36_walkforward import kpis_full, seg_kpi
from quantlib.evergreen.ev38_exhaust import bench
from quantlib.evergreen.ev43_live_refit import LabL
from quantlib.evergreen.ev53_mdd_cap import (ALLFOLDS, LIVE, LIVE_T0, LIVE_T1,
                                             MDD_CAP, TOPN_P5, build_sc, nav_of)

BASES = {
    "live": LIVE,
    "div": dict(gate="inst5", score="xadv_inv", pool_months=2, h120=0.0,
                trail=0.25, lts=30, n_slots=10, max_new=1, abs_stop=0.15),
}
SIGMA = (0.10, 0.12, 0.15, 0.20, 0.25, 0.30)   # 年化目標波動
LOOKBACK = (20, 40)                # 已實現波動估計窗(交易日)
CAP = 1.0                          # 曝險上限(不加槓桿)
VT_COST = 0.001                    # 曝險調整單邊摩擦

SMOKE = "--smoke" in sys.argv
if SMOKE:
    BASES = {"live": LIVE}
    SIGMA, LOOKBACK = (0.20, 0.30), (20,)


def vt_overlay(nav: pl.DataFrame, sigma_tgt: float, lookback: int) -> pl.DataFrame:
    """預應式波動率目標:今日曝險 = min(cap, σ_tgt_日 / σ_實現(過去 lookback))。
    σ_實現以『昨日為止』的基座日報酬估計(因果、無前視)。"""
    v = nav["nav"].to_numpy()
    r = np.concatenate([[0.0], v[1:] / v[:-1] - 1])
    # 因果 rolling std(shift 1:今日曝險只能用到昨日為止的報酬)
    rv = (pd.Series(r).rolling(lookback, min_periods=5).std(ddof=1)
          .shift(1).to_numpy())
    daily_tgt = sigma_tgt / np.sqrt(252)
    exp = np.where(np.isnan(rv), 1.0, np.minimum(CAP, daily_tgt / np.maximum(rv, 1e-6)))
    eq = np.empty_like(v)
    eq[0] = 1.0
    prev = 1.0
    for i in range(1, len(v)):
        e = exp[i]
        eq[i] = eq[i - 1] * (1.0 + e * r[i] - VT_COST * abs(e - prev))
        prev = e
    return pl.DataFrame({"date": nav["date"], "nav": eq})


def base_nav(lab, cfg, t0, t1):
    sc, pf = build_sc(lab, cfg["gate"], cfg["score"], cfg["pool_months"],
                      cfg["n_slots"])
    return nav_of(lab, sc, pf, t0, t1, trail=cfg["trail"], lts=cfg["lts"],
                  n_slots=cfg["n_slots"], abs_stop=cfg["abs_stop"])


def select_vt(lab, t0, t1):
    cand, raw = [], {}
    for bname, base in BASES.items():
        tnav = base_nav(lab, base, t0, t1)
        raw[bname] = seg_kpi(tnav)
        for sig, lb in itertools.product(SIGMA, LOOKBACK):
            k = seg_kpi(vt_overlay(tnav, sig, lb))
            if k["mdd"] >= -MDD_CAP:
                cand.append(dict(base=bname, sigma=sig, lookback=lb,
                                 cagr=k["cagr"], mdd=k["mdd"], martin=k["martin"]))
    cand.sort(key=lambda c: -c["martin"])
    best = None
    for c in cand[:TOPN_P5]:
        tnav = base_nav(lab, BASES[c["base"]], t0, t1)
        p5 = kpis_full(vt_overlay(tnav, c["sigma"], c["lookback"]))["p5"]
        if best is None or p5 > best["p5"]:
            best = {**c, "p5": p5}
    return best, len(cand), raw


def eval_vt_oos(lab, c, o0, o1):
    onav = base_nav(lab, BASES[c["base"]], o0, o1)
    return seg_kpi(onav), seg_kpi(vt_overlay(onav, c["sigma"], c["lookback"]))


def tag(c):
    return f"{c['base']}+VT(σ{c['sigma']:.0%}/lb{c['lookback']})"


def main() -> None:
    lab = LabL()
    print(f"EV53c 波動率目標;帽子 {MDD_CAP:.0%};基座 {list(BASES)};"
          f"VT {len(SIGMA) * len(LOOKBACK)} 組{'(SMOKE)' if SMOKE else ''}")

    print("\n########## Part A:walk-forward(選 train、驗 OOS)##########")
    for fold in ALLFOLDS:
        best, ncand, raw = select_vt(lab, fold["t0"], fold["t1"])
        print(f"\n=== {fold['name']}(OOS {fold['o0']}~{fold['o1']})===")
        for bn, rk in raw.items():
            print(f"  基座 {bn} 裸 train:MDD {rk['mdd']:6.1%} CAGR {rk['cagr']:7.0%}")
        if best is None:
            print("  ⚠ 連 vol-target 都無 MDD≤20% + 正報酬")
            continue
        rawo, ko = eval_vt_oos(lab, best, fold["o0"], fold["o1"])
        held = "✓" if ko["mdd"] >= -MDD_CAP else f"✗ 破 {MDD_CAP:.0%}"
        print(f"  達標 {ncand};選 {tag(best)}")
        print(f"  → train MDD {best['mdd']:6.1%} CAGR {best['cagr']:7.0%}"
              f" P5 {best['p5']:6.1%} | OOS 裸MDD {rawo['mdd']:6.1%}"
              f"→VT {ko['mdd']:6.1%} CAGR {ko['cagr']:7.0%}  {held}")

    print("\n########## Part B:production refit(live 窗)##########")
    best, ncand, raw = select_vt(lab, LIVE_T0, LIVE_T1)
    for bn, rk in raw.items():
        print(f"  基座 {bn} 裸 train:MDD {rk['mdd']:6.1%} CAGR {rk['cagr']:7.0%}")
    if best is None:
        print("⚠ live 窗連 vol-target 都無解")
        return
    print(f"\nMDD≤20% 達標 {ncand};最佳(max P5):{tag(best)}")
    print(f"  train MDD {best['mdd']:6.1%} CAGR {best['cagr']:7.0%} P5 {best['p5']:6.1%}")
    print("walk-forward OOS 驗證(20% 帽子樣本外守不守得住):")
    all_held = True
    for fold in ALLFOLDS:
        rawo, ko = eval_vt_oos(lab, best, fold["o0"], fold["o1"])
        ok = ko["mdd"] >= -MDD_CAP
        all_held &= ok
        b = bench(fold)
        print(f"  {fold['name']} OOS: 裸MDD {rawo['mdd']:6.1%}→VT {ko['mdd']:6.1%}"
              f" CAGR {rawo['cagr']:7.0%}→{ko['cagr']:7.0%} {'✓' if ok else '✗破帽'}"
              + "".join(f" | {nm} {v['cagr']:+.0%}" for nm, v in b.items() if v))

    cfg = {**BASES[best["base"]],
           "vol_target": {"sigma_annual": best["sigma"], "lookback": best["lookback"],
                          "cap": CAP, "switch_cost": VT_COST}}
    doc = {"refit_date": "2026-07-20", "base": best["base"], "config": cfg,
           "mdd_cap": MDD_CAP,
           "train_kpi": {"cagr": best["cagr"], "mdd": best["mdd"], "p5": best["p5"]},
           "oos_all_held": bool(all_held),
           "selection_metric": "MDD≤20% 硬約束下 max P5(EV53c 波動率目標)",
           "note": "預應式 vol-targeting overlay(因果無前視);未覆蓋 "
                   "live_config.json——待使用者裁決報酬代價。"}
    json.dump(doc, open("src/quantlib/evergreen/data/live_config_mdd20_vt.json", "w"),
              ensure_ascii=False, indent=1)
    print(f"\n候選存 live_config_mdd20_vt.json;OOS 全守={all_held}")


if __name__ == "__main__":
    main()
