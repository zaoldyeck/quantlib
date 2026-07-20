"""EV53b — 投組層斷路器(回撤觸發減碼到現金)為 Evergreen 套 MDD ≤ 20% 硬帽。

EV53 部位層(單檔 abs_stop/trail)證實:樣本外相關性崩盤(2024-08 日圓套利、
2025-04 關稅)守不住 20%——單檔停損擋不住組合層系統性回撤。斷路器是機械式
封頂:以策略『自身』回撤觸發整體降曝險到現金(因果、無前視),回撤修復再復原。
使用者已確認允許空倉。斷路器套在基座配置之上,故較單檔緊停損更不傷贏家。

選擇紀律同 EV53:train 選(MDD≤20% 硬約束 → top-Martin 計 P5 → max P5),
OOS 只驗不選。Part A=三折 walk-forward;Part B=live 窗 refit 產候選。

Run: uv run --project research python -m research.evergreen.ev53b_circuit_breaker [--smoke]
依賴 cache: 是
"""
from __future__ import annotations

import itertools
import json
import sys

import numpy as np
import polars as pl

from research.evergreen.ev36_walkforward import kpis_full, seg_kpi
from research.evergreen.ev38_exhaust import bench
from research.evergreen.ev43_live_refit import LabL
from research.evergreen.ev53_mdd_cap import (ALLFOLDS, LIVE, LIVE_T0, LIVE_T1,
                                             MDD_CAP, TOPN_P5, build_sc, nav_of)

# 基座(斷路器套其上):現任高火力 live + 一個溫和分散版(部位層先降一級風險)
BASES = {
    "live": LIVE,
    "div": dict(gate="inst5", score="xadv_inv", pool_months=2, h120=0.0,
                trail=0.25, lts=30, n_slots=10, max_new=1, abs_stop=0.15),
}
D_OFF = (0.10, 0.12, 0.15)   # 回撤達此 → 減碼
E_LOW = (0.0, 0.3, 0.5)      # 減碼後曝險
D_ON = (0.05, 0.08)          # 回撤修復至此 → 復原(遲滯)
CB_COST = 0.001              # 曝險切換單邊摩擦(進出現金成本,不美化)

SMOKE = "--smoke" in sys.argv
if SMOKE:
    BASES = {"live": LIVE}
    D_OFF, E_LOW, D_ON = (0.12,), (0.0, 0.3), (0.06,)


def cb_overlay(nav: pl.DataFrame, d_off, e_low, d_on) -> pl.DataFrame:
    """因果斷路器:今日曝險由『昨日為止』策略自身回撤決定。回撤 ≤ −d_off →
    曝險 e_low;回撤 ≥ −d_on → 復原 1.0;之間維持(遲滯)。"""
    v = nav["nav"].to_numpy()
    r = np.concatenate([[0.0], v[1:] / v[:-1] - 1])
    eq = np.empty_like(v)
    eq[0] = 1.0
    peak = exp = prev = 1.0
    for i in range(1, len(v)):
        dd = eq[i - 1] / peak - 1.0
        if dd <= -d_off:
            exp = e_low
        elif dd >= -d_on:
            exp = 1.0
        eq[i] = eq[i - 1] * (1.0 + exp * r[i] - CB_COST * abs(exp - prev))
        peak = max(peak, eq[i])
        prev = exp
    return pl.DataFrame({"date": nav["date"], "nav": eq})


def base_nav(lab, cfg, t0, t1):
    sc, pf = build_sc(lab, cfg["gate"], cfg["score"], cfg["pool_months"],
                      cfg["n_slots"])
    return nav_of(lab, sc, pf, t0, t1, trail=cfg["trail"], lts=cfg["lts"],
                  n_slots=cfg["n_slots"], abs_stop=cfg["abs_stop"])


def select_cb(lab, t0, t1):
    """(t0,t1) 上每基座裸 NAV 一次算好,掃 CB 網格;MDD≤20% survivor
    取 top-Martin TOPN_P5 計 P5 → max P5。回 (best, n_surv, raw_by_base)。"""
    cand, raw = [], {}
    for bname, base in BASES.items():
        tnav = base_nav(lab, base, t0, t1)
        raw[bname] = seg_kpi(tnav)
        for d_off, e_low, d_on in itertools.product(D_OFF, E_LOW, D_ON):
            k = seg_kpi(cb_overlay(tnav, d_off, e_low, d_on))
            if k["mdd"] >= -MDD_CAP:
                cand.append(dict(base=bname, d_off=d_off, e_low=e_low, d_on=d_on,
                                 cagr=k["cagr"], mdd=k["mdd"], martin=k["martin"]))
    cand.sort(key=lambda c: -c["martin"])
    best = None
    for c in cand[:TOPN_P5]:
        tnav = base_nav(lab, BASES[c["base"]], t0, t1)
        p5 = kpis_full(cb_overlay(tnav, c["d_off"], c["e_low"], c["d_on"]))["p5"]
        if best is None or p5 > best["p5"]:
            best = {**c, "p5": p5}
    return best, len(cand), raw


def eval_cb_oos(lab, c, o0, o1):
    onav = base_nav(lab, BASES[c["base"]], o0, o1)
    return seg_kpi(onav), seg_kpi(cb_overlay(onav, c["d_off"], c["e_low"], c["d_on"]))


def tag(c):
    return f"{c['base']}+CB(off{c['d_off']:.0%}/low{c['e_low']:.0%}/on{c['d_on']:.0%})"


def main() -> None:
    lab = LabL()
    print(f"EV53b 斷路器;帽子 {MDD_CAP:.0%};基座 {list(BASES)};"
          f"CB {len(D_OFF) * len(E_LOW) * len(D_ON)} 組{'(SMOKE)' if SMOKE else ''}")

    # ── Part A:walk-forward(選 train、驗 OOS)──
    print("\n########## Part A:walk-forward(選 train、驗 OOS)##########")
    for fold in ALLFOLDS:
        best, ncand, raw = select_cb(lab, fold["t0"], fold["t1"])
        print(f"\n=== {fold['name']}(OOS {fold['o0']}~{fold['o1']})===")
        for bn, rk in raw.items():
            print(f"  基座 {bn} 裸 train:MDD {rk['mdd']:6.1%} CAGR {rk['cagr']:7.0%}")
        if best is None:
            print("  ⚠ 連斷路器都無 MDD≤20% + 正報酬")
            continue
        rawo, ko = eval_cb_oos(lab, best, fold["o0"], fold["o1"])
        held = "✓" if ko["mdd"] >= -MDD_CAP else f"✗ 破 {MDD_CAP:.0%}"
        print(f"  達標 {ncand};選 {tag(best)}")
        print(f"  → train MDD {best['mdd']:6.1%} CAGR {best['cagr']:7.0%}"
              f" P5 {best['p5']:6.1%} | OOS 裸MDD {rawo['mdd']:6.1%}"
              f"→CB {ko['mdd']:6.1%} CAGR {ko['cagr']:7.0%}  {held}")

    # ── Part B:production refit(live 窗)──
    print("\n########## Part B:production refit(live 窗)##########")
    best, ncand, raw = select_cb(lab, LIVE_T0, LIVE_T1)
    for bn, rk in raw.items():
        print(f"  基座 {bn} 裸 train:MDD {rk['mdd']:6.1%} CAGR {rk['cagr']:7.0%}")
    if best is None:
        print("⚠ live 窗連斷路器都無解")
        return
    print(f"\nMDD≤20% 達標 {ncand};最佳(max P5):{tag(best)}")
    print(f"  train MDD {best['mdd']:6.1%} CAGR {best['cagr']:7.0%} P5 {best['p5']:6.1%}")
    print("walk-forward OOS 驗證(20% 帽子樣本外守不守得住):")
    all_held = True
    for fold in ALLFOLDS:
        rawo, ko = eval_cb_oos(lab, best, fold["o0"], fold["o1"])
        ok = ko["mdd"] >= -MDD_CAP
        all_held &= ok
        b = bench(fold)
        print(f"  {fold['name']} OOS: 裸MDD {rawo['mdd']:6.1%}→CB {ko['mdd']:6.1%}"
              f" CAGR {rawo['cagr']:7.0%}→{ko['cagr']:7.0%} {'✓' if ok else '✗破帽'}"
              + "".join(f" | {nm} {v['cagr']:+.0%}" for nm, v in b.items() if v))

    cfg = {**BASES[best["base"]],
           "circuit_breaker": {"dd_off": best["d_off"], "exp_low": best["e_low"],
                               "dd_on": best["d_on"], "switch_cost": CB_COST}}
    doc = {"refit_date": "2026-07-20", "base": best["base"], "config": cfg,
           "mdd_cap": MDD_CAP,
           "train_kpi": {"cagr": best["cagr"], "mdd": best["mdd"], "p5": best["p5"]},
           "oos_all_held": bool(all_held),
           "selection_metric": "MDD≤20% 硬約束下 max P5(EV53b 投組斷路器)",
           "note": "投組層回撤斷路器 overlay(因果無前視);未覆蓋 "
                   "live_config.json——待使用者裁決報酬代價與是否採斷路器。"}
    json.dump(doc, open("research/evergreen/data/live_config_mdd20_cb.json", "w"),
              ensure_ascii=False, indent=1)
    print(f"\n候選存 live_config_mdd20_cb.json;OOS 全守={all_held}")


if __name__ == "__main__":
    main()
