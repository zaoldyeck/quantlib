"""EV53 — MDD ≤ 20% 硬約束下的 Evergreen 引擎重優化(預註冊見 LEDGER.md EV53)。

使用者指令:Evergreen MDD 最高只能 20%,以此重新優化量化引擎。現任 live
(P5 選出)train MDD −44%。加開引擎既有但未用的風控槓桿(abs_stop 絕對停損
首次啟用 + 更緊 trail + 更多席位分散 + 更短輸家時間止損)。

Stage 1(部位層,本檔):abs_stop × trail × n_slots × lts × 選擇軸。
Stage 2(投組層斷路器,僅部位層 OOS 守不住 20% 才升級,見 ev53b)。

選擇紀律:train 選(MDD≤20% 硬約束 → survivor top-Martin 計 P5 → max P5),
OOS 只驗不選。Part A=三折 walk-forward;Part B=live 窗 refit + frontier。

Run: uv run --project research python -m research.evergreen.ev53_mdd_cap [--smoke]
依賴 cache: 是
"""
from __future__ import annotations

import itertools
import json
import sys
from datetime import date as Date

import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import C, kpis_full, seg_kpi
from research.evergreen.ev38_exhaust import FOLDS, bench
from research.evergreen.ev43_live_refit import LabL

FOLD0 = {"name": "折0", "t0": Date(2022, 7, 11), "t1": Date(2023, 7, 10),
         "o0": Date(2023, 7, 11), "o1": Date(2024, 7, 10)}
ALLFOLDS = [FOLD0] + FOLDS
LIVE_T0, LIVE_T1 = Date(2023, 7, 11), Date(2026, 7, 9)
MDD_CAP = 0.20
TOPN_P5 = 40
OUT = "research/evergreen/data/live_config_mdd20.json"

SMOKE = "--smoke" in sys.argv
GATES = ("none", "inst5")
SCORES = ("base", "xadv_inv")
PM = (2, 3)
TRAILS = (0.15, 0.20, 0.25, 0.30, 0.40)
SLOTS = (5, 8, 10, 12)
ABS = (None, 0.12, 0.15, 0.20)
LTS = (30, 45)
if SMOKE:  # 8 配置/窗,驗正確性 + 量單位耗時
    GATES, SCORES, PM = ("inst5",), ("xadv_inv",), (2,)
    TRAILS, SLOTS, ABS, LTS = (0.20, 0.40), (5, 10), (None, 0.15), (45,)

# 現任 live 基準(P5 選出,MDD −44%)
LIVE = dict(gate="inst5", score="xadv_inv", pool_months=2, h120=0.0,
            trail=0.40, lts=45, n_slots=5, max_new=1, abs_stop=None)
CFG_KEYS = ("gate", "score", "pool_months", "h120", "trail", "lts",
            "n_slots", "max_new", "abs_stop")


def _rk(c):
    return (pl.col(c).rank() / pl.len()).over("date")


def build_sc(lab, gate, score, pm, n_slots):
    """score 面板(與 trail/lts/abs 無關,可跨風控軸快取)。"""
    memb, pool_flag = lab.memb(pm)
    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > 0.0))
    if gate != "none":
        sc = sc.filter(pl.col(gate).fill_null(False))
    base = _rk("h52") * _rk("h120")
    expr = base if score == "base" else base * (1.0 - _rk("adv20"))
    sc = (sc.with_columns(expr.alias("score"))
          .with_columns(pl.lit(1.0 / n_slots).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    return sc, pool_flag


def nav_of(lab, sc, pf, start, end, *, trail, lts, n_slots, abs_stop):
    res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                   exit_flags=pf, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=n_slots, max_new_per_day=1),
                   exit_spec=ExitSpec(trailing_stop=trail, abs_stop=abs_stop,
                                      loser_time_stop=lts), start=start)
    return res.nav.sort("date").filter(
        (pl.col("date") >= start) & (pl.col("date") <= end))


def sweep(lab, t0, t1):
    """(t0,t1) 全網格 train seg_kpi;回 rows + sc 快取。"""
    sc_cache, rows = {}, []
    for gate, score, pm, ns in itertools.product(GATES, SCORES, PM, SLOTS):
        key = (gate, score, pm, ns)
        if key not in sc_cache:
            sc_cache[key] = build_sc(lab, gate, score, pm, ns)
        sc, pf = sc_cache[key]
        for trail, abss, lts in itertools.product(TRAILS, ABS, LTS):
            k = seg_kpi(nav_of(lab, sc, pf, t0, t1, trail=trail, lts=lts,
                               n_slots=ns, abs_stop=abss))
            rows.append(dict(gate=gate, score=score, pool_months=pm, h120=0.0,
                             trail=trail, lts=lts, n_slots=ns, max_new=1,
                             abs_stop=abss, tr_cagr=k["cagr"], tr_mdd=k["mdd"],
                             tr_martin=k["martin"]))
    return rows, sc_cache


def pick(rows, sc_cache, lab, t0, t1):
    """MDD≤20% survivor 取 top-Martin TOPN_P5 計 P5,回 max P5 者 + survivor 數。"""
    surv = sorted((r for r in rows if r["tr_mdd"] >= -MDD_CAP),
                  key=lambda r: -r["tr_martin"])
    best = None
    for r in surv[:TOPN_P5]:
        sc, pf = sc_cache[(r["gate"], r["score"], r["pool_months"], r["n_slots"])]
        p5 = kpis_full(nav_of(lab, sc, pf, t0, t1, trail=r["trail"], lts=r["lts"],
                              n_slots=r["n_slots"], abs_stop=r["abs_stop"]))["p5"]
        if best is None or p5 > best["tr_p5"]:
            best = {**r, "tr_p5": p5}
    return best, len(surv)


def cfg_of(r):
    return {k: r[k] for k in CFG_KEYS}


def eval_on(lab, cfg, t0, t1):
    sc, pf = build_sc(lab, cfg["gate"], cfg["score"], cfg["pool_months"],
                      cfg["n_slots"])
    return seg_kpi(nav_of(lab, sc, pf, t0, t1, trail=cfg["trail"], lts=cfg["lts"],
                          n_slots=cfg["n_slots"], abs_stop=cfg["abs_stop"]))


def tag_of(r):
    return (f"{r['gate']}/{r['score']}/pm{r['pool_months']}/tr{r['trail']}"
            f"/lts{r['lts']}/s{r['n_slots']}/abs{r['abs_stop']}")


def main() -> None:
    lab = LabL()
    ncfg = (len(GATES) * len(SCORES) * len(PM) * len(TRAILS) * len(SLOTS)
            * len(ABS) * len(LTS))
    print(f"EV53 MDD 帽子 {MDD_CAP:.0%};{ncfg} 配置/窗"
          f"{'(SMOKE)' if SMOKE else ''}")

    # ── Part A:walk-forward(選 train、驗 OOS)──
    print("\n########## Part A:walk-forward 驗證(選 train、驗 OOS)##########")
    for fold in ALLFOLDS:
        rows, scc = sweep(lab, fold["t0"], fold["t1"])
        best, nsurv = pick(rows, scc, lab, fold["t0"], fold["t1"])
        lk = eval_on(lab, LIVE, fold["t0"], fold["t1"])
        lko = eval_on(lab, LIVE, fold["o0"], fold["o1"])
        print(f"\n=== {fold['name']}(OOS {fold['o0']}~{fold['o1']})===")
        print(f"  現任 live 44%版:train MDD {lk['mdd']:6.1%} CAGR {lk['cagr']:7.0%}"
              f" | OOS MDD {lko['mdd']:6.1%} CAGR {lko['cagr']:7.0%}")
        if best is None:
            print("  ⚠ 部位層 0 配置 train MDD≤20% → 需 Stage 2 斷路器")
            continue
        ko = eval_on(lab, cfg_of(best), fold["o0"], fold["o1"])
        held = "✓" if ko["mdd"] >= -MDD_CAP else f"✗ OOS 破 {MDD_CAP:.0%}"
        print(f"  MDD≤20% survivor {nsurv};選 {tag_of(best)}")
        print(f"  → train MDD {best['tr_mdd']:6.1%} CAGR {best['tr_cagr']:7.0%}"
              f" P5 {best['tr_p5']:6.1%} | OOS MDD {ko['mdd']:6.1%}"
              f" CAGR {ko['cagr']:7.0%}  {held}")
        b = bench(fold)
        print("    對手 OOS:" + "".join(
            f" {nm} {v['cagr']:+.0%}/MDD{v['mdd']:.0%}" for nm, v in b.items() if v))

    # ── Part B:production refit(live 窗)+ frontier ──
    print("\n########## Part B:production refit(live 窗 select)##########")
    rows, scc = sweep(lab, LIVE_T0, LIVE_T1)
    lk = eval_on(lab, LIVE, LIVE_T0, LIVE_T1)
    print(f"現任 live 44%版:train MDD {lk['mdd']:6.1%} CAGR {lk['cagr']:7.0%}")
    print("\nfrontier(各 MDD 帽子下 max-Martin 的代價):")
    for cap in (0.15, 0.20, 0.25, 0.30, 0.40, 0.99):
        cand = [r for r in rows if r["tr_mdd"] >= -cap]
        if not cand:
            print(f"  帽子 {cap:.0%}:無配置")
            continue
        top = max(cand, key=lambda r: r["tr_martin"])
        print(f"  帽子 {cap:4.0%}:{tag_of(top):48s} MDD {top['tr_mdd']:6.1%}"
              f" CAGR {top['tr_cagr']:7.0%} Martin {top['tr_martin']:5.1f}")

    best, nsurv = pick(rows, scc, lab, LIVE_T0, LIVE_T1)
    if best is None:
        print("\n⚠ 部位層 live 窗無解 → Stage 2 斷路器")
        return
    print(f"\nMDD≤20% survivor {nsurv};新候選(max P5):")
    print(f"  {json.dumps(cfg_of(best), ensure_ascii=False)}")
    print(f"  train MDD {best['tr_mdd']:6.1%} CAGR {best['tr_cagr']:7.0%}"
          f" P5 {best['tr_p5']:6.1%}")
    print("新候選 walk-forward OOS 驗證(20% 帽子樣本外守不守得住):")
    all_held = True
    for fold in ALLFOLDS:
        ko = eval_on(lab, cfg_of(best), fold["o0"], fold["o1"])
        ok = ko["mdd"] >= -MDD_CAP
        all_held &= ok
        print(f"  {fold['name']} OOS: MDD {ko['mdd']:6.1%} CAGR {ko['cagr']:7.0%}"
              f"  {'✓' if ok else '✗ 破帽'}")

    doc = {"refit_date": "2026-07-20", "train_window": [str(LIVE_T0), str(LIVE_T1)],
           "config": cfg_of(best), "mdd_cap": MDD_CAP,
           "train_kpi": {"cagr": best["tr_cagr"], "mdd": best["tr_mdd"],
                         "p5": best["tr_p5"]},
           "oos_all_held": bool(all_held),
           "selection_metric": "MDD≤20% 硬約束下 max P5(EV53)",
           "note": "使用者指令 MDD≤20% 重優化;abs_stop 首次啟用。"
                   "未覆蓋 live_config.json——待使用者裁決報酬代價。"}
    json.dump(doc, open(OUT, "w"), ensure_ascii=False, indent=1)
    print(f"\n候選已存 {OUT}(未動 live_config.json;OOS 全守={all_held})")


if __name__ == "__main__":
    main()
