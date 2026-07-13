"""EV44 — 選擇量尺敏感度:CAGR / Sortino / Martin / P5 各自選型的 OOS 後果。

背景:EV36 起選擇量尺用 Martin 單尺;apex KPI v3 定案主尺是 bootstrap
P5 CAGR(+Martin 並列)。本輪在 EV43 聯集網格 × 三折上,讓四種量尺
各自在 train 選 top-1,對比 OOS——量尺不敏感則現制無虞;敏感則改回
KPI v3(P5 主尺)並重產 live 參數。

Run: uv run --project research python -m research.evergreen.ev44_metric_sensitivity
依賴 cache: 是
"""
from __future__ import annotations

import itertools
from datetime import date as Date

import numpy as np
import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import C, kpis_full, seg_kpi
from research.evergreen.ev38_exhaust import FOLDS, bench
from research.evergreen.ev43_live_refit import GATES, SCORES, LabL

FOLD0 = {"name": "折0", "t0": Date(2022, 7, 11), "t1": Date(2023, 7, 10),
         "o0": Date(2023, 7, 11), "o1": Date(2024, 7, 10)}
ALLFOLDS = [FOLD0] + FOLDS
METRICS = ("cagr", "sortino", "martin", "p5")


def run_fold(lab: LabL, fold: dict, cfg: dict, want_oos=False):
    memb, pool_flag = lab.memb(cfg["pool_months"])

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > cfg["h120"]))
    if cfg["gate"] != "none":
        sc = sc.filter(pl.col(cfg["gate"]).fill_null(False))
    base = rank("h52") * rank("h120")
    expr = base if cfg["score"] == "base" else base * (1.0 - rank("adv20"))
    sc = (sc.with_columns(expr.alias("score"))
          .with_columns(pl.lit(1.0 / cfg["n_slots"]).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))

    def one(start, end):
        res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                       exit_flags=pool_flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=cfg["n_slots"],
                                          max_new_per_day=cfg["max_new"]),
                       exit_spec=ExitSpec(trailing_stop=cfg["trail"],
                                          loser_time_stop=cfg["lts"]),
                       start=start)
        return res.nav.sort("date").filter(
            (pl.col("date") >= start) & (pl.col("date") <= end))

    out = {"train": kpis_full(one(fold["t0"], fold["t1"]))}
    if want_oos:
        out["oos"] = seg_kpi(one(fold["o0"], fold["o1"]))
    return out


def main() -> None:
    lab = LabL()
    grid = [dict(gate=g, score=sm, pool_months=pm, h120=h1, trail=tr,
                 lts=lt, n_slots=ns, max_new=mn)
            for g, sm, pm, h1, tr, lt, ns, mn in itertools.product(
                GATES, SCORES, (2, 3), (0.0, 0.6), (0.30, 0.40), (30, 45),
                (5, 6), (1, 2))]
    for fold in ALLFOLDS:
        rows = []
        for cfg in grid:
            k = run_fold(lab, fold, cfg)["train"]
            rows.append({**cfg, **{f"tr_{x}": v for x, v in k.items()}})
        df = pl.DataFrame(rows)
        print(f"\n=== {fold['name']}(OOS {fold['o0']}~{fold['o1']})===")
        b = bench(fold)
        for metric in METRICS:
            top = (df.sort([f"tr_{metric}", "tr_cagr"], descending=True)
                   .head(1).to_dicts()[0])
            cfg = {k: top[k] for k in ("gate", "score", "pool_months", "h120",
                                       "trail", "lts", "n_slots", "max_new")}
            out = run_fold(lab, fold, cfg, want_oos=True)
            o = out["oos"]
            tag = (f"{cfg['gate']}/{cfg['score']}/pm{cfg['pool_months']}"
                   f"/h{cfg['h120']}/t{cfg['trail']}/l{cfg['lts']}"
                   f"/s{cfg['n_slots']}/m{cfg['max_new']}")
            print(f"  量尺 {metric:8s} 選 {tag:44s} → OOS CAGR {o['cagr']:7.1%} "
                  f"MDD {o['mdd']:6.1%} Martin {o['martin']:5.1f}")
        line = "  對手:"
        for nm, k in b.items():
            if k:
                line += f" {nm} {k['cagr']:+.1%}"
        print(line)


if __name__ == "__main__":
    main()
