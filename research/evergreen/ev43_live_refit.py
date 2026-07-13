"""EV43 — live refit:最近 3 年 train × 聯集凍結網格 → 上場參數。

網格 = 全 campaign 存活軸的聯集(EV38 有效軸 + EV42 的 xadv_inv;
被兩折證明過擬/無效的軸一律不入):
  gate{none,f5,inst5,any_confirm} × score{base,xadv_inv} × pm{2,3}
  × h120{0,.6} × trail{.30,.40} × lts{30,45} × slots{5,6} × mn{1,2}
train = 2023-07-11 ~ 2026-07-09(m01 結論:3 年窗)。
top-1(train P5/tie CAGR;EV44 量尺裁決:P5 三折幾何均 261.9% 最高,
回歸 apex KPI v3 定案主尺)→ data/live_config.json。

每年(或每半年)重跑本腳本即為滾動 refit。網格不准臨場加軸——
變更網格須經兩折 walk-forward 重新驗證(LEDGER 紀律)。

Run: uv run --project research python -m research.evergreen.ev43_live_refit
依賴 cache: 是(需最新)
"""
from __future__ import annotations

import itertools
import json
from datetime import date as Date

import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import C, kpis_full, seg_kpi
from research.evergreen.ev38_exhaust import LabX

LIVE_T0, LIVE_T1 = Date(2023, 7, 11), Date(2026, 7, 9)
OUT = "research/evergreen/data/live_config.json"

GATES = ("none", "f5", "inst5", "any_confirm")
SCORES = ("base", "xadv_inv")


class LabL(LabX):
    def __init__(self):
        super().__init__()
        adv = (self.panel.sort([C, "date"])
               .with_columns(pl.col("trade_value").cast(pl.Float64)
                             .rolling_median(20).over(C).alias("adv20"))
               .select(["date", C, "adv20"]))
        self.trig = self.trig.join(adv, on=["date", C], how="left").with_columns(
            pl.col("adv20").fill_null(1e12))


def run_live(lab: LabL, *, gate, score, pool_months, h120, trail, lts,
             n_slots, max_new, nav_out=False):
    memb, pool_flag = lab.memb(pool_months)

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .join(lab.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120))
    if gate != "none":
        sc = sc.filter(pl.col(gate).fill_null(False))
    base = rank("h52") * rank("h120")
    expr = base if score == "base" else base * (1.0 - rank("adv20"))
    sc = (sc.with_columns(expr.alias("score"))
          .with_columns(pl.lit(1.0 / n_slots).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    res = simulate(lab.panel.filter(pl.col("date") <= LIVE_T1), sc,
                   exit_flags=pool_flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new),
                   exit_spec=ExitSpec(trailing_stop=trail, loser_time_stop=lts),
                   start=LIVE_T0)
    nav = res.nav.sort("date").filter(
        (pl.col("date") >= LIVE_T0) & (pl.col("date") <= LIVE_T1))
    return (kpis_full(nav), nav) if nav_out else (kpis_full(nav), None)


def main() -> None:
    lab = LabL()
    grid = list(itertools.product(
        GATES, SCORES, (2, 3), (0.0, 0.6), (0.30, 0.40), (30, 45),
        (5, 6), (1, 2)))
    rows = []
    for g, sm, pm, h1, tr, lt, ns, mn in grid:
        cfg = dict(gate=g, score=sm, pool_months=pm, h120=h1, trail=tr,
                   lts=lt, n_slots=ns, max_new=mn)
        k, _ = run_live(lab, **cfg)
        rows.append({**cfg, **{f"tr_{x}": v for x, v in k.items()}})
    df = pl.DataFrame(rows).sort(["tr_p5", "tr_cagr"], descending=True)
    top = df.head(1).to_dicts()[0]
    cfg = {k: top[k] for k in ("gate", "score", "pool_months", "h120",
                               "trail", "lts", "n_slots", "max_new")}
    print("live refit top-3(train 2023-07-11~2026-07-09):")
    with pl.Config(tbl_cols=-1, tbl_width_chars=180):
        print(df.head(3))
    doc = {
        "refit_date": "2026-07-13",
        "train_window": [str(LIVE_T0), str(LIVE_T1)],
        "config": cfg,
        "train_kpi": {k: top[f"tr_{k}"] for k in ("cagr", "mdd", "martin", "p5")},
        "selection_metric": "p5(EV44 裁決,KPI v3 主尺)",
        "validation": "EV40 borderline(perm p=0.000 / PBO 0.30 ✓;"
                      "bootstrap 下界 / DSR ✗)——倉位上限 25% NAV(EV41 配置)",
        "next_refit_due": "2027-01(半年)或 2027-07(一年)",
        "grid_frozen": "EV38∪EV42 存活軸;變更須重過兩折 walk-forward",
    }
    json.dump(doc, open(OUT, "w"), ensure_ascii=False, indent=1)
    print(f"\n✓ live 參數已存 {OUT}")
    print(json.dumps(doc["config"], ensure_ascii=False))


if __name__ == "__main__":
    main()
