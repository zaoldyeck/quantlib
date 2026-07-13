"""EV24 Phase 2 — registry_v2 × 凍結 v3 引擎端到端 vs v1 基準。

v1 基準:train 115.4% / P5 48.8 / MDD −29.9 / OOS +428.9;
Serenity 同窗(2025-01-02~2026-07-03)263.2% / −22.4。

Run: uv run --project research python -m research.evergreen.ev24_baseline
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.experiments.g01_ml_ranker import kpi
from research.evergreen.harvest import build_feats, harvest, monthly_membership


def main() -> None:
    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=300))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    feats = build_feats(panel)
    for name, path in [("v1", "research/evergreen/data/registry_v1.parquet"),
                       ("v2", "research/evergreen/data/registry_v2.parquet")]:
        reg = pl.read_parquet(path)
        memb = monthly_membership(reg, dates_all, Date(2022, 7, 1))
        res = harvest(panel, feats, memb, Date(2022, 7, 1))
        nav = res.nav.sort("date")
        tr = kpi(nav.filter(pl.col("date") <= Date(2025, 6, 30)))
        oos = nav.filter(pl.col("date") > Date(2025, 6, 30))
        sw = nav.filter((pl.col("date") >= Date(2025, 1, 2))
                        & (pl.col("date") <= Date(2026, 7, 3)))
        yrs = (sw["date"][-1] - sw["date"][0]).days / 365.25
        scagr = (sw["nav"][-1] / sw["nav"][0]) ** (1 / yrs) - 1
        smdd = (sw["nav"] / sw["nav"].cum_max() - 1).min()
        print(f"{name}({reg.height} 筆):train CAGR {tr['cagr']:7.1%}  "
              f"P5 {tr['p5']:.1%}  MDD {tr['mdd']:.1%}  "
              f"OOS {oos['nav'][-1]/oos['nav'][0]-1:+.1%}  "
              f"Serenity同窗 {scagr:.1%}/{smdd:.1%}")


if __name__ == "__main__":
    main()
