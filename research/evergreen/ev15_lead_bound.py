"""EV15 — 日頻化理論上界:registry_v1 池籍窗提前 N 交易日。

作弊上界(標記資訊當時不存在):量「完美日頻標記」的天花板,回答
「月改日能否補上對 Serenity 的 24pp 差距」。預註冊見 LEDGER EV15。
判準:lead=15 train CAGR 增量 <15pp → 日頻化維持低優先;≥30pp → 重估。

需要 cache 最新。Run: uv run --project research python -m research.evergreen.ev15_lead_bound
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.experiments.g01_ml_ranker import kpi
from research.evergreen.harvest import build_feats, harvest, monthly_membership

TRAIN_END = Date(2025, 6, 30)


def main() -> None:
    reg = pl.read_parquet("research/evergreen/data/registry_v1.parquet")
    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=300))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    feats = build_feats(panel)

    base_cagr = None
    for lead in [0, 5, 10, 15]:
        memb = monthly_membership(reg, dates_all, Date(2022, 7, 1),
                                  lead_tdays=lead)
        res = harvest(panel, feats, memb, Date(2022, 7, 1))
        nav = res.nav.sort("date")
        tr = kpi(nav.filter(pl.col("date") <= TRAIN_END))
        oos = nav.filter(pl.col("date") > TRAIN_END)
        oos_ret = oos["nav"][-1] / oos["nav"][0] - 1
        if lead == 0:
            base_cagr = tr["cagr"]
        print(f"lead {lead:2d} 交易日:train CAGR {tr['cagr']:7.1%}"
              f"(Δ {tr['cagr'] - base_cagr:+.1%})  P5 {tr['p5']:.1%}  "
              f"MDD {tr['mdd']:.1%}  OOS {oos_ret:+.1%}")


if __name__ == "__main__":
    main()
