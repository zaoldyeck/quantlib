"""EV33 — 出場成熟度最終掃(新基座 pm3+h120.6+h52-only,零 token)。

假說:lts30 誤殺醞釀型(EV27:醞釀期常數週~數月;v3 標記 88% 醞釀型)。
掃:lts 長刻度、abs_stop(論點失效代理)、trail 重驗、h52 回看窗。
Run: uv run --project research python -m research.evergreen.ev33_exit_maturity
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev30_baseline import midmonth_membership
from research.evergreen.ev30_campaign import SW0, SW1, Lab3
from research.evergreen.harvest import C


def run(lab, *, lts=30, abs_stop=None, trail=0.35, h52_lb=252):
    memb = midmonth_membership(lab.reg, lab.dates_all, 3)
    feats = (lab.panel.sort([C, "date"])
             .with_columns([
                 (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
                 (pl.col("close") / pl.col("close").rolling_max(h52_lb)).over(C).alias("h52"),
             ]).select(["date", C, "h120", "h52"]))

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(feats, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > 0.6)
          .with_columns(rank("h52").alias("score"))
          .with_columns(pl.lit(0.2).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    days = [d for d in lab.dates_all if d >= Date(2024, 10, 14)]
    flag = (pl.DataFrame({"date": days})
            .join(pl.DataFrame({C: memb[C].unique().to_list()}), how="cross")
            .join(memb.select(["date", C]), on=["date", C], how="anti")
            .sort(["date", C]))
    res = simulate(lab.panel, sc, exit_flags=flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=trail, loser_time_stop=lts,
                                      abs_stop=abs_stop),
                   start=Date(2024, 10, 14))
    nav = res.nav.sort("date")
    sw = nav.filter((pl.col("date") >= SW0) & (pl.col("date") <= SW1))
    yrs = (sw["date"][-1] - sw["date"][0]).days / 365.25
    cagr = (sw["nav"][-1] / sw["nav"][0]) ** (1 / yrs) - 1
    dd = sw["nav"] / sw["nav"].cum_max() - 1
    mdd = dd.min()
    martin = cagr / max(float(np.sqrt((dd.to_numpy() ** 2).mean())), 1e-9)
    q = (sw.group_by(pl.col("date").dt.strftime("%Y-Q%q").alias("q"))
         .agg((pl.col("nav").last() / pl.col("nav").first() - 1).alias("r")).sort("q"))
    return {"cagr": cagr, "mdd": mdd, "martin": martin, "tr": res.trades.height,
            "q": {r["q"]: round(r["r"], 2) for r in q.to_dicts()}}


def main() -> None:
    lab = Lab3()
    grid = [
        ("基準 v3.2(lts30)", {}),
        ("lts45", {"lts": 45}),
        ("lts60", {"lts": 60}),
        ("lts90", {"lts": 90}),
        ("ltsNone", {"lts": None}),
        ("ltsNone+abs20", {"lts": None, "abs_stop": 0.20}),
        ("ltsNone+abs25", {"lts": None, "abs_stop": 0.25}),
        ("lts60+abs20", {"lts": 60, "abs_stop": 0.20}),
        ("trail30(新基座)", {"trail": 0.30}),
        ("trail40(新基座)", {"trail": 0.40}),
        ("h52 lb126", {"h52_lb": 126}),
        ("h52 lb180", {"h52_lb": 180}),
    ]
    for name, kw in grid:
        k = run(lab, **kw)
        print(f"{name:16s}:CAGR {k['cagr']:7.1%}  MDD {k['mdd']:6.1%}  "
              f"Martin {k['martin']:5.2f}  tr {k['tr']}")
        if name in ("基準 v3.2(lts30)", "lts60", "ltsNone", "ltsNone+abs20"):
            print(f"    逐季 {k['q']}")


if __name__ == "__main__":
    main()
