"""EV32 — v3 引擎極限第二輪:池籍語義/出場族/入場權重(零 token)。

基準 = midmonth(4 站位月)+ h52-only = 216.2/−36.7/Martin 23.4。
Run: uv run --project research python -m research.evergreen.ev32_campaign
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev30_baseline import midmonth_membership
from research.evergreen.ev30_campaign import SW0, SW1, Lab3
from research.evergreen.harvest import C


def run(lab: Lab3, *, pool_months=4, h120=0.7, h52_gate=None, don60=False,
        weight="equal", n_slots=5, max_new=2, trail=0.35, lts=30,
        time_stop=None, uw_trail=None, recycle=None, min_hold=1):
    memb = midmonth_membership(lab.reg, lab.dates_all, pool_months)

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    feats = lab.feats
    if don60:
        feats = (lab.panel.sort([C, "date"])
                 .with_columns([
                     (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
                     (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
                     (pl.col("close") / pl.col("close").shift(1).rolling_max(60) - 1)
                     .over(C).alias("don60"),
                 ]).select(["date", C, "h120", "h52", "don60"]))
    sc = (memb.join(feats, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120))
    if h52_gate is not None:
        sc = sc.filter(pl.col("h52") > h52_gate)
    if don60:
        sc = sc.filter(pl.col("don60") > 0)
    sc = sc.with_columns(rank("h52").alias("score"))
    if weight == "equal":
        sc = sc.with_columns(pl.lit(1.0 / n_slots).alias("weight"))
    elif weight == "score":
        sc = sc.with_columns(
            (0.5 / n_slots + pl.col("score") / n_slots).alias("weight"))
    sc = (sc.select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    days = [d for d in lab.dates_all if d >= Date(2024, 10, 14)]
    flag = (pl.DataFrame({"date": days})
            .join(pl.DataFrame({C: memb[C].unique().to_list()}), how="cross")
            .join(memb.select(["date", C]), on=["date", C], how="anti")
            .sort(["date", C]))
    res = simulate(lab.panel, sc, exit_flags=flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new,
                                      min_hold_days=min_hold),
                   exit_spec=ExitSpec(trailing_stop=trail, loser_time_stop=lts,
                                      time_stop=time_stop,
                                      underwater_trail=uw_trail,
                                      profit_recycle=recycle),
                   start=Date(2024, 10, 14))
    nav = res.nav.sort("date")
    sw = nav.filter((pl.col("date") >= SW0) & (pl.col("date") <= SW1))
    yrs = (sw["date"][-1] - sw["date"][0]).days / 365.25
    cagr = (sw["nav"][-1] / sw["nav"][0]) ** (1 / yrs) - 1
    dd = sw["nav"] / sw["nav"].cum_max() - 1
    mdd = dd.min()
    martin = cagr / max(float(np.sqrt((dd.to_numpy() ** 2).mean())), 1e-9)
    return {"cagr": cagr, "mdd": mdd, "martin": martin, "tr": res.trades.height}


def main() -> None:
    lab = Lab3()
    grid = [
        ("基準 midmonth4+h52", {}),
        # 池籍語義
        ("pm3", {"pool_months": 3}),
        ("pm5", {"pool_months": 5}),
        ("pm6", {"pool_months": 6}),
        # 出場族
        ("time_stop 120", {"time_stop": 120}),
        ("time_stop 180", {"time_stop": 180}),
        ("recycle(0.6,0.4)", {"recycle": (0.6, 0.4)}),
        ("recycle(1.0,0.5)", {"recycle": (1.0, 0.5)}),
        ("uw_trail 20", {"uw_trail": 0.20}),
        ("min_hold 10", {"min_hold": 10}),
        ("min_hold 20", {"min_hold": 20}),
        ("trail40+lts20", {"trail": 0.40, "lts": 20}),
        # 入場/權重
        ("h52_gate 0.85", {"h52_gate": 0.85}),
        ("h52_gate 0.95", {"h52_gate": 0.95}),
        ("don60 突破gate", {"don60": True}),
        ("score加權", {"weight": "score"}),
    ]
    for name, kw in grid:
        k = run(lab, **kw)
        print(f"{name:20s}:CAGR {k['cagr']:7.1%}  MDD {k['mdd']:6.1%}  "
              f"Martin {k['martin']:5.2f}  tr {k['tr']}")


if __name__ == "__main__":
    main()
