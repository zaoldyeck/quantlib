"""EV32 二階:pm3 基座組合 + 跨季穩定檢驗。

Run: uv run --project research python -m research.evergreen.ev32_combo
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev30_baseline import midmonth_membership
from research.evergreen.ev30_campaign import SW0, SW1, Lab3
from research.evergreen.ev32_campaign import run
from research.evergreen.harvest import C


def quarterly(lab: Lab3, pool_months: int):
    memb = midmonth_membership(lab.reg, lab.dates_all, pool_months)

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > 0.7)
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
                   exit_spec=ExitSpec(trailing_stop=0.35, loser_time_stop=30),
                   start=Date(2024, 10, 14))
    nav = res.nav.sort("date").filter(
        (pl.col("date") >= SW0) & (pl.col("date") <= SW1))
    q = (nav.group_by(pl.col("date").dt.strftime("%Y-Q%q").alias("q"))
         .agg((pl.col("nav").last() / pl.col("nav").first() - 1).alias("r"))
         .sort("q"))
    return {r["q"]: f"{r['r']:+.0%}" for r in q.to_dicts()}


def main() -> None:
    lab = Lab3()
    print("=== 跨季穩定(pm4 vs pm3)===")
    print("pm4:", quarterly(lab, 4))
    print("pm3:", quarterly(lab, 3))
    print("\n=== pm3 基座二階 ===")
    grid = [
        ("pm3(一階最優)", {"pool_months": 3}),
        ("pm3+ts120", {"pool_months": 3, "time_stop": 120}),
        ("pm3+6席", {"pool_months": 3, "n_slots": 6}),
        ("pm3+4席", {"pool_months": 3, "n_slots": 4}),
        ("pm3+mn3", {"pool_months": 3, "max_new": 3}),
        ("pm3+h120.6", {"pool_months": 3, "h120": 0.6}),
        ("pm2", {"pool_months": 2}),
    ]
    for name, kw in grid:
        k = run(lab, **kw)
        print(f"{name:16s}:CAGR {k['cagr']:7.1%}  MDD {k['mdd']:6.1%}  "
              f"Martin {k['martin']:5.2f}  tr {k['tr']}")


if __name__ == "__main__":
    main()
