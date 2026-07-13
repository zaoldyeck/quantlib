"""EV34 驗證鏈:分佈 → 單調性 → 端到端(加權 vs 等權,v3.3 引擎)。

Run: uv run --project research python -m research.evergreen.ev34_verdict <task_output>
"""
from __future__ import annotations

import json
import sys
from datetime import date as Date

import numpy as np
import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev30_baseline import midmonth_membership
from research.evergreen.ev30_campaign import SW0, SW1, Lab3
from research.evergreen.harvest import C


def main() -> None:
    doc = json.load(open(sys.argv[1]))
    rows = []
    for r in doc["result"]:
        rows.append(pl.DataFrame(r["items"]))
    rs = pl.concat(rows).rename({"new_conviction": "nc"})
    rs.write_parquet("research/evergreen/data/ev34_rescored.parquet")
    print(f"落盤 {rs.height} 筆;分佈:",
          dict(rs.group_by("nc").len().sort("nc").iter_rows()))

    lab = Lab3()
    reg = lab.reg.join(rs.select([pl.col("month"), pl.col("code"), "nc"]),
                       on=["month", "code"], how="left")
    print(f"join 命中 {reg.filter(pl.col('nc').is_not_null()).height}/{reg.height}")
    reg = reg.with_columns(pl.col("nc").fill_null(3))

    # 單調性:標記層 fwd63(站位次日起)
    px = (lab.panel.sort([C, "date"])
          .with_columns((pl.col("close").shift(-63) / pl.col("close") - 1)
                        .over(C).alias("fwd63"))
          .select(["date", C, "fwd63"]))
    outs = []
    for r in reg.to_dicts():
        y, m = int(r["month"][:4]), int(r["month"][5:7])
        st = min(d for d in lab.dates_all if d.year == y and d.month == m and d.day > 10)
        d0 = min(d for d in lab.dates_all if d > st)
        hit = px.filter((pl.col("date") == d0) & (pl.col(C) == r["code"]))
        if hit.height and hit["fwd63"][0] is not None:
            outs.append({"nc": r["nc"], "fwd63": hit["fwd63"][0]})
    mono = (pl.DataFrame(outs).group_by("nc")
            .agg(pl.col("fwd63").mean(), pl.len()).sort("nc"))
    print("單調性(nc 組 fwd63):")
    for r in mono.to_dicts():
        print(f"  nc{r['nc']}: {r['fwd63']:+.1%}(n={r['len']})")

    # 端到端:weight ∝ nc vs 等權
    memb_reg = reg.select(["month", "code", pl.col("nc").alias("conviction")])
    for name, weighted in [("等權(現行 v3.3)", False), ("nc 加權", True)]:
        memb = midmonth_membership(
            memb_reg if weighted else lab.reg, lab.dates_all, 3)

        def rank(c):
            return (pl.col(c).rank() / pl.len()).over("date")

        sc = (memb.join(lab.feats, on=["date", C], how="left")
              .filter(pl.col("h120").fill_null(0) > 0.6)
              .with_columns(rank("h52").alias("score")))
        if weighted:
            sc = sc.with_columns(
                ((pl.col("conv") / pl.col("conv").mean().over("date")) / 5)
                .clip(0.08, 0.40).alias("weight"))
        else:
            sc = sc.with_columns(pl.lit(0.2).alias("weight"))
        sc = (sc.select(["date", C, "score", "weight"]).drop_nulls()
              .sort(["date", "score", C], descending=[False, True, False]))
        days = [d for d in lab.dates_all if d >= Date(2024, 10, 14)]
        flag = (pl.DataFrame({"date": days})
                .join(pl.DataFrame({C: memb[C].unique().to_list()}), how="cross")
                .join(memb.select(["date", C]), on=["date", C], how="anti")
                .sort(["date", C]))
        res = simulate(lab.panel, sc, exit_flags=flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                       exit_spec=ExitSpec(trailing_stop=0.40, loser_time_stop=45),
                       start=Date(2024, 10, 14))
        nav = res.nav.sort("date")
        sw = nav.filter((pl.col("date") >= SW0) & (pl.col("date") <= SW1))
        yrs = (sw["date"][-1] - sw["date"][0]).days / 365.25
        cagr = (sw["nav"][-1] / sw["nav"][0]) ** (1 / yrs) - 1
        mdd = (sw["nav"] / sw["nav"].cum_max() - 1).min()
        print(f"{name}:CAGR {cagr:.1%}  MDD {mdd:.1%}")


if __name__ == "__main__":
    main()
