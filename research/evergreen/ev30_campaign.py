"""EV30 — registry_v3 池上引擎重優化(薄池適配)。

紀律:同窗 KPI(CAGR/MDD/Martin);只採大幅且跨月穩定的改進(18 個月
短窗,微小改進視為過擬合)。conviction 幾乎全 4 → conv 軸失效,
排位軸重掃。

Run: uv run --project research python -m research.evergreen.ev30_campaign
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev30_baseline import midmonth_membership
from research.evergreen.harvest import C, build_feats

SW0, SW1 = Date(2025, 1, 2), Date(2026, 7, 3)


class Lab3:
    def __init__(self) -> None:
        self.reg = pl.read_parquet("research/evergreen/data/registry_v3.parquet")
        con = data.connect()
        self.panel = data.common_stocks(
            data.load_panel(con, "2023-06-01", "2026-07-09", warmup_days=300))
        self.dates_all = (self.panel.select("date").unique()
                          .sort("date")["date"].to_list())
        self.feats = build_feats(self.panel)
        self._memb = {}

    def memb(self, pool_months: int) -> pl.DataFrame:
        if pool_months not in self._memb:
            self._memb[pool_months] = midmonth_membership(
                self.reg, self.dates_all, pool_months)
        return self._memb[pool_months]

    def run(self, *, pool_months=4, h120=0.7, axes=("h52", "mom"),
            n_slots=5, max_new=2, trail=0.35, lts=30):
        memb = self.memb(pool_months)

        def rank(c):
            return (pl.col(c).rank() / pl.len()).over("date")

        sc = (memb.join(self.feats, on=["date", C], how="left")
              .filter(pl.col("h120").fill_null(0) > h120))
        expr = None
        for a in axes:
            expr = rank(a) if expr is None else expr * rank(a)
        sc = (sc.with_columns(expr.alias("score"))
              .with_columns(pl.lit(1.0 / n_slots).alias("weight"))
              .select(["date", C, "score", "weight"]).drop_nulls()
              .sort(["date", "score", C], descending=[False, True, False]))
        days = [d for d in self.dates_all if d >= Date(2024, 10, 14)]
        all_codes = memb[C].unique().to_list()
        flag = (pl.DataFrame({"date": days})
                .join(pl.DataFrame({C: all_codes}), how="cross")
                .join(memb.select(["date", C]), on=["date", C], how="anti")
                .sort(["date", C]))
        res = simulate(self.panel, sc, exit_flags=flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new),
                       exit_spec=ExitSpec(trailing_stop=trail, loser_time_stop=lts),
                       start=Date(2024, 10, 14))
        nav = res.nav.sort("date")
        sw = nav.filter((pl.col("date") >= SW0) & (pl.col("date") <= SW1))
        yrs = (sw["date"][-1] - sw["date"][0]).days / 365.25
        cagr = (sw["nav"][-1] / sw["nav"][0]) ** (1 / yrs) - 1
        dd = (sw["nav"] / sw["nav"].cum_max() - 1)
        mdd = dd.min()
        martin = cagr / max(float(np.sqrt((dd.to_numpy() ** 2).mean())), 1e-9)
        return {"cagr": cagr, "mdd": mdd, "martin": martin,
                "tr": res.trades.height}


def main() -> None:
    lab = Lab3()
    grid = [
        ("基準 5席mn2 h.7 pm4 t35", {}),
        ("3席", {"n_slots": 3}),
        ("4席", {"n_slots": 4}),
        ("6席", {"n_slots": 6}),
        ("pm5", {"pool_months": 5}),
        ("pm6", {"pool_months": 6}),
        ("h120 .5", {"h120": 0.5}),
        ("h120 .6", {"h120": 0.6}),
        ("h120 .8", {"h120": 0.8}),
        ("mom only", {"axes": ("mom",)}),
        ("h52 only", {"axes": ("h52",)}),
        ("trail 25", {"trail": 0.25}),
        ("trail 30", {"trail": 0.30}),
        ("trail 45", {"trail": 0.45}),
        ("lts 45", {"lts": 45}),
        ("lts None", {"lts": None}),
        ("mn1", {"max_new": 1}),
        ("mn3", {"max_new": 3}),
    ]
    for name, kw in grid:
        k = lab.run(**kw)
        print(f"{name:22s}:CAGR {k['cagr']:7.1%}  MDD {k['mdd']:6.1%}  "
              f"Martin {k['martin']:5.2f}  tr {k['tr']}")


if __name__ == "__main__":
    main()
