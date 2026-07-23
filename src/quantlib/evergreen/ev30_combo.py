"""EV30 二階:h52-only 基座上的組合掃描 + 跨月穩定性檢查。

Run: uv run --project . python -m quantlib.evergreen.ev30_combo
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from quantlib.evergreen.ev30_campaign import SW0, SW1, Lab3


def main() -> None:
    lab = Lab3()
    grid = [
        ("h52 only(一階最優)", {"axes": ("h52",)}),
        ("h52+6席", {"axes": ("h52",), "n_slots": 6}),
        ("h52+3席", {"axes": ("h52",), "n_slots": 3}),
        ("h52+trail25", {"axes": ("h52",), "trail": 0.25}),
        ("h52+mn1", {"axes": ("h52",), "max_new": 1}),
        ("h52+6席+trail25", {"axes": ("h52",), "n_slots": 6, "trail": 0.25}),
        ("h52+h120.6", {"axes": ("h52",), "h120": 0.6}),
        ("h52+ltsNone", {"axes": ("h52",), "lts": None}),
    ]
    for name, kw in grid:
        k = lab.run(**kw)
        print(f"{name:24s}:CAGR {k['cagr']:7.1%}  MDD {k['mdd']:6.1%}  "
              f"Martin {k['martin']:5.2f}  tr {k['tr']}")

    # 跨月穩定性:h52 only vs 基準的逐季報酬
    import numpy as np
    for name, kw in [("基準(h52×mom)", {}), ("h52 only", {"axes": ("h52",)})]:
        memb = lab.memb(kw.get("pool_months", 4))
        # 重跑取 nav
        from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
        from quantlib.evergreen.harvest import C
        def rank(c):
            return (pl.col(c).rank() / pl.len()).over("date")
        axes = kw.get("axes", ("h52", "mom"))
        sc = (memb.join(lab.feats, on=["date", C], how="left")
              .filter(pl.col("h120").fill_null(0) > 0.7))
        expr = None
        for a in axes:
            expr = rank(a) if expr is None else expr * rank(a)
        sc = (sc.with_columns(expr.alias("score"))
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
        print(f"\n{name} 逐季:", {r["q"]: f"{r['r']:+.0%}" for r in q.to_dicts()})


if __name__ == "__main__":
    main()
