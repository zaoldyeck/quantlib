"""F11 — S × 動態加減碼(預註冊見 ledger/batches.md F11 段)。

Run: uv run --project research python -m research.apex.experiments.f11_pyramid
依賴 cache: 是
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex.assemble import entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.f08_downmkt import C, DEV0, DEV1, WREL, prep
from research.evergreen.ev36_walkforward import kpis_full


def run_pyr(panel, feat, elig, *, pyr=None, rc=None) -> dict:
    pool = feat.filter(pl.col("rev_fresh_days") <= 7)
    df = (pool.join(elig.filter(pl.col("eligible")).select(["date", C]),
                    on=["date", C], how="semi")
          .drop_nulls(subset=list(WREL))
          .filter(pl.col("cfo_ni_ratio_ttm")
                  >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    expr = None
    for c_, wt in WREL.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        expr = term if expr is None else expr * term
    sc = (df.with_columns(expr.alias("score"))
          .select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(DEV0).str.to_date()))
    entries, _ = entries_and_flags(sc, 5, 10**9)
    stale = (feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C])
             .filter(pl.col("date") >= pl.lit(DEV0).str.to_date()))
    ps = (PortSpec(n_slots=5, max_new_per_day=2) if pyr is None
          else PortSpec(n_slots=5, max_new_per_day=2, pyramid_trigger=pyr[0],
                        pyramid_max=pyr[1], pyramid_frac=pyr[2]))
    res = simulate(panel, entries, exit_flags=stale, exec_spec=ExecSpec(),
                   port_spec=ps,
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                      loser_time_stop=15, profit_recycle=rc),
                   start=Date.fromisoformat(DEV0))
    nav = (res.nav.sort("date")
           .filter(pl.col("date") >= pl.lit(DEV0).str.to_date())
           .select(["date", "nav"]))
    return kpis_full(nav)


def main() -> None:
    panel, feat, elig = prep()
    variants = [
        ("S 基準(全關)", dict()),
        ("加碼 .15×1×0.5", dict(pyr=(0.15, 1, 0.5))),
        ("加碼 .15×1×1.0", dict(pyr=(0.15, 1, 1.0))),
        ("加碼 .30×1×0.5", dict(pyr=(0.30, 1, 0.5))),
        ("加碼 .30×2×0.5", dict(pyr=(0.30, 2, 0.5))),
        ("減碼 rc(0.6,0.4)", dict(rc=(0.6, 0.4))),
        ("減碼 rc(1.0,0.5)", dict(rc=(1.0, 0.5))),
        ("加(.15,1,.5)+減(0.6,.4)", dict(pyr=(0.15, 1, 0.5), rc=(0.6, 0.4))),
    ]
    print(f"dev 窗 {DEV0}~{DEV1};S 基準 120.9%/P5 ~67/MDD −32.6")
    for name, kw in variants:
        k = run_pyr(panel, feat, elig, **kw)
        flag = "★" if (k["p5"] > 0.744 and k["mdd"] > -0.376) else " "
        print(f"{flag} {name:20s} CAGR {k['cagr']:7.1%}  P5 {k['p5']:6.1%}  "
              f"MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}")


if __name__ == "__main__":
    main()
