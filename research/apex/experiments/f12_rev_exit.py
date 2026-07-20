"""F12 — S × 營收轉衰出場(預註冊見 ledger/batches.md F12 段)。

Run: uv run --project research python -m research.apex.experiments.f12_rev_exit
依賴 cache: 是
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.assemble import entries_and_flags
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.f08_downmkt import C, DEV0, DEV1, WREL, prep
from research.evergreen.ev36_walkforward import kpis_full


def rev_flags(con, panel) -> dict[str, pl.DataFrame]:
    rev = (data.load_monthly_revenue(con, DEV1)
           .sort([C, "year", "month"])
           .with_columns([
               pl.date(pl.col("year") + pl.col("month") // 12,
                       pl.col("month") % 12 + 1, 10).alias("avail"),
               (pl.col("monthly_revenue_yoy") < 0).alias("neg"),
               ((pl.col("monthly_revenue_yoy") < 0)
                & (pl.col("monthly_revenue_yoy").shift(1).over(C) < 0))
               .alias("neg2"),
           ]).select([C, "avail", "neg", "neg2"])
           .drop_nulls(subset=["avail"]).sort("avail"))
    days = (panel.select("date").unique().sort("date")
            .filter(pl.col("date") >= pl.lit(DEV0).str.to_date()))
    codes = panel.select(C).unique()
    grid = (days.join(codes, how="cross").sort("date")
            .join_asof(rev, left_on="date", right_on="avail", by=C,
                       strategy="backward", tolerance="70d"))
    return {k: grid.filter(pl.col(v).fill_null(False)).select(["date", C])
            for k, v in (("neg1", "neg"), ("neg2", "neg2"))}


def run_rx(panel, feat, elig, extra_flag=None) -> dict:
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
    flag = stale if extra_flag is None else (
        pl.concat([stale, extra_flag]).unique(subset=["date", C])
        .sort(["date", C]))
    res = simulate(panel, entries, exit_flags=flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30,
                                      loser_time_stop=15),
                   start=Date.fromisoformat(DEV0))
    nav = (res.nav.sort("date")
           .filter(pl.col("date") >= pl.lit(DEV0).str.to_date())
           .select(["date", "nav"]))
    return kpis_full(nav)


def main() -> None:
    panel, feat, elig = prep()
    con = data.connect()
    fl = rev_flags(con, panel)
    print(f"dev 窗 {DEV0}~{DEV1};S 基準 120.9%/P5 ~67/MDD −32.6")
    for name, ef in (("S 基準(無營收出場)", None),
                     ("+ 營收 YoY<0 出場", fl["neg1"]),
                     ("+ 連兩月 YoY<0 出場", fl["neg2"])):
        k = run_rx(panel, feat, elig, ef)
        flag = "★" if (k["p5"] > 0.744 and k["mdd"] > -0.376) else " "
        print(f"{flag} {name:20s} CAGR {k['cagr']:7.1%}  P5 {k['p5']:6.1%}  "
              f"MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}")


if __name__ == "__main__":
    main()
