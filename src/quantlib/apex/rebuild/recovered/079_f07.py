"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T19:47:30.457Z(工具 Bash)
涵蓋 trials(4):f07_stale26, f07_stale28, f07_stale30, f07_stale32
"""
"""F07 — stale 跨揭露續抱消融(W3)+ 天二個案帳。"""
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger
from quantlib.apex.assemble import entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.apex.experiments.g01_ml_ranker import C, S_WTS, W3_START, prep, kpi, paired

con, panel, feat = prep()
elig = (data.eligibility(panel, min_adv=5_000_000.0)
        .filter(pl.col("eligible")).select(["date", C]))
pool = (feat.filter(pl.col("rev_fresh_days") <= 7)
        .join(elig, on=["date", C], how="semi").drop_nulls(subset=list(S_WTS))
        .filter(pl.col("cfo_ni_ratio_ttm") >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
geo = None
for c_, wt in S_WTS.items():
    term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
    geo = term if geo is None else geo * term
sc = (pool.with_columns(geo.alias("score")).select(["date", C, "score"])
      .filter(pl.col("date") >= pl.lit(W3_START).str.to_date()))
e, _ = entries_and_flags(sc, 5, 10**9)

rows, navs = [], {}
for stale in [26, 28, 30, 32]:
    f = (feat.filter(pl.col("rev_fresh_days") >= stale).select(["date", C])
         .filter(pl.col("date") >= pl.lit(W3_START).str.to_date()))
    res = simulate(panel, e, exit_flags=f, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15),
                   start=Date.fromisoformat(W3_START))
    navs[stale] = res.nav.select(["date", "nav"]).sort("date")
    k = kpi(navs[stale])
    n_stale = res.trades.filter(pl.col("exit_reason") == "signal").height
    ledger.log_trial(family="f_line", name=f"f07_stale{stale}", hypothesis="跨揭露續抱",
                     config={"stale": stale}, window=f"{W3_START}..2026-07-09",
                     metrics={kk: float(vv) for kk, vv in k.items()},
                     batch="F07", curve=navs[stale])
    rows.append({"stale": stale, "n_trades": res.trades.height,
                 "n_stale_exit": n_stale, **{kk: round(vv, 3) for kk, vv in k.items()}})
print(pl.DataFrame(rows))
for s in [28, 30, 32]:
    d = paired(navs[s], navs[26])
    print(f"配對 stale{s} − base26:{d['mean']:+.2%}/年  CI [{d['lo']:+.2%}, {d['hi']:+.2%}]")

# 天二個案:7/7 出場價 vs 續抱到 7/9 的差
tz = panel.filter((pl.col(C) == "6834") & (pl.col("date").is_between(Date(2026,7,6), Date(2026,7,9)))
     ).select(["date", "close", "open"]).sort("date")
print("\n天二 7/6-7/9 價格:")
print(tz)
