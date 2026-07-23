"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T19:41:04.141Z(工具 Bash)
涵蓋 trials(5):f06_base(5M/10元/60根), f06_no_ADV, f06_no_history, f06_no_price, f06_全移除
"""
"""F06 — 資格門檻消融(W3 窗):移除 ADV/價格/掛牌門檻的績效影響。"""
import numpy as np
import polars as pl
from datetime import date as Date
from quantlib.apex import data, ledger
from quantlib.apex.assemble import entries_and_flags
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.apex.experiments.g01_ml_ranker import C, S_WTS, W3_START, prep, kpi

con, panel, feat = prep()
VARIANTS = {
    "base(5M/10元/60根)": dict(min_adv=5e6, min_price=10.0, min_history=60),
    "no_ADV":            dict(min_adv=0.0,  min_price=10.0, min_history=60),
    "no_price":          dict(min_adv=5e6,  min_price=0.0,  min_history=60),
    "no_history":        dict(min_adv=5e6,  min_price=10.0, min_history=1),
    "全移除":            dict(min_adv=0.0,  min_price=0.0,  min_history=1),
}
f_stale = (feat.filter(pl.col("rev_fresh_days") >= 26).select(["date", C])
           .filter(pl.col("date") >= pl.lit(W3_START).str.to_date()))
rows = []
for name, kw in VARIANTS.items():
    elig = (data.eligibility(panel, **kw)
            .filter(pl.col("eligible")).select(["date", C]))
    pool = (feat.filter(pl.col("rev_fresh_days") <= 7)
            .join(elig, on=["date", C], how="semi")
            .drop_nulls(subset=list(S_WTS))
            .filter(pl.col("cfo_ni_ratio_ttm")
                    >= pl.col("cfo_ni_ratio_ttm").median().over("date")))
    geo = None
    for c_, wt in S_WTS.items():
        term = ((pl.col(c_).rank() / pl.len()).over("date")) ** wt
        geo = term if geo is None else geo * term
    sc = (pool.with_columns(geo.alias("score")).select(["date", C, "score"])
          .filter(pl.col("date") >= pl.lit(W3_START).str.to_date()))
    e, _ = entries_and_flags(sc, 5, 10**9)
    res = simulate(panel, e, exit_flags=f_stale, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=5, max_new_per_day=2),
                   exit_spec=ExitSpec(trailing_stop=0.35, time_stop=30, loser_time_stop=15),
                   start=Date.fromisoformat(W3_START))
    k = kpi(res.nav)
    ledger.log_trial(family="f_line", name=f"f06_{name}", hypothesis="資格門檻消融",
                     config=kw, window=f"{W3_START}..2026-07-09",
                     metrics={kk: float(vv) for kk, vv in k.items()},
                     batch="F06", curve=res.nav)
    rows.append({"variant": name, **{kk: round(vv, 3) for kk, vv in k.items()}})
print(pl.DataFrame(rows))
