"""EV30 定版:registry_v3 × h52-only 引擎——同窗終局數字 + 置換檢定。

置換:同月同標記數,從當日全池隨機抽股(200 次),看真實 CAGR 的分位。
Run: uv run --project . python -m quantlib.evergreen.ev30_final
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.evergreen.ev30_campaign import SW0, SW1, Lab3
from quantlib.evergreen.harvest import C
from quantlib import paths


def run_reg(lab: Lab3, reg: pl.DataFrame):
    from quantlib.evergreen.ev30_baseline import midmonth_membership
    memb = midmonth_membership(reg, lab.dates_all, 4)

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
    nav = res.nav.sort("date")
    sw = nav.filter((pl.col("date") >= SW0) & (pl.col("date") <= SW1))
    yrs = (sw["date"][-1] - sw["date"][0]).days / 365.25
    cagr = (sw["nav"][-1] / sw["nav"][0]) ** (1 / yrs) - 1
    mdd = (sw["nav"] / sw["nav"].cum_max() - 1).min()
    return cagr, mdd, res.trades.height


def main() -> None:
    lab = Lab3()
    reg = lab.reg
    cagr, mdd, tr = run_reg(lab, reg)
    print(f"定版:registry_v3 × h52-only:同窗 CAGR {cagr:.1%}  MDD {mdd:.1%}  tr {tr}")

    # 置換:每月同數量隨機股(從當月站位日全池抽)
    import duckdb
    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    per_month = dict(reg.group_by("month").len().iter_rows())
    stance_pool = {}
    for ym in per_month:
        y, m = int(ym[:4]), int(ym[5:7])
        d = min(x for x in lab.dates_all if x.year == y and x.month == m and x.day > 10)
        codes = [r[0] for r in raw.execute(
            "SELECT DISTINCT company_code FROM daily_quote WHERE date = ? "
            "AND company_code GLOB '[1-9][0-9][0-9][0-9]'", [d]).fetchall()]
        stance_pool[ym] = codes
    rng = np.random.default_rng(42)
    perm_cagrs = []
    for i in range(200):
        rows = []
        for ym, n in per_month.items():
            for c in rng.choice(stance_pool[ym], size=n, replace=False):
                rows.append({"month": ym, "code": str(c), "conviction": 4})
        pc, _, _ = run_reg(lab, pl.DataFrame(rows))
        perm_cagrs.append(pc)
        if (i + 1) % 50 == 0:
            print(f"  置換 {i+1}/200...")
    arr = np.array(perm_cagrs)
    p = (arr >= cagr).mean()
    print(f"置換分佈:median {np.median(arr):.1%}  P95 {np.percentile(arr,95):.1%}  "
          f"真實 {cagr:.1%} → p = {p:.3f}")


if __name__ == "__main__":
    main()
