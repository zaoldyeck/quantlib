"""EV35 — S × Serenity × Evergreen v3.3 三強組合前緣(零 token)。

Run: uv run --project research python -m research.evergreen.ev35_allocation
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

SW0, SW1 = Date(2025, 1, 2), Date(2026, 7, 3)


def load_navs() -> pl.DataFrame:
    s = (pl.read_parquet("research/apex/ledger/curves/T0334.parquet")
         .select(["date", pl.col("nav").alias("S")]))
    ser = (pl.read_csv("research/strat_lab/results/abl_adv_l0_ev_v2_thesis_inst_daily.csv",
                       try_parse_dates=True)
           .select(["date", pl.col("nav").alias("SER")]))
    # v3.3 NAV:重跑一次落檔(決定性)
    import os
    v3p = "research/evergreen/data/v33_nav.parquet"
    if not os.path.exists(v3p):
        from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
        from research.evergreen.ev30_baseline import midmonth_membership
        from research.evergreen.ev30_campaign import Lab3
        from research.evergreen.harvest import C
        lab = Lab3()
        memb = midmonth_membership(lab.reg, lab.dates_all, 3)

        def rank(c):
            return (pl.col(c).rank() / pl.len()).over("date")

        sc = (memb.join(lab.feats, on=["date", C], how="left")
              .filter(pl.col("h120").fill_null(0) > 0.6)
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
                       exit_spec=ExitSpec(trailing_stop=0.40, loser_time_stop=45),
                       start=Date(2024, 10, 14))
        res.nav.sort("date").select(["date", "nav"]).write_parquet(v3p)
    v3 = (pl.read_parquet(v3p).select(["date", pl.col("nav").alias("EV")]))
    j = (s.join(ser, on="date", how="inner").join(v3, on="date", how="inner")
         .filter((pl.col("date") >= SW0) & (pl.col("date") <= SW1)).sort("date"))
    return j


def main() -> None:
    j = load_navs()
    print(f"對齊 {j.height} 個交易日({j['date'][0]} ~ {j['date'][-1]})")
    rets = {}
    for c in ["S", "SER", "EV"]:
        v = j[c].to_numpy()
        rets[c] = v[1:] / v[:-1] - 1
    R = np.vstack([rets["S"], rets["SER"], rets["EV"]])
    corr = np.corrcoef(R)
    print("\n日報酬相關矩陣:")
    print("        S     SER    EV")
    for i, n in enumerate(["S  ", "SER", "EV "]):
        print(f"  {n}  " + "  ".join(f"{corr[i][k]:+.2f}" for k in range(3)))

    yrs = (j["date"][-1] - j["date"][0]).days / 365.25

    def stats(r):
        nav = np.cumprod(1 + r)
        cagr = nav[-1] ** (1 / yrs) - 1
        dd = nav / np.maximum.accumulate(nav) - 1
        mdd = dd.min()
        martin = cagr / max(float(np.sqrt((dd ** 2).mean())), 1e-9)
        return cagr, mdd, martin

    print("\n單一策略(同窗):")
    for c in ["S", "SER", "EV"]:
        cg, md, mt = stats(rets[c])
        print(f"  {c:3s}:CAGR {cg:7.1%}  MDD {md:6.1%}  Martin {mt:6.2f}")

    rows = []
    for w1 in range(0, 101, 5):
        for w2 in range(0, 101 - w1, 5):
            w3 = 100 - w1 - w2
            r = (w1 * rets["S"] + w2 * rets["SER"] + w3 * rets["EV"]) / 100
            cg, md, mt = stats(r)
            rows.append({"S": w1, "SER": w2, "EV": w3,
                         "cagr": cg, "mdd": md, "martin": mt})
    df = pl.DataFrame(rows)

    def show(title, d):
        print(f"\n{title}:")
        for r in d.to_dicts():
            print(f"  S{r['S']:3d}/SER{r['SER']:3d}/EV{r['EV']:3d}:"
                  f"CAGR {r['cagr']:7.1%}  MDD {r['mdd']:6.1%}  Martin {r['martin']:6.2f}")

    show("Max Martin top5", df.sort("martin", descending=True).head(5))
    show("Max CAGR top3", df.sort("cagr", descending=True).head(3))
    show("MDD ≥ −20% 下 Max CAGR top3",
         df.filter(pl.col("mdd") >= -0.20).sort("cagr", descending=True).head(3))
    show("MDD ≥ −25% 下 Max CAGR top3",
         df.filter(pl.col("mdd") >= -0.25).sort("cagr", descending=True).head(3))


if __name__ == "__main__":
    main()
