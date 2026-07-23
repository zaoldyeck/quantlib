"""EV31 — 捕捉時鐘:事件日入池 vs 站位日入池(registry_v3,零 token)。

僅「兩次站位間的新事件」提前(防前視);引擎 = h52-only 定版。
Run: uv run --project . python -m quantlib.evergreen.ev31_event_entry
"""
from __future__ import annotations

import re
from datetime import date as Date

import polars as pl

from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.evergreen.ev30_campaign import SW0, SW1, Lab3
from quantlib.evergreen.harvest import C

DATE_PAT = re.compile(r"(20\d{2})-(\d{1,2})-(\d{1,2})")


def stance_dates(lab: Lab3) -> dict:
    out = {}
    for ym in sorted(lab.reg["month"].unique().to_list()):
        y, m = int(ym[:4]), int(ym[5:7])
        out[ym] = min(d for d in lab.dates_all
                      if d.year == y and d.month == m and d.day > 10)
    return out


def build_membership(lab: Lab3, event_entry: bool) -> pl.DataFrame:
    st = stance_dates(lab)
    yms = sorted(st)
    prev = {ym: (st[yms[i - 1]] if i else None) for i, ym in enumerate(yms)}
    idx = {d: i for i, d in enumerate(lab.dates_all)}
    rows, promoted, kept = [], 0, 0
    for r in lab.reg.to_dicts():
        ym = r["month"]
        entry = st[ym]
        if event_entry:
            m = DATE_PAT.search(r.get("event", "") or "")
            if m:
                try:
                    ed = Date(int(m[1]), int(m[2]), int(m[3]))
                    p = prev[ym]
                    if p is not None and p < ed <= st[ym]:
                        entry = min(d for d in lab.dates_all if d > ed)
                        promoted += 1
                    else:
                        kept += 1
                except ValueError:
                    kept += 1
            else:
                kept += 1
        i0 = idx[entry]
        for d in lab.dates_all[i0: i0 + 84]:
            rows.append({"date": d, C: r["code"], "conv": r["conviction"]})
    if event_entry:
        print(f"  提前 {promoted} 筆 / 維持 {kept} 筆")
    return (pl.DataFrame(rows).group_by(["date", C])
            .agg(pl.col("conv").max()).sort(["date", C]))


def run(lab: Lab3, memb: pl.DataFrame):
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
    for name, ee in [("站位日入池(現行)", False), ("事件日入池(日頻捕捉)", True)]:
        memb = build_membership(lab, ee)
        cagr, mdd, tr = run(lab, memb)
        print(f"{name}:同窗 CAGR {cagr:.1%}  MDD {mdd:.1%}  tr {tr}")


if __name__ == "__main__":
    main()
