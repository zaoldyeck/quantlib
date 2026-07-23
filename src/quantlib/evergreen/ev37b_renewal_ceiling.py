"""EV37b — 「續期判斷」的零成本上界:後見之明續期(look-ahead,僅界定空間)。

規則:標的池籍到期的站位日,若其後 63 交易日報酬 > 0 → 池籍延長至下一
站位日,迭代直到轉負。此為「完美續期判斷」上界——LLM 續期的現實成績
必然低於此。上界不顯著抬升績效 → 續期路線終止,零 LLM 成本。

Run: uv run --project . python -m quantlib.evergreen.ev37b_renewal_ceiling
依賴 cache: 是
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.evergreen.ev36_walkforward import (C, OOS0, OOS1, TRAIN0, TRAIN1,
                                                 Lab, seg_kpi)


def renewal_membership(lab: Lab, pool_months: int = 3) -> pl.DataFrame:
    """midmonth 池籍 + 後見之明續期(fwd63>0 於到期站位日則續 1 站位月)。"""
    reg, dates_all = lab.reg, lab.dates_all
    yms = sorted(reg["month"].unique().to_list())
    stance = {}
    for ym in yms:
        y, m = int(ym[:4]), int(ym[5:7])
        stance[ym] = min(d for d in dates_all
                         if d.year == y and d.month == m and d.day > 10)
    ordered = [stance[ym] for ym in yms]
    idx = {d: i for i, d in enumerate(dates_all)}

    # fwd63 查表
    px = lab.panel.select(["date", C, "close"]).sort([C, "date"])
    fw = (px.with_columns((pl.col("close").shift(-63) / pl.col("close") - 1)
                          .over(C).alias("fwd63"))
          .select(["date", C, "fwd63"]))
    fw_map = {(r["date"], r[C]): r["fwd63"] for r in fw.to_dicts()}

    def next_stance(d: Date) -> Date | None:
        later = [s for s in ordered if s > d]
        return later[0] if later else None

    rows = []
    for i, ym in enumerate(yms):
        start = ordered[i]
        base_end = (ordered[i + pool_months]
                    if i + pool_months < len(ordered) else dates_all[-1])
        window = yms[max(0, i - pool_months + 1): i + 1]
        cur = (reg.filter(pl.col("month").is_in(window))
               .group_by("code").agg(pl.col("conviction").max()))
        for r in cur.to_dicts():
            end = base_end
            # 後見之明續期:到期站位日 fwd63>0 → 延至下一站位日
            while True:
                f = fw_map.get((end, r["code"]))
                if f is None or f <= 0:
                    break
                nx = next_stance(end)
                if nx is None:
                    end = dates_all[-1]
                    break
                end = nx
            rows.append({"m_start": start, "m_end": end, C: r["code"],
                         "conv": r["conviction"]})
    memb = pl.DataFrame(rows)
    days = [d for d in dates_all if d >= ordered[0]]
    return (pl.DataFrame({"date": days}).join(memb, how="cross")
            .filter((pl.col("date") >= pl.col("m_start"))
                    & (pl.col("date") < pl.col("m_end")))
            .select(["date", C, "conv"]).unique(subset=["date", C])
            .sort(["date", C]))


def run(lab: Lab, memb: pl.DataFrame, *, h120=0.5, trail=0.30, lts=30,
        n_slots=6, max_new=1, rank_mode="h52_h120"):
    """EV36 top-1 引擎參數,僅換 membership。"""
    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120)
          .with_columns((rank("h52") * rank("h120")).alias("score"))
          .with_columns(pl.lit(1.0 / n_slots).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    days = [d for d in lab.dates_all if d >= TRAIN0]
    flag = (pl.DataFrame({"date": days})
            .join(pl.DataFrame({C: memb[C].unique().to_list()}), how="cross")
            .join(memb.select(["date", C]), on=["date", C], how="anti")
            .sort(["date", C]))

    def one(start, end):
        res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                       exit_flags=flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new),
                       exit_spec=ExitSpec(trailing_stop=trail, loser_time_stop=lts),
                       start=start)
        return res.nav.sort("date").filter(
            (pl.col("date") >= start) & (pl.col("date") <= end))

    return seg_kpi(one(TRAIN0, TRAIN1)), seg_kpi(one(OOS0, OOS1))


def main() -> None:
    lab = Lab()
    base_memb, _ = lab.memb(3)
    ren_memb = renewal_membership(lab, 3)
    d0 = base_memb.group_by("date").len()["len"].mean()
    d1 = ren_memb.group_by("date").len()["len"].mean()
    print(f"日均池大小:基準 {d0:.1f} → 完美續期 {d1:.1f}")
    for name, m in (("基準(池籍 3 月)", base_memb), ("完美續期上界", ren_memb)):
        tr, oo = run(lab, m)
        print(f"{name}: train CAGR {tr['cagr']:7.1%} MDD {tr['mdd']:6.1%} "
              f"Martin {tr['martin']:5.1f} | OOS CAGR {oo['cagr']:7.1%} "
              f"MDD {oo['mdd']:6.1%} Martin {oo['martin']:5.1f}")
    print("\n(上界為 look-ahead,僅界定 LLM 續期的理論空間;"
          "對照 Serenity OOS 572.6%)")


if __name__ == "__main__":
    main()
