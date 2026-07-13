"""EV36b — OOS 窗池天花板:完美前瞻排位下引擎能到多少(零 LLM)。

問題:EV36 OOS(2025-07-11~2026-07-03)Evergreen refit 413% < Serenity 573%。
繼續優化引擎前,先問「池允許嗎」——把排位分數換成上帝視角的未來報酬
(look-ahead,僅作理論上界,絕非可實現策略),同骨架跑 OOS:
  ceiling < 573% → 瓶頸在池(月頻標記資訊密度),引擎無解
  ceiling ≫ 573% → 引擎仍有空間,回 train 窗繼續挖

Run: uv run --project research python -m research.evergreen.ev36b_ceiling
依賴 cache: 是
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev36_walkforward import (C, OOS0, OOS1, Lab, seg_kpi)


def ceiling(lab: Lab, fwd: int, *, pool_months=3, n_slots=6, max_new=1,
            trail=0.30, lts=30, no_exit=False) -> dict:
    memb, flag = lab.memb(pool_months)
    fw = (lab.panel.sort([C, "date"])
          .with_columns((pl.col("close").shift(-fwd - 1) / pl.col("close").shift(-1) - 1)
                        .over(C).alias("fwd"))
          .select(["date", C, "fwd"]))
    sc = (memb.join(fw, on=["date", C], how="left")
          .with_columns(((pl.col("fwd").rank() / pl.len()).over("date")).alias("score"))
          .with_columns(pl.lit(1.0 / n_slots).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    es = (ExitSpec() if no_exit
          else ExitSpec(trailing_stop=trail, loser_time_stop=lts))
    res = simulate(lab.panel.filter(pl.col("date") <= OOS1), sc, exit_flags=flag,
                   exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new),
                   exit_spec=es, start=OOS0)
    nav = res.nav.sort("date").filter(
        (pl.col("date") >= OOS0) & (pl.col("date") <= OOS1))
    return seg_kpi(nav)


def pool_beta(lab: Lab) -> dict:
    """池等權日報酬(每日池成分等權,無席位限制)——池的 beta。"""
    memb, _ = lab.memb(3)
    ret = (lab.panel.sort([C, "date"])
           .with_columns((pl.col("close") / pl.col("close").shift(1) - 1)
                         .over(C).alias("r")).select(["date", C, "r"]))
    daily = (memb.join(ret, on=["date", C], how="left")
             .group_by("date").agg(pl.col("r").mean()).sort("date")
             .filter((pl.col("date") >= OOS0) & (pl.col("date") <= OOS1))
             .with_columns((1 + pl.col("r").fill_null(0)).cum_prod().alias("nav")))
    return seg_kpi(daily.select(["date", "nav"]))


def main() -> None:
    lab = Lab()
    print(f"OOS 窗 {OOS0} ~ {OOS1};池骨架 = EV36 top-1(pm3/6席/mn1/trail30/lts30)")
    b = pool_beta(lab)
    print(f"池等權 beta(無選股無席位):CAGR {b['cagr']:7.1%}  MDD {b['mdd']:6.1%}")
    for fwd in (21, 63):
        k = ceiling(lab, fwd)
        print(f"完美前瞻 fwd{fwd:2d} + 正常出場:  CAGR {k['cagr']:7.1%}  "
              f"MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}")
        k2 = ceiling(lab, fwd, no_exit=True)
        print(f"完美前瞻 fwd{fwd:2d} + 純池籍出場:CAGR {k2['cagr']:7.1%}  "
              f"MDD {k2['mdd']:6.1%}  Martin {k2['martin']:5.1f}")
    print("\n對照:Serenity 同窗 572.6% / EV36 top-1 實跑 413.2%")


if __name__ == "__main__":
    main()
