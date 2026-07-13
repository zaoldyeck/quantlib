"""transcript 逐字復原(零改動)。

來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-09T21:51:25.147Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/research/apex/experiments/n01_momentum_single.py)
涵蓋 trials(48):n01_lb10s0n10b1, n01_lb10s0n10b2, n01_lb10s0n12b1, n01_lb10s0n12b2, n01_lb10s0n8b1, n01_lb10s0n8b2, n01_lb10s5n10b1, n01_lb10s5n10b2, n01_lb10s5n12b1, n01_lb10s5n12b2, n01_lb10s5n8b1, n01_lb10s5n8b2, n01_lb21s0n10b1, n01_lb21s0n10b2, n01_lb21s0n12b1, n01_lb21s0n12b2, n01_lb21s0n8b1, n01_lb21s0n8b2, n01_lb21s5n10b1, n01_lb21s5n10b2 …
"""
"""N01 — 自研動能單策略(Codex 概念的正確規格移植)。

月頻輪換 top-N 動能書:分散(N 8-12)、換倉壓縮(buffer)、無停損
(僅 loser-time 50 日)。預註冊見 ledger/batches.md N-LINE。

Run: uv run --project research python -m research.apex.experiments.n01_momentum_single
"""
from __future__ import annotations

import itertools
import time
from datetime import date as Date

import polars as pl

from research.apex import data, ledger
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.apex.experiments.g01_ml_ranker import C, W3_START, kpi, prep

GRID = list(itertools.product([10, 21, 42, 63], [0, 5], [8, 10, 12], [1, 2]))


def month_firsts(dates: list[Date]) -> list[Date]:
    out, cur = [], None
    for d in dates:
        if (d.year, d.month) != cur:
            out.append(d)
            cur = (d.year, d.month)
    return out


def run_cell(panel, elig, dates_all, rebals, lb, skip, n, buf):
    mom = (panel.sort([C, "date"])
           .with_columns((pl.col("close").shift(skip)
                          / pl.col("close").shift(skip + lb) - 1)
                         .over(C).alias("mom"))
           .select(["date", C, "mom"]))
    day = (mom.filter(pl.col("date").is_in(rebals))
           .join(elig, on=["date", C], how="semi")
           .drop_nulls(subset=["mom"])
           .with_columns(pl.col("mom").rank(descending=True).over("date").alias("rk")))
    entries = (day.filter(pl.col("rk") <= n)
               .select(["date", C, pl.col("mom").alias("score")]))
    # 換倉壓縮:跌出 top n×buf 才觸發輪換賣出(flag 全池非 buffer 名單)
    keep = day.filter(pl.col("rk") <= n * buf).select(["date", C])
    all_rebal_pool = day.select(["date", C])
    flags = all_rebal_pool.join(keep, on=["date", C], how="anti")
    res = simulate(panel, entries, exit_flags=flags, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=n),
                   exit_spec=ExitSpec(loser_time_stop=50),
                   start=Date.fromisoformat(W3_START))
    return res


def main() -> None:
    t0 = time.time()
    con, panel, feat = prep()
    elig = (data.eligibility(panel, min_adv=5_000_000.0)
            .filter(pl.col("eligible")).select(["date", C]))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()
    w3 = [d for d in dates_all if d >= Date.fromisoformat(W3_START)]
    rebals = month_firsts(w3)
    print(f"prep {time.time()-t0:.0f}s;rebal 日 {len(rebals)}")

    rows = []
    for lb, skip, n, buf in GRID:
        res = run_cell(panel, elig, dates_all, rebals, lb, skip, n, buf)
        k = kpi(res.nav)
        name = f"lb{lb}s{skip}n{n}b{buf}"
        ledger.log_trial(family="n_line", name=f"n01_{name}",
                         hypothesis="動能單策略正確規格",
                         config={"lb": lb, "skip": skip, "n": n, "buf": buf},
                         window=f"{W3_START}..2026-07-09",
                         metrics={kk: float(vv) for kk, vv in k.items()},
                         batch="N01", curve=res.nav)
        rows.append({"cell": name, "trades": res.trades.height,
                     **{kk: round(vv, 3) for kk, vv in k.items()}})
    out = pl.DataFrame(rows).sort("p5", descending=True)
    with pl.Config(tbl_rows=20):
        print(out.head(16))
    print(f"\nS 基準:CAGR 96.0 / P5 45.9 / Martin 13.4 / MDD −20.7")
    print(f"Iter95 同尺:CAGR 84.5 / P5 32.9(共同窗)")
    print(f"total {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()

