"""EV37 — 出場解耦 × 行為觸發進場(設計見 LEDGER.md EV37 預註冊段)。

結構軸:
- exit_mode "pool"  :現狀——出池即強制出場(池籍輪換)
- exit_mode "signal":進場資格=池籍;出場只由 trail/lts/time 決定,出池不賣
- entry_gate none / don60(當日收盤破 60 日高)/ h52_95(收盤 ≥ 年高 95%)

Run: uv run --project . python -m quantlib.evergreen.ev37_decouple
依賴 cache: 是。輸出:data/ev37_results.parquet + stdout 榜單。
"""
from __future__ import annotations

import itertools
from datetime import date as Date

import polars as pl

from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.evergreen.ev36_walkforward import (C, OOS0, OOS1, TRAIN0, TRAIN1,
                                                 Lab, seg_kpi)

OUT = "src/quantlib/evergreen/data/ev37_results.parquet"


class Lab37(Lab):
    def __init__(self):
        super().__init__()
        self.feats = self.feats.join(
            (self.panel.sort([C, "date"])
             .with_columns((pl.col("close")
                            > pl.col("close").shift(1).rolling_max(60))
                           .over(C).alias("don60"))
             .select(["date", C, "don60"])),
            on=["date", C], how="left")
        self._empty_flag = pl.DataFrame(
            schema={"date": pl.Date, C: pl.Utf8})


def run37(lab: Lab37, *, exit_mode, entry_gate, pool_months, h120, trail, lts,
          time_stop, n_slots, max_new, want_nav=False):
    memb, pool_flag = lab.memb(pool_months)

    sc = (memb.join(lab.feats, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > h120))
    if entry_gate == "don60":
        sc = sc.filter(pl.col("don60").fill_null(False))
    elif entry_gate == "h52_95":
        sc = sc.filter(pl.col("h52") > 0.95)
    sc = (sc.with_columns(((pl.col("h52").rank() / pl.len()).over("date"))
                          .alias("score"))
          .with_columns(pl.lit(1.0 / n_slots).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    flag = pool_flag if exit_mode == "pool" else lab._empty_flag

    def one(start, end):
        res = simulate(lab.panel.filter(pl.col("date") <= end), sc,
                       exit_flags=flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=n_slots, max_new_per_day=max_new),
                       exit_spec=ExitSpec(trailing_stop=trail,
                                          loser_time_stop=lts,
                                          time_stop=time_stop),
                       start=start)
        return res.nav.sort("date").filter(
            (pl.col("date") >= start) & (pl.col("date") <= end))

    out = {"train": seg_kpi(one(TRAIN0, TRAIN1))}
    if want_nav:
        out["oos"] = seg_kpi(one(OOS0, OOS1))
    return out


def main() -> None:
    lab = Lab37()
    grid = list(itertools.product(
        ("pool", "signal"), ("none", "don60", "h52_95"),
        (2, 3), (0.0, 0.6),
        (0.30, 0.40, None), (30, 45, None), (None, 120),
        (5, 6, 8), (1, 2)))
    rows = []
    for i, (em, eg, pm, h1, tr, lt, ts, ns, mn) in enumerate(grid):
        cfg = dict(exit_mode=em, entry_gate=eg, pool_months=pm, h120=h1,
                   trail=tr, lts=lt, time_stop=ts, n_slots=ns, max_new=mn)
        k = run37(lab, **cfg)["train"]
        rows.append({**cfg, **{f"tr_{x}": v for x, v in k.items()}})
        if (i + 1) % 400 == 0:
            print(f"  {i + 1}/{len(grid)}")
    df = (pl.DataFrame(rows, schema_overrides={
              "trail": pl.Float64, "lts": pl.Int64, "time_stop": pl.Int64},
              infer_schema_length=None)
          .sort(["tr_martin", "tr_cagr"], descending=True))
    df.write_parquet(OUT)

    print("\n=== EV37 train 榜首 8(對照 EV36 榜首 Martin 30.3)===")
    with pl.Config(tbl_cols=-1, tbl_width_chars=200):
        print(df.head(8))

    top = df.head(3).to_dicts()
    if top[0]["tr_martin"] > 30.3:
        print("\n★ 新 top-1 勝過 EV36 榜首 → 動用 OOS 窺視 #2(LEDGER 已記帳)")
        for j, r in enumerate(top):
            cfg = {k: r[k] for k in ("exit_mode", "entry_gate", "pool_months",
                                     "h120", "trail", "lts", "time_stop",
                                     "n_slots", "max_new")}
            out = run37(lab, **cfg, want_nav=True)
            print(f"top{j + 1} {cfg}")
            print(f"   train CAGR {out['train']['cagr']:7.1%} MDD {out['train']['mdd']:6.1%} "
                  f"Martin {out['train']['martin']:5.1f} | OOS CAGR {out['oos']['cagr']:7.1%} "
                  f"MDD {out['oos']['mdd']:6.1%} Martin {out['oos']['martin']:5.1f}")
        print("\n對手同窗:Serenity 572.6%/−14.5;S 95.6%/−20.7")
    else:
        print("\n新結構未勝過 EV36 榜首——OOS 不動用,誠實收錄")


if __name__ == "__main__":
    main()
