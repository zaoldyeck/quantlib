"""G05 — ML 第四臂執行層(預註冊見 ledger/batches.md G05 段)。

訊號 = g04_scores_fwd10.parquet 的 walk-forward OOS 預測(凍結);
引擎 = apex simulate(手續費/證交稅/滑價/T+1/現金約束全真)。
掃執行層:席位 × 遲滯帶 × 最短持有 × 每日換倉節流,目標壓換手保訊號。

Run: uv run --project . python -m quantlib.apex.experiments.g05_ml_exec
依賴 cache: 是。輸出:stdout 榜 + ledger/g05_results.parquet。
"""
from __future__ import annotations

import itertools
from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.evergreen.ev36_walkforward import kpis_full

C = "company_code"
SC_PATH = "src/quantlib/apex/ledger/g04_scores_fwd10.parquet"
OUT = "src/quantlib/apex/ledger/g05_results.parquet"


def main() -> None:
    sc_raw = (pl.read_parquet(SC_PATH)
              .with_columns(pl.col("pred").rank(descending=True)
                            .over("date").alias("rk")))
    codes = (sc_raw.filter(pl.col("rk") <= 40)[C].unique().to_list())
    print(f"訊號:{sc_raw['date'].min()} ~ {sc_raw['date'].max()};"
          f"top-40 累計 {len(codes)} 檔")
    con = data.connect()
    panel = (data.common_stocks(
        data.load_panel(con, "2018-06-01", "2026-07-15", warmup_days=50))
        .filter(pl.col(C).is_in(codes)).sort([C, "date"]))
    d0, d1 = sc_raw["date"].min(), sc_raw["date"].max()

    rows = []
    grid = list(itertools.product((5, 10), (None, 10, 20, 30),
                                  (1, 5, 10), (1, 2, 5)))
    for ns, buf, mh, mn in grid:
        entries = (sc_raw.filter(pl.col("rk") <= ns)
                   .with_columns((pl.col("pred") - pl.col("pred").min().over("date"))
                                 .alias("score"))
                   .with_columns(pl.lit(1.0 / ns).alias("weight"))
                   .select(["date", C, "score", "weight"])
                   .sort(["date", "score", C], descending=[False, True, False]))
        if buf is None:
            flag = (sc_raw.filter(pl.col("rk") > ns).select(["date", C])
                    .sort(["date", C]))
        else:
            flag = (sc_raw.filter(pl.col("rk") > buf).select(["date", C])
                    .sort(["date", C]))
        res = simulate(panel, entries, exit_flags=flag, exec_spec=ExecSpec(),
                       port_spec=PortSpec(n_slots=ns, max_new_per_day=mn,
                                          min_hold_days=mh),
                       exit_spec=ExitSpec(),
                       start=d0)
        nav = (res.nav.sort("date")
               .filter((pl.col("date") >= d0) & (pl.col("date") <= d1))
               .select(["date", "nav"]))
        k = kpis_full(nav)
        tr_per_yr = res.trades.height / ((d1 - d0).days / 365.25)
        rows.append({"n_slots": ns, "buffer": buf, "min_hold": mh,
                     "max_new": mn, "trades_yr": tr_per_yr, **k})
    df = (pl.DataFrame(rows, schema_overrides={"buffer": pl.Int64},
                       infer_schema_length=None)
          .sort(["p5", "cagr"], descending=True))
    df.write_parquet(OUT)
    with pl.Config(tbl_cols=-1, tbl_width_chars=200, tbl_rows=12):
        print(df.head(12))
    top = df.head(1).to_dicts()[0]
    ok = top["cagr"] > 0.80 and top["mdd"] > -0.50
    print(f"\n判準(淨成本 CAGR>80%、MDD≥−50):{'✓ 通過' if ok else '✗ 未過'}")
    print(f"top-1:{ {k: top[k] for k in ('n_slots','buffer','min_hold','max_new')} } "
          f"CAGR {top['cagr']:.1%} MDD {top['mdd']:.1%} P5 {top['p5']:.1%} "
          f"年均交易 {top['trades_yr']:.0f} 筆")


if __name__ == "__main__":
    main()
