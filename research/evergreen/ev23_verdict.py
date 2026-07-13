"""EV23 裁決:否決官審核後池 vs 原池(fwd63),veto 組有效性檢查。

判準(LEDGER EV23):審核後 ≥ 原池 +3pp 且 veto 組 fwd63 < 原池均值
→ 全量加審核層;veto 錯殺(被殺者高報酬)→ 蓋棺。

前置:ev23_veto_verdicts.parquet。
Run: uv run --project research python -m research.evergreen.ev23_verdict
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data

C = "company_code"
ASOF = ["2023-02-01", "2023-08-01", "2024-03-01", "2025-04-01"]


def main() -> None:
    v = pl.read_parquet("research/evergreen/data/ev23_veto_verdicts.parquet")
    ev17 = (pl.read_parquet("research/evergreen/data/ev17_chips_labels.parquet")
            .select(["month", pl.col("code").alias(C), "conviction"]))
    merged = ev17.join(v.select(["month", pl.col("code").alias(C), "action"]),
                       on=["month", C], how="left")

    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2022-06-01", "2026-07-09", warmup_days=10))
    px = (panel.sort([C, "date"])
          .with_columns([
              (pl.col("close").shift(-63) / pl.col("close") - 1)
              .over(C).alias("fwd63"),
              (pl.col("close") / pl.col("close").rolling_max(120))
              .over(C).alias("h120"),
          ]).select(["date", C, "fwd63", "h120"]))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()

    def next_td(d: Date) -> Date:
        return min(x for x in dates_all if x > d)

    outs = []
    for m in ASOF:
        d0 = next_td(Date.fromisoformat(m))
        outs.append(px.filter(pl.col("date") == d0)
                    .join(merged.filter(pl.col("month") == m),
                          on=C, how="inner"))
    j = pl.concat(outs).drop_nulls(subset=["fwd63"])

    flt = j.filter(pl.col("h120") > 0.7)
    orig = flt["fwd63"].mean()
    kept = flt.filter(pl.col("action") == "keep")["fwd63"].mean()
    veto = j.filter(pl.col("action") == "veto")
    print(f"原池(濾後):{orig:+.1%}(n={flt.height})")
    print(f"審核後池:  {kept:+.1%}(n={flt.filter(pl.col('action')=='keep').height})")
    print(f"\n被 veto 組(裸,n={veto.height}):")
    for r in veto.sort("fwd63", descending=True).to_dicts():
        print(f"  {r['month'][:7]} {r[C]}  fwd63 {r['fwd63']:+.1%}")
    vm = veto["fwd63"].mean() if veto.height else float("nan")
    print(f"  veto 組均值 {vm:+.1%} vs 原池 {orig:+.1%}")
    ok = (kept - orig >= 0.03) and (vm < orig)
    print(f"\n裁決:{'審核層通過' if ok else '審核層未過(veto 錯殺或增益不足)→ 蓋棺'}")


if __name__ == "__main__":
    main()
