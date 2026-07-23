"""EV25 對比:月中站位(月報甫公佈)vs 月初站位(EV17)。

fwd63 各自從站位 asof 次一交易日起算(站位時點變因的真實語義);
h120>0.7 濾後。判準(LEDGER EV25):月中 − 月初 ≥ +3pp → 站位改制;
≤ −3pp 或之間 → 月初維持。

Run: uv run --project . python -m quantlib.evergreen.ev25_compare
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data

C = "company_code"
MID_ASOF = {"2023-02": Date(2023, 2, 13), "2023-08": Date(2023, 8, 11),
            "2024-03": Date(2024, 3, 11), "2025-04": Date(2025, 4, 11)}
BEG_ASOF = {"2023-02": Date(2023, 2, 1), "2023-08": Date(2023, 8, 1),
            "2024-03": Date(2024, 3, 1), "2025-04": Date(2025, 4, 1)}


def main() -> None:
    mid = (pl.read_parquet("src/quantlib/evergreen/data/ev25_midmonth_labels.parquet")
           .with_columns(pl.col("code").str.extract(r"(\d{4})", 1)))
    beg = (pl.read_parquet("src/quantlib/evergreen/data/ev17_chips_labels.parquet")
           .with_columns(pl.col("month").str.slice(0, 7)))

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

    def fm(xs):
        return f"{np.mean(xs):+.1%}" if len(xs) else "  na "

    agg = {"月中": {"flt": []}, "月初": {"flt": []}}
    print("=== 逐月(fwd63 各自站位次交易日起;濾後 h120>0.7)===")
    for m in MID_ASOF:
        line = f"{m}:"
        for name, labels, asof in [("月中", mid, MID_ASOF[m]),
                                   ("月初", beg, BEG_ASOF[m])]:
            d0 = next_td(asof)
            day = px.filter(pl.col("date") == d0)
            sub = labels.filter(pl.col("month") == m)
            hit = (day.join(sub.select(pl.col("code").alias(C)), on=C, how="semi")
                   .filter(pl.col("h120") > 0.7))
            f = hit["fwd63"].drop_nulls().to_list()
            agg[name]["flt"] += f
            line += f"  {name} n={sub.height:2d} 濾後 {fm(f)}({len(f)})"
        print(line)
    print()
    means = {}
    for name, a in agg.items():
        arr = np.array(a["flt"])
        means[name] = arr.mean()
        print(f"{name} 合計:濾後 {arr.mean():+.1%}(n={len(arr)},"
              f"失敗率 {(arr < 0).mean():.0%})")
    diff = means["月中"] - means["月初"]
    verdict = ("站位改制候選(月中勝)" if diff >= 0.03
               else "月初維持")
    print(f"\n月中 − 月初 = {diff:+.1%} → {verdict}")


if __name__ == "__main__":
    main()
