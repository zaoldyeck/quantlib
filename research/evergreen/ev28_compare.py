"""EV28 裁決:純質化搜尋標記(月中)vs EV17 有表版(月初)vs v1。

各臂 fwd63 自各自站位次交易日起算、h120>0.7 濾後(歷次 pilot 同度量)。
判準(LEDGER EV28):EV28 vs EV17 +15.8%,±3pp 規則。

Run: uv run --project research python -m research.evergreen.ev28_compare
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data

C = "company_code"
MID = {"2023-02": Date(2023, 2, 13), "2023-08": Date(2023, 8, 11),
       "2024-03": Date(2024, 3, 11), "2025-04": Date(2025, 4, 11)}
BEG = {"2023-02": Date(2023, 2, 1), "2023-08": Date(2023, 8, 1),
       "2024-03": Date(2024, 3, 1), "2025-04": Date(2025, 4, 1)}


def main() -> None:
    ev28 = (pl.read_parquet("research/evergreen/data/ev28_pilot_labels.parquet")
            .select(["month", pl.col("code").alias(C), "conviction"]))
    ev17 = (pl.read_parquet("research/evergreen/data/ev17_chips_labels.parquet")
            .with_columns(pl.col("month").str.slice(0, 7))
            .select(["month", pl.col("code").alias(C), "conviction"]))
    v1 = (pl.read_parquet("research/evergreen/data/registry_v1.parquet")
          .with_columns(pl.col("month").str.slice(0, 7))
          .filter(pl.col("month").is_in(list(MID)))
          .select(["month", pl.col("code").alias(C), "conviction"]))

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

    arms = [("EV28 純質化搜尋", ev28, MID), ("EV17 有表版   ", ev17, BEG),
            ("v1 切片       ", v1, BEG)]
    agg = {n: [] for n, _, _ in arms}
    print("=== 逐月(fwd63 自各自站位次交易日;濾後 h120>0.7)===")
    for m in MID:
        line = f"{m}:"
        for name, labels, stance in arms:
            d0 = next_td(stance[m])
            day = px.filter(pl.col("date") == d0)
            sub = labels.filter(pl.col("month") == m)
            hit = (day.join(sub.select(C), on=C, how="semi")
                   .filter(pl.col("h120") > 0.7))
            f = hit["fwd63"].drop_nulls().to_list()
            agg[name] += f
            line += f"  {name.strip()} n={sub.height:2d} 濾後 {fm(f)}({len(f)})"
        print(line)
    print()
    means = {}
    for name, a in agg.items():
        arr = np.array(a)
        means[name] = arr.mean()
        print(f"{name}:濾後合計 {arr.mean():+.1%}(n={len(arr)},"
              f"失敗率 {(arr < 0).mean():.0%})")
    diff = means["EV28 純質化搜尋"] - means["EV17 有表版   "]
    verdict = ("純質化架構勝出 → 進全量設計" if diff >= 0.03
               else "平手帶 → 架構純度+live 一致性選純質化" if diff > -0.03
               else "停下與使用者共同驗屍")
    print(f"\nEV28 − EV17 = {diff:+.1%} → {verdict}")


if __name__ == "__main__":
    main()
