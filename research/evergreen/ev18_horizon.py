"""EV18 延伸:蒸餾版 vs 手寫版池品質的兌現期結構(fwd63/126/252,無濾網)。

回答「蒸餾版(value 傾斜)配自己的引擎能否超越 v3」的前置證據:
若蒸餾版只是『太早的正確』,裸池 fwd126/252 應反超手寫版;若長窗仍輸,
value 池的 alpha 密度不足,自建引擎戰線蓋棺。

Run: uv run --project research python -m research.evergreen.ev18_horizon
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data

C = "company_code"
ASOF = ["2023-02-01", "2023-08-01", "2024-03-01", "2025-04-01"]


def main() -> None:
    dist = (pl.read_parquet("research/evergreen/data/ev18_distilled_labels.parquet")
            .select(["month", pl.col("code").alias(C)]))
    chips = (pl.read_parquet("research/evergreen/data/ev17_chips_labels.parquet")
             .select(["month", pl.col("code").alias(C)]))

    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2022-06-01", "2026-07-09", warmup_days=10))
    px = (panel.sort([C, "date"])
          .with_columns([
              (pl.col("close").shift(-h) / pl.col("close") - 1)
              .over(C).alias(f"fwd{h}") for h in (63, 126, 252)
          ]).select(["date", C, "fwd63", "fwd126", "fwd252"]))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()

    def next_td(d: Date) -> Date:
        return min(x for x in dates_all if x > d)

    src = {"EV18 蒸餾(value 傾斜)": dist, "EV17 手寫(動能相容)": chips}
    print("=== 裸池(無 h120 濾網)兌現期結構 ===")
    for name, labels in src.items():
        vals = {h: [] for h in (63, 126, 252)}
        for m in ASOF:
            d0 = next_td(Date.fromisoformat(m))
            hit = (px.filter(pl.col("date") == d0)
                   .join(labels.filter(pl.col("month") == m), on=C, how="semi"))
            for h in (63, 126, 252):
                vals[h] += hit[f"fwd{h}"].drop_nulls().to_list()
        line = f"{name}:"
        for h in (63, 126, 252):
            a = np.array(vals[h])
            line += f"  fwd{h} {a.mean():+.1%}(n={len(a)},敗率 {(a<0).mean():.0%})"
        print(line)


if __name__ == "__main__":
    main()
