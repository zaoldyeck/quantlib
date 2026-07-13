"""EV18 三方對比:蒸餾版 vs EV17 手寫版 vs registry_v1 同月切片。

三臂同站位(月初)、同起點(asof 次一交易日)、同表家族;變因:
- v1 切片:舊哲學 × 原表(無籌碼欄)
- EV17 手寫:舊哲學+手寫第七道 × 籌碼表
- EV18 蒸餾:全感官盲蒸哲學 × 籌碼表
指標同 EV16/17:fwd63 裸池 + h120>0.7 濾後。
預註冊判準(LEDGER EV18):蒸餾 vs 手寫,高者進全量;|差|<2pp 取蒸餾版。

前置:ev18_distilled_labels.parquet。
Run: uv run --project research python -m research.evergreen.ev18_compare
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
            .select(["month", pl.col("code").alias(C), "conviction"]))
    chips = (pl.read_parquet("research/evergreen/data/ev17_chips_labels.parquet")
             .select(["month", pl.col("code").alias(C), "conviction"]))
    v1 = (pl.read_parquet("research/evergreen/data/registry_v1.parquet")
          .filter(pl.col("month").is_in(ASOF))
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

    def fm(xs: list) -> str:
        return f"{np.mean(xs):+.1%}" if xs else "  na "

    src = {"EV18 蒸餾": dist, "EV17 手寫": chips, "v1 切片 ": v1}
    agg = {k: {"raw": [], "flt": []} for k in src}
    print("=== 逐月池品質(fwd63 自 asof 次一交易日;濾後 = h120>0.7)===")
    for m in ASOF:
        d0 = next_td(Date.fromisoformat(m))
        day = px.filter(pl.col("date") == d0)
        line = f"{m[:7]}:"
        for name, labels in src.items():
            sub = labels.filter(pl.col("month") == m)
            hit = day.join(sub, on=C, how="semi")
            flt = hit.filter(pl.col("h120") > 0.7)
            raw = hit["fwd63"].drop_nulls().to_list()
            fl = flt["fwd63"].drop_nulls().to_list()
            agg[name]["raw"] += raw
            agg[name]["flt"] += fl
            line += f"  {name} n={sub.height:2d} 濾後 {fm(fl)}({len(fl)})"
        print(line)
    print()
    means = {}
    for name, a in agg.items():
        raw, flt = np.array(a["raw"]), np.array(a["flt"])
        means[name] = flt.mean()
        print(f"{name} 合計:裸 {raw.mean():+.1%}(n={len(raw)},"
              f"失敗率 {(raw < 0).mean():.0%})  濾後 {flt.mean():+.1%}"
              f"(n={len(flt)},失敗率 {(flt < 0).mean():.0%})")

    diff = means["EV18 蒸餾"] - means["EV17 手寫"]
    verdict = ("蒸餾版進全量" if diff > 0.02
               else "手寫版進全量(污染嫌疑 → 議中性重跑)" if diff < -0.02
               else "平手帶 → 取蒸餾版(方法論自洽)")
    print(f"\n蒸餾 − 手寫 = {diff:+.1%} → {verdict}")


if __name__ == "__main__":
    main()
