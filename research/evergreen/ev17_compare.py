"""EV17 對比:升級表(v1 舊紀律+籌碼欄)vs registry_v1 同月切片。

兩臂同紀律(EV3 原版 prompt)、同站位(月初)、同起點(asof 次一交易日),
唯一變因 = 表尾 5 欄籌碼。指標同 EV16:fwd63 裸池 + h120>0.7 濾後。
預註冊見 LEDGER EV17(判準:濾後合計差 ≥+3pp → 籌碼欄進全量配方)。

前置:ev17_chips_labels.parquet(由 workflow output 落盤)。
Run: uv run --project research python -m research.evergreen.ev17_compare
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from research.apex import data

C = "company_code"
ASOF = ["2023-02-01", "2023-08-01", "2024-03-01", "2025-04-01"]


def main() -> None:
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

    agg = {"升級表(+籌碼)": {"raw": [], "flt": []},
           "v1 切片(原表)": {"raw": [], "flt": []}}
    src = {"升級表(+籌碼)": chips, "v1 切片(原表)": v1}
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
            line += (f"  {name} n={sub.height:2d} 裸 {fm(raw)}"
                     f" 濾後 {fm(fl)}({len(fl)})")
        print(line)
    print()
    means = {}
    for name, a in agg.items():
        raw, flt = np.array(a["raw"]), np.array(a["flt"])
        means[name] = flt.mean()
        print(f"{name} 合計:裸 {raw.mean():+.1%}(n={len(raw)},"
              f"失敗率 {(raw < 0).mean():.0%})  濾後 {flt.mean():+.1%}"
              f"(n={len(flt)},失敗率 {(flt < 0).mean():.0%})")

    kc = set(zip(chips["month"], chips[C]))
    kv = set(zip(v1["month"], v1[C]))
    print(f"\n重疊 {len(kc & kv)};升級表獨有 {len(kc - kv)};"
          f"v1 獨有 {len(kv - kc)}")

    diff = means["升級表(+籌碼)"] - means["v1 切片(原表)"]
    verdict = ("籌碼欄進全量配方" if diff >= 0.03
               else "籌碼欄砍掉" if diff <= -0.03
               else "平手帶 → 不加(省表面積)")
    print(f"\n濾後合計差(升級−原)= {diff:+.1%} → {verdict}")


if __name__ == "__main__":
    main()
