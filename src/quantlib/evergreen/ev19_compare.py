"""EV19 對比:增強版(估值+財報欄+證據版判讀)vs EV17 手寫版 vs v1 切片。

同站位(月初)、同起點(asof 次一交易日)。預註冊判準(LEDGER EV19):
濾後 fwd63,增強 − 手寫 ≥ +3pp → 增強版進全量;≤ −3pp → 手寫版;
之間 → 手寫版(保守)。

Run: uv run --project . python -m quantlib.evergreen.ev19_compare
"""
from __future__ import annotations

from datetime import date as Date

import numpy as np
import polars as pl

from quantlib.apex import data

C = "company_code"
ASOF = ["2023-02-01", "2023-08-01", "2024-03-01", "2025-04-01"]


def main() -> None:
    enh = (pl.read_parquet("src/quantlib/evergreen/data/ev19_enhanced_labels.parquet")
           .select(["month", pl.col("code").alias(C), "conviction"]))
    chips = (pl.read_parquet("src/quantlib/evergreen/data/ev17_chips_labels.parquet")
             .select(["month", pl.col("code").alias(C), "conviction"]))
    v1 = (pl.read_parquet("src/quantlib/evergreen/data/registry_v1.parquet")
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

    src = {"EV19 增強": enh, "EV17 手寫": chips, "v1 切片 ": v1}
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
            agg[name]["raw"] += hit["fwd63"].drop_nulls().to_list()
            fl = flt["fwd63"].drop_nulls().to_list()
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

    diff = means["EV19 增強"] - means["EV17 手寫"]
    verdict = ("增強版進全量" if diff >= 0.03
               else "手寫版進全量" if diff <= -0.03
               else "平手帶 → 手寫版進全量(增強未證,保守)")
    print(f"\n增強 − 手寫 = {diff:+.1%} → {verdict}")


if __name__ == "__main__":
    main()
