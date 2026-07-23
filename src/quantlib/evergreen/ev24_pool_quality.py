"""EV24 驗屍 2:49 個月逐月池品質(fwd63 濾後)v1 vs v2——分解標記品質
vs 引擎路徑效應。

Run: uv run --project . python -m quantlib.evergreen.ev24_pool_quality
"""
from __future__ import annotations

import numpy as np
import polars as pl

from quantlib.apex import data

C = "company_code"


def main() -> None:
    con = data.connect()
    panel = data.common_stocks(
        data.load_panel(con, "2022-01-01", "2026-07-09", warmup_days=10))
    px = (panel.sort([C, "date"])
          .with_columns([
              (pl.col("close").shift(-63) / pl.col("close") - 1)
              .over(C).alias("fwd63"),
              (pl.col("close") / pl.col("close").rolling_max(120))
              .over(C).alias("h120"),
          ]).select(["date", C, "fwd63", "h120"]))
    dates_all = panel.select("date").unique().sort("date")["date"].to_list()

    regs = {n: pl.read_parquet(f"src/quantlib/evergreen/data/registry_{n}.parquet")
            .with_columns(pl.col("month").str.slice(0, 7).alias("ym"))
            for n in ["v1", "v2"]}
    yms = sorted(regs["v1"]["ym"].unique().to_list())

    yearly = {}
    monthly_rows = []
    for ym in yms:
        y, m = int(ym[:4]), int(ym[5:7])
        from datetime import date as Date
        d0 = min((d for d in dates_all if d >= Date(y, m, 1)), default=None)
        if d0 is None:
            continue
        day = px.filter(pl.col("date") == d0)
        vals = {}
        for n, reg in regs.items():
            sub = reg.filter(pl.col("ym") == ym)
            hit = (day.join(sub.select(pl.col("code").alias(C)), on=C, how="semi")
                   .filter(pl.col("h120") > 0.7))
            f = hit["fwd63"].drop_nulls()
            vals[n] = f.mean() if f.len() else None
            yearly.setdefault((ym[:4], n), []).extend(f.to_list())
        monthly_rows.append({"ym": ym, "v1": vals["v1"], "v2": vals["v2"],
                             "diff": (vals["v2"] or 0) - (vals["v1"] or 0)})
    mdf = pl.DataFrame(monthly_rows)
    print("=== 年度池品質(濾後 fwd63 等權)===")
    for y in ["2022", "2023", "2024", "2025", "2026"]:
        a1 = np.mean(yearly.get((y, "v1"), [np.nan]))
        a2 = np.mean(yearly.get((y, "v2"), [np.nan]))
        print(f"{y}:v1 {a1:+.1%}  v2 {a2:+.1%}  diff {a2-a1:+.1%}")
    print(f"\n全期:v1 {np.mean(sum((yearly[(y,'v1')] for y in ['2022','2023','2024','2025','2026'] if (y,'v1') in yearly), [])):+.1%}"
          f"  v2 {np.mean(sum((yearly[(y,'v2')] for y in ['2022','2023','2024','2025','2026'] if (y,'v2') in yearly), [])):+.1%}")
    print("\nv2 最差 6 個月(diff):")
    with pl.Config(tbl_rows=6):
        print(mdf.sort("diff").head(6))


if __name__ == "__main__":
    main()
