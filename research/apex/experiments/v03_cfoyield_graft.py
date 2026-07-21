"""V03 — V01 晉級因子 cfo_yield × S 引擎嫁接(判準同 F08/Q03)。

cfo_yield = CFO_ttm/市值(V01 唯一晉級;cfo_ta 的估值版——分母帶價格資訊)。
變體:S+cfo_yield^0.5 第七軸、cfo_ni 閘替換為 cfo_yield 閘。
dev 窗 2019-01-02~2025-06-30;判準 P5 > 74.4 且 MDD 劣化 ≤5pp。

Run: uv run --project research python -m research.apex.experiments.v03_cfoyield_graft
依賴 cache:是。
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.apex import data
from research.apex.experiments.q03_stability_graft import (DEV0, WREL, kpis_full,
                                                           run_variant)
from research.apex.experiments import q03_stability_graft as q03

C = "company_code"


def main() -> None:
    panel, feat, elig = q03.prep()
    # cfo_yield:季報 CFO_ttm(PIT)/當日市值(raw_close × capital_stock)
    pos = lambda c: pl.when(pl.col(c) > 0).then(pl.col(c))
    td = pl.DataFrame({"td": panel.select(pl.col("date").unique().sort())
                       .get_column("date")}).sort("td")
    rq = (pl.read_parquet(data.RAW_QUARTERLY_PARQUET)
          .sort([C, "year", "quarter"])
          .with_columns(
              pl.when(pl.col("quarter") == 1).then(pl.date(pl.col("year"), 5, 15))
              .when(pl.col("quarter") == 2).then(pl.date(pl.col("year"), 8, 14))
              .when(pl.col("quarter") == 3).then(pl.date(pl.col("year"), 11, 14))
              .otherwise(pl.date(pl.col("year") + 1, 3, 31)).alias("deadline"))
          .sort("deadline")
          .join_asof(td, left_on="deadline", right_on="td", strategy="forward")
          .rename({"td": "q_avail"}).drop_nulls(subset=["q_avail"])
          .select([C, "q_avail", "cfo_ttm", "capital_stock"]).sort("q_avail"))
    feat = (feat.sort("date")
            .join_asof(rq, left_on="date", right_on="q_avail", by=C,
                       strategy="backward", tolerance="150d")
            .join(panel.select(["date", C, "raw_close"]), on=["date", C], how="left")
            .with_columns(
                (pos("cfo_ttm") / (pl.col("raw_close") * pos("capital_stock")))
                .alias("cfo_yield"))
            .sort([C, "date"]))

    def gate_cfoy(df):
        return df.filter(pl.col("cfo_yield")
                         >= pl.col("cfo_yield").median().over("date"))

    variants = [
        ("S 基準(重現驗證)", dict(wts=WREL)),
        ("S + cfo_yield^0.5", dict(wts={**WREL, "cfo_yield": 0.5})),
    ]
    print(f"dev 窗 {DEV0}~2025-06-30;S 官方基準 CAGR 120.9%/P5 74.4/MDD −32.6")
    for name, kw in variants:
        k = run_variant(panel, feat, elig, **kw)
        flag = "★" if (k["p5"] > 0.744 and k["mdd"] > -0.376) else " "
        print(f"{flag} {name:20s} CAGR {k['cagr']:7.1%}  P5 {k['p5']:6.1%}  "
              f"MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}")
    # 閘替換:cfo_ni median → cfo_yield median(run_variant gate 參數不支援
    # 自訂欄 → 就地實作:先濾 cfo_yield,再以 gate="none" 跑)
    feat_gated = feat.drop_nulls(subset=["cfo_yield"])
    feat_gated = feat_gated.join(
        gate_cfoy(feat_gated.select(["date", C, "cfo_yield"]))
        .select(["date", C]), on=["date", C], how="semi")
    k = run_variant(panel, feat_gated, elig, wts=WREL, gate="none")
    flag = "★" if (k["p5"] > 0.744 and k["mdd"] > -0.376) else " "
    print(f"{flag} {'閘替換 cfo_ni→cfo_yield':20s} CAGR {k['cagr']:7.1%}  "
          f"P5 {k['p5']:6.1%}  MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}")


if __name__ == "__main__":
    main()
