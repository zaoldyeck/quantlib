"""F09 — ML 排名分數作 S 排位軸(預註冊見 ledger/batches.md F09 段)。

Run: uv run --project research python -m research.apex.experiments.f09_ml_axis
依賴 cache: 是 + ledger/g04_scores_fwd10.parquet。
"""
from __future__ import annotations

import polars as pl

from research.apex.experiments.f08_downmkt import (C, DEV0, DEV1, WREL, prep,
                                                   run_variant)


def main() -> None:
    panel, feat, elig = prep()
    ml = (pl.read_parquet("research/apex/ledger/g04_scores_fwd10.parquet")
          .select(["date", C, pl.col("pred").alias("ml")]))
    feat = (feat.join(ml, on=["date", C], how="left")
            .with_columns(pl.col("ml").fill_null(strategy="mean").over("date")
                          .fill_null(0.0)))
    variants = [
        ("S 基準(重現)", dict(wts=WREL)),
        ("S + ml^0.5", dict(wts={**WREL, "ml": 0.5})),
        ("S + ml^1.0", dict(wts={**WREL, "ml": 1.0})),
        ("ml 替換 mom", dict(wts={**{k: v for k, v in WREL.items()
                                     if k != "mom_126_5"}, "ml": 0.5})),
        ("ml 替換 h52", dict(wts={**{k: v for k, v in WREL.items()
                                     if k != "high_52w"}, "ml": 1.0})),
        ("ml_only(六軸全換)", dict(wts={"ml": 1.0})),
    ]
    print(f"dev 窗 {DEV0}~{DEV1};S 基準 CAGR 120.9%/P5 ~67(n_boot500)/MDD −32.6")
    for name, kw in variants:
        k = run_variant(panel, feat, elig, **kw)
        flag = "★" if (k["p5"] > 0.744 and k["mdd"] > -0.376) else " "
        print(f"{flag} {name:18s} CAGR {k['cagr']:7.1%}  P5 {k['p5']:6.1%}  "
              f"MDD {k['mdd']:6.1%}  Martin {k['martin']:5.1f}")


if __name__ == "__main__":
    main()
