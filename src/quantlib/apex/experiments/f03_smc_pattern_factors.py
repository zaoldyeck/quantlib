"""F03 — SMC / 型態 / 量價分佈 proxy 單因子廣篩(預註冊見 ledger/batches.md)。

Run: uv run --project . python -m quantlib.apex.experiments.f03_smc_pattern_factors
"""
from __future__ import annotations

import time

import polars as pl

from quantlib.apex import data, factors

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
BATCH = "F03"
C = "company_code"


def win(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(
        pl.col("date").is_between(
            pl.lit(DEV_START).str.to_date(), pl.lit(DEV_END).str.to_date()
        )
    )


t0 = time.time()
con = data.connect()
panel = data.common_stocks(data.load_panel(con, DEV_START, DEV_END, warmup_days=420))
elig = win(data.eligibility(panel))
fwd = factors.forward_returns(panel)

p = (
    panel.sort([C, "date"])
    .with_columns(
        [
            (pl.col("close") / pl.col("close").shift(1) - 1).over(C).alias("ret"),
            pl.col("low").shift(3).rolling_min(60).over(C).alias("prior_low60"),
            pl.col("high").shift(5).rolling_max(60).over(C).alias("swing_high60"),
        ]
    )
    .with_columns(
        [
            # SMC:bullish fair-value gap(3-bar:今低 > 前二日高)
            (pl.col("low") > pl.col("high").shift(2)).over(C).cast(pl.Float64).alias("_fvg"),
            # SMC:liquidity sweep + 收復(3 日內破前低又收回其上)
            (
                (pl.col("low").rolling_min(3) < pl.col("prior_low60"))
                & (pl.col("close") > pl.col("prior_low60"))
            ).over(C).cast(pl.Float64).alias("_sweep"),
            # SMC:break of structure(收盤破 60d swing high)
            (pl.col("close") > pl.col("swing_high60")).cast(pl.Float64).alias("_bos"),
            # 量價分佈:對 120d VWAP 的距離(raw 價格空間)
            (
                pl.col("raw_close")
                / (
                    pl.col("trade_value").cast(pl.Float64).rolling_sum(120)
                    / pl.col("volume").cast(pl.Float64).rolling_sum(120)
                ).over(C)
                - 1
            ).alias("hvn_dist"),
            # 區間位置(TPO value-area proxy)
            (
                (pl.col("close") - pl.col("low").rolling_min(60))
                / (pl.col("high").rolling_max(60) - pl.col("low").rolling_min(60) + 1e-12)
            ).over(C).alias("range_pos_60"),
            ((pl.col("ret") > 0).cast(pl.Float64).rolling_mean(20)).over(C).alias("updays_20"),
            (
                (pl.col("open") > pl.col("high").shift(1)).cast(pl.Float64).rolling_sum(5)
            ).over(C).alias("gap_up_5"),
            # Wyckoff 吸收:日內收盤位置(一字板 → null)
            pl.when(pl.col("high") > pl.col("low"))
            .then((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low")))
            .otherwise(None)
            .alias("_clv"),
        ]
    )
    .with_columns(
        [
            pl.col("_fvg").rolling_sum(20).over(C).alias("fvg_20"),
            pl.col("_sweep").rolling_max(10).over(C).alias("sweep_rec_10"),
            pl.col("_bos").rolling_max(20).over(C).alias("bos_20"),
            pl.col("_clv").rolling_mean(20, min_samples=10).over(C).alias("close_pos_20"),
        ]
    )
)

print(f"data+signals ready in {time.time()-t0:.1f}s\n")

NAMES = [
    "fvg_20", "sweep_rec_10", "bos_20", "hvn_dist",
    "range_pos_60", "updays_20", "gap_up_5", "close_pos_20",
]
for name in NAMES:
    fac = win(p.select(["date", C, pl.col(name).alias("value")]))
    r = factors.evaluate_factor(name, fac, fwd, elig, family="smc_pattern", batch=BATCH)
    print(factors.fmt_factor(r))

print(f"\ntotal {time.time()-t0:.1f}s")
