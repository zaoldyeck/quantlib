"""Point-in-time ICT2022 / SMC / TPO-proxy features for Iter100.

This module intentionally separates strict concepts from daily proxies.  With
daily OHLCV data we can test deterministic approximations of liquidity sweeps,
market-structure shifts, fair-value gaps, displacement, compression, and value
area behavior.  Strict ICT session timing and true TPO profiles require
intraday price distribution and are not claimed here.
"""

from __future__ import annotations

import polars as pl


REQUIRED_COLUMNS = {
    "date",
    "company_code",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_value",
}


def _require_columns(frame: pl.DataFrame) -> None:
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"missing required columns for Iter100 features: {missing}")


def add_iter100_features(panel: pl.DataFrame) -> pl.DataFrame:
    """Add deterministic point-in-time daily features.

    All rolling breakout/sweep reference levels are shifted by one bar, so a
    signal at date D never uses future bars.  Current date OHLC is allowed
    because Iter100 executes at next open after signal-day close.
    """
    _require_columns(panel)
    prev_close = pl.col("close").shift(1).over("company_code")
    true_range = pl.max_horizontal(
        [
            pl.col("high") - pl.col("low"),
            (pl.col("high") - prev_close).abs(),
            (pl.col("low") - prev_close).abs(),
        ]
    )
    typical_price = (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
    body_pct = (pl.col("close") / pl.col("open").clip(1e-9, None) - 1.0)
    range_pct = ((pl.col("high") - pl.col("low")) / pl.col("close").clip(1e-9, None))

    out = (
        panel.sort(["company_code", "date"])
        .with_columns(
            [
                prev_close.alias("prev_close"),
                true_range.alias("true_range"),
                typical_price.alias("typical_price"),
                body_pct.alias("body_pct"),
                range_pct.alias("range_pct"),
                pl.col("close").rolling_max(20).over("company_code").shift(1).over("company_code").alias("swing_high20_prev"),
                pl.col("close").rolling_max(60).over("company_code").shift(1).over("company_code").alias("swing_high60_prev"),
                pl.col("close").rolling_min(20).over("company_code").shift(1).over("company_code").alias("swing_low20_prev"),
                pl.col("close").rolling_min(60).over("company_code").shift(1).over("company_code").alias("swing_low60_prev"),
                pl.col("high").shift(2).over("company_code").alias("high_2ago"),
                pl.col("low").shift(2).over("company_code").alias("low_2ago"),
                pl.col("open").shift(1).over("company_code").alias("open_1ago"),
                pl.col("close").shift(1).over("company_code").alias("close_1ago"),
                pl.col("high").shift(1).over("company_code").alias("high_1ago"),
                pl.col("low").shift(1).over("company_code").alias("low_1ago"),
            ]
        )
        .with_columns(
            [
                pl.col("true_range").rolling_mean(14).over("company_code").alias("atr14"),
                pl.col("true_range").rolling_mean(20).over("company_code").alias("atr20"),
                pl.col("trade_value").rolling_mean(20).over("company_code").alias("adv20"),
                pl.col("trade_value").rolling_mean(60).over("company_code").alias("adv60"),
                pl.col("typical_price").rolling_quantile(0.15, window_size=20).over("company_code").alias("tpo_proxy_val20"),
                pl.col("typical_price").rolling_quantile(0.50, window_size=20).over("company_code").alias("tpo_proxy_poc20"),
                pl.col("typical_price").rolling_quantile(0.85, window_size=20).over("company_code").alias("tpo_proxy_vah20"),
                pl.col("range_pct").rolling_mean(20).over("company_code").alias("range20_mean_pct"),
                pl.col("range_pct").rolling_mean(60).over("company_code").alias("range60_mean_pct"),
            ]
        )
        .with_columns(
            [
                (pl.col("low") < pl.col("swing_low20_prev")).alias("liquidity_sweep_down20"),
                (
                    (pl.col("low") < pl.col("swing_low20_prev"))
                    & (pl.col("close") > pl.col("swing_low20_prev"))
                    & (pl.col("close") > pl.col("open"))
                ).alias("liquidity_sweep_reclaim20"),
                (pl.col("close") > pl.col("swing_high20_prev")).alias("mss_up20"),
                (pl.col("close") > pl.col("swing_high60_prev")).alias("bos_up60"),
                (pl.col("low") > pl.col("high_2ago")).alias("bullish_fvg_daily_proxy"),
                (
                    (pl.col("close") > pl.col("open"))
                    & ((pl.col("close") - pl.col("open")) > pl.col("atr14").fill_null(0.0) * 0.85)
                ).alias("bullish_displacement_proxy"),
                (
                    (pl.col("close_1ago") < pl.col("open_1ago"))
                    & (pl.col("close") > pl.col("high_1ago"))
                    & ((pl.col("high_1ago") - pl.col("low_1ago")) <= pl.col("atr20").fill_null(float("inf")) * 1.50)
                ).alias("order_block_reclaim_proxy"),
                (
                    (pl.col("range20_mean_pct") < pl.col("range60_mean_pct") * 0.78)
                    & (pl.col("close") > pl.col("swing_high20_prev"))
                ).alias("compression_breakout_proxy"),
                (pl.col("close") > pl.col("tpo_proxy_vah20")).alias("tpo_proxy_acceptance_above_value"),
                (
                    (pl.col("low") < pl.col("tpo_proxy_val20"))
                    & (pl.col("close") > pl.col("tpo_proxy_poc20"))
                ).alias("tpo_proxy_rejection_from_low_value"),
            ]
        )
        .with_columns(
            [
                (
                    1.0 * pl.col("liquidity_sweep_reclaim20").cast(pl.Float64)
                    + 1.0 * pl.col("mss_up20").cast(pl.Float64)
                    + 0.8 * pl.col("bullish_fvg_daily_proxy").cast(pl.Float64)
                    + 1.0 * pl.col("bullish_displacement_proxy").cast(pl.Float64)
                    + 0.7 * pl.col("order_block_reclaim_proxy").cast(pl.Float64)
                    + 0.6 * pl.col("compression_breakout_proxy").cast(pl.Float64)
                    + 0.4 * pl.col("tpo_proxy_acceptance_above_value").cast(pl.Float64)
                    + 0.5 * pl.col("tpo_proxy_rejection_from_low_value").cast(pl.Float64)
                ).alias("iter100_structure_score")
            ]
        )
    )
    return out


def iter100_feature_columns() -> list[str]:
    """Columns generated by :func:`add_iter100_features` that may feed overlays."""
    return [
        "liquidity_sweep_down20",
        "liquidity_sweep_reclaim20",
        "mss_up20",
        "bos_up60",
        "bullish_fvg_daily_proxy",
        "bullish_displacement_proxy",
        "order_block_reclaim_proxy",
        "compression_breakout_proxy",
        "tpo_proxy_acceptance_above_value",
        "tpo_proxy_rejection_from_low_value",
        "iter100_structure_score",
    ]
