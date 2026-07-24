"""Daily multi-timeframe features from the TAIFEX RPT tick/bar data lake.

The RPT lake is independent from PostgreSQL.  This module converts already
validated intraday bars into compact daily features that can be safely joined to
the daily futures simulator.  Feature dates use `source_date`: everything from
the prior night session plus the current regular session is only usable after
that source date's close, and strategy builders still lag signals by one day.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
from quantlib import paths


REPO_ROOT = paths.REPO
DEFAULT_RPT_LAKE_DIR = REPO_ROOT / "data" / "taifex" / "rpt" / "lake"
DEFAULT_RPT_FEATURE_DIR = DEFAULT_RPT_LAKE_DIR / "features"
DEFAULT_PRODUCTS = ("TX", "MTX", "TMF", "TE", "TF")


def _feature_path(
    *,
    product: str,
    timeframe: str,
    lake_dir: Path = DEFAULT_RPT_LAKE_DIR,
    feature_dir: Path | None = None,
) -> Path:
    root = feature_dir or (lake_dir / "features")
    return root / f"product={product}" / f"timeframe={timeframe}" / "daily_features.parquet"


def _bar_glob(lake_dir: Path, timeframe: str) -> str:
    return str(lake_dir / "bars" / f"timeframe={timeframe}" / "product=*" / "year=*" / "month=*" / "*.parquet")


def build_rpt_daily_features(
    *,
    product: str = "TX",
    timeframe: str = "5m",
    lake_dir: Path = DEFAULT_RPT_LAKE_DIR,
    feature_dir: Path | None = None,
    force: bool = False,
) -> Path:
    """Build source-date daily features for one product/timeframe."""

    product = product.upper()
    out = _feature_path(product=product, timeframe=timeframe, lake_dir=lake_dir, feature_dir=feature_dir)
    if out.exists() and not force:
        return out

    bar_root = lake_dir / "bars" / f"timeframe={timeframe}"
    if not bar_root.exists():
        raise FileNotFoundError(f"missing RPT bar directory: {bar_root}")

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    con = duckdb.connect()
    try:
        con.sql(
            f"""
            COPY (
                WITH bars AS (
                    SELECT
                        product,
                        contract_month,
                        CAST(max_source_date AS DATE) AS source_date,
                        bar_start,
                        CAST(bar_start AS TIME) AS bar_time,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        tick_count
                    FROM read_parquet('{_bar_glob(lake_dir, timeframe)}', hive_partitioning=true)
                    WHERE product = '{product}'
                      AND open IS NOT NULL
                      AND close IS NOT NULL
                ),
                tagged_base AS (
                    SELECT
                        *,
                        bar_time >= TIME '08:45:00' AND bar_time <= TIME '13:45:00' AS is_regular,
                        bar_time >= TIME '15:00:00' OR bar_time < TIME '08:45:00' AS is_night,
                        bar_time >= TIME '08:45:00' AND bar_time < TIME '09:15:00' AS is_first30,
                        bar_time >= TIME '12:45:00' AND bar_time <= TIME '13:45:00' AS is_last60,
                        lag(close) OVER (
                            PARTITION BY product, contract_month, source_date
                            ORDER BY bar_start
                        ) AS prev_close
                    FROM bars
                ),
                tagged AS (
                    SELECT
                        *,
                        CASE
                            WHEN close > 0 AND prev_close > 0 THEN ln(close / prev_close)
                            ELSE NULL
                        END AS log_ret
                    FROM tagged_base
                ),
                daily AS (
                    SELECT
                        source_date AS date,
                        product,
                        contract_month,
                        min(bar_start) AS first_bar_start,
                        max(bar_start) AS last_bar_start,
                        first(open ORDER BY bar_start) AS rpt_open,
                        last(close ORDER BY bar_start) AS rpt_close,
                        max(high) AS rpt_high,
                        min(low) AS rpt_low,
                        sum(volume)::DOUBLE AS rpt_volume,
                        sum(tick_count)::DOUBLE AS rpt_tick_count,
                        sqrt(sum(CASE WHEN log_ret IS NULL OR NOT isfinite(log_ret) THEN 0 ELSE log_ret * log_ret END)) AS rpt_rv,
                        first(open ORDER BY bar_start) FILTER (WHERE is_regular) AS regular_open,
                        last(close ORDER BY bar_start) FILTER (WHERE is_regular) AS regular_close,
                        max(high) FILTER (WHERE is_regular) AS regular_high,
                        min(low) FILTER (WHERE is_regular) AS regular_low,
                        sum(volume) FILTER (WHERE is_regular)::DOUBLE AS regular_volume,
                        first(open ORDER BY bar_start) FILTER (WHERE is_night) AS night_open,
                        last(close ORDER BY bar_start) FILTER (WHERE is_night) AS night_close,
                        max(high) FILTER (WHERE is_night) AS night_high,
                        min(low) FILTER (WHERE is_night) AS night_low,
                        sum(volume) FILTER (WHERE is_night)::DOUBLE AS night_volume,
                        first(open ORDER BY bar_start) FILTER (WHERE is_first30) AS first30_open,
                        last(close ORDER BY bar_start) FILTER (WHERE is_first30) AS first30_close,
                        max(high) FILTER (WHERE is_first30) AS first30_high,
                        min(low) FILTER (WHERE is_first30) AS first30_low,
                        sum(volume) FILTER (WHERE is_first30)::DOUBLE AS first30_volume,
                        first(open ORDER BY bar_start) FILTER (WHERE is_last60) AS last60_open,
                        last(close ORDER BY bar_start) FILTER (WHERE is_last60) AS last60_close,
                        max(high) FILTER (WHERE is_last60) AS last60_high,
                        min(low) FILTER (WHERE is_last60) AS last60_low,
                        sum(volume) FILTER (WHERE is_last60)::DOUBLE AS last60_volume
                    FROM tagged
                    GROUP BY date, product, contract_month
                ),
                enriched AS (
                    SELECT
                        *,
                        rpt_close / NULLIF(rpt_open, 0) - 1.0 AS rpt_total_ret,
                        regular_close / NULLIF(regular_open, 0) - 1.0 AS rpt_regular_ret,
                        night_close / NULLIF(night_open, 0) - 1.0 AS rpt_night_ret,
                        first30_close / NULLIF(first30_open, 0) - 1.0 AS rpt_first30_ret,
                        last60_close / NULLIF(last60_open, 0) - 1.0 AS rpt_last60_ret,
                        rpt_high / NULLIF(rpt_low, 0) - 1.0 AS rpt_range_pct,
                        regular_high / NULLIF(regular_low, 0) - 1.0 AS rpt_regular_range_pct,
                        night_high / NULLIF(night_low, 0) - 1.0 AS rpt_night_range_pct,
                        regular_volume / NULLIF(rpt_volume, 0) AS rpt_regular_volume_share,
                        night_volume / NULLIF(rpt_volume, 0) AS rpt_night_volume_share,
                        first30_volume / NULLIF(regular_volume, 0) AS rpt_first30_volume_share,
                        last60_volume / NULLIF(regular_volume, 0) AS rpt_last60_volume_share
                    FROM daily
                )
                SELECT *
                FROM enriched
                ORDER BY date, contract_month
            )
            TO '{tmp}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        tmp.replace(out)
    finally:
        con.close()
        tmp.unlink(missing_ok=True)
    return out


def build_all_rpt_daily_features(
    *,
    products: tuple[str, ...] = DEFAULT_PRODUCTS,
    timeframes: tuple[str, ...] = ("5m", "15m", "60m"),
    lake_dir: Path = DEFAULT_RPT_LAKE_DIR,
    feature_dir: Path | None = None,
    force: bool = False,
) -> list[Path]:
    paths: list[Path] = []
    for product in products:
        for timeframe in timeframes:
            path = build_rpt_daily_features(
                product=product,
                timeframe=timeframe,
                lake_dir=lake_dir,
                feature_dir=feature_dir,
                force=force,
            )
            paths.append(path)
            print(f"[rpt-features] {product} {timeframe}: {path}")
    return paths


def load_rpt_daily_features(
    *,
    product: str,
    timeframe: str = "5m",
    lake_dir: Path = DEFAULT_RPT_LAKE_DIR,
    feature_dir: Path | None = None,
    force: bool = False,
) -> pl.DataFrame:
    path = build_rpt_daily_features(
        product=product,
        timeframe=timeframe,
        lake_dir=lake_dir,
        feature_dir=feature_dir,
        force=force,
    )
    prefix = f"rpt_{timeframe}_"
    base_cols = {"date", "product", "contract_month"}
    frame = pl.read_parquet(path).sort("date")
    rename = {col: prefix + col.removeprefix("rpt_") for col in frame.columns if col not in base_cols}
    return (
        frame.rename(rename)
        .with_columns(pl.col("date").cast(pl.Date))
        .sort(["date", "contract_month"])
        .unique(subset=["date", "contract_month"], keep="last", maintain_order=True)
    )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Build TAIFEX RPT daily multi-timeframe features")
    parser.add_argument("--products", default=",".join(DEFAULT_PRODUCTS))
    parser.add_argument("--timeframes", default="5m,15m,60m")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    products = tuple(item.strip().upper() for item in args.products.split(",") if item.strip())
    timeframes = tuple(item.strip() for item in args.timeframes.split(",") if item.strip())
    build_all_rpt_daily_features(products=products, timeframes=timeframes, force=args.force)


if __name__ == "__main__":
    main()
