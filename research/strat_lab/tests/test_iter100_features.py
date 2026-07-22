from datetime import date, timedelta
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "research" / "strat_lab"))

from iter100_features import add_iter100_features, iter100_feature_columns


def _panel(closes: list[float] | None = None) -> pl.DataFrame:
    closes = closes or [100 + i for i in range(80)]
    rows = []
    start = date(2024, 1, 1)
    for i, close in enumerate(closes):
        rows.append(
            {
                "date": start + timedelta(days=i),
                "company_code": "1234",
                "open": close * 0.99,
                "high": close * 1.01,
                "low": close * 0.98,
                "close": close,
                "volume": 1000.0,
                "trade_value": close * 1000.0,
            }
        )
    return pl.DataFrame(rows)


def test_iter100_features_requires_source_columns() -> None:
    with pytest.raises(ValueError):
        add_iter100_features(pl.DataFrame({"date": [date(2024, 1, 1)]}))


def test_bullish_fvg_daily_proxy_uses_two_bars_back_only() -> None:
    frame = _panel([100.0] * 31)
    frame = frame.with_columns(
        [
            pl.when(pl.arange(0, pl.len()) == 28).then(100.0).otherwise(pl.col("high")).alias("high"),
            pl.when(pl.arange(0, pl.len()) == 30).then(105.0).otherwise(pl.col("low")).alias("low"),
            pl.when(pl.arange(0, pl.len()) == 30).then(106.0).otherwise(pl.col("close")).alias("close"),
        ]
    )

    out = add_iter100_features(frame)
    row = out.filter(pl.col("date") == date(2024, 1, 31)).row(0, named=True)

    assert row["bullish_fvg_daily_proxy"] is True


def test_liquidity_sweep_reclaim_detects_sweep_and_close_reclaim() -> None:
    closes = [100.0] * 41
    frame = _panel(closes)
    target_date = date(2024, 2, 10)
    frame = frame.with_columns(
        [
            pl.when(pl.col("date") == target_date).then(94.0).otherwise(pl.col("low")).alias("low"),
            pl.when(pl.col("date") == target_date).then(101.0).otherwise(pl.col("close")).alias("close"),
            pl.when(pl.col("date") == target_date).then(96.0).otherwise(pl.col("open")).alias("open"),
        ]
    )

    out = add_iter100_features(frame)
    row = out.filter(pl.col("date") == target_date).row(0, named=True)

    assert row["liquidity_sweep_down20"] is True
    assert row["liquidity_sweep_reclaim20"] is True


def test_feature_columns_are_present() -> None:
    out = add_iter100_features(_panel())

    for column in iter100_feature_columns():
        assert column in out.columns
