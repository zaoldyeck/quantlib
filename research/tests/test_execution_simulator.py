from __future__ import annotations

from datetime import date

import polars as pl

from research.execution import (
    ExecutionConfig,
    ExitConfig,
    FubonFeeSchedule,
    MonthlyFeeMeter,
    RealisticExecutionSimulator,
)


def test_fubon_fee_meter_splits_monthly_tiers() -> None:
    schedule = FubonFeeSchedule(minimum_commission=0.0)
    meter = MonthlyFeeMeter(schedule)

    first = meter.commission(date(2026, 5, 1), 800_000)
    second = meter.commission(date(2026, 5, 2), 500_000)

    assert round(first, 6) == round(800_000 * 0.001425 * 0.18, 6)
    assert round(second, 6) == round(
        200_000 * 0.001425 * 0.18 + 300_000 * 0.001425 * 0.40,
        6,
    )


def test_fubon_fee_meter_resets_each_month() -> None:
    meter = MonthlyFeeMeter(FubonFeeSchedule(minimum_commission=0.0))

    may = meter.commission(date(2026, 5, 31), 1_200_000)
    june = meter.commission(date(2026, 6, 1), 1_000_000)

    assert may > june
    assert round(june, 6) == round(1_000_000 * 0.001425 * 0.18, 6)


def test_realistic_simulator_lot_rounding_and_min_commission() -> None:
    bars = pl.DataFrame(
        {
            "date": [date(2026, 1, 2), date(2026, 1, 5)],
            "company_code": ["2330", "2330"],
            "open": [100.0, 110.0],
            "high": [101.0, 111.0],
            "low": [99.0, 109.0],
            "close": [110.0, 120.0],
            "volume": [10_000.0, 10_000.0],
            "trade_value": [1_000_000.0, 1_100_000.0],
            "prev_close": [None, 110.0],
            "adv60": [1_000_000.0, 1_000_000.0],
        }
    )
    config = ExecutionConfig(
        capital=100_000.0,
        lot_size=1000,
        max_participation_rate=1.0,
        fixed_slippage_bps=0.0,
        impact_bps_per_1pct_volume=0.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
    )
    result = RealisticExecutionSimulator(bars, config).simulate(
        [date(2026, 1, 2), date(2026, 1, 5)],
        {date(2026, 1, 2): {"2330": 1.0}},
    )

    fill = result.fills.row(0, named=True)
    assert fill["filled_shares"] == 0.0
    assert fill["reason"] == "insufficient_cash"


def test_realistic_simulator_blocks_limit_up_buy() -> None:
    bars = pl.DataFrame(
        {
            "date": [date(2026, 1, 5)],
            "company_code": ["2330"],
            "open": [110.0],
            "high": [110.0],
            "low": [110.0],
            "close": [110.0],
            "volume": [10_000.0],
            "trade_value": [1_100_000.0],
            "prev_close": [100.0],
            "adv60": [1_000_000.0],
        }
    )
    config = ExecutionConfig(
        capital=1_000_000.0,
        lot_size=1,
        max_participation_rate=1.0,
        fixed_slippage_bps=0.0,
        impact_bps_per_1pct_volume=0.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=0.0),
    )
    result = RealisticExecutionSimulator(bars, config).simulate(
        [date(2026, 1, 5)],
        {date(2026, 1, 5): {"2330": 1.0}},
    )

    fill = result.fills.row(0, named=True)
    assert fill["filled_shares"] == 0.0
    assert fill["reason"] == "limit_blocked"


def test_realistic_simulator_partial_volume_cap() -> None:
    bars = pl.DataFrame(
        {
            "date": [date(2026, 1, 5)],
            "company_code": ["2330"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.0],
            "volume": [1_000.0],
            "trade_value": [100_000.0],
            "prev_close": [100.0],
            "adv60": [100_000.0],
        }
    )
    config = ExecutionConfig(
        capital=1_000_000.0,
        lot_size=1,
        max_participation_rate=0.10,
        fixed_slippage_bps=0.0,
        impact_bps_per_1pct_volume=0.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=0.0),
    )
    result = RealisticExecutionSimulator(bars, config).simulate(
        [date(2026, 1, 5)],
        {date(2026, 1, 5): {"2330": 1.0}},
    )

    fill = result.fills.row(0, named=True)
    assert fill["filled_shares"] == 100.0
    assert fill["reason"] == "partial_volume_cap"


def test_realistic_simulator_executes_stop_loss_exit() -> None:
    bars = pl.DataFrame(
        {
            "date": [date(2026, 1, 5), date(2026, 1, 6)],
            "company_code": ["2330", "2330"],
            "open": [100.0, 100.0],
            "high": [102.0, 101.0],
            "low": [99.0, 93.0],
            "close": [100.0, 94.0],
            "volume": [10_000.0, 10_000.0],
            "trade_value": [1_000_000.0, 1_000_000.0],
            "prev_close": [100.0, 100.0],
            "adv60": [1_000_000.0, 1_000_000.0],
        }
    )
    config = ExecutionConfig(
        capital=100_000.0,
        lot_size=1,
        max_participation_rate=1.0,
        fixed_slippage_bps=0.0,
        impact_bps_per_1pct_volume=0.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=0.0),
        exit_config=ExitConfig(name="stop7", stop_loss_pct=0.07),
    )
    result = RealisticExecutionSimulator(bars, config).simulate(
        [date(2026, 1, 5), date(2026, 1, 6)],
        {date(2026, 1, 5): {"2330": 0.5}},
    )

    exit_fill = result.fills.filter(pl.col("reason") == "stop_loss").row(0, named=True)
    assert exit_fill["side"] == "sell"
    assert exit_fill["fill_price"] == 93.0
    assert result.daily.row(1, named=True)["active"] == 0
    assert result.trades.row(0, named=True)["exit_reason"] == "stop_loss"


def test_realistic_simulator_conservatively_prioritizes_stop_before_profit() -> None:
    bars = pl.DataFrame(
        {
            "date": [date(2026, 1, 5)],
            "company_code": ["2330"],
            "open": [100.0],
            "high": [112.0],
            "low": [94.0],
            "close": [106.0],
            "volume": [10_000.0],
            "trade_value": [1_000_000.0],
            "prev_close": [100.0],
            "adv60": [1_000_000.0],
        }
    )
    config = ExecutionConfig(
        capital=100_000.0,
        lot_size=1,
        max_participation_rate=1.0,
        fixed_slippage_bps=0.0,
        impact_bps_per_1pct_volume=0.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=0.0),
        exit_config=ExitConfig(name="stop_profit", stop_loss_pct=0.05, take_profit_pct=0.10),
    )
    result = RealisticExecutionSimulator(bars, config).simulate(
        [date(2026, 1, 5)],
        {date(2026, 1, 5): {"2330": 0.5}},
    )

    exit_fill = result.fills.filter(pl.col("side") == "sell").row(0, named=True)
    assert exit_fill["reason"] == "stop_loss"
    assert exit_fill["fill_price"] == 95.0
