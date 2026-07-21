import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from futures.simulator import FuturesExecutionConfig, simulate_single_product
from futures.specs import FuturesCostConfig, FuturesMarginConfig
from futures.strategies import StrategyCandidate, build_signal


def _bars(rows):
    return pl.DataFrame(rows).with_columns(pl.col("date").str.to_date())


def _cfg(**kwargs):
    base = dict(
        capital=1_000_000.0,
        target_vol=0.35,
        cost=FuturesCostConfig(commission_by_product={"TX": 70.0}, slippage_ticks=1.0, cost_multiplier=1.0),
        margin=FuturesMarginConfig(max_notional_leverage=6.0, required_buffer=1.35),
        stop_loss_atr=None,
        trailing_stop_atr=None,
        time_stop_days=None,
    )
    base.update(kwargs)
    return FuturesExecutionConfig(**base)


def test_futures_long_pnl_includes_multiplier_tax_commission_and_slippage():
    bars = _bars(
        [
            {"date": "2026-01-02", "product": "TX", "contract_month": "202601", "open": 1000.0, "high": 1010.0, "low": 995.0, "close": 1010.0, "atr": 100.0},
            {"date": "2026-01-03", "product": "TX", "contract_month": "202601", "open": 1010.0, "high": 1020.0, "low": 1008.0, "close": 1020.0, "atr": 100.0},
        ]
    )
    targets = pl.DataFrame({"date": ["2026-01-02", "2026-01-03"], "signal": [1.0, 1.0]}).with_columns(pl.col("date").str.to_date())
    result = simulate_single_product(bars, targets, product="TX", name="long", cfg=_cfg())
    # One TX contract: buy fill=1001, tax=1001*200*0.00002=4.004, commission=70.
    expected = 1_000_000 - 74.004 + (1010 - 1001) * 200 + (1020 - 1010) * 200
    assert result.daily["nav"][-1] == pytest.approx(expected)
    assert result.summary["max_leverage"] > 0
    assert result.summary["margin_breach"] is False


def test_futures_short_pnl_uses_signed_contracts():
    bars = _bars(
        [
            {"date": "2026-01-02", "product": "TX", "contract_month": "202601", "open": 1000.0, "high": 1002.0, "low": 990.0, "close": 990.0, "atr": 100.0},
            {"date": "2026-01-03", "product": "TX", "contract_month": "202601", "open": 990.0, "high": 992.0, "low": 980.0, "close": 980.0, "atr": 100.0},
        ]
    )
    targets = pl.DataFrame({"date": ["2026-01-02", "2026-01-03"], "signal": [-1.0, -1.0]}).with_columns(pl.col("date").str.to_date())
    result = simulate_single_product(bars, targets, product="TX", name="short", cfg=_cfg())
    assert result.daily["nav"][-1] > 1_000_000
    assert result.daily["contracts"][-1] == -1


def test_trade_pnl_records_full_round_trip_costs_on_signal_exit():
    bars = _bars(
        [
            {"date": "2026-01-02", "product": "TX", "contract_month": "202601", "open": 1000.0, "high": 1012.0, "low": 999.0, "close": 1010.0, "atr": 100.0},
            {"date": "2026-01-03", "product": "TX", "contract_month": "202601", "open": 1020.0, "high": 1022.0, "low": 1018.0, "close": 1020.0, "atr": 100.0},
        ]
    )
    targets = pl.DataFrame({"date": ["2026-01-02", "2026-01-03"], "signal": [1.0, 0.0]}).with_columns(pl.col("date").str.to_date())
    result = simulate_single_product(
        bars,
        targets,
        product="TX",
        name="round_trip",
        cfg=_cfg(cost=FuturesCostConfig(commission_by_product={"TX": 70.0}, slippage_ticks=0.0, cost_multiplier=1.0)),
    )
    entry_costs = 70.0 + 1000.0 * 200 * 0.00002
    exit_costs = 70.0 + 1020.0 * 200 * 0.00002
    expected_trade_pnl = (1020.0 - 1000.0) * 200 - entry_costs - exit_costs
    assert result.trades.height == 1
    assert result.trades["reason"][0] == "signal_exit"
    assert result.trades["pnl"][0] == pytest.approx(expected_trade_pnl)


def test_stop_loss_uses_stop_first_daily_bar_policy():
    bars = _bars(
        [
            {"date": "2026-01-02", "product": "TX", "contract_month": "202601", "open": 1000.0, "high": 1005.0, "low": 995.0, "close": 1000.0, "atr": 10.0},
            {"date": "2026-01-03", "product": "TX", "contract_month": "202601", "open": 1000.0, "high": 1100.0, "low": 970.0, "close": 1090.0, "atr": 10.0},
        ]
    )
    targets = pl.DataFrame({"date": ["2026-01-02", "2026-01-03"], "signal": [1.0, 1.0]}).with_columns(pl.col("date").str.to_date())
    result = simulate_single_product(bars, targets, product="TX", name="stop", cfg=_cfg(stop_loss_atr=2.0, take_profit_atr=5.0))
    assert "stop_loss" in result.fills["reason"].to_list()
    assert result.daily["exit_reason"].to_list()[1] == "stop_loss"


def test_strategy_signals_are_lagged_one_day():
    frame = pl.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
            "continuous_close": [100.0, 100.0, 100.0, 120.0, 121.0],
            "open": [100.0, 100.0, 100.0, 120.0, 121.0],
            "high": [101.0, 101.0, 101.0, 121.0, 122.0],
            "low": [99.0, 99.0, 99.0, 119.0, 120.0],
            "close": [100.0, 100.0, 100.0, 120.0, 121.0],
            "tx_spot_basis_pct": [0.0] * 5,
            "tx_next_term_spread_pct": [0.0] * 5,
            "foreign_tx_net_oi": [0.0] * 5,
            "te_close": [100.0] * 5,
            "tf_close": [100.0] * 5,
        }
    ).with_columns(pl.col("date").str.to_date())
    sig = build_signal(frame, StrategyCandidate("bo", "TX", "breakout", {"lookback": 2}))
    raw = sig["raw_signal"].to_list()
    lagged = sig["signal"].to_list()
    assert raw[3] == 1.0
    assert lagged[3] == 0.0
    assert lagged[4] == 1.0
