"""Smoke tests for shared backtest engine.

驗證重構後 _engine.py 的行為跟過去手寫 simulator 一致。
跑：
    cd /Users/zaoldyeck/Documents/scala/quantlib
    uv run --project research python -m pytest research/tests/ -v
"""
from __future__ import annotations

import os
import sys
from datetime import date

import numpy as np
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.constants import CAPITAL, ROUND_TRIP_COST
from research.strat_lab._engine import (
    assert_no_weight_compound,
    compute_metrics,
    simulate_dollar_tracking,
)


# ──────────────────────────────────────────────
# compute_metrics
# ──────────────────────────────────────────────

def test_compute_metrics_zero_volatility():
    """完全平坦 NAV → vol=0, sortino=0, mdd=0."""
    days = [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)]
    nav = np.array([CAPITAL, CAPITAL, CAPITAL])
    m = compute_metrics(nav, days)
    assert m["mdd"] == 0
    assert m["vol"] == 0
    assert m["final_nav"] == CAPITAL


def test_compute_metrics_positive_cagr():
    """單調上漲 → CAGR > 0, MDD = 0."""
    days = [date(2020, 1, 1), date(2021, 1, 1)]  # 1 year
    nav = np.array([CAPITAL * 1.20, CAPITAL * 1.20])  # +20%
    m = compute_metrics(nav, days)
    assert m["cagr"] > 0.15  # 接近 +20% CAGR
    assert m["mdd"] == 0


def test_compute_metrics_with_drawdown():
    """中間有 -50% drawdown → mdd ≈ -0.5."""
    days = [date(2020, 1, 1), date(2020, 6, 1), date(2021, 1, 1)]
    nav = np.array([CAPITAL, CAPITAL * 0.5, CAPITAL * 1.1])
    m = compute_metrics(nav, days)
    assert m["mdd"] < -0.45 and m["mdd"] > -0.55


# ──────────────────────────────────────────────
# simulate_dollar_tracking — 防 weight-compound bug
# ──────────────────────────────────────────────

def test_simulator_no_weight_compound():
    """Simulator 用 dollar-tracking，**不該** 因為 sum(weights) > 1 造成虛假槓桿。

    過去 weight × ret 累乘版本會把 5% × 5% return 在 sum(w)=1.2 下變成
    1.2 × 5% = 6% portfolio return（虛假槓桿）。
    Dollar-tracking 不會有這個問題：每股獨立 × ret，總 NAV 自然加總。
    """
    days = [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)]
    # Single position 100% in '2330'
    rebal_picks = [(date(2020, 1, 1), [("2330", 1.0)])]
    # Each day '2330' returns +5%
    ret_dict = {
        (date(2020, 1, 2), "2330"): 0.05,
        (date(2020, 1, 3), "2330"): 0.05,
    }

    nav, _ = simulate_dollar_tracking(rebal_picks, ret_dict, days)

    # Day 0 (rebal): delta = |1M - 0| / 2 = 0.5M. cost = 0.5M × ROUND_TRIP_COST
    # pos = (1M - cost) × 1.0
    # Day 1: pos × 1.05
    # Day 2: pos × 1.05²
    cost = 0.5 * CAPITAL * ROUND_TRIP_COST
    expected_d2 = (CAPITAL - cost) * 1.05 * 1.05

    assert abs(nav[-1] - expected_d2) / expected_d2 < 0.001, \
        f"Simulator NAV {nav[-1]:.0f} != expected {expected_d2:.0f}"


def test_simulator_cash_buffer():
    """Empty rebal picks → all to cash buffer (0050) at first rebal day."""
    days = [date(2020, 1, 1), date(2020, 1, 2)]
    # No picks at any rebal — should default to cash
    rebal_picks = []
    # 0050 returns +1% on day 2
    ret_dict = {(date(2020, 1, 2), "0050"): 0.01}

    nav, _ = simulate_dollar_tracking(rebal_picks, ret_dict, days)

    # Cash buffer in 0050 → 1% on day 2
    assert nav[-1] > CAPITAL  # +ve due to 0050 return


# ──────────────────────────────────────────────
# assert_no_weight_compound — defensive guard
# ──────────────────────────────────────────────

def test_assert_no_weight_compound_pass():
    """Valid weights (sum ≈ 1) → no raise."""
    picks = [(date(2020, 1, 1), [("2330", 0.6), ("2308", 0.4)])]
    assert_no_weight_compound(picks)  # should not raise


def test_assert_no_weight_compound_catches_inflated():
    """Sum > 1.05 → ValueError."""
    picks = [(date(2020, 1, 1), [("2330", 0.6), ("2308", 0.6)])]  # sum 1.2
    with pytest.raises(ValueError, match="weight-compound"):
        assert_no_weight_compound(picks)


def test_assert_no_weight_compound_catches_underweight():
    """Sum < 0.95 → ValueError (除非空 list = fallback to cash)."""
    picks = [(date(2020, 1, 1), [("2330", 0.5), ("2308", 0.3)])]  # sum 0.8
    with pytest.raises(ValueError):
        assert_no_weight_compound(picks)


def test_assert_no_weight_compound_allows_empty():
    """Empty picks list = fallback to cash → OK，不該 raise."""
    picks = [(date(2020, 1, 1), [])]
    assert_no_weight_compound(picks)  # should not raise


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
