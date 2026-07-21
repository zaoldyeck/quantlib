from datetime import date, timedelta
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "research" / "strat_lab"))

from pm_allocator import (
    MomentumAllocatorConfig,
    RiskBudgetAllocatorConfig,
    lagged_momentum_allocations,
    risk_budget_allocations,
    simulate_allocator_nav,
)


def test_lagged_momentum_allocator_selects_recent_winner() -> None:
    start = date(2020, 1, 1)
    dates = [start + timedelta(days=i) for i in range(12)]
    ret_panel = pl.DataFrame(
        {
            "date": dates,
            "sleeve_a": [0.01] * 12,
            "sleeve_b": [-0.005] * 12,
        }
    )

    weights = lagged_momentum_allocations(
        ret_panel,
        MomentumAllocatorConfig(lookback_days=5, top_k=1, min_score=0.0, vol_penalty=0.0),
    )

    last = weights.tail(1).row(0, named=True)
    assert last["sleeve_a__weight"] == 1.0
    assert last["sleeve_b__weight"] == 0.0


def test_simulate_allocator_nav_produces_validated_metrics() -> None:
    start = date(2010, 1, 1)
    dates = [start + timedelta(days=i) for i in range(300)]
    ret_panel = pl.DataFrame({"date": dates, "sleeve_a": [0.0005] * 300})
    weights = pl.DataFrame({"date": dates, "sleeve_a__weight": [1.0] * 300})

    daily, metrics = simulate_allocator_nav(ret_panel, weights)

    assert daily.height == 300
    assert metrics["oos_log_cagr"] > 0
    assert "robust_growth_score" in metrics


def test_risk_budget_allocator_reduces_exposure_to_drawdown_sleeve() -> None:
    start = date(2020, 1, 1)
    dates = [start + timedelta(days=i) for i in range(80)]
    ret_panel = pl.DataFrame(
        {
            "date": dates,
            "steady": [0.001] * 80,
            "drawdown": [0.004] * 40 + [-0.02] * 40,
        }
    )

    weights = risk_budget_allocations(
        ret_panel,
        RiskBudgetAllocatorConfig(
            lookback_days=30,
            min_history_days=20,
            top_k=1,
            min_score=-1.0,
            vol_penalty=0.0,
            current_dd_penalty=4.0,
            max_dd_penalty=2.0,
            target_vol=None,
        ),
    )

    last = weights.tail(1).row(0, named=True)
    assert last["steady__weight"] > 0.0
    assert last["drawdown__weight"] == 0.0
