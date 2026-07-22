import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "research" / "strat_lab"))

from datetime import date

from iter100_objective import (
    add_iter100_objective,
    cost_below_entry_objective,
    cost_below_trade_metrics,
    portfolio_below_cost_metrics,
)


def test_above_cost_giveback_does_not_increase_loss_penalty() -> None:
    trades = pl.DataFrame(
        {
            "gross_return": [0.30, 0.15],
            "mae_pct": [0.02, 0.00],
            "mfe_pct": [0.80, 0.50],
        }
    )

    metrics = cost_below_trade_metrics(trades)

    assert metrics["trade_below_cost_mae_mean"] == 0.0
    assert metrics["trade_below_cost_mae_p95"] == 0.0
    assert metrics["trade_below_cost_penalty"] == 0.0
    assert metrics["trade_winner_giveback_above_cost_mean"] > 0.0


def test_below_cost_mae_and_duration_increase_penalty() -> None:
    shallow = pl.DataFrame(
        {
            "gross_return": [0.12, -0.02],
            "mae_pct": [-0.01, -0.03],
            "mfe_pct": [0.20, 0.03],
            "below_cost_days": [1.0, 3.0],
        }
    )
    deep = pl.DataFrame(
        {
            "gross_return": [0.12, -0.18],
            "mae_pct": [-0.01, -0.25],
            "mfe_pct": [0.20, 0.03],
            "below_cost_days": [1.0, 18.0],
        }
    )

    shallow_metrics = cost_below_trade_metrics(shallow)
    deep_metrics = cost_below_trade_metrics(deep)

    assert deep_metrics["trade_below_cost_mae_mean"] > shallow_metrics["trade_below_cost_mae_mean"]
    assert deep_metrics["trade_below_cost_days_mean"] > shallow_metrics["trade_below_cost_days_mean"]
    assert deep_metrics["trade_below_cost_penalty"] > shallow_metrics["trade_below_cost_penalty"]


def test_objective_rewards_growth_but_discounts_below_cost_risk() -> None:
    base = {
        "oos_log_cagr": 0.35,
        "recent_1y_cagr": 1.0,
        "dsr": 0.98,
        "pbo": 0.08,
        "fill_ratio": 0.99,
    }
    clean = {
        **base,
        "trade_below_cost_penalty": 0.02,
        "trade_below_cost_mae_p95": 0.05,
    }
    risky = {
        **base,
        "trade_below_cost_penalty": 0.75,
        "trade_below_cost_mae_p95": 0.30,
    }

    assert cost_below_entry_objective(clean) > cost_below_entry_objective(risky)
    assert cost_below_entry_objective(clean) > 0.0


def test_add_iter100_objective_merges_metrics() -> None:
    row = {"oos_log_cagr": 0.25, "recent_1y_cagr": 0.5, "dsr": 0.95, "pbo": 0.10, "fill_ratio": 1.0}
    trades = pl.DataFrame({"gross_return": [0.20], "mae_pct": [0.01], "mfe_pct": [0.30]})

    out = add_iter100_objective(row, trades)

    assert out["trade_count"] == 1.0
    assert "iter100_cost_below_objective" in out


def test_portfolio_below_cost_metrics_uses_all_in_buy_commission() -> None:
    fills = pl.DataFrame(
        {
            "date": [date(2024, 1, 2)],
            "company_code": ["1234"],
            "side": ["buy"],
            "filled_shares": [100.0],
            "notional": [10_000.0],
            "commission": [100.0],
        }
    )
    bars = pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "company_code": ["1234", "1234"],
            "low": [100.5, 100.2],
            "close": [101.0, 102.0],
        }
    )
    daily = pl.DataFrame({"date": [date(2024, 1, 2), date(2024, 1, 3)], "nav": [10_100.0, 10_200.0]})

    metrics = portfolio_below_cost_metrics(fills, bars, daily)

    assert metrics["portfolio_below_cost_days_ratio"] == 1.0
    assert metrics["portfolio_below_cost_ulcer"] > 0.0
    assert metrics["portfolio_below_cost_area_mean"] > 0.0


def test_portfolio_below_cost_metrics_ignores_above_all_in_cost_path() -> None:
    fills = pl.DataFrame(
        {
            "date": [date(2024, 1, 2)],
            "company_code": ["1234"],
            "side": ["buy"],
            "filled_shares": [100.0],
            "notional": [10_000.0],
            "commission": [0.0],
        }
    )
    bars = pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "company_code": ["1234", "1234"],
            "low": [100.0, 101.0],
            "close": [110.0, 105.0],
        }
    )
    daily = pl.DataFrame({"date": [date(2024, 1, 2), date(2024, 1, 3)], "nav": [11_000.0, 10_500.0]})

    metrics = portfolio_below_cost_metrics(fills, bars, daily)

    assert metrics["portfolio_below_cost_days_ratio"] == 0.0
    assert metrics["portfolio_below_cost_ulcer"] == 0.0
