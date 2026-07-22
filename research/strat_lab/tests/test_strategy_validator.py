from datetime import date, timedelta
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "research" / "strat_lab"))

from validator import recent_one_year_metrics, validate_daily_nav


def test_validate_daily_nav_adds_canonical_objective_metrics() -> None:
    start = date(2010, 1, 1)
    daily = pl.DataFrame(
        {
            "date": [start + timedelta(days=i) for i in range(300)],
            "nav": [1_000_000 * (1.0005 ** (i + 1)) for i in range(300)],
        }
    )

    row = validate_daily_nav("smooth", daily, n_trials=4)

    assert row["name"] == "smooth"
    assert row["oos_log_cagr"] > 0
    assert row["oos_k_ratio"] > 0
    assert "oos_ulcer_index" in row
    assert "oos_cdar_95" in row
    assert "robust_growth_score" in row
    assert row["recent_1y_cagr"] > 0


def test_recent_one_year_metrics_is_annualized_to_actual_window() -> None:
    daily = pl.DataFrame(
        {
            "date": [date(2025, 5, 15), date(2026, 5, 15)],
            "nav": [1_000_000.0, 1_500_000.0],
        }
    )

    row = recent_one_year_metrics(daily)
    expected = 1.5 ** (365.25 / 365.0) - 1.0

    assert row["recent_1y_start"] == date(2025, 5, 15)
    assert row["recent_1y_end"] == date(2026, 5, 15)
    assert abs(row["recent_1y_cagr"] - expected) < 1e-12
