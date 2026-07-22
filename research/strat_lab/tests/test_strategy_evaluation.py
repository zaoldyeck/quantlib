from datetime import date, timedelta
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "research" / "strat_lab"))

from evaluation import nav_metrics, robust_growth_score, trade_distribution_metrics


def _daily(values: list[float]) -> pl.DataFrame:
    start = date(2020, 1, 1)
    return pl.DataFrame(
        {
            "date": [start + timedelta(days=i) for i in range(len(values))],
            "nav": values,
        }
    )


def test_nav_metrics_rewards_smooth_positive_growth() -> None:
    metrics = nav_metrics(_daily([1_001_000 + i * 1_000 for i in range(30)]))

    assert metrics["cagr"] > 0
    assert metrics["log_cagr"] > 0
    assert metrics["mdd"] == 0
    assert metrics["ulcer_index"] == 0
    assert metrics["tail_ratio"] > 0
    assert metrics["k_ratio"] > 0


def test_drawdown_metrics_capture_path_pain() -> None:
    metrics = nav_metrics(_daily([1_050_000, 800_000, 820_000, 850_000, 900_000, 1_100_000]))

    assert metrics["mdd"] < -0.20
    assert metrics["ulcer_index"] > 0.10
    assert metrics["cdar_95"] > 0.20


def test_robust_growth_score_penalizes_bad_overfit_diagnostics() -> None:
    good = {
        "oos_log_cagr": 0.20,
        "oos_calmar": 1.0,
        "oos_upi": 2.0,
        "oos_tail_ratio": 1.5,
        "oos_k_ratio": 3.0,
        "oos_mdd": -0.20,
        "oos_cdar_95": 0.15,
        "dsr": 0.95,
        "pbo": 0.10,
    }
    bad = {**good, "dsr": 0.10, "pbo": 0.49, "oos_mdd": -0.60}

    assert robust_growth_score(good) > robust_growth_score(bad)
    assert robust_growth_score(good) > 0


def test_trade_distribution_metrics_profit_factor_and_sqn() -> None:
    metrics = trade_distribution_metrics([10, 12, -3, 8, -2])

    assert metrics["trade_count"] == 5
    assert metrics["profit_factor"] > 1
    assert metrics["sqn"] > 0
