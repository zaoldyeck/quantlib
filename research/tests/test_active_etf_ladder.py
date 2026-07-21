from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "research" / "analyses"))

from active_etf_ladder import _decision_rank  # noqa: E402


def test_decision_rank_penalizes_one_window_hot_hand() -> None:
    formal = pl.DataFrame(
        [
            {
                "rank": 2,
                "code": "STABLE",
                "name": "stable",
                "ir": 2.0,
                "alpha": 0.4,
                "active_cum": 0.2,
                "sortino": 5.0,
                "mdd": -0.1,
                "downside_capture": 0.8,
                "calmar": 20.0,
                "cum": 0.4,
                "upside_capture": 1.1,
                "annual_fee": 0.008,
                "liquidity_20d": 150_000_000.0,
                "listed_rows": 230,
            },
            {
                "rank": 1,
                "code": "HOT",
                "name": "hot",
                "ir": 2.0,
                "alpha": 0.4,
                "active_cum": 0.2,
                "sortino": 5.0,
                "mdd": -0.1,
                "downside_capture": 0.8,
                "calmar": 20.0,
                "cum": 0.4,
                "upside_capture": 1.1,
                "annual_fee": 0.008,
                "liquidity_20d": 150_000_000.0,
                "listed_rows": 70,
            },
        ]
    )
    all_ladder = pl.DataFrame(
        [
            {"cohort_min_rows": 60, "code": "HOT", "rank": 1},
            {"cohort_min_rows": 60, "code": "STABLE", "rank": 2},
            {"cohort_min_rows": 80, "code": "STABLE", "rank": 1},
            {"cohort_min_rows": 120, "code": "STABLE", "rank": 1},
            {"cohort_min_rows": 190, "code": "STABLE", "rank": 1},
            {"cohort_min_rows": 225, "code": "STABLE", "rank": 1},
        ]
    )

    ranked = _decision_rank(formal, all_ladder)

    assert ranked["code"][0] == "STABLE"
    assert ranked["stability_score"][0] > ranked["stability_score"][1]
