from datetime import date
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src" / "quantlib" / "strat_lab"))

from iter_40_research_campaign import CampaignConfig, build_event_candidates, build_targets


def _tie_panel() -> pl.DataFrame:
    codes = ["1007", "1003", "1001", "1010", "1002", "1009", "1005", "1008", "1004", "1006"]
    return pl.DataFrame(
        {
            "date": [date(2020, 1, 2)] * len(codes),
            "company_code": codes,
            "is_etf": [False] * len(codes),
            "is_finance": [False] * len(codes),
            "listed_days": [250] * len(codes),
            "adv60": [100_000_000.0] * len(codes),
            "open": [10.0] * len(codes),
            "close": [10.0] * len(codes),
            "rev_accel_score": [1.0] * len(codes),
        }
    )


def test_build_targets_tie_breaks_by_company_code() -> None:
    cfg = CampaignConfig(name="tie_target", family="quality_compounder", score_kind="rev_accel", topn=2, min_adv=1.0)

    targets = build_targets(_tie_panel(), [date(2020, 1, 2), date(2020, 1, 3)], cfg)

    assert list(targets[date(2020, 1, 3)]) == ["1001", "1002"]


def test_build_event_candidates_tie_breaks_by_company_code() -> None:
    cfg = CampaignConfig(name="tie_event", family="breakout", score_kind="rev_accel", topn=2, min_adv=1.0)

    candidates = build_event_candidates(_tie_panel(), cfg)

    assert candidates[date(2020, 1, 2)] == ["1001", "1002", "1003", "1004", "1005", "1006", "1007"]
