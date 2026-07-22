from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, os.fspath(ROOT / "research" / "strat_lab"))

from iter_54_cross_family_switch import load_switch_base  # noqa: E402


def test_limited_switch_base_preserves_full_history() -> None:
    df = load_switch_base(
        {
            "iter42_w59_champion",
            "iter44_w74_q3_trend",
            "iter52_squeeze_top5",
        }
    )

    assert df["date"][0].isoformat() == "2005-01-03"
    assert df["date"][-1].isoformat() == "2026-05-08"
    assert df.height > 5_000
    assert "ret_iter52_squeeze_top5" in df.columns
    assert "ret_iter53_lgbm_weekly_top10" not in df.columns
