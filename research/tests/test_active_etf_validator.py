from datetime import date
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "research"))
sys.path.insert(0, str(ROOT / "research" / "strat_lab"))

from active_etf_validator import compare_to_active_etfs


def test_compare_to_active_etfs_reports_losses_and_total_return_gap() -> None:
    dates = [date(2026, 1, 1), date(2026, 2, 1)]
    daily = pl.DataFrame({"date": dates, "nav": [100.0, 120.0]})
    etfs = {
        "WIN": pl.DataFrame({"date": dates, "etf_nav": [100.0, 110.0]}),
        "LOSS": pl.DataFrame({"date": dates, "etf_nav": [100.0, 130.0]}),
    }

    summary, details = compare_to_active_etfs("strategy", daily, etfs)

    assert summary.active_etf_count == 2
    assert summary.active_etf_wins == 1
    assert summary.active_etf_losses == 1
    assert summary.active_etf_loss_list == "LOSS"
    assert summary.active_etf_all_win is False
    assert abs(summary.active_etf_worst_total_return_alpha - (-0.10)) < 1e-12
    assert details.filter(pl.col("etf") == "LOSS")["win"].item() is False
