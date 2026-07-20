"""PnL 儀表板「防凍結守護」的回歸測試。

守護 2026-07-20 定案的不變式:策略線(cache 全窗重放)末日必須貼齊 cache 最新日,
任一條落後過多就大聲炸掉、不覆蓋成過時儀表板(根因:chart 腳本把資料截止寫死成
DE="2026-07-09" 字面值,dashboard 重用時 apex_revcycle_S 線靜靜凍結)。

跑:
    cd /Users/zaoldyeck/Documents/scala/quantlib
    uv run --project research python -m pytest research/tests/test_pnl_dashboard_guard.py -v
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from research.tri.pnl_dashboard import _assert_current

END = date(2026, 7, 17)


def _series(last_day: date) -> pd.Series:
    idx = pd.to_datetime([date(2022, 7, 11), last_day])
    return pd.Series([1.0, 2.0], index=idx)


def test_guard_passes_when_all_current():
    navs = {n: _series(END) for n in
            ("Serenity(ev_v3_wf)", "Evergreen(live-refit)", "apex_revcycle_S", "0050")}
    _assert_current(navs, END)  # 全部貼齊 → 不 raise


def test_guard_raises_on_stale_strategy_line():
    navs = {"Serenity(ev_v3_wf)": _series(END),
            "Evergreen(live-refit)": _series(END),
            "apex_revcycle_S": _series(date(2026, 7, 9)),  # 凍結 8 日(正是本次事故)
            "0050": _series(END)}
    with pytest.raises(RuntimeError, match="apex_revcycle_S"):
        _assert_current(navs, END)


def test_guard_tolerates_benchmark_or_fund_lag():
    # 基金 NAV 自然落後(發布延遲)→ 只報告,不 raise
    navs = {"Serenity(ev_v3_wf)": _series(END),
            "Evergreen(live-refit)": _series(END),
            "apex_revcycle_S": _series(END),
            "安聯台灣科技基金": _series(date(2026, 7, 10))}
    _assert_current(navs, END)


def test_guard_tolerates_long_weekend():
    # 策略線落後 3 日(長週末/假期)在容忍度 4 內 → 不 raise
    navs = {n: _series(date(2026, 7, 14)) for n in
            ("Serenity(ev_v3_wf)", "Evergreen(live-refit)", "apex_revcycle_S")}
    _assert_current(navs, END)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
