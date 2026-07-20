"""出場規則逐日重放的路徑語義(離線,不碰 DB)。

守護使用者定調的紀律(2026-07-16):「我就算延遲了,該賣還是得賣,不能過時間了
就當作沒發生」——出場必須逐日重放價格路徑,不能用今日快照評估。

run: uv run --project research python -m pytest research/tests/test_exit_replay.py -q
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from research.trading.exit_replay import (evergreen_rule, replay,  # noqa: E402
                                          s_rule, serenity_rule)


def path_of(closes: list[float], start: date = date(2026, 7, 1)) -> pl.DataFrame:
    days = [start + timedelta(days=i) for i in range(len(closes))]
    return pl.DataFrame({"date": days, "closing_price": closes,
                         "inst20": [None] * len(closes), "yoy3": [None] * len(closes),
                         "fresh_days": [None] * len(closes)})


def test_fired_then_recovered_still_counts():
    """核心:中途觸發、事後反彈 → 仍算觸發(快照評估會漏掉,這才是重放的意義)。"""
    # 錨 168:絕對停損 142.8。路徑 168 → 140(破)→ 170(反彈)
    p = path_of([168, 140, 170])
    fire, now = replay(p, date(2026, 7, 1), serenity_rule(168), peak_floor=168)
    assert fire is not None, "第 2 天破絕對停損,必須被記到"
    assert fire.day == date(2026, 7, 2) and fire.reason == "abs_stop"
    assert fire.price == 140
    assert now.px == 170, "今日狀態仍要回報"
    assert fire.is_overdue(date(2026, 7, 3)), "觸發日 < 今日 = 逾期未出場"


def test_snapshot_would_have_missed_it():
    """對照:只看今天(170 > 142.8)會說『續抱』——那正是要根除的偷偷放寬。"""
    p = path_of([168, 140, 170])
    _fire, now = replay(p, date(2026, 7, 1), serenity_rule(168), peak_floor=168)
    assert serenity_rule(168)(now) is None, "今日快照確實不觸發(所以不能只看快照)"


def test_peak_starts_at_entry_not_before():
    """峰值只能從進場日起算——載入窗裡的『進場前高點』不屬於這筆部位。"""
    # 進場前 1900(高點),進場後 1000 → 1000。若誤用 1900 當峰,trail 會誤觸發
    p = path_of([1900, 1000, 1000])
    fire, now = replay(p, date(2026, 7, 2), evergreen_rule(None, trail=0.4, lts=45))
    assert fire is None, "進場後從未回落 40%,不該觸發"
    assert now.peak == 1000


def test_peak_floor_is_the_fill_price():
    """成交價高於當日收盤時,峰值下限=成交價(與回測 peak_close=entry_close 對齊)。"""
    p = path_of([5295, 5300])
    _fire, now = replay(p, date(2026, 7, 1), serenity_rule(5465), peak_floor=5465)
    assert now.peak == 5465, "手動買在 5465,峰值不得低於它(否則 trail 比回測寬)"


def test_entry_day_itself_is_not_evaluated():
    """T 日收盤買進,T+1 起受管束(回測語意)。"""
    p = path_of([100, 200])  # 進場當天就 +100% 也不該當天止盈
    fire, _ = replay(p, date(2026, 7, 1), serenity_rule(100), peak_floor=100)
    assert fire is not None and fire.day == date(2026, 7, 2)


def test_first_fire_wins():
    """多門先後觸發 → 回報第一次(那才是實際會出場的時點與價格)。"""
    p = path_of([100, 84, 50])  # 第 2 天 abs(-15%)先觸發
    fire, _ = replay(p, date(2026, 7, 1), serenity_rule(100), peak_floor=100)
    assert fire.day == date(2026, 7, 2) and fire.price == 84


def test_s_signal_expiry_uses_path():
    p = path_of([100, 101, 102]).with_columns(pl.Series("fresh_days", [3, 20, 26]))
    fire, _ = replay(p, date(2026, 7, 1), s_rule(cost=100))
    assert fire is not None and "訊號過期" in fire.reason and fire.day == date(2026, 7, 3)


def test_no_fire_returns_today_state():
    p = path_of([100, 105, 110])
    fire, now = replay(p, date(2026, 7, 1), serenity_rule(100), peak_floor=100)
    assert fire is None and now.px == 110 and now.peak == 110


# ── 報告層:逾期出場必須置頂且講清楚 ──────────────────────────────
def test_action_block_surfaces_overdue():
    from research.tri.advisors import Advice
    from research.tri.report import action_block

    adv = Advice("Serenity")
    adv.detail["6274"] = {"fire_day": "2026-07-14", "fire_price": 1395.0, "px": 1435.0,
                          "fire_reason": "abs_stop", "overdue": True}
    out = action_block({"Serenity": adv}, {"6274": "台燿"})
    assert "該賣沒賣" in out and "6274 台燿" in out and "2026-07-14" in out
    assert "1,395" in out and "今天賣" in out, "必須明說:延遲不代表沒發生"


def test_action_block_clean_when_nothing_fired():
    from research.tri.advisors import Advice
    from research.tri.report import action_block

    adv = Advice("Serenity")
    adv.detail["2408"] = {"fire_day": None, "overdue": False, "px": 481.0}
    out = action_block({"Serenity": adv}, {"2408": "南亞科"})
    assert "沒有" in out and "該賣沒賣" not in out
