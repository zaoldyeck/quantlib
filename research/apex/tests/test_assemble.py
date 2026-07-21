"""組裝層回歸測試 — 防 set_sorted 謊 flag 損壞(2026-07-09 B01 事故)。

事故:對 (code, date) 排序的 frame 呼叫 .set_sorted("date"),polars 信以為
全域有序 → over("date")/group_by/filter 走有序快徑 → 每列自成一組、rank 全 1、
top-N 過濾失效。本檔驗證 feature/score pipeline 的 date 分組型態正確。
"""
from __future__ import annotations

import os

import polars as pl
import pytest

CACHE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "cache.duckdb"
)


@pytest.mark.skipif(not os.path.exists(CACHE), reason="cache.duckdb not present")
def test_features_and_score_daily_groups_sane():
    from research.apex import data
    from research.apex.assemble import blend_score, build_features, entries_and_flags

    con = data.connect()
    panel, feat, elig = build_features(con, "2019-01-02", "2019-12-31", warmup_days=420)

    g = feat.group_by("date").len()
    assert g.height < 600, f"date 群數 {g.height} 異常(2019 全年 + 420 日曆天暖機 ≈ 527)"
    assert g["len"].median() > 200, "每日截面股數異常少 — 分組疑似損壞"
    assert feat.select(["date", "company_code"]).is_duplicated().sum() == 0

    sc = blend_score(feat, elig, {"rev_yoy_accel": 1.0, "high_52w": 1.0})
    d = sc.group_by("date").len()
    assert d["len"].median() > 100

    entries, flags = entries_and_flags(sc, 10, 40)
    per_day = entries.group_by("date").len()
    assert per_day["len"].max() <= 10, "top-N 過濾失效"
    assert flags.height > sc.height * 0.5  # rank>40 應覆蓋大多數股票
