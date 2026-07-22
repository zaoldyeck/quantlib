"""測試用的環境探測:**這台機器有沒有跑這支測試所需的資料?**

為什麼需要它:同一份程式碼跑在兩種環境——
  本機   全表 cache(2.2 GB,2003 年起)+ 完整回測產物
  雲端 VM 瘦身 cache(206 MB,只留 S 決策要的近年)+ 無回測產物

需要長歷史的測試在 VM 上必然失敗。**讓它紅是錯的**:那不是缺陷,是這台機器
本來就沒有那份資料;而假紅會訓練人忽略紅燈,等真的壞掉時沒有人會停下來看。
正解是**誠實地 skip 並說出原因**。

用法:
    pytestmark = testkit.requires_history("2019-01-02", "2019-12-31")
    pytestmark = testkit.requires_file(paths.OUT_STRAT_LAB / "xxx.csv")
"""
from __future__ import annotations

import functools
from pathlib import Path

import pytest

from research import paths


@functools.lru_cache(maxsize=1)
def cache_span() -> tuple[str, str] | None:
    """cache 內 daily_quote 的日期範圍;cache 不存在或讀不到回 None。"""
    if not paths.CACHE_DB.exists():
        return None
    try:
        import duckdb
        con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
        try:
            lo, hi = con.execute(
                "SELECT min(date), max(date) FROM daily_quote").fetchone()
        finally:
            con.close()
    except Exception:                      # noqa: BLE001 - 探測失敗即視為沒有
        return None
    return (str(lo), str(hi)) if lo and hi else None


def requires_history(start: str, end: str):
    """需要 cache 覆蓋 [start, end] 才跑;否則 skip 並說明實際覆蓋範圍。"""
    span = cache_span()
    if span is None:
        return pytest.mark.skip(reason=f"{paths.CACHE_DB.name} 不存在或無 daily_quote")
    lo, hi = span
    ok = lo <= start and hi >= end
    return pytest.mark.skipif(
        not ok, reason=f"本機 cache 只覆蓋 {lo}~{hi},此測試需要 {start}~{end}"
                       "(雲端 VM 用瘦身 cache 屬正常)")


def requires_file(path: Path, what: str = "回測產物"):
    """需要某份產物才跑(例如回測輸出);缺就 skip,不當成缺陷。"""
    return pytest.mark.skipif(
        not Path(path).exists(),
        reason=f"缺{what} {Path(path).name}(可重生;此環境未產生過)")
