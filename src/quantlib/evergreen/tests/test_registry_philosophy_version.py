"""registry 不得混用兩套哲學——重新蒸餾會分批重標,這是必然會踩到的。

為什麼要擋而不是警告:混版的 NAV 看起來完全正常。沒有缺值、沒有斷點,只是前半年
用一套判斷標準、後半年用另一套。任何從它算出來的 CAGR、Sharpe、置換檢定都不對應
任何一個真實可執行的策略,而且**事後從數字上完全看不出來**——這正是本專案最警惕的
那種無聲失效。

Run: uv run --project . python -m pytest src/quantlib/evergreen/tests/test_registry_philosophy_version.py
"""
from __future__ import annotations

import polars as pl
import pytest

from quantlib.evergreen.label_monthly import REGISTRY, assert_single_philosophy


def _reg(vers: list[str]) -> pl.DataFrame:
    return pl.DataFrame({"month": [f"2022-{i + 1:02d}-01" for i in range(len(vers))],
                         "code": ["1234"] * len(vers), "phil_version": vers})


def test_single_version_passes() -> None:
    assert assert_single_philosophy(_reg(["ev27", "ev27"])) == "ev27"


def test_mixed_versions_rejected() -> None:
    with pytest.raises(ValueError, match="混了 2 套哲學"):
        assert_single_philosophy(_reg(["ev27", "ev58"]))


def test_missing_column_rejected() -> None:
    """沒有欄位不能當成「沒問題」——那是無從判定,不是判定為單一版本。"""
    with pytest.raises(ValueError, match="沒有 phil_version"):
        assert_single_philosophy(pl.DataFrame({"month": ["2022-01-01"], "code": ["1234"]}))


def test_live_registry_is_single_version() -> None:
    """現役 registry 必須是單一版本——它正在被回測與 live 讀。"""
    assert assert_single_philosophy(pl.read_parquet(REGISTRY))
