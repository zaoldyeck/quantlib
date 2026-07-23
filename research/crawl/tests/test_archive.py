"""原始檔封存守護:封存必須**位元保真**(原樣 bytes),不得 re-encode。

2026-07-23 事故(自己的 bug,正是使用者鐵律要防的):第一版 archive 整合把
http.fetch_text() 解碼後的 str 存成 UTF-8——存下來的不是原始檔,而是 re-encode 過
的版本,用原編碼(Big5-HKSCS)讀回會炸/解析出 0 列,失去災難復原保真度。

修法:save_raw 只收伺服器原樣 bytes(拒絕 str),爬蟲一律 fetch_bytes → 存 bytes →
decode → parse。本測試鎖死這條。

Run: uv run --project research python -m pytest research/crawl/tests/test_archive.py
"""
from __future__ import annotations

from datetime import date as Date

import pytest

from research.crawl import archive


@pytest.fixture
def tmp_raw(tmp_path, monkeypatch):
    monkeypatch.setattr(archive.paths, "RAW", tmp_path)
    return tmp_path


def test_save_raw_rejects_str(tmp_raw) -> None:
    """傳已解碼的 str → 拒絕(否則會 re-encode 成 UTF-8,失去原編碼)。"""
    with pytest.raises(TypeError, match="原樣 bytes"):
        archive.save_raw("daily_quote", "twse", Date(2026, 7, 20), "some,text")


def test_save_raw_is_byte_identical(tmp_raw) -> None:
    """存進去的必須與原始 bytes 逐位元相同(含 Big5-HKSCS 原編碼)。"""
    # 「證券」的 Big5-HKSCS 位元組
    original = "1101,台泥,證券".encode("Big5-HKSCS")
    p = archive.save_raw("daily_quote", "twse", Date(2026, 7, 20), original)
    assert p.read_bytes() == original, "封存的 bytes 與原始不一致(被 re-encode?)"
    # 用原編碼讀得回、內容正確
    assert p.read_bytes().decode("Big5-HKSCS") == "1101,台泥,證券"


def test_atomic_no_partial_file(tmp_raw) -> None:
    """落地是原子的:不留 .tmp 半檔。"""
    archive.save_raw("daily_quote", "twse", Date(2026, 7, 20), b"x,y,z")
    d = tmp_raw / "daily_quote" / "twse" / "2026"
    assert not list(d.glob("*.tmp")), "殘留 .tmp 半檔"
    assert (d / "2026_7_20.csv").exists()


def test_path_convention(tmp_raw) -> None:
    """路徑一律 data/<source>/<market>/<year>/<year>_<m>_<d>.<ext>(統一整理)。"""
    p = archive.raw_path("margin_transactions", "tpex", Date(2016, 1, 4))
    assert p.as_posix().endswith("margin_transactions/tpex/2016/2016_1_4.csv")


def test_sentinel_is_zero_byte(tmp_raw) -> None:
    p = archive.save_sentinel("daily_quote", "twse", Date(2026, 7, 18))
    assert p.exists() and p.stat().st_size == 0
    assert archive.has_raw("daily_quote", "twse", Date(2026, 7, 18))
