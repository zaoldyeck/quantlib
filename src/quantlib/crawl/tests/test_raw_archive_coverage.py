"""守護:每個會抓網路的爬蟲源都必須封存 raw(原始檔封存鐵律)。

2026-07-23 事故:operating_revenue + capital_reduction 的 refresh()/fetch 沒封存 raw
→ 每次更新只進 cache、raw 沒留 → 破壞「cache 可從 raw 重建」不變式(paths.RAW 是
不可重生的事實地基)。本測試靜態掃 sources/*.py:凡呼叫 http.fetch_bytes/fetch_text
(=抓網路),必須也用 archive.save_raw*(或 cash_flows 的 _archive_zip 自訂原子封存)。

Run: uv run --project . python -m pytest src/quantlib/crawl/tests/test_raw_archive_coverage.py
"""
from __future__ import annotations

import pathlib

import pytest

_SOURCES = pathlib.Path(__file__).resolve().parents[1] / "sources"
#: 例外:macro 走 parquet lane(央行/NDC 指標,非 raw+cache-TABLE 源),另行驗證。
_EXEMPT = {"macro", "__init__"}
_FETCH = ("http.fetch_bytes", "http.fetch_text")
_ARCHIVE = ("archive.save_raw", "save_raw_bytes_at", "_archive_zip")


def _source_modules() -> list[str]:
    return sorted(p.stem for p in _SOURCES.glob("*.py") if p.stem not in _EXEMPT)


@pytest.mark.parametrize("src", _source_modules())
def test_fetching_source_archives_raw(src: str) -> None:
    text = (_SOURCES / f"{src}.py").read_text(encoding="utf-8")
    if not any(f in text for f in _FETCH):
        pytest.skip(f"{src} 不抓網路(無 http.fetch_*)")
    assert any(a in text for a in _ARCHIVE), (
        f"{src} 抓網路但未封存 raw —— 違反原始檔封存鐵律"
        "(先 archive.save_raw* 原子落地 data/,才 parse 進 cache)")
