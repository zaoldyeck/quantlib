"""續傳判定守護:**「已下載」必須是「覆蓋到該覆蓋的最後一天」,不是「檔案存在」。**

事故型態(2026-07-22 使用者追問「起點」時發現,尚未釀成資料缺口就先攔下):
抓取請求的區間是 [月初, min(月底, 今天)]。若某次執行發生在**月中**,該月的檔案
只覆蓋到那天為止;等日曆翻頁、這個月不再是「當月」,舊規則(只有當月才檢查新鮮度)
會直接判定完成——**那幾天的資料就永遠補不回來,而且無聲**。

  6/28 跑一次 → 2026-06 檔案覆蓋 6/01~6/28
  7/05 再跑   → 舊規則:2026-06 不是當月 → 判定完成 → 6/29、6/30 永久缺失

無聲的資料缺口是最惡劣的一種:回測照跑、數字照出,只是少了幾天,沒有人會發現。

Run: uv run --project research python -m pytest research/intraday/tests/test_resume.py
"""
from __future__ import annotations

import os
import time
from datetime import date as Date
from datetime import timedelta

import pytest

from research.intraday import pull_kbars as pk


@pytest.fixture
def out(tmp_path, monkeypatch):
    monkeypatch.setattr(pk, "OUT", tmp_path)
    return tmp_path


def _touch(out, tag: str, code: str, when: Date, suffix: str = "parquet"):
    d = out / tag
    d.mkdir(parents=True, exist_ok=True)
    f = d / f"{code}.{suffix}"
    f.write_bytes(b"x")
    ts = time.mktime(when.timetuple())
    os.utime(f, (ts, ts))
    return f


def test_missing_file_is_not_done(out) -> None:
    assert not pk._done("2026-06", "2330", Date(2026, 6, 30))


def test_file_written_after_month_end_is_done(out) -> None:
    """月結束後才抓的 → 一定涵蓋整個月 → 完成。"""
    _touch(out, "2026-06", "2330", Date(2026, 7, 3))
    assert pk._done("2026-06", "2330", Date(2026, 6, 30))


def test_file_written_on_month_end_is_done(out) -> None:
    """在該月最後一天抓的 → 請求區間就是整個月 → 完成(邊界是閉區間)。"""
    _touch(out, "2026-06", "2330", Date(2026, 6, 30))
    assert pk._done("2026-06", "2330", Date(2026, 6, 30))


def test_file_written_mid_month_is_NOT_done(out) -> None:
    """**這就是那個無聲缺口**:月中抓的檔只覆蓋到那天,不得視為完成。"""
    _touch(out, "2026-06", "2330", Date(2026, 6, 28))
    assert not pk._done("2026-06", "2330", Date(2026, 6, 30)), \
        "月中抓的檔被判為完成 → 該月剩下幾天將永遠缺失且無聲"


def test_current_month_needs_today(out) -> None:
    """當月:end = 今天。昨天抓的不算完成(今天又多了一天資料)。"""
    today = Date.today()
    tag = f"{today.year:04d}-{today.month:02d}"
    _touch(out, tag, "2330", today - timedelta(days=1))
    assert not pk._done(tag, "2330", today)
    _touch(out, tag, "2330", today)
    assert pk._done(tag, "2330", today)


def test_empty_sentinel_follows_the_same_rule(out) -> None:
    """0-byte 哨兵(該月無資料)同樣受覆蓋度規範——月中掛牌的新股會在月中才有量,
    若哨兵在那之前寫下就被永久當成「整月無資料」,等於漏掉一檔股票的上市首月。"""
    _touch(out, "2026-06", "9999", Date(2026, 6, 10), suffix="empty")
    assert not pk._done("2026-06", "9999", Date(2026, 6, 30))
    _touch(out, "2026-06", "9999", Date(2026, 7, 1), suffix="empty")
    assert pk._done("2026-06", "9999", Date(2026, 6, 30))


def test_newest_first_ordering() -> None:
    """工作序必須由近而遠——任何時刻停下來,手上都要是「從今天往回連續」的資料。"""
    ms = pk._months(pk.HIST_FLOOR, Date.today())
    ms.reverse()
    tags = [t for t, _, _ in ms]
    assert tags == sorted(tags, reverse=True), "月份序不是由新到舊"
    today = Date.today()
    assert tags[0] == f"{today.year:04d}-{today.month:02d}", \
        "起點必須是**執行當下**的月份,不得寫死"
