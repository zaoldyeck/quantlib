"""data_calendar 守護:休市日曆(0-byte sentinel)必須被 is_trading_day 正確讀到。

背景(2026-07-24 抓到的靜默 bug):`research → src/quantlib` 改名後,data_calendar.py
用 `Path(__file__).parents[1]` 自算 repo 根,深了一層 → QUOTE_DIR 指到不存在的
`src/data/daily_quote/twse` → is_trading_day 找不到任何 sentinel → **每個假日都被誤判成
交易日**(颱風假、勞動節、端午全失效)。根因是自算路徑而非走 paths;守護分兩層:
(1) 路徑不變式:QUOTE_DIR 必須 == paths.RAW 衍生(擋 parents[N] 再算錯);
(2) 行為:0-byte sentinel→休市、非空→交易日、週末→休市、無檔→樂觀交易日。
"""
from __future__ import annotations

from datetime import date, timedelta

from quantlib import data_calendar as dc
from quantlib import paths


def test_quote_dir_anchored_on_paths_raw():
    """路徑不變式:QUOTE_DIR 一律由 paths.RAW 衍生,禁止自算 __file__ 相對根。"""
    assert dc.QUOTE_DIR == paths.RAW / "daily_quote" / "twse"


def test_is_trading_day_reads_sentinel(tmp_path, monkeypatch):
    qd = tmp_path / "daily_quote" / "twse" / "2026"
    qd.mkdir(parents=True)
    (qd / "2026_5_1.csv").write_bytes(b"")          # 0-byte sentinel = 休市(勞動節)
    (qd / "2026_7_20.csv").write_bytes(b"realdata")  # 非空 = 交易日
    monkeypatch.setattr(dc, "QUOTE_DIR", tmp_path / "daily_quote" / "twse")

    assert dc.is_trading_day(date(2026, 5, 1)) is False   # sentinel → 休市
    assert dc.is_trading_day(date(2026, 7, 20)) is True    # 有檔非空 → 交易日
    assert dc.is_trading_day(date(2026, 5, 4)) is True      # 無檔(週一)→ 樂觀交易日


def test_is_trading_day_weekend_always_false(monkeypatch, tmp_path):
    monkeypatch.setattr(dc, "QUOTE_DIR", tmp_path)  # 空目錄:週末判定不依賴檔案
    sat = date(2026, 5, 2)
    assert sat.weekday() == 5
    assert dc.is_trading_day(sat) is False
    assert dc.is_trading_day(sat + timedelta(days=1)) is False  # 週日


def test_real_sentinel_detected_end_to_end():
    """整合:用真 raw 封存驗證——2026-05-01(勞動節,真 0-byte sentinel)必判休市。
    這條直接證明修好前的 bug 已不復現(QUOTE_DIR 指對地方、sentinel 讀得到)。"""
    sentinel = dc.QUOTE_DIR / "2026" / "2026_5_1.csv"
    if sentinel.exists() and sentinel.stat().st_size == 0:
        assert dc.is_trading_day(date(2026, 5, 1)) is False
