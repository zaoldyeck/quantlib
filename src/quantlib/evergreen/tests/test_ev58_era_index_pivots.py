"""EV58 era-brief 轉折偵測的守護。

事故重演(2026-08-06):`zigzag` 在 `direction == 0`(尚未定向)時,
`if px > ext` 與 `elif px < ext` 兩支都可達,於是 `ext_i` 每根 K 都被推到 `i`,
`drop` 恆為 0 → 狀態機永遠停在 direction 0、一個轉折都吐不出來。
實測:TAIEX 2015-06-01~2017-12-31 含一段 -23% 的崩跌,舊碼回傳空 list。

macro_timeline 要求「區間內每一次指數的顯著轉折都要有一條」,轉折表恆空
=== 研究員只能憑印象寫時間軸,正是 era_brief 明令禁止的事。故立此守護。
"""

from __future__ import annotations

import polars as pl
import pytest

from quantlib.evergreen.ev58_era_index_pivots import zigzag


def _frame(closes: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [f"2020-01-{i + 1:02d}" for i in range(len(closes))],
            "close": closes,
        }
    )


def test_single_peak_then_trough_is_detected() -> None:
    """先漲 20% 再跌 20%:必須吐出一個 peak(頂點錨在最高那根)。"""
    closes = [100.0, 105.0, 110.0, 115.0, 120.0, 110.0, 100.0, 96.0]
    pivots = zigzag(_frame(closes), threshold=0.05)

    assert pivots, "含 -20% 反轉的序列不該回傳空轉折表"
    peak = pivots[0]
    assert peak.kind == "peak"
    assert peak.close == 120.0
    assert peak.date == "2020-01-05"
    # leg 自視窗起點 100 量到 120
    assert peak.move_pct == pytest.approx(0.20)


def test_alternating_peaks_and_troughs() -> None:
    """完整的 上→下→上:peak 與 trough 必須交替出現且錨在真正的極值。"""
    closes = (
        [100.0, 110.0, 120.0]  # 漲
        + [110.0, 100.0, 90.0]  # 跌 -25%
        + [100.0, 110.0, 120.0]  # 再漲 +33%
    )
    pivots = zigzag(_frame(closes), threshold=0.05)

    kinds = [p.kind for p in pivots]
    assert kinds == ["peak", "trough"], kinds
    assert [p.close for p in pivots] == [120.0, 90.0]
    # leg 幅度:第一段自序列起點 100 → 120
    assert pivots[0].move_pct == pytest.approx(0.20)
    # 第二段自 peak 120 → trough 90
    assert pivots[1].move_pct == pytest.approx(-0.25)


def test_first_leg_down_is_detected() -> None:
    """區間一開始就往下:第一個轉折是那個低點,leg 自視窗起點量起。"""
    closes = [100.0, 95.0, 90.0, 80.0, 88.0, 95.0]
    pivots = zigzag(_frame(closes), threshold=0.05)

    assert pivots, "開頭即下跌的序列不該回傳空轉折表"
    assert pivots[0].kind == "trough"
    assert pivots[0].close == 80.0
    assert pivots[0].move_pct == pytest.approx(-0.20)


def test_window_edge_is_not_reported_as_a_pivot() -> None:
    """視窗第一根不得當成轉折輸出。

    它的 leg 恆為 0%,且真正的轉折可能落在視窗之前——輸出它會讓下游把
    「區間起點」誤讀成「當年的高/低點」寫進 macro_timeline。
    """
    # 單調上漲後回落:index 0 是第一段的起點低點,但它是切邊不是轉折
    closes = [100.0, 106.0, 112.0, 120.0, 110.0]
    pivots = zigzag(_frame(closes), threshold=0.05)

    assert all(p.date != "2020-01-01" for p in pivots), [
        (p.date, p.kind) for p in pivots
    ]
    assert all(p.move_pct != 0.0 for p in pivots)
    assert [(p.kind, p.close) for p in pivots] == [("peak", 120.0)]


def test_noise_below_threshold_yields_no_pivot() -> None:
    """波動全部小於門檻:不得產生轉折(避免把雜訊當事件)。"""
    closes = [100.0, 101.0, 100.0, 101.5, 100.5, 102.0, 101.0]
    assert zigzag(_frame(closes), threshold=0.05) == []


def test_empty_frame() -> None:
    assert zigzag(pl.DataFrame({"date": [], "close": []}), threshold=0.05) == []
