"""Pyramiding(獲利加碼)單元測試 — 先紅後綠(G51 引擎升級保護網)。

場景:單檔單調上漲,trigger=10%、frac=0.5、max=2。
驗證:觸發時點、加碼次數上限、加權平均成本、現金約束、與 None 時的不變性。
"""
from __future__ import annotations

from datetime import date as Date, timedelta

import polars as pl
import pytest

from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

XC0 = ExecSpec(commission=0.0, sell_tax=0.0, slippage=0.0, fill_at="next_close")


def _panel_mono(days: int = 40, start_px: float = 100.0, step: float = 2.0):
    d0 = Date(2024, 1, 2)
    dates, px = [], []
    d, p = d0, start_px
    while len(dates) < days:
        if d.weekday() < 5:
            dates.append(d)
            px.append(p)
            p += step
        d += timedelta(days=1)
    return pl.DataFrame({
        "date": dates, "company_code": ["1111"] * days,
        "open": px, "close": px,
    })


def _entries(panel):
    d = panel["date"][0]
    return pl.DataFrame({"date": [d], "company_code": ["1111"],
                         "score": [1.0], "weight": [0.5]})


def test_pyramid_adds_and_cap():
    panel = _panel_mono()
    res = simulate(
        panel, _entries(panel), exec_spec=XC0,
        port_spec=PortSpec(n_slots=2, capital=1_000_000.0,
                           pyramid_trigger=0.10, pyramid_max=2,
                           pyramid_frac=0.5),
        exit_spec=ExitSpec(),
    )
    tr = res.trades
    assert tr.height == 1
    row = tr.to_dicts()[0]
    # 首倉 50 萬 @102(next_close);+10% 觸發後加碼兩次(各 25 萬),第三次不加
    # 加權平均成本必高於首倉價(向上加碼)
    assert row["entry_px"] > 102.0
    # 總成本 = 50 萬 + 2 × 25 萬 = 100 萬(零費率)
    assert row["cost"] == pytest.approx(1_000_000.0, rel=1e-6)
    # NAV 全程無負現金
    assert (res.nav["cash"] >= -1e-6).all()


def test_pyramid_cash_constraint():
    panel = _panel_mono()
    res = simulate(
        panel, _entries(panel), exec_spec=XC0,
        port_spec=PortSpec(n_slots=2, capital=600_000.0,
                           pyramid_trigger=0.10, pyramid_max=5,
                           pyramid_frac=1.0),
        exit_spec=ExitSpec(),
    )
    # 首倉 30 萬;加碼單位 30 萬:現金只夠一次,第二次起餘 0 不足即停
    assert (res.nav["cash"] >= -1e-6).all()
    assert res.trades["cost"][0] <= 600_000.0 + 1e-6


def test_pyramid_off_is_bitexact():
    panel = _panel_mono()
    base = simulate(panel, _entries(panel), exec_spec=XC0,
                    port_spec=PortSpec(n_slots=2, capital=1_000_000.0),
                    exit_spec=ExitSpec(trailing_stop=0.30))
    off = simulate(panel, _entries(panel), exec_spec=XC0,
                   port_spec=PortSpec(n_slots=2, capital=1_000_000.0,
                                      pyramid_trigger=None),
                   exit_spec=ExitSpec(trailing_stop=0.30))
    assert base.nav["nav"].to_list() == off.nav["nav"].to_list()
    assert base.trades.height == off.trades.height
