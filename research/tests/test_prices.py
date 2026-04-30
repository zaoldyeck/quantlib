"""Tests for `research/_adjusted_prices.py`.

驗證 back-adjustment 數學正確：
  1. 0050 21y cum return 跟 active_etf_metrics 的獨立實作 match (< 0.1pp drift)
  2. 最近一日 adj_close == raw_close（無未來事件）
  3. 除息日次日 adj_close 連續（無 raw 那種 div 跳空）
  4. ETF 個股有除息歷史 → adj_close < raw_close on early dates
  5. Empty universe → empty panel（不 crash）

跑：
    cd /Users/zaoldyeck/Documents/scala/quantlib
    uv run --project research python -m pytest research/tests/test_adjusted_prices.py -v
"""
from __future__ import annotations

import os
import sys

import polars as pl
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import connect
from prices import (
    daily_returns_from_panel,
    fetch_adjusted_panel,
    fetch_daily_returns,
)


@pytest.fixture(scope="module")
def con():
    c = connect()
    yield c
    c.close()


# ──────────────────────────────────────────────────────────
# active_etf_metrics 跨實作一致性
# ──────────────────────────────────────────────────────────

def _independent_total_return(con, code: str, market: str = "twse"):
    """獨立實作（從 active_etf_metrics.py 抄）— 用 forward-pass back-prop。"""
    prices = con.sql(f"""
        SELECT date, closing_price
        FROM daily_quote
        WHERE market='{market}' AND company_code='{code}' AND closing_price > 0
        ORDER BY date
    """).pl()
    divs = con.sql(f"""
        SELECT date AS ex_date, cash_dividend
        FROM ex_right_dividend
        WHERE market='{market}' AND company_code='{code}' AND cash_dividend > 0
        ORDER BY date
    """).pl()
    s = prices.with_columns(adj_close=pl.col("closing_price")).sort("date")
    for row in divs.sort("ex_date", descending=True).iter_rows(named=True):
        ex, d = row["ex_date"], row["cash_dividend"]
        pre = s.filter(pl.col("date") < ex).tail(1)
        if pre.height == 0:
            continue
        p_pre = pre["closing_price"][0]
        if p_pre <= d:
            continue
        scale = (p_pre - d) / p_pre
        s = s.with_columns(
            adj_close=pl.when(pl.col("date") < ex)
                       .then(pl.col("adj_close") * scale)
                       .otherwise(pl.col("adj_close"))
        )
    return s


def test_0050_matches_independent_implementation(con):
    """新模組跟 active_etf_metrics.py 的獨立實作對 0050 應一致到 <0.1pp。"""
    panel = fetch_adjusted_panel(con, "2003-06-30", "2026-04-25",
                                  codes=["0050"], market="twse").sort("date")
    indep = _independent_total_return(con, "0050").sort("date")

    cum_new = panel["close"][-1] / panel["close"][0] - 1
    cum_indep = indep["adj_close"][-1] / indep["adj_close"][0] - 1
    diff_pp = abs(cum_new - cum_indep) * 100
    print(f"\n0050 cum: new={cum_new*100:.2f}%, indep={cum_indep*100:.2f}%, "
          f"diff={diff_pp:.4f}pp")
    assert diff_pp < 0.1, f"0050 cum return drift {diff_pp:.4f}pp too large"


def test_2330_matches_independent_implementation(con):
    """同上對 2330（多次配息 + 配股事件）— more stress test."""
    panel = fetch_adjusted_panel(con, "2005-01-03", "2026-04-25",
                                  codes=["2330"], market="twse").sort("date")
    indep = _independent_total_return(con, "2330").sort("date")

    common_start = max(panel["date"][0], indep["date"][0])
    panel_window = panel.filter(pl.col("date") >= common_start)
    indep_window = indep.filter(pl.col("date") >= common_start)

    cum_new = panel_window["close"][-1] / panel_window["close"][0] - 1
    cum_indep = indep_window["adj_close"][-1] / indep_window["adj_close"][0] - 1
    diff_pp = abs(cum_new - cum_indep) * 100
    print(f"\n2330 cum: new={cum_new*100:.2f}%, indep={cum_indep*100:.2f}%, "
          f"diff={diff_pp:.4f}pp")
    assert diff_pp < 0.5, f"2330 cum return drift {diff_pp:.4f}pp too large"


# ──────────────────────────────────────────────────────────
# 結構性 invariants
# ──────────────────────────────────────────────────────────

def test_latest_date_adj_equals_raw(con):
    """最近一日 adj_close == raw_close（沒有未來事件）。"""
    panel = fetch_adjusted_panel(con, "2024-01-01", "2026-04-25",
                                  codes=["0050", "2330", "2317"], market="twse")
    latest = panel.sort("date").group_by("company_code").last()
    bad = latest.filter(
        ((pl.col("close") - pl.col("raw_close")).abs() > 0.001)
    )
    assert bad.is_empty(), f"latest date adj != raw for {bad['company_code'].to_list()}"


def test_adj_factor_le_one_for_dividend_stocks(con):
    """有配息歷史的 stock，早期 adj_factor 應 < 1。"""
    panel = fetch_adjusted_panel(con, "2005-01-03", "2026-04-25",
                                  codes=["0050"], market="twse").sort("date")
    early_factor = panel["adj_factor"][0]
    latest_factor = panel["adj_factor"][-1]
    assert 0 < early_factor < 1, f"0050 early factor {early_factor} should be < 1"
    assert latest_factor == pytest.approx(1.0, abs=1e-6), \
           f"0050 latest factor {latest_factor} != 1"


def test_ohlc_ratios_preserved(con):
    """high >= close >= low after adjustment（OHLC ratio 保留）。"""
    panel = fetch_adjusted_panel(con, "2024-01-01", "2026-04-25",
                                  codes=["2330"], market="twse")
    bad = panel.filter(
        (pl.col("high") < pl.col("close"))
        | (pl.col("close") < pl.col("low"))
        | (pl.col("high") < pl.col("open"))
        | (pl.col("open") < pl.col("low"))
    )
    assert bad.is_empty(), f"OHLC ratio violated on {bad.height} rows"


def test_no_dividend_jump_in_adj(con):
    """除息日 raw 會跳空，adj 不應跳空（< 5% single-day move 對大型股）。"""
    # 0050 有清楚的除息歷史
    panel = fetch_adjusted_panel(con, "2024-06-30", "2024-08-30",
                                  codes=["0050"], market="twse").sort("date")
    if panel.is_empty():
        pytest.skip("no data")
    panel = panel.with_columns([
        ((pl.col("close") / pl.col("close").shift(1)) - 1).alias("adj_ret"),
        ((pl.col("raw_close") / pl.col("raw_close").shift(1)) - 1).alias("raw_ret"),
    ])
    big_raw = panel.filter(pl.col("raw_ret").abs() > 0.025)
    # 對 big_raw 那些日期，adj_ret 應該沒這麼大
    if big_raw.is_empty():
        pytest.skip("no big-raw-move days in this window")
    for row in big_raw.iter_rows(named=True):
        # adj_ret 應該明顯小於 raw_ret（差距即是 dividend yield）
        assert abs(row["adj_ret"]) < abs(row["raw_ret"]) + 1e-6, \
               f"on {row['date']}: adj_ret {row['adj_ret']:.4f} should not exceed raw_ret {row['raw_ret']:.4f}"


# ──────────────────────────────────────────────────────────
# Edge cases
# ──────────────────────────────────────────────────────────

def test_empty_codes_returns_empty(con):
    panel = fetch_adjusted_panel(con, "2024-01-01", "2024-12-31",
                                  codes=[], market="twse")
    assert panel.is_empty()


def test_invalid_market_raises():
    with pytest.raises(ValueError):
        fetch_adjusted_panel(None, "2024-01-01", "2024-12-31", market="us")


# ──────────────────────────────────────────────────────────
# daily_returns helpers
# ──────────────────────────────────────────────────────────

def test_daily_returns_roundtrip(con):
    """compound(daily_returns) == panel close ratio (within float tolerance)."""
    panel = fetch_adjusted_panel(con, "2005-01-03", "2026-04-25",
                                  codes=["0050"], market="twse").sort("date")
    cum_panel = panel["close"][-1] / panel["close"][0] - 1
    rets = daily_returns_from_panel(panel)
    cum_compound = (1 + rets["ret"]).product() - 1
    diff_pp = abs(cum_panel - cum_compound) * 100
    assert diff_pp < 0.01, f"daily-returns roundtrip drift {diff_pp:.6f}pp"


def test_fetch_daily_returns_drop_in_replacement(con):
    """`fetch_daily_returns` returns same schema as v4.compute_daily_returns_sql.

    Expected: columns = (date, company_code, ret), no NULL ret rows.
    """
    rets = fetch_daily_returns(con, "2024-01-01", "2024-12-31",
                                codes=["0050", "2330"], market="twse")
    assert set(rets.columns) == {"date", "company_code", "ret"}
    assert rets["ret"].null_count() == 0
    assert rets["company_code"].n_unique() == 2
