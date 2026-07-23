"""FC1 金鑰測試:配股(除權)必須被還原,不得留下幽靈崩跌。

2026-07-23 稽核發現:prices.py 的除權息還原只認 cash_dividend > 0,於是 2,304 筆
純配股(「權」)與配股配息的股票腿整批落空 → 除權日原始收盤因配股稀釋而跳水、
卻無因子還原 → 幽靈崩跌(中位 -3.94%、最深 -23.76%)。這條直接汙染所有 NAV 回測
與 live S 的 exit_replay(用調整價判止損)。

修法:改用交易所公告的「參考價 / 除權息前收盤」當還原因子(配息+配股一體涵蓋),
對純配息事件與舊 cash 法實測僅差 4e-5,故等價且更完整。

本測試走 cache(PG 已退役 2026-07-23);cache 的 ex_right_dividend 由
quantlib.crawl.rebuild 寫入 FC1 參考價欄(closing_price_before / reference_price)。
cache 缺該欄則 skip(rebuild 未帶入,見 rebuild._EXD_KEEP)。
Run: uv run --project . python -m pytest src/quantlib/tests/test_prices_dividend_fc1.py
"""
from __future__ import annotations

import polars as pl
import pytest

from quantlib import db, prices


@pytest.fixture(scope="module")
def con():
    try:
        c = db.connect(read_only=True)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"cache 不可用:{exc}")
    # 確認 cache 已帶參考價欄(否則測不到修復)
    cols = {r[0] for r in c.sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='ex_right_dividend'").fetchall()}
    if "ex_right_ex_dividend_reference_price" not in cols:
        pytest.skip("ex_right_dividend cache 尚未帶參考價欄(rebuild 未帶入)")
    return c


def test_stock_dividend_no_phantom_crash(con) -> None:
    """6446 藥華藥 2022-09-26 純配股(參考價/前收盤 = 570.47/580 = 0.9836):
    還原後除權日的日報酬必須接近真實(|ret| < 5%),不得是配股稀釋造成的幽靈崩跌。"""
    p = prices.fetch_adjusted_panel(con, "2022-09-20", "2022-09-30",
                                    market="tpex", codes=["6446"])
    r = prices.daily_returns_from_panel(p)
    exd = r.filter(pl.col("date") == pl.date(2022, 9, 26))
    assert exd.height == 1
    ret = exd["ret"][0]
    assert abs(ret) < 0.05, f"除權日仍是幽靈崩跌 {ret:.2%}(配股未還原)"


def test_cash_dividend_factor_matches_reference_method(con) -> None:
    """等價守護:對有參考價的純配息事件,參考價法 vs 舊 cash 法差異必須極小
    (SQL 層量到平均 4e-5)。這保證切換不會動到現金股利的還原結果。"""
    diff = con.sql("""
        SELECT avg(abs(
            (ex_right_ex_dividend_reference_price / closing_price_before_ex_right_ex_dividend)
          - ((closing_price_before_ex_right_ex_dividend - cash_dividend)
             / closing_price_before_ex_right_ex_dividend)))
        FROM ex_right_dividend
        WHERE right_or_dividend='息' AND cash_dividend>0
          AND closing_price_before_ex_right_ex_dividend>0
          AND ex_right_ex_dividend_reference_price>0
    """).fetchone()[0]
    assert diff < 1e-3, f"參考價法與 cash 法對配息事件平均差異過大:{diff}"
    # 逐事件最大差異約 0.4%,那些差異處參考價法更正確(它是官方公告因子本身)


def test_all_zero_events_excluded_not_crashing(con) -> None:
    """三欄全 0 的壞列(如 6446 2025-09-11)無法算因子 → 必須被排除、不得拋、
    也不得產生離譜因子。這些列待重解析補回(FC1-parse),但在此之前不能讓程式崩。"""
    p = prices.fetch_adjusted_panel(con, "2025-09-05", "2025-09-19",
                                    market="twse", codes=["6446"])
    assert not p.is_empty()
    assert (p["adj_factor"] > 0).all()
