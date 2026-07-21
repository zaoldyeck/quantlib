"""apex 引擎 golden tests — 買進持有 parity、成本精算、漲跌停擋單、T+1、
trailing stop、下市清算、零槓桿。全部合成 panel,外加一個 cache 整合測試。

執行:uv run --project research python -m pytest research/apex/tests -q
"""
from __future__ import annotations

import os
from datetime import date as Date, timedelta

import polars as pl
import pytest

from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

ZERO_COST = ExecSpec(commission=0.0, sell_tax=0.0, slippage=0.0, fill_at="next_close")
COST = ExecSpec(commission=0.001, sell_tax=0.003, slippage=0.002, fill_at="next_close")


def weekdays(start: Date, n: int) -> list[Date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def make_panel(paths: dict[str, list], start: Date = Date(2020, 1, 6)) -> pl.DataFrame:
    """paths: code → list of close 或 (open, close)。open 未指定時 = close。"""
    rows = []
    for code, seq in paths.items():
        for d, x in zip(weekdays(start, len(seq)), seq):
            o, c = (x, x) if isinstance(x, (int, float)) else x
            rows.append(
                {
                    "market": "twse", "date": d, "company_code": code,
                    "open": float(o), "high": float(max(o, c)), "low": float(min(o, c)),
                    "close": float(c), "raw_close": float(c), "adj_factor": 1.0,
                    "volume": 1_000_000, "trade_value": 100_000_000.0,
                }
            )
    return pl.DataFrame(rows)


def entries_at(code: str, dates: list[Date], score: float = 1.0) -> pl.DataFrame:
    return pl.DataFrame(
        {"date": dates, "company_code": [code] * len(dates), "score": [score] * len(dates)}
    )


def test_cost_accounting_exact():
    """單筆完整回合的現金流必須逐分錢對帳。"""
    days = weekdays(Date(2020, 1, 6), 6)
    panel = make_panel({"1101": [100, 100, 100, 100, 110, 110]})
    res = simulate(
        panel,
        entries_at("1101", [days[0]]),
        exec_spec=COST,
        port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
        exit_spec=ExitSpec(time_stop=2),
    )
    # 買:d1 @100;time_stop=2 於 d3 決策(held=2)→ d4 @110 賣
    n = 1_000_000.0 / (1 + COST.commission)
    shares = n / (100 * (1 + COST.slippage))
    cash_after_buy = 1_000_000.0 - n * (1 + COST.commission)
    proceeds = shares * 110 * (1 - COST.slippage) * (1 - COST.commission - COST.sell_tax)
    expected_final = cash_after_buy + proceeds

    t = res.trades.to_dicts()
    assert len(t) == 1
    assert t[0]["exit_reason"] == "time"
    assert t[0]["entry_date"] == days[1] and t[0]["exit_date"] == days[4]
    assert abs(res.nav["nav"][-1] - expected_final) < 1e-6
    assert abs(t[0]["ret_net"] - (proceeds / (n * (1 + COST.commission)) - 1)) < 1e-12


def test_limit_up_buy_blocked_no_retry():
    """成交日 +10%(2015-06 後制度)→ 買單擋掉且不重試。"""
    days = weekdays(Date(2020, 1, 6), 4)
    panel = make_panel({"1101": [100, 110, 112, 113]})
    res = simulate(
        panel, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    assert res.trades.height == 0
    assert res.nav["nav"][-1] == pytest.approx(1_000_000.0)
    # 隔日再發訊號 → d2 成交(+1.8% 不擋)
    res2 = simulate(
        panel, entries_at("1101", [days[0], days[1]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    t = res2.trades.to_dicts()
    assert len(t) == 1 and t[0]["entry_date"] == days[2] and t[0]["entry_px"] == 112


def test_limit_7pct_era():
    """2015-06-01 前漲跌幅 7%:+7% 成交日應被擋。"""
    days = weekdays(Date(2014, 3, 3), 3)
    panel = make_panel({"1101": [100, 107, 108]}, start=days[0])
    res = simulate(
        panel, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    assert res.trades.height == 0


def test_limit_down_sell_retries_until_filled():
    """跌停鎖死賣不掉 → 自動隔日重試;出場理由保留最初觸發原因。"""
    days = weekdays(Date(2020, 1, 6), 5)
    panel = make_panel({"1101": [100, 100, 89.5, 80.6, 85]})
    res = simulate(
        panel, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST,
        port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
        exit_spec=ExitSpec(abs_stop=0.05),
    )
    t = res.trades.to_dicts()
    # d2 決策觸發 abs_stop → d3 成交日 80.6/89.5-1 = -9.94% 被擋 → d4 成交 @85
    assert len(t) == 1
    assert t[0]["exit_reason"] == "abs_stop"
    assert t[0]["exit_date"] == days[4]
    assert t[0]["exit_px"] == 85


def test_t1_next_open_fill():
    """fill_at=next_open:T 決策 → T+1 開盤價成交(含滑價)。"""
    days = weekdays(Date(2020, 1, 6), 3)
    panel = make_panel({"1101": [(100, 100), (101, 103), (104, 105)]})
    res = simulate(
        panel, entries_at("1101", [days[0]]),
        exec_spec=ExecSpec(commission=0.0, sell_tax=0.0, slippage=0.002, fill_at="next_open"),
        port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    t = res.trades.to_dicts()  # 期末未平倉 → exit_reason="open"
    assert len(t) == 1 and t[0]["exit_reason"] == "open"
    assert t[0]["entry_px"] == pytest.approx(101 * 1.002)


def test_trailing_stop_from_peak():
    days = weekdays(Date(2020, 1, 6), 6)
    panel = make_panel({"1101": [100, 100, 120, 95, 95, 95]})
    res = simulate(
        panel, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST,
        port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
        exit_spec=ExitSpec(trailing_stop=0.20),
    )
    t = res.trades.to_dicts()
    # peak=120(d2);d3 收 95 → 95/120-1=-20.8% 觸發 → d4 成交
    assert len(t) == 1
    assert t[0]["exit_reason"] == "trail"
    assert t[0]["exit_date"] == days[4]


def test_delist_forced_liquidation():
    days = weekdays(Date(2020, 1, 6), 8)
    panel = make_panel({"1101": [100, 100, 100], "2330": [50] * 8})
    res = simulate(
        panel, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    t = res.trades.to_dicts()
    assert len(t) == 1
    assert t[0]["exit_reason"] == "delist"
    assert t[0]["exit_date"] == days[2]  # 最後一根 bar 當日收盤清算
    assert res.nav["nav"][-1] == pytest.approx(1_000_000.0)


def test_no_leverage_cash_constrained():
    """兩檔同日進場,第二筆被現金約束縮小;全程 cash ≥ 0。"""
    days = weekdays(Date(2020, 1, 6), 4)
    panel = make_panel({"1101": [100] * 4, "2330": [50] * 4})
    res = simulate(
        panel,
        pl.concat([entries_at("1101", [days[0]], 2.0), entries_at("2330", [days[0]], 1.0)]),
        exec_spec=COST,
        port_spec=PortSpec(n_slots=2, capital=1_000_000.0),
    )
    assert res.nav["n_pos"][-1] == 2
    assert (res.nav["cash"] >= -1e-6).all()
    assert (res.nav["invested"] <= res.nav["nav"] + 1e-6).all()


def test_eligibility_filters_entries():
    days = weekdays(Date(2020, 1, 6), 4)
    panel = make_panel({"1101": [100] * 4})
    elig = pl.DataFrame(
        {"date": [days[0]], "company_code": ["1101"], "eligible": [False]}
    )
    res = simulate(
        panel, entries_at("1101", [days[0]]), eligibility=elig,
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    assert res.trades.height == 0


def test_signal_exit_flag():
    days = weekdays(Date(2020, 1, 6), 6)
    panel = make_panel({"1101": [100, 100, 101, 102, 103, 104]})
    flags = pl.DataFrame({"date": [days[3]], "company_code": ["1101"]})
    res = simulate(
        panel, entries_at("1101", [days[0]]), exit_flags=flags,
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    t = res.trades.to_dicts()
    assert len(t) == 1
    assert t[0]["exit_reason"] == "signal"
    assert t[0]["exit_date"] == days[4]


def test_asymmetric_exits():
    """非對稱出場:水下用較緊 trail;輸家較短時間止損;贏家不受影響。"""
    days = weekdays(Date(2020, 1, 6), 8)
    # 水下 trail:進場 100,峰 100,跌到 84(-16%,水下)→ underwater_trail 15% 觸發
    panel = make_panel({"1101": [100, 100, 95, 84, 84, 84, 84, 84]})
    res = simulate(
        panel, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
        exit_spec=ExitSpec(trailing_stop=0.35, underwater_trail=0.15),
    )
    t = res.trades.to_dicts()
    assert len(t) == 1 and t[0]["exit_reason"] == "trail" and t[0]["exit_date"] == days[4]

    # 輸家時間止損:水下 3 日即出;贏家用 time_stop 6 日
    panel2 = make_panel({"1101": [100, 100, 99, 98, 98, 98, 98, 98]})
    res2 = simulate(
        panel2, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
        exit_spec=ExitSpec(time_stop=6, loser_time_stop=3),
    )
    t2 = res2.trades.to_dicts()
    assert len(t2) == 1 and t2[0]["exit_reason"] == "time_loser"
    assert t2[0]["exit_date"] == days[5]  # entry d1;決策 t=d4(held=3)→ 成交 d5
    # 上行版本不觸發 loser stop(time_stop=5:t=d6 決策 → d7 成交,仍在 8 根內)
    panel3 = make_panel({"1101": [100, 100, 105, 110, 115, 120, 125, 130]})
    res3 = simulate(
        panel3, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
        exit_spec=ExitSpec(time_stop=5, loser_time_stop=3),
    )
    t3 = res3.trades.to_dicts()
    assert len(t3) == 1 and t3[0]["exit_reason"] == "time"


def test_same_day_exit_threshold():
    """same_day_exit:trailing 觸發當日收盤即賣(vs 預設隔日);signal 型仍隔日。"""
    days = weekdays(Date(2020, 1, 6), 6)
    # 單日跌幅皆在跌停 buffer 內(單日 −20.8% 會被 sd_sell_block 正確判定鎖死、遞延隔日)
    panel = make_panel({"1101": [100, 100, 120, 110, 100, 94]})
    res = simulate(
        panel, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
        exit_spec=ExitSpec(trailing_stop=0.20, same_day_exit=True),
    )
    t = res.trades.to_dicts()
    assert len(t) == 1 and t[0]["exit_reason"] == "trail"
    assert t[0]["exit_date"] == days[5]      # 當日(94/120−1=−21.7% 破線)收盤賣
    assert t[0]["exit_px"] == 94
    # signal 型仍隔日
    flags = pl.DataFrame({"date": [days[2]], "company_code": ["1101"]})
    res2 = simulate(
        panel, entries_at("1101", [days[0]]), exit_flags=flags,
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
        exit_spec=ExitSpec(same_day_exit=True),
    )
    t2 = res2.trades.to_dicts()
    assert len(t2) == 1 and t2[0]["exit_reason"] == "signal"
    assert t2[0]["exit_date"] == days[3]     # flag 於 d2,隔日 d3 成交


def test_weighted_sizing_exact():
    """entries 帶 weight 欄:倉位 = NAV × weight,現金約束保證零槓桿。"""
    days = weekdays(Date(2020, 1, 6), 4)
    panel = make_panel({"1101": [100] * 4, "2330": [50] * 4})
    entries = pl.DataFrame(
        {"date": [days[0], days[0]], "company_code": ["1101", "2330"],
         "score": [2.0, 1.0], "weight": [0.30, 0.10]}
    )
    res = simulate(
        panel, entries,
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=2, capital=1_000_000.0),
    )
    nav = res.nav
    assert nav["n_pos"][-1] == 2
    assert nav["invested"][-1] == pytest.approx(1_000_000.0 * 0.40)
    assert nav["cash"][-1] == pytest.approx(1_000_000.0 * 0.60)
    t = {r["company_code"]: r for r in res.trades.to_dicts()}
    assert t["1101"]["cost"] == pytest.approx(300_000.0)
    assert t["2330"]["cost"] == pytest.approx(100_000.0)


def test_next_mid_fill_price():
    """next_mid:成交價 = (O+C)/2 ×(1+滑價)。"""
    days = weekdays(Date(2020, 1, 6), 3)
    panel = make_panel({"1101": [(100, 100), (101, 105), (104, 105)]})
    res = simulate(
        panel, entries_at("1101", [days[0]]),
        exec_spec=ExecSpec(commission=0.0, sell_tax=0.0, slippage=0.002, fill_at="next_mid"),
        port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    t = res.trades.to_dicts()
    assert len(t) == 1
    assert t[0]["entry_px"] == pytest.approx(0.5 * (101 + 105) * 1.002)


def test_exact_lock_blocks_only_locked_limit():
    """E01:有掛單資料時,+10% 收盤「未鎖死」可買、「鎖死」不可買。"""
    days = weekdays(Date(2020, 1, 6), 4)
    base = make_panel({"1101": [100, 110, 112, 113]})
    unlocked = base.with_columns(
        [pl.lit(False).alias("ask_missing"), pl.lit(False).alias("bid_missing")]
    )
    res_u = simulate(
        unlocked, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    assert res_u.trades.height == 1  # 未鎖死 → 成交
    assert res_u.trades.to_dicts()[0]["entry_px"] == 110

    locked = base.with_columns(
        [
            (pl.col("date") == days[1]).alias("ask_missing"),
            pl.lit(False).alias("bid_missing"),
        ]
    )
    res_l = simulate(
        locked, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    assert res_l.trades.height == 0  # 鎖死 → 擋單


def test_exact_lock_sell_retry_on_locked_limit_down():
    """E01:跌停鎖死賣不掉自動重試;未鎖死跌停可賣。"""
    days = weekdays(Date(2020, 1, 6), 5)
    base = make_panel({"1101": [100, 100, 89.5, 80.6, 85]})
    locked = base.with_columns(
        [
            pl.lit(False).alias("ask_missing"),
            (pl.col("date") == days[3]).alias("bid_missing"),  # d3 跌停鎖死
        ]
    )
    res = simulate(
        locked, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
        exit_spec=ExitSpec(abs_stop=0.05),
    )
    t = res.trades.to_dicts()
    assert len(t) == 1 and t[0]["exit_date"] == days[4] and t[0]["exit_px"] == 85

    unlocked = base.with_columns(
        [pl.lit(False).alias("ask_missing"), pl.lit(False).alias("bid_missing")]
    )
    res2 = simulate(
        unlocked, entries_at("1101", [days[0]]),
        exec_spec=ZERO_COST, port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
        exit_spec=ExitSpec(abs_stop=0.05),
    )
    t2 = res2.trades.to_dicts()
    assert len(t2) == 1 and t2[0]["exit_date"] == days[3]  # 未鎖死 → 當日 -9.9% 照樣賣出


CACHE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "cache.duckdb"
)


@pytest.mark.skipif(not os.path.exists(CACHE), reason="cache.duckdb not present")
def test_buy_hold_parity_vs_canonical_prices():
    """引擎買進持有(零成本)必須與 prices.py 正典 total-return 精確一致。"""
    from research import prices
    from research.apex import data

    con = data.connect()
    panel = prices.fetch_adjusted_panel(
        con, "2018-01-02", "2024-12-30", codes=["0050"], market="twse",
        include_extra_history_days=0,
    ).sort("date")
    first = panel["date"][0]
    res = simulate(
        panel,
        pl.DataFrame({"date": [first], "company_code": ["0050"], "score": [1.0]}),
        exec_spec=ZERO_COST,
        port_spec=PortSpec(n_slots=1, capital=1_000_000.0),
    )
    fill_close = panel["close"][1]
    expected = panel["close"][-1] / fill_close
    got = res.nav["nav"][-1] / 1_000_000.0
    assert abs(got / expected - 1) < 1e-9
