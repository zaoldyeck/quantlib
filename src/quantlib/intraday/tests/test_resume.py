"""完整度守護:**「有沒有這一天」必須是算得出來的事實,不是猜的。**

2026-07-22 使用者連問兩題,各戳出一個無聲的資料毀損路徑:

  Q1「額度剛好不足怎麼辦?會缺幾個日期,你知道要補哪幾天嗎?」
     舊碼:API 額度超限時回 null(官方文件明載)→ 被當成「這個月沒有資料」
     → **寫下 0-byte 哨兵 → 永久生效**。暫時性故障就這樣固化成假事實。

  Q2「當月會不會一直重複抓?」
     舊碼:是,每天重抓「月初→今天」整段,月中平均浪費 19% 的當日額度。
     而中間若有缺口(額度在月中斷過),用 max(dt) 推進的增量邏輯還會**跳過缺口**。

兩題的根都一樣:**拿代理指標(檔案存在 / mtime / max(dt))當完整度**。
正解是逐日核對權威來源——`daily_quote` 早就記著每檔在哪幾天有交易,
「缺哪幾天」= 應有集合 − 實有集合,是減法,不是推測。

Run: uv run --project . python -m pytest src/quantlib/intraday/tests/test_resume.py
"""
from __future__ import annotations

from datetime import date as Date
from datetime import datetime, timedelta
from datetime import time as dtime

import polars as pl
import pytest

from quantlib.intraday import pull_kbars as pk


@pytest.fixture
def out(tmp_path, monkeypatch):
    monkeypatch.setattr(pk, "OUT", tmp_path)
    return tmp_path


class _FakeKbars:
    def __init__(self, days: list[Date]):
        self.ts = [int(datetime.combine(d, dtime(9, 0)).timestamp() * 1e9) for d in days]
        n = len(self.ts)
        self.Open = self.High = self.Low = self.Close = [10.0] * n
        self.Volume = self.Amount = [1.0] * n


class _FakeApi:
    """假 API;`blackout` 內的日期一律回空 —— 模擬額度耗盡時 API 回 null。"""

    def __init__(self, blackout: set | None = None):
        self.calls: list[tuple[Date, Date]] = []
        self.blackout = blackout or set()

    def kbars(self, contract, start, end):
        s, e = Date.fromisoformat(start), Date.fromisoformat(end)
        self.calls.append((s, e))
        days, d = [], s
        while d <= e:
            if d.weekday() < 5 and d not in self.blackout:
                days.append(d)
            d += timedelta(days=1)
        return _FakeKbars(days)


def _tdays(*days: str) -> set:
    return {Date.fromisoformat(d) for d in days}


# ── 缺口是算出來的,不是猜的 ──────────────────────────────────────────
def test_missing_days_are_computed_not_guessed(out) -> None:
    exp = _tdays("2026-07-01", "2026-07-02", "2026-07-03")
    api = _FakeApi()
    todo = pk._month_todo([("2330", object())], "2026-07", {"2330": exp}, set())
    assert todo and todo[0][3] == sorted(exp)
    pk._pull(api, object(), "2330", "2026-07", sorted(exp))
    # 補完之後就不該再列入待辦
    assert not pk._month_todo([("2330", object())], "2026-07", {"2330": exp}, set())


def test_gap_in_the_middle_is_found_and_filled(out) -> None:
    """**額度在月中斷掉留下的洞**:用 max(dt) 推進會直接跳過,逐日核對則抓得到。"""
    exp = _tdays("2026-07-01", "2026-07-02", "2026-07-03", "2026-07-06", "2026-07-07")
    api = _FakeApi()
    # 先只補頭尾,中間 07-02、07-03 留洞
    pk._pull(api, object(), "2330", "2026-07",
             [Date(2026, 7, 1), Date(2026, 7, 6), Date(2026, 7, 7)])
    todo = pk._month_todo([("2330", object())], "2026-07", {"2330": exp}, set())
    assert todo, "中間的洞沒被發現 —— 這正是 max(dt) 推進法的盲點"
    assert todo[0][3] == [Date(2026, 7, 2), Date(2026, 7, 3)]


def test_quota_blackout_never_records_completion(out) -> None:
    """**最關鍵的一條**:額度耗盡(API 回空)不得留下任何「已完成」痕跡。

    否則暫時性故障會固化成「這個月沒有資料」,而且永久生效、無人知曉。
    """
    exp = _tdays("2026-07-01", "2026-07-02")
    api = _FakeApi(blackout=exp)               # 整段都回空 = 額度耗盡
    n = pk._pull(api, object(), "2330", "2026-07", sorted(exp))
    assert n == 0
    assert not (out / "2026-07" / "2330.empty").exists(), "不得寫下哨兵"
    assert not (out / "2026-07" / "2330.parquet").exists(), "不得落下空檔"
    todo = pk._month_todo([("2330", object())], "2026-07", {"2330": exp}, set())
    assert todo and todo[0][3] == sorted(exp), "下次必須整段重補"


def test_partial_blackout_keeps_what_it_got_and_retries_the_rest(out) -> None:
    """抓到一半沒額度:抓到的要留住,沒抓到的下次補——不得整段丟棄或整段當完成。"""
    exp = _tdays("2026-07-01", "2026-07-02", "2026-07-03")
    api = _FakeApi(blackout={Date(2026, 7, 3)})
    pk._pull(api, object(), "2330", "2026-07", sorted(exp))
    have = pk._have("2026-07", "2330")
    assert have == _tdays("2026-07-01", "2026-07-02")
    todo = pk._month_todo([("2330", object())], "2026-07", {"2330": exp}, set())
    assert todo[0][3] == [Date(2026, 7, 3)]


def test_merge_never_loses_or_duplicates(out) -> None:
    """多次補齊後必須是完整聯集且無重複——重複會讓成交量統計整個失真。"""
    api = _FakeApi()
    pk._pull(api, object(), "2330", "2026-07", [Date(2026, 7, 1), Date(2026, 7, 2)])
    pk._pull(api, object(), "2330", "2026-07", [Date(2026, 7, 2), Date(2026, 7, 3)])
    df = pl.read_parquet(out / "2026-07" / "2330.parquet")
    assert df["ts"].n_unique() == df.height, "有重複"
    assert pk._have("2026-07", "2330") == _tdays("2026-07-01", "2026-07-02", "2026-07-03")
    assert df["dt"].is_sorted()


def test_no_trading_days_means_nothing_to_do(out) -> None:
    """daily_quote 說該月沒交易(未上市/已下市)→ 本來就不該有,不列待辦、不發呼叫。"""
    assert not pk._month_todo([("9999", object())], "2026-07", {}, set())


def test_ranges_merge_across_weekends(out) -> None:
    """缺的日子併成連續區間以省呼叫;週末造成的 2 天間隔不得把區間切碎。"""
    days = [Date(2026, 7, 3), Date(2026, 7, 6), Date(2026, 7, 7)]   # 五、一、二
    assert pk._ranges(days) == [(Date(2026, 7, 3), Date(2026, 7, 7))]
    far = [Date(2026, 7, 3), Date(2026, 7, 20)]
    assert len(pk._ranges(far)) == 2, "相隔半個月不該併成一段"


def test_newest_first_and_start_is_dynamic() -> None:
    """工作序由近而遠,且起點必須是**執行當下**的月份,不得寫死。"""
    ms = pk._months(pk.HIST_FLOOR, Date.today())
    ms.reverse()
    tags = [t for t, _, _ in ms]
    assert tags == sorted(tags, reverse=True)
    today = Date.today()
    assert tags[0] == f"{today.year:04d}-{today.month:02d}"
