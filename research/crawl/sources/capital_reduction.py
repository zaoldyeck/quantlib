"""capital_reduction 源:TWSE TWTAUU + TPEx revivt(減資恢復買賣;不定期,GET CSV)。

還原價因子(減資參考價)的原料。cache 欄:market, date, company_code,
post_reduction_reference_price, reason_for_capital_reduction。

移植自 TradingReader.readCapitalReduction:
- TWSE(12 欄):date=民國 values[0] `y/m/d`,code=values[1],
  post_red=values[4],reason=values[9]。query 日期用西元 yyyymmdd。
- TPEx(10 欄):head 為民國 7 碼 `yyyMMdd`,同欄位位置。query 日期用西元 yyyy/MM/dd。
所有儲存格先去空白/逗號(對齊 Reader)。
"""
from __future__ import annotations

import re
from datetime import date as Date, timedelta

import polars as pl

from research.crawl import http, parse
from research.crawl.sink import Sink

TABLE = "capital_reduction"
KEY_COLS = ["market", "date", "company_code"]
MARKETS = ("twse", "tpex")

_TWSE = ("https://www.twse.com.tw/exchangeReport/TWTAUU"
         "?response=csv&strDate={s}&endDate={e}")
_TPEX = ("https://www.tpex.org.tw/www/zh-tw/bulletin/revivt"
         "?response=csv&startDate={s}&endDate={e}")
#: 每次補抓的視窗(過去 90 日抓已恢復、未來 30 日抓已排定的恢復)
_BACK_DAYS, _FWD_DAYS = 90, 30

_SCHEMA = {"market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
           "post_reduction_reference_price": pl.Float64,
           "reason_for_capital_reduction": pl.Utf8}
_SLASH = re.compile(r"^(\d+)/(\d+)/(\d+)$")
_MINGUO7 = re.compile(r"^\d{7}$")


def _price(v: str) -> float | None:
    try:
        return float(v)
    except ValueError:
        return None


def _parse_twse(text: str) -> list[dict]:
    recs = []
    for r in parse.parse_csv(text):
        if len(r) != 12:
            continue
        c = [x.replace(" ", "").replace(",", "") for x in r]
        m = _SLASH.match(c[0])
        if not m or c[0] == "恢復買賣日期":
            continue
        y, mo, d = int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3))
        recs.append({"market": "twse", "date": Date(y, mo, d), "company_code": c[1],
                     "post_reduction_reference_price": _price(c[4]),
                     "reason_for_capital_reduction": c[9]})
    return recs


def _parse_tpex(text: str) -> list[dict]:
    recs = []
    for r in parse.parse_csv(text):
        if len(r) != 10:
            continue
        c = [x.replace(" ", "").replace(",", "") for x in r]
        if not _MINGUO7.match(c[0]):
            continue
        y, mo, d = int(c[0][:3]) + 1911, int(c[0][3:5]), int(c[0][5:7])
        recs.append({"market": "tpex", "date": Date(y, mo, d), "company_code": c[1],
                     "post_reduction_reference_price": _price(c[4]),
                     "reason_for_capital_reduction": c[9]})
    return recs


def fetch_range(market: str, start: Date, end: Date) -> pl.DataFrame | None:
    if market == "twse":
        url = _TWSE.format(s=parse.twse_date(start), e=parse.twse_date(end))
        recs = _parse_twse(http.fetch_text(url))
    else:
        fmt = "{d.year:04d}/{d.month:02d}/{d.day:02d}"
        url = _TPEX.format(s=fmt.format(d=start), e=fmt.format(d=end))
        recs = _parse_tpex(http.fetch_text(url))
    if not recs:
        return None
    return (pl.DataFrame(recs, schema=_SCHEMA)
            .unique(subset=["date", "company_code"], keep="first", maintain_order=True))


def refresh(sink: Sink, upto: Date) -> int:
    start, end = upto - timedelta(days=_BACK_DAYS), upto + timedelta(days=_FWD_DAYS)
    total = 0
    for market in MARKETS:
        try:
            df = fetch_range(market, start, end)
        except Exception as exc:  # noqa: BLE001
            print(f"[crawl] capital_reduction/{market} 抓取失敗:{exc}")
            continue
        if df is None:
            continue
        n = sink.upsert(TABLE, df, KEY_COLS)
        total += n
        print(f"[crawl] capital_reduction/{market} {start}~{end}: {n} 列")
    return total
