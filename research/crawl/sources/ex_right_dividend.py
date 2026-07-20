"""ex_right_dividend 源:MOPS t108sb27 除權息(兩步 POST → CSV,月頻)。

還原價因子(現金股利)的原料。cache 欄:market, date, company_code, cash_dividend。

兩步流(移植自 Crawler.getExRightDividend + TradingReader.parseMopsRows):
1. POST ajax_t108sb27(step=1,TYPEK,民國 year,month)→ HTML,取 `input[name=filename]`。
2. POST t105sb02(firstin=true,step=10,filename)→ Big5 CSV。
每公司每期最多兩列:除息日一列(cash_dividend=現金股利合計)、除權日一列
(cash_dividend=0);日期為西元 yyyy/MM/dd。
"""
from __future__ import annotations

import re
from datetime import date as Date

import polars as pl

from research.crawl import http, parse
from research.crawl.sink import Sink

TABLE = "ex_right_dividend"
KEY_COLS = ["market", "date", "company_code"]
MARKETS = ("twse", "tpex")

_PAGE = "https://mopsov.twse.com.tw/mops/web/ajax_t108sb27"
_FILE = "https://mopsov.twse.com.tw/server-java/t105sb02"
_TYPEK = {"twse": "sii", "tpex": "otc"}
_REFRESH_MONTHS = 3

_SCHEMA = {"market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
           "cash_dividend": pl.Float64}
_INPUT_RE = re.compile(r'<input[^>]*name=["\']?filename["\']?[^>]*>', re.IGNORECASE)
_VALUE_RE = re.compile(r'value=["\']([^"\']*)["\']', re.IGNORECASE)
_CODE = re.compile(r"^\d{4}[0-9A-Z]?$")


def _d(v: str) -> float:
    try:
        return float(v.replace(",", "").strip())
    except (ValueError, AttributeError):
        return 0.0


def _slash_date(s: str) -> Date | None:
    """西元 yyyy/MM/dd → date。"""
    s = s.strip()
    try:
        y, m, d = s.split("/")
        return Date(int(y), int(m), int(d))
    except (ValueError, TypeError):
        return None


def fetch_month(market: str, year: int, month: int) -> pl.DataFrame | None:
    """抓某市場某年月的除權息公告;無事件 → None。"""
    form1 = {"step": "1", "firstin": "ture", "off": "1", "TYPEK": _TYPEK[market],
             "year": str(year - 1911), "month": str(month),
             "b_date": "1", "e_date": "31", "type": "0"}
    html = http.fetch_text(_PAGE, encoding="Big5-HKSCS", form=form1)
    tag = _INPUT_RE.search(html)
    if not tag:
        return None  # 該月無除權息事件
    val = _VALUE_RE.search(tag.group(0))
    if not val or not val.group(1).endswith(".csv"):
        return None
    csv_text = http.fetch_text(
        _FILE, encoding="Big5-HKSCS",
        form={"firstin": "true", "step": "10", "filename": val.group(1)})
    recs = []
    for r in parse.parse_csv(csv_text):
        if len(r) < 17 or not r[0].strip() or r[0].strip() == "公司代號":
            continue
        code = r[0].strip()
        if not _CODE.match(code):
            continue
        total_stock = _d(r[4]) + _d(r[5])
        total_cash = _d(r[7]) + _d(r[8]) + _d(r[9])
        ex_right = _slash_date(r[6])
        ex_div = _slash_date(r[10])
        if total_cash > 0 and ex_div:
            recs.append({"market": market, "date": ex_div,
                         "company_code": code, "cash_dividend": total_cash})
        if total_stock > 0 and ex_right:
            recs.append({"market": market, "date": ex_right,
                         "company_code": code, "cash_dividend": 0.0})
    if not recs:
        return None
    return (pl.DataFrame(recs, schema=_SCHEMA)
            .unique(subset=["date", "company_code"], keep="first", maintain_order=True))


def _recent_months(upto: Date, n: int) -> list[tuple[int, int]]:
    y, m, out = upto.year, upto.month, []
    for _ in range(n):
        out.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


def refresh(sink: Sink, upto: Date) -> int:
    """補抓最近數月除權息(含當月,因除權息日多在公告月內或稍後)。"""
    total = 0
    for market in MARKETS:
        for year, month in _recent_months(upto, _REFRESH_MONTHS):
            try:
                df = fetch_month(market, year, month)
            except Exception as exc:  # noqa: BLE001
                print(f"[crawl] ex_right_dividend/{market} {year}-{month:02d} 抓取失敗:{exc}")
                continue
            if df is None:
                continue
            n = sink.upsert(TABLE, df, KEY_COLS)
            total += n
            print(f"[crawl] ex_right_dividend/{market} {year}-{month:02d}: {n} 列")
    return total
