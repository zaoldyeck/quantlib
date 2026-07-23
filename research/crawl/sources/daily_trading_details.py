"""daily_trading_details 源:TWSE T86 + TPEx 3itrade_hedge(三大法人買賣超)。

cache 只留 4 欄差額:foreign_investors_difference、trust_difference、
dealers_difference、total_difference(移植自 TradingReader.readDailyTradingDetails
的現行格式分支)。

- TWSE(20 欄):foreign = 外陸資買賣超 + 外資自營商買賣超(cells[4]+cells[7]);
  trust = cells[10];dealers = cells[11](自營商合計);total = cells[18]。
- TPEx(24 欄):foreign = 外資及陸資合計買賣超(cells[10]);trust = cells[13];
  dealers = cells[22];total = cells[23]。
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.crawl import archive, http, parse

TABLE = "daily_trading_details"
KEY_COLS = ["market", "date"]
MARKETS = ("twse", "tpex")

_TWSE_URL = ("https://www.twse.com.tw/rwd/zh/fund/T86"
             "?response=csv&selectType=ALLBUT0999&date={d}")
_TPEX_URL = ("https://www.tpex.org.tw/web/stock/3insti/daily_trade/"
             "3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d={d}")

_SCHEMA = {"market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
           "foreign_investors_difference": pl.Int64, "trust_difference": pl.Int64,
           "dealers_difference": pl.Int64, "total_difference": pl.Int64}

_TWSE_GUARD = {4: "外陸資買賣超股數(不含外資自營商)", 7: "外資自營商買賣超股數",
               10: "投信買賣超股數", 11: "自營商買賣超股數", 18: "三大法人買賣超股數"}
_TPEX_GUARD = {4: "外資及陸資(不含外資自營商)-買賣超股數",
               10: "外資及陸資-買賣超股數", 13: "投信-買賣超股數"}


def _int(v: str) -> int:
    try:
        return int(parse.clean(v))
    except ValueError:
        return 0  # 對齊 Reader 的 Try(toInt).getOrElse(0)


def _guard(header: list[str], guard: dict[int, str], what: str) -> None:
    cells = [c.replace(" ", "") for c in header]
    for i, name in guard.items():
        if i >= len(cells) or cells[i] != name:
            got = cells[i] if i < len(cells) else "<缺>"
            raise parse.SchemaDrift(
                f"T86 {what} 欄位位移:col[{i}] 期望 '{name}' 實得 '{got}'")


def _parse(text: str, day: Date, market: str) -> pl.DataFrame | None:
    rows = parse.parse_csv(text)
    marker = "證券代號" if market == "twse" else "代號"
    h = parse.find_header(rows, marker)
    if h < 0:
        return None
    if market == "twse":
        _guard(rows[h], _TWSE_GUARD, "TWSE")
        need, fi, tr, de, to = 19, (4, 7), 10, 11, 18
    else:
        _guard(rows[h], _TPEX_GUARD, "TPEx")
        need, fi, tr, de, to = 24, (10,), 13, 22, 23
    recs = []
    for r in rows[h + 1:]:
        if len(r) < need:
            continue
        foreign = sum(_int(r[i]) for i in fi)
        recs.append({
            "market": market, "date": day, "company_code": parse.clean(r[0]),
            "foreign_investors_difference": foreign,
            "trust_difference": _int(r[tr]),
            "dealers_difference": _int(r[de]),
            "total_difference": _int(r[to]),
        })
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    d = parse.twse_date(day) if market == "twse" else parse.minguo_slash(day)
    url = (_TWSE_URL if market == "twse" else _TPEX_URL).format(d=d)
    text = http.fetch_text(url)
    archive.save_raw("daily_trading_details", market, day, text)   # 原始檔封存鐵律:先落地再 parse
    return _parse(text, day, market)
