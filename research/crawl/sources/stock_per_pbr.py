"""stock_per_pbr 源:TWSE BWIBBU_d + TPEx pera_result(本益比/殖利率/股價淨值比)。

cache 欄:price_book_ratio、dividend_yield、price_to_earning_ratio(移植自
TradingReader.readStockPER_PBR_DividendYield 現行格式):
- TWSE(9 欄):PE=cells[5]、PB=cells[6]、殖利率=cells[3]。
- TPEx(8 欄):PE=cells[2]、PB=cells[6]、殖利率=cells[5]。
`-`/空 → null(toDoubleOption);同代碼取首見。
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.crawl import http, parse

TABLE = "stock_per_pbr"
KEY_COLS = ["market", "date"]
MARKETS = ("twse", "tpex")

_TWSE_URL = ("https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
             "?response=csv&selectType=ALL&date={d}")
_TPEX_URL = ("https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/"
             "pera_result.php?l=zh-tw&o=csv&d={d}")

_SCHEMA = {"market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
           "price_book_ratio": pl.Float64, "dividend_yield": pl.Float64,
           "price_to_earning_ratio": pl.Float64}

_TWSE_GUARD = {3: "殖利率(%)", 5: "本益比", 6: "股價淨值比"}
_TPEX_GUARD = {2: "本益比", 5: "殖利率(%)", 6: "股價淨值比"}


def _dbl(v: str) -> float | None:
    try:
        return float(parse.clean(v))
    except ValueError:
        return None  # 對齊 Reader 的 toDoubleOption('-'/空 → None)


def _guard(header: list[str], guard: dict[int, str], what: str) -> None:
    cells = [c.replace(" ", "") for c in header]
    for i, name in guard.items():
        if i >= len(cells) or cells[i] != name:
            got = cells[i] if i < len(cells) else "<缺>"
            raise parse.SchemaDrift(
                f"BWIBBU {what} 欄位位移:col[{i}] 期望 '{name}' 實得 '{got}'")


def _parse(text: str, day: Date, market: str) -> pl.DataFrame | None:
    rows = parse.parse_csv(text)
    marker = "證券代號" if market == "twse" else "股票代號"
    h = parse.find_header(rows, marker)
    if h < 0:
        return None
    if market == "twse":
        _guard(rows[h], _TWSE_GUARD, "TWSE")
        need, pe, pb, dy = 7, 5, 6, 3
    else:
        _guard(rows[h], _TPEX_GUARD, "TPEx")
        need, pe, pb, dy = 7, 2, 6, 5
    recs = []
    for r in rows[h + 1:]:
        if len(r) < need:
            continue
        recs.append({
            "market": market, "date": day, "company_code": parse.clean(r[0]),
            "price_book_ratio": _dbl(r[pb]), "dividend_yield": _dbl(r[dy]),
            "price_to_earning_ratio": _dbl(r[pe]),
        })
    if not recs:
        return None
    return (pl.DataFrame(recs, schema=_SCHEMA)
            .unique(subset=["company_code"], keep="first", maintain_order=True))


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    d = parse.twse_date(day) if market == "twse" else parse.minguo_slash(day)
    url = (_TWSE_URL if market == "twse" else _TPEX_URL).format(d=d)
    return _parse(http.fetch_text(url), day, market)
