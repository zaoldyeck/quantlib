"""daily_quote 源:TWSE MI_INDEX + TPEx stk_wn1430(當前 CSV 格式)。

cache 欄:market, date, company_code, opening/highest/lowest/closing_price,
trade_volume, trade_value, last_best_bid_price, last_best_ask_price。

移植自 TradingReader.readDailyQuote:
- TWSE 值轉換:`--`→null、``/` `/`X`→0、`+`→1、`-`→-1、else float。
- TPEx 值轉換:`---`/`----`→null、除權息字樣→0、else float。
- 欄位以 header 位置驗證(fail-loud 防悄悄加欄)。
"""
from __future__ import annotations

from datetime import date as Date

import polars as pl

from research.crawl import http, parse

TABLE = "daily_quote"
KEY_COLS = ["market", "date"]
MARKETS = ("twse", "tpex")

_TWSE_URL = ("https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
             "?response=csv&type=ALLBUT0999&date={d}")
_TPEX_URL = ("https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/"
             "stk_wn1430_result.php?l=zh-tw&o=csv&se=EW&d={d}")

_SCHEMA = {
    "market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
    "opening_price": pl.Float64, "highest_price": pl.Float64,
    "lowest_price": pl.Float64, "closing_price": pl.Float64,
    "trade_volume": pl.Int64, "trade_value": pl.Int64,
    "last_best_bid_price": pl.Float64, "last_best_ask_price": pl.Float64,
}
# TWSE header 位置守衛(cells index → 期望欄名):抓到位移就 fail-loud
_TWSE_GUARD = {2: "成交股數", 4: "成交金額", 5: "開盤價", 8: "收盤價",
               11: "最後揭示買價", 13: "最後揭示賣價"}
_TPEX_GUARD = {2: "收盤", 4: "開盤", 5: "最高", 6: "最低", 7: "成交股數",
               10: "最後買價", 12: "最後賣價"}


def _twse_num(v: str) -> float | None:
    if v == "--":
        return None
    if v in ("", " ", "X"):
        return 0.0
    if v == "+":
        return 1.0
    if v == "-":
        return -1.0
    return float(v)


def _tpex_num(v: str) -> float | None:
    if v in ("---", "----"):
        return None
    if v in ("除權息", "除權", "除息"):
        return 0.0
    return float(v)


def _guard(header: list[str], guard: dict[int, str], what: str) -> None:
    cells = [c.replace(" ", "") for c in header]
    for i, name in guard.items():
        if i >= len(cells) or cells[i] != name:
            got = cells[i] if i < len(cells) else "<缺>"
            raise parse.SchemaDrift(f"daily_quote {what} 欄位位移:col[{i}] 期望 "
                                    f"'{name}' 實得 '{got}'(TWSE/TPEx 改格式?)")


def _parse_twse(text: str, day: Date) -> pl.DataFrame | None:
    rows = parse.parse_csv(text)
    h = parse.find_header(rows, "證券代號")
    if h < 0:
        return None
    _guard(rows[h], _TWSE_GUARD, "TWSE")
    recs = []
    for r in rows[h + 1:]:
        if len(r) < 17:
            continue
        c = [x.replace(" ", "").replace(",", "") for x in r]
        tv = [_twse_num(x) for x in c[2:-1]]
        recs.append({
            "market": "twse", "date": day, "company_code": c[0],
            "opening_price": tv[3], "highest_price": tv[4], "lowest_price": tv[5],
            "closing_price": tv[6], "trade_volume": int(tv[0]), "trade_value": int(tv[2]),
            "last_best_bid_price": tv[9], "last_best_ask_price": tv[11],
        })
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


def _parse_tpex(text: str, day: Date) -> pl.DataFrame | None:
    rows = parse.parse_csv(text)
    h = parse.find_header(rows, "代號")
    if h < 0:
        return None
    _guard(rows[h], _TPEX_GUARD, "TPEx")
    seg = rows[h:]
    data = seg[1:-1]  # Reader 的 .init.tail:去 header、去末列(合計列)
    recs = []
    for r in data:
        if len(r) < 15:
            continue
        c = [x.replace(" ", "").replace(",", "") for x in r]
        tv = [_tpex_num(x) for x in c[2:-1]]
        ask = tv[9] if len(r) == 15 else tv[10]  # 大格式在買價後多一個買量欄
        recs.append({
            "market": "tpex", "date": day, "company_code": c[0],
            "opening_price": tv[2], "highest_price": tv[3], "lowest_price": tv[4],
            "closing_price": tv[0], "trade_volume": int(tv[5]), "trade_value": int(tv[6]),
            "last_best_bid_price": tv[8], "last_best_ask_price": ask,
        })
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    if market == "twse":
        text = http.fetch_text(_TWSE_URL.format(d=parse.twse_date(day)))
        return _parse_twse(text, day)
    text = http.fetch_text(_TPEX_URL.format(d=parse.minguo_slash(day)))
    return _parse_tpex(text, day)
