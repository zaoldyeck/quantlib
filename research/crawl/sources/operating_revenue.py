"""operating_revenue 源:MOPS t21sc03 月營收(afterIFRSs 合併,POST FileDownLoad → CSV)。

S 進場訊號的原料(新鮮月營收 cohort)。cache 欄:market, type, year, month,
company_code, company_name, industry, monthly_revenue, monthly_revenue_yoy。

modern CSV(year≥2013,UTF-8;移植自 FinancialReader.readOperatingRevenue 的
`case _` splitAt(5) 分支):code=v[2]、name=v[3]、industry=v[4]、
monthly_revenue=v[5]、去年同月增減%=v[9]。afterIFRSs 檔一律 type='consolidated'。

月頻:`refresh` 每次補最近數月(idempotent);更新後須 `rebuild_industry_taxonomy`。
"""
from __future__ import annotations

import re
from datetime import date as Date

import polars as pl

from research.crawl import http, parse
from research.crawl.sink import Sink

TABLE = "operating_revenue"
KEY_COLS = ["market", "type", "year", "month"]
MARKETS = ("twse", "tpex")

_URL = "https://mopsov.twse.com.tw/server-java/FileDownLoad"
_FILEPATH = {"twse": "/home/html/nas/t21/sii/", "tpex": "/home/html/nas/t21/otc/"}
#: 每次補抓的最近月數(涵蓋「M 月營收於 M+1 月 10 日左右公告」的發布延遲)
_REFRESH_MONTHS = 3

_SCHEMA = {"market": pl.Utf8, "type": pl.Utf8, "year": pl.Int32, "month": pl.Int32,
           "company_code": pl.Utf8, "company_name": pl.Utf8, "industry": pl.Utf8,
           "monthly_revenue": pl.Float64, "monthly_revenue_yoy": pl.Float64}

_CODE = re.compile(r"^\d{4}[0-9A-Z]?$")


def _dbl(v: str) -> float | None:
    try:
        return float(v.replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def fetch_month(market: str, year: int, month: int) -> pl.DataFrame | None:
    """抓某市場某年月的月營收;檔案不存在/尚未公告 → None。"""
    form = {"step": "9", "functionName": "show_file", "filePath": _FILEPATH[market],
            "fileName": f"t21sc03_{year - 1911}_{month}.csv"}
    text = http.fetch_text(_URL, encoding="utf-8", form=form)
    if "查詢無資料" in text or "無應揭露資訊" in text:
        return None
    rows = parse.parse_csv(text)
    if len(rows) < 2:
        return None
    recs = []
    for r in rows[1:]:  # Reader 的 .tail:去表頭
        if len(r) < 11:
            continue
        code = r[2].strip()
        if not _CODE.match(code):
            continue
        recs.append({
            "market": market, "type": "consolidated", "year": year, "month": month,
            "company_code": code, "company_name": r[3].strip(),
            "industry": (r[4].strip() or None),
            "monthly_revenue": _dbl(r[5]), "monthly_revenue_yoy": _dbl(r[9]),
        })
    if not recs:
        return None
    return (pl.DataFrame(recs, schema=_SCHEMA)
            .unique(subset=["company_code"], keep="first", maintain_order=True))


def _recent_months(upto: Date, n: int) -> list[tuple[int, int]]:
    y, m = upto.year, upto.month
    out = []
    for _ in range(n):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        out.append((y, m))
    return out


def refresh(sink: Sink, upto: Date) -> int:
    """補抓最近 _REFRESH_MONTHS 個月(idempotent upsert)。回新增列數。"""
    total = 0
    for market in MARKETS:
        for year, month in _recent_months(upto, _REFRESH_MONTHS):
            try:
                df = fetch_month(market, year, month)
            except Exception as exc:  # noqa: BLE001 - 單月失敗不擋其餘月
                print(f"[crawl] operating_revenue/{market} {year}-{month:02d} 抓取失敗:{exc}")
                continue
            if df is None:
                continue
            n = sink.upsert(TABLE, df, KEY_COLS)
            total += n
            print(f"[crawl] operating_revenue/{market} {year}-{month:02d}: {n} 列")
    return total


def rebuild_industry_taxonomy(sink: Sink) -> None:
    """月營收更新後重算 PIT 產業分類(重用 research.industry_taxonomy,唯一真源)。"""
    from research.industry_taxonomy import build_industry_taxonomy_pit

    df = build_industry_taxonomy_pit(sink.con)
    sink.con.register("_it_new", df)
    try:
        sink.con.execute("DROP TABLE IF EXISTS industry_taxonomy_pit")
        sink.con.execute("CREATE TABLE industry_taxonomy_pit AS SELECT * FROM _it_new")
    finally:
        sink.con.unregister("_it_new")
    print(f"[crawl] industry_taxonomy_pit 重算 {df.height} 列")
