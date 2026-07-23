"""capital_reduction 源:TWSE TWTAUU + TPEx revivt(減資恢復買賣;不定期,GET CSV)。

還原價因子(減資參考價)的原料。cache 欄:market, date, company_code,
post_reduction_reference_price, reason_for_capital_reduction。

移植自 TradingReader.readCapitalReduction。**格式世代(實測全史封存 raw:twse
2011-01-25~、tpex 2013-01-16~,交叉核對 docs/data_audit/_done/{A,C}-capital_reduction):
每市場只有單一格式、零欄數漂移**——不同於 daily_trading_details 的多世代:
- TWSE(12 欄):恢復買賣日期(民國 `y/m/d`,col0)| 股票代號(col1)| 名稱 |
  停止買賣前收盤價格 | 恢復買賣參考價(col4=post_red)| 漲停 | 跌停 | 開盤競價基準 |
  除權參考價 | 減資原因(col9)| 詳細資料 | 空尾欄。query 日期用西元 yyyymmdd。
- TPEx(10 欄):恢復買賣日期(民國 7 碼 `yyyMMdd`,col0)| 股票代號(col1)| 名稱 |
  最後交易日之收盤價格 | 減資恢復買賣開始日參考價格(col4)| 漲停 | 跌停 |
  開始交易基準價 | 除權參考價 | 減資原因(col9)。query 日期用西元 yyyy/MM/dd。

世代以 **header 內容判定 + guard 鎖關鍵欄語意**;欄位位移即 `parse.SchemaDrift`
fail-loud(取代舊 `len(r)!=12` 靜默 continue——TWSE 悄悄加欄地雷不可靜默錯位/丟世代,
對齊 CLAUDE.md「explicit case dispatch, never fall-through」)。
**只清數值欄**:參考價去千分位逗號/空白,`-`/`--`(未定價/無)→ None 不炸;
**減資原因(文字欄)保留原樣**(修 Reader `.replace(","/" ")` 套到所有欄、會傷文字欄的
解析層 bug;實測三類原因彌補虧損/退還股款/現金減資皆無逗號空白,輸出與舊版逐位一致)。
"""
from __future__ import annotations

import re
from datetime import date as Date, timedelta

import polars as pl

from quantlib.crawl import archive, http, parse
from quantlib.crawl.sink import Sink

TABLE = "capital_reduction"
KEY_COLS = ["market", "date", "company_code"]
MARKETS = ("twse", "tpex")

_TWSE = ("https://www.twse.com.tw/exchangeReport/TWTAUU"
         "?response=csv&startDate={s}&endDate={e}")  # 2026-07 TWSE 改參數名 strDate→startDate
_TPEX = ("https://www.tpex.org.tw/www/zh-tw/bulletin/revivt"
         "?response=csv&startDate={s}&endDate={e}")
#: 每次補抓的視窗(過去 90 日抓已恢復、未來 30 日抓已排定的恢復)
_BACK_DAYS, _FWD_DAYS = 90, 30

_SCHEMA = {"market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
           "post_reduction_reference_price": pl.Float64,
           "reason_for_capital_reduction": pl.Utf8}
_SLASH = re.compile(r"^(\d+)/(\d+)/(\d+)$")
_MINGUO7 = re.compile(r"^\d{7}$")

#: 各市場的單一格式世代:need=最少欄數,post/reason=欄位索引,guard=關鍵欄期望標頭名。
#: header 任一 guard 欄位移 → parse.SchemaDrift(fail-loud,不靜默丟世代/錯位)。
_GEN = {
    "twse": {"need": 12, "post": 4, "reason": 9,
             "guard": {0: "恢復買賣日期", 4: "恢復買賣參考價", 9: "減資原因"}},
    "tpex": {"need": 10, "post": 4, "reason": 9,
             "guard": {0: "恢復買賣日期", 4: "減資恢復買賣開始日參考價格", 9: "減資原因"}},
}


def _price(v: str) -> float | None:
    """數值欄:去千分位逗號/空白後轉 float;`-`/`--`/空(未定價或無)→ None,不炸。"""
    try:
        return float(v.replace(",", "").replace(" ", ""))
    except (ValueError, AttributeError):
        return None


def _guard(header: list[str], gen: dict, market: str) -> None:
    """鎖 header 關鍵欄語意——欄位位移即 fail-loud(不靜默錯位/丟世代)。"""
    cells = [c.strip().replace(" ", "") for c in header]
    for i, name in gen["guard"].items():
        got = cells[i] if i < len(cells) else "<缺>"
        if got != name:
            raise parse.SchemaDrift(
                f"capital_reduction {market} 欄位位移:col[{i}] 期望 '{name}' 實得 '{got}'")


def _parse(market: str, text: str) -> list[dict]:
    """解析單一市場的減資快照為 list[dict]。無 header(空窗/休市)→ []。

    以 header 判世代 + guard 鎖關鍵欄;數值欄清洗(去千分位),文字欄(減資原因)
    保留原樣。日期一律取**每列內容**的民國日(twse `y/m/d`、tpex 7 碼),非檔名。
    """
    rows = parse.parse_csv(text)
    h = parse.find_header(rows, "恢復買賣日期")
    if h < 0:
        return []
    gen = _GEN[market]
    _guard(rows[h], gen, market)
    recs = []
    for r in rows[h + 1:]:
        if len(r) < gen["need"]:
            continue
        c0 = r[0].strip().replace(" ", "")
        if market == "twse":
            m = _SLASH.match(c0)
            if not m:
                continue
            date = Date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
        else:
            if not _MINGUO7.match(c0):
                continue
            date = Date(int(c0[:3]) + 1911, int(c0[3:5]), int(c0[5:7]))
        recs.append({
            "market": market, "date": date,
            "company_code": r[1].strip().replace(" ", ""),
            "post_reduction_reference_price": _price(r[gen["post"]]),
            "reason_for_capital_reduction": r[gen["reason"]].strip(),
        })
    return recs


def fetch_range(market: str, start: Date, end: Date) -> pl.DataFrame | None:
    if market == "twse":
        url = _TWSE.format(s=parse.twse_date(start), e=parse.twse_date(end))
    else:
        fmt = "{d.year:04d}/{d.month:02d}/{d.day:02d}"
        url = _TPEX.format(s=fmt.format(d=start), e=fmt.format(d=end))
    raw = http.fetch_bytes(url)  # bytes:位元保真封存後才解碼(範圍回應原樣落地)
    # 原始檔封存鐵律:先原子落地 raw 才 parse。範圍檔命名 {end}_r.csv(r=range);
    # 每日刷新的重疊範圍由 rebuild 的 unique(date,company_code) 收斂,不重複入庫。
    archive.save_raw_named(TABLE, market, end.year,
                           f"{end.year}_{end.month}_{end.day}_r.csv", raw)
    recs = _parse(market, raw.decode("Big5-HKSCS", errors="replace"))
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
