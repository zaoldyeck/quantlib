"""sbl_borrowing 源:借券賣出餘額(機構結構性空頭,日頻)。

- TWSE `TWT93U`(Big5-HKSCS CSV,2016-01-04 起)。
- TPEx `margin/sbl`(UTF-8 JSON,存檔副檔名沿用 `.csv`,~2013 起)。

兩市場經抽取後**同一套欄位佈局**(見 `_BORROW_*`):
    0 代號 | 1 名稱 | 2..7 融券(已由 margin_transactions 涵蓋,不接)
    8 借券前日餘額 | 9 當日賣出 | 10 當日還券 | 11 當日調整 |
    12 當日餘額 | 13 次一營業日可限額 | 14 備註

移植自 `TradingReader.readSblBorrowing`,**忠實**保留:欄位對位(借券區塊 8-13)、
值轉換(cleanCell 去逗號/%/空白 → toLong)、當日調整可為負、市場分流、以
`[0-9][0-9A-Z]*` 過濾資料列、以 (market,date,code) 去重。

## 稽核發現、本 port 一次寫對的 bug(A/C-sbl_borrowing)

1. **日期用內容標題、非只認檔名(BUG 1,根因守護)**。TWSE `TWT93U` 對某些請求
   (多為非交易日)回傳**過期報表**;Scala reader 只用檔名日期,把 2017-12-18 的
   數字蓋上 2016-10-29 的日期戳(26 個 TWSE 日期、26,354 列汙染)。本 port 從
   **內容**取日期(TWSE 民國標題 `NNN年NN月NN日`、TPEx JSON 頂層 `date`),
   `fetch_day` 內容日期 ≠ 請求日期即 `DateMismatch` **拒收**(不靜默插入)。

2. **name-strip 只清數值欄(本源實證為真 bug)**。Scala `cleanCell` 對**每一格**
   去空白,把含半形空白的 ETF 名稱打壞:`元大MSCI A股`→`元大MSCIA股`、
   `凱基ESG BBB債15+`→`凱基ESGBBB債15+`(PG 現存即壞值)。本 port 只對**數值欄**
   做 cleanCell,名稱僅 `strip()` 保留內部空白 → 名稱正確。

3. **型別 Int64**。六欄餘額本源最大 ~1.3e9(未溢 int32),但 Scala 用 Long、
   cache/PG 為 BIGINT;本 port 一律 Int64 對齊,杜絕未來溢位。

4. **版型用明確 header/fields 守衛**(`parse.SchemaDrift` fail-loud),不用
   fallthrough——TWSE 悄悄加欄地雷。TWSE 有資料列卻找不到『代號』表頭即 fail-loud
   (不靜默跳過守衛而錯位)。TPEx `json.loads` 對 **0-byte 哨兵 / 壞 JSON** 回空 DF
   (不炸),讓 rebuild-from-raw 把休市日當「空」乾淨略過、不誤計為 error(與姊妹源
   `foreign_holding_ratio.parse_tpex` 同一守衛;缺此守衛時 1,074 個 tpex 哨兵會在
   rebuild 被 `json.loads("")` 炸成假 error、掩蓋真解析錯)。

## 歷史格式世代(2026-07 全量掃描 6,266 檔實證)

**兩市場各只有一個封存世代**,因全史於 2026-04 用現行端點一次回補、格式一致:
- TWSE `TWT93U` Big5-HKSCS CSV,16 欄,標頭固定在第 3 列(`代號…備註`),
  借券區塊 index 8-13;2016-01-04~今無漂移。
- TPEx `margin/sbl` UTF-8 JSON,`{date, tables:[{fields, data}], stat}`,
  data 每列 15 欄;2016 起 `fields` 陣列未變。
無 2007-2015 舊世代(封存不含;端點回補即現行版)。故單一 case 即涵蓋全史,
非多世代分派——但守衛仍 fail-loud,未來端點改版立即紅燈而非靜默錯位。

## 刻意不接的欄位 / 語義坑(留註以免下一人重查)

- **備註欄(raw index 14)刻意不接**。取值為主管機關旗標,其中 `Z`=借券賣出餘額
  已達總量控管(軋空關鍵情境)。cache 表 `sbl_borrowing` **無 note 欄**,接它需依
  Slick Schema Contract 加欄(改 schema)——超出本 port 範圍(不碰 cache 寫入結構)。
  若日後要用天花板/停止買賣訊號,加 `note` 欄後於此處取 `cells[14]` 回填。
- **單位為『股』**,而同源融券落在 margin_transactions 是『張』(差 1000 倍);
  跨表比對須換算(稽核 A REAL)。

## 欄位/型別:回傳 DF 與 cache 表同構(9 欄,無 company_name)

cache `sbl_borrowing` 投影掉 PG 的 `id`/`company_name`(稽核 C:兩者於 research/
零消費者)。`fetch_day` 回 cache 的 9 欄;`parse_raw` 另帶 `company_name`(供 parity
測試對 PG 逐欄比對 + 佐證 name-strip 修正)。

## cache 依賴

`fetch_day` 走 archive→parse,不讀 cache;parity 測試讀 PG(或 cache,C-audit 證
cache==PG 逐位)當對照。
"""
from __future__ import annotations

import json
import re
from datetime import date as Date

import polars as pl

from research.crawl import archive, http, parse

TABLE = "sbl_borrowing"
KEY_COLS = ["market", "date"]
MARKETS = ("twse", "tpex")

_TWSE_URL = "https://www.twse.com.tw/exchangeReport/TWT93U?response=csv&date={d}"
_TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/margin/sbl?date={d}"

#: 資料列代號正規(對齊 Scala `[0-9][0-9A-Z]*`,全串匹配);套在 raw head 上。
_STOCK = re.compile(r"[0-9][0-9A-Z]*")

#: 借券區塊欄位索引(TWSE CSV 與 TPEx JSON 抽取後一致)。
_I_PREV, _I_SOLD, _I_RET, _I_ADJ, _I_BAL, _I_LIMIT = 8, 9, 10, 11, 12, 13
_MIN_COLS = 14  # 需存取到 index 13

#: parse_raw 輸出(含 company_name;fetch_day 會投影掉以對齊 cache 9 欄)。
_PARSE_SCHEMA = {
    "company_code": pl.Utf8, "company_name": pl.Utf8,
    "prev_day_balance": pl.Int64, "daily_sold": pl.Int64,
    "daily_returned": pl.Int64, "daily_adjustment": pl.Int64,
    "daily_balance": pl.Int64, "next_day_limit": pl.Int64,
}
#: cache 表 sbl_borrowing 的 9 欄(fetch_day 對齊)。
CACHE_COLS = ["market", "date", "company_code", "prev_day_balance", "daily_sold",
              "daily_returned", "daily_adjustment", "daily_balance", "next_day_limit"]

#: header/fields 位置守衛(借券側 8-14):抓到位移就 fail-loud。
_TWSE_GUARD = {8: "前日餘額", 9: "當日賣出", 10: "當日還券", 11: "當日調整",
               12: "當日餘額", 13: "次一營業日可限額", 14: "備註"}
_TPEX_GUARD = {8: "前日餘額", 9: "當日賣出", 10: "當日還券", 11: "當日調整數額",
               12: "當日餘額", 13: "次一營業日可借券賣出限額", 14: "備註"}

_TWSE_DATE_RE = re.compile(r"(\d+)年(\d+)月(\d+)日")


class DateMismatch(parse.SchemaDrift):
    """內容自報日期 ≠ 請求/檔名日期——過期報表,拒收不靜默插入(稽核 BUG 1 根因守護)。"""


def _guard(names: list[str], guard: dict[int, str], what: str) -> None:
    cells = [c.replace(" ", "") for c in names]
    for i, name in guard.items():
        got = cells[i] if i < len(cells) else "<缺>"
        if got != name:
            raise parse.SchemaDrift(
                f"sbl_borrowing {what} 欄位位移:col[{i}] 期望 '{name}' 實得 '{got}'"
                f"(TWSE/TPEx 改格式?)")


def _twse_content_date(rows: list[list[str]]) -> Date | None:
    """從 TWSE 標題列取民國日期(`NNN年NN月NN日 信用額度總量管制餘額表`)→ 西元。"""
    for r in rows[:3]:
        if r and (m := _TWSE_DATE_RE.search(r[0])):
            try:
                return Date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
            except ValueError:
                return None
    return None


def _tpex_content_date(obj: dict) -> Date | None:
    """TPEx JSON 內容日期:優先頂層 `date`(西元 yyyymmdd),退回 tables[0].date(民國)。"""
    d = obj.get("date")
    if isinstance(d, str) and len(d) == 8 and d.isdigit():
        try:
            return Date(int(d[:4]), int(d[4:6]), int(d[6:8]))
        except ValueError:
            return None
    tables = obj.get("tables") or []
    td = tables[0].get("date") if tables else None
    return parse.parse_minguo_slash(td) if isinstance(td, str) else None


def _record(cells: list[str]) -> dict | None:
    """一列 → record;數值欄任一非整數 → 整列丟棄(對齊 Scala 外層 Try)。

    - 名稱只 `strip()`(保留內部空白,name-strip 修正);代號/數值走 cleanCell。
    - 次一營業日限額(index 13)個別容錯 → 0(對齊 Scala `Try(..).getOrElse(0L)`)。
    """
    try:
        prev = int(parse.clean(cells[_I_PREV]))
        sold = int(parse.clean(cells[_I_SOLD]))
        ret = int(parse.clean(cells[_I_RET]))
        adj = int(parse.clean(cells[_I_ADJ]))
        bal = int(parse.clean(cells[_I_BAL]))
    except ValueError:
        return None
    try:
        limit = int(parse.clean(cells[_I_LIMIT]))
    except ValueError:
        limit = 0
    return {
        "company_code": parse.clean(cells[0]),
        "company_name": cells[1].strip(),
        "prev_day_balance": prev, "daily_sold": sold, "daily_returned": ret,
        "daily_adjustment": adj, "daily_balance": bal, "next_day_limit": limit,
    }


def _frame(rows: list[list[str]]) -> pl.DataFrame:
    """把已抽取的 raw 列 → 去重(by code,keep first)→ polars DF(parse schema)。"""
    seen: set[str] = set()
    recs: list[dict] = []
    for r in rows:
        if len(r) < _MIN_COLS or not _STOCK.fullmatch(r[0]):
            continue
        rec = _record(r)
        if rec is None or rec["company_code"] in seen:
            continue
        seen.add(rec["company_code"])
        recs.append(rec)
    return pl.DataFrame(recs, schema=_PARSE_SCHEMA)


def parse_twse(raw: bytes) -> tuple[Date | None, pl.DataFrame]:
    """TWSE Big5 CSV → (內容日期, DF)。header 位置守衛 fail-loud。"""
    rows = parse.parse_csv(raw.decode("Big5-HKSCS", errors="replace"))
    content_date = _twse_content_date(rows)
    h = parse.find_header(rows, "代號")
    if h >= 0:
        _guard(rows[h], _TWSE_GUARD, "TWSE")
    df = _frame(rows)
    if df.height and h < 0:
        raise parse.SchemaDrift("sbl_borrowing TWSE 有資料列卻找不到『代號』表頭(格式漂移?)")
    if df.height and content_date is None:
        raise parse.SchemaDrift("sbl_borrowing TWSE 有資料列卻無法從標題解析日期(格式漂移?)")
    return content_date, df


def parse_tpex(raw: bytes) -> tuple[Date | None, pl.DataFrame]:
    """TPEx UTF-8 JSON → (內容日期, DF)。fields 位置守衛 fail-loud。"""
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None, pl.DataFrame([], schema=_PARSE_SCHEMA)  # 0-byte 哨兵 / 壞 JSON
    content_date = _tpex_content_date(obj)
    tables = obj.get("tables") or []
    if not tables:
        return content_date, pl.DataFrame([], schema=_PARSE_SCHEMA)
    fields = tables[0].get("fields")
    if fields:
        _guard(fields, _TPEX_GUARD, "TPEx")
    df = _frame(tables[0].get("data") or [])
    if df.height and content_date is None:
        raise parse.SchemaDrift("sbl_borrowing TPEx 有資料列卻無法解析內容日期(格式漂移?)")
    return content_date, df


def parse_raw(market: str, raw: bytes) -> tuple[Date | None, pl.DataFrame]:
    """依市場分流解析封存原始檔的 bytes → (內容日期, DF含company_name)。"""
    return parse_twse(raw) if market == "twse" else parse_tpex(raw)


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    """抓當日借券餘額 → **先原樣封存原始檔到 data/** → parse → 回 cache 9 欄 DF。

    原始檔封存鐵律:save_raw 在 parse 之前。內容日期 ≠ 請求日期 → `DateMismatch`
    拒收(稽核 BUG 1 根因守護);交易所回無資料(休市)→ None(交呼叫端寫 sentinel)。
    """
    if market == "twse":
        raw = http.fetch_bytes(_TWSE_URL.format(d=parse.twse_date(day)))
    else:
        raw = http.fetch_bytes(_TPEX_URL.format(d=parse.minguo_slash(day)))
    archive.save_raw(TABLE, market, day, raw)  # 原樣 bytes,位元保真
    content_date, df = parse_raw(market, raw)
    if df.is_empty():
        return None
    if content_date != day:
        raise DateMismatch(
            f"sbl_borrowing {market} 內容日期 {content_date} ≠ 請求 {day}"
            f"(過期報表;拒收不插入)")
    return df.with_columns(
        pl.lit(market).alias("market"),
        pl.lit(day).alias("date"),
    ).select(CACHE_COLS)
