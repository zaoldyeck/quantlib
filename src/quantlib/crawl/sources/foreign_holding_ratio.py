"""foreign_holding_ratio 源:外資及陸資投資持股比率**快照**(snapshot,日頻)。

- TWSE `MI_QFIIS`(Big5-HKSCS CSV,2005-01-03 起)。
- TPEx `insti/qfii`(UTF-8 JSON,存檔副檔名沿用 `.csv`,2011-01-03 起有真資料)。

與 daily_trading_details 的 `foreign_investors_difference`(當日淨買賣超 FLOW)不同:
本表是**累積總持股佔發行股數 %**(存量),`foreign_held_ratio` 逼近法令上限 =
外資接頂訊號。兩市場經抽取後**數值區塊 index 3~8 完全對齊**,只有 code/name 前綴不同:

    TWSE CSV(QuantlibCSVReader 去 = 與引號後):
      0 證券代號 | 1 證券名稱 | 2 ISIN | 3 發行股數 | 4 尚可投資股數 | 5 持有股數 |
      6 尚可投資比率 | 7 持股比率 | 8 (共用)法令投資上限 | 9 陸資上限(2009-10+) |
      10 與前日異動原因 | 11 最近申報日
    TPEx JSON(tables[0].data):
      0 排行 | 1 代號 | 2 名稱 | 3 發行股數 | 4 尚可投資股數 | 5 持有股數 |
      6 尚可投資比率 "%" | 7 持股比率 "%" | 8 法令上限 "%" | 9 備註
    → 兩者皆:code, name, outstanding=[3], remaining=[4], held=[5],
      remaining_ratio=[6], held_ratio=[7], limit_ratio=[8]。

移植自 `TradingReader.readForeignHoldingRatio`(TradingReader.scala:886-954),**忠實**
保留:欄位對位、值轉換(cleanCell 去逗號/%/空白 → toLong/toDouble)、市場分流、以
`[0-9][0-9A-Z]*` 過濾資料列、以 (market,date,code) 去重、`row.size>=10`/`<9` 過濾。

## 稽核發現、本 port 一次寫對的 bug(docs/data_audit/_done/{A,C}-foreign_holding_ratio.json)

1. **日期用內容標題、非只認檔名(BUG 1,根因守護)**。TPEx insti/qfii 端點對「沒有
   資料的日期」回**當下最新快照**;Scala reader 只認檔名(TradingReader.scala:899-901,
   tables[0].date 從沒被讀)→ 2010 整年 361 天被貼上 2026-04-24 的快照(319,124 列
   前視汙染,412 檔在 2010 還沒掛牌、111 個幽靈日)。本 port 從**內容**取日期(TWSE
   民國標題 `NNN年NN月NN日`、TPEx tables[0].date 民國 `YYY/MM/DD`),`fetch_day`
   內容日期 ≠ 請求日期即 `DateMismatch` **拒收**。這是 stock_per_pbr 之後同株病第二次。

2. **name-strip 只清數值欄(A 稽核 #5,實證為真 bug)**。Scala `cleanCell` 對**每一格**
   去空白,把含半形空白的 ETF 名稱打壞:`元大MSCI A股`→`元大MSCIA股`(PG 現存即壞值)。
   本 port 只對**數值欄**做 cleanCell,名稱僅 `strip()` 保留內部空白 → 名稱正確。

3. **型別 Int64**。發行股數 > 2^31(台積電 25,932,370,067),三個股數欄一律 Int64;
   純 Python int 解析無溢位,杜絕 int32 溢位。

4. **版型用明確表頭/fields 位置守衛**(`parse.SchemaDrift` fail-loud),不用 fallthrough
   ——TWSE 悄悄加欄地雷。TWSE 兩代標頭(2005「外資」/ 2010+「外資及陸資」)index 0-8
   語意全史對齊(稽核逐格確認),故守衛用「位置 → 關鍵字**子字串**」涵蓋兩代措辭
   (與 sbl_borrowing 的精確比對不同,因本源橫跨 ECFA 改名;差異在此明記)。

## 刻意不接的欄位(A 稽核 #3 SUSPECT;明記以免下一人重查)

原始檔有、但 cache/PG schema 從未接的 6 欄——接它們需改 Slick + ALTER TABLE
(Schema Contract),屬 schema 擴充而非 reader 移植,**本 unit 不碰 cache 寫入結構**,
故維持與現有表同構、不擅自加欄。留給後續 schema 擴充(`_record_*` 已是可擴充解析點):
  - TPEx[9] 備註(「已達上限」526 列 /「禁止投資」8,054 列)← 正是接頂訊號
  - TWSE[9] 陸資法令投資上限比率(與共用上限不同者 20.8%)
  - TWSE[10] 與前日異動原因、TWSE[11] 最近申報日(PIT)、TWSE[2] ISIN、TPEx[0] 排行

## 欄位/型別:回傳 DF 與 cache 表同構(9 欄,無 company_name)

cache `foreign_holding_ratio` 投影掉 PG 的 `id`/`company_name`(稽核 C)。`fetch_day`
回 cache 的 9 欄;`parse_raw` 另帶 `company_name`(供 parity 對 PG 逐欄比對 + 佐證
name-strip 修正)。`fetch_day` 走 archive→parse 不讀 cache;parity 測試讀 PG 當對照。
"""
from __future__ import annotations

import json
import re
from datetime import date as Date

import polars as pl

from quantlib.crawl import archive, http, parse

TABLE = "foreign_holding_ratio"
KEY_COLS = ["market", "date"]
MARKETS = ("twse", "tpex")

# application.conf: data.foreignHoldingRatio.{twse,tpex}.file
_TWSE_URL = ("https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS"
             "?response=csv&selectType=ALLBUT0999&date={d}")
_TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/insti/qfii?date={d}"

#: 資料列代號正規(對齊 Scala `[0-9][0-9A-Z]*`,全串匹配)。
_STOCK = re.compile(r"[0-9][0-9A-Z]*")

#: 數值區塊索引(TWSE / TPEx 抽取後一致):發行/尚可/持有 股數 + 三個比率。
_I_OUT, _I_REM, _I_HELD, _I_REMR, _I_HELDR, _I_LIMR = 3, 4, 5, 6, 7, 8
_MIN_COLS = 9  # 需存取到 index 8

#: parse_raw 輸出(含 company_name;fetch_day 會加 market/date 並投影成 cache 9 欄)。
_PARSE_SCHEMA = {
    "company_code": pl.Utf8, "company_name": pl.Utf8,
    "outstanding_shares": pl.Int64, "foreign_remaining_shares": pl.Int64,
    "foreign_held_shares": pl.Int64, "foreign_remaining_ratio": pl.Float64,
    "foreign_held_ratio": pl.Float64, "foreign_limit_ratio": pl.Float64,
}
#: cache 表 foreign_holding_ratio 的 9 欄(fetch_day 對齊;無 company_name)。
CACHE_COLS = ["market", "date", "company_code", "outstanding_shares",
              "foreign_remaining_shares", "foreign_held_shares",
              "foreign_remaining_ratio", "foreign_held_ratio", "foreign_limit_ratio"]

# 表頭位置守衛(index → 關鍵字**子字串**):跨代標頭措辭不同(「外資」↔「外資及陸資」),
# 但 index 0-8 語意全史對齊(稽核逐格確認);抓到位移就 fail-loud。
_TWSE_GUARD = {0: "證券代號", 3: "發行股數", 4: "尚可投資股數", 5: "持有股數",
               6: "尚可投資比率", 7: "持股比率", 8: "投資上限比率"}
_TPEX_GUARD = {1: "代號", 2: "名稱", 3: "發行股數", 5: "持有股數",
               7: "持股比率", 8: "法令投資上限"}

_TWSE_DATE_RE = re.compile(r"(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日")


class DateMismatch(parse.SchemaDrift):
    """內容自報日期 ≠ 請求/檔名日期——端點回別日快照,拒收不靜默插入(稽核 BUG 1 根因守護)。"""


def _guard(names: list[str], guard: dict[int, str], what: str) -> None:
    cells = [c.replace(" ", "") for c in names]
    for i, kw in guard.items():
        got = cells[i] if i < len(cells) else "<缺>"
        if kw not in got:  # 子字串:容跨代措辭,擋真位移
            raise parse.SchemaDrift(
                f"foreign_holding_ratio {what} 表頭位移:col[{i}] 期望含 '{kw}' "
                f"實得 '{got}'(TWSE/TPEx 改格式?)")


def _twse_content_date(rows: list[list[str]]) -> Date | None:
    """從 TWSE 標題列取民國日期(`NNN年NN月NN日 外資[及陸資]投資持股統計`)→ 西元。"""
    for r in rows[:3]:
        if r and (m := _TWSE_DATE_RE.search(r[0])):
            try:
                return Date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
            except ValueError:
                return None
    return None


def _tpex_content_date(obj: dict) -> Date | None:
    """TPEx JSON 內容日期:tables[0].date(民國 `YYY/MM/dd`)。"""
    tables = obj.get("tables") or []
    td = tables[0].get("date") if tables else None
    return parse.parse_minguo_slash(td) if isinstance(td, str) else None


def _record(cells: list[str], code_i: int, name_i: int) -> dict | None:
    """一列 → record;六數值欄任一非數值 → 整列丟棄(對齊 Scala 外層 Try{...}.toOption)。

    名稱只 `strip()`(保留內部空白,name-strip 修正);代號/數值走 cleanCell。
    """
    try:
        rec = {
            "company_code": parse.clean(cells[code_i]),
            "company_name": str(cells[name_i]).strip(),
            "outstanding_shares": int(parse.clean(cells[_I_OUT])),
            "foreign_remaining_shares": int(parse.clean(cells[_I_REM])),
            "foreign_held_shares": int(parse.clean(cells[_I_HELD])),
            "foreign_remaining_ratio": float(parse.clean(cells[_I_REMR])),
            "foreign_held_ratio": float(parse.clean(cells[_I_HELDR])),
            "foreign_limit_ratio": float(parse.clean(cells[_I_LIMR])),
        }
    except ValueError:
        return None
    return rec


def _frame(rows: list[list[str]], code_i: int, name_i: int) -> pl.DataFrame:
    """已抽取 raw 列 → 過濾(size/stockCode)→ 去重(by code,keep first)→ DF。"""
    seen: set[str] = set()
    recs: list[dict] = []
    for r in rows:
        if len(r) < _MIN_COLS or not _STOCK.fullmatch(str(r[code_i]).strip()):
            continue
        rec = _record(r, code_i, name_i)
        if rec is None or rec["company_code"] in seen:
            continue
        seen.add(rec["company_code"])
        recs.append(rec)
    return pl.DataFrame(recs, schema=_PARSE_SCHEMA)


def parse_twse(raw: bytes) -> tuple[Date | None, pl.DataFrame]:
    """TWSE Big5 CSV → (內容日期, DF)。表頭位置守衛 fail-loud。"""
    rows = parse.parse_csv(raw.decode(parse.TWSE_ENC, errors="replace"))
    content_date = _twse_content_date(rows)
    h = parse.find_header(rows, "證券代號")
    if h >= 0:
        _guard(rows[h], _TWSE_GUARD, "TWSE")
    df = _frame(rows, code_i=0, name_i=1)          # TWSE: code=[0], name=[1]
    if df.height and h < 0:
        raise parse.SchemaDrift("TWSE 有資料列卻找不到『證券代號』表頭(格式漂移?)")
    if df.height and content_date is None:
        raise parse.SchemaDrift("TWSE 有資料列卻無法從標題解析日期(格式漂移?)")
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
    df = _frame(tables[0].get("data") or [], code_i=1, name_i=2)  # TPEx: code=[1], name=[2]
    if df.height and content_date is None:
        raise parse.SchemaDrift("TPEx 有資料列卻無 tables[0].date(格式漂移?)")
    return content_date, df


def parse_raw(market: str, raw: bytes) -> tuple[Date | None, pl.DataFrame]:
    """依市場分流解析封存原始檔的 bytes → (內容日期, DF 含 company_name)。"""
    return parse_twse(raw) if market == "twse" else parse_tpex(raw)


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    """抓當日外資持股 → **先原樣封存原始檔到 data/** → parse → 回 cache 9 欄 DF。

    原始檔封存鐵律:save_raw 一定在 parse 之前(位元保真,先落地才解析)。
    內容日期閘門:端點對無資料日回別日快照 → 內容日期 ≠ 請求日期即 `DateMismatch`
    拒收(稽核 BUG 1 根因守護,2010 TPEx 汙染同型);交易所回無資料(休市)→ None。
    """
    if market == "twse":
        raw = http.fetch_bytes(_TWSE_URL.format(d=parse.twse_date(day)))
    elif market == "tpex":
        raw = http.fetch_bytes(_TPEX_URL.format(d=parse.minguo_slash(day)))
    else:
        raise ValueError(f"未知 market:{market}")
    archive.save_raw(TABLE, market, day, raw)      # 原樣 bytes,先落地再 parse
    content_date, df = parse_raw(market, raw)
    if df.is_empty():
        return None                                # 交易所回無資料 / 休市
    if content_date != day:
        raise DateMismatch(
            f"foreign_holding_ratio {market} 內容日期 {content_date} ≠ 請求 {day}"
            f"(端點回別日快照,2010 TPEx 汙染同型;拒收不插入)")
    return df.with_columns(
        pl.lit(market).alias("market"),
        pl.lit(day).alias("date"),
    ).select(CACHE_COLS)
