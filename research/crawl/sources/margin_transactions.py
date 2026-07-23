"""margin_transactions 源:TWSE MI_MARGN(信用交易統計)+ TPEx 融資融券餘額。

移植自 Scala `reader.TradingReader.readMarginTransactions`(TradingReader.scala:650-698)
與 `util.QuantlibCSVReader`(兩條 CSV 特規)。**忠實移植欄位對位/值轉換,同時把稽核
(docs/data_audit/_done/A-margin_transactions.json、C-margin_transactions.json)找出的
parser bug 一次寫對——不複製舊 bug。**

## 產出 schema(= PG `margin_transactions` 全 14 值欄,非 cache 的 7 欄 slim 投影)

Scala reader 寫入 PG 的 17 欄(market/date/company_code/company_name + 13 個整數欄)。
本 port 產出**同一組全欄**,理由:(1) parity 對照 PG 全 14 欄逐位;(2) 忠實取代 reader;
(3) 保留全資訊供 raw→cache 重建。cache 目前只投影 4 個數值欄(margin_balance=
margin_balance_of_the_day、short_balance=short_balance_of_the_day、margin_quota、
short_quota)——那是 cache_tables 下游投影的事,不是 parser 該丟資料的地方。

## Scala 欄位對位(逐格對過原始檔 + PG,見本源 parity 測試)

TWSE 資料列固定 17 格(idx15=註記、idx16=尾逗號空格,皆不入庫):
  0 代號 1 名稱 2 資買 3 資賣 4 現金償還 5 資前餘 6 資今餘 7 資限額
  8 券買(covering) 9 券賣(short sale) 10 現券償還 11 券前餘 12 券今餘 13 券限額 14 資券相抵

TPEx 資料列固定 20 格,**尾三欄(券限額/資券相抵/備註)版型隨年代位移**——這是稽核
BUG#1/#3 的根:Scala 寫死 era-C 索引(短額=v17、相抵=v18)套用全史,於是:
  · era A(<2007-06-01):真值 v17=券限額、v18=空、v19=資券相抵 → 寫死 v18 讓相抵恆 0(BUG#3)
  · era B(2007-06-01~2008-09-29):來源不印券限額(=資限額)、v17 裝的是資券相抵 →
    寫死 v17 讓 short_quota 變成相抵的小數字、v18 空讓相抵恆 0(BUG#1,48.5% 列 券餘>券限)
  · era C(≥2008-09-30):v17=券限額、v18=資券相抵、v19=備註(寫死索引在此才正確)
本 port 依**日期**分版型取值(下方 `_tpex_tail`),三段各有第一手原始檔實例佐證。

## 稽核 bug 修復清單(本 port 一次寫對)

- **BUG#1 tpex era B short_quota 錯位** → `_tpex_tail` era B:短額 = margin_quota
  (真券限額 = 資限額,來源未另印,由「券使用率=券餘/券限×100」反推證實),相抵 = v17。
- **BUG#2 tpex 2011-2014 右補空白代號被丟** → 代號**先 strip 空白再** regex 比對
  (Scala 在未清空白的 raw head 上比對 `"1336  "` → 失敗 → 靜靜丟 46.7 萬列)。
- **BUG#3 tpex era A(2007Q1)資券相抵重跑變 0** → `_tpex_tail` era A:相抵 = v19。
- **BUG#4 檔名≠內容日期的複製汙染** → `fetch_day` 落檔前比對內容民國標題日期,不符即
  `DateMismatch`(deferred、不封存);catches twse 2003-09-12/2011-03-26 這類前視汙染。
- **int32 溢位防線** → 數值欄一律 Int64(本源實測 max≈1,387 萬 < int32,但 Int64 零風險)。
- **company_name 逐格正規化(= Scala,忠實對齊 PG)** → Scala 對**每個** cell 一律
  `.replace(" ","").replace(",","")`,名稱同受此清洗。**不可只 `.strip()` 去頭尾**——
  舊 TWSE 用半形空白補齊短名(2322 於 2001-10 印成「致  福」),另有 4 檔 ETF 名稱內含
  半形空白(元大MSCI A股 / 新光Shiller CAPE / 凱基ESG BBB債15+ / 中信上櫃ESG 30);
  全史 4,711 列 PG 一律存**去空白版**(元大MSCIA股…),只 `.strip()` 會與 PG 逐位不符
  (本源 14 欄 PG parity 實測定位:唯一非 bug-fix 的名稱分歧就是這批)。
- **版型用明確 case 不用 fallthrough** → 通過代號比對的股票列寬度必須 == 17(twse)/20
  (tpex),否則 `SchemaDrift` fail-loud(Scala 的 `row.size>=22` 分支全史零命中,是死碼)。
- **margin_quota 的 idx8/idx9 對調保命索**(tpex 2008-01-25 交易所印反)→ 保留
  `try v9 except v8`,但改成具名函式 + 註解標明第一手來源(Scala 是無註解的 Try.getOrElse)。

## 整合層(update.py 接線時處理,非單檔 parser 能為;不在此半實作以免留假防線)

- **休市日曆超集守護**:margin 的 (market,date) 必須 ⊇ daily_quote 同市場;缺日
  (10 個真交易日 4-byte 空回應)靠重抓補,需日曆判定。
- **TPEx 颱風幽靈日整日內容指紋去重**:TPEx 對休市日回上一交易日資料且**標頭日期是對的**
  (與 twse 不同),`fetch_day` 的內容日期守護抓不到,只有跨日整日指紋能抓;屬 update.py
  的跨檔職責。此二者稽核 fix 明列,且與「不改 update.py」邊界一致。

Run(parity):uv run --project research python -m research.crawl.tests.test_margin_transactions_parity
"""
from __future__ import annotations

import csv
import re
from datetime import date as Date

import polars as pl

from research.crawl import archive, http
from research.crawl.parse import SchemaDrift, minguo_slash, twse_date

TABLE = "margin_transactions"
KEY_COLS = ["market", "date"]
MARKETS = ("twse", "tpex")

# application.conf data.marginTransactions.{twse,tpex}.file(日期由 {d} 帶入)
_TWSE_URL = ("https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
             "?response=csv&selectType=ALL&date={d}")
_TPEX_URL = ("https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/"
             "margin_bal_result.php?l=zh-tw&o=csv&d={d}")

#: TPEx 尾三欄版型切換日(第一手證據:docs/data_audit/_done/A-margin_transactions.json
#: BUG#1「2007-06-01 起 idx17 命中率變 0、2008-09-30 起恢復」;逐日原始檔已覆核)。
_TPEX_ERA_B_START = Date(2007, 6, 1)   # era A→B:券限額欄從有到無(=資限額)
_TPEX_ERA_C_START = Date(2008, 9, 30)  # era B→C:券限額欄回歸 v17、相抵回 v18

#: 產出欄 = PG margin_transactions 全 14 值欄(去 id);數值一律 Int64。
_SCHEMA: dict[str, pl.DataType] = {
    "market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
    "company_name": pl.Utf8,
    "margin_purchase": pl.Int64, "margin_sales": pl.Int64, "cash_redemption": pl.Int64,
    "margin_balance_of_previous_day": pl.Int64, "margin_balance_of_the_day": pl.Int64,
    "margin_quota": pl.Int64,
    "short_covering": pl.Int64, "short_sale": pl.Int64, "stock_redemption": pl.Int64,
    "short_balance_of_previous_day": pl.Int64, "short_balance_of_the_day": pl.Int64,
    "short_quota": pl.Int64,
    "offsetting_of_margin_purchases_and_short_sales": pl.Int64,
}

#: 代號正則(Scala StockCode = "[0-9][0-9A-Z]*",full-match)。
_STOCK_CODE = re.compile(r"[0-9][0-9A-Z]*")

#: 民國標題日期。twse 標題「YYY年MM月DD日 信用交易統計」;tpex「資料日期:YYY/MM/DD」。
_TWSE_TITLE_DATE = re.compile(r"(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")
_TPEX_TITLE_DATE = re.compile(r"資料日期[:：]\s*(\d{2,3})/(\d{1,2})/(\d{1,2})")

#: twse 標頭首格兩簽章(2024-10 起改名、欄序不變:股票代號→代號、限額→次一營業日限額)。
_TWSE_HEADER_FIRST = {"股票代號", "代號"}


class DateMismatch(RuntimeError):
    """內容標題日期 ≠ 檔名/請求日期:交易所對休市/失敗回了別天的資料(稽核 BUG#4)。

    落檔前攔下 → 標 deferred 重抓,絕不以錯誤檔名封存(否則就是把前視汙染寫進事實地基)。
    """


# --------------------------------------------------------------------------- #
# CSV 讀取:忠實移植 util.QuantlibCSVReader 的兩條特規                          #
# --------------------------------------------------------------------------- #
def _read_rows(text: str) -> list[list[str]]:
    """Big5 解碼後文字 → rows,原樣複製 QuantlibCSVReader 兩條特規:

    - 第 21 行:一行**含 `""` 但不含 `,""`** → 跳過整列(malformed 護欄)。
    - 第 23 行:`line.replace("=", "")`(去掉 TWSE `="0050"` Excel 護甲的 `=`)。

    這兩條決定「哪些列被讀進來/被丟掉」,必須逐位相同才能對 PG parity。多行引號欄在本
    corpus 不存在(每筆一行,稽核全掃 badlen=0),故不需 tototoshi 的 leftover 累積。
    """
    out: list[list[str]] = []
    for raw_line in text.split("\n"):
        line = raw_line.rstrip("\r")
        if not line:
            continue
        if '""' in line and ',""' not in line:      # QuantlibCSVReader.scala:21
            continue
        line = line.replace("=", "")                 # QuantlibCSVReader.scala:23
        out.append(next(csv.reader([line])))
    return out


def _clean(cell: str) -> str:
    """逐格清洗,對齊 Scala 對**每個** cell 的 `.replace(" ","").replace(",","")`
    (Scala `rows.map(_.map(_.replace…))` 是全欄套用:代號、company_name、數值欄一體適用)。"""
    return cell.replace(" ", "").replace(",", "")


def _i(cell: str) -> int:
    """嚴格整數(對齊 Scala `.toInt`:非整數就拋 → fail-loud,不靜默補 0)。"""
    return int(_clean(cell))


def _int_or_zero(cell: str) -> int:
    """空/非整數 → 0(對齊 Scala 相抵欄的 `if nonEmpty … Try(...).getOrElse(0) else 0`)。"""
    c = _clean(cell)
    if not c:
        return 0
    try:
        return int(c)
    except ValueError:
        return 0


def _content_date(text: str, market: str) -> Date | None:
    """從檔案內容抽民國標題日期 → 西元 date(抓不到回 None)。"""
    pat = _TWSE_TITLE_DATE if market == "twse" else _TPEX_TITLE_DATE
    m = pat.search(text)
    if not m:
        return None
    return Date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))


# --------------------------------------------------------------------------- #
# 標頭位置守衛(SchemaDrift fail-loud;比照 daily_quote._guard)                 #
# --------------------------------------------------------------------------- #
def _find_header(rows: list[list[str]], first_cells: set[str]) -> list[str] | None:
    for r in rows:
        if r and r[0].replace(" ", "").replace('"', "").strip() in first_cells:
            return r
    return None


def _guard_twse(rows: list[list[str]]) -> None:
    hdr = _find_header(rows, _TWSE_HEADER_FIRST)
    if hdr is None:
        return  # 空表/休市檔無股票區標頭 → 無資料列可驗(寬度守衛仍會擋錯位資料)
    cells = [c.replace(" ", "") for c in hdr]

    def want(i: int, ok, desc: str) -> None:
        got = cells[i] if i < len(cells) else "<缺>"
        if i >= len(cells) or not ok(cells[i]):
            raise SchemaDrift(f"twse margin 標頭位移 col[{i}] 期望 {desc} 實得 '{got}'")

    want(4, lambda c: c == "現金償還", "現金償還")
    want(7, lambda c: c.endswith("限額"), "…限額(資)")
    want(10, lambda c: c == "現券償還", "現券償還")
    want(13, lambda c: c.endswith("限額"), "…限額(券)")
    want(14, lambda c: c == "資券互抵", "資券互抵")
    want(15, lambda c: c == "註記", "註記")


def _guard_tpex(rows: list[list[str]]) -> None:
    hdr = _find_header(rows, {"代號"})
    if hdr is None:
        return
    cells = [c.replace(" ", "") for c in hdr]

    def want(i: int, prefix: str) -> None:
        got = cells[i] if i < len(cells) else "<缺>"
        if i >= len(cells) or not cells[i].startswith(prefix):
            raise SchemaDrift(f"tpex margin 標頭位移 col[{i}] 期望 '{prefix}…' 實得 '{got}'")

    for i, prefix in ((3, "資買"), (4, "資賣"), (6, "資餘額"), (9, "資限額"),
                      (11, "券賣"), (12, "券買"), (14, "券餘額"),
                      (17, "券限額"), (18, "資券相抵")):
        want(i, prefix)


# --------------------------------------------------------------------------- #
# 解析:TWSE / TPEx                                                             #
# --------------------------------------------------------------------------- #
def _parse_twse(text: str, day: Date) -> pl.DataFrame | None:
    rows = _read_rows(text)
    _guard_twse(rows)
    recs = []
    for r in rows:
        code = _clean(r[0])                       # BUG#2 修:先清空白再比對代號
        if not _STOCK_CODE.fullmatch(code):
            continue
        if len(r) != 17:                          # 明確 case:股票列必為 17 格,否則 fail-loud
            raise SchemaDrift(f"twse margin {day} 股票列 {code} 寬度 {len(r)} != 17")
        recs.append({
            "market": "twse", "date": day, "company_code": code,
            "company_name": _clean(r[1]),         # 對齊 Scala/PG:去內部空白+逗號(非只 strip)
            "margin_purchase": _i(r[2]), "margin_sales": _i(r[3]),
            "cash_redemption": _i(r[4]),
            "margin_balance_of_previous_day": _i(r[5]),
            "margin_balance_of_the_day": _i(r[6]),
            "margin_quota": _i(r[7]),
            "short_covering": _i(r[8]), "short_sale": _i(r[9]),
            "stock_redemption": _i(r[10]),
            "short_balance_of_previous_day": _i(r[11]),
            "short_balance_of_the_day": _i(r[12]),
            "short_quota": _i(r[13]),
            "offsetting_of_margin_purchases_and_short_sales": _i(r[14]),
        })
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


def _tpex_margin_quota(r: list[str]) -> int:
    """資限額 = v9;唯一例外 tpex 2008-01-25 交易所把 idx8/idx9 對調(v9 印成使用率
    「32.16」)→ 退回 v8。第一手:data/margin_transactions/tpex/2008/2008_1_25.csv
    的 1333 恩得利 …,"1,030","20,756","32.16",…(標頭宣告 idx8=資使用率、idx9=資限額)。
    Scala 原為無註解的 `Try(values(9).toInt).getOrElse(values(8).toInt)`(全史僅此日觸發)。
    """
    try:
        return _i(r[9])
    except ValueError:
        return _i(r[8])


def _tpex_tail(r: list[str], day: Date, margin_quota: int) -> tuple[int, int]:
    """回 (short_quota, offsetting),依日期分三段版型(稽核 BUG#1/#3 的根因修復)。"""
    if day < _TPEX_ERA_B_START:                   # era A:v17=券限額、v18=空、v19=資券相抵
        return _i(r[17]), _int_or_zero(r[19])     #   BUG#3 修:相抵取 v19(非寫死 v18→0)
    if day < _TPEX_ERA_C_START:                   # era B:來源不印券限額(=資限額)、v17=相抵
        return margin_quota, _int_or_zero(r[17])  #   BUG#1 修:短額=資限額、相抵=v17
    return _i(r[17]), _int_or_zero(r[18])         # era C:寫死索引在此才正確


def _parse_tpex(text: str, day: Date) -> pl.DataFrame | None:
    rows = _read_rows(text)
    _guard_tpex(rows)
    recs = []
    for r in rows:
        code = _clean(r[0])                       # BUG#2 修:"1336  " → "1336" 才不被丟
        if not _STOCK_CODE.fullmatch(code):
            continue
        if len(r) != 20:
            raise SchemaDrift(f"tpex margin {day} 股票列 {code} 寬度 {len(r)} != 20")
        margin_quota = _tpex_margin_quota(r)
        short_quota, offsetting = _tpex_tail(r, day, margin_quota)
        recs.append({
            "market": "tpex", "date": day, "company_code": code,
            "company_name": _clean(r[1]),         # 對齊 Scala/PG:去內部空白+逗號(非只 strip)
            "margin_purchase": _i(r[3]), "margin_sales": _i(r[4]),
            "cash_redemption": _i(r[5]),
            "margin_balance_of_previous_day": _i(r[2]),
            "margin_balance_of_the_day": _i(r[6]),
            "margin_quota": margin_quota,
            "short_covering": _i(r[12]), "short_sale": _i(r[11]),
            "stock_redemption": _i(r[13]),
            "short_balance_of_previous_day": _i(r[10]),
            "short_balance_of_the_day": _i(r[14]),
            "short_quota": short_quota,
            "offsetting_of_margin_purchases_and_short_sales": offsetting,
        })
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


def parse(market: str, text: str, day: Date) -> pl.DataFrame | None:
    """純解析(供 fetch_day 與 parity 測試共用):market + 已解碼文字 + 權威日期 → DF。"""
    return _parse_twse(text, day) if market == "twse" else _parse_tpex(text, day)


def _req_date(market: str, day: Date) -> str:
    """端點日期參數:twse 西元 yyyymmdd;tpex 民國 y/MM/dd(稽核實測 d=112/06/09)。"""
    return twse_date(day) if market == "twse" else minguo_slash(day)


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    """抓當日融資融券 → **內容日期完整性驗證 → 原樣封存 data/ → 解析**(原始檔封存鐵律)。

    順序:先抓 bytes,**落檔前**比對內容標題民國日期(BUG#4:休市/失敗時交易所會回別天的
    資料且把請求日印在標題,twse 尤然)——不符即 `DateMismatch`(deferred、不封存錯檔),
    符合才 `archive.save_raw` 再 parse。回 None = 該日無股票資料列(休市/空回應),由呼叫端
    依日曆決定寫 sentinel 或標 deferred 重抓(見模組 docstring「整合層」)。
    """
    url = (_TWSE_URL if market == "twse" else _TPEX_URL).format(d=_req_date(market, day))
    raw = http.fetch_bytes(url)
    text = raw.decode("Big5-HKSCS", errors="replace")
    cdate = _content_date(text, market)
    if cdate is not None and cdate != day:
        raise DateMismatch(
            f"{market} margin {day}:內容標題日期 {cdate} ≠ 請求日 {day} → 汙染回應,"
            f"[deferred] 不封存重抓(稽核 BUG#4)")
    archive.save_raw(TABLE, market, day, raw)     # 位元保真(原始 bytes),tmp→os.replace
    return parse(market, text, day)
