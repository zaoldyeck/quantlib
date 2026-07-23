"""daily_quote 源:TWSE MI_INDEX + TPEx stk_wn1430。**吃全部歷史格式世代**。

歷史格式世代(以 header 欄數判定,明確 case、無 fallthrough;各世代固定欄位對映):

- **TWSE**:單一世代,17 欄(16 named + 結尾逗號的空欄),2004-02-11 起零漂移。
  欄序:證券代號0 證券名稱1 成交股數2 成交筆數3 成交金額4 開盤價5 最高價6 最低價7
  收盤價8 漲跌(+/-)9 漲跌價差10 最後揭示買價11 買量12 賣價13 賣量14 本益比15 (空16)。
- **TPEx 世代一**:15 欄,2007-07-02 ~ 2020-04-29(無最後買/賣量)。
  欄序:代號0 名稱1 收盤2 漲跌3 開盤4 最高5 最低6 成交股數7 成交金額(元)8 成交筆數9
  最後買價10 最後賣價11 發行股數12 次日漲停13 次日跌停14。
- **TPEx 世代二**:17 欄,2020-04-30 起(新增最後買量11/賣量13,發行股數等右移)。
  2025-01-10 起量欄名由「(千股)」改「(張數)」,**僅欄名變、位置不變**——故守衛只釘
  穩定的價格欄,不守衛會改名的量欄。

cache 欄(刻意只投影 11 欄;company_name/成交筆數/漲跌/最後買賣量/本益比/發行股數
不入 cache,對齊 C-daily_quote 記載的取捨):market, date, company_code,
opening/highest/lowest/closing_price, trade_volume, trade_value,
last_best_bid_price, last_best_ask_price。

移植自 TradingReader.readDailyQuote,並修掉稽核(A/C-daily_quote)記載的解析層 bug:
- **15 欄 TPEx 世代原本被 header 守衛誤判 SchemaDrift**:舊守衛位置 12 期望「最後賣價」,
  但 15 欄世代該位置是「發行股數」→ 2007-2020 全史上櫃無法從 raw 重建。改為逐世代守衛。
- **整列 `.replace(" ","")` 會抹掉公司名內部空白**(元大MSCI A股→元大MSCIA股)→ 只清
  要解析的數值欄,代號/名稱不做全域去空白(本 parser 不投影 company_name,結構上杜絕)。
- **半截檔靜默吞列**(2017-04-17 上櫃下載中斷,21 檔整天遺失,舊碼 `size>=15` 靜靜丟掉)
  → 逐列欄寬檢查 + TPEx「共N筆」完整性守衛,不符即 fail-loud(SchemaDrift),絕不靜默
  產出殘缺日(下游 rebuild 收集為錯誤 → 標記重抓,不汙染 cache)。
- 值轉換:TWSE `--`→null、``/`X`→0、`+`→1、`-`→-1、else float;TPEx `---`/`----`→null、
  除權息字樣→0、else float。成交股數/金額全史皆為整數字串(掃描確認無哨兵),int 化零溢位。

日期取自檔名(rebuild)/呼叫端(fetch):A-daily_quote 全史核對 15,151 檔「檔名日期 ==
內容標頭日期」零不符,故不需從內容重讀;唯一幽靈日 2009-12-12 是 TWSE 對非交易日回聲
請求日期(內容標頭亦寫 12-12),單檔解析器無法辨識,由 rebuild/crawler 層的跨日內容
指紋守衛負責(C-daily_quote),非本解析器職責。
"""
from __future__ import annotations

import re
from datetime import date as Date

import polars as pl

from quantlib.crawl import archive, http, parse

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

# --- 世代欄位對映(值 = header 位置)+ header 守衛(位置→期望欄名,抓位移即 fail-loud)
# TWSE 17 欄(單一世代)
_TWSE_NCOLS = 17
_TWSE_COLS = {"code": 0, "vol": 2, "val": 4, "open": 5, "high": 6, "low": 7,
              "close": 8, "bid": 11, "ask": 13}
_TWSE_GUARD = {2: "成交股數", 4: "成交金額", 5: "開盤價", 8: "收盤價",
               11: "最後揭示買價", 13: "最後揭示賣價"}
# TPEx 世代一 15 欄(2007-07-02~2020-04-29)
_TPEX15_COLS = {"code": 0, "vol": 7, "val": 8, "open": 4, "high": 5, "low": 6,
                "close": 2, "bid": 10, "ask": 11}
_TPEX15_GUARD = {2: "收盤", 4: "開盤", 5: "最高", 6: "最低", 7: "成交股數",
                 10: "最後買價", 11: "最後賣價"}
# TPEx 世代二 17 欄(2020-04-30 起;最後買量11/賣量13 插在買價/賣價後)
_TPEX17_COLS = {"code": 0, "vol": 7, "val": 8, "open": 4, "high": 5, "low": 6,
                "close": 2, "bid": 10, "ask": 12}
_TPEX17_GUARD = {2: "收盤", 4: "開盤", 5: "最高", 6: "最低", 7: "成交股數",
                 10: "最後買價", 12: "最後賣價"}

_CODE_RE = re.compile(r"[0-9][0-9A-Za-z]*")
_TOTAL_RE = re.compile(r"共(\d+)筆")


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


def _num_int(v: str, numf) -> int:
    """成交股數/金額 → int。整數字串直接 int(無小數、零 float 溢位);哨兵(null 化)
    → 0(全史掃描確認量/額欄永不出現哨兵,此分支僅防禦)。"""
    if v.lstrip("-").isdigit():
        return int(v)
    x = numf(v)
    return int(x) if x is not None else 0


def _is_code(cell: str) -> bool:
    """資料列辨識:首欄為股票代號(數字開頭的英數)。排除『共N筆』/備註/註記/空列。"""
    return _CODE_RE.fullmatch(cell.strip()) is not None


def _guard(header: list[str], guard: dict[int, str], what: str) -> None:
    cells = [c.replace(" ", "") for c in header]
    for i, name in guard.items():
        if i >= len(cells) or cells[i] != name:
            got = cells[i] if i < len(cells) else "<缺>"
            raise parse.SchemaDrift(f"daily_quote {what} 欄位位移:col[{i}] 期望 "
                                    f"'{name}' 實得 '{got}'(TWSE/TPEx 改格式?)")


def _tpex_declared_count(rows: list[list[str]]) -> int | None:
    """TPEx 每檔尾列『共N筆』自報的證券數(截斷檔如 2017-04-17 無此列)。"""
    for r in rows:
        for cell in r:
            mm = _TOTAL_RE.search(cell.replace(" ", ""))
            if mm:
                return int(mm.group(1))
    return None


def _parse_twse(text: str, day: Date) -> pl.DataFrame | None:
    rows = parse.parse_csv(text)
    h = parse.find_header(rows, "證券代號")
    if h < 0:
        return None
    header = rows[h]
    if len(header) != _TWSE_NCOLS:
        raise parse.SchemaDrift(
            f"daily_quote TWSE {day} 未知 header 欄數 {len(header)}(期望 {_TWSE_NCOLS})")
    _guard(header, _TWSE_GUARD, "TWSE")
    m = _TWSE_COLS
    recs = []
    for r in rows[h + 1:]:
        if not r or not _is_code(r[0]):
            continue  # 備註區/空列/非證券列
        if len(r) != _TWSE_NCOLS:
            raise parse.SchemaDrift(
                f"daily_quote TWSE {day} 資料列欄寬異常:code={r[0].strip()} 得 "
                f"{len(r)} 欄(期望 {_TWSE_NCOLS})——原始檔可能下載截斷")
        recs.append({
            "market": "twse", "date": day, "company_code": r[m["code"]].strip(),
            "opening_price": _twse_num(parse.clean(r[m["open"]])),
            "highest_price": _twse_num(parse.clean(r[m["high"]])),
            "lowest_price": _twse_num(parse.clean(r[m["low"]])),
            "closing_price": _twse_num(parse.clean(r[m["close"]])),
            "trade_volume": _num_int(parse.clean(r[m["vol"]]), _twse_num),
            "trade_value": _num_int(parse.clean(r[m["val"]]), _twse_num),
            "last_best_bid_price": _twse_num(parse.clean(r[m["bid"]])),
            "last_best_ask_price": _twse_num(parse.clean(r[m["ask"]])),
        })
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


def _parse_tpex(text: str, day: Date) -> pl.DataFrame | None:
    rows = parse.parse_csv(text)
    h = parse.find_header(rows, "代號")
    if h < 0:
        return None  # 休市「共0筆」回應或 0-byte sentinel
    header = rows[h]
    n = len(header)
    if n == 15:
        m, guard = _TPEX15_COLS, _TPEX15_GUARD
    elif n == 17:
        m, guard = _TPEX17_COLS, _TPEX17_GUARD
    else:
        raise parse.SchemaDrift(
            f"daily_quote TPEx {day} 未知 header 欄數 {n}(僅支援 15/17 欄世代)")
    _guard(header, guard, f"TPEx-{n}")
    body = rows[h + 1:]
    recs = []
    for r in body:
        if not r or not _is_code(r[0]):
            continue  # 『共N筆』/ETF 註記/空列
        if len(r) != n:
            raise parse.SchemaDrift(
                f"daily_quote TPEx {day} 資料列欄寬異常:code={r[0].strip()} 得 "
                f"{len(r)} 欄(期望 {n})——原始檔可能下載截斷")
        recs.append({
            "market": "tpex", "date": day, "company_code": r[m["code"]].strip(),
            "opening_price": _tpex_num(parse.clean(r[m["open"]])),
            "highest_price": _tpex_num(parse.clean(r[m["high"]])),
            "lowest_price": _tpex_num(parse.clean(r[m["low"]])),
            "closing_price": _tpex_num(parse.clean(r[m["close"]])),
            "trade_volume": _num_int(parse.clean(r[m["vol"]]), _tpex_num),
            "trade_value": _num_int(parse.clean(r[m["val"]]), _tpex_num),
            "last_best_bid_price": _tpex_num(parse.clean(r[m["bid"]])),
            "last_best_ask_price": _tpex_num(parse.clean(r[m["ask"]])),
        })
    # 完整性守衛:TPEx 每檔尾列『共N筆』必存在且等於解析列數。截斷檔(2017-04-17)
    # 無此列或列數不符 → fail-loud,不靜默產出殘缺日(對齊 A-daily_quote 修法 ③④)。
    declared = _tpex_declared_count(body)
    if declared is None:
        raise parse.SchemaDrift(
            f"daily_quote TPEx {day} 缺『共N筆』結尾——原始檔可能下載中斷")
    if declared != len(recs):
        raise parse.SchemaDrift(
            f"daily_quote TPEx {day} 列數不符:自報共 {declared} 筆 但解析 {len(recs)} 列"
            "——原始檔可能截斷")
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    """抓當日報價 → **先原樣封存原始檔到 data/** → 再 parse(原始檔封存鐵律)。"""
    if market == "twse":
        raw = http.fetch_bytes(_TWSE_URL.format(d=parse.twse_date(day)))
        archive.save_raw("daily_quote", "twse", day, raw)   # 原樣 bytes(位元保真)
        return _parse_twse(raw.decode("Big5-HKSCS", errors="replace"), day)
    raw = http.fetch_bytes(_TPEX_URL.format(d=parse.minguo_slash(day)))
    archive.save_raw("daily_quote", "tpex", day, raw)
    return _parse_tpex(raw.decode("Big5-HKSCS", errors="replace"), day)
