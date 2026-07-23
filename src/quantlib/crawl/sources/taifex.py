"""taifex 源:TAIFEX 期貨每日行情(單式契約 + 價差/組合式契約)。

移植自 `TradingReader.readTaifexFuturesDaily`。cache 表 `taifex_futures_daily`
**無 market 欄**;唯一鍵 = (date, contract_code, contract_month, trading_session)。

## 端點(application.conf `data.taifex.futuresDaily`)

POST https://www.taifex.com.tw/cht/3/futDataDown

- **年檔**(歷史):`down_type=2, his_year=YYYY` → 回 ZIP,內含單一 `<year>_fut.csv`
  (Big5-HKSCS)。1998 起每年一檔;抓到 `today.year - 1`(去年)為止。
- **月檔**(當年):`down_type=1, queryStartDate/queryEndDate(yyyy/MM/dd),
  commodity_id=all, commodity_id2=""` → 回當月區間 CSV。

**封存版型**(沿用 Scala 慣例,故 parity 讀得到既有檔):
  年檔 `data/taifex/futures_daily/<year>_fut.csv`(頂層,無年子目錄)
  月檔 `data/taifex/futures_daily/<year>/<year>_<m>.csv`(年子目錄)
**原始檔封存鐵律**:fetch → `archive.save_raw_bytes_at` 先原樣落地 → 再 parse。

## 欄位對位(16/17/19 欄表頭世代;資料列 idx 0-18 固定,idx19 尾逗號空欄忽略)

    0 交易日期      → date(**內容**西元 yyyy/M/d lenient,非檔名)
    1 契約          → contract_code(trim)
    2 到期月份(週別) → contract_month(trim;價差契約形如 '202506/202507')
    3 開盤價        → open           4 最高價 → high      5 最低價 → low
    6 收盤價        → close          7 漲跌價 → change    8 漲跌%  → change_pct
    9 成交量        → volume(None→0) 10 結算價 → settlement_price
   11 未沖銷契約數  → open_interest  12 最後最佳買價 → best_bid
   13 最後最佳賣價  → best_ask       14 歷史最高價 → historical_high
   15 歷史最低價    → historical_low
   16 是否因訊息面暫停交易 → trading_halt(idx>16 才有,trim,空→None)
   17 交易時段      → trading_session(idx>17 且非空,否則「一般」)
   18 價差對單式委託成交量 → spread_single_volume(idx>18 才有)

## 稽核修復(A-taifex BUG,本 port 一次寫對、不複製)

Scala `taifexOptPrice = taifexOptDouble.filter(_ > 0.0)`,把**價差(組合式)契約**
合法的**負值/零值報價**無聲清成 NULL——價差報的是月份間價差、本來就會負或 0。
受害欄:open/high/low/close/settlement_price/best_bid/best_ask/historical_high/
historical_low。PG 723,826 筆價差契約有 573,097 筆 close 變 NULL(其中 287,394 筆
volume>0、確實成交卻查無成交價)。**修法**:9 個價格欄一律用 `_opt_double`
(允許負/零,只在空字串/`-`/`--` 時 None),移除 `> 0` 過濾。其原意(濾未成交外月
的佔位值)原始檔早已用 `-` 表達、`_opt_double` 本就轉 None,故 `>0` 過濾對**單式契約
多餘(值恆正,no-op)、對價差契約有害**——單式契約逐位零漂移(parity 驗證)。

其餘欄位、單式契約、四種欄式世代經稽核逐欄比對保真,忠實照搬;dedup 依 Scala
「同鍵取最完整列」語義。header 位置守衛(SchemaDrift fail-loud)為本 port 新增,
Scala 原無——擋 TWSE/TAIFEX 悄悄插欄的無聲錯位。

Run(月檔補抓範例):
    uv run --project . python -c "from quantlib.crawl.sources import taifex; \
        print(taifex.fetch_month(2026, 6).height)"

依賴 cache_tables.py:否(本模組 fetch/parse 不讀 cache;parity 測試才讀 PG 對照)。
"""
from __future__ import annotations

import csv
import io
import zipfile
from datetime import date as Date, timedelta

import polars as pl

from quantlib import paths
from quantlib.crawl import archive, http, parse

TABLE = "taifex_futures_daily"
#: 批次 upsert key = Scala 的 delete-by-date(一個年/月檔含多日,整日刪+插)。
KEY_COLS = ["date"]
#: 去重/唯一鍵(對齊 PG unique index);dedup 在此鍵上收斂為最完整一列。
UNIQUE_COLS = ["date", "contract_code", "contract_month", "trading_session"]
#: taifex 無市場維度,單一命名空間(介面一致性;fetch 不依 market 分流)。
MARKETS = ("taifex",)

_ENDPOINT = "https://www.taifex.com.tw/cht/3/futDataDown"
#: Scala data.taifex.futuresDaily.dir 的葉目錄(封存根 = data/taifex/futures_daily)。
_RAW_DIR = paths.RAW / "taifex" / "futures_daily"

_SCHEMA = {
    "date": pl.Date, "contract_code": pl.Utf8, "contract_month": pl.Utf8,
    "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
    "change": pl.Float64, "change_pct": pl.Float64, "volume": pl.Int64,
    "settlement_price": pl.Float64, "open_interest": pl.Int64,
    "best_bid": pl.Float64, "best_ask": pl.Float64,
    "historical_high": pl.Float64, "historical_low": pl.Float64,
    "trading_halt": pl.Utf8, "trading_session": pl.Utf8,
    "spread_single_volume": pl.Int64,
}

#: 表頭位置守衛(idx → 期望欄名,空白已剝除)。核心 0-15 跨全世代恆同;16/17/18 為
#: 後加世代欄,只在表頭夠長時比對(舊 16/17 欄檔合法沒有,不誤報)。
_HEADER_GUARD = {
    0: "交易日期", 1: "契約", 2: "到期月份(週別)", 3: "開盤價", 4: "最高價",
    5: "最低價", 6: "收盤價", 7: "漲跌價", 8: "漲跌%", 9: "成交量", 10: "結算價",
    11: "未沖銷契約數", 12: "最後最佳買價", 13: "最後最佳賣價", 14: "歷史最高價",
    15: "歷史最低價", 16: "是否因訊息面暫停交易", 17: "交易時段",
    18: "價差對單式委託成交量",
}


# --------------------------------------------------------------------------- #
# 值轉換(對齊 Scala helper;價格欄的 >0 過濾已移除 = 稽核修復)                   #
# --------------------------------------------------------------------------- #
def _clean(s: str) -> str:
    """Scala cleanCell:去逗號/百分號/空白 + trim。"""
    return s.replace(",", "").replace("%", "").replace(" ", "").strip()


def opt_double(v: str) -> float | None:
    """Scala taifexOptDouble:清洗後 ''/`-`/`--` → None,否則 float(含負值與 0)。

    **價格欄一律用本函式**(不再 filter >0)——這就是 A-taifex BUG 的修復點。
    """
    c = _clean(v)
    if c in ("", "-", "--"):
        return None
    try:
        return float(c)
    except ValueError:
        return None


def _long(v: str) -> int:
    """Scala taifexLong:None → 0,否則截尾為整數(Double.toLong 向零截斷)。"""
    d = opt_double(v)
    return int(d) if d is not None else 0


def _opt_long(v: str) -> int | None:
    """Scala taifexOptLong:None 保留,否則截尾為整數。"""
    d = opt_double(v)
    return int(d) if d is not None else None


def parse_date(s: str) -> Date | None:
    """Scala parseTaifexDate:西元 yyyy/M/d(lenient,吃 '2014/1/2' 與 '2018/01/02')。

    非民國;不可解析(如表頭 '交易日期')回 None → 該列被濾除。
    """
    parts = s.strip().split("/")
    if len(parts) != 3:
        return None
    try:
        return Date(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, TypeError):
        return None


def _guard_header(rows: list[list[str]]) -> None:
    """找到表頭列(row[0]=='交易日期')並驗證欄位位置;錯位即 fail-loud。"""
    header = next((r for r in rows if r and r[0].strip() == "交易日期"), None)
    if header is None:
        raise parse.SchemaDrift("taifex:找不到表頭列(row[0]=='交易日期')——版型變更?")
    cells = [c.replace(" ", "") for c in header]
    if len(cells) < 16:
        raise parse.SchemaDrift(f"taifex:表頭僅 {len(cells)} 欄(<16)——非期貨日檔或截斷")
    for i, name in _HEADER_GUARD.items():
        if i < len(cells) and cells[i] != name:
            raise parse.SchemaDrift(
                f"taifex 欄位位移:col[{i}] 期望 '{name}' 實得 '{cells[i]}'(TAIFEX 改格式?)")


# --------------------------------------------------------------------------- #
# parse(單一檔文字 → 去重後 cache-schema DF)                                     #
# --------------------------------------------------------------------------- #
def _row_to_cols(r: list[str], cols: dict[str, list]) -> None:
    """一列(已知 len>=16 且日期可解析)→ append 進 column-oriented buffers。"""
    n = len(r)
    cols["date"].append(parse_date(r[0]))
    cols["contract_code"].append(r[1].strip())
    cols["contract_month"].append(r[2].strip())
    cols["open"].append(opt_double(r[3]))
    cols["high"].append(opt_double(r[4]))
    cols["low"].append(opt_double(r[5]))
    cols["close"].append(opt_double(r[6]))
    cols["change"].append(opt_double(r[7]))
    cols["change_pct"].append(opt_double(r[8]))
    cols["volume"].append(_long(r[9]))
    cols["settlement_price"].append(opt_double(r[10]))
    cols["open_interest"].append(_opt_long(r[11]))
    cols["best_bid"].append(opt_double(r[12]))
    cols["best_ask"].append(opt_double(r[13]))
    cols["historical_high"].append(opt_double(r[14]))
    cols["historical_low"].append(opt_double(r[15]))
    halt = r[16].strip() if n > 16 else ""
    cols["trading_halt"].append(halt or None)
    sess = r[17].strip() if n > 17 else ""
    cols["trading_session"].append(sess if sess else "一般")
    cols["spread_single_volume"].append(_opt_long(r[18]) if n > 18 else None)


def _dedup(df: pl.DataFrame) -> pl.DataFrame:
    """Scala dedup:同 (date,code,month,session) 取「最完整」一列。

    完整度 = 結算價非空×1000 + 未平倉非空×100 + 收盤價非空×10 + min(成交量,9)。
    平手取**檔案順序在前者**(Scala maxBy 嚴格 `>` 才替換 = first-wins)。
    """
    if df.height == 0:
        return df
    score = (
        pl.when(pl.col("settlement_price").is_not_null()).then(1000).otherwise(0)
        + pl.when(pl.col("open_interest").is_not_null()).then(100).otherwise(0)
        + pl.when(pl.col("close").is_not_null()).then(10).otherwise(0)
        + pl.min_horizontal(pl.col("volume"), pl.lit(9, dtype=pl.Int64))
    )
    return (
        df.with_row_index("_ord")
          .with_columns(score.alias("_score"))
          .sort(["_score", "_ord"], descending=[True, False])
          .unique(subset=UNIQUE_COLS, keep="first", maintain_order=True)
          .drop(["_score", "_ord"])
    )


def parse_text(text: str) -> pl.DataFrame:
    """單一 TAIFEX 期貨日檔(整年或整月)的 CSV 文字 → 去重後 cache-schema DF。

    篩選 = Scala `row.size >= 16 && parseTaifexDate(row.head).isDefined`(丟表頭列
    與 DOS EOF `\\x1a` 標記);TAIFEX 檔無 `="` 護甲,plain csv 即可。
    """
    rows = list(csv.reader(io.StringIO(text)))
    _guard_header(rows)
    cols: dict[str, list] = {name: [] for name in _SCHEMA}
    for r in rows:
        if len(r) >= 16 and parse_date(r[0]) is not None:
            _row_to_cols(r, cols)
    df = pl.DataFrame(cols, schema=_SCHEMA)
    return _dedup(df)


# --------------------------------------------------------------------------- #
# fetch(抓取 → 原樣封存 → parse;原始檔封存鐵律:順序不可顛倒)                   #
# --------------------------------------------------------------------------- #
def _annual_raw_path(year: int):
    return _RAW_DIR / f"{year}_fut.csv"


def _month_raw_path(year: int, month: int):
    return _RAW_DIR / f"{year}" / f"{year}_{month}.csv"


def _unzip_single(zip_bytes: bytes) -> bytes:
    """年檔 ZIP → 取單一 CSV entry 的 bytes(對齊 Scala unzipSingleTaifexCsv)。"""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
        if not names:
            raise RuntimeError("TAIFEX 年檔 ZIP 無 CSV entry")
        return zf.read(names[0])


def fetch_year(year: int) -> pl.DataFrame:
    """抓某年整年年檔(down_type=2)→ 解壓 → **先封存 unzipped CSV** → parse → DF。"""
    zip_bytes = http.fetch_bytes(
        _ENDPOINT, form={"down_type": "2", "his_year": str(year)}, timeout=300.0)
    csv_bytes = _unzip_single(zip_bytes)          # 封存單位 = 解壓後 CSV(對齊既有年檔)
    archive.save_raw_bytes_at(_annual_raw_path(year), csv_bytes)  # 原樣落地(位元保真)
    return parse_text(csv_bytes.decode("Big5-HKSCS", errors="replace"))


def _month_end(year: int, month: int) -> Date:
    first_next = Date(year + 1, 1, 1) if month == 12 else Date(year, month + 1, 1)
    return first_next - timedelta(days=1)


def fetch_month(year: int, month: int, today: Date | None = None) -> pl.DataFrame:
    """抓當年某月月檔(down_type=1)→ **先原樣封存 CSV** → parse → DF。

    區間迄日不超過今天(對齊 Scala:未來日截到 today)。
    """
    today = today or Date.today()
    start = Date(year, month, 1)
    end = min(_month_end(year, month), today)
    form = {
        "down_type": "1",
        "queryStartDate": start.strftime("%Y/%m/%d"),
        "queryEndDate": end.strftime("%Y/%m/%d"),
        "commodity_id": "all",
        "commodity_id2": "",
    }
    raw = http.fetch_bytes(_ENDPOINT, form=form, timeout=180.0)
    archive.save_raw_bytes_at(_month_raw_path(year, month), raw)  # 原樣 bytes 落地
    return parse_text(raw.decode("Big5-HKSCS", errors="replace"))
