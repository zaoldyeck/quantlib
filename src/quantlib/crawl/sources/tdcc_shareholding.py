"""tdcc_shareholding 源:集保戶股權分散表(千張大戶/籌碼分散,週頻)。

TDCC opendata `getOD.ashx?id=1-5`(UTF-8-with-BOM CSV)。**單一全市場檔**
(TWSE + TPEx 同一張表,無 market 分檔),每檔 = 全市場所有證券 × 17 個持股分級。
endpoint **無日期參數**,永遠回「最新一週」快照;歷史回補須走 TDCC 歷史查詢介面
(Task #20,主流程統一安排,不在本 port 範圍)。

移植自 `TradingReader.readTdccShareholding`,**忠實**保留:
- **日期取自內容而非檔名**:每列第 0 欄為西元 `yyyyMMdd` 資料日期;檔名用「抓取日」
  (下載日常晚內容日約一週,例:2026_4_24.csv 內容實為 20260417)。reader 以
  第一筆資料列的日期當全檔 data_date(稽核證每檔僅 1 個 distinct 資料日,安全)。
- **標頭以 `\\d{8}` 丟棄**(dropWhile 到第一個「第 0 欄剛好 8 位數字」的列);UTF-8 BOM
  由 utf-8-sig 解碼一併化解(Scala 靠 `\\d{8}` 把帶 BOM 的標頭丟掉,結果等價)。
- **六欄對位**:col0 資料日期、col1 證券代號、col2 持股分級(1-17)、col3 人數、
  col4 股數、col5 占集保庫存數比例%。
- **值轉換**:代號 `.strip()`(去尾端補空白,`2330  `→`2330`);人數/股數
  `replace(",","")` 後轉整數;比例轉 float。任一欄轉換失敗 → **整列丟棄**
  (對齊 Scala 外層 `Try{}.toOption`)。
- **檔內去重** by (data_date, company_code, holding_tier),keep first(對齊 `.distinctBy`)。

## 稽核結論:此源解析 100% 乾淨(A/C-tdcc_shareholding 皆 verdict=OK)

**與 dtd/margin/sbl 不同,本源 Scala parser 零 bug**——稽核對 12 個資料日、813,110 列、
六欄逐格比對:數值/列數/單位/正負號/日期/代號空白全對(A-tdcc RESULT PASS)。故本 port
是**忠實 1:1 重現**,parity 測試要求六欄**全部逐位一致**;**不存在「Python 對、PG 錯」
的欄**(不像 sbl 的 name-strip)。以下兩點是本 port 對稽核殘留項的處置,不是資料修正:

1. **num_shares 一律 Int64(BIGINT)**:2330 合計股數 25,932,525,515 遠超 int32 上限
   (2,147,483,647)。Scala 已用 `.toLong`、cache/PG 為 BIGINT,本 port 用 Int64 對齊
   ——非修 bug(PG 本就正確),是杜絕未來以 int32 重寫時的溢位。num_holders 上界
   受人口數物理封頂(~千萬),對齊 cache 的 INTEGER 用 Int32;holding_tier 用 Int16
   (SMALLINT)。

2. **平行匯入去重競態(A-tdcc SUSPECT)→ 由 Sink 天然化解**:Scala 全庫重建時同一資料日
   的多個檔(如 20260703 落在 6 個下載檔)會同時通過 stale 快照閘、全靠唯一索引擋例外
   (非決定性)。本 port 不重現該路徑:fetch 一次只取最新一週、回一個 data_date 的 DF;
   主流程以 `Sink.upsert(TABLE, df, KEY_COLS=["data_date"])` **刪整週再插入**,idempotent、
   決定性、無競態。**不碰 cache 寫入**(整合由主流程接線),此處只保證回傳的 DF 形態正確。

## 已知缺漏日期(稽核 C-tdcc SUSPECT,非本 port 可解)

已收集區間 [2026-04-17, 2026-07-17] 缺 **2026-04-30、2026-05-29** 兩個真交易週——
那兩週爬蟲沒跑到、快照已被下一週覆蓋,標準 endpoint 補不回(Task #20)。此為**來源端
漏收**、非解析問題;兩日在 raw 內容日期集合與 PG 皆不存在,parity 測試明確排除並註明。

## 型別/欄位:回傳 DF 與 cache 表 `tdcc_shareholding` 同構(6 欄,無 id)

cache 投影掉 PG 的自增 `id`(稽核 C:research/ 零消費者)。`fetch_latest` 回 cache 的 6 欄。

## cache 依賴

`fetch_latest` 走 archive→parse,不讀 cache;parity 測試讀 PG(或 `--cache` 讀 cache.duckdb)
當對照,稽核 C 已證 cache 與 PG 全量逐位一致。
"""
from __future__ import annotations

import re
from datetime import date as Date

import polars as pl

from quantlib.crawl import archive, http, parse

TABLE = "tdcc_shareholding"
#: 週頻快照批次 key:一次 fetch = 一個 data_date 的全市場全分級快照。
#: PG/cache 唯一索引為 (data_date, company_code, holding_tier);批次以整週為單位替換。
KEY_COLS = ["data_date"]
#: 單一全市場檔;archive 的 market 段沿用既有路徑慣例 data/tdcc_shareholding/weekly/。
MARKETS = ("weekly",)
_MARKET = "weekly"

#: 靜態 endpoint,無日期參數,永遠回最新一週(application.conf data.tdcc.file)。
_URL = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"

#: 回傳 DF 與 cache 表同構(6 欄):tier→SMALLINT、holders→INTEGER、shares→BIGINT。
_SCHEMA = {
    "data_date": pl.Date,
    "company_code": pl.Utf8,
    "holding_tier": pl.Int16,
    "num_holders": pl.Int32,
    "num_shares": pl.Int64,
    "pct_of_outstanding": pl.Float64,
}

#: 標頭位置守衛(col index → 期望欄名):抓到位移/加欄就 fail-loud(比照 daily_quote._guard)。
_HEADER_GUARD = {
    0: "資料日期", 1: "證券代號", 2: "持股分級",
    3: "人數", 4: "股數", 5: "占集保庫存數比例%",
}

#: 資料列判準:第 0 欄剛好 8 位數字(西元 yyyyMMdd)。對齊 Scala `row.head.matches("\\d{8}")`。
_YYYYMMDD = re.compile(r"\d{8}")


def _is_data_row(row: list[str]) -> bool:
    """對齊 Scala dropWhile 條件的反面:非空且第 0 欄 fullmatch `\\d{8}`。"""
    return bool(row) and bool(_YYYYMMDD.fullmatch(row[0]))


def _guard_header(rows: list[list[str]]) -> None:
    """找『資料日期』標頭列並驗證六欄位置;位移即 `parse.SchemaDrift` fail-loud。

    找不到標頭但有 8 位數資料列 → 視為格式漂移(TDCC 改版)亦 fail-loud;
    完全無資料(空檔)則容忍(回空 DF 由呼叫端處理)。
    """
    h = next((i for i, r in enumerate(rows) if r and r[0].strip() == "資料日期"), -1)
    if h < 0:
        if any(_is_data_row(r) for r in rows):
            raise parse.SchemaDrift(
                "tdcc_shareholding 找不到『資料日期』標頭卻有資料列(TDCC 改格式?)")
        return
    cells = [c.strip() for c in rows[h]]
    for i, name in _HEADER_GUARD.items():
        got = cells[i] if i < len(cells) else "<缺>"
        if got != name:
            raise parse.SchemaDrift(
                f"tdcc_shareholding 欄位位移:col[{i}] 期望 '{name}' 實得 '{got}'"
                f"(TDCC 改格式?)")


def _record(row: list[str], data_date: Date) -> dict | None:
    """一列 → record;任一欄轉換/越界失敗 → 整列丟棄(對齊 Scala 外層 `Try{}.toOption`)。

    - 代號僅 `.strip()`(去尾端補空白,保留內部字元);人數/股數去逗號後轉整數;比例轉 float。
    - data_date 一律用「第一筆資料列」的日期(對齊 Scala `rows.head.head`,非逐列 col0)。
    """
    try:
        code = row[1].strip()
        tier = int(row[2])
        holders = int(row[3].replace(",", ""))
        shares = int(row[4].replace(",", ""))
        pct = float(row[5])
    except (ValueError, IndexError):
        return None
    return {
        "data_date": data_date,
        "company_code": code,
        "holding_tier": tier,
        "num_holders": holders,
        "num_shares": shares,
        "pct_of_outstanding": pct,
    }


def parse_raw(raw: bytes) -> tuple[Date | None, pl.DataFrame]:
    """封存原始檔 bytes → (內容資料日期, cache 同構 6 欄 DF)。

    utf-8-sig 解碼(化解 BOM)→ 標頭守衛 → dropWhile 到第一個 8 位數資料列 →
    以其日期當全檔 data_date → 逐列轉換(失敗丟列)→ 檔內去重(date,code,tier)。
    無資料列 → (None, 空 DF)。
    """
    text = raw.decode("utf-8-sig", errors="replace")
    rows = parse.parse_csv(text)
    _guard_header(rows)

    start = next((i for i, r in enumerate(rows) if _is_data_row(r)), -1)
    if start < 0:
        return None, pl.DataFrame([], schema=_SCHEMA)

    data_date = Date(
        int(rows[start][0][:4]), int(rows[start][0][4:6]), int(rows[start][0][6:8]))

    seen: set[tuple] = set()
    recs: list[dict] = []
    for r in rows[start:]:
        rec = _record(r, data_date)
        if rec is None:
            continue
        key = (rec["data_date"], rec["company_code"], rec["holding_tier"])
        if key in seen:
            continue
        seen.add(key)
        recs.append(rec)
    return data_date, pl.DataFrame(recs, schema=_SCHEMA)


def fetch_latest(fetch_date: Date | None = None) -> pl.DataFrame | None:
    """抓 TDCC 最新一週集保股權分散 → **先原樣封存 raw 到 data/** → parse → 回 6 欄 DF。

    原始檔封存鐵律:`save_raw` 在 parse 之前(順序不可顛倒)。封存檔名用「抓取日」
    (fetch_date,預設今天;endpoint 無日期參數故檔名 = 下載日),data_date 由內容還原。
    endpoint 永遠回最新一週:無「請求日 vs 內容日」可比,故**無 DateMismatch 拒收**
    (與 sbl 不同——sbl 端點吃日期會回過期報表,tdcc 端點不吃日期、內容日即真源)。
    交易所回無資料 → None。
    """
    fetch_date = fetch_date or Date.today()
    raw = http.fetch_bytes(_URL)
    archive.save_raw(TABLE, _MARKET, fetch_date, raw)  # 原樣 bytes,位元保真(鐵律)
    _content_date, df = parse_raw(raw)
    return None if df.is_empty() else df
