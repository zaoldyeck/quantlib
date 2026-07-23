"""financial_analysis 源:MOPS 財務分析資料查詢彙總表(t51sb02 / server-java t105sb02)。

**年頻整表源**:每個 (市場, IFRS 版型) 一張全公司 CSV。移植自 Scala
`reader.FinancialReader.readFinancialAnalysis`(FinancialReader.scala:30-61)+
`setting.FinancialAnalysisSetting`(URL/formData/檔名/版型分流)。**忠實移植欄位對位/
值轉換,同時把稽核找出的 parser bug 一次寫對——不複製舊 bug。**

## 稽核 BUG(docs/data_audit/_done/A-financial_analysis.json)—— 本 port 一次寫對

Scala reader 用**寫死索引** `transferValues(0..18) = raw[2..20]`、**完全不依欄數分支**,
但本源有兩套 schema(實測 76 檔全掃):

    _a(IFRS 後,2012-2025):21 欄、19 指標欄,與 DB 對齊 ✅
    _b(IFRS 前,1989-2014):22 欄,比 _a **多第 [15] 欄「營業利益佔實收資本比率」** ❌

於是所有 _b 年度(18,591 列 = 45%,全部 year ≤ 2014 的 _b 部分)**尾 6 欄整體右移一格**:
  · `earnings_per_share(NTD)` 欄裝的其實是「純益率(%)」(**錯指標又錯單位**),
    真 EPS(raw[18])被擠去 `cash_flow_ratio` 欄;
  · 真「現金再投資比率」(raw[21])**整欄被丟棄**(reader 只讀到 raw[20])。

本 port 依**欄數明確分版型**(`_COLS_A`/`_COLS_B`,無 fallthrough,對齊 CLAUDE.md
「TWSE CSV schema drift」教訓),把 _b 尾欄放回正確語義、**補回被丟棄的 raw[21] 現金
再投資比率**,並**保留 _b 專有的 raw[15] 營業利益佔實收資本比率**(新增欄,_a 為 null),
**零資訊遺失**。逐位證明見 `test_financial_analysis_parity.py`(_b 位移鏈 + 台泥 1101 2011 錨)。

## 產出 schema(= PG 19 指標欄 + 1 個 port 補回的 _b 專有欄)

`market, year, company_code, company_name` + 20 指標欄(全 Float64 = Scala `Option[Double]`)。
新增 `operating_income_to_paid_in_capital(%)`(raw[15],僅 _b 有值)置於 `return_on_equity`
之後(即其原始位置)。PG 只有 19 欄(無此欄),故 parity 對 _b 斷言「port 補回、PG 缺」。

## 值轉換(對齊 Scala `String.toDoubleOption`)

Scala:`splitValues._2.map(_.toDoubleOption)`——**不去逗號/空白**,parse 失敗即 None。
`_num` 用 `float()`(同樣 trim 首尾空白)try/except → None,對 "NA"/""/含逗號 皆回 None,
與 Scala 逐格等值(稽核實測本源數值欄 0 格含逗號)。company_code/name 存原文(Scala 亦不清)。

## fetch:MOPS 兩步(第一手實測 2026-07-23,四個 市場×版型 組合皆通)

    step1  POST https://mopsov.twse.com.tw/mops/web/ajax_t51sb02
           body: encodeURIComponent=1&step=1&firstin=1&off=1&TYPEK=sii|otc&year=<民國>[&ifrs=Y]
           → 回 HTML,擷取 <input name='filename' value='t51sb02_....csv'>
    step2  POST https://mopsov.twse.com.tw/server-java/t105sb02
           body: firstin=true&step=10&filename=<step1 檔名>
           → 回 Big5-HKSCS CSV(21/22 欄)

**原始檔封存鐵律**:step2 bytes → **版型守衛(封存前,不落地錯檔)** → `archive`(原樣落地
扁平 `data/`)→ `parse`。封存採**扁平佈局**(既有 76 檔如此;見 `archive.raw_named_path`
的 `subdir=False`),fetch 覆寫同一路徑保單一副本。

## cache 依賴 / 邊界

本源目前**不在 cache**(稽核確認 research/ 零引用)。本模組只 fetch/parse/archive,
**不寫 PG/cache、不改 update.py**(主流程整合由使用者接線)。

Run(parity):uv run --project research python -m research.crawl.tests.test_financial_analysis_parity
"""
from __future__ import annotations

import csv
import io
import re

import polars as pl

from research.crawl import archive, http
from research.crawl.parse import SchemaDrift

TABLE = "financial_analysis"
KEY_COLS = ["market", "year"]          # 批次替換粒度 = 整年整表(Scala 以 (market,year) 去重)
MARKETS = ("twse", "tpex")

# application.conf data.financialAnalysis.{page,file}
_PAGE = "https://mopsov.twse.com.tw/mops/web/ajax_t51sb02"
_DOWNLOAD = "https://mopsov.twse.com.tw/server-java/t105sb02"
_FILENAME_RE = re.compile(r"""name=['"]?filename['"]?\s+value=['"]([^'"]+)['"]""")

# --------------------------------------------------------------------------- #
# 輸出欄位(順序 = 產出 DF 欄序)。19 個對齊 PG DB 欄名,operating_income 為 port 補回。 #
# --------------------------------------------------------------------------- #
_METRIC_COLS = [
    "liabilities/assets_ratio(%)",
    "Long-term_funds_to_property&plant&equipment(%)",
    "current_ratio(%)",
    "quick_ratio(%)",
    "times_interest_earned_ratio(%)",
    "average_collection_turnover(times)",
    "average_collection_days",
    "average_inventory_turnover(times)",
    "average_inventory_days",
    "property&plant&equipment_turnover(times)",
    "total_assets_turnover(times)",
    "return_on_total_assets(%)",
    "return_on_equity(%)",
    "operating_income_to_paid_in_capital(%)",   # ← port 補回(僅 _b);置於原始位置
    "profit_before_tax_to_capital(%)",
    "profit_to_sales(%)",
    "earnings_per_share(NTD)",
    "cash_flow_ratio(%)",
    "cash_flow_adequacy_ratio(%)",
    "cash_flow_reinvestment_ratio(%)",
]

_SCHEMA: dict[str, pl.DataType] = {
    "market": pl.Utf8, "year": pl.Int64,
    "company_code": pl.Utf8, "company_name": pl.Utf8,
    **{c: pl.Float64 for c in _METRIC_COLS},
}

# raw 欄索引 → 輸出欄名。**明確兩套版型,無 fallthrough**(稽核根因修復)。            #
# _a(21 欄):raw[2..20] → 19 指標欄(與 DB 對齊)。                                  #
_COLS_A: dict[int, str] = {
    2: "liabilities/assets_ratio(%)",
    3: "Long-term_funds_to_property&plant&equipment(%)",
    4: "current_ratio(%)",
    5: "quick_ratio(%)",
    6: "times_interest_earned_ratio(%)",
    7: "average_collection_turnover(times)",
    8: "average_collection_days",
    9: "average_inventory_turnover(times)",
    10: "average_inventory_days",
    11: "property&plant&equipment_turnover(times)",
    12: "total_assets_turnover(times)",
    13: "return_on_total_assets(%)",
    14: "return_on_equity(%)",
    15: "profit_before_tax_to_capital(%)",
    16: "profit_to_sales(%)",
    17: "earnings_per_share(NTD)",
    18: "cash_flow_ratio(%)",
    19: "cash_flow_adequacy_ratio(%)",
    20: "cash_flow_reinvestment_ratio(%)",
}
# _b(22 欄):raw[15] = 營業利益佔實收資本(_b 專有),尾欄整體右移一格,raw[21] 補回。 #
_COLS_B: dict[int, str] = {
    2: "liabilities/assets_ratio(%)",
    3: "Long-term_funds_to_property&plant&equipment(%)",
    4: "current_ratio(%)",
    5: "quick_ratio(%)",
    6: "times_interest_earned_ratio(%)",
    7: "average_collection_turnover(times)",
    8: "average_collection_days",
    9: "average_inventory_turnover(times)",
    10: "average_inventory_days",
    11: "property&plant&equipment_turnover(times)",
    12: "total_assets_turnover(times)",
    13: "return_on_total_assets(%)",
    14: "return_on_equity(%)",
    15: "operating_income_to_paid_in_capital(%)",   # _b 專有(Scala 誤塞進 pbt 欄)
    16: "profit_before_tax_to_capital(%)",
    17: "profit_to_sales(%)",
    18: "earnings_per_share(NTD)",                   # 真 EPS(Scala 誤塞進 cash_flow_ratio)
    19: "cash_flow_ratio(%)",
    20: "cash_flow_adequacy_ratio(%)",
    21: "cash_flow_reinvestment_ratio(%)",           # 補回(Scala 丟棄)
}

_WIDTH = {"a": 21, "b": 22}


# --------------------------------------------------------------------------- #
# 值轉換 / CSV 讀取                                                             #
# --------------------------------------------------------------------------- #
def _num(cell: str) -> float | None:
    """對齊 Scala `String.toDoubleOption`:能 parse 成 double 回 float,否則 None。

    `float()` 與 Java `parseDouble` 同樣 trim 首尾空白;"NA"/""/含逗號 → ValueError → None。
    **不去逗號**(Scala 此處亦不去;稽核實測本源數值欄 0 格含逗號,故等值)。
    """
    try:
        return float(cell)
    except ValueError:
        return None


def _read_rows(text: str) -> list[list[str]]:
    """Big5 解碼後文字 → rows(標準 RFC-CSV,對齊 tototoshi CSVReader.all())。

    本源 CSV 用普通 `"..."` 引號(非 TWSE MI_INDEX 的 `="..."` Excel 護甲),
    csv 模組直接去引號即與 Scala 等值。跳過全空列(防禦性;實測 corpus 無)。
    """
    return [r for r in csv.reader(io.StringIO(text)) if r]


# --------------------------------------------------------------------------- #
# 標頭位置守衛(SchemaDrift fail-loud;比照 daily_quote._guard / margin._guard_*)  #
# --------------------------------------------------------------------------- #
def _schema_of(header: list[str], market: str, year: int) -> str:
    """依標頭欄數判版型:21→_a、22→_b,其餘 fail-loud(不靜默錯位)。"""
    n = len(header)
    if n == _WIDTH["a"]:
        return "a"
    if n == _WIDTH["b"]:
        return "b"
    raise SchemaDrift(
        f"financial_analysis {market} {year}:標頭欄數 {n} 不是 21(_a)或 22(_b)"
        f"——TWSE 改格式?(head={header[:3]})")


def _guard_header(header: list[str], schema: str, market: str, year: int) -> None:
    """驗證關鍵欄名在預期位置(容忍 IFRS 更名的同義欄 + 標頭內 <br>)。"""
    cells = [c.replace(" ", "") for c in header]

    def want(i: int, needle: str) -> None:
        got = cells[i] if i < len(cells) else "<缺>"
        if i >= len(cells) or needle not in cells[i]:
            raise SchemaDrift(
                f"financial_analysis {market} {year} _{schema}:標頭 col[{i}] "
                f"期望含 '{needle}' 實得 '{got}'(欄位位移/改格式?)")

    want(0, "公司代號")
    want(1, "公司")                       # 公司簡稱 / 公司名稱
    want(2, "負債佔資產")
    want(3, "長期資金佔")                  # 固定資產(_b)/ 不動產、廠房及設備(_a)
    want(13, "資產報酬率")
    want(14, "權益報酬率")                 # 股東權益報酬率(_b)/ 權益報酬率(_a)
    if schema == "a":
        want(15, "稅前純益佔實收資本")
        want(17, "每股盈餘")
        want(18, "現金流量比率")
        want(20, "現金再投")
    else:                                  # _b:多 [15] 營業利益,尾欄右移一格
        want(15, "營業利益佔實收資本")
        want(16, "稅前純益佔實收資本")
        want(18, "每股盈餘")
        want(21, "現金再投")


# --------------------------------------------------------------------------- #
# 解析(供 fetch 與 parity 測試共用):market + year + 已解碼文字 → DF              #
# --------------------------------------------------------------------------- #
def parse(market: str, year: int, text: str) -> pl.DataFrame | None:
    """解析單一版型檔(_a 或 _b 由標頭欄數自動判定)→ 產出 DF(_SCHEMA)。回 None = 無資料列。"""
    rows = _read_rows(text)
    if not rows:
        return None
    header = rows[0]
    schema = _schema_of(header, market, year)
    _guard_header(header, schema, market, year)
    colmap = _COLS_A if schema == "a" else _COLS_B
    width = _WIDTH[schema]

    recs: list[dict] = []
    for r in rows[1:]:
        if not r:
            continue
        if len(r) != width:               # 明確 case:資料列寬度必 == 標頭,否則 fail-loud
            raise SchemaDrift(
                f"financial_analysis {market} {year} _{schema}:資料列 {r[:1]} "
                f"寬度 {len(r)} != {width}")
        rec: dict = {c: None for c in _METRIC_COLS}
        rec["market"] = market
        rec["year"] = year
        rec["company_code"] = r[0]        # 存原文(Scala values.head 亦不清)
        rec["company_name"] = r[1]        # 存原文(Scala values(1) 亦不清)
        for idx, col in colmap.items():
            rec[col] = _num(r[idx])
        recs.append(rec)
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


# --------------------------------------------------------------------------- #
# fetch:MOPS 兩步 → 版型守衛 → 原樣封存(扁平)→ parse                            #
# --------------------------------------------------------------------------- #
def _combos_for_year(year: int) -> list[tuple[str, str]]:
    """(market, schema) 組合,移植 FinancialAnalysisSetting.markets 的年份分流。"""
    if year < 1993:
        return [("twse", "b")]
    if year < 2012:
        return [("twse", "b"), ("tpex", "b")]
    if year > 2014:
        return [("twse", "a"), ("tpex", "a")]
    return [("twse", "b"), ("twse", "a"), ("tpex", "b"), ("tpex", "a")]  # 2012-2014


def _form(market: str, schema: str, year: int) -> dict[str, str]:
    """step1 formData(移植 FinancialAnalysisSetting 的四個 Detail)。"""
    f = {
        "encodeURIComponent": "1", "step": "1", "firstin": "1", "off": "1",
        "TYPEK": "otc" if market == "tpex" else "sii",
        "year": str(year - 1911),        # 民國年(Scala: endDate.getYear - 1911)
    }
    if schema == "a":                     # IFRS 後版型多帶 ifrs=Y
        f["ifrs"] = "Y"
    return f


def _fetch_download_bytes(market: str, schema: str, year: int) -> bytes | None:
    """兩步下載:step1 拿 filename → step2 抓 CSV bytes。無 filename → None(無資料,不封存)。"""
    page = http.fetch_bytes(_PAGE, form=_form(market, schema, year))
    m = _FILENAME_RE.search(page.decode("utf-8", errors="replace"))
    if not m:
        return None
    return http.fetch_bytes(
        _DOWNLOAD, form={"firstin": "true", "step": "10", "filename": m.group(1)})


def fetch_year(year: int) -> pl.DataFrame | None:
    """抓某年全部 (市場×版型)→ **版型守衛 → 原樣封存扁平 data/ → parse**(封存鐵律)。

    回傳該年所有版型/市場列的合併 DF(_a 與 _b 公司集互斥,直接 concat);全無資料 → None。
    """
    frames: list[pl.DataFrame] = []
    for market, schema in _combos_for_year(year):
        raw = _fetch_download_bytes(market, schema, year)
        if raw is None:
            continue
        text = raw.decode("Big5-HKSCS", errors="replace")
        rows = _read_rows(text)
        if not rows:
            raise SchemaDrift(
                f"financial_analysis {market} {year} _{schema}:下載回應無列(錯誤頁?)"
                f"——不封存,fail-loud 供整合層重試")
        # 封存前守衛:確認真是本源該版型 CSV,不把錯誤頁當 {year}_{schema}.csv 落地。
        header = rows[0]
        got_schema = _schema_of(header, market, year)
        if got_schema != schema:          # 請求 _a 卻回 _b(或反之)→ 不落地錯標的檔名
            raise SchemaDrift(
                f"financial_analysis {market} {year}:請求 _{schema} 卻回 _{got_schema} "
                f"版型({len(header)} 欄)——不封存,fail-loud 供整合層重試")
        _guard_header(header, got_schema, market, year)
        archive.save_raw_named(
            TABLE, market, year, f"{year:04d}_{schema}.csv", raw, subdir=False)
        df = parse(market, year, text)
        if df is not None:
            frames.append(df)
    return pl.concat(frames) if frames else None
