"""stock_per_pbr 源:TWSE BWIBBU_d + TPEx pera_result(本益比/殖利率/股價淨值比)。

cache 欄:price_book_ratio、dividend_yield、price_to_earning_ratio。

## 全歷史格式世代(實測 14,762 raw + A-audit 全庫逐格比對佐證)

**以中文標頭欄名定位欄位**,不靠欄數猜版型:TWSE 每列尾隨逗號會多切一個空欄,世代
間欄數為 6/8/9(TWSE)、7/8(TPEx),但『本益比/股價淨值比/殖利率』三個欄名在所有
世代都在且穩定 → 以名定位逐世代自動正確。A-audit BUG 2:Scala 以欄數分派,b14f115
後把 6 欄世代 PB 取到空字串→NULL、8 欄世代 PE→PB/PB→NULL/DY→股利年度(105.0),
只靠『日期已在庫就跳過』遮住,一旦重匯歷史約 387 萬列全錯位;此處以標頭定位根治。

| 市場 | 期間 | 標頭(代號/名稱之後) | PE/PB/DY 欄位 |
|---|---|---|---|
| twse | 2005-09 ~ 2017-04 | 本益比, 殖利率(%), 股價淨值比 | 2 / 4 / 3 |
| twse | 2017-04 ~ 2024-06 | 殖利率(%), 股利年度, 本益比, 股價淨值比, 財報年/季 | 4 / 5 / 2 |
| twse | 2024-07 ~ 今 | 收盤價, 殖利率(%), 股利年度, 本益比, 股價淨值比, 財報年/季 | 5 / 6 / 3 |
| tpex | 2007-01 ~ 2024-12 | 本益比, 每股股利, 股利年度, 殖利率(%), 股價淨值比 | 2 / 6 / 5 |
| tpex | 2025-01 ~ 今 | 本益比, 每股股利(註), 股利年度, 殖利率(%), 股價淨值比, 財報年/季 | 2 / 6 / 5 |

## 內容日期閘門(A/C-audit BUG 1)

TWSE BWIBBU_d 對無資料日回別日快照(常見 2017-12-18,標題誠實印『106年12月18日』),
舊 reader 只用檔名當日期 → 19 個 twse 日期存成別天估值(含未來寫進過去的前視)。此處
parse 前解析標題民國日期,與請求日不符即 fail-loud:rebuild 端計為 error 跳過該日、
絕不寫錯資料(那 19 檔本就該刪檔重抓/寫 sentinel);live 端寧可中止告警也不靜默汙染。
TPEx 標題『資料日期:NNN/MM/DD』同樣核對(A-audit:tpex 全史 7,137 檔零錯位)。

## 刻意不接的欄位(非遺漏)

『財報年/季』(估值 PIT 錨)、股利年度、每股股利、收盤價 原始檔有但**刻意不接**——
cache `stock_per_pbr` 為 6 欄(market/date/code/PE/PB/DY),接新欄屬 schema 變更(須先
改 cache 表 + parity),不在本 parser 職責;要接見 A-audit SUSPECT。dividend_yield 全期
乾淨(A-audit 以『殖利率×收盤=實際現金股利』全庫驗,2010 起中位數比值 1.000;TPEx
2007-2010 的股利年度/每股股利雖為抓取當下值〔前視〕但那兩欄本就不接,殖利率不受影響)。
`-`(TWSE)/`N/A`(TPEx)/空 → null;同代碼取首見(對齊 Scala distinctBy)。
"""
from __future__ import annotations

import re
from datetime import date as Date

import polars as pl

from research.crawl import archive, http, parse

TABLE = "stock_per_pbr"
KEY_COLS = ["market", "date"]
MARKETS = ("twse", "tpex")

_TWSE_URL = ("https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
             "?response=csv&selectType=ALL&date={d}")
_TPEX_URL = ("https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/"
             "pera_result.php?l=zh-tw&o=csv&d={d}")

_SCHEMA = {"market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
           "price_book_ratio": pl.Float64, "dividend_yield": pl.Float64,
           "price_to_earning_ratio": pl.Float64}

_MARKER = {"twse": "證券代號", "tpex": "股票代號"}

#: 標題民國日期(TWSE『115年07月15日』、TPEx『資料日期:115/07/17』)——內容日期閘門
#: 用它比對請求日,擋 BWIBBU 別日快照冒名(A/C-audit BUG 1)。半形/全形冒號皆吃。
_TWSE_DATE_RE = re.compile(r"(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日")
_TPEX_DATE_RE = re.compile(r"資料日期[:：]?\s*(\d+)\s*/\s*(\d+)\s*/\s*(\d+)")


def _dbl(v: str) -> float | None:
    try:
        return float(parse.clean(v))
    except ValueError:
        return None  # 對齊 Reader 的 toDoubleOption('-'/空 → None)


def _content_date(meta_rows: list[list[str]], market: str) -> Date | None:
    """標頭上方標題列的民國日期 → 西元 date;無可解析日期回 None。

    TWSE 標題『NNN年MM月DD日 個股日…』、TPEx『資料日期:NNN/MM/DD』(A/C-audit BUG 1:
    BWIBBU 對無資料日回別日快照,標題誠實印真日期,只認檔名會被別天資料冒名頂替)。
    """
    pat = _TWSE_DATE_RE if market == "twse" else _TPEX_DATE_RE
    for row in meta_rows:
        for cell in row:
            m = pat.search(cell)
            if m:
                y, mo, d = (int(x) for x in m.groups())
                try:
                    return Date(y + 1911, mo, d)  # 民國 → 西元
                except ValueError:
                    return None
    return None


def _locate(header: list[str], market: str) -> tuple[int, int, int, int]:
    """用中文標頭欄名定位 (code, pe, pb, dy) 欄索引——吃全部歷史世代。

    欄數在世代間為 6/8/9(TWSE)、7/8(TPEx),但『本益比/股價淨值比/殖利率』欄名在
    所有世代都在且穩定,以名定位逐世代自動正確(A-audit BUG 2:欄數分派會把 6/8 欄
    世代的 PE/PB/DY 全錯位)。找不到任一必要欄即 fail-loud(名稱變 = 真的新格式)。
    """
    cells = [c.replace(" ", "").replace('"', "") for c in header]

    def col(pred, what: str) -> int:
        for i, c in enumerate(cells):
            if pred(c):
                return i
        raise parse.SchemaDrift(f"{market} 標頭找不到「{what}」欄:{cells}")

    return (col(lambda c: c in (_MARKER["twse"], _MARKER["tpex"]), "代號"),
            col(lambda c: c == "本益比", "本益比"),
            col(lambda c: c == "股價淨值比", "股價淨值比"),
            col(lambda c: c.startswith("殖利率"), "殖利率"))


def _parse(text: str, day: Date, market: str) -> pl.DataFrame | None:
    rows = parse.parse_csv(text)
    marker = _MARKER[market]
    h = parse.find_header(rows, marker)
    if h < 0:
        return None  # 空回應/休市 sentinel/無資料 → 無標頭,交由呼叫端處理
    # BUG 1 守護:標題內容日期必須等於請求日,否則是別日快照冒名頂替,fail-loud。
    cdate = _content_date(rows[:h], market)
    if cdate is None:
        raise parse.SchemaDrift(
            f"{market} {day}:有資料標頭卻無可解析的標題日期,拒絕入庫(格式異常)")
    if cdate != day:
        raise parse.SchemaDrift(
            f"{market} 內容日期 {cdate} ≠ 請求日 {day}:BWIBBU 別日快照冒名/檔名"
            f"錯位(A/C-audit BUG 1),拒絕入庫")
    # BUG 2 修復:用中文標頭欄名定位,吃 TWSE 6/8/9 欄 + TPEx 7/8 欄全部世代。
    code_i, pe_i, pb_i, dy_i = _locate(rows[h], market)
    need = max(code_i, pe_i, pb_i, dy_i) + 1
    recs = []
    for r in rows[h + 1:]:
        if len(r) < need:
            continue
        code = parse.clean(r[code_i])
        if not code or code == marker:
            continue
        recs.append({
            "market": market, "date": day, "company_code": code,
            "price_book_ratio": _dbl(r[pb_i]), "dividend_yield": _dbl(r[dy_i]),
            "price_to_earning_ratio": _dbl(r[pe_i]),
        })
    if not recs:
        return None
    return (pl.DataFrame(recs, schema=_SCHEMA)
            .unique(subset=["company_code"], keep="first", maintain_order=True))


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    d = parse.twse_date(day) if market == "twse" else parse.minguo_slash(day)
    url = (_TWSE_URL if market == "twse" else _TPEX_URL).format(d=d)
    raw = http.fetch_bytes(url)
    archive.save_raw("stock_per_pbr", market, day, raw)   # 原樣 bytes(位元保真:先落地再 parse)
    return _parse(raw.decode("Big5-HKSCS", errors="replace"), day, market)
