"""index 源:TWSE MI_INDEX(type=IND,價格指數)+ TPEx indexSummary(上櫃指數收盤行情)。

移植自 Scala `reader.TradingReader.readIndex`(TradingReader.scala:398-463)、
`setting.IndexSetting` / `TwseDetail` / `TpexV2Detail`(URL / 民國標題日期驗證)與
`util.QuantlibCSVReader`。**忠實移植欄位對位與值轉換,同時把稽核
(docs/data_audit/_done/A-index.json、C-market_index.json)列出的 parser bug 一次寫對
——絕不複製舊 bug。**

## 產出 schema(= cache `market_index` 六欄;PG `index` 去掉 Slick 流水號 id)

    market, date, name, close, change, change_pct

- `close` / `change` / `change_pct` 一律 **nullable**(Float64,None 表「當天未公布」)。
  這是稽核 BUG#3 的修復:PG `index.change` / `"change(%)"` 宣告成非 nullable Double,
  於是 TWSE 原始檔的『--』(未公布)被 `getOrElse(0)` 寫成 0,1,578 列「無資料」在庫裡
  長得像「當天平盤」。本 port 用 None 保留「未公布 ≠ 平盤」的語義。

## Scala 欄位對位(逐格對過原始檔 + PG,見本源 parity 測試)

TWSE(Big5 CSV,一列一指數,平列所有價格+報酬指數):
  `"指數","收盤指數","漲跌(+/-)","漲跌點數","漲跌百分比(%)","特殊處理註記",`
  0 name 1 close 2 方向(+/-/空) 3 漲跌點數(無號) 4 漲跌幅%(自帶號) 5 註記(不入庫)
  → change = 方向 套用到 漲跌點數;change_pct = 第 4 欄原樣(已含負號)。

TPEx(Big5 CSV,4 欄,分「價格區」與「報酬指數」兩段,中間一列 `報酬指數` 表頭):
  `"指數","收市指數","漲跌","漲跌幅度"`(價格區表頭)
  0 name 1 close 2 change(自帶號) 3 change_pct(自帶號)
  報酬區每檔 name 需加「報酬」以免與價格區同名撞 (market,date,name) unique index。

## 稽核 bug 修復清單(本 port 一次寫對;先紅後綠見 test_index_parity.py)

- **BUG#3 change/change_pct 歸零** → `close`/`change`/`change_pct` 皆 nullable;原始『--』/空
  一律 None(不 `getOrElse(0)`)。涵蓋三種:①全列未公布(close 也 --)②方向 +/- 但點數 --
  ③方向空但點數 -- → 全部 change=None(而非 Scala 的 0),幅度『--』→ change_pct=None。
- **BUG#4 TPEx 報酬指數改名器多疊「報酬」** → `_return_name`:名稱**已含「報酬」**即保留原名
  (原名已是唯一、不與價格區撞名);否則才 `name.replace("指數","")+"報酬指數"`(= Scala,
  用以消除價格區同名)。Scala 對已自帶「報酬指數」的 11 檔(如『櫃買半導體領航報酬指數』)
  無條件再疊 → 『…報酬報酬指數』(7,506 列,tpex 全表 4.72%),用官方名 join 查無資料。
  判準用「含『報酬』」而非「endsWith 報酬指數」:『富櫃200報酬正向2倍指數』末尾非『報酬指數』
  但已自帶『報酬』,endsWith 版會漏修(仍被疊成『…2倍報酬指數』)——稽核明列此檔為失真之一。
- **name-strip 只清數值欄** → `name` 保留原文(只去頭尾空白),不像 Scala 連名稱一起
  `.replace(" ","")`(把『Quality 50指數』『上櫃ESG 30指數』『TPEx FactSet…』的空白抹掉)。
  數值欄才去逗號/空白。
- **footer 切割對兩代版型皆有效** → TWSE 檔尾『備註:』footer:Scala 用 `startsWith("備註:")`,
  2025 前 footer 首行是 `"備註:"`(前導雙引號)→ 守不住(SUSPECT#5,靠下游 size 濾網僥倖無害)。
  本 port 去前導引號後再判 `startswith("備註")`,兩代都切得掉,且避免 2026 版 `備註:"` 未終結
  引號流進 CSV parser 造成多列黏合。
- **方向欄明確 case,無 fallthrough** → 方向只認 `+ / - / 空`,第四種值 `SchemaDrift` fail-loud
  (Scala 的 match 無 default,來源多一種取值會讓整批平行匯入 MatchError 炸掉;SUSPECT#6)。
- **標頭位置守衛** → 比照 daily_quote._guard,抓到欄位位移即 `SchemaDrift`(TWSE/TPEx 悄悄加欄地雷)。
- **name=='null' 丟棄** → TPEx 來源端偶把指數名寫成字面 'null'(2022-01~02 共 32 列),Scala
  `.filterNot(_._3=="null")` 丟掉(丟是對的:兩列同名會撞 unique index);本 port 同樣丟,順序
  (改名後才判 'null')與 Scala 一致。

## 非本 parser 能修的(整合層 update.py 職責;不在此半實作以免留假防線)

- **整片是別天資料的汙染(8 個 twse 日,947 列)**:原始檔檔頭日期**是對的**、body 卻是別天
  (TWSE 靜默 fallback 的 body 汙染;其中 3 天是週六幽靈、1 天 2019-07-05 是前視)。單檔解析
  抓不到(檔頭日期相符),Scala 與本 port 都會忠實搬運同一份壞 raw → parity 對得上但資料本身
  錯。守護要靠跨日:『本檔加權指數 close − change ?= 前一交易日 close(容差 0.02)』對不上即
  deferred 重抓(A/C 稽核明列)。`fetch_day` 的 `_content_date` 只擋得住**檔頭日期就錯**的
  fallback(IndexSetting.validate 的目標),擋不住 body 汙染 —— 後者列入整合層。
- **缺日 / 半殘檔盤點**:tpex 2024-06-27~08-12(31 交易日連檔案都沒有)、twse 2009-12-12 /
  2026-03-12(0-byte)、半殘 twse 2026-02-26 / 03-11(舊名冊 135 列)——靠與 daily_quote 交易日
  日曆對照補抓,屬 update.py 跨表職責。

Run(parity):uv run --project . python -m quantlib.crawl.tests.test_index_parity
"""
from __future__ import annotations

import csv
import io
import re
from datetime import date as Date

import polars as pl

from quantlib.crawl import archive, http
from quantlib.crawl.parse import SchemaDrift, twse_date

TABLE = "market_index"
#: 日頻批次 key(sink.upsert_day 刪整日再插);自然唯一鍵是 (market, date, name)。
KEY_COLS = ["market", "date"]
MARKETS = ("twse", "tpex")

# application.conf data.index.{twse,tpex}.file(日期由 {d} 帶入)。
_TWSE_URL = ("https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
             "?response=csv&type=IND&date={d}")
_TPEX_URL = ("https://www.tpex.org.tw/www/zh-tw/afterTrading/indexSummary"
             "?response=csv&date={d}")

#: 產出欄 = cache market_index 六欄;數值一律 nullable Float64。
_SCHEMA: dict[str, pl.PolarsDataType] = {
    "market": pl.Utf8, "date": pl.Date, "name": pl.Utf8,
    "close": pl.Float64, "change": pl.Float64, "change_pct": pl.Float64,
}

#: 民國標題日期。twse『YYY年MM月DD日 價格指數…』;tpex『Data Date:YYY/MM/DD』。
_TWSE_TITLE_DATE = re.compile(r"(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")
_TPEX_TITLE_DATE = re.compile(r"Data\s*Date[:：]\s*(\d{2,3})/(\d{1,2})/(\d{1,2})")

#: 標頭位置守衛(cells index → 期望欄名前綴;抓到位移就 fail-loud)。
_TWSE_GUARD = {0: "指數", 1: "收盤指數", 2: "漲跌(+/-)", 3: "漲跌點數", 4: "漲跌百分比(%)"}
_TPEX_GUARD = {1: "收市指數", 2: "漲跌", 3: "漲跌幅度"}


class DateMismatch(RuntimeError):
    """內容標題日期 ≠ 請求日期:交易所對缺資料日回了別天的**檔頭**(TWSE 靜默 fallback,
    IndexSetting.validate 的目標)→ deferred 重抓,絕不以錯誤檔名封存。

    注意:此守護只擋「檔頭日期就錯」;檔頭對、body 卻是別天的 body 汙染(8 個 twse 日)
    抓不到,屬整合層跨日守護(見模組 docstring)。
    """


# --------------------------------------------------------------------------- #
# 值轉換                                                                        #
# --------------------------------------------------------------------------- #
def _num(cell: str) -> float | None:
    """數值欄 → float;去逗號/空白後為空或『--』家族一律 None(BUG#3:未公布≠0)。

    對齊 Scala 的 `toDoubleOption`(close/pct)語義,但**擴及 change**:任何無法解析的
    未公布標記都成 None,而非 `getOrElse(0)` 的假 0。
    """
    c = cell.replace(",", "").replace(" ", "").strip()
    if c in ("", "--", "---", "----"):
        return None
    try:
        return float(c)
    except ValueError:
        return None


def _twse_change(sign: str, magnitude: str) -> float | None:
    """TWSE 漲跌:方向欄(col2)套用到無號漲跌點數(col3)。

    - `+` → +點數;`-` → -點數;空 → 點數原值(honest,取代 Scala `case ""=>0`)。
    - 點數為『--』(未公布)→ None(取代 Scala `Try(...).getOrElse(0)` 的假 0)。
    - 第四種方向值 → `SchemaDrift` fail-loud(Scala match 無 default,SUSPECT#6)。
    """
    m = _num(magnitude)
    s = sign.strip()
    if s == "+":
        return m
    if s == "-":
        return None if m is None else -m
    if s == "":
        return m
    raise SchemaDrift(f"twse index 未知漲跌方向 {sign!r}(來源多一種取值?)")


def _return_name(name: str) -> str:
    """TPEx 報酬區指數改名(BUG#4 修):名稱已含『報酬』即保留原名;否則加『報酬』消除
    與價格區的同名撞鍵。判準用『含報酬』而非『endsWith 報酬指數』——後者漏修
    『富櫃200報酬正向2倍指數』這類末尾非『報酬指數』卻已自帶『報酬』的名稱。

    非報酬區(價格區)不呼叫本函式,名稱原樣保留。
    """
    if "報酬" in name:
        return name
    return name.replace("指數", "") + "報酬指數"


# --------------------------------------------------------------------------- #
# 標頭守衛                                                                       #
# --------------------------------------------------------------------------- #
def _guard(header: list[str], guard: dict[int, str], what: str) -> None:
    cells = [c.replace(" ", "").replace('"', "").strip() for c in header]
    for i, name in guard.items():
        got = cells[i] if i < len(cells) else "<缺>"
        if i >= len(cells) or cells[i] != name:
            raise SchemaDrift(f"index {what} 標頭位移:col[{i}] 期望 '{name}' 實得 '{got}'"
                              f"(TWSE/TPEx 改格式?)")


# --------------------------------------------------------------------------- #
# TWSE                                                                          #
# --------------------------------------------------------------------------- #
def _twse_data_lines(text: str) -> list[str]:
    """切掉檔尾『備註:』footer(去前導引號後判,兩代版型皆有效;SUSPECT#5 修)。"""
    out: list[str] = []
    for ln in text.split("\n"):
        if ln.lstrip('"').lstrip().startswith("備註"):
            break
        out.append(ln)
    return out


def _parse_twse(text: str, day: Date) -> pl.DataFrame | None:
    rows = list(csv.reader(io.StringIO("\n".join(_twse_data_lines(text)))))
    hdr = next((r for r in rows if r and r[0].strip().replace('"', "") == "指數"), None)
    if hdr is not None:
        _guard(hdr, _TWSE_GUARD, "twse")
    recs = []
    for r in rows:
        if len(r) not in (6, 7):                 # Scala:row.size == 6 || 7
            continue
        name = r[0].strip()                      # 只清頭尾,保留名稱原文(不去內部空白)
        if name in ("指數", "報酬指數"):
            continue
        if name == "null":                       # Scala filterNot _._3=="null"
            continue
        recs.append({
            "market": "twse", "date": day, "name": name,
            "close": _num(r[1]),
            "change": _twse_change(r[2], r[3]),
            "change_pct": _num(r[4]),
        })
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


# --------------------------------------------------------------------------- #
# TPEx                                                                          #
# --------------------------------------------------------------------------- #
def _parse_tpex(text: str, day: Date) -> pl.DataFrame | None:
    four = [r for r in csv.reader(io.StringIO(text)) if len(r) == 4]  # Scala:_.size==4
    if not four:
        return None
    _guard(four[0], _TPEX_GUARD, "tpex 價格區")   # four[0] = 價格區表頭(head=指數)
    # span 至第一列 head=='報酬指數'(報酬區表頭);其前為價格區、其後為報酬區。
    ridx = next((i for i, r in enumerate(four) if r[0].strip() == "報酬指數"), None)
    if ridx is None:                              # 無報酬區(全史零命中,防禦性)
        price, ret = four[1:], []
    else:
        _guard(four[ridx], _TPEX_GUARD, "tpex 報酬區")
        price, ret = four[1:ridx], four[ridx + 1:]

    recs = []
    for r in price:
        recs.append(_tpex_rec(day, r, r[0].strip()))
    for r in ret:
        recs.append(_tpex_rec(day, r, _return_name(r[0].strip())))
    recs = [x for x in recs if x["name"] != "null"]  # 改名後才判 'null'(順序同 Scala)
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


def _tpex_rec(day: Date, r: list[str], name: str) -> dict:
    return {
        "market": "tpex", "date": day, "name": name,
        "close": _num(r[1]),
        "change": _num(r[2]),
        "change_pct": _num(r[3]),
    }


# --------------------------------------------------------------------------- #
# 對外介面                                                                       #
# --------------------------------------------------------------------------- #
def parse(market: str, text: str, day: Date) -> pl.DataFrame | None:
    """純解析(供 fetch_day 與 parity 測試共用):market + 已解碼文字 + 權威日期 → DF。"""
    return _parse_twse(text, day) if market == "twse" else _parse_tpex(text, day)


def _content_date(text: str, market: str) -> Date | None:
    pat = _TWSE_TITLE_DATE if market == "twse" else _TPEX_TITLE_DATE
    m = pat.search(text)
    if not m:
        return None
    return Date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))


def _req_date(market: str, day: Date) -> str:
    """端點日期參數:twse 西元 yyyymmdd(TwseDetail);tpex 西元 yyyy/MM/dd(TpexV2Detail)。"""
    if market == "twse":
        return twse_date(day)
    return f"{day.year:04d}/{day.month:02d}/{day.day:02d}"


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    """抓當日指數 → **檔頭日期驗證 → 原樣封存 data/ → 解析**(原始檔封存鐵律)。

    順序:先抓 bytes,**落檔前**比對檔頭民國日期(擋 TWSE 靜默 fallback 的錯檔頭)——不符即
    `DateMismatch`(deferred、不封存錯檔),符合才 `archive.save_raw` 再 parse。回 None =
    該檔無資料列(休市/空回應)。body 汙染(檔頭對、內容別天)非本層能擋,見模組 docstring。
    """
    raw = http.fetch_bytes((_TWSE_URL if market == "twse" else _TPEX_URL)
                           .format(d=_req_date(market, day)))
    text = raw.decode("Big5-HKSCS", errors="replace")
    cdate = _content_date(text, market)
    if cdate is not None and cdate != day:
        raise DateMismatch(
            f"{market} index {day}:檔頭日期 {cdate} ≠ 請求日 {day} → fallback 錯檔頭,"
            f"[deferred] 不封存重抓")
    archive.save_raw(TABLE, market, day, raw)     # 位元保真(原始 bytes),tmp→os.replace
    return parse(market, text, day)
