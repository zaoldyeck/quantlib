"""daily_trading_details 源:TWSE T86 + TPEx 3itrade_hedge(三大法人買賣超)。

cache 只留 4 欄差額:foreign_investors_difference、trust_difference、
dealers_difference、total_difference(移植自 TradingReader.readDailyTradingDetails,
但修掉其解析層 bug——不複製)。

## 全格式世代(逐「列」依欄寬分派,不逐「檔」)

交易所歷年悄悄改欄位;更麻煩的是**同一檔內會混不同寬度的列**——現行 TWSE 檔多為
20 欄,但夾雜合法的 17 欄窄列(某些個股當日無外資自營商拆分)。故必須逐列判寬,
用明確的 case 對映,禁止 fallthrough 靜默錯位(對照 Scala reader 的 `values.size match`)。

TWSE(證券代號起頭;欄寬 13/17/20,末欄恆為空尾欄):
  - 13 欄(2012-05-02..2014-11-28):外資買賣超[4],投信買賣超[7],
    **自營商買賣超(淨)[8]**,自營商買[9],自營商賣[10],三大法人買賣超[11]。
    → foreign=[4] trust=[7] dealers=[8](淨,非右移一格的 [10]) total=[11]
  - 17 欄(2014-12-01 起;亦為現行檔的混合窄列):外資單一欄(未拆外資自營商),
    自營商買賣超(淨)[8] 在自行/避險明細之前,三大法人[15]。
    → foreign=[4] trust=[7] dealers=[8] total=[15]
  - 20 欄(現行主格式):外陸資(不含外資自營商)[2..4] + 外資自營商 [5..7],
    投信[8..10],自營商買賣超合計[11],自行[12..14],避險[15..17],三大法人[18]。
    → foreign=[4]+[7] trust=[10] dealers=[11] total=[18]

TPEx(代號起頭;欄寬 12/16/24,無尾空欄):
  - 12 欄(2007-04-23..2014-11-28):外資淨買[4],投信淨買[7],自營商買[8],
    自營商賣[9],**自營淨買[10]**,三大法人[11]。
    → foreign=[4] trust=[7] dealers=[10](淨,非「買股數」[8]) total=[11]
  - 16 欄(2014-12-01..2018-01-12):外資淨買[4],投信淨買[7],自營淨買[8],三大法人[15]。
    → foreign=[4] trust=[7] dealers=[8] total=[15]
  - 24 欄(2018-01-15 起;現行):外資不含自營[2..4] + 外資自營商[5..7] + 外資合計[8..10],
    投信[11..13],自行[14..16],避險[17..19],自營合計[20..22],三大法人合計[23]。
    → foreign=[10](已含外資自營商) trust=[13] dealers=[22] total=[23]

## 修掉的解析層 bug(對照 docs/data_audit/_done/{A,C}-daily_trading_details.json)

1. **自營商三欄對位**:13 欄 TWSE 的 dealers 取淨額 [8](Scala case 13 整組右移一格、
   dealers 存到 [10]=賣出 → 七年零負值);12 欄 TPEx 的 dealers 取淨額 [10](Scala case 12
   第三格誤用 [8]=買股數 → 自營淨買從未入庫、七年零負值)。
2. **Int32→Int64**:schema 宣告 pl.Int64,且解析走 Python 原生 int(無溢位上界)。
   大於 21.47 億的股數(如 00403A 2026-05-12 避險賣出 2,482,291,567)不再靜默歸零
   (Scala `Try(toInt).getOrElse(0)` + PG integer 的溢位地雷不復現)。
3. **fail-loud 不靜默歸零**:數值欄空白 → 0(交易所留空即無此類法人進出,語意等價);
   但非空且非整數 → 拋 SchemaDrift 中止該檔(欄位錯位最壞的失敗模式是靜默給 0)。
4. **日期用內容標題**:CSV 標題的民國日期與檔名日期不符 → 該檔是別天資料的複本,
   fail-loud 拒解析(稽核記 23 天複本;其中標題露餡可辨者 2 天由本閘門擋下,其餘 21 天
   標題印的是「請求日」需跨日指紋閘門,屬 validate 層、非本解析器)。
5. **依列寬分派、不丟合法窄列**:現行 TWSE 檔的 17 欄列(2018 至今 549 列)不再被
   舊 `len(r) < 19` 條件丟掉(稽核 finding F:live cache 每 ~10 天漏十幾檔法人買賣超)。
"""
from __future__ import annotations

import re
from datetime import date as Date

import polars as pl

from research.crawl import archive, http, parse

TABLE = "daily_trading_details"
KEY_COLS = ["market", "date"]
MARKETS = ("twse", "tpex")

_TWSE_URL = ("https://www.twse.com.tw/rwd/zh/fund/T86"
             "?response=csv&selectType=ALLBUT0999&date={d}")
_TPEX_URL = ("https://www.tpex.org.tw/web/stock/3insti/daily_trade/"
             "3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d={d}")

_SCHEMA = {"market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
           "foreign_investors_difference": pl.Int64, "trust_difference": pl.Int64,
           "dealers_difference": pl.Int64, "total_difference": pl.Int64}

#: 個股代碼(去護甲/空白後):首位數字,其餘數字或英文(如 0050 / 00403A / 00632R)。
_CODE = re.compile(r"^[0-9][0-9A-Za-z]*$")
#: 內容標題的民國日期,如「115年07月08日 三大法人買賣超日報」。
_TITLE_DATE = re.compile(r"(\d{2,3})年(\d{1,2})月(\d{1,2})日")

#: (market, 列寬) → (外資買賣超欄, 投信買賣超欄, 自營商買賣超欄, 三大法人買賣超欄)。
#: 外資欄為 tuple:20 欄 TWSE 需 [4]+[7](外陸資 + 外資自營商)才是外資合計;其餘世代
#: 已是合計欄,單欄即可。**逐列以列寬選世代**——同檔混寬時每列各取對映。
_GEN: dict[str, dict[int, tuple[tuple[int, ...], int, int, int]]] = {
    "twse": {
        13: ((4,), 7, 8, 11),
        17: ((4,), 7, 8, 15),
        20: ((4, 7), 10, 11, 18),
    },
    "tpex": {
        12: ((4,), 7, 10, 11),
        16: ((4,), 7, 8, 15),
        24: ((10,), 13, 22, 23),
    },
}

#: 標頭護甲:以「宣告的標頭列寬」對關鍵欄名做 fail-loud 檢查(交易所悄悄加欄地雷)。
#: 只護標頭(=檔案宣告的格式);列則逐寬分派(窄列語義固定、已逐檔驗證)。字串為
#: 去空白後的實測標頭(見 docs/data_audit A 維六世代標頭)。
_HDR_GUARD: dict[tuple[str, int], dict[int, str]] = {
    ("twse", 13): {4: "外資買賣超股數", 7: "投信買賣超股數",
                   8: "自營商買賣超股數", 11: "三大法人買賣超股數"},
    ("twse", 17): {4: "外資買賣超股數", 7: "投信買賣超股數",
                   8: "自營商買賣超股數", 15: "三大法人買賣超股數"},
    ("twse", 20): {4: "外陸資買賣超股數(不含外資自營商)", 7: "外資自營商買賣超股數",
                   10: "投信買賣超股數", 11: "自營商買賣超股數", 18: "三大法人買賣超股數"},
    ("tpex", 12): {4: "外資及陸資淨買股數", 7: "投信淨買股數",
                   10: "自營淨買股數", 11: "三大法人買賣超股數"},
    ("tpex", 16): {4: "外資及陸資淨買股數", 7: "投信淨買股數",
                   8: "自營淨買股數", 15: "三大法人買賣超股數"},
    ("tpex", 24): {10: "外資及陸資-買賣超股數", 13: "投信-買賣超股數",
                   22: "自營商-買賣超股數", 23: "三大法人買賣超股數合計"},
}


def _int(v: str) -> int:
    """數值欄 → int。空白 → 0(交易所留空即無此類法人進出,語意等價 0);
    非空且非整數 → 拋 SchemaDrift(欄位錯位/汙染,絕不靜默歸零)。Python int 無
    溢位上界,故 int32 邊界的大股數忠實保留。"""
    s = parse.clean(v)
    if not s:
        return 0
    try:
        return int(s)
    except ValueError as exc:
        raise parse.SchemaDrift(f"數值欄非整數:{v!r}") from exc


def _guard_header(header: list[str], market: str, hw: int) -> None:
    guard = _HDR_GUARD.get((market, hw))
    if guard is None:
        raise parse.SchemaDrift(
            f"{market} 未知標頭列寬 {hw}(已知 {sorted(_GEN[market])});疑似新格式世代")
    cells = [c.replace(" ", "") for c in header]
    for i, name in guard.items():
        got = cells[i] if i < len(cells) else "<缺>"
        if got != name:
            raise parse.SchemaDrift(
                f"{market} {hw} 欄標頭位移:col[{i}] 期望 {name!r} 實得 {got!r}")


def _guard_title_date(rows: list[list[str]], hidx: int, day: Date) -> None:
    """標頭前的標題列若含民國日期且與檔名日期不符 → 該檔為別天資料的複本,fail-loud。

    找不到可解析的標題日期則不設限(部分檔無標題,不因此拒解析)。"""
    for r in rows[:hidx]:
        if not r:
            continue
        m = _TITLE_DATE.search(r[0])
        if not m:
            continue
        title = Date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
        if title != day:
            raise parse.SchemaDrift(
                f"內容標題日期 {title} 與檔名日期 {day} 不符:別天資料的複本,拒解析")
        return  # 標題日期正確,毋須再看後續列


def _parse(text: str, day: Date, market: str) -> pl.DataFrame | None:
    rows = parse.parse_csv(text)
    marker = "證券代號" if market == "twse" else "代號"
    h = parse.find_header(rows, marker)
    if h < 0:
        return None  # 休市空表/無標頭 → 無資料
    _guard_title_date(rows, h, day)
    _guard_header(rows[h], market, len(rows[h]))
    gens = _GEN[market]
    recs = []
    for r in rows[h + 1:]:
        if not r:
            continue
        code = parse.clean(r[0])
        if not _CODE.match(code):
            continue  # 頁尾說明列(單欄文字)等非資料列
        spec = gens.get(len(r))
        if spec is None:
            raise parse.SchemaDrift(
                f"{market} {code} 資料列未知列寬 {len(r)}(已知 {sorted(gens)})")
        fi, tr, de, to = spec
        recs.append({
            "market": market, "date": day, "company_code": code,
            "foreign_investors_difference": sum(_int(r[i]) for i in fi),
            "trust_difference": _int(r[tr]),
            "dealers_difference": _int(r[de]),
            "total_difference": _int(r[to]),
        })
    return pl.DataFrame(recs, schema=_SCHEMA) if recs else None


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    d = parse.twse_date(day) if market == "twse" else parse.minguo_slash(day)
    url = (_TWSE_URL if market == "twse" else _TPEX_URL).format(d=d)
    raw = http.fetch_bytes(url)
    archive.save_raw("daily_trading_details", market, day, raw)   # 原樣 bytes(位元保真:先落地再 parse)
    return _parse(raw.decode("Big5-HKSCS", errors="replace"), day, market)
