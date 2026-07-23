"""income_statement 源:MOPS 綜合損益表(季頻,兩步 ajax → 多產業模板 chunk CSV)。

cache 表 `is_progressive_raw` 欄:market, type, year, quarter, company_code, title,
value(long-form;一筆 = 某公司某季某科目一個數值。合併報表 type='consolidated')。

## 移植自 FinancialReader.readIncomeStatement(melt / long-form 設計)

Scala(`src/main/scala/reader/FinancialReader.scala:114-163`)的解析是**依表頭名對映**
的 melt,不是固定欄位序:
  1. `allWithHeaders()` → 每列 = Map[表頭 → 值](`headers.zip(line).toMap`:短者截斷、
     重複表頭後者覆蓋)。
  2. 排除 5 個 meta 欄:公司代號/公司名稱/出表日期/年度/季別。
  3. 其餘欄:key `.replace(" ","")`、value `.replace(" ","").replace(",","")`;**只留
     可解析為 double 的 cell**(`Try(toDouble).isSuccess`),melt 成 (title, value)。
  4. type 取自檔名 token(i→individual,c→consolidated);year/quarter 取自檔名。

稽核 `docs/data_audit/_done/A-income_statement.json` 判定此 reader 解析**零錯**
(值/完整性/正負號/單位/編碼/日期全過;melt 依表頭名故「新增欄→fall-through 錯位」
在此結構上不可能)。本 port 對 20 年、兩市場、個別/合併、一般業/金融/證券/保險/金控
各模板**逐位重現 PG**(見 `tests/test_income_statement_parity.py`)。

## 對 Scala 的兩處**修正**(皆有稽核依據,非臆測)

- **編碼 fail-loud 守衛**(A-income_statement SUSPECT):Scala 寫死 Big5-HKSCS 無
  UTF-8 分支,若 MOPS 改用 UTF-8 供檔(營收表 2012 就發生過),中文表頭解碼成亂碼
  → `values.get("公司代號")=None` → `companyCode.get` **裸拋** NoSuchElementException。
  本 port:Big5 優先(對齊 Scala,實測 1989-2026 全 Big5),表頭找不到「公司代號」
  且非 HTML 錯誤頁時**改試 UTF-8**;仍找不到才 `SchemaDrift` fail-loud(而非裸拋)。
- **union upsert 防存活者偏誤**(C-is_progressive_raw BUG#3):KEY_COLS = 完整 unique
  key,`sink.upsert` 只覆蓋本次抓到的 (code,title),**不刪既有列** → 重抓某季不會
  抹掉期間已下市公司(Scala 對舊季 DELETE-then-INSERT 曾把 16 檔已下市股洗掉)。

## 不在本模組(屬 update.py orchestration,主流程接線時處理)

- **開放季度何時停止重抓**(C-is_progressive_raw BUG#1 的完備閘門:連兩次家數相同才
  封存)。本模組的 `refresh` 只保證「開放季度一律重抓」以修根因(不 skip-if-exists),
  精確閘門由呼叫端決定。
- 下游單季還原(`raw_quarterly.py` 的 lag-diff、op_income 別名)是**消費端** bug,
  非本 parser——本 parser 忠實把 `營業淨利(淨損)` 等舊科目名 melt 落地。

fetch(afterIFRSs a_c,2013+):`ajax_t163sb04`(取 chunk filename 清單)→ `t105sb02`
(逐 chunk 下載)。每個 chunk raw **先 archive 原樣落地** data/(封存鐵律,順序不可
顛倒)再 parse。歷史 b_i/b_c(pre-2013)已凍結於 data/,`parse_bytes` 一體適用
(melt 與模板無關,型別由檔名 token 決定)。

需 `cache_tables.py`?否——本模組是 cache 的**寫入端**(增量 upsert),不讀 cache。
parity 測試唯讀對照 PostgreSQL,不碰任何寫入。
"""
from __future__ import annotations

import re
from datetime import date as Date
from pathlib import Path

import polars as pl

from quantlib.crawl import archive, http, parse

TABLE = "is_progressive_raw"
#: 批次 upsert key = DB unique index(market,type,year,quarter,code,title)。用完整
#: unique key(而非整季 (market,type,year,quarter))是刻意的:union 語意,重抓只覆蓋
#: 本次抓到的列、保留既有列 → 修 C-is_progressive_raw BUG#3 存活者偏誤(見模組 docstring)。
KEY_COLS = ["market", "type", "year", "quarter", "company_code", "title"]
MARKETS = ("twse", "tpex")

#: afterIFRSs 綜合損益(a_c)兩步端點(application.conf data.incomeStatement)。
_PAGE = "https://mopsov.twse.com.tw/mops/web/ajax_t163sb04"
_FILE = "https://mopsov.twse.com.tw/server-java/t105sb02"
_TYPEK = {"twse": "sii", "tpex": "otc"}
_TEMPLATE = "a_c"  # 現行只抓 IFRS 合併;舊 GAAP b_i/b_c 已凍結於 data/

#: melt 排除的 5 個 meta 欄(對齊 Scala filterNot;比對用**原始表頭字串**,不 strip)。
_META_COLS = frozenset({"公司代號", "公司名稱", "出表日期", "年度", "季別"})
_CODE_HEADER = "公司代號"

#: 回傳 DF 欄序 = cache 表 is_progressive_raw(year/quarter = DuckDB INTEGER = Int32)。
_SCHEMA = {
    "market": pl.Utf8, "type": pl.Utf8, "year": pl.Int32, "quarter": pl.Int32,
    "company_code": pl.Utf8, "title": pl.Utf8, "value": pl.Float64,
}

#: 封存檔名回推 (year, quarter, type):{year}_{quarter}_{a|b}_{i|c}_{index}.csv。
_FNAME_RE = re.compile(r"^(\d+)_(\d+)_([ab])_([ic])_(\d+)\.csv$")
#: ajax 回傳 HTML 內 <input name=filename value=...>(屬性順序不定,先抓 tag 再抓 value)。
_INPUT_TAG_RE = re.compile(r"<input[^>]*\bname=[\"']?filename[\"']?[^>]*>", re.IGNORECASE)
_VALUE_RE = re.compile(r"\bvalue=[\"']([^\"']*)[\"']", re.IGNORECASE)


def _try_double(v: str) -> float | None:
    """對齊 Scala `Try(v.toDouble)`:可解析為 double 回 float,否則 None。

    值已先 `.replace(" ","").replace(",","")`。實測資料全為 minus 記號的十進位數(稽核:
    無括號負數、無 NaN/Inf),故 `float()` 與 Scala `toDouble` 對本資料集**收/退完全一致**。
    唯一 Python 專屬的寬鬆(PEP515 底線分組,如 `1_000`)在此明確拒收——MOPS 用逗號
    千分位(已被上一步移除),不會有底線,守衛只為與 Scala 嚴格對齊、零副作用。
    """
    if not v or "_" in v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _decode_and_header(raw: bytes) -> tuple[list[str] | None, list[list[str]]]:
    """解碼 raw → (表頭列, 資料列)。找不到有效表頭時 header=None。

    Big5-HKSCS 優先(對齊 Scala reader;稽核實測 1989-2026 全 Big5)。表頭無「公司代號」
    時:若為 MOPS「檔案不存在」HTML 錯誤頁 → 回 (None, []) 讓上層產 0 列(對齊 Scala 對
    HTML chunk 的安全吞掉);否則改試 UTF-8(MOPS 未來若改編碼的防呆)。兩種編碼都找不到
    「公司代號」且非 HTML → `SchemaDrift` fail-loud(取代 Scala `companyCode.get` 裸拋)。
    """
    for enc in ("big5hkscs", "utf-8"):
        try:
            text = raw.decode(enc)
        except UnicodeDecodeError:
            continue
        rows = parse.parse_csv(text)
        if rows and _CODE_HEADER in rows[0]:
            return rows[0], rows[1:]
        if text.lstrip()[:1] == "<":  # <html>… / <font>檔案不存在 → 空產業模板,非漏抓
            return None, []
    raise parse.SchemaDrift(
        "income_statement:表頭無『公司代號』(Big5-HKSCS 與 UTF-8 皆試過)——"
        f"MOPS 改編碼或改欄名?前 80 bytes={raw[:80]!r}")


def _melt(header: list[str], data_rows: list[list[str]], market: str,
          type_: str, year: int, quarter: int) -> list[dict]:
    """把資料列 melt 成 (…, title, value) records。忠實移植 Scala 的欄位對位與值轉換。"""
    recs: list[dict] = []
    for row in data_rows:
        # Scala `headers.zip(line).toMap`:zip 對短者截斷,dict 對重複表頭後者覆蓋。
        cells = dict(zip(header, row))
        numeric: list[tuple[str, float]] = []
        for k, v in cells.items():
            if k in _META_COLS:
                continue
            d = _try_double(v.replace(" ", "").replace(",", ""))
            if d is not None:
                numeric.append((k.replace(" ", ""), d))
        if not numeric:
            continue  # 無數值 cell → 不觸及 company code(對齊 Scala:.get 在 filter 之後)
        code = cells.get(_CODE_HEADER)
        if code is None:
            continue  # header 有公司代號但此列截短取不到(且竟有數值)——理論上不達
        for title, value in numeric:
            recs.append({
                "market": market, "type": type_, "year": year, "quarter": quarter,
                "company_code": code, "title": title, "value": value,
            })
    return recs


def parse_bytes(raw: bytes, market: str, type_: str, year: int,
                quarter: int) -> list[dict]:
    """解析單一 chunk raw bytes → records(cache 欄位)。空/HTML 錯誤頁 → []。"""
    header, data_rows = _decode_and_header(raw)
    if header is None:
        return []
    return _melt(header, data_rows, market, type_, year, quarter)


def parse_raw_file(path: Path) -> list[dict]:
    """解析磁碟上已封存的季報 chunk 檔(市場/年/季/type 由路徑與檔名回推)。

    供 parity 測試與「從 raw 重建 cache」使用;非季報檔名 → ValueError(fail-loud)。
    """
    m = _FNAME_RE.match(path.name)
    if not m:
        raise ValueError(f"非 income_statement 季報 chunk 檔名:{path.name}")
    year, quarter = int(m.group(1)), int(m.group(2))
    type_ = "individual" if m.group(4) == "i" else "consolidated"
    market = path.parts[-3]  # …/income_statement/<market>/<year>/<file>
    return parse_bytes(path.read_bytes(), market, type_, year, quarter)


def _filenames(html: str) -> list[str]:
    """從 ajax HTML 抽 <input name=filename> 的 .csv 值(distinct + sorted,對齊 Scala)。"""
    out: set[str] = set()
    for tag in _INPUT_TAG_RE.finditer(html):
        v = _VALUE_RE.search(tag.group(0))
        if v and v.group(1).endswith(".csv"):
            out.add(v.group(1))
    return sorted(out)


def fetch_quarter(market: str, year: int, quarter: int) -> pl.DataFrame | None:
    """抓某市場某季綜合損益(afterIFRSs a_c)。**每 chunk raw 先 archive 落地再 parse**
    (封存鐵律)。無資料/尚未公告 → None;回 cache-schema DF(type='consolidated')。
    """
    form = {
        "encodeURIComponent": "1", "step": "1", "firstin": "1", "off": "1",
        "isQuery": "Y", "TYPEK": _TYPEK[market],
        "year": str(year - 1911), "season": f"0{quarter}",  # 對齊 Scala s"0${quarter}"
    }
    html = http.fetch_text(_PAGE, encoding="big5hkscs", form=form)
    filenames = _filenames(html)
    if not filenames:
        return None
    frames: list[pl.DataFrame] = []
    for idx, fn in enumerate(filenames):
        raw = http.fetch_bytes(_FILE, form={"firstin": "true", "step": "10", "filename": fn})
        archive.save_raw_named("income_statement", market, year,
                               f"{year}_{quarter}_{_TEMPLATE}_{idx}.csv", raw)
        recs = parse_bytes(raw, market, "consolidated", year, quarter)
        if recs:
            frames.append(pl.DataFrame(recs, schema=_SCHEMA))
    if not frames:
        return None
    return (pl.concat(frames)
            .unique(subset=KEY_COLS, keep="first", maintain_order=True))


def _recent_quarters(upto: Date, n: int) -> list[tuple[int, int]]:
    """由 upto 回推最近 n 個季度(含當季)。"""
    y, q, out = upto.year, (upto.month - 1) // 3 + 1, []
    for _ in range(n):
        out.append((y, q))
        q -= 1
        if q == 0:
            y, q = y - 1, 4
    return out


def refresh(sink, upto: Date, quarters_back: int = 4) -> int:
    """補抓最近 quarters_back 個季度並 upsert(full-key union,idempotent)。回新增列數。

    **恆重抓、不 skip-if-exists**:直接修 C-is_progressive_raw BUG#1 根因——Scala
    `Task.pullQuarterlyFiles`「該季已有檔就永不重抓」把申報期限前的殘檔凍結,金控/銀行/
    KY 股(晚申報)永遠缺料;此處每次都重抓開放中的近期季度,晚申報公司於後續 run 補齊。
    **union 語意**(KEY_COLS = 全 unique key)使重抓不刪既有列 → 不抹掉期間已下市公司
    (修 BUG#3)。完備閘門(連兩次家數相同才封存)屬 update.py,由呼叫端決定 quarters_back。
    """
    total = 0
    for market in MARKETS:
        for year, quarter in _recent_quarters(upto, quarters_back):
            try:
                df = fetch_quarter(market, year, quarter)
            except Exception as exc:  # noqa: BLE001 - 單季失敗不擋其餘季
                print(f"[crawl] income_statement/{market} {year}Q{quarter} 抓取失敗:{exc}")
                continue
            if df is None:
                continue
            n = sink.upsert(TABLE, df, KEY_COLS)
            total += n
            print(f"[crawl] income_statement/{market} {year}Q{quarter}: {n} 列")
    return total
