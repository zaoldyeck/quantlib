"""operating_revenue 源:MOPS t21sc03 月營收(POST FileDownLoad;歷史含 HTML)。

S 進場訊號的原料(新鮮月營收 cohort)。cache 欄:market, type, year, month,
company_code, company_name, industry, monthly_revenue, monthly_revenue_yoy,
report_date。移植自 FinancialReader.readOperatingRevenue,**吃全部三個歷史格式世代**
(從 raw 重建歷史時各世代都要能正確解析,不能只吃現行格式):

  | 世代 | 年代 / type | 實體 | 欄位對映(cache 用到的) |
  |---|---|---|---|
  | HTML 個別 | 2001-2012 `_i.html`(Big5) | 每列 10 cell | code=c0、name=c1、rev=c2、yoy=c6;`產業別：X` 標題列累積 industry;report_date=None(出表日期是批次再匯出日,非 PIT 真值) |
  | CSV IFRS 前合併 | 2005-2012 `_c.csv`(Big5) | 資料列 11 欄 | code=v0、name=v1、industry=None、rev=v2、yoy=v6;report_date=None(無出表日期欄) |
  | CSV IFRS 後 | 2013+ `_c.csv`(UTF-8) | 14 欄 | code=v2、name=v3、industry=v4、rev=v5、yoy=v9、report_date=v0(出表日期,FC8 PIT 錨) |

**yoy = 去年同月增減%**(HTML/11 欄 = 第 6 欄、14 欄 = 第 9 欄;14 欄第 8 欄是「上月比較
增減%」非 yoy——A-operating_revenue 引的 -23.92 即此欄,勿誤取)。世代由**內容嗅探
(HTML vs CSV)+ 欄數明確 case**判定;非 {11,14} 欄的 CSV 資料列 → `parse.SchemaDrift`
fail-loud(取代 Scala `case _` 靜默把新欄數當 14 欄的錯位風險)。忠實照抄來源錯值
(聯發科 2007-7 合併 rev 溢位 69 億仟元、720 筆 999999.99 百分比哨兵)——那是來源壞、
非解析錯,不無中生有修改。

月頻:`refresh` 補最近數月(idempotent);更新後須 `rebuild_industry_taxonomy`。
`parse_file`/`parse_bytes` = 從封存 raw 重建 + parity 對照的入口(對齊 income_statement
`parse_raw_file` / balance_sheet `parse_file`)。
"""
from __future__ import annotations

import re
from datetime import date as Date
from html.parser import HTMLParser
from pathlib import Path

import polars as pl

from research.crawl import archive, http, parse
from research.crawl.sink import Sink

TABLE = "operating_revenue"
KEY_COLS = ["market", "type", "year", "month"]
MARKETS = ("twse", "tpex")

_URL = "https://mopsov.twse.com.tw/server-java/FileDownLoad"
_FILEPATH = {"twse": "/home/html/nas/t21/sii/", "tpex": "/home/html/nas/t21/otc/"}
#: 每次補抓的最近月數(涵蓋「M 月營收於 M+1 月 10 日左右公告」的發布延遲)
_REFRESH_MONTHS = 3

_SCHEMA = {"market": pl.Utf8, "type": pl.Utf8, "year": pl.Int32, "month": pl.Int32,
           "company_code": pl.Utf8, "company_name": pl.Utf8, "industry": pl.Utf8,
           "monthly_revenue": pl.Float64, "monthly_revenue_yoy": pl.Float64,
           # 出表日期(原始檔第一欄,民國):**產業別 PIT 的真實生效錨**(FC8)。MOPS 月報回傳
           # 的產業別是**當前**分類,套到歷史月會前視(3687 歐買尬 2020 的「數位雲端」被回填
           # 到 2013);出表日期記錄該檔實際產出時點,industry_taxonomy 用它(取每家每產業最早
           # 出表日期)當 effective_date,即消除前視。缺欄(極舊格式)→ null,builder 退回月推算。
           "report_date": pl.Date}

#: 代號:4 碼一般股 + 6 碼 TDR(910069/912000/912398 存託憑證,2011+ 即出現)+ 可帶
#: 一位英數尾碼(KY/特別股)。舊式 `^\d{4}[0-9A-Z]?$` 卡死 6 碼 → C-operating_revenue
#: BUG:280 列 TDR 月營收被 `_CODE.match` 靜默丟棄(下市/存活者偏誤)。實測 raw 全史
#: CSV 代號僅 {4 碼, 6 碼} 兩型,無 5 碼、無英數尾碼,放寬到 4-6 碼零誤納。
_CODE = re.compile(r"^\d{4,6}[0-9A-Z]?$")
#: HTML 個別檔的產業標題列(`產業別：水泥`)——與 Scala `產業別：(.*)` 同義,累積給後續資料列。
_INDUSTRY_RE = re.compile(r"^產業別：(.*)$")
#: Jsoup 把 `公司<br>代號` 正規成中間單一空白;HTML 表頭列以此為守衛(對齊 Scala `!= "公司 代號"`)。
_HTML_HEADER = "公司 代號"
#: 封存檔名 → (year, month, type token):`<year>_<month>_<i|c>.<html|csv>`。
_FNAME_RE = re.compile(r"^(\d+)_(\d+)_([ic])\.(?:html|csv)$")
_TYPE = {"i": "individual", "c": "consolidated"}


def _dbl(v: str) -> float | None:
    try:
        return float(v.replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def _roc_date(v: str) -> Date | None:
    """民國日期字串(如 109/06/14 或 "109/6/14")→ 西元 date;無法解析回 None。"""
    try:
        parts = v.strip().strip('"').split("/")
        if len(parts) != 3:
            return None
        roc_y, mo, dy = int(parts[0]), int(parts[1]), int(parts[2])
        return Date(roc_y + 1911, mo, dy)
    except (ValueError, AttributeError):
        return None


class _TableRows(HTMLParser):
    """寬鬆 HTML → 每 `<tr>` 的 td/th 文字列。

    **遇新 `<tr>` 自動關閉上一列**(HTML5 錯誤修復,對齊 Scala 用的 Jsoup):解掉舊
    HTML 缺 `</tr>` 閉合的地雷——tpex 2001-6 4108 懷特新藥列即缺 `</tr>`,stdlib regex
    土炮解析會把它與下一列合併漏抓,本類與 Jsoup 一致正確切出 10 cell(A-operating_revenue
    實測 Jsoup 勝出點)。`<br>` → 空白(Jsoup 對 `公司<br>代號` 的正規化),charref
    (`&nbsp;`)由 stdlib 轉成 U+00A0,清洗時併入空白 → 空 cell → 數值欄轉 None。
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag == "tr":
            self._flush()
            self._row = []
        elif tag in ("td", "th"):
            if self._row is None:
                self._row = []
            self._end_cell()
            self._cell = []
        elif tag == "br" and self._cell is not None:
            self._cell.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th"):
            self._end_cell()
        elif tag == "tr":
            self._flush()

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def _end_cell(self) -> None:
        if self._cell is not None:
            self._row.append(re.sub(r"\s+", " ", "".join(self._cell)).strip())
            self._cell = None

    def _flush(self) -> None:
        self._end_cell()
        if self._row:
            self.rows.append(self._row)
        self._row = None

    def close(self) -> None:
        super().close()
        self._flush()


def _parse_html(raw: bytes, market: str, type_: str, year: int,
                month: int) -> list[dict]:
    """HTML 個別檔(2001-2012):code=c0、name=c1、rev=c2、yoy=c6;`產業別：` 標題列累積 industry。"""
    p = _TableRows()
    p.feed(raw.decode("big5hkscs", errors="replace"))  # Jsoup 寬鬆解碼(A-audit:1 檔壞 byte 不整檔炸)
    p.close()
    recs: list[dict] = []
    industry = ""
    for v in p.rows:
        if len(v) == 10 and v[0] != _HTML_HEADER:  # 對齊 Scala size==10 && head!="公司 代號"
            code = v[0].strip()
            if not _CODE.match(code):
                continue  # 合計/總計 等非資料列(size 通常≠10,此為雙保險)
            recs.append({
                "market": market, "type": type_, "year": year, "month": month,
                "company_code": code, "company_name": v[1].strip(),
                "industry": (industry or None),
                "monthly_revenue": _dbl(v[2]), "monthly_revenue_yoy": _dbl(v[6]),
                # 舊 HTML 的「出表日期」是批次再匯出日(2010-07 檔標 2013-05),非 PIT 真值 → None
                "report_date": None,
            })
        elif v:
            m = _INDUSTRY_RE.match(v[0])
            if m:
                industry = m.group(1).strip()
    return recs


def _parse_csv(raw: bytes, market: str, type_: str, year: int,
               month: int) -> list[dict]:
    """CSV 合併檔:11 欄(IFRS 前,code=v0)/ 14 欄(IFRS 後,code=v2)。**明確 case,非
    fallthrough**——非 {11,14} 欄的資料列 → `parse.SchemaDrift`(TWSE 悄悄加欄時 fail-loud,
    取代 Scala `case _` 把新欄數當 14 欄靜默錯位)。"""
    text = raw.decode("utf-8" if year > 2012 else "big5hkscs", errors="replace")
    rows = parse.parse_csv(text)  # 還原 `="..."` 護甲(共用清洗)
    if len(rows) < 2:
        return []
    recs: list[dict] = []
    for r in rows[1:]:  # 對齊 Scala reader.all().tail:丟表頭(表頭恆 row[0])
        n = len(r)
        if n == 11:      # IFRS 前合併(2005-2012 Big5):代號=v0、無產業欄、無出表日期
            code = r[0].strip()
            rec = {"company_code": code, "company_name": r[1].strip(),
                   "industry": None, "monthly_revenue": _dbl(r[2]),
                   "monthly_revenue_yoy": _dbl(r[6]), "report_date": None}
        elif n == 14:    # IFRS 後(2013+ UTF-8):代號=v2、產業=v4、出表日期=v0
            code = r[2].strip()
            rec = {"company_code": code, "company_name": r[3].strip(),
                   "industry": (r[4].strip() or None),
                   "monthly_revenue": _dbl(r[5]), "monthly_revenue_yoy": _dbl(r[9]),
                   "report_date": _roc_date(r[0])}  # 出表日期(FC8:產業別 PIT 生效錨)
        else:
            raise parse.SchemaDrift(
                f"operating_revenue CSV {market} {year}-{month}:資料列 {n} 欄"
                f"(僅支援 11 欄 IFRS 前 / 14 欄 IFRS 後)——TWSE 改版加欄?列首={r[:4]}")
        if not _CODE.match(code):
            continue  # 表頭殘留 / 合計 / 非資料列
        rec.update({"market": market, "type": type_, "year": year, "month": month})
        recs.append(rec)
    return recs


def parse_bytes(raw: bytes, market: str, type_: str, year: int,
                month: int) -> list[dict]:
    """單檔 raw bytes → records(cache 欄位)。由**內容嗅探**分派 HTML vs CSV 世代
    (MOPS 對錯誤請求回 HTML 錯誤頁,嗅探也順帶擋掉);空/錯誤頁 → []。"""
    head = raw.lstrip()[:16].lower()
    is_html = head.startswith(b"<") or b"<html" in head or b"<tr" in raw[:4096].lower()
    if is_html:
        return _parse_html(raw, market, type_, year, month)
    return _parse_csv(raw, market, type_, year, month)


def _to_df(recs: list[dict]) -> pl.DataFrame | None:
    """records → cache-schema DF,單檔內 (market, code) 去重取首見(對齊 Scala
    `distinctBy(market, code)`)。空 → None。"""
    if not recs:
        return None
    return (pl.DataFrame(recs, schema=_SCHEMA)
            .unique(subset=["company_code"], keep="first", maintain_order=True))


def parse_file(path: str | Path) -> pl.DataFrame | None:
    """讀一個封存原始檔 → cache-schema DF(從 raw 重建 + parity 入口)。

    market/type/year/month 由**路徑與檔名**推得(type/期別取自檔名不看內容日期);
    format(HTML/CSV)由 `parse_bytes` 嗅探內容。非本源檔名 → ValueError fail-loud。

    **兩種封存佈局都吃**:年子目錄 `<market>/<year>/<file>`(Scala 慣例)與扁平
    `<market>/<file>`(近月 2025-06~2026-03 部分檔即扁平落地)。故 market 以「往上找
    第一個 twse/tpex 目錄」判定,而非固定層數——否則扁平檔會被誤標成 market='operating_revenue'。
    """
    path = Path(path)
    m = _FNAME_RE.match(path.name)
    if not m:
        raise ValueError(f"operating_revenue:檔名不符 <year>_<month>_<i|c>.<html|csv>:{path.name}")
    year, month, tok = int(m.group(1)), int(m.group(2)), m.group(3)
    market = next((p for p in reversed(path.parts[:-1]) if p in MARKETS), None)
    if market is None:
        raise ValueError(f"operating_revenue:路徑找不到 market(twse/tpex 目錄):{path}")
    recs = parse_bytes(path.read_bytes(), market, _TYPE[tok], year, month)
    return _to_df(recs)


def fetch_month(market: str, year: int, month: int) -> pl.DataFrame | None:
    """抓某市場某年月的月營收(現行 afterIFRSs 合併 CSV)。無資料/尚未公告 → None。

    走全世代 `parse_bytes`(現行檔即 14 欄分支);歷史世代由 `parse_file` 從封存 raw 重建。
    """
    form = {"step": "9", "functionName": "show_file", "filePath": _FILEPATH[market],
            "fileName": f"t21sc03_{year - 1911}_{month}.csv"}
    raw = http.fetch_bytes(_URL, form=form)  # bytes:讓 parse_bytes 自行按世代解碼
    text = raw.decode("utf-8", errors="replace")
    if "查詢無資料" in text or "無應揭露資訊" in text:
        return None  # 無資料 → 不封存(對齊 ex_right:無資料檔不落地)
    # 原始檔封存鐵律:先原樣原子落地 raw 才 parse(歷史命名 {年}_{月}_c.csv,c=合併)
    archive.save_raw_named(TABLE, market, year, f"{year}_{month}_c.csv", raw)
    return _to_df(parse_bytes(raw, market, "consolidated", year, month))


def _recent_months(upto: Date, n: int) -> list[tuple[int, int]]:
    y, m = upto.year, upto.month
    out = []
    for _ in range(n):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        out.append((y, m))
    return out


def refresh(sink: Sink, upto: Date) -> int:
    """補抓最近 _REFRESH_MONTHS 個月(idempotent upsert)。回新增列數。"""
    total = 0
    for market in MARKETS:
        for year, month in _recent_months(upto, _REFRESH_MONTHS):
            try:
                df = fetch_month(market, year, month)
            except Exception as exc:  # noqa: BLE001 - 單月失敗不擋其餘月
                print(f"[crawl] operating_revenue/{market} {year}-{month:02d} 抓取失敗:{exc}")
                continue
            if df is None:
                continue
            n = sink.upsert(TABLE, df, KEY_COLS)
            total += n
            print(f"[crawl] operating_revenue/{market} {year}-{month:02d}: {n} 列")
    return total


def rebuild_industry_taxonomy(sink: Sink) -> None:
    """月營收更新後重算 PIT 產業分類(重用 research.industry_taxonomy,唯一真源)。"""
    from research.industry_taxonomy import build_industry_taxonomy_pit

    df = build_industry_taxonomy_pit(sink.con)
    sink.con.register("_it_new", df)
    try:
        sink.con.execute("DROP TABLE IF EXISTS industry_taxonomy_pit")
        sink.con.execute("CREATE TABLE industry_taxonomy_pit AS SELECT * FROM _it_new")
    finally:
        sink.con.unregister("_it_new")
    print(f"[crawl] industry_taxonomy_pit 重算 {df.height} 列")
