"""cash_flows 源:MOPS IFRSs 財報現金流量表(季頻,合併基礎優先)。

移植自 Scala `FinancialReader.readFinancialStatements`(cash_flows 部分)+
`Crawler.getFinancialStatements` + `FinancialStatementsSetting`。目標表(cache)=
`cf_progressive_raw`,PG 表 = `cash_flows_progressive`。**長表**:一列 = 一個
(公司, 年, 季, 會計科目, 累計金額);value 為**年度累計數(progressive/YTD)**,
下游用「本季累計 − 上季累計」還原單季。

cache 欄:market, year, quarter, company_code, title, value(market 一律 'tw',
Scala 硬寫)。

## 三個版型(explicit case,不用 fallthrough)

MOPS 財報 HTML 三個時代,欄位對位與值轉換各不同——**忠實逐位移植 Scala**:

| 年代        | 編碼        | 版面                                   | 標題欄 | 金額欄 | 負值表示 |
|-------------|-------------|----------------------------------------|--------|--------|----------|
| < 2013      | Big5-HKSCS  | 單一 `table.result_table`,三表以空列分段 | td[0]  | td[1]  | 負號     |
| 2013–2018   | UTF-8       | 各表獨立 `table`(BS/IS/CF)             | td[0]  | td[1]  | 負號     |
| ≥ 2019      | UTF-8       | bulk ZIP,`div.content > table`(CF 第 3 張) | td[1]  | td[2]  | 括號 `()` |

**現金流量表以「內容」定位,不靠脆弱的 `table:nth-child(N)`**(稽核鐵律:版位會
漂移,內容不會——比對 Scala nth-child 逐位一致,見 tests/test_cash_flows_parity.py)。
每個版型都有 header 守衛(`parse.SchemaDrift` fail-loud),MOPS 一改版就炸,絕不靜默錯位。

## 稽核修正(docs/data_audit/_done/C-cf_progressive_raw.json)

解析本身零瑕疵(cache==PG 逐位),真正的 bug 在**爬蟲抓取閘門**:

- **「資料夾存在 = 抓完了」→ 整季殘缺**(2026Q1 只有 544/2263 家、2025Q2/2023Q2/
  2024Q1 缺金融業+KY 股):Scala `Task.pullFinancialStatements` 只要
  `data/financial_statements/<Y>_<Q>/` 存在就永不重抓,而月份閘門 `m<5 → 允許抓當年 Q1`
  早於 5/15 申報期限。**本移植的修正**:`fetch_quarter` 永遠可強制重抓(idempotent
  覆寫封存),不以資料夾存在為由跳過;`needs_refetch()` 改用**可交易公司**的現金流量表
  覆蓋率 vs 前一年同季判齊備(跌幅 > 4.5pp 或絕對 < 50% 就重抓,門檻由歷史齊備季實測——
  見下方齊備閘門段落),不靠猜出來的日曆天。
- **report_basis(cr/ir/er)未落欄**(SUSPECT):屬 schema 變更(動 Slick + cache_tables),
  超出本移植範圍;此處忠實複製 Scala 的 cr>er>ir 選檔偏好(`BASIS_PREFERENCE` 具名常數),
  並在下方註明「每季約 350~450 家為個體基礎,與 is/bs 的 consolidated 不同基礎」。

## 原始檔封存鐵律

`fetch_quarter`:下載 ZIP → **先原樣封存 ZIP 到 data/financial_statements/_zip/** →
解壓 HTML 到 data/financial_statements/<Y>_<Q>/(沿用 Scala 時代目錄)→ 再 parse。
順序不可顛倒(archive.py 鐵律)。

## cache 依賴

模組本身解析不依賴 cache。parity 測試(tests/test_cash_flows_parity.py)讀
`cf_progressive_raw` 當對照基準 → 需 `research/cache_tables.py` 為當前世代。

Run(單季解析已封存 raw):
    uv run --project research python -m research.crawl.sources.cash_flows 2020 2
"""
from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path

import lxml.html as _LH
import polars as pl

from research import paths
from research.crawl import http, parse

TABLE = "cf_progressive_raw"
KEY_COLS = ["market", "year", "quarter", "company_code"]
MARKETS = ("tw",)  # Scala 硬寫 market='tw';cache 亦 WHERE market='tw'

_SCHEMA = {
    "market": pl.Utf8, "year": pl.Int32, "quarter": pl.Int32,
    "company_code": pl.Utf8, "title": pl.Utf8, "value": pl.Float64,
}

#: 首季 = FinancialStatementsSetting.firstDate 2009-04-01 → 2009Q4(quarter=month)
FIRST_YEAR, FIRST_QUARTER = 2009, 4
#: 2019 起改用整季 bulk ZIP(FinancialStatementsSetting bulkInstanceDocuments)
_BULK_ERA_YEAR = 2019
#: pre-IFRS 換版年(< 2013 用 Big5 + 單表分段;≥ 2013 UTF-8 + 獨立表)
_PRE_IFRS_YEAR = 2013

#: 合併/個體選檔偏好(Scala `.sortBy(_._1).distinctBy(_._2)` 的字典序 cr<er<ir)。
#: cr=合併(consolidated,優先)、er=其他、ir=個體(individual)。同一公司同季若有多種,
#: 取 cr;每季約 350~450 家「只有 ir」的公司是個體基礎(無子公司,ir 就是其唯一正式財報),
#: 與 is_progressive/bs_concise 的 type='consolidated' **不同基礎**——消費者做「CFO>NI」
#: 比較時要留意(稽核 C-cf_progressive_raw SUSPECT)。
BASIS_PREFERENCE = ("cr", "er", "ir")

_FS_DIR = paths.RAW / "financial_statements"
_ZIP_DIR = _FS_DIR / "_zip"
_BULK_URL = ("https://mopsov.twse.com.tw/server-java/FileDownLoad?step=9"
             "&filePath=/home/html/nas/ifrs/{year}/&fileName=tifrs-{year}Q{quarter}.zip")

#: 2019+ 檔名:tifrs-<frX>-<mN>-<industry>-<basis>-<code>-<YYYYQq>.html
#: split('-')[4]=basis、[5]=code(對齊 Scala splitName(4)/splitName(5))。
_TIFRS_RE = re.compile(
    r"^tifrs-[^-]+-[^-]+-[^-]+-(?P<basis>[A-Za-z]+)-(?P<code>[0-9A-Za-z]+)-\d{4}Q\d\.html$",
    re.IGNORECASE)
_CODE_HEAD = re.compile(r"^(\w+)")
_XML_DECL = re.compile(r"^\s*<\?xml[^>]*\?>", re.IGNORECASE)

#: 現金流量表定位/守衛用內容標記
_CF_TITLE_2019 = "現金流量表"       # 2019+ 表首
_CF_MARKER = "營業活動"             # 每張現金流量表必含的科目段落(CFO)
_STMT_HEADER = "會計項目"           # 2013+ 各表 header 首格(2019+ 現金流量表亦含)


# ─────────────────────────── HTML 基礎工具 ───────────────────────────

def _encoding(year: int) -> str:
    return "big5-hkscs" if year < _PRE_IFRS_YEAR else "utf-8"


def _doc(raw: bytes, year: int):
    """bytes → lxml 文件。以年代編碼自解(Python 認得 big5-hkscs),去掉 `<?xml?>`
    宣告(lxml 不吃帶宣告的 str)。lxml/libxml2 為寬鬆解析器,對 MOPS 的殘缺 HTML
    做與 jsoup(Scala)同級的正規化(自動補閉合、保留文件順序)。"""
    text = raw.decode(_encoding(year), errors="replace")
    return _LH.fromstring(_XML_DECL.sub("", text, count=1))


def _rows(table):
    """表格頂層資料列(jsoup `table > tbody > tr` 自動補 tbody 後 == 本表頂層列;
    libxml2 不補 tbody,故三路徑都收,且不含巢狀表的列)。"""
    return table.xpath("./tr | ./tbody/tr | ./thead/tr")


def _tds(tr):
    return tr.xpath("./td")


def _strip_ws(s: str) -> str:
    """去掉所有空白(含全形 　)——對齊 Scala `filterNot(_.isWhitespace)`。
    str.split() 以 str.isspace() 斷詞,　.isspace()=True,與 Java 等價
    (parity 逐位驗證)。"""
    return "".join(s.split())


def _td_text_zh(td) -> str:
    """2019+ 專用:優先取 `span.zh` 文字(中文科目名,排除英文),否則整格文字
    (數字格無 span.zh → 取 `<pre>(...)`,括號=負)。對齊 Scala
    `(td >?> text("span.zh")).getOrElse(td.text)`。"""
    zh = td.cssselect("span.zh")
    return zh[0].text_content() if zh else td.text_content()


def _num_paren(raw_text: str) -> float | None:
    """2019+ 金額:去空白/逗號;含 `(` 與 `)` → 負;strip 括號後 float。
    對齊 Scala `valueString.replace("(","").replace(")","").toDouble` +
    `if (contains('(') && contains(')')) -vDouble`。"""
    v = _strip_ws(raw_text).replace(",", "")
    if not v:
        return None
    neg = "(" in v and ")" in v
    v = v.replace("(", "").replace(")", "")
    if not v:
        return None
    return -_to_float(v, raw_text) if neg else _to_float(v, raw_text)


def _num_plain(raw_text: str) -> float | None:
    """< 2019 金額:去空白/逗號後直接 float(負號表示,無括號邏輯)——對齊 Scala
    pre-2013 / 2013-2018 的 `.replace(",","").toDouble`。若出現非數值(如括號),
    Scala 會 NumberFormatException;此處 fail-loud(SchemaDrift),不靜默錯號。"""
    v = _strip_ws(raw_text).replace(",", "")
    if not v:
        return None
    return _to_float(v, raw_text)


def _to_float(v: str, orig: str) -> float:
    try:
        return float(v)
    except ValueError as exc:
        raise parse.SchemaDrift(
            f"cash_flows 金額非數值:{orig!r}(MOPS 版型改變?)") from exc


# ─────────────────────────── 三版型解析 ───────────────────────────

def _guard(cond: bool, msg: str) -> None:
    if not cond:
        raise parse.SchemaDrift(f"cash_flows 版型守衛失敗:{msg}")


def _emit(out: dict[str, float | None], key_title: str, value: float | None) -> None:
    # distinctBy(_._1):同科目名保留第一次出現(文件順序)。
    if key_title not in out:
        out[key_title] = value


def _parse_2019(doc, code: str) -> dict[str, float | None]:
    """≥ 2019:div.content 下的現金流量表;title=td[1](span.zh)、value=td[2]。

    無現金流量表 → 回空(對齊 Scala `cashFlowsOption.getOrElse(Seq.empty)`):部分
    特殊申報人(債券/票券 000xxx、興櫃)本就不編現金流量表,cache 亦 0 列。
    **header 位置守衛(fail-loud,比照 daily_quote._guard)**只在「找到現金流量表卻
    欄位對位失效」時觸發——這是 live(2019+)MOPS 改版會踩到的地雷;大規模缺表則由
    fetch 端 `needs_refetch` 覆蓋率閘門攔截。
    """
    tables = doc.cssselect("div.content > table")
    cands = [t for t in tables if t.text_content().lstrip().startswith(_CF_TITLE_2019)]
    if not cands:
        return {}
    cf = cands[0]
    _guard(_STMT_HEADER in cf.text_content(),
           f"{code}: 現金流量表缺 header「{_STMT_HEADER}」(欄位對位失效?)")
    out: dict[str, float | None] = {}
    seen_wide = False
    for tr in _rows(cf):
        cells = [_td_text_zh(td) for td in _tds(tr)]
        if len(cells) <= 1:
            continue
        if len(cells) >= 3:
            seen_wide = True
        title = cells[1]
        value = cells[2] if len(cells) > 2 else cells[1]
        if not value.strip():
            continue
        _emit(out, _strip_ws(title), _num_paren(value))
    _guard(seen_wide, f"{code}: 現金流量表無 ≥3 欄資料列(title=td[1]/value=td[2] 對位失效)")
    return out


def _stmt_tables(doc) -> list:
    """2013-2018:#content_d 下 header 首格 == 會計項目 的表(BS/IS/CF,文件順序)。"""
    out = []
    for t in doc.cssselect("#content_d table"):
        rs = _rows(t)
        if not rs:
            continue
        head = rs[0].xpath("./th | ./td")
        if head and _strip_ws(head[0].text_content()) == _STMT_HEADER:
            out.append(t)
    return out


def _parse_2013_2018(doc, code: str) -> dict[str, float | None]:
    """2013-2018:三張獨立表中含「營業活動」者為現金流量表;title=td[0]、value=td[1]。"""
    stmts = _stmt_tables(doc)
    cands = [t for t in stmts if _CF_MARKER in t.text_content()]
    if not cands:
        # 有 statement 表卻無現金流量表(僅 BS/IS)→ 罕見但 Scala getOrElse(empty)
        # 亦回空;parity 若因此漏公司會立刻紅燈,故此處回空、由 parity 當守衛。
        return {}
    cf = cands[0]
    out: dict[str, float | None] = {}
    for tr in _rows(cf):
        cells = [td.text_content() for td in _tds(tr)]
        if len(cells) <= 1:
            continue
        title, value = cells[0], cells[1]
        if not value.strip():
            continue
        _emit(out, _strip_ws(title), _num_plain(value))
    return out


def _parse_pre_2013(doc, code: str) -> dict[str, float | None]:
    """< 2013:單一 result_table,三表以「空列(0 td 的 tr)」分段。
    drop(2) → span(nonEmpty)=BS,rest drop(2) → span=IS,其餘=CF(對齊 Scala span 邏輯)。"""
    tables = doc.cssselect("table.result_table")
    if not tables:
        return {}
    rows = [[td.text_content() for td in _tds(tr)] for tr in _rows(tables[0])]

    def _span(rs):
        i = 0
        while i < len(rs) and len(rs[i]) > 0:
            i += 1
        return rs[:i], rs[i:]

    # 無 header 守衛:此段以「空列(0 td 的 tr)」分段,不依賴 header 文字——不同季
    # header 有時落在 <th>(rows[0]/[1] 為空 td 列),span 邏輯照樣正確切段(2011Q2/Q4
    # 皆逐位對上 cache)。且 pre-2013 檔為凍結歷史,無 MOPS 改版風險。
    _, rest = _span(rows[2:])         # BS 段丟棄,rest 由空列起
    _, cf_rows = _span(rest[2:])      # IS 段丟棄,cf_rows 為現金流量表段
    out: dict[str, float | None] = {}
    for r in cf_rows:
        if len(r) <= 1 or not r[1].strip():
            continue
        _emit(out, _strip_ws(r[0]), _num_plain(r[1]))
    return out


def _parse_file(path: Path, year: int, quarter: int, code: str) -> list[dict]:
    """解析單一公司財報 HTML,回現金流量表長表列。"""
    raw = path.read_bytes()
    doc = _doc(raw, year)
    if year >= _BULK_ERA_YEAR:
        cf = _parse_2019(doc, code)
    elif year >= _PRE_IFRS_YEAR:
        cf = _parse_2013_2018(doc, code)
    else:
        cf = _parse_pre_2013(doc, code)
    return [
        {"market": "tw", "year": year, "quarter": quarter,
         "company_code": code, "title": title, "value": value}
        for title, value in cf.items()
    ]


# ─────────────────────────── 選檔 + 全季解析 ───────────────────────────

def _quarter_dir(year: int, quarter: int) -> Path:
    return _FS_DIR / f"{year}_{quarter}"


def _select_company_files(dir_path: Path, year: int) -> list[tuple[str, Path]]:
    """每公司選一個檔。2019+ 依 BASIS_PREFERENCE(cr>er>ir)去重;< 2019 為
    per-company `{code}.html`(合併已覆蓋個體,磁碟上每公司單檔)。"""
    files = sorted(dir_path.glob("*.html"))
    if year >= _BULK_ERA_YEAR:
        picked: dict[str, tuple[str, Path]] = {}
        for f in files:
            m = _TIFRS_RE.match(f.name)
            if not m:
                continue
            basis, code = m.group("basis").lower(), m.group("code")
            cur = picked.get(code)
            # Scala sortBy(basis) 字典序穩定 → 取最小 basis;同 basis 取檔名序第一
            if cur is None or (basis, f.name) < (cur[0], cur[1].name):
                picked[code] = (basis, f)
        return sorted((code, f) for code, (_b, f) in picked.items())
    out: list[tuple[str, Path]] = []
    for f in files:
        if f.stat().st_size == 0:
            continue
        m = _CODE_HEAD.match(f.stem)
        if m:
            out.append((m.group(1), f))
    return sorted(out)


def parse_quarter(year: int, quarter: int, *, base_dir: Path | None = None) -> pl.DataFrame:
    """解析已封存的某季全部公司 → cache 同構長表 DataFrame(market/year/quarter/
    company_code/title/value)。base_dir 預設 data/financial_statements/<Y>_<Q>/。"""
    dir_path = base_dir if base_dir is not None else _quarter_dir(year, quarter)
    if not dir_path.exists():
        return pl.DataFrame(schema=_SCHEMA)
    records: list[dict] = []
    for code, path in _select_company_files(dir_path, year):
        records.extend(_parse_file(path, year, quarter, code))
    if not records:
        return pl.DataFrame(schema=_SCHEMA)
    return pl.DataFrame(records, schema=_SCHEMA)


# ─────────────────────────── fetch(2019+ bulk ZIP) ───────────────────────────

def _looks_like_zip(data: bytes) -> bool:
    return data[:2] == b"PK"


def _archive_zip(year: int, quarter: int, data: bytes) -> Path:
    """原子封存原始 ZIP(archive 鐵律:parse 前先落地)。"""
    _ZIP_DIR.mkdir(parents=True, exist_ok=True)
    dest = _ZIP_DIR / f"tifrs-{year}Q{quarter}.zip"
    tmp = dest.with_suffix(".zip.tmp")
    tmp.write_bytes(data)
    tmp.replace(dest)
    return dest


def _extract_html(data: bytes, dest_dir: Path) -> int:
    """解壓 ZIP 中所有 *.html(攤平 basename)到 dest_dir。回寫出檔數。"""
    dest_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = Path(info.filename).name
            if not name.lower().endswith(".html"):
                continue
            out = dest_dir / name
            tmp = out.with_suffix(out.suffix + ".tmp")
            tmp.write_bytes(zf.read(info))
            tmp.replace(out)
            n += 1
    return n


def fetch_quarter(year: int, quarter: int, *, dest_dir: Path | None = None,
                  force: bool = True) -> pl.DataFrame:
    """抓某季財報 → **先封存 ZIP** → 解壓 HTML → parse → 回 cache 同構 DataFrame。

    僅支援 bulk ZIP 版型(year ≥ 2019),即 live 唯一前進路徑;< 2019 為凍結歷史,
    per-company 舊端點不移植——改由 `parse_quarter` 讀既有封存(見 module docstring)。

    - `force=True`(預設):不以資料夾存在為由跳過(修稽核「資料夾存在=抓完了」bug),
      總是重抓、覆寫封存、重解壓。
    - 原子順序:下載 → 封存 ZIP(archive 鐵律)→ 解壓 → parse。
    """
    if year < _BULK_ERA_YEAR:
        raise parse.SchemaDrift(
            f"fetch_quarter 僅支援 ≥ {_BULK_ERA_YEAR}(bulk ZIP);{year}Q{quarter} "
            f"為凍結歷史,請用 parse_quarter 讀既有封存 data/financial_statements/{year}_{quarter}/")
    dir_path = dest_dir if dest_dir is not None else _quarter_dir(year, quarter)
    data = http.fetch_bytes(_BULK_URL.format(year=year, quarter=quarter))
    if not _looks_like_zip(data):
        raise parse.SchemaDrift(
            f"{year}Q{quarter}: 下載內容非 ZIP(magic={data[:4]!r});端點回錯誤頁?")
    if dest_dir is None:
        _archive_zip(year, quarter, data)  # 封存原始 ZIP(僅真實封存路徑)
    _extract_html(data, dir_path)
    return parse_quarter(year, quarter, base_dir=dir_path)


# ─────────────────────────── 齊備閘門(修 BUG 1&2 根因) ───────────────────────────
#
# 根因(稽核 C-cf_progressive_raw):Scala 以「資料夾存在=抓完了」封季 + 月份閘門
# 猜早於申報期限,四類缺口:2026Q1(整季 22.8% 覆蓋)、2025Q2/2023Q2(金融+KY 缺 118~148 家)、
# 2024Q1(僅 13~17 家,金額大但家數在雜訊內)。
#
# 修法分兩層:
#   (1) 根因:`fetch_quarter(force=True)` 永不以資料夾存在封季,呼叫即重抓(idempotent)。
#   (2) 資料閘門 `needs_refetch`:用**可交易公司**的現金流量表覆蓋率 vs 前一年同季比較
#       (前一年同季抵銷掉 ETF/權證/興櫃 等結構性非申報者)。實測 24 個齊備季 YoY 覆蓋率
#       跌幅 ≤ 3.4pp,3 個實質缺口季 ≥ 5.6pp → 門檻取 4.5pp(中間,兩側各有餘裕);另設
#       絕對地板 50%(整季災難缺料,如 2026Q1 22.8%,無論有無基準都攔)。證據可重跑:
#       docs/data_audit 對照腳本 / research/crawl/tests(cache 內 daily_quote × cf 覆蓋率)。
#
# **2024Q1 這種 13 家的小缺口,任何資料閘門都與正常年度汰換(齊備季亦有 ≤67 家 YoY churn)
# 無法區分**;它的根因是「抓於一般 5/15 期限、早於金融/KY 較晚期限」,由 orchestrator 的
# **時序重抓策略**兜底(近幾季在其最晚申報期限過後才封季;fetch 為 idempotent,重抓無害)。
# 時序策略屬主流程,使用者接線;本模組提供資料閘門 + idempotent 重抓原語。

#: 可交易門檻:該季 daily_quote 有 ≥ N 個交易日才算「可交易、應申報」(濾掉權證/盤中下市
#: 雜訊)。對當季與前一年同季**對稱套用**,結構性差異相消。
_REFETCH_MIN_TRADING_DAYS = 20
#: YoY 可交易覆蓋率跌幅門檻(實測:齊備季 ≤ 3.4pp、實質缺口季 ≥ 5.6pp)。
_REFETCH_COVERAGE_DROP = 0.045
#: 絕對覆蓋率地板(整季災難缺料);低於此無論有無 YoY 基準都重抓。
_REFETCH_SEVERE_COVERAGE = 0.50


def _quarter_date_range(year: int, quarter: int) -> tuple[str, str]:
    start_month = (quarter - 1) * 3 + 1
    end = {1: (year, 3, 31), 2: (year, 6, 30),
           3: (year, 9, 30), 4: (year, 12, 31)}[quarter]
    return f"{year:04d}-{start_month:02d}-01", f"{end[0]:04d}-{end[1]:02d}-{end[2]:02d}"


def _traded_coverage(sink, year: int, quarter: int) -> tuple[int, int]:
    """回 (有現金流量表的可交易家數, 可交易家數)。可交易 = 該季 daily_quote
    交易日 ≥ _REFETCH_MIN_TRADING_DAYS。無可交易公司(如未來季)→ (0, 0)。"""
    start, end = _quarter_date_range(year, quarter)
    row = sink.con.execute(
        f"""
        WITH traded AS (
            SELECT company_code FROM daily_quote
            WHERE date BETWEEN ? AND ?
            GROUP BY company_code HAVING COUNT(*) >= ?
        )
        SELECT
            COUNT(*) FILTER (WHERE company_code IN
                (SELECT DISTINCT company_code FROM {TABLE}
                 WHERE year = ? AND quarter = ?)) AS covered,
            COUNT(*) AS traded
        FROM traded
        """,
        [start, end, _REFETCH_MIN_TRADING_DAYS, year, quarter]).fetchone()
    return (int(row[0]), int(row[1])) if row else (0, 0)


def needs_refetch(year: int, quarter: int, sink) -> bool:
    """資料齊備閘門(取代 Scala 的「資料夾存在=抓完了」)。

    以**可交易公司**的現金流量表覆蓋率判齊備,不塞猜出來的日曆天:
    - 覆蓋率 < 50%(整季災難缺料)→ True;
    - 相對前一年同季覆蓋率跌幅 > 4.5pp → True(前一年同季抵銷結構性非申報者)。

    orchestrator(update.py)呼叫此判準 + 時序策略決定是否 `fetch_quarter`;本移植
    不接線主流程。無可交易公司(未來季)→ False。小缺口(如 2024Q1 的 13 家)在資料
    雜訊內、本閘門不攔,靠時序重抓兜底(見上方段落)。
    """
    covered, traded = _traded_coverage(sink, year, quarter)
    if traded == 0:
        return False
    coverage = covered / traded
    if coverage < _REFETCH_SEVERE_COVERAGE:
        return True
    prior_covered, prior_traded = _traded_coverage(sink, year - 1, quarter)
    if prior_traded == 0:
        return False
    prior_coverage = prior_covered / prior_traded
    return (prior_coverage - coverage) > _REFETCH_COVERAGE_DROP


if __name__ == "__main__":
    import sys

    y, q = int(sys.argv[1]), int(sys.argv[2])
    df = parse_quarter(y, q)
    n_co = df.select(pl.col("company_code").n_unique()).item() if df.height else 0
    print(f"cash_flows {y}Q{q}: {df.height} 列 / {n_co} 家公司")
    print(df.head(8))
