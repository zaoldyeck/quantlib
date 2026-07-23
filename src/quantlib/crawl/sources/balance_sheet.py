"""balance_sheet 源:MOPS t163sb05 簡明資產負債表(afterIFRSs 合併,melt 長格式)。

移植自 `FinancialReader.readBalanceSheet`(src/main/scala/reader/FinancialReader.scala:63-112)。
稽核背書:docs/data_audit/_done/A-balance_sheet.json(verdict OK,解析零錯位/零漏欄/
正負號單位保真)、C-bs_concise_raw.json(cache↔PG 214 萬列逐位一致;唯一 BUG 屬
「下載完整性」不是 parser——見下方「完整性守護」)。

## 資料形狀(melt / 長格式,非寬表)

MOPS 一期回**多張表**(依產業模板:一般業/金融業/證券業/保險業/DR…),Scala 爬蟲
把每張表另存成 `data/balance_sheet/<market>/<year>/<year>_<quarter>_<a|b>_<i|c>_<idx>.csv`
(Big5-HKSCS)。每檔有共同的 metadata 欄(出表日期/年度/季別/公司代號/公司名稱)+
一組隨模板而異的**數值科目欄**。reader 的核心是 **melt**:每個「可轉 double 的數值
儲格」→ 一列 `(market, type, year, quarter, company_code, company_name, title, value)`。
非數值儲格(`--` 不適用、DR 檔的「換算匯率」等文字欄)一律**丟棄**(不寫 0)。

cache 表 `bs_concise_raw` 只投影 7 欄(丟 company_name,只留 consolidated;
research/cache_tables.py:96-99)。fetch 回傳即與此 7 欄同構。

## 忠實移植的關鍵決策(對齊 Scala,經稽核逐位驗證)

- **title 只去空白**(`k.replace(" ","")`),**value 去空白+千分位逗號**再轉 double。
- **company_code / company_name 取原樣**(Scala `values("公司代號")` 不 strip)——
  name-strip 只該清數值欄,代號/名稱保真(稽核 name 健全性通過)。
- **type 取自檔名第 4 token**(i→individual、c→consolidated),year/quarter 取自檔名
  regex,**不看內容 出表日期/年度/季別**(稽核:465 個 a_c 檔檔名 vs 內容日期零不符;
  b_i/b_c 檔根本無內容日期欄)。
- **`--`/文字欄以「能否轉 double」過濾**(Scala `Try(_.toDouble).isSuccess`)——1216
  的 4 個 `--` 欄不落地、4 個 `0.00` 股數欄落成 0.0(稽核逐位確認)。
- **值為 Float64**:資產負債表無 int 溢位風險(全 double),不涉「int32→Int64」修法。

## header 位置守衛(SchemaDrift fail-loud,比照 daily_quote._guard)

melt 以「欄名」定位,故守衛檢查**結構前提**而非固定 index:公司代號/公司名稱 必在
表頭、**表頭無重名**(重名會讓 melt 靜默吃掉科目);a_c(現行 live 格式)另強制前
5 欄恰為 metadata、順序不變——MOPS 悄悄改欄立即 fail-loud,絕不靜默錯位。

## 完整性守護(修掉稽核 C 的唯一 BUG,不複製)

稽核 C 抓到:Scala `Task.pullQuarterlyFiles` 的視窗**早於法定申報截止日**就開
(Q1 於 5/1 開、截止 5/15),又以「(年,季)有任一檔就永不重抓」去重 → 2026Q1 在
5/10 抓到只有 28% 公司的**部分檔並永久凍結**;同根因造成 13 個歷史季永久缺 14~111 家。
本 port 從結構上消除此 BUG:
1. `fetch_quarter` **無狀態**——每次呼叫都重抓、絕不「有檔就跳過」,結構上凍結不了。
2. `window_open_date` 把開窗挪到**法定截止日 + 緩衝**(Q1 6/1、Q2 9/1、Q3 12/1、
   Q4 次年 5/1;金融業半年報更晚,緩衝已含),`refresh` 只抓已開窗的季,**不會**在
   截止日前撈到部分檔。
3. `refresh` 對「開窗後一段時間內仍可能長大」的近季**滾動重抓**(upsert=刪該季+重插),
   晚申報公司於下次 run 自動補齊、部分檔自癒。

Run(整合由主流程接線,見 update.py):
    from quantlib.crawl.sources import balance_sheet
    balance_sheet.refresh(sink, upto)
parity(離線 raw→PG,無網路):
    uv run --project . python -m quantlib.crawl.tests.test_balance_sheet_parity
"""
from __future__ import annotations

import csv
import html as _html
import io
import re
import time
from datetime import date as Date
from html.parser import HTMLParser
from pathlib import Path

import polars as pl

from quantlib.crawl import archive, http, parse
from quantlib.crawl.sink import Sink

TABLE = "bs_concise_raw"
#: 批次 upsert 粒度 = 一季一模板一市場(刪該季 + 重插,冪等且可自癒部分檔)
KEY_COLS = ["market", "type", "year", "quarter"]
MARKETS = ("twse", "tpex")
SOURCE = "balance_sheet"

# ── MOPS 端點(application.conf data.balanceSheet)──────────────────────────
#: step1:ajax 頁,回一頁含 N 個 <input name=filename>(每產業模板一個)
_AJAX_AFTER_IFRS = "https://mopsov.twse.com.tw/mops/web/ajax_t163sb05"
#: step2:憑 filename 下載該模板的 CSV
_FILE_DOWNLOAD = "https://mopsov.twse.com.tw/server-java/t105sb02"
_TYPEK = {"twse": "sii", "tpex": "otc"}
#: 有效表頭標記(下載回來若無此字串 = 失敗下載/錯誤頁,不封存、交由重抓)
_HEADER_MARKER = "公司代號"
#: MOPS 反爬節流(對齊 Scala Crawler.getBalanceSheet 的 20s/10s;load-bearing——
#: 少了會被 MOPS 擋)。測試/一次性可覆寫為 0。
_TABLE_SLEEP = 10.0    # step2 每張表下載前
_QUARTER_SLEEP = 20.0  # refresh 每 (市場,季) 前

#: 檔名 regex(對齊 Scala `(\d+)_(\d+)_(\w)_(\w).*`):year_quarter_template_type_idx
_FILENAME_RE = re.compile(r"(\d+)_(\d+)_(\w)_(\w).*")
_TYPE = {"i": "individual", "c": "consolidated"}

#: melt 要丟棄的 metadata 欄(以**原始表頭名**比對,對齊 Scala filterNot)
_META_COLS = {"公司代號", "公司名稱", "出表日期", "年度", "季別"}
#: a_c(現行 live 格式)前 5 欄固定順序——守衛用
_AC_META_HEAD = ["出表日期", "年度", "季別", "公司代號", "公司名稱"]

#: fetch 回傳(與 cache bs_concise_raw 同構,7 欄,無 company_name)
_CACHE_SCHEMA = {
    "market": pl.Utf8, "type": pl.Utf8, "year": pl.Int32, "quarter": pl.Int32,
    "company_code": pl.Utf8, "title": pl.Utf8, "value": pl.Float64,
}
#: 忠實 melt(含 company_name,供 parity 對 PG concise_balance_sheet 全欄比對)
_FULL_SCHEMA = {
    "market": pl.Utf8, "type": pl.Utf8, "year": pl.Int32, "quarter": pl.Int32,
    "company_code": pl.Utf8, "company_name": pl.Utf8, "title": pl.Utf8,
    "value": pl.Float64,
}


# ── 值轉換 ──────────────────────────────────────────────────────────────────
def _to_double(v: str) -> float | None:
    """對齊 Scala `Try(v.toDouble)`(= Java Double.parseDouble)。

    Python `float()` 額外接受底線分隔("1_000")而 Java 不接受——擋掉這唯一實質差異;
    NaN/Infinity 字面在 TWSE 財報儲格不會出現(稽核:0 NaN/0 Inf)。回 None = 非數值
    (`--`、DR 文字欄、空字串),對齊 Scala 的 `.filter(Try(...).isSuccess)` 丟棄語義。
    """
    if "_" in v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


# ── header 守衛(fail-loud)──────────────────────────────────────────────────
def _guard_header(header: list[str], template: str, what: str) -> None:
    """melt 結構前提守衛(**名稱**定位,故查「欄名是否在」而非固定 index)。

    - 表頭無重名:重名會讓 melt 靜默吃掉同名科目。
    - 公司代號/公司名稱 必在:否則定位不到 code/name。
    - a_c 另須 出表日期/年度/季別 三欄都在:它們靠**欄名**被丟棄(_META_COLS),
      一旦 MOPS 改名,年度="115"/季別="1" 會被當數值科目 melt 進去污染(Scala 同病)。
    順序不查:melt 與欄序無關,harmless 重排不該 fail(正常路徑必過)。
    """
    if len(header) != len(set(header)):
        dup = sorted({h for h in header if header.count(h) > 1})
        raise parse.SchemaDrift(
            f"{SOURCE} {what} 表頭重名 {dup}——melt 會靜默吃掉同名科目(MOPS 改格式?)")
    need = list(_AC_META_HEAD) if template == "a" else ["公司代號", "公司名稱"]
    missing = [c for c in need if c not in header]
    if missing:
        raise parse.SchemaDrift(
            f"{SOURCE} {what} 表頭缺 metadata 欄 {missing}"
            "(melt 定位不到代號/名稱,或該欄會被當數值科目污染;MOPS 改格式?)")


def _guard_content_date(header: list[str], rows: list[list[str]], year: int,
                        quarter: int, what: str) -> None:
    """a_c 內容帶 年度(民國)/季別 → 交叉驗證檔名日期(Scala 只信檔名,這裡加固)。

    防 MOPS silent-fallback:被要求 X 季卻回 Y 季的資料、仍以 X 季檔名落地
    (daily_quote 的 2018-02-18 回退即此類,見 CLAUDE.md「Known Bug Patterns」)。
    稽核 A 實測 465 個 a_c 檔零不符,故正常路徑必過;首筆有效列即代表全檔同季。
    """
    for row in rows:
        rec = dict(zip(header, row))
        code, cy, cq = rec.get("公司代號"), rec.get("年度", "").strip(), rec.get("季別", "").strip()
        if not code or not cy or not cq:
            continue
        if not (cy.lstrip("-").isdigit() and cq.isdigit()):
            return  # 非數字日期(不預期)——不硬擋,交由值 melt
        if int(cy) + 1911 != year or int(cq) != quarter:
            raise parse.SchemaDrift(
                f"{SOURCE} {what} 內容日期 {cy}年(民國)Q{cq} ≠ 檔名 {year}Q{quarter}"
                "——MOPS 回錯季(silent-fallback)?")
        return


# ── melt 核心(忠實移植 readBalanceSheet)────────────────────────────────────
def _melt(header: list[str], rows: list[list[str]], market: str, typ: str,
          year: int, quarter: int, template: str, what: str) -> list[dict]:
    # 無資料列 → 0 列,**不**觸發守衛:對齊 Scala allWithHeaders 對「只有表頭/失敗
    # 下載殘骸」(如 2025_3_a_c_3.csv = 'Unreachable Server')melt 出 0 列、從不存取
    # 公司代號 的寬容行為。有資料列時才守衛(有列卻定位不到代號才是真 drift/會炸)。
    if not any(r for r in rows):
        return []
    _guard_header(header, template, what)
    if template == "a":
        _guard_content_date(header, rows, year, quarter, what)
    recs: list[dict] = []
    for row in rows:
        # tototoshi allWithHeaders 語義:header zip row(短列缺尾鍵、長列丟多值)
        rec = dict(zip(header, row))
        code = rec.get("公司代號")
        name = rec.get("公司名稱")
        if not code:  # 空/尾列無代號(稽核:真實資料列必有代號)——跳過不污染
            continue
        for k, v in rec.items():
            if k in _META_COLS:
                continue
            val = _to_double(v.replace(" ", "").replace(",", ""))
            if val is None:  # `--` / 文字欄 → 丟棄(不寫 0)
                continue
            recs.append({
                "market": market, "type": typ, "year": year, "quarter": quarter,
                "company_code": code, "company_name": name,
                "title": k.replace(" ", ""), "value": val,
            })
    return recs


def _parse_bytes(raw: bytes, market: str, typ: str, year: int, quarter: int,
                 template: str, what: str) -> list[dict]:
    text = raw.decode("Big5-HKSCS", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return []
    return _melt(rows[0], rows[1:], market, typ, year, quarter, template, what)


def parse_file(path: str | Path, market: str) -> pl.DataFrame:
    """讀一個封存的原始檔 → 忠實 melt DF(_FULL_SCHEMA,含 company_name)。

    type/year/quarter 由**檔名**推得(對齊 Scala,不看內容日期)。供 parity 對 PG 全欄
    比對;fetch 走 fetch_quarter(回 cache 7 欄)。
    """
    path = Path(path)
    m = _FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"{SOURCE}: 檔名不符 year_quarter_t_t 格式:{path.name}")
    year, quarter, template, tcode = int(m.group(1)), int(m.group(2)), m.group(3), m.group(4)
    typ = _TYPE.get(tcode)
    if typ is None:
        raise ValueError(f"{SOURCE}: 未知 type token '{tcode}'(檔名 {path.name})")
    recs = _parse_bytes(path.read_bytes(), market, typ, year, quarter, template,
                        what=path.name)
    return pl.DataFrame(recs, schema=_FULL_SCHEMA)


# ── 抓取(live:僅 a_c afterIFRSs,對齊 BalanceSheetSetting 現行期別分支)───────
class _FilenameCollector(HTMLParser):
    """抓 step1 頁面所有 <input name=filename value=...> 的 value(比照 Jsoup)。"""

    def __init__(self) -> None:
        super().__init__()
        self.filenames: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "input":
            return
        a = dict(attrs)
        if a.get("name") == "filename" and a.get("value"):
            self.filenames.append(_html.unescape(a["value"]))  # Jsoup 會解 entity


def _extract_filenames(html: str) -> list[str]:
    """distinct + sorted(對齊 Scala `.toSeq.distinct.sorted`)。"""
    c = _FilenameCollector()
    c.feed(html)
    return sorted(set(c.filenames))


def fetch_quarter(market: str, year: int, quarter: int) -> pl.DataFrame | None:
    """抓某市場某季 a_c 簡明資產負債表(2-step MOPS)→ **逐檔原樣封存** → melt。

    無狀態:每次呼叫都重抓(不看本地是否已有檔),故永不凍結部分季(修稽核 C 的
    去重凍結 BUG)。回 cache 同構 7 欄 DF;該季無任何模板檔 → None。
    """
    # step1:ajax 頁 → 各產業模板的 filename
    form1 = {
        "encodeURIComponent": "1", "step": "1", "firstin": "1", "off": "1",
        "isQuery": "Y", "TYPEK": _TYPEK[market],
        "year": str(year - 1911), "season": f"0{quarter}",
    }
    html = http.fetch_text(_AJAX_AFTER_IFRS, encoding="Big5-HKSCS", form=form1)
    filenames = _extract_filenames(html)
    if not filenames:
        return None
    # step2:逐 filename 下載 CSV → 驗證 → **原樣封存** → melt
    recs: list[dict] = []
    for index, filename in enumerate(filenames):
        time.sleep(_TABLE_SLEEP)
        form2 = {"firstin": "true", "step": "10", "filename": filename}
        raw = http.fetch_bytes(_FILE_DOWNLOAD, form=form2)
        # 失敗下載(如 'Unreachable Server')不封存、直接拋——避免把錯誤字串存成
        # 有效表並凍結(稽核 C 的 2025Q3 template-loss 即此類);無狀態重抓會補回。
        if _HEADER_MARKER not in raw.decode("Big5-HKSCS", errors="replace"):
            raise RuntimeError(
                f"{SOURCE}/{market} {year}Q{quarter} 表 {index} 下載無效"
                f"(無 '{_HEADER_MARKER}' 表頭)——交由重抓,不封存殘骸")
        archive.save_raw_named(SOURCE, market, year,
                               f"{year}_{quarter}_a_c_{index}.csv", raw)
        recs.extend(_parse_bytes(raw, market, "consolidated", year, quarter,
                                 template="a", what=f"{year}_{quarter}_a_c_{index}"))
    if not recs:
        return None
    return (pl.DataFrame(recs, schema=_FULL_SCHEMA)
            .unique(subset=["company_code", "title"], keep="first", maintain_order=True)
            .select(list(_CACHE_SCHEMA)))


# ── 完整性視窗(修稽核 C 的早開窗 BUG)───────────────────────────────────────
#: 各季**開窗日**(法定申報截止日 + 緩衝;稽核 C 建議值)。key=quarter。
#: Q4 為次年;金融業半年報晚於一般業,Q2/Q3 緩衝已含。
def window_open_date(year: int, quarter: int) -> Date:
    """該季可安全開抓的最早日(法定截止 + 緩衝),早於此日抓到的必是部分檔。"""
    return {
        1: Date(year, 6, 1),       # 截止 5/15
        2: Date(year, 9, 1),       # 截止 8/14(金融半年報更晚)
        3: Date(year, 12, 1),      # 截止 11/14
        4: Date(year + 1, 5, 1),   # 截止次年 3/31
    }[quarter]


def _recent_open_quarters(upto: Date, back: int) -> list[tuple[int, int]]:
    """upto 當下**已開窗**且最近的 `back` 個季(新→舊)。"""
    y, q = upto.year, (upto.month - 1) // 3 + 1
    out: list[tuple[int, int]] = []
    while len(out) < back:
        if window_open_date(y, q) <= upto:
            out.append((y, q))
        q -= 1
        if q == 0:
            y, q = y - 1, 4
        if y < 2013:  # a_c 始於 2013Q1
            break
    return out


def refresh(sink: Sink, upto: Date, back: int = 6) -> int:
    """滾動重抓最近 `back` 個**已開窗**季(idempotent upsert)。回新增列數。

    只抓已過「法定截止 + 緩衝」的季 → 不會撈到部分檔;滾動重抓 → 晚申報公司自癒。
    """
    total = 0
    for market in MARKETS:
        for year, quarter in _recent_open_quarters(upto, back):
            time.sleep(_QUARTER_SLEEP)  # MOPS 反爬節流(對齊 Scala 每 detail 20s)
            try:
                df = fetch_quarter(market, year, quarter)
            except Exception as exc:  # noqa: BLE001 - 單季失敗不擋其餘
                print(f"[crawl] balance_sheet/{market} {year}Q{quarter} 抓取失敗:{exc}")
                continue
            if df is None:
                continue
            n = sink.upsert(TABLE, df, KEY_COLS)
            total += n
            print(f"[crawl] balance_sheet/{market} {year}Q{quarter}: {n} 列")
    return total
