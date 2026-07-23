"""insider_holding 源:內部人持股轉讓「事前申報」日報 — MOPS t56sb12_q1(上市)/ q2(上櫃)。

內部人(董監事 / 經理人 / 持股 ≥ 10% 大股東)申報「即將轉讓」持股的日報。每一列 =
一筆事前申報;`transfer_method ∈ (一般交易 / 鉅額逐筆交易 / 盤後定價交易 / 贈與 /
信託 / 洽特定人 / 轉讓私募股票 …)`。「一般交易 / 鉅額逐筆」是真賣壓訊號
(TW 學術:forward 5-30d -2~-5% CAR);「信託 / 贈與」是所有權重配、`transfer_shares=0`。

移植自 `reader/TradingReader.scala::readInsiderHolding`(1090-1151)+ `parseMopsHtml`
(972-989)+ `cleanCell`(40-42)+ `setting/InsiderHoldingSetting.scala`。**忠實**保留:
欄位對位([1..13] → 15 欄)、值轉換、市場分流(twse=SY / tpex=OY)、以 cols[2] 股號
守門過濾資料列、六鍵去重(keep first)、`cols.size < 14` 過濾。

## 端點(2-step ajax,verified InsiderHoldingSetting.scala:8-91;2026-07-23 實測近期日可直取)

    step1  POST /mops/web/ajax_t56sb12_q1(twse)| _q2(tpex)
           encodeURIComponent=1&step=0&firstin=1&off=1&year=民國&month=MM&day=DD  (中繼頁)
    step2  POST /mops/web/ajax_t56sb12
           encodeURIComponent=1&run=&step=2&year=民國&month=MM&day=DD&report=SY|OY&firstin=true
           → 真正的資料 HTML 表格(report=SY 上市 / OY 上櫃)

CLAUDE.md 記載的「IP/TLS 指紋牆需 Playwright」屬**大量歷史回補**情境;近期單日 2-step
以 urllib 實測可取(step2 回正常 table)。深度回補(2007-01-10 ~ 2026-03-30 的 19 年
空窗,見稽核 C REAL#2)仍可能受限,非本 port 範圍。檔名 `YYYY_M_D.html`(月/日不補零,
對齊 Scala `getMonthValue`/`getDayOfMonth`);`report_date` = 檔名日(=發布日,此端點
就是「當日新增事前申報」),`declare_date` = 內文 cols[1] 申報日期(此端點恆 = report_date)。

## 稽核發現、本 port 一次寫對的 bug(docs/data_audit/_done/C-insider_holding.json)

1. **transfer_shares 雙方式黏接(BUG,根因修復)**。同一張申報同時含兩種轉讓方式時,
   cols[7] 是兩個以 `<br>` 分隔的股數(jsoup `.text` 併成 `"656,000 6,360,000"`),
   Scala `cleanCell` 的 `.replace(" ","")` 把兩數**字元黏接**成天文數字
   (656000+6360000 → `6560006360000`)。全表 3 筆(2856/2026-06 6994/2026-05 8284)
   PG 現存壞值(MAX transfer_shares = 57,000,001,000,000)。**修法**:數值格以空白拆
   token、逐一 parse、**加總**(非字元黏接)。**逐位鐵證**:三筆的 token 和恰好 =
   同列 `planned_shares_own`(cols[12] 預定轉讓總股數-自有):7,016,000 / 6,700,000 /
   253,580 — 來源自己的「總股數」欄就是這個和,證明加總是正解。舉一反三:此「多 token
   數值格」通用守護(`_parse_shares`)套用**全部 6 個數值欄**,杜絕同類黏接復發。

2. **int32 溢位改 Int64**。PG 欄型 bigint;黏接壞值(57 兆)遠超 int32,且大型股
   (鴻海 ~130 億股)持股欄可 > 2^31。六個股數欄一律 `pl.Int64`;Python int 解析無溢位。

3. **name-strip 只清數值欄**。`reporter_name` / `transferee` / `company_name` /
   `reporter_title` / `transfer_method` 只做空白正規化(jsoup 語義),**不**做 cleanCell
   的去逗號/去空白 → 保留 `First Steamship S.A` 等含空白外文名的內部空白。

4. **版型用明確表頭位置守衛**(`parse.SchemaDrift` fail-loud,比照 daily_quote._guard),
   不用 fallthrough。兩列表頭(colspan 展開),守衛 top[1..12] + sub[0..1] 的 13+2 個
   關鍵字位置;有資料列卻無表頭即 fail-loud(TWSE/MOPS 悄悄改欄地雷)。

5. **jsoup `.text()` 空白語義精確重現**(否則字串欄無法逐位 parity)。儲存格被
   non-breaking space(U+00A0)包裹(如 `\xa04958\xa0`);jsoup `isActuallyWhitespace`
   視 U+00A0 為空白(collapse + trim),但**不**視全形空白 U+3000 為空白(保留)。
   Scala `.trim` 亦不去 U+3000(> 0x20)。故正規化集 = `[ \t\n\r\f ]`,U+3000 保留
   (受讓人 `陳耀倫　`、外文名 `First　Steamship` 都靠這點才與 PG 逐位相同)。

## 欄位/型別:回傳 DF 與 cache 表 insider_holding 同構(15 欄,cache 投影掉 PG 的 id)

`parse_raw` / `fetch_day` 皆回這 15 欄;`fetch_day` 走 archive→parse、不讀 cache;
parity 測試讀 PG 當對照。cache 同步 SQL:research/cache_tables.py:70;Slick 真源:
src/main/scala/db/table/InsiderHolding.scala(16 欄,去重唯一索引見下 KEY 註)。

依賴 `research/cache_tables.py`?否(本模組是爬蟲寫入側;parity 測試才讀 cache/PG)。
"""
from __future__ import annotations

import re
from datetime import date as Date

import polars as pl
from bs4 import BeautifulSoup

from quantlib.crawl import archive, http, parse

TABLE = "insider_holding"
#: cache 增量 upsert 的批次鍵(刪整日 + 插入;此表用 report_date 非 date)。
KEY_COLS = ["market", "report_date"]
#: 日頻機制(update._missing_days / sink.upsert_day)的日期欄——此表無 date 欄,用 report_date。
#: 缺此宣告會讓 `SELECT max(date)` 炸、insider 日頻刷新靜默壞掉(2026-07-24 修根因)。
DATE_COL = "report_date"
MARKETS = ("twse", "tpex")

#: 2-step ajax(InsiderHoldingSetting.scala:39-44, 80)。
_STEP1_URL = {
    "twse": "https://mopsov.twse.com.tw/mops/web/ajax_t56sb12_q1",
    "tpex": "https://mopsov.twse.com.tw/mops/web/ajax_t56sb12_q2",
}
_STEP2_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t56sb12"
_REPORT = {"twse": "SY", "tpex": "OY"}  # step2 report= (上市 / 上櫃)

#: 資料列股號正規(對齊 Scala `stockCodeRegex = [0-9][0-9A-Z]{3,}` + `.matches()` 全串)。
_STOCK = re.compile(r"[0-9][0-9A-Z]{3,}")

#: jsoup `StringUtil.isActuallyWhitespace` 空白集 = ASCII 空白 + U+00A0(nbsp);
#: **不含** U+3000(全形空白,jsoup 與 Scala .trim 皆保留)。見 docstring #5。
_WS = re.compile("[ \t\n\r\f\u00a0]+")
_STRIP = " \t\n\r\f\u00a0"
#: 表頭關鍵字比對用:去掉所有空白(含全形)再做子字串比對。
_ANY_WS = re.compile("[ \t\n\r\f\u00a0\u3000]")

#: DF / cache schema(15 欄,順序 = Slick `*` 去 id)。六股數欄 Int64(int32 溢位修正)。
_SCHEMA = {
    "market": pl.Utf8, "report_date": pl.Date, "declare_date": pl.Date,
    "company_code": pl.Utf8, "company_name": pl.Utf8,
    "reporter_title": pl.Utf8, "reporter_name": pl.Utf8,
    "transfer_method": pl.Utf8, "transferee": pl.Utf8,
    "transfer_shares": pl.Int64, "max_intraday_shares": pl.Int64,
    "current_shares_own": pl.Int64, "current_shares_trust": pl.Int64,
    "planned_shares_own": pl.Int64, "planned_shares_trust": pl.Int64,
}
CACHE_COLS = list(_SCHEMA)

# 表頭位置守衛(index → 關鍵字子字串,去空白後比對)。兩列 colspan 表頭:
#   top:  異動情形 申報日期 公司代號 公司名稱 申報人身分 姓名 預定轉讓方式及股數
#         每日於盤中交易最大得轉讓股數 受讓人 目前持有股數 預定轉讓總股數 預定轉讓後持股 …
#   sub(colspan 展開): 轉讓方式 轉讓股數 | 自有持股 信託 | 自有持股 信託 | 自有持股 信託
_TOP_GUARD = {1: "申報日期", 2: "公司代號", 3: "公司名稱", 4: "申報人身分",
              5: "姓名", 6: "預定轉讓方式及股數", 7: "最大得轉讓股數", 8: "受讓人",
              9: "目前持有股數", 10: "預定轉讓總股數", 11: "預定轉讓後持股",
              12: "有效轉讓期間"}
_SUB_GUARD = {0: "轉讓方式", 1: "轉讓股數"}


def _decode(raw: bytes) -> str:
    """MOPS 混編碼嗅探(對齊 TradingReader.parseMopsHtml:977-981):
    先試 UTF-8,含替換字元 U+FFFD → 退回 Big5-HKSCS。t56sb12(內部人)實為 UTF-8。"""
    try:
        s = raw.decode("utf-8")
        if "�" in s:
            raise ValueError
        return s
    except (UnicodeDecodeError, ValueError):
        return raw.decode("big5-hkscs", errors="replace")


def _norm(el) -> str:
    """儲存格文字 = jsoup `.text()` 語義:`<br>`/塊界→空白、collapse ASCII+nbsp 空白、
    trim(不去 U+3000)。`separator=" "` 讓 `<br>` 分隔的多值不黏死(雙方式股數靠此拆得開)。"""
    return _WS.sub(" ", el.get_text(separator=" ")).strip(_STRIP)


def _rmws(s: str) -> str:
    return _ANY_WS.sub("", s)


def _parse_shares(cell: str) -> int:
    """數值格 → int。**根因修法**:格內多個空白分隔的數字 token 逐一 parse 後**加總**
    (雙方式申報的兩股數 = planned_shares_own),而非 Scala cleanCell 的字元黏接。
    空/非數值 → 0(對齊 Scala `parseLong` 的 `Try{...}.getOrElse(0L)`)。"""
    total = 0
    seen_num = False
    for tok in cell.split(" "):
        c = tok.strip(_STRIP).replace(",", "").replace("%", "")
        if not c:
            continue
        try:
            total += int(c)
            seen_num = True
        except ValueError:
            pass  # 非數值 token 忽略(空欄→0)
    return total if seen_num else 0


def _minguo(cell: str, fallback: Date) -> Date:
    """民國 `yyy/MM/dd` → 西元;不可解析 → fallback(對齊 Scala getOrElse(reportDate))。"""
    return parse.parse_minguo_slash(cell.strip(_STRIP)) or fallback


def _check(header: list[str], guard: dict[int, str], what: str) -> None:
    cells = [_rmws(c) for c in header]
    for i, kw in guard.items():
        got = cells[i] if i < len(cells) else "<缺>"
        if kw not in got:
            raise parse.SchemaDrift(
                f"insider_holding {what}位移:col[{i}] 期望含 '{kw}' 實得 '{got}'"
                f"(MOPS 改格式?)")


def _guard(th_rows: list[list[str]], has_data: bool) -> None:
    """表頭位置守衛 fail-loud。無資料日(MOPS『查無資料』頁,無 `<th>`)→ 放行(0 列)。
    有資料列卻找不到表頭 → 位移/漂移,fail-loud。"""
    top = next((r for r in th_rows if any("申報日期" in _rmws(c) for c in r)), None)
    if top is None:
        if has_data:
            raise parse.SchemaDrift(
                "insider_holding 有資料列卻找不到『申報日期』表頭(MOPS 改格式?)")
        return
    _check(top, _TOP_GUARD, "表頭")
    sub = next((r for r in th_rows if r and _rmws(r[0]) == "轉讓方式"), None)
    if sub is None:
        raise parse.SchemaDrift("insider_holding 缺子表頭『轉讓方式/轉讓股數』(MOPS 改格式?)")
    _check(sub, _SUB_GUARD, "子表頭")


def parse_raw(market: str, raw: bytes, report_date: Date) -> pl.DataFrame:
    """封存原始檔 bytes → 15 欄 cache-schema DF(空 = 該日無申報)。

    忠實移植 readInsiderHolding:`<table tr>` 取 `<td>` 文字(jsoup 語義)→ len≥14 且
    cols[2] 為股號的資料列 → 六鍵去重(keep first)。`_guard` 先驗表頭位置。
    """
    if len(raw) < 1024:  # 對齊 parseMopsHtml:< 1024 bytes 視為無資料(哨兵/空頁)
        return pl.DataFrame([], schema=_SCHEMA)
    soup = BeautifulSoup(_decode(raw), "html.parser")
    th_rows: list[list[str]] = []
    td_rows: list[list[str]] = []
    for tr in soup.select("table tr"):
        ths = [_norm(t) for t in tr.find_all("th")]
        tds = [_norm(t) for t in tr.find_all("td")]
        if ths:
            th_rows.append(ths)
        if tds:
            td_rows.append(tds)

    data = [r for r in td_rows if len(r) >= 14 and _STOCK.fullmatch(r[2])]
    _guard(th_rows, bool(data))

    seen: set[tuple[str, ...]] = set()
    recs: list[dict] = []
    for r in data:
        # reporter_name / transferee: Scala 額外 .replace("\n","")(正規化後已無 \n,存真)。
        rname = r[5].replace("\n", "")
        transferee = r[9].replace("\n", "")
        # 去重唯一鍵(對齊 Slick idx + Scala distinctBy):code(cols[2]) / reporter_name
        # (cols[5]) / transfer_method(cols[6]) / transferee(cols[9]);keep first。
        dkey = (r[2], rname, r[6], transferee)
        if dkey in seen:
            continue
        seen.add(dkey)
        recs.append({
            "market": market,
            "report_date": report_date,
            "declare_date": _minguo(r[1], report_date),
            "company_code": r[2],
            "company_name": r[3],
            "reporter_title": r[4],
            "reporter_name": rname,
            "transfer_method": r[6],
            "transferee": transferee,
            "transfer_shares": _parse_shares(r[7]),        # BUG#1 修:多 token 加總
            "max_intraday_shares": _parse_shares(r[8]),
            "current_shares_own": _parse_shares(r[10]),
            "current_shares_trust": _parse_shares(r[11]),
            "planned_shares_own": _parse_shares(r[12]),
            "planned_shares_trust": _parse_shares(r[13]),
        })
    return pl.DataFrame(recs, schema=_SCHEMA)


def fetch_day(market: str, day: Date) -> pl.DataFrame | None:
    """抓當日內部人事前申報 → **先原樣封存原始檔到 data/** → parse → 回 15 欄 DF。

    原始檔封存鐵律:`archive.save_raw` 一定在 parse 之前(位元保真,先落地才解析)。
    2-step ajax:step1 中繼頁(結果丟棄,建立端點狀態)→ step2 取資料 HTML。
    該日無申報(空表)→ None(交由呼叫端決定是否寫哨兵)。
    """
    if market not in _STEP1_URL:
        raise ValueError(f"未知 market:{market}")
    yy = day.year - 1911
    mmdd = {"year": str(yy), "month": f"{day.month:02d}", "day": f"{day.day:02d}"}
    # step1:中繼頁(InsiderHoldingSetting.formData:step=0/firstin=1/off=1)。
    http.fetch_bytes(_STEP1_URL[market], form={
        "encodeURIComponent": "1", "step": "0", "firstin": "1", "off": "1", **mmdd})
    # step2:真資料(step2FormData:step=2/report=SY|OY/firstin=true)。
    raw = http.fetch_bytes(_STEP2_URL, form={
        "encodeURIComponent": "1", "run": "", "step": "2",
        "report": _REPORT[market], "firstin": "true", **mmdd})
    archive.save_raw(TABLE, market, day, raw, ext="html")  # 原樣 bytes,先落地再 parse
    df = parse_raw(market, raw, day)
    return df if not df.is_empty() else None
