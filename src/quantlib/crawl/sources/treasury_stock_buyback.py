"""treasury_stock_buyback 源:庫藏股執行情形(公司宣告買回自家股票)— MOPS t35sc09。

公司依證交法 §28-2 宣告買回自家股票的執行情形。買回宣告當日通常 +3~5%
(Vermaelen 1981 / TW 學術),與 SBL 借券餘額同時擴大時可能形成 squeeze setup。
每一列 = 一家公司一次買回宣告(以〈市場, 董事會決議日, 公司代號〉去重)。

移植自 `reader/TradingReader.scala::readTreasuryStockBuyback`(1016-1059)+ `parseMopsHtml`
(972-989)+ `cleanCell`(40-41)+ `setting/TreasuryStockBuybackSetting.scala` +
`MopsDirectDetail.scala`。**忠實**保留:欄位對位、值轉換(cleanCell 去逗號/%/空白 →
parseLong/parseDouble、fallback 0)、市場分流(twse=sii / tpex=otc,市場來自封存目錄名)、
以 cols[1] 股號守門過濾資料列、三鍵去重(keep first,文件序)。

## 端點(verified TreasuryStockBuybackSetting.scala + application.conf:278)

    POST https://mopsov.twse.com.tw/mops/web/ajax_t35sc09
    step=1&firstin=1&TYPEK=sii|otc&yearb=民國&monthb=MM&yeare=民國&monthe=MM

**這是全史快照端點**:不論送哪個年月,回傳的都是 2000~今全部買回宣告的**同一張 4.4MB
表**(實測 data/…/2026_7.html 含 twse 4247 列、2000-2026;月參數不篩選)。故每月跑一次
刷新即可,封存檔名沿用 Scala 慣例 `{year}_{month}.html`(非日頻,用 `save_raw_named`)。
Big5-HKSCS/UTF-8 混編碼(現行快照為乾淨 UTF-8,`_decode` 先試 UTF-8 再退 Big5)。

## 稽核發現、本 port 一次寫對的 4 個 bug(docs/data_audit/_done/C-treasury_stock_buyback.json)

Scala reader 第 1006-1012 行自附的「18 欄」對照表**是錯的**:實際資料列 20 格
(表頭 18 邏輯欄,其中「買回價格區間」「預定買回期間」各 colspan=2 → +2 格)。
Scala 用錯位索引 → 兩整欄裝錯值。逐欄實證(台泥 1101 2019-05-10,見 tests):

1. **BUG#1 2000-2010 整 11 年被丟光(根因修復,最嚴重)**。Scala `minguoSlashFormatter`
   用 pattern `"yyy/MM/dd"`;Java `DateTimeFormatter` 的 `yyy`(3 個 y)**最少吃 3 位數**,
   民國 89-99(2000-2010)只有 2 位 → parse 拋例外 → `getOrElse(throw).toOption` 吞成
   None → 整列 flatMap 丟棄。PG 因此 min(announce_date)=2011-01-05,砍掉約 2,760 列
   (twse 1,879 + tpex 881)、幾乎半張表。**修法**:`parse.parse_minguo_slash` 用
   `int(y)+1911` 吃任意位數 → 2 位民國年正常解析。原始快照本就含 2000-2010,無需重抓。

2. **BUG#2 pct_of_capital 裝錯欄(根因修復)**。真「占已發行股份總數比例(%)」在
   **cols[18]**(法定上限 10%);Scala 讀 cols[16]=「本次已買回**總金額**」(億元級)。
   PG 2743/2933 列 pct>10(不可能),max 85 億。台泥 2019-05-10:PG 存 348,959,120
   (=已買回總金額),真值 0.15。**修法**:cols[16]→cols[18]。

3. **BUG#3 executed_shares 整欄全 0(根因修復)**。真「本次已買回股數」在 **cols[13]**;
   Scala 讀 cols[12]=「買回達一定標準資料」(幾乎全空)→ parseLong("")=0 → 整欄 2,933 筆
   歸零。台泥 2019-05-10:真已買回 8,000,000 股,PG 存 0。**修法**:cols[12]→cols[13]。

4. **BUG#4 company_name 近九成亂碼(根因修復)**。PG 2,603/2,933 列含 U+FFFD
   (台泥='�唳野'、台積='�啁���/td>' 還混入洩漏 HTML 標籤)——初次 bulk import 用錯
   編碼、之後 insert-only 去重不覆蓋既有。現行封存快照為乾淨 UTF-8;`_decode` 正確
   解碼 → 全列乾淨中文名(舊列 Python 對、PG 錯;近期列兩邊皆乾淨)。影響低(下游一律
   用 company_code join),但一併修對。

## 20 格資料列 → cache 11 欄對位(0-indexed,實測 2026_7.html 台泥 1101)

    [ 1] 公司代號        → company_code   (股號守門)
    [ 2] 公司名稱        → company_name   (_norm,不 cleanCell,存真中文)
    [ 3] 董事會決議日期  → announce_date  (民國;不可解析 → 丟列,對齊 Scala)
    [ 6] 預定買回股數    → planned_shares (股,無 ×1000;欄名註解的「×1000」是 stale)
    [ 7] 買回價格區間最低 → price_low
    [ 8] 買回價格區間最高 → price_high
    [ 9] 預定買回期間起  → period_start   (民國;不可解析 → fallback announce_date)
    [10] 預定買回期間迄  → period_end     (民國;fallback announce_date)
    [13] 本次已買回股數  → executed_shares  ★BUG#3 修(Scala 讀 [12])
    [18] 佔已發行股份比例 → pct_of_capital  ★BUG#2 修(Scala 讀 [16]=總金額)
    (略過:[0]序號 [4]買回目的 [5]金額上限 [11]是否執行完畢 [12]達標資料
     [14]已註銷/轉讓股數 [15]佔預定比例 [16]已買回總金額 [17]平均每股價 [19]未執行原因)

## 版型守衛(parse.SchemaDrift fail-loud,比照 daily_quote._guard / insider_holding)

MOPS 悄悄增/減欄正是 BUG#2/#3 的成因。故三重鎖:①股號資料列必為 20 格(否則 fail-loud)
②頂表頭固定邏輯位置含關鍵字(尤其 [11]本次已買回股數、[16]佔已發行比例——Scala 錯位的
兩欄)③子表頭含「最低/最高/起/迄」(鎖死兩個 colspan=2 → 資料列 +2 偏移)。任一漂移即紅。

## 欄位/型別:回傳 DF 與 cache 表 treasury_stock_buyback 同構(11 欄,cache 投影掉 PG 的 id)

`parse_raw` / `fetch_month` 皆回這 11 欄;`fetch_month` 走 archive→parse、不讀 cache;
parity 測試讀 PG 當對照。cache 同步 SQL:research/cache_tables.py:66;Slick 真源:
src/main/scala/db/table/TreasuryStockBuyback.scala(12 欄,去重唯一索引見 KEY 註)。

依賴 `research/cache_tables.py`?否(本模組是爬蟲寫入側;parity 測試才讀 cache/PG)。
"""
from __future__ import annotations

import re
from datetime import date as Date

import polars as pl
from bs4 import BeautifulSoup

from quantlib.crawl import archive, http, parse
from quantlib.crawl.sink import Sink

TABLE = "treasury_stock_buyback"
#: 去重 / cache upsert 批次鍵(對齊 Slick 唯一索引 idx_TreasuryStockBuyback_market_date_code)。
KEY_COLS = ["market", "announce_date", "company_code"]
MARKETS = ("twse", "tpex")

#: 全史快照端點(TreasuryStockBuybackSetting.scala:26-28;月參數不篩選,回全部歷史)。
_PAGE = "https://mopsov.twse.com.tw/mops/web/ajax_t35sc09"
_TYPEK = {"twse": "sii", "tpex": "otc"}

#: 資料列股號守門(對齊 Scala `stockCodeRegex = [0-9][0-9A-Z]{3,}` + `.matches()` 全串)。
_STOCK = re.compile(r"[0-9][0-9A-Z]{3,}")
#: 實測所有股號資料列恆為 20 格(表頭 18 邏輯欄 + 兩個 colspan=2)。
_NCELLS = 20

#: jsoup `.text()` 空白集 = ASCII 空白 + U+00A0(nbsp);**不含** U+3000(全形空白保留,
#: 對齊 Scala `.trim`)。見 insider_holding docstring #5(同一 parseMopsHtml 語義)。
_WS = re.compile("[ \t\n\r\f\u00a0]+")
_STRIP = " \t\n\r\f\u00a0"
#: 表頭關鍵字比對:去掉所有空白(含全形,MOPS 表頭夾雜 <br>/全形空白)再子字串比對。
_ANY_WS = re.compile("[ \t\n\r\f\u00a0\u3000]")

#: DF / cache schema(11 欄,順序 = Slick `*` 去 id)。兩股數欄 Int64(大型股 > 2^31)。
_SCHEMA = {
    "market": pl.Utf8, "announce_date": pl.Date, "company_code": pl.Utf8,
    "company_name": pl.Utf8, "planned_shares": pl.Int64,
    "price_low": pl.Float64, "price_high": pl.Float64,
    "period_start": pl.Date, "period_end": pl.Date,
    "executed_shares": pl.Int64, "pct_of_capital": pl.Float64,
}
CACHE_COLS = list(_SCHEMA)

#: 頂表頭位置守衛(邏輯 th index → 關鍵字子字串,去空白後比對)。18 邏輯欄:
#:   序號 公司代號 公司名稱 董事會決議日期 買回目的 買回股份總金額上限 預定買回股數
#:   買回價格區間(colspan2) 預定買回期間(colspan2) 是否執行完畢 買回達一定標準資料
#:   本次已買回股數 本次執行完畢已註銷或轉讓股數 佔預定買回股數比例 本次已買回總金額
#:   本次平均每股買回價格 佔公司已發行股份總數比例 本次未執行完畢之原因
#: 特別鎖 [11]本次已買回股數 + [14]本次已買回總金額 + [16]佔已發行比例 —— Scala 錯位的三欄。
_TOP_GUARD = {1: "公司代號", 2: "公司名稱", 3: "董事會決議日期", 6: "預定買回股數",
              9: "是否執行完畢", 11: "本次已買回股數", 14: "本次已買回總金額",
              16: "佔公司已發行股份總數比例"}
#: 子表頭(colspan 展開):買回價格區間→最低/最高、預定買回期間→起/迄。
_SUB_GUARD = {0: "最低", 1: "最高", 2: "起", 3: "迄"}


def _decode(raw: bytes) -> str:
    """MOPS 混編碼嗅探(對齊 TradingReader.parseMopsHtml:977-981):先試 UTF-8,含替換
    字元 U+FFFD → 退回 Big5-HKSCS。t35sc09 現行快照為乾淨 UTF-8(BUG#4 舊 PG 亂碼在此修對)。"""
    try:
        s = raw.decode("utf-8")
        if "�" in s:
            raise ValueError
        return s
    except (UnicodeDecodeError, ValueError):
        return raw.decode("big5-hkscs", errors="replace")


def _norm(el) -> str:
    """儲存格文字 = jsoup `.text()` 語義:塊界→空白、collapse ASCII+nbsp 空白、trim
    (不去 U+3000)。`separator=" "` 對齊 jsoup 不把相鄰節點黏死。"""
    return _WS.sub(" ", el.get_text(separator=" ")).strip(_STRIP)


def _rmws(s: str) -> str:
    return _ANY_WS.sub("", s)


def _long(s: str) -> int:
    """對齊 Scala `parseLong = Try(cleanCell(s).toLong).getOrElse(0L)`:cleanCell 去
    逗號/%/空白後轉整數,空/非數 → 0。"""
    try:
        return int(parse.clean(s))
    except ValueError:
        return 0


def _double(s: str) -> float:
    """對齊 Scala `parseDouble = Try(cleanCell(s).toDouble).getOrElse(0.0)`。"""
    try:
        return float(parse.clean(s))
    except ValueError:
        return 0.0


def _check(header: list[str], guard: dict[int, str], what: str) -> None:
    cells = [_rmws(c) for c in header]
    for i, kw in guard.items():
        got = cells[i] if i < len(cells) else "<缺>"
        if kw not in got:
            raise parse.SchemaDrift(
                f"treasury_stock_buyback {what}位移:col[{i}] 期望含 '{kw}' 實得 '{got}'"
                f"(MOPS 改格式?BUG#2/#3 即此類欄位錯位)")


def _guard(th_rows: list[list[str]], has_data: bool) -> None:
    """表頭位置守衛 fail-loud。無資料(空頁,無頂表頭)→ 放行(0 列);有資料列卻找不到
    頂表頭 / 子表頭,或關鍵欄位移 → SchemaDrift。"""
    top = next((r for r in th_rows if any("公司代號" in _rmws(c) for c in r)), None)
    if top is None:
        if has_data:
            raise parse.SchemaDrift(
                "treasury_stock_buyback 有資料列卻找不到『公司代號』頂表頭(MOPS 改格式?)")
        return
    _check(top, _TOP_GUARD, "頂表頭")
    sub = next((r for r in th_rows
                if r and _rmws(r[0]) == "最低" and any("最高" in _rmws(c) for c in r)), None)
    if sub is None:
        raise parse.SchemaDrift(
            "treasury_stock_buyback 缺子表頭『最低/最高/起/迄』"
            "(價格區間/買回期間 colspan 改格式?)")
    _check(sub, _SUB_GUARD, "子表頭")


def parse_raw(market: str, raw: bytes) -> pl.DataFrame:
    """封存原始檔 bytes → 11 欄 cache-schema DF(空 = 該市場無買回宣告)。

    忠實移植 readTreasuryStockBuyback:`<table tr>` 取 `<td>` 文字(jsoup 語義)→
    20 格且 cols[1] 為股號的資料列 → 三鍵去重(keep first,文件序)。`_guard` 先驗表頭。
    四個稽核 bug 一次寫對:2000-2010 納入(任意位數民國年)、executed=cols[13]、
    pct=cols[18]、company_name 乾淨 UTF-8。
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

    # 股號資料列:cols[1] 為股號。實測恆為 20 格 → 非 20 格即版型漂移,fail-loud(不靜默漏)。
    stock_rows = [r for r in td_rows if len(r) >= 2 and _STOCK.fullmatch(r[1])]
    drift = [r for r in stock_rows if len(r) != _NCELLS]
    if drift:
        raise parse.SchemaDrift(
            f"treasury_stock_buyback 股號資料列非 {_NCELLS} 格:{len(drift)} 列"
            f"(首例 {len(drift[0])} 格 code={drift[0][1]!r};MOPS 增/減欄?)")
    _guard(th_rows, bool(stock_rows))

    seen: set[tuple[str, Date, str]] = set()
    recs: list[dict] = []
    for r in stock_rows:
        announce = parse.parse_minguo_slash(r[3])  # BUG#1 修:任意位數民國年(含 2000-2010)
        if announce is None:  # 無法解析公告日 → 丟列(對齊 Scala getOrElse(throw).toOption)
            continue
        code = r[1]
        dkey = (market, announce, code)
        if dkey in seen:  # 三鍵去重 keep first(對齊 Scala distinctBy,文件序)
            continue
        seen.add(dkey)
        recs.append({
            "market": market,
            "announce_date": announce,
            "company_code": code,
            "company_name": r[2],                     # _norm,不 cleanCell(存真中文)
            "planned_shares": _long(r[6]),            # 股(無 ×1000)
            "price_low": _double(r[7]),
            "price_high": _double(r[8]),
            "period_start": parse.parse_minguo_slash(r[9]) or announce,
            "period_end": parse.parse_minguo_slash(r[10]) or announce,
            "executed_shares": _long(r[13]),          # ★BUG#3 修(Scala 讀 [12] 空欄→0)
            "pct_of_capital": _double(r[18]),         # ★BUG#2 修(Scala 讀 [16] 總金額)
        })
    return pl.DataFrame(recs, schema=_SCHEMA)


def fetch_month(market: str, year: int, month: int) -> pl.DataFrame | None:
    """抓庫藏股全史快照 → **先原樣封存原始檔到 data/** → parse → 回 11 欄 DF。

    原始檔封存鐵律:`archive.save_raw_named` 一定在 parse 之前(位元保真,先落地才解析)。
    端點回全史快照(月參數不篩選),故封存為 `{year}_{month}.html`(沿用 Scala 慣例、
    非日頻);每月刷新一次即涵蓋新宣告。空快照 → None(交由呼叫端決定)。
    """
    if market not in _TYPEK:
        raise ValueError(f"未知 market:{market}")
    yy = year - 1911
    raw = http.fetch_bytes(_PAGE, form={
        "step": "1", "firstin": "1", "TYPEK": _TYPEK[market],
        "yearb": str(yy), "monthb": f"{month:02d}",
        "yeare": str(yy), "monthe": f"{month:02d}",
    })
    archive.save_raw_named(TABLE, market, year, f"{year}_{month}.html", raw)  # 先落地再 parse
    df = parse_raw(market, raw)
    return df if not df.is_empty() else None


def refresh(sink: Sink, upto: Date) -> int:
    """月頻刷新便捷:抓當月全史快照(twse+tpex)→ 以三鍵 upsert 進 cache。回插入列數。

    端點每次回全史,upsert(刪匹配鍵 + 插入)自然把 port 修正值覆蓋 PG/舊 cache 的錯值
    (executed=0、pct=金額、名稱亂碼、缺 2000-2010)。整合進 update.py 由呼叫端接線。
    """
    total = 0
    for market in MARKETS:
        try:
            df = fetch_month(market, upto.year, upto.month)
        except Exception as exc:  # noqa: BLE001
            print(f"[crawl] {TABLE}/{market} 抓取失敗:{exc}")
            continue
        if df is None:
            continue
        n = sink.upsert(TABLE, df, KEY_COLS)
        total += n
        print(f"[crawl] {TABLE}/{market} {upto:%Y-%m} 快照: {n} 列(全史)")
    return total
