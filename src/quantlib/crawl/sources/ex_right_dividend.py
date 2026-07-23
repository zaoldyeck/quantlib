"""ex_right_dividend 源:全世代除權息解析(twse 舊制 16 欄 / tpex 舊制 21~22 欄 /
MOPS 月檔 19 欄),供「從 raw 重建 cache」與每月增量更新共用。

還原價因子的原料。cache 欄(FC1,2026-07-23):market, date, company_code,
cash_dividend, right_or_dividend, closing_price_before_ex_right_ex_dividend,
ex_right_ex_dividend_reference_price——prices.py 以「參考價/前收盤」為首選還原因子
(配息+配股一體涵蓋),無參考價才退回「(前收盤−現金股利)/前收盤」。

## 三個歷史格式世代(移植自 Scala TradingReader.readExRightDividend + parseMopsRows,
   世代以「標頭內容」判定〔非檔名、非 fallthrough〕:公司代號→MOPS、資料日期→twse 舊制、
   除權息日期→tpex 舊制;皆不符 → SchemaDrift fail-loud)

1. **twse 舊制 TWT49U(2003–2024/06),16 欄**。日期「109年07月15日」(民國+1911)。
   欄:0 日期 / 1 代號 / 2 名稱 / 3 除權息前收盤 / 4 參考價 / 5 權值+息值(cash_dividend)/
   6 權息別(權/息/權息,原樣)/ 7 漲停 / 8 跌停 / 9 開盤競價基準 / 10 減除股利參考價;
   11–15 詳細/季別/淨值/盈餘丟棄。
2. **tpex 舊制(2008–2026 範圍檔),21 或 22 欄**。日期「114/07/29」(民國+1911)。
   欄:0 日期 / 1 代號 / 2 名稱 / 3 前收盤 / 4 參考價 / 7 權值+息值(cash_dividend;
   5 權值、6 息值丟棄)/ 8 除權/除息/除權息→權/息/權息 / 9 漲停 / 10 跌停 /
   11 開始交易基準價 / 12 減除股利參考價;13+ 丟棄。22 欄變體多「員工紅利轉增資」插第 15 欄,
   只讀到第 12 欄故無害。**tpex「日檔」實為範圍檔**(檔名=區間結尾、內容橫跨多年),
   日期一律取列內容非檔名。
3. **MOPS t108sb27 月檔(twse 2024-07+ / tpex 2026-05+),19 欄**,無價格欄。
   欄:0 代號 / 1 名稱 / 4+5 股票股利(元/股)/ 6 除權交易日 / 7+8+9 現金股利 /
   10 除息交易日(皆西元 yyyy/MM/dd)。每公司每期最多兩列:除息列(cash>0)、除權列(stock>0)。

## 修掉的稽核 bug(A/C-ex_right_dividend)

- **BUG 1(主 bug):純配股除權被存成整列 0 → 還原不回、假跌**。MOPS 無價格欄,舊碼把
  股票股利只當「>0 就發列」的布林、數值丟棄。改為對「除權列」以面額回推合成參考價:令
  closing_price_before = 面額 10 + 股票股利元/股、reference_price = 面額 10,則 prices.py
  首選因子 ref/pre = 10/(10+股票股利) = 正確稀釋因子。**配股+配息同日**(如必應 6625
  2024-07-18 股 1.0+現 3.5)分兩列(除息列走現金 fallback、除權列走合成 ref/pre),
  prices.py 對同 (code,ex_date) 取因子乘積 → (前收−現金)/前收 × 10/(10+股票股利)
  = 正確合併因子。
- **舊制存真實 ref/pre**:prices.py 首選 ref/pre,故舊制「權值+息值」為負的 24 筆
  (參考價>前收盤)也能正確還原(不再被 cache 的 cash_dividend>0 濾掉)。
- **name-strip 只清數值欄**:公司名只 trim 前後空白、保留內部空白(數值欄才去空白/逗號),
  不打壞含半形空白的 ETF 名。
- **日期取列內容非檔名**:三世代皆用列內日期,識破 tpex 範圍檔(檔名 2020_7_10 內容 2008–2020)。

## 世代邊界(非本單檔 parser 可獨力解決,留給重建編排 / 新端點)

- **雙源同鍵(SUSPECT 2)**:tpex 2025-2026 舊制範圍檔與 MOPS 月檔涵蓋同批事件、同鍵。
  舊制有真實價格、MOPS 為合成——重建時**須以 (market,date,company_code) 保留舊制、丟 MOPS**
  (否則 prices.py 乘積雙重稀釋)。本 parser 為單檔解析,跨檔優先序由重建編排負責。
- **ETF 收益分配 / 2023-07~2024-06 缺月 / 純現增除權**:屬來源覆蓋範圍(需另接端點或補抓
  公告月),非解析層,見 C-ex_right_dividend。

兩步流(fetch_month,移植自 Crawler.getExRightDividend):
1. POST ajax_t108sb27(step=1,TYPEK,民國 year,month)→ HTML,取 input[name=filename]。
2. POST t105sb02(step=10,filename)→ Big5 CSV → **原樣封存 data/** → parse_raw。
"""
from __future__ import annotations

import re
from datetime import date as Date

import polars as pl

from quantlib.crawl import archive, http, parse
from quantlib.crawl.sink import Sink

TABLE = "ex_right_dividend"
KEY_COLS = ["market", "date", "company_code"]
MARKETS = ("twse", "tpex")

_PAGE = "https://mopsov.twse.com.tw/mops/web/ajax_t108sb27"
_FILE = "https://mopsov.twse.com.tw/server-java/t105sb02"
_TYPEK = {"twse": "sii", "tpex": "otc"}
_REFRESH_MONTHS = 3

#: 台股面額 10 元(公司法 §156 面額股慣例;股票股利以「元/股」計,除權還原因子基準)。
_FACE_VALUE = 10.0

#: parse_raw 全欄輸出(對齊 Scala/PG 12 資料欄,供 parity + 稽核已知值交叉核對);
#: fetch_month/refresh 經 sink.upsert 投影到 cache 現有欄(FC1 為 7 欄)。
_RAW_SCHEMA = {
    "market": pl.Utf8, "date": pl.Date, "company_code": pl.Utf8,
    "company_name": pl.Utf8,
    "closing_price_before_ex_right_ex_dividend": pl.Float64,
    "ex_right_ex_dividend_reference_price": pl.Float64,
    "cash_dividend": pl.Float64, "right_or_dividend": pl.Utf8,
    "limit_up": pl.Float64, "limit_down": pl.Float64,
    "opening_reference_price": pl.Float64,
    "ex_dividend_reference_price": pl.Float64,
}

_INPUT_RE = re.compile(r'<input[^>]*name=["\']?filename["\']?[^>]*>', re.IGNORECASE)
_VALUE_RE = re.compile(r'value=["\']([^"\']*)["\']', re.IGNORECASE)
_CODE = re.compile(r"^\d{4}[0-9A-Z]?$")
_TWSE_DATE = re.compile(r"^(\d+)年(\d+)月(\d+)日$")
_RIGHT_MAP = {"除權": "權", "除息": "息", "除權息": "權息"}


def _num(v: str) -> float | None:
    """數值欄清洗:去空白/逗號 → float;空或不可解析 → None(對齊 Reader 數值語義)。"""
    s = v.replace(",", "").replace(" ", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _f(v: str) -> float:
    """同 _num 但空/壞 → 0.0(供 MOPS 現金/股票股利加總)。"""
    n = _num(v)
    return n if n is not None else 0.0


def _twse_date(s: str) -> Date | None:
    """民國「109年07月15日」→ 西元 date;非此格式 → None。"""
    m = _TWSE_DATE.match(s.strip())
    if not m:
        return None
    try:
        return Date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _slash_date(s: str) -> Date | None:
    """西元 yyyy/MM/dd → date(MOPS 除權/除息交易日)。"""
    s = s.strip()
    try:
        y, m, d = s.split("/")
        return Date(int(y), int(m), int(d))
    except (ValueError, TypeError):
        return None


def _parse_twse_legacy(rows: list[list[str]]) -> list[dict]:
    """twse 舊制 16 欄。列內日期(民國)、原樣權息別、數值欄 None-safe。"""
    recs = []
    for r in rows:
        if len(r) < 16 or r[0].strip() == "資料日期":
            continue
        d = _twse_date(r[0])
        if d is None:
            continue
        recs.append({
            "market": "twse", "date": d, "company_code": r[1].strip(),
            "company_name": r[2].strip(),
            "closing_price_before_ex_right_ex_dividend": _num(r[3]),
            "ex_right_ex_dividend_reference_price": _num(r[4]),
            "cash_dividend": _num(r[5]),          # 舊制 = 權值+息值(還原用 ref/pre)
            "right_or_dividend": r[6].strip(),    # 權 / 息 / 權息(原樣)
            "limit_up": _num(r[7]), "limit_down": _num(r[8]),
            "opening_reference_price": _num(r[9]),
            "ex_dividend_reference_price": _num(r[10]),
        })
    return recs


def _parse_tpex_legacy(rows: list[list[str]]) -> list[dict]:
    """tpex 舊制 21/22 欄(範圍檔,列內日期民國)。權值+息值在 col 7,權息別映射。"""
    recs = []
    for r in rows:
        if len(r) < 21 or r[0].strip() == "除權息日期":
            continue
        d = parse.parse_minguo_slash(r[0])
        if d is None:
            continue
        raw_rd = r[8].strip()
        recs.append({
            "market": "tpex", "date": d, "company_code": r[1].strip(),
            "company_name": r[2].strip(),
            "closing_price_before_ex_right_ex_dividend": _num(r[3]),
            "ex_right_ex_dividend_reference_price": _num(r[4]),
            "cash_dividend": _num(r[7]),          # 權值+息值(非 col 13 的純現金股利)
            "right_or_dividend": _RIGHT_MAP.get(raw_rd, raw_rd),
            "limit_up": _num(r[9]), "limit_down": _num(r[10]),
            "opening_reference_price": _num(r[11]),
            "ex_dividend_reference_price": _num(r[12]),
        })
    return recs


def _parse_mops(rows: list[list[str]], market: str) -> list[dict]:
    """MOPS 月檔 19 欄。每公司每期最多兩列:除息列(現金)、除權列(股票→合成 ref/pre)。

    BUG 1 修:MOPS 無價格欄,除權列以面額回推 ref/pre = 10/(10+股票股利元/股),
    讓 prices.py 首選因子自然還原配股稀釋(舊碼寫整列 0 → 假跌)。
    """
    recs = []
    for r in rows:
        if len(r) < 17 or not r[0].strip() or r[0].strip() == "公司代號":
            continue
        code = r[0].strip()
        if not _CODE.match(code):
            continue
        name = r[1].strip()
        stock = _f(r[4]) + _f(r[5])            # 股票股利:盈餘轉增資 + 資本公積轉增資
        cash = _f(r[7]) + _f(r[8]) + _f(r[9])  # 現金股利:盈餘分配 + 法定公積 + 特別股
        ex_right = _slash_date(r[6])
        ex_div = _slash_date(r[10])
        if cash > 0 and ex_div:
            recs.append({
                "market": market, "date": ex_div, "company_code": code,
                "company_name": name,
                "closing_price_before_ex_right_ex_dividend": None,
                "ex_right_ex_dividend_reference_price": None,
                "cash_dividend": cash, "right_or_dividend": "息",
                "limit_up": None, "limit_down": None,
                "opening_reference_price": None,
                "ex_dividend_reference_price": None,
            })
        if stock > 0 and ex_right:
            recs.append({
                "market": market, "date": ex_right, "company_code": code,
                "company_name": name,
                "closing_price_before_ex_right_ex_dividend": _FACE_VALUE + stock,
                "ex_right_ex_dividend_reference_price": _FACE_VALUE,
                "cash_dividend": 0.0, "right_or_dividend": "權",
                "limit_up": None, "limit_down": None,
                "opening_reference_price": None,
                "ex_dividend_reference_price": None,
            })
    return recs


def parse_raw(market: str, raw: bytes) -> pl.DataFrame:
    """解析任一世代的封存原始檔 → 全欄 DataFrame(世代以標頭內容判定,fail-loud)。

    同檔去重鍵 (date, code, 權/息別):除息+除權同日為兩個真事件不可壓平(prices.py
    對同 (code,ex_date) 取因子乘積 = 合併稀釋),只折疊意外重複列(keep first)。
    """
    text = raw.decode("Big5-HKSCS", errors="replace")
    rows = parse.parse_csv(text)
    if parse.find_header(rows, "公司代號") >= 0:
        recs = _parse_mops(rows, market)
    elif market == "twse" and parse.find_header(rows, "資料日期") >= 0:
        recs = _parse_twse_legacy(rows)
    elif market == "tpex" and parse.find_header(rows, "除權息日期") >= 0:
        recs = _parse_tpex_legacy(rows)
    elif any(sum(1 for c in r if c.strip()) >= 5 for r in rows):
        # 有資料形狀的列卻標頭不認 = 真格式漂移 → fail-loud(勿靜默錯位)。
        raise parse.SchemaDrift(
            f"ex_right_dividend/{market}:無法辨識世代"
            "(標頭非 公司代號 / 資料日期 / 除權息日期,但有資料形狀列)")
    else:
        # 無標頭且無資料列:TWSE 舊制端點對「當日無除權息事件」回 \r\n(2 bytes)——
        # 空檔非漂移,回空 df(對齊 Scala reader 對這些檔 filter 後 0 列、不炸)。
        recs = []
    return (pl.DataFrame(recs, schema=_RAW_SCHEMA)
            .unique(subset=["date", "company_code", "right_or_dividend"],
                    keep="first", maintain_order=True))


def fetch_month(market: str, year: int, month: int) -> pl.DataFrame | None:
    """抓某市場某年月的 MOPS 除權息公告 → **原樣封存 raw** → parse_raw;無事件 → None。"""
    form1 = {"step": "1", "firstin": "ture", "off": "1", "TYPEK": _TYPEK[market],
             "year": str(year - 1911), "month": str(month),
             "b_date": "1", "e_date": "31", "type": "0"}
    html = http.fetch_text(_PAGE, encoding="Big5-HKSCS", form=form1)
    tag = _INPUT_RE.search(html)
    if not tag:
        return None  # 該月無除權息事件
    val = _VALUE_RE.search(tag.group(0))
    if not val or not val.group(1).endswith(".csv"):
        return None
    raw = http.fetch_bytes(
        _FILE, form={"firstin": "true", "step": "10", "filename": val.group(1)})
    # 原始檔封存鐵律:先原子落地 raw(位元保真)才 parse。月檔名 <year>_<month>.csv。
    archive.save_raw_named(TABLE, market, year, f"{year}_{month}.csv", raw)
    df = parse_raw(market, raw)
    return df if not df.is_empty() else None


def _recent_months(upto: Date, n: int) -> list[tuple[int, int]]:
    y, m, out = upto.year, upto.month, []
    for _ in range(n):
        out.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


def refresh(sink: Sink, upto: Date) -> int:
    """補抓最近數月除權息(含當月,因除權息日多在公告月內或稍後)。"""
    total = 0
    for market in MARKETS:
        for year, month in _recent_months(upto, _REFRESH_MONTHS):
            try:
                df = fetch_month(market, year, month)
            except Exception as exc:  # noqa: BLE001
                print(f"[crawl] ex_right_dividend/{market} {year}-{month:02d} 抓取失敗:{exc}")
                continue
            if df is None:
                continue
            n = sink.upsert(TABLE, df, KEY_COLS)
            total += n
            print(f"[crawl] ex_right_dividend/{market} {year}-{month:02d}: {n} 列")
    return total
