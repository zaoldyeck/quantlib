"""financial_analysis port 忠實度守護:Python 獨立解析既有封存原始檔
(data/financial_analysis/<market>/<year>_<a|b>.csv)對照 PG `financial_analysis`
(Scala `FinancialReader.readFinancialAnalysis` 的產物),**逐位證明 port 正確、
並定位 PG 已知的 schema bug**。這是 port 忠實度的證明(先紅後綠)。

## 對照基準 = PG(非 cache)

本源**不在 cache**(稽核確認 research/ 零引用),唯一結構化真源是 PG。用 DuckDB
`ATTACH … (TYPE postgres, READ_ONLY)`(與稽核 A 腳本、其他 parity 測試同法),零寫入。

## 稽核「壞資料」的處理(docs/data_audit/_done/A-financial_analysis.json)

本源稽核**無「壞日期」**(年頻整表,非日頻),PG 的已知錯誤是**系統性 schema bug**:
所有 _b 版型年度(1989-2014 的 IFRS 前部分,18,591 列 = 45%)因 reader 寫死索引、
不依欄數分支,**尾 6 欄整體右移一格**。故本測試**不用「排除壞日期」**,改以**位移鏈
逐格斷言「port 對、PG 錯」**取代 —— 這比排除更強(精確定位每一格錯在哪)。

## 四道證明(全 76 檔、全 41,144 列)

- **A 基準**:`_parse_buggy`(逐字複刻 Scala 寫死索引 raw[2..20])== PG,**每檔每列每欄**。
  → 證 PG 完全 = Scala reader 產物(我對基準的理解逐位精確;把 bug 忠實記錄下來)。
- **B 正確性(方法獨立)**:`fa.parse`(依欄數分版型)== `_ref_by_name`(依**中文標頭文字**
  對映,與 port 的位置邏輯完全獨立)== ,**每檔每列 20 欄**。→ 證 port 兩種獨立方法一致 = 對。
- **C 修復差異(先紅後綠)**:
  · **_a 檔**:`fa.parse` 19 欄逐位 == PG(兩者皆對);`operating_income` 為 null(_a 無此欄)。
  · **_b 檔**:對齊前 13 欄 == PG;尾 6 欄依**位移鏈** `port[X] == PG[X_右移一格]`(port≠PG 且
    port 對);**補回** raw[21] 現金再投資比率(PG 丟棄)、**保留** raw[15] 營業利益佔實收資本(PG 誤塞)。
- **D 人工錨**:台泥(1101)2011 —— port EPS=2.33、純益率=35.84、營業利益=1.71、現金再投資=0.86;
  PG EPS=**35.84**(裝的是純益率,錯指標又錯單位)。人可直接核對的地面真值。
- **E 結構**:每 (market,year) 的封存檔公司集 == PG 公司集(無漏列/多列);混合年 _a/_b 互斥。

## 兩個入口:pytest 快速守護 vs 全史 PG parity 腳本(對齊 tdcc/sbl 慣例)

- **pytest(離線、秒級、進 218-test 套件)**:`test_*` 只讀封存原始檔 + parse,不連 PG,
  鎖住 port 關鍵行為(_b 位移修復錨、_a 正確、operating_income/reinvestment 補回、
  版型守衛 fail-loud、封存鐵律、**全 corpus port==ref_by_name 逐位**)。
- **`python -m ...`(對照 PG,重工、手動跑)**:上面 A–E 四道全史逐位證據(改 port 後必跑)。

Run:
    uv run --project research python -m pytest research/crawl/tests/test_financial_analysis_parity.py -q
    uv run --project research python -m research.crawl.tests.test_financial_analysis_parity
    uv run --project research python -m research.crawl.tests.test_financial_analysis_parity 2011
"""
from __future__ import annotations

import csv
import io
import re
import sys

import duckdb

from research import paths
from research.crawl.sources import financial_analysis as fa
from research.db import DEFAULT_DSN

TABLE = "financial_analysis"

# --------------------------------------------------------------------------- #
# PG 19 指標欄(DB 順序)。前 13 對齊(port==PG 恆真)、尾 6 為 _b 位移區。          #
# --------------------------------------------------------------------------- #
_ALIGNED = [
    "liabilities/assets_ratio(%)",
    "Long-term_funds_to_property&plant&equipment(%)",
    "current_ratio(%)",
    "quick_ratio(%)",
    "times_interest_earned_ratio(%)",
    "average_collection_turnover(times)",
    "average_collection_days",
    "average_inventory_turnover(times)",
    "average_inventory_days",
    "property&plant&equipment_turnover(times)",
    "total_assets_turnover(times)",
    "return_on_total_assets(%)",
    "return_on_equity(%)",
]
_TAIL_PG = [
    "profit_before_tax_to_capital(%)",
    "profit_to_sales(%)",
    "earnings_per_share(NTD)",
    "cash_flow_ratio(%)",
    "cash_flow_adequacy_ratio(%)",
    "cash_flow_reinvestment_ratio(%)",
]
_DB19 = _ALIGNED + _TAIL_PG                     # Scala 寫死映射的 19 欄(DB 順序)

#: _b 位移鏈:同一 raw 索引,port 的**正確**欄 vs PG 的**錯位**欄(右移一格)。
_B_SHIFT = [
    ("operating_income_to_paid_in_capital(%)", "profit_before_tax_to_capital(%)"),  # raw15
    ("profit_before_tax_to_capital(%)",        "profit_to_sales(%)"),               # raw16
    ("profit_to_sales(%)",                     "earnings_per_share(NTD)"),          # raw17
    ("earnings_per_share(NTD)",                "cash_flow_ratio(%)"),               # raw18
    ("cash_flow_ratio(%)",                     "cash_flow_adequacy_ratio(%)"),      # raw19
    ("cash_flow_adequacy_ratio(%)",            "cash_flow_reinvestment_ratio(%)"),  # raw20
]

#: 中文標頭文字(去空白)→ 輸出欄名。**獨立於 port 的欄數/位置邏輯**(全 26 種標頭實測窮舉,
#: 含 IFRS 更名的雙變體 + _b 的 stray '->' + '<br>')。_ref_by_name 用它交叉驗證 port。
_HEADER_TO_COL = {
    "財務結構-負債佔資產比率(%)": "liabilities/assets_ratio(%)",
    "財務結構-長期資金佔固定資產比率(%)": "Long-term_funds_to_property&plant&equipment(%)",
    "財務結構-長期資金佔不動產、廠房及設備比率(%)": "Long-term_funds_to_property&plant&equipment(%)",
    "償債能力-流動比率(%)": "current_ratio(%)",
    "償債能力-速動比率(%)": "quick_ratio(%)",
    "償債能力-利息保障倍數(%)": "times_interest_earned_ratio(%)",
    "經營能力-應收款項週轉率(次)": "average_collection_turnover(times)",
    "經營能力->應收款項收現日數": "average_collection_days",
    "經營能力-平均收現日數": "average_collection_days",
    "經營能力-存貨週轉率(次)": "average_inventory_turnover(times)",
    "經營能力-平均售貨日數": "average_inventory_days",
    "經營能力-固定資產週轉率(次)": "property&plant&equipment_turnover(times)",
    "經營能力-不動產、廠房及設備週轉率(次)": "property&plant&equipment_turnover(times)",
    "經營能力-總資產週轉率(次)": "total_assets_turnover(times)",
    "獲利能力-資產報酬率(%)": "return_on_total_assets(%)",
    "獲利能力-股東權益報酬率(%)": "return_on_equity(%)",
    "獲利能力-權益報酬率(%)": "return_on_equity(%)",
    "獲利能力-營業利益佔實收資本比率(%)": "operating_income_to_paid_in_capital(%)",
    "獲利能力-稅前純益佔實收資本比率(%)": "profit_before_tax_to_capital(%)",
    "獲利能力-純益率(%)": "profit_to_sales(%)",
    "獲利能力-每股盈餘(元)": "earnings_per_share(NTD)",
    "現金流量-現金流量比率(%)": "cash_flow_ratio(%)",
    "現金流量-現金流量允當比率(%)": "cash_flow_adequacy_ratio(%)",
    "現金流量-現金再投<br>資比率(%)": "cash_flow_reinvestment_ratio(%)",
}
_CODE_NAME_HEADERS = {"公司代號", "公司簡稱"}


# --------------------------------------------------------------------------- #
def _eq(a, b) -> bool:
    """None-aware 精確相等(同一十進位字串 parse 兩次的 double 逐位相同,故用 ==)。"""
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) and isinstance(b, float):
        if a != a and b != b:            # NaN==NaN(本 corpus 無,防禦)
            return True
    return a == b


def _rows_of(text: str) -> list[list[str]]:
    return [r for r in csv.reader(io.StringIO(text)) if r]


def _parse_buggy(text: str) -> dict[str, dict]:
    """逐字複刻 Scala 寫死索引:transferValues(i)=raw[2+i],i=0..18 → _DB19。

    _b(22 欄):用 raw[2..20],**丟棄 raw[21]**(= Scala 的 bug 本體)。code→{name+19 欄}。
    """
    out: dict[str, dict] = {}
    for r in _rows_of(text)[1:]:
        if not r:
            continue
        vals = [fa._num(x) for x in r[2:]]
        rec: dict = {"company_name": r[1]}
        for i, col in enumerate(_DB19):
            rec[col] = vals[i] if i < len(vals) else None
        out[r[0]] = rec
    return out


def _ref_by_name(text: str) -> dict[str, dict]:
    """**與 port 位置邏輯獨立**的正確解析:依中文標頭文字定位每欄。code→{name+20 指標欄}。"""
    rows = _rows_of(text)
    header = [c.replace(" ", "") for c in rows[0]]
    idx_to_col: dict[int, str] = {}
    for i, h in enumerate(header):
        if h in _CODE_NAME_HEADERS:
            continue
        if h not in _HEADER_TO_COL:
            raise AssertionError(f"未知標頭欄 {h!r}(_ref_by_name 需補 _HEADER_TO_COL)")
        idx_to_col[i] = _HEADER_TO_COL[h]
    out: dict[str, dict] = {}
    for r in rows[1:]:
        if not r:
            continue
        rec: dict = {c: None for c in fa._METRIC_COLS}
        rec["company_name"] = r[1]
        for i, col in idx_to_col.items():
            rec[col] = fa._num(r[i])
        out[r[0]] = rec
    return out


def _port_rows(market: str, year: int, text: str) -> dict[str, dict]:
    df = fa.parse(market, year, text)
    return {} if df is None else {d["company_code"]: d for d in df.to_dicts()}


# --------------------------------------------------------------------------- #
def _connect():
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{DEFAULT_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    return con


def _load_pg(con) -> dict[tuple[str, int], dict[str, dict]]:
    """一次載入整表 → {(market,year): {code: {name + 19 db 欄}}}。"""
    cols = ",".join(f'"{c}"' for c in (["company_name"] + _DB19))
    df = con.execute(
        f"SELECT market, year, company_code, {cols} FROM pg.public.{TABLE}").pl()
    out: dict[tuple[str, int], dict[str, dict]] = {}
    for d in df.to_dicts():
        key = (d["market"], d["year"])
        out.setdefault(key, {})[d["company_code"]] = d
    return out


def _archived_files():
    """遞迴掃(對齊 Scala deepFiles),回 (market, year, schema, path);既有扁平佈局。"""
    out = []
    for market in fa.MARKETS:
        base = paths.RAW / TABLE / market
        if not base.exists():
            continue
        for p in sorted(base.rglob("*.csv")):
            m = re.fullmatch(r"(\d{4})_([ab])\.csv", p.name)
            if m:
                out.append((market, int(m.group(1)), m.group(2), p))
    return out


# --------------------------------------------------------------------------- #
def _check_file(market, year, schema, path, pg_all, fails):
    text = path.read_text("Big5-HKSCS", errors="replace")
    port = _port_rows(market, year, text)
    ref = _ref_by_name(text)
    buggy = _parse_buggy(text)
    pg = pg_all.get((market, year), {})
    tag = f"{market} {year}_{schema}"

    def fail(msg):
        fails.append(f"{tag}: {msg}")

    # E 結構:封存檔公司集 ⊆ PG(此檔所有 code 都應在 PG;混合年另一版型檔補足其餘)。
    missing = [c for c in port if c not in pg]
    if missing:
        fail(f"{len(missing)} 檔內 code 不在 PG(如 {missing[:5]})")
        return  # 缺 PG 對照,後續逐格比對無意義

    n_a = n_b = 0
    for code, prow in port.items():
        g = pg[code]
        b = buggy[code]
        rf = ref[code]

        # A 基準:buggy(Scala 複刻)== PG,name + 19 db 欄。
        for col in ["company_name"] + _DB19:
            if not _eq(b[col], g[col]):
                fail(f"[A] {code}.{col}: buggy={b[col]!r} != PG={g[col]!r}")

        # B 正確性:port == ref_by_name,name + 20 指標欄。
        for col in ["company_name"] + fa._METRIC_COLS:
            if not _eq(prow[col], rf[col]):
                fail(f"[B] {code}.{col}: port={prow[col]!r} != ref={rf[col]!r}")

        # C 修復差異:_a 全等 PG;_b 對齊 13 == PG + 尾位移鏈。
        if schema == "a":
            n_a += 1
            for col in ["company_name"] + _DB19:
                if not _eq(prow[col], g[col]):
                    fail(f"[C_a] {code}.{col}: port={prow[col]!r} != PG={g[col]!r}")
            if prow["operating_income_to_paid_in_capital(%)"] is not None:
                fail(f"[C_a] {code}: _a 竟有 operating_income="
                     f"{prow['operating_income_to_paid_in_capital(%)']!r}(應為 null)")
        else:
            n_b += 1
            for col in ["company_name"] + _ALIGNED:
                if not _eq(prow[col], g[col]):
                    fail(f"[C_b] {code}.{col}: 對齊欄 port={prow[col]!r} != PG={g[col]!r}")
            for pc, gc in _B_SHIFT:      # port 的正確欄 == PG 的右移一格欄
                if not _eq(prow[pc], g[gc]):
                    fail(f"[C_b] {code}: 位移鏈斷 port[{pc}]={prow[pc]!r} != PG[{gc}]={g[gc]!r}")

    return schema, len(port), n_a, n_b


def _check_structural(files, pg_all, fails):
    """E:每 (market,year) 封存公司集 == PG 公司集;混合年 _a/_b 互斥。"""
    by_key: dict[tuple[str, int], dict[str, set]] = {}
    for market, year, schema, path in files:
        text = path.read_text("Big5-HKSCS", errors="replace")
        codes = set(_port_rows(market, year, text))
        by_key.setdefault((market, year), {})[schema] = codes
    for (market, year), per_schema in sorted(by_key.items()):
        a = per_schema.get("a", set())
        b = per_schema.get("b", set())
        if a & b:
            fails.append(f"{market} {year}: _a/_b 公司集非互斥,交集 {sorted(a & b)[:5]}")
        file_codes = a | b
        pg_codes = set(pg_all.get((market, year), {}))
        only_file = file_codes - pg_codes
        only_pg = pg_codes - file_codes
        if only_file or only_pg:
            fails.append(f"{market} {year}: 封存 vs PG 公司集不符 "
                         f"file-only {sorted(only_file)[:5]} / PG-only {sorted(only_pg)[:5]}")


def _check_anchor(pg_all, fails):
    """D:台泥(1101)2011 人工錨 —— port 對、PG 錯。"""
    path = paths.RAW / TABLE / "twse" / "2011_b.csv"
    if not path.exists():
        fails.append("[D] 缺 twse/2011_b.csv 錨檔")
        return
    text = path.read_text("Big5-HKSCS", errors="replace")
    p = _port_rows("twse", 2011, text).get("1101")
    g = pg_all.get(("twse", 2011), {}).get("1101")
    if not p or not g:
        fails.append("[D] 1101 2011 在 port/PG 缺列")
        return
    want_port = {
        "earnings_per_share(NTD)": 2.33, "profit_to_sales(%)": 35.84,
        "operating_income_to_paid_in_capital(%)": 1.71,
        "cash_flow_reinvestment_ratio(%)": 0.86,
        "profit_before_tax_to_capital(%)": 23.59, "cash_flow_ratio(%)": 142.86,
    }
    for col, want in want_port.items():
        if not _eq(p[col], want):
            fails.append(f"[D] port 1101.{col}={p[col]!r} 應為 {want}")
    if not _eq(g["earnings_per_share(NTD)"], 35.84):
        fails.append(f"[D] 前提失效:PG 1101 EPS={g['earnings_per_share(NTD)']!r} 應為 35.84(bug)")
    if not _eq(p["earnings_per_share(NTD)"], 2.33) or _eq(g["earnings_per_share(NTD)"], 2.33):
        fails.append("[D] 未證實 port 對(EPS=2.33)、PG 錯(EPS=35.84)")


# --------------------------------------------------------------------------- #
# pytest 入口:離線快速守護(只讀封存 + parse,不連 PG)                          #
# --------------------------------------------------------------------------- #
def _arch(market: str, year: int, schema: str):
    return paths.RAW / TABLE / market / f"{year:04d}_{schema}.csv"


def _need(p):
    import pytest
    if not p.exists() or p.stat().st_size == 0:
        pytest.skip(f"無封存原始檔 {p}")


def test_b_shift_fix_1101_2011() -> None:
    """核心修復錨(台泥 1101 2011,_b):EPS 回真值、純益率歸位、_b 專有欄保留、raw[21] 補回。"""
    p = _arch("twse", 2011, "b")
    _need(p)
    row = _port_rows("twse", 2011, p.read_text("Big5-HKSCS", errors="replace"))["1101"]
    assert _eq(row["earnings_per_share(NTD)"], 2.33)          # 真 EPS(非 35.84 純益率)
    assert _eq(row["profit_to_sales(%)"], 35.84)             # 純益率歸位
    assert _eq(row["profit_before_tax_to_capital(%)"], 23.59)
    assert _eq(row["cash_flow_ratio(%)"], 142.86)
    assert _eq(row["operating_income_to_paid_in_capital(%)"], 1.71)  # _b 專有欄保留
    assert _eq(row["cash_flow_reinvestment_ratio(%)"], 0.86)         # raw[21] 補回(Scala 丟棄)


def test_a_schema_correct_1101_2020() -> None:
    """_a 版型(台泥 1101 2020):EPS 正確 4.32;operating_income 為 null(_a 無此欄)。"""
    p = _arch("twse", 2020, "a")
    _need(p)
    row = _port_rows("twse", 2020, p.read_text("Big5-HKSCS", errors="replace"))["1101"]
    assert _eq(row["earnings_per_share(NTD)"], 4.32)
    assert row["operating_income_to_paid_in_capital(%)"] is None


def test_operating_income_null_for_a_present_for_b() -> None:
    """operating_income:_a 檔整欄 null、_b 檔有值(補回的 _b 專有欄語義正確)。"""
    pa, pb = _arch("twse", 2020, "a"), _arch("twse", 2011, "b")
    _need(pa)
    _need(pb)
    dfa = fa.parse("twse", 2020, pa.read_text("Big5-HKSCS", errors="replace"))
    dfb = fa.parse("twse", 2011, pb.read_text("Big5-HKSCS", errors="replace"))
    col = "operating_income_to_paid_in_capital(%)"
    assert dfa[col].null_count() == dfa.height          # _a:整欄 null(IFRS 後無此指標)
    assert dfb[col].drop_nulls().len() > 0              # _b:有值(補回的 _b 專有欄)


def test_schema_discrimination_and_guard_fail_loud() -> None:
    """欄數判版型(21→a/22→b);欄數或欄名位移 → SchemaDrift fail-loud(不靜默錯位)。"""
    import pytest
    pa, pb = _arch("twse", 2020, "a"), _arch("twse", 2011, "b")
    _need(pa)
    _need(pb)
    ha = _rows_of(pa.read_text("Big5-HKSCS", errors="replace"))[0]
    hb = _rows_of(pb.read_text("Big5-HKSCS", errors="replace"))[0]
    assert fa._schema_of(ha, "twse", 2020) == "a"
    assert fa._schema_of(hb, "twse", 2011) == "b"
    with pytest.raises(fa.SchemaDrift):
        fa._schema_of(["a", "b", "c"], "twse", 2020)     # 非 21/22 欄
    bad = list(ha)
    bad[15] = "獲利能力-錯欄名(%)"                        # _a col[15] 應含「稅前純益佔實收資本」
    with pytest.raises(fa.SchemaDrift):
        fa._guard_header(bad, "a", "twse", 2020)


def test_port_equals_ref_by_name_full_corpus() -> None:
    """**全 corpus 離線逐位**:port(依欄數位置)== ref_by_name(依中文標頭文字)。

    兩種**完全獨立**的解析法在全 41k 列每一格一致 → port 正確,且不依賴 PG。
    """
    files = _archived_files()
    if not files:
        import pytest
        pytest.skip("無封存原始檔")
    total = 0
    for market, year, schema, path in files:
        text = path.read_text("Big5-HKSCS", errors="replace")
        port = _port_rows(market, year, text)
        ref = _ref_by_name(text)
        assert set(port) == set(ref), f"{market} {year}: port/ref 代號集不符"
        for code, prow in port.items():
            rf = ref[code]
            for col in ["company_name"] + fa._METRIC_COLS:
                assert _eq(prow[col], rf[col]), \
                    f"{market} {year} {code}.{col}: port={prow[col]!r} ref={rf[col]!r}"
            total += 1
    assert total > 40000, f"只比對 {total} 列,corpus 疑似不全"


def test_fetch_year_archives_before_parse_flat(monkeypatch) -> None:
    """封存鐵律:fetch_year 先 archive(扁平 subdir=False)才 parse;兩步順序正確。"""
    p = _arch("twse", 1990, "b")
    _need(p)
    raw = p.read_bytes()
    order: list[str] = []
    saved: dict = {}

    def fake_fetch(url, **k):
        order.append("fetch")
        if url == fa._PAGE:
            return b"<input type='hidden' name='filename' value='t51sb02_x.csv'>"
        return raw

    def fake_save(source, market, year, filename, content, subdir=True):
        order.append("save")
        saved.update(source=source, filename=filename, subdir=subdir)
        return paths.RAW / filename

    monkeypatch.setattr(fa.http, "fetch_bytes", fake_fetch)
    monkeypatch.setattr(fa.archive, "save_raw_named", fake_save)
    df = fa.fetch_year(1990)                              # <1993 → 只 twse_b 一組
    assert order == ["fetch", "fetch", "save"]            # step1, step2, 才封存(在 parse 前)
    assert saved["subdir"] is False                       # 扁平佈局(既有 76 檔如此)
    assert saved["filename"] == "1990_b.csv"
    assert saved["source"] == TABLE
    assert df is not None and df.height > 0


# --------------------------------------------------------------------------- #
def main() -> None:
    args = sys.argv[1:]
    only_year = int(args[0]) if args and args[0].isdigit() else None

    con = _connect()
    try:
        pg_all = _load_pg(con)
    finally:
        con.close()
    pg_total = sum(len(v) for v in pg_all.values())

    files = _archived_files()
    if only_year is not None:
        files = [f for f in files if f[1] == only_year]
    print(f"financial_analysis parity(PG: {DEFAULT_DSN})")
    print(f"  PG 列數 {pg_total}、封存檔 {len(files)} 個"
          + (f"(篩 year={only_year})" if only_year else ""))

    fails: list[str] = []
    tot = {"a_files": 0, "b_files": 0, "a_rows": 0, "b_rows": 0, "rows": 0}
    for market, year, schema, path in files:
        res = _check_file(market, year, schema, path, pg_all, fails)
        if res is None:
            continue
        sch, n, n_a, n_b = res
        tot["rows"] += n
        tot["a_files" if sch == "a" else "b_files"] += 1
        tot["a_rows"] += n_a
        tot["b_rows"] += n_b

    _check_structural(files, pg_all, fails)
    if only_year is None:
        _check_anchor(pg_all, fails)

    print(f"  比對列數 {tot['rows']}("
          f"_a 檔 {tot['a_files']}/{tot['a_rows']} 列、_b 檔 {tot['b_files']}/{tot['b_rows']} 列)")
    print(f"  A 基準 buggy==PG、B port==ref_by_name、C 修復差異(_a 全等/_b 位移鏈)、D 錨、E 結構")
    if fails:
        print(f"\n✗ 失敗 {len(fails)} 項(前 40):")
        for m in fails[:40]:
            print(f"  ✗ {m}")
        raise SystemExit(1)
    print(f"\n✓ 全數逐位一致:port 正確(A+B+C+D+E),PG 的 _b 尾 6 欄位移已逐格定位、"
          f"port 補回現金再投資+保留營業利益(零資訊遺失)")


if __name__ == "__main__":
    main()
