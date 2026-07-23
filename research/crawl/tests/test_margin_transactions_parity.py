"""margin_transactions port 忠實度守護:Python 獨立解析既有封存原始檔
(data/margin_transactions/<market>/<year>/<y>_<m>_<d>.csv)必須逐位重現 PG
`margin_transactions`(Scala reader 的產物)——**乾淨日全 14 值欄逐格一致**,
稽核列出的**壞日期明確排除並斷言「Python 對、PG 錯」**。這是 port 忠實度的證明。

## 對照基準 = PG(非 cache)

cache 只投影 7 欄(margin_balance/short_balance/margin_quota/short_quota);要驗全 14 值欄
的對位,必須對 PG。用 DuckDB `ATTACH … (TYPE postgres, READ_ONLY)`(與稽核 A/C 腳本同法),
零寫入。稽核 C-margin_transactions 已證 cache==PG 逐位,故「port==PG」即「port==真源」。

## parity 語義:parse-of-archive vs Scala-parse-of-same-archive

PG 是 Scala `TradingReader.readMarginTransactions` 解析同一批原始檔的結果。本測試用
Python `margin_transactions.parse` 解析**同一批檔**,兩者對「乾淨日」必須逐位相同。

## 稽核壞日期的處理(docs/data_audit/_done/{A,C}-margin_transactions.json)

- **tpex era B(2007-06-01 ≤ date < 2008-09-30)**:PG 的 short_quota/offsetting 錯位
  (BUG#1)。本測試斷言 → 12 個好欄 port==PG;short_quota/offsetting **port≠PG 且 port 對**
  (port.short_quota==port.margin_quota、port 把「券餘>券限」的物理不可能列修為 0,PG 仍有)。
- **tpex 2011-01-03~2014-10-30 右補空白代號(BUG#2)**:PG 有全部列(舊程式匯入),Scala
  現行 raw-head 比對會丟 46.7 萬列。本測試斷言 port **不丟列**(代號集合==PG)且逐位一致,
  並證明該日原始 head 帶尾空白(Scala full-match 會失敗)。
- **檔名≠內容日期的複製汙染(BUG#4)**:twse 2003-09-12/2011-03-26 內容是別天 → 排除嚴格
  比對,改斷言 `_content_date` 抓得到不符(port 的 fetch 會 deferred 不封存)。tpex 7 個颱風
  幽靈日 + 2008-08-29 標頭日期是對的(整合層跨日指紋才抓得到)→ 排除,不判 port 失敗。
- **10 個真交易日整日缺資料**:原始檔為 4-byte 空回應,port/ PG 皆 0 列 → 無可比,略過。
- **tpex era A 2007-04~05 資券相抵來源即 0**:port 忠實取 v19(=0)== PG,屬乾淨(值為 0)。

Run:
    uv run --project research python -m research.crawl.tests.test_margin_transactions_parity
    uv run --project research python -m research.crawl.tests.test_margin_transactions_parity --full
    uv run --project research python -m research.crawl.tests.test_margin_transactions_parity 2019-11-07
"""
from __future__ import annotations

import sys
from datetime import date as Date

import duckdb

from research.crawl import archive
from research.crawl.sources import margin_transactions as mt
from research.db import DEFAULT_DSN

TABLE = "margin_transactions"

#: 14 個值欄(company_name + 13 數值);key = company_code 單獨比。
VALUE_COLS = [
    "company_name",
    "margin_purchase", "margin_sales", "cash_redemption",
    "margin_balance_of_previous_day", "margin_balance_of_the_day", "margin_quota",
    "short_covering", "short_sale", "stock_redemption",
    "short_balance_of_previous_day", "short_balance_of_the_day", "short_quota",
    "offsetting_of_margin_purchases_and_short_sales",
]
#: 13 個數值/整數欄:一律對 PG 嚴格逐位。
_NUM_COLS = [c for c in VALUE_COLS if c != "company_name"]
#: era B 中被 port 修正、與 PG 不同的兩欄(short_quota / offsetting)。
_ERA_B_FIX = {"short_quota", "offsetting_of_margin_purchases_and_short_sales"}
#: era B 中應與 PG 相同的 11 個數值欄。
_GOOD_NUM = [c for c in _NUM_COLS if c not in _ERA_B_FIX]

# ---- 稽核壞日期集合(A/C-margin_transactions.json)------------------------- #
_CONTAM_TWSE = {Date(2003, 9, 12), Date(2011, 3, 26)}
_CONTAM_TPEX = {Date(2008, 8, 29), Date(2012, 8, 2), Date(2014, 7, 23),
                Date(2015, 7, 10), Date(2015, 9, 29), Date(2016, 7, 8),
                Date(2016, 9, 27), Date(2016, 9, 28)}
_MISSING_TWSE = {Date(2002, 10, 24), Date(2004, 8, 20), Date(2007, 9, 6),
                 Date(2008, 11, 28), Date(2010, 8, 3), Date(2011, 3, 28),
                 Date(2011, 6, 23), Date(2013, 8, 30), Date(2014, 5, 23),
                 Date(2018, 11, 1)}
_MISSING_TPEX = {Date(2023, 6, 9)}

_ERA_B_START = mt._TPEX_ERA_B_START   # 2007-06-01
_ERA_C_START = mt._TPEX_ERA_C_START   # 2008-09-30


def _is_era_b(market: str, day: Date) -> bool:
    return market == "tpex" and _ERA_B_START <= day < _ERA_C_START


#: 代表性樣本(kind 決定斷言方式)。涵蓋 twse 兩簽章、tpex 三版型、四類 bug 邊界。
SAMPLE: list[tuple[str, str, str]] = [
    ("twse", "2001-01-02", "CLEAN"),      # 最舊(pre-2024 標頭簽章)
    ("twse", "2015-01-05", "CLEAN"),      # pre-2024-10 改名前
    ("twse", "2024-12-31", "CLEAN"),      # post-2024-10 改名後(欄序不變)
    ("twse", "2026-07-17", "CLEAN"),      # 近期
    ("tpex", "2007-01-10", "CLEAN"),      # era A(相抵=v19;PG 舊程式值,port 逐位重現)
    ("tpex", "2007-05-31", "CLEAN"),      # era A 尾(相抵來源=0,port 忠實 0==PG)
    ("tpex", "2007-06-05", "ERA_B"),      # era B 頭(BUG#1:port 修 short_quota/offsetting)
    ("tpex", "2008-01-25", "ERA_B"),      # era B + margin_quota idx8/idx9 對調保命索
    ("tpex", "2008-09-26", "ERA_B"),      # era B 尾
    ("tpex", "2008-09-30", "CLEAN"),      # era C 頭(版型切換點)
    ("tpex", "2011-10-11", "SPACE"),      # BUG#2:右補空白代號,port 不丟列
    ("tpex", "2014-10-30", "SPACE"),      # 空白版型最後一天
    ("tpex", "2019-11-07", "CLEAN"),      # era C 近期
    ("tpex", "2026-07-17", "CLEAN"),      # 最近
    ("twse", "2011-03-26", "CONTAM"),     # BUG#4:內容是 2017-12-18(前視汙染)
    ("twse", "2003-09-12", "CONTAM"),     # BUG#4:內容是 2003-09-18
]


# --------------------------------------------------------------------------- #
def _connect():
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{DEFAULT_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    return con


def _pg_rows(con, market: str, day: Date) -> dict[str, dict]:
    df = con.execute(
        f"SELECT company_code, {','.join(VALUE_COLS)} FROM pg.public.{TABLE} "
        "WHERE market = ? AND date = ? ORDER BY company_code", [market, day]).pl()
    return {r["company_code"]: r for r in df.to_dicts()}


def _port_rows(market: str, day: Date):
    p = archive.raw_path(TABLE, market, day)
    if not p.exists():
        return None
    text = p.read_bytes().decode("Big5-HKSCS", errors="replace")
    df = mt.parse(market, text, day)
    if df is None:
        return {}
    return {r["company_code"]: r for r in df.to_dicts()}


def _raw_head_has_padding(market: str, day: Date) -> bool:
    """該檔是否有「代號帶尾/前空白」的股票列(Scala 未清空白的 full-match 會丟掉)。"""
    p = archive.raw_path(TABLE, market, day)
    text = p.read_bytes().decode("Big5-HKSCS", errors="replace")
    for r in mt._read_rows(text):
        if not r:
            continue
        head = r[0]
        stripped = head.replace(" ", "").replace(",", "")
        if mt._STOCK_CODE.fullmatch(stripped) and head != stripped:
            return True
    return False


def _cmp_num(pg: dict, port: dict, cols: list[str]) -> list[str]:
    """共同代號上 cols 的**嚴格**差異訊息(空 = 逐位一致)。數值欄一律走這條。"""
    msgs = []
    for code in sorted(set(pg) & set(port)):
        for col in cols:
            a, b = pg[code][col], port[code][col]
            if a != b:
                msgs.append(f"{code}.{col}: PG={a!r} port={b!r}")
    return msgs


def _cmp_name(pg: dict, port: dict) -> tuple[list[str], int]:
    """company_name 比對。回 (硬差異, name-strip 修復數)。

    **唯一允許的 name 差 = 「port 保留原文、PG 被 Scala 去了空白/逗號」**(稽核 fix
    『name-strip 只清數值欄』:港 對、PG 錯,如 00739 raw『元大MSCI A股』被寫成
    『元大MSCIA股』)。其餘任何 name 差 = 真失敗。"""
    bad, fixed = [], 0
    for code in sorted(set(pg) & set(port)):
        pn, on = pg[code]["company_name"], port[code]["company_name"]
        if pn == on:
            continue
        if pn == on.replace(" ", "").replace(",", ""):
            fixed += 1                     # port 保留原文(對),PG 去了空白(Scala 的 bug)
        else:
            bad.append(f"{code}.company_name: PG={pn!r} port={on!r}(非空白修復)")
    return bad, fixed


def _set_diff(pg: dict, port: dict) -> tuple[list, list]:
    return sorted(set(pg) - set(port))[:8], sorted(set(port) - set(pg))[:8]


# --------------------------------------------------------------------------- #
# 純評估器(pg/port 已就位)→ 供單日 con-查詢與 --full 年批次共用。               #
def _eval_clean(pg: dict, port, market: str, day: Date) -> tuple[str, str]:
    if port is None:
        return "SKIP", f"{market} {day}: 無封存原始檔 → SKIP"
    if not pg and not port:
        return "SKIP", f"{market} {day}: 兩邊皆 0 列 → SKIP"
    only_pg, only_port = _set_diff(pg, port)
    if only_pg or only_port:
        return "FAIL", (f"{market} {day}: 代號集合不符 "
                        f"PG-only {only_pg} / port-only {only_port} "
                        f"(PG {len(pg)} / port {len(port)})")
    bad = _cmp_num(pg, port, _NUM_COLS)
    name_bad, nfix = _cmp_name(pg, port)
    allbad = bad + name_bad
    if allbad:
        return "FAIL", f"{market} {day}: {len(allbad)} 格不符:{'; '.join(allbad[:6])}"
    note = f";名稱 {nfix} 名為 port 保留原文(PG 被 Scala 去空白,port 對)" if nfix else ""
    return "OK", f"{market} {day}: {len(pg)} 列 × 13 數值欄逐位一致 + 名稱一致{note}"


def _eval_era_b(pg: dict, port, market: str, day: Date) -> tuple[str, str]:
    """PG 錯(BUG#1)、port 對:12 好欄一致;short_quota/offsetting port≠PG 且 port 自洽。"""
    if not port or not pg:
        return "SKIP", f"{market} {day}: era B 無資料 → SKIP"
    only_pg, only_port = _set_diff(pg, port)
    if only_pg or only_port:
        return "FAIL", f"{market} {day}: era B 代號集合不符 {only_pg}/{only_port}"
    good_bad = _cmp_num(pg, port, _GOOD_NUM)
    name_bad, _nfix = _cmp_name(pg, port)
    good_bad += name_bad
    if good_bad:
        return "FAIL", f"{market} {day}: era B 好欄不該變卻變:{'; '.join(good_bad[:6])}"

    common = sorted(set(pg) & set(port))
    # port 修復自洽:券限額 = 資限額(來源未印,由使用率反推;era B 恆等)
    non_invariant = [c for c in common if port[c]["short_quota"] != port[c]["margin_quota"]]
    if non_invariant:
        return "FAIL", (f"{market} {day}: era B port short_quota≠margin_quota "
                        f"{non_invariant[:6]}(修復不變式破裂)")
    # 先紅後綠:PG 有「券餘>券限」的物理不可能列(bug),port 已歸零
    pg_impossible = [c for c in common
                     if pg[c]["short_balance_of_the_day"] > pg[c]["short_quota"]]
    port_impossible = [c for c in common
                       if port[c]["short_balance_of_the_day"] > port[c]["short_quota"]]
    if not pg_impossible:
        return "FAIL", f"{market} {day}: 預期 PG 有『券餘>券限』bug 列卻沒有(測試前提失效)"
    if port_impossible:
        return "FAIL", (f"{market} {day}: port 仍有『券餘>券限』列 {port_impossible[:6]}"
                        f"(未修淨)")
    # short_quota/offsetting 確實 port≠PG(證明修了東西)
    changed = [c for c in common if pg[c]["short_quota"] != port[c]["short_quota"]
               or pg[c]["offsetting_of_margin_purchases_and_short_sales"]
               != port[c]["offsetting_of_margin_purchases_and_short_sales"]]
    if not changed:
        return "FAIL", f"{market} {day}: short_quota/offsetting 竟與 PG 全同(沒修到?)"
    return "OK", (f"{market} {day}: era B {len(common)} 列 — 12 好欄==PG;"
                  f"PG『券餘>券限』{len(pg_impossible)} 列→port 0 列;"
                  f"short_quota/offsetting 修正 {len(changed)} 列(port 對、PG 錯)")


def check_clean(con, market: str, day: Date) -> tuple[str, str]:
    return _eval_clean(_pg_rows(con, market, day), _port_rows(market, day), market, day)


def check_era_b(con, market: str, day: Date) -> tuple[str, str]:
    return _eval_era_b(_pg_rows(con, market, day), _port_rows(market, day), market, day)


def check_space(con, market: str, day: Date) -> tuple[str, str]:
    """BUG#2:右補空白代號。port 不丟列 → 代號集合 + 全欄 == PG,且證明確有帶空白的 head。"""
    status, msg = check_clean(con, market, day)
    if status != "OK":
        return status, msg
    if not _raw_head_has_padding(market, day):
        return "FAIL", f"{market} {day}: 預期有帶空白代號列卻沒有(樣本失效)"
    pg = _pg_rows(con, market, day)
    return "OK", (f"{market} {day}: {len(pg)} 列全欄==PG,且含 Scala raw-match 會丟的"
                  f"帶空白代號列 → port 修 BUG#2 未丟列")


def check_contam(con, market: str, day: Date) -> tuple[str, str]:
    """BUG#4:檔名≠內容日期。斷言 port 的內容日期守護抓得到(fetch 會 deferred 不封存)。"""
    p = archive.raw_path(TABLE, market, day)
    if not p.exists():
        return "SKIP", f"{market} {day}: 無封存檔 → SKIP"
    text = p.read_bytes().decode("Big5-HKSCS", errors="replace")
    cdate = mt._content_date(text, market)
    if cdate is None:
        return "FAIL", f"{market} {day}: 抽不到內容日期(守護失效)"
    if cdate == day:
        return "FAIL", f"{market} {day}: 內容日期竟==檔名日期(此檔非汙染?)"
    return "OK", (f"{market} {day}: 內容標題日期 {cdate} ≠ 檔名 → port fetch 守護會擋"
                  f"(deferred 不封存;PG 現存此汙染列已由稽核列冊)")


_CHECKERS = {"CLEAN": check_clean, "ERA_B": check_era_b,
             "SPACE": check_space, "CONTAM": check_contam}


# --------------------------------------------------------------------------- #
def _pg_year(con, market: str, year: int) -> dict[Date, dict[str, dict]]:
    """一次拉整年 PG 列,按 date → {code: row} 分組(避免逐檔往返 PG)。"""
    df = con.execute(
        f"SELECT date, company_code, {','.join(VALUE_COLS)} FROM pg.public.{TABLE} "
        "WHERE market = ? AND date >= ? AND date < ?",
        [market, Date(year, 1, 1), Date(year + 1, 1, 1)]).pl()
    out: dict[Date, dict[str, dict]] = {}
    for r in df.to_dicts():
        out.setdefault(r["date"], {})[r["company_code"]] = r
    return out


def _market_years(market: str):
    root = archive.raw_path(TABLE, market, Date(2000, 1, 1)).parent.parent
    years = sorted(int(p.name) for p in root.iterdir()
                   if p.is_dir() and p.name.isdigit())
    return root, years


def _year_files(root, year: int):
    for f in sorted((root / f"{year:04d}").glob("*.csv")):
        parts = f.stem.split("_")
        if len(parts) != 3:
            continue
        try:
            yield Date(int(parts[0]), int(parts[1]), int(parts[2]))
        except ValueError:
            continue


def check_full(con) -> tuple[int, int, int, list[str]]:
    """全封存檔逐檔 parity(依日期自動分類乾淨/era B/汙染/缺日;PG 年批次載入)。"""
    ok = skip = fail = 0
    fails: list[str] = []
    for market in mt.MARKETS:
        # 壞日期以 (market) 分流:同一日期在另一市場可能是正常交易日,不可跨市場誤跳。
        m_skip = ((_CONTAM_TWSE | _MISSING_TWSE) if market == "twse"
                  else (_CONTAM_TPEX | _MISSING_TPEX))
        root, years = _market_years(market)
        n = 0
        for year in years:
            pg_year = _pg_year(con, market, year)
            for day in _year_files(root, year):
                if day in m_skip:  # 該市場的 4-byte 空回應 / 整日複製汙染
                    skip += 1
                    continue
                pg = pg_year.get(day, {})
                port = _port_rows(market, day)
                status, msg = (_eval_era_b if _is_era_b(market, day)
                               else _eval_clean)(pg, port, market, day)
                if status == "OK":
                    ok += 1
                elif status == "SKIP":
                    skip += 1
                else:
                    fail += 1
                    fails.append(msg)
                n += 1
        print(f"  {market}: 掃 {n} 檔")
    return ok, skip, fail, fails


# --------------------------------------------------------------------------- #
def main() -> None:
    args = sys.argv[1:]
    con = _connect()
    try:
        if args and args[0] == "--full":
            print(f"margin_transactions FULL parity(PG: {DEFAULT_DSN})")
            ok, skip, fail, fails = check_full(con)
            print(f"\n結果:逐位一致 {ok}、SKIP {skip}、失敗 {fail}")
            for m in fails[:40]:
                print(f"  ✗ {m}")
            raise SystemExit(1 if fail else 0)

        if args:  # 單日:<market? 預設兩市場都試> YYYY-MM-DD
            day = Date.fromisoformat(args[-1])
            print(f"margin_transactions parity 單日 {day}")
            rc = 0
            for market in mt.MARKETS:
                kind = "ERA_B" if _is_era_b(market, day) else "CLEAN"
                status, msg = _CHECKERS[kind](con, market, day)
                mark = {"OK": "✓", "FAIL": "✗", "SKIP": "·"}[status]
                print(f"  {mark} {msg}")
                rc |= (status == "FAIL")
            raise SystemExit(1 if rc else 0)

        print(f"margin_transactions parity 樣本(PG: {DEFAULT_DSN});{len(SAMPLE)} 組")
        tally = {"OK": 0, "FAIL": 0, "SKIP": 0}
        fails = []
        for market, iso, kind in SAMPLE:
            day = Date.fromisoformat(iso)
            status, msg = _CHECKERS[kind](con, market, day)
            tally[status] += 1
            mark = {"OK": "✓", "FAIL": "✗", "SKIP": "·"}[status]
            print(f"  {mark} [{kind}] {msg}")
            if status == "FAIL":
                fails.append((market, iso, kind))
    finally:
        con.close()

    print(f"\n結果:逐位一致 {tally['OK']}、失敗 {tally['FAIL']}、SKIP {tally['SKIP']}")
    if fails:
        print(f"失敗:{fails}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
