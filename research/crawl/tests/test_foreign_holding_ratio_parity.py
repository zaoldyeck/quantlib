"""foreign_holding_ratio port 忠實度守護:Python 解析既有封存原始檔
(data/foreign_holding_ratio/<market>/<year>/YYYY_M_D.csv)必須逐位重現 PG 表
`foreign_holding_ratio`——除稽核列出的**已知壞日期**(前視汙染)與**已知壞欄**
(名稱被 Scala strip 掉內部空白)外。

## 為什麼比對 PG(且 == 比對 cache)

直接 ATTACH PostgreSQL READ_ONLY 逐欄比對(task 明訂「與 PG 該表逐欄比對」;PG 尚存、
且含 cache 投影掉的 `company_name`,才驗得了 name-strip 修正)。稽核 C-foreign_holding_ratio
已證 cache 與 PG 全史 8,225,920 列逐位一致,故結論同時適用 cache。PG 不可達 → SKIP
(cache-only 環境用 `--cache` 對照 9 欄)。

## parity 語義:parse-of-archive vs PG(parse-of-same-archive,先紅後綠)

PG 是 Scala reader 解析**同一批**封存原始檔的產物。本測試以 port 的 `fhr.parse_raw`
解析同檔,兩者對「好日期」須逐位相同——證明 Python parser 對位/值轉換與 Scala 等價。

## 已知壞日期(稽核 A/C-foreign_holding_ratio):tpex date < 2011-01-03(319,124 列 / 361 日)

TPEx insti/qfii 端點對「沒有資料的日期」回**當下最新快照**;Scala 只認檔名 → 把
2026-04-24 的快照蓋上 2010 全年的日期戳(412 檔在 2010 還沒掛牌、111 個幽靈日)。
port 從**內容** tables[0].date 取日期,故對這 361 檔:斷言 `內容日期 ≠ 檔名日期`
(Python 抓得出、PG/Scala 抓不出 → **Python 對、PG 錯**),不做逐位相等;PG 那 319,124
列排除於比對。其餘所有日期則反向斷言 `內容日期 == 檔名日期`——若稽核漏列了某錯日,
這裡會紅。另 4 個上游假休市日(2021-08-18/2025-08-15/2026-04-29/2026-05-28)兩市場
PG 皆 0 列、原始檔不存在,對逐位比對無影響,僅註記。

## 已知壞欄:company_name(name-strip,Python 對 / PG 錯)

Scala `cleanCell` 對每格去空白,打壞含半形空白的 ETF 名(`元大MSCI A股`→`元大MSCIA股`)。
port 只 trim 名稱、保內部空白。名稱差異中「PG = Python 去空白/標點後」者判為**預期修正**
(Python 對、PG 錯),計數不判失敗;其餘(clean 後仍不同)才是真不符 → FAIL。六個數值欄
一律要求逐位相等(股數精確、比率容差 1e-9)。

## 定點:int32 溢位 / 表頭守衛

`_check_int64`:發行股數 > 2^31(台積電 25,932,370,067)以 Int64 保真、且 == PG。
`_check_guards`:TWSE 表頭 / TPEx fields 位移 → `SchemaDrift` fail-loud。

Run:
    uv run --project research python -m research.crawl.tests.test_foreign_holding_ratio_parity
    uv run --project research python -m research.crawl.tests.test_foreign_holding_ratio_parity --all
    uv run --project research python -m research.crawl.tests.test_foreign_holding_ratio_parity --cache
    uv run --project research python -m pytest research/crawl/tests/test_foreign_holding_ratio_parity.py -q
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date as Date
from pathlib import Path

import duckdb
import polars as pl

from research import paths
from research.crawl import parse
from research.crawl.parse import SchemaDrift
from research.crawl.sink import CACHE_DB
from research.crawl.sources import foreign_holding_ratio as fhr

#: TPEx 前視汙染邊界:< 此日的 tpex 全是別日快照(內容日期 = 2026-04-24)。
_TPEX_CONTAM_BEFORE = Date(2011, 1, 3)
#: 上游假休市日(兩市場 PG 皆 0 列、原始檔不存在;非 parser 錯,僅註記)。
_MISSING_DAYS = ("2021-08-18", "2025-08-15", "2026-04-29", "2026-05-28")

#: 六數值欄(股數精確、比率容差 1e-9)。company_name 另類比對(name-strip 修正)。
NUM_COLS = ["outstanding_shares", "foreign_remaining_shares", "foreign_held_shares",
            "foreign_remaining_ratio", "foreign_held_ratio", "foreign_limit_ratio"]
_PG_COLS = ["company_code", "company_name", *NUM_COLS]


def _is_bad(market: str, day: Date) -> bool:
    return market == "tpex" and day < _TPEX_CONTAM_BEFORE


def _archive_date(p: Path) -> Date:
    y, m, d = p.stem.split("_")
    return Date(int(y), int(m), int(d))


def _num_eq(a, b) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        return abs(float(a) - float(b)) <= 1e-9
    return a == b


# ---- 對照來源:PG(預設)或 cache(--cache,無 company_name)------------------

def _connect(use_cache: bool):
    if use_cache:
        con = duckdb.connect(CACHE_DB, read_only=True)
        con.execute("CREATE OR REPLACE VIEW _src AS SELECT * FROM foreign_holding_ratio")
        return con, False
    user = os.environ.get("USER", "zaoldyeck")
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH 'host=localhost port=5432 dbname=quantlib user={user}' "
                "AS pg (TYPE postgres, READ_ONLY)")
    con.execute("CREATE OR REPLACE VIEW _src AS SELECT * FROM pg.public.foreign_holding_ratio")
    return con, True


#: --all 全掃時預載(每市場一次查詢,取代逐檔 round-trip);None = 逐檔查詢模式。
_PRELOAD: dict[tuple[str, Date], dict[str, dict]] | None = None


def _src_day(con, has_name: bool, market: str, day: Date) -> dict[str, dict]:
    if _PRELOAD is not None:
        return _PRELOAD.get((market, day), {})
    cols = _PG_COLS if has_name else ["company_code", *NUM_COLS]
    rows = con.execute(
        f"SELECT {','.join(cols)} FROM _src WHERE market = ? AND date = ?",
        [market, day]).fetchall()
    return {r[0]: dict(zip(cols, r)) for r in rows}


def _preload(con, has_name: bool, market: str) -> None:
    """全掃前把整個市場的對照資料一次拉進 `_PRELOAD`(排除 tpex 汙染區)。"""
    global _PRELOAD
    if _PRELOAD is None:
        _PRELOAD = {}
    cols = _PG_COLS if has_name else ["company_code", *NUM_COLS]
    where = "market = ?"
    args: list = [market]
    if market == "tpex":
        where += " AND date >= ?"
        args.append(_TPEX_CONTAM_BEFORE)
    for row in con.execute(
            f"SELECT date,{','.join(cols)} FROM _src WHERE {where} ORDER BY date",
            args).fetchall():
        _PRELOAD.setdefault((market, row[0]), {})[row[1]] = dict(zip(cols, row[1:]))


# ---- 逐檔比對 ---------------------------------------------------------------

def _compare_good(src: dict[str, dict], has_name: bool,
                  df: pl.DataFrame) -> tuple[list[str], int]:
    """好日期:六數值欄逐位相等 + 代碼集合相等;名稱差異中屬 name-strip 修正者計數。"""
    bad: list[str] = []
    py = {r["company_code"]: r for r in df.to_dicts()}
    ps, gs = set(src), set(py)
    if ps != gs:
        bad.append(f"代碼集合不同:PG 多 {sorted(ps - gs)[:5]}… / py 多 {sorted(gs - ps)[:5]}… "
                   f"(PG {len(ps)} vs py {len(gs)})")
    name_fixes = 0
    for code in sorted(ps & gs):
        s, g = src[code], py[code]
        for col in NUM_COLS:
            if not _num_eq(s[col], g[col]):
                bad.append(f"{code}.{col}: PG={s[col]} py={g[col]}")
        if has_name and s["company_name"] != g["company_name"]:
            if parse.clean(s["company_name"]) == parse.clean(g["company_name"]):
                name_fixes += 1                    # name-strip 修正:Python 對、PG 去空白
            else:
                bad.append(f"{code}.company_name: PG={s['company_name']!r} "
                           f"py={g['company_name']!r}(clean 後仍不同)")
        if len(bad) > 30:
            bad.append("…(超過 30 筆,截斷)")
            break
    return bad, name_fixes


def check_file(con, has_name: bool, market: str, p: Path) -> tuple[str, str, int]:
    """回 (狀態, 訊息, 名稱修正數)。狀態 ∈ {OK, OK-BADDATE, FAIL, SKIP}。"""
    fname_date = _archive_date(p)
    raw = p.read_bytes()
    if not raw.strip():
        return "SKIP", f"{market} {fname_date}: 0-byte 哨兵(休市)→ SKIP", 0
    content_date, df = fhr.parse_raw(market, raw)

    if _is_bad(market, fname_date):
        # 前視汙染:port 必須從內容抓到「別日」(2026-04-24;PG/Scala 抓不到)。
        if content_date is not None and content_date != fname_date:
            n = len(_src_day(con, has_name, market, fname_date))
            return ("OK-BADDATE",
                    f"{market} {fname_date}: 內容實為 {content_date} → port 正確識破"
                    f"(PG 誤存 {n} 列於錯日)", 0)
        return ("FAIL",
                f"{market} {fname_date}: 已知汙染日,但 port 內容日期={content_date} 未 ≠ 檔名", 0)

    # 好日期:先鎖「內容日期 == 檔名日期」(稽核未漏列錯日的守護)。
    if df.is_empty():
        src = _src_day(con, has_name, market, fname_date)
        if src:
            return ("FAIL", f"{market} {fname_date}: port 解析 0 列,PG 有 {len(src)} 列", 0)
        return ("SKIP", f"{market} {fname_date}: 雙方皆無資料(只有標題/休市)→ SKIP", 0)
    if content_date != fname_date:
        return ("FAIL",
                f"{market} {fname_date}: 內容日期 {content_date} ≠ 檔名(疑似稽核漏列的錯日)", 0)

    src = _src_day(con, has_name, market, fname_date)
    if not src:
        return ("SKIP", f"{market} {fname_date}: PG 無此日(缺漏)→ SKIP", 0)
    bad, fixes = _compare_good(src, has_name, df)
    if bad:
        return ("FAIL", f"{market} {fname_date}: {len(bad)} 筆不符(共 {df.height} 列):"
                        + "; ".join(bad[:6]), fixes)
    tag = f",{fixes} 筆名稱修正(Python 對/PG 去空白)" if fixes else ""
    return ("OK", f"{market} {fname_date}: {df.height} 列六數值欄逐位一致{tag}", fixes)


# ---- 樣本選取 / 全掃 --------------------------------------------------------

def _all_paths(market: str) -> list[Path]:
    root = paths.RAW / fhr.TABLE / market
    return sorted(p for yd in root.glob("*") if yd.is_dir() for p in yd.glob("*.csv"))


def _sample_paths(market: str, n_good: int = 6, n_bad: int = 4) -> list[Path]:
    """好日期跨年均勻取 n_good;tpex 另取 n_bad 個 2010 汙染日以驗『識破』。"""
    good = sorted(p for p in _all_paths(market)
                  if p.stat().st_size > 0 and not _is_bad(market, _archive_date(p)))
    pick = good[::max(1, len(good) // n_good)][:n_good] if good else []
    bad = sorted(p for p in _all_paths(market)
                 if p.stat().st_size > 0 and _is_bad(market, _archive_date(p)))
    pick += bad[::max(1, len(bad) // n_bad)][:n_bad] if bad else []
    return pick


def run(mode: str) -> tuple[dict[str, int], list[str], int]:
    use_cache = mode == "--cache"
    con, has_name = _connect(use_cache)
    tally = {"OK": 0, "OK-BADDATE": 0, "FAIL": 0, "SKIP": 0}
    fails: list[str] = []
    total_fixes = 0
    print(f"foreign_holding_ratio parity 對照 {'cache(9 欄)' if use_cache else 'PG(11 欄)'};"
          f"模式 {mode or 'sample'}")
    try:
        for market in fhr.MARKETS:
            if mode == "--all":
                _preload(con, has_name, market)
                paths_ = _all_paths(market)
            else:
                paths_ = _sample_paths(market)
            print(f"— {market}:{len(paths_)} 檔")
            for p in paths_:
                status, msg, fixes = check_file(con, has_name, market, p)
                tally[status] += 1
                total_fixes += fixes
                if status == "FAIL":
                    fails.append(msg)
                if mode != "--all":
                    mark = {"OK": "✓", "OK-BADDATE": "◐", "FAIL": "✗", "SKIP": "·"}[status]
                    print(f"  {mark} {msg}")
    finally:
        con.close()
    print(f"\n結果:逐位一致 {tally['OK']}、汙染識破 {tally['OK-BADDATE']}、"
          f"失敗 {tally['FAIL']}、SKIP {tally['SKIP']};名稱修正累計 {total_fixes} 筆")
    return tally, fails, total_fixes


# ---- 定點:int32 溢位 / 表頭守衛 --------------------------------------------

def _check_int64(con, has_name: bool) -> tuple[bool, str]:
    p = paths.RAW / fhr.TABLE / "twse" / "2026" / "2026_7_17.csv"
    if not p.exists():
        return True, "int64 定點:找不到 twse/2026/2026_7_17.csv → SKIP"
    _d, df = fhr.parse_raw("twse", p.read_bytes())
    if df.schema["outstanding_shares"] != pl.Int64:
        return False, f"int64 定點:dtype={df.schema['outstanding_shares']}(應 Int64)"
    val = df.filter(pl.col("company_code") == "2330")["outstanding_shares"][0]
    src = _src_day(con, has_name, "twse", Date(2026, 7, 17)).get("2330")
    pg = src["outstanding_shares"] if src else None
    if val <= 2**31 or (pg is not None and val != pg):
        return False, f"int64 定點:2330 發行股數 port={val} PG={pg}(需 >2^31 且相等)"
    return True, f"int64 定點:2330 發行股數 {val:,} > 2^31,port==PG,無 int32 溢位"


def _check_guards() -> tuple[bool, str]:
    good = ('"115年07月17日 外資及陸資投資持股統計"\n'
            '"證券代號","證券名稱","國際證券編碼","發行股數","外資及陸資尚可投資股數",'
            '"全體外資及陸資持有股數","外資及陸資尚可投資比率","全體外資及陸資持股比率",'
            '"外資及陸資共用法令投資上限比率","陸資法令投資上限比率","x","y"\n'
            '="2330","台積電","TW","1","1","1","1","1","1","0","",""\n')
    drift_twse = good.replace('"發行股數"', '"XXXX"').encode("Big5-HKSCS")
    try:
        fhr.parse_twse(drift_twse)
        return False, "守衛:TWSE 表頭位移未 fail-loud"
    except SchemaDrift:
        pass
    bad_tpex = json.dumps({"tables": [{"date": "115/07/17",
        "fields": ["排行", "代號", "名稱", "發行股數", "B", "持有股數", "D", "XXX", "法令投資上限", "備註"],
        "data": [["1", "8455", "大拓-KY", "1", "1", "1", "0%", "0%", "100%", ""]]}]}).encode("utf-8")
    try:
        fhr.parse_tpex(bad_tpex)
        return False, "守衛:TPEx fields 位移未 fail-loud"
    except SchemaDrift:
        return True, "守衛:TWSE 表頭 / TPEx fields 位移 → SchemaDrift fail-loud"


def main() -> None:
    mode = next((a for a in sys.argv[1:] if a in ("--all", "--cache")), "")
    print(f"排除已知壞日期:tpex < {_TPEX_CONTAM_BEFORE} 前視汙染 319,124 列;"
          f"4 個上游假休市日 {list(_MISSING_DAYS)}(兩市場皆 0 列)")
    tally, fails, _ = run(mode)
    con, has_name = _connect(mode == "--cache")
    try:
        ok_i, msg_i = _check_int64(con, has_name)
    finally:
        con.close()
    ok_g, msg_g = _check_guards()
    print(f"  {'✓' if ok_i else '✗'} {msg_i}")
    print(f"  {'✓' if ok_g else '✗'} {msg_g}")
    if fails or not ok_i or not ok_g:
        for f in fails[:20]:
            print("  ✗", f)
        raise SystemExit(1)


# ---- pytest 入口(離線:讀本機 PG + 封存;PG 不可達或無封存 → skip)----------

def test_foreign_holding_ratio_parity_sample() -> None:
    import pytest

    if not (paths.RAW / fhr.TABLE).exists():
        pytest.skip("無 foreign_holding_ratio 封存原始檔")
    try:
        tally, fails, fixes = run("")
        con, has_name = _connect(False)
        try:
            ok_i, msg_i = _check_int64(con, has_name)
        finally:
            con.close()
    except duckdb.Error as exc:  # PG 不可達
        import pytest
        pytest.skip(f"PG 不可達,parity 需對照 PG:{exc}")
    ok_g, _ = _check_guards()
    assert not fails, f"{len(fails)} 檔 parity 失敗:{fails[:5]}"
    assert tally["OK"] > 0, "無任何逐位一致樣本(對照/封存缺失?)"
    assert tally["OK-BADDATE"] > 0, "未覆蓋任何汙染日(識破邏輯未被驗證)"
    assert fixes > 0, "未觀察到 name-strip 修正(Python 對/PG 錯 未被驗證)"
    assert ok_i, msg_i
    assert ok_g, "表頭/fields 守衛未 fail-loud"


if __name__ == "__main__":
    main()
