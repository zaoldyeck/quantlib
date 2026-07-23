"""sbl_borrowing port 忠實度守護:Python 解析既有封存原始檔(data/sbl_borrowing/
<market>/<year>/YYYY_M_D.csv)必須逐位重現 PG 表 `sbl_borrowing`——除稽核列出的
**已知壞日期**(過期報表汙染)與**已知壞欄**(名稱被 Scala strip 掉內部空白)外。

## 為什麼比對 PG(且 == 比對 cache)

本測試直接 ATTACH PostgreSQL READ_ONLY 逐欄比對(task 明訂「與 PG 該表逐欄比對」;
PG 尚存、且含 cache 投影掉的 `company_name`,才驗得了 name-strip 修正)。稽核
C-sbl_borrowing 已證 cache 與 PG 全史逐位一致(5,116 個 (market,date) 指紋 mismatch=0),
故此結論同時適用 cache。PG 不可達 → SKIP(留 cache-only 環境用 `--cache` 對照 9 欄)。

## parity 語義:parse-of-archive vs PG(parse-of-same-archive)

PG 是 Scala reader 解析**同一批**封存原始檔的產物。本測試以 port 的
`sbl_borrowing.parse_raw` 解析同檔,兩者必須逐位相同——證明 Python parser 對位/
值轉換與 Scala 等價(先紅後綠)。

## 已知壞日期(稽核 A/C-sbl_borrowing,26 個 TWSE 日期,共 26,354 列)

TWT93U 對某些請求回傳**過期報表**,Scala 只認檔名日期 → 把別天數字蓋上錯日期戳。
port 從**內容標題**取日期,故對這 26 檔:斷言 `parse 內容日期 ≠ 檔名日期`
(Python 抓得出、PG/Scala 抓不出 → **Python 對、PG 錯**),不做逐位相等。
其餘所有日期則反向斷言 `內容日期 == 檔名日期`——若稽核漏列了某錯日,這裡會紅。

## 已知壞欄:company_name(name-strip,Python 對 / PG 錯)

Scala `cleanCell` 對每格去空白,打壞含半形空白的 ETF 名(`元大MSCI A股`→
`元大MSCIA股`)。port 只 trim 名稱、保留內部空白。故名稱差異中「PG = Python 去掉
空白/標點後」者判為**預期修正**(Python 對、PG 錯),計數不判失敗;其餘名稱差異
(clean 後仍不同)才是真不符 → FAIL。六個數值欄一律要求逐位相等,無容差。

## 兩個入口:pytest 快速守護 vs 全史 PG parity 腳本

- **pytest(離線、秒級、進 218-test 套件)**:`test_*` 只讀封存原始檔 + parse,鎖住
  port 四個關鍵行為(值對位、name-strip 修正、內容日期識破過期報表、封存先於拒收)。
  不連 PG——避免把重工塞進每次套件跑。
- **`python -m ... [--all|--cache]`(對照 PG,重工、手動跑)**:改 port 後的逐位證據。
  `--all` 掃全部 6,266 檔對 PG 逐欄;預設 sample 取跨年樣本 + 全部壞日;`--cache`
  改對 cache.duckdb(9 欄,無 company_name)。實測(2026-07-23,--all):逐位一致
  5,090 檔、壞日識破 26、失敗 0、SKIP 1,150(=稽核 0-byte 哨兵數)、名稱修正 4,248 筆。

## cache 依賴

腳本讀 PG 當對照(或 `--cache` 讀 cache.duckdb 9 欄)。archive 為封存原始檔,無網路。

Run:
    uv run --project research python -m pytest research/crawl/tests/test_sbl_borrowing_parity.py -q
    uv run --project research python -m research.crawl.tests.test_sbl_borrowing_parity
    uv run --project research python -m research.crawl.tests.test_sbl_borrowing_parity --all
    uv run --project research python -m research.crawl.tests.test_sbl_borrowing_parity --cache
"""
from __future__ import annotations

import os
import sys
from datetime import date as Date
from pathlib import Path

import duckdb
import polars as pl

from research import paths
from research.crawl import parse
from research.crawl.sink import CACHE_DB
from research.crawl.sources import sbl_borrowing as sbl

#: 稽核 A/C-sbl_borrowing 列出的 26 個 TWSE 過期/幽靈日期(內容為別天報表)。
KNOWN_BAD_TWSE_DATES: frozenset[Date] = frozenset(
    Date.fromisoformat(s) for s in (
        "2016-04-08", "2016-08-07", "2016-10-29", "2016-12-20", "2017-01-02",
        "2017-05-15", "2017-06-30", "2017-07-04", "2017-08-06", "2017-09-23",
        "2017-12-14", "2018-01-06", "2018-01-12", "2018-02-19", "2018-04-28",
        "2018-09-06", "2018-10-07", "2018-10-08", "2018-10-13", "2018-12-12",
        "2021-02-12", "2021-05-16", "2021-11-13", "2022-02-01", "2022-06-05",
        "2022-06-19",
    )
)

#: 六個數值欄(逐位相等,無容差)。company_name 另類比對(name-strip 修正)。
NUM_COLS = ["prev_day_balance", "daily_sold", "daily_returned",
            "daily_adjustment", "daily_balance", "next_day_limit"]
_PG_COLS = ["company_code", "company_name", *NUM_COLS]


def _is_bad(market: str, day: Date) -> bool:
    return market == "twse" and day in KNOWN_BAD_TWSE_DATES


def _archive_date(p: Path) -> Date:
    y, m, d = p.stem.split("_")
    return Date(int(y), int(m), int(d))


# ---- 對照來源:PG(預設)或 cache(--cache,無 company_name)------------------

def _connect(use_cache: bool):
    """回 (con, has_name)。PG:ATTACH READ_ONLY,11 欄含 name;cache:9 欄無 name。"""
    con = duckdb.connect()  # in-memory writable;對照庫一律 READ_ONLY attach(不碰其寫入)
    if use_cache:
        con.execute(f"ATTACH '{CACHE_DB}' AS cache (READ_ONLY)")
        con.execute("CREATE OR REPLACE VIEW _src AS SELECT * FROM cache.sbl_borrowing")
        return con, False
    user = os.environ.get("USER", "zaoldyeck")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH 'host=localhost port=5432 dbname=quantlib user={user}' "
                "AS pg (TYPE postgres, READ_ONLY)")
    con.execute("CREATE OR REPLACE VIEW _src AS SELECT * FROM pg.public.sbl_borrowing")
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
    """全掃前把整個市場的 PG 資料一次拉進 `_PRELOAD`,避免 6,266 次 round-trip。"""
    global _PRELOAD
    if _PRELOAD is None:
        _PRELOAD = {}
    cols = _PG_COLS if has_name else ["company_code", *NUM_COLS]
    for row in con.execute(
            f"SELECT date,{','.join(cols)} FROM _src WHERE market = ? "
            "ORDER BY date", [market]).fetchall():
        day = row[0]
        _PRELOAD.setdefault((market, day), {})[row[1]] = dict(zip(cols, row[1:]))


# ---- 逐檔比對 ---------------------------------------------------------------

def _compare_good(market: str, day: Date, src: dict[str, dict], has_name: bool,
                  df: pl.DataFrame) -> tuple[list[str], int]:
    """好日期:六數值欄逐位相等 + 代碼集合相等;名稱差異中屬 name-strip 修正者計數。
    回 (硬失敗訊息, 預期名稱修正數)。"""
    bad: list[str] = []
    py = {r["company_code"]: r for r in df.to_dicts()}
    ps, gs = set(src), set(py)
    if ps != gs:
        only_pg, only_py = sorted(ps - gs)[:5], sorted(gs - ps)[:5]
        bad.append(f"代碼集合不同:PG 多 {only_pg}… / py 多 {only_py}… "
                   f"(PG {len(ps)} vs py {len(gs)})")
    name_fixes = 0
    for code in sorted(ps & gs):
        s, g = src[code], py[code]
        for col in NUM_COLS:
            if s[col] != g[col]:
                bad.append(f"{code}.{col}: PG={s[col]} py={g[col]}")
        if has_name and s["company_name"] != g["company_name"]:
            # name-strip 修正:PG = Python 去空白/標點後 → Python 對、PG 錯。
            if parse.clean(s["company_name"]) == parse.clean(g["company_name"]):
                name_fixes += 1
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
    content_date, df = sbl.parse_raw(market, raw)

    if _is_bad(market, fname_date):
        # 過期報表:port 必須從內容抓到「別天」的日期(PG/Scala 抓不到)。
        if content_date is not None and content_date != fname_date:
            return ("OK-BADDATE",
                    f"{market} {fname_date}: 內容實為 {content_date} → port 正確識破"
                    f"(PG 誤存 {len(_src_day(con, has_name, market, fname_date))} 列於錯日)",
                    0)
        return ("FAIL",
                f"{market} {fname_date}: 已知壞日,但 port 內容日期={content_date} "
                f"未 ≠ 檔名(識破失敗)", 0)

    # 好日期:先鎖「內容日期 == 檔名日期」(稽核未漏列錯日的守護)。
    if df.is_empty():
        src = _src_day(con, has_name, market, fname_date)
        if src:
            return ("FAIL", f"{market} {fname_date}: port 解析 0 列,PG 有 {len(src)} 列", 0)
        return ("SKIP", f"{market} {fname_date}: 雙方皆無資料 → SKIP", 0)
    if content_date != fname_date:
        return ("FAIL",
                f"{market} {fname_date}: 內容日期 {content_date} ≠ 檔名(疑似稽核漏列的錯日)", 0)

    src = _src_day(con, has_name, market, fname_date)
    if not src:
        return ("SKIP", f"{market} {fname_date}: PG 無此日(缺漏)→ SKIP", 0)
    bad, fixes = _compare_good(market, fname_date, src, has_name, df)
    if bad:
        return ("FAIL", f"{market} {fname_date}: {len(bad)} 筆不符(共 {df.height} 列):"
                        + "; ".join(bad[:6]), fixes)
    tag = f",{fixes} 筆名稱修正(Python 對/PG 去空白)" if fixes else ""
    return ("OK", f"{market} {fname_date}: {df.height} 列六數值欄逐位一致{tag}", fixes)


# ---- 樣本選取 / 全掃 --------------------------------------------------------

def _sample_paths(con, has_name: bool, market: str, n_good: int = 6) -> list[Path]:
    """好日期:有 archive、非空、非壞日,跨年均勻取 n_good 個;外加全部壞日(該市場)。"""
    good, bad = [], []
    for p in _all_paths(market):
        if _is_bad(market, _archive_date(p)):
            bad.append(p)
        elif p.stat().st_size > 0:
            good.append(p)
    step = max(1, len(good) // n_good) if good else 1
    return good[::step][:n_good] + sorted(bad)


def _all_paths(market: str) -> list[Path]:
    root = paths.RAW / "sbl_borrowing" / market
    return sorted(p for yd in root.glob("*") if yd.is_dir() for p in yd.glob("*.csv"))


def run(mode: str) -> tuple[dict[str, int], list[str], int]:
    use_cache = mode == "--cache"
    con, has_name = _connect(use_cache)
    tally = {"OK": 0, "OK-BADDATE": 0, "FAIL": 0, "SKIP": 0}
    fails: list[str] = []
    total_fixes = 0
    src_label = "cache(9 欄)" if use_cache else "PG(11 欄)"
    print(f"sbl_borrowing parity 對照 {src_label};模式 {mode or 'sample'}")
    try:
        for market in sbl.MARKETS:
            if mode == "--all":
                _preload(con, has_name, market)
                paths_ = _all_paths(market)
            else:
                paths_ = _sample_paths(con, has_name, market)
            print(f"— {market}:{len(paths_)} 檔")
            for p in paths_:
                status, msg, fixes = check_file(con, has_name, market, p)
                tally[status] += 1
                total_fixes += fixes
                if status == "FAIL":
                    fails.append(msg)
                if mode != "--all":  # 全掃時只印摘要,避免洗版
                    mark = {"OK": "✓", "OK-BADDATE": "◐", "FAIL": "✗", "SKIP": "·"}[status]
                    print(f"  {mark} {msg}")
    finally:
        con.close()
    print(f"\n結果:逐位一致 {tally['OK']}、壞日識破 {tally['OK-BADDATE']}、"
          f"失敗 {tally['FAIL']}、SKIP {tally['SKIP']};名稱修正累計 {total_fixes} 筆")
    return tally, fails, total_fixes


def main() -> None:
    mode = ""
    for a in sys.argv[1:]:
        if a in ("--all", "--cache"):
            mode = a
    _, fails, _ = run(mode)
    if fails:
        print("失敗樣本:")
        for f in fails[:20]:
            print("  ", f)
        raise SystemExit(1)


# ---- pytest 入口:離線快速守護(只讀封存 + parse,不連 PG)--------------------
# 全史逐位 PG parity 是重工(需 PG + 掃 6,266 檔),留給 `python -m ... --all`(每次
# 改 port 手動跑,先紅後綠證據見 module docstring)。此處鎖住 port 的四個關鍵行為,
# 秒級、無網路、可進 218-test 套件:值對位、name-strip 修正、內容日期識破過期報表。

def _archive(market: str, y: int, m: int, d: int) -> Path:
    return (paths.RAW / "sbl_borrowing" / market / f"{y}" / f"{y}_{m}_{d}.csv")


def _need(p: Path):
    import pytest
    if not p.exists() or p.stat().st_size == 0:
        pytest.skip(f"無封存原始檔 {p}")


def test_good_twse_value_alignment_and_content_date() -> None:
    """好日 TWSE:內容日期==檔名、借券區塊(8-13)對位正確(00400A 逐值)。"""
    p = _archive("twse", 2026, 7, 17)
    _need(p)
    cd, df = sbl.parse_twse(p.read_bytes())
    assert cd == Date(2026, 7, 17)
    row = df.filter(pl.col("company_code") == "00400A").to_dicts()[0]
    assert (row["prev_day_balance"], row["daily_sold"], row["daily_returned"],
            row["daily_adjustment"], row["daily_balance"], row["next_day_limit"]) == \
           (20624000, 621000, 0, 0, 21245000, 17765244)


def test_name_strip_fix_preserves_internal_space() -> None:
    """name-strip 修正:含半形空白的 ETF 名保留空白(Scala 會打成無空白)。"""
    pt = _archive("twse", 2026, 7, 17)
    pp = _archive("tpex", 2026, 7, 17)
    _need(pt)
    _need(pp)
    _, dt = sbl.parse_twse(pt.read_bytes())
    _, dp = sbl.parse_tpex(pp.read_bytes())
    assert dt.filter(pl.col("company_code") == "00739")["company_name"][0] == "元大MSCI A股"
    assert dp.filter(pl.col("company_code") == "00890B")["company_name"][0] == "凱基ESG BBB債15+"


def test_stale_report_detected_by_content_date() -> None:
    """稽核 BUG 1:過期報表由內容日期識破——2016_10_29.csv 內容實為 2017-12-18。"""
    p = _archive("twse", 2016, 10, 29)
    _need(p)
    cd, df = sbl.parse_twse(p.read_bytes())
    assert cd == Date(2017, 12, 18) != Date(2016, 10, 29)
    # 汙染值:2330 在此檔 = 真 2017-12-18 的餘額(PG 卻誤存於 2016-10-29)。
    r = df.filter(pl.col("company_code") == "2330").to_dicts()[0]
    assert (r["prev_day_balance"], r["daily_balance"], r["next_day_limit"]) == \
           (9348000, 10557000, 8270071)


def test_fetch_day_rejects_stale_report(monkeypatch) -> None:
    """fetch_day 對過期報表拋 DateMismatch(拒收不靜默插入);且封存在 parse 之前。"""
    import pytest
    p = _archive("twse", 2016, 10, 29)
    _need(p)
    stale = p.read_bytes()
    saved = {}
    monkeypatch.setattr(sbl.http, "fetch_bytes", lambda url, **k: stale)
    monkeypatch.setattr(sbl.archive, "save_raw",
                        lambda *a, **k: saved.setdefault("called", True))
    with pytest.raises(sbl.DateMismatch):
        sbl.fetch_day("twse", Date(2016, 10, 29))
    assert saved.get("called"), "原始檔封存鐵律:save_raw 必須在 parse/拒收之前先落地"


if __name__ == "__main__":
    main()
