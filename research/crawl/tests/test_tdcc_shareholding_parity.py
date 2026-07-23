"""tdcc_shareholding port 忠實度守護:Python 解析既有封存原始檔(data/tdcc_shareholding/
weekly/YYYY/YYYY_M_D.csv)必須逐位重現 PG 表 `tdcc_shareholding`。

## 為什麼此源要求「六欄全部逐位一致」(不像 sbl 有 Python-對/PG-錯 的欄)

稽核 A-tdcc / C-tdcc **皆 verdict=OK**:對 12 個資料日、813,110 列、六欄逐格比對,
數值/列數/單位/正負號/日期/代號空白全對(A-tdcc RESULT PASS、C-tdcc 全量指紋 mismatch=0)。
故本 port 是**忠實 1:1 重現**——**不存在任何「Python 修好而 PG 錯」的欄**(對照 sbl 的
name-strip、dtd 的自營商錯位)。所以本測試對六欄一律要求逐位相等、無容差;若哪天出現不符,
那就是 port 引入了漂移,直接紅燈。

## 為什麼比對 PG(且 == 比對 cache)

直接 ATTACH PostgreSQL READ_ONLY 逐欄比對(task 明訂「與 PG 該表逐欄比對」)。稽核
C-tdcc 已證 cache 與 PG 全量逐位一致(12 個 data_date 指紋 mismatch=0),故結論同時適用
cache;PG 不可達 → 用 `--cache` 對照 cache.duckdb(同 6 欄)。

## parity 語義:parse-of-archive vs PG(parse-of-same-archive)

PG 是 Scala reader 解析**同一批**封存原始檔的產物。本測試以 port 的 `tdcc.parse_raw`
解析同檔,兩者必須逐位相同——證明 Python parser 對位/值轉換與 Scala 等價(先紅後綠)。
同一資料日的多個下載檔(週頻:endpoint 只給當週,一週被下載多次)各自比對 PG,順帶
驗證同週多檔一致。

## 已知缺漏日期(稽核 C-tdcc SUSPECT,非 port 可解;明確排除並註明)

已收集區間 [2026-04-17, 2026-07-17] 依交易日曆應有 14 個週快照,實得 12——缺
**2026-04-30、2026-05-29** 兩個真交易週(爬蟲沒跑到、快照被下一週覆蓋、標準 endpoint
補不回,Task #20)。這兩日在 **raw 內容日期集合與 PG 皆不存在**,故不進逐位比對;
`run()` 額外斷言:(a) PG data_date 集合 == 12 個預期、(b) 兩缺漏日確實不在 PG、
(c) 兩缺漏日確實不在任何 raw 檔的內容日期——把「週頻漏抓」從無聲變成可觀測(回補後恆綠)。

## 兩個入口:pytest 快速守護 vs 全史 PG parity 腳本

- **pytest(離線、秒級、進套件)**:`test_*` 只讀封存原始檔 + parse,鎖住 port 關鍵行為
  (六欄對位、內容日期非檔名、num_shares Int64 不溢位、代號去空白、檔內去重、標頭守衛
  fail-loud、封存先於 parse)。不連 PG。
- **`python -m ... [--all|--cache]`(對照 PG,重工、手動跑)**:改 port 後的逐位證據。
  `--all` 掃全部封存檔對 PG 逐欄;預設 sample 取每個內容日一個代表檔;`--cache` 改對
  cache.duckdb。

## cache 依賴

腳本讀 PG(或 `--cache` 讀 cache.duckdb);archive 為封存原始檔,無網路。

Run:
    uv run --project research python -m pytest research/crawl/tests/test_tdcc_shareholding_parity.py -q
    uv run --project research python -m research.crawl.tests.test_tdcc_shareholding_parity
    uv run --project research python -m research.crawl.tests.test_tdcc_shareholding_parity --all
    uv run --project research python -m research.crawl.tests.test_tdcc_shareholding_parity --cache
"""
from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import date as Date
from pathlib import Path

import duckdb
import polars as pl

from research import paths
from research.crawl import parse
from research.crawl.sink import CACHE_DB
from research.crawl.sources import tdcc_shareholding as tdcc

#: 已收集區間內應有的 12 個資料日(稽核 A/C-tdcc)。
EXPECTED_DATES: frozenset[Date] = frozenset(
    Date.fromisoformat(s) for s in (
        "2026-04-17", "2026-04-24", "2026-05-08", "2026-05-15", "2026-05-22",
        "2026-06-05", "2026-06-12", "2026-06-18", "2026-06-26", "2026-07-03",
        "2026-07-09", "2026-07-17",
    )
)
#: 稽核 C-tdcc 列出的兩個來源端漏收週(真交易日,標準 endpoint 補不回,Task #20)。
KNOWN_GAP_DATES: frozenset[Date] = frozenset(
    (Date(2026, 4, 30), Date(2026, 5, 29)))

#: 三個數值欄(逐位相等,無容差)。data_date/company_code/holding_tier 為列 key。
NUM_COLS = ["num_holders", "num_shares", "pct_of_outstanding"]
_RAW_DIR = paths.RAW / "tdcc_shareholding" / "weekly"


# ---- 對照來源:PG(預設)或 cache(--cache)-----------------------------------

def _connect(use_cache: bool):
    """回 DuckDB con(對照庫一律 READ_ONLY attach,不碰其寫入);view `_src` 統一 6 欄。"""
    con = duckdb.connect()  # in-memory writable;對照庫 READ_ONLY
    if use_cache:
        con.execute(f"ATTACH '{CACHE_DB}' AS cache (READ_ONLY)")
        con.execute("CREATE OR REPLACE VIEW _src AS SELECT data_date, company_code, "
                    "holding_tier, num_holders, num_shares, pct_of_outstanding "
                    "FROM cache.tdcc_shareholding")
        return con
    user = os.environ.get("USER", "zaoldyeck")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH 'host=localhost port=5432 dbname=quantlib user={user}' "
                "AS pg (TYPE postgres, READ_ONLY)")
    con.execute("CREATE OR REPLACE VIEW _src AS SELECT data_date, company_code, "
                "holding_tier, num_holders, num_shares, pct_of_outstanding "
                "FROM pg.public.tdcc_shareholding")
    return con


def _load_src(con) -> dict[Date, dict[tuple[str, int], tuple]]:
    """整表一次拉進 {data_date: {(code, tier): (holders, shares, pct)}}(避免逐檔 round-trip)。"""
    out: dict[Date, dict[tuple[str, int], tuple]] = defaultdict(dict)
    for dd, code, tier, holders, shares, pct in con.execute(
            "SELECT data_date, company_code, holding_tier, num_holders, num_shares, "
            "pct_of_outstanding FROM _src").fetchall():
        out[dd][(code, int(tier))] = (int(holders), int(shares), float(pct))
    return out


# ---- 逐檔比對 ---------------------------------------------------------------

def _compare(day: Date, src_day: dict[tuple[str, int], tuple],
             df: pl.DataFrame) -> list[str]:
    """六欄逐位:列 key 集合(code,tier)須相等 + 三數值欄逐位相等。回硬失敗訊息 list。"""
    bad: list[str] = []
    py = {(r["company_code"], r["holding_tier"]):
          (r["num_holders"], r["num_shares"], r["pct_of_outstanding"])
          for r in df.to_dicts()}
    ps, gs = set(src_day), set(py)
    if ps != gs:
        only_pg, only_py = sorted(ps - gs)[:5], sorted(gs - ps)[:5]
        bad.append(f"列 key 集合不同:PG 多 {only_pg}… / py 多 {only_py}… "
                   f"(PG {len(ps)} vs py {len(gs)})")
    for k in sorted(ps & gs):
        s, g = src_day[k], py[k]
        if s != g:
            bad.append(f"{k}: PG={s} py={g}")
        if len(bad) > 30:
            bad.append("…(超過 30 筆,截斷)")
            break
    return bad


def _all_paths() -> list[Path]:
    return sorted(p for yd in _RAW_DIR.glob("*") if yd.is_dir()
                  for p in yd.glob("*.csv"))


def check_file(src: dict[Date, dict], p: Path) -> tuple[str, str]:
    """回 (狀態, 訊息)。狀態 ∈ {OK, FAIL, SKIP}。"""
    raw = p.read_bytes()
    if not raw.strip():
        return "SKIP", f"{p.name}: 0-byte(空檔)→ SKIP"
    content_date, df = tdcc.parse_raw(raw)
    if df.is_empty():
        return "SKIP", f"{p.name}: 解析 0 列 → SKIP"
    if content_date in KNOWN_GAP_DATES:  # 理論上不會發生(raw 無此內容日),防呆
        return "SKIP", f"{p.name}: 內容日 {content_date} 為已知缺漏週 → SKIP"
    src_day = src.get(content_date)
    if not src_day:
        return "FAIL", f"{p.name}: 內容日 {content_date} 在對照庫查無(port 解析出但 PG 缺)"
    bad = _compare(content_date, src_day, df)
    if bad:
        return "FAIL", (f"{p.name}(內容 {content_date}):{len(bad)} 筆不符"
                        f"(共 {df.height} 列):" + "; ".join(bad[:6]))
    return "OK", f"{p.name}(內容 {content_date}):{df.height} 列六欄逐位一致"


def run(mode: str) -> tuple[dict[str, int], list[str]]:
    use_cache = mode == "--cache"
    con = _connect(use_cache)
    src = _load_src(con)
    src_label = "cache" if use_cache else "PG"
    print(f"tdcc_shareholding parity 對照 {src_label};模式 {mode or 'sample'}")

    # 缺漏週守護:兩個已知缺漏日確實不在對照庫,也不在任何 raw 內容日。
    src_dates = set(src)
    raw_content_dates = set()
    for p in _all_paths():
        if p.stat().st_size == 0:
            continue
        cd, df = tdcc.parse_raw(p.read_bytes())
        if not df.is_empty():
            raw_content_dates.add(cd)
    assert src_dates == set(EXPECTED_DATES), (
        f"對照庫 data_date 集合 {sorted(map(str, src_dates))} != 預期 12 個")
    assert not (KNOWN_GAP_DATES & src_dates), "已知缺漏週竟出現在對照庫(回補了?更新測試)"
    assert not (KNOWN_GAP_DATES & raw_content_dates), "已知缺漏週竟出現在 raw 內容日"
    print(f"  缺漏週守護 OK:對照庫 12 日齊、{sorted(str(d) for d in KNOWN_GAP_DATES)} "
          f"兩缺漏日確實不在 PG 也不在 raw(Task #20 回補後此段轉綠)")

    if mode == "--all":
        paths_ = _all_paths()
    else:  # sample:每個內容日取第一個檔(byte-identical 群組任一皆可)
        by_date: dict[Date, Path] = {}
        for p in _all_paths():
            if p.stat().st_size == 0:
                continue
            cd, df = tdcc.parse_raw(p.read_bytes())
            by_date.setdefault(cd, p)
        paths_ = [by_date[d] for d in sorted(by_date)]

    tally = {"OK": 0, "FAIL": 0, "SKIP": 0}
    fails: list[str] = []
    print(f"— {len(paths_)} 檔")
    for p in paths_:
        status, msg = check_file(src, p)
        tally[status] += 1
        if status == "FAIL":
            fails.append(msg)
        if mode != "--all":
            mark = {"OK": "✓", "FAIL": "✗", "SKIP": "·"}[status]
            print(f"  {mark} {msg}")
    con.close()
    print(f"\n結果:逐位一致 {tally['OK']}、失敗 {tally['FAIL']}、SKIP {tally['SKIP']}")
    return tally, fails


def main() -> None:
    mode = ""
    for a in sys.argv[1:]:
        if a in ("--all", "--cache"):
            mode = a
    _, fails = run(mode)
    if fails:
        print("失敗樣本:")
        for f in fails[:20]:
            print("  ", f)
        raise SystemExit(1)


# ---- pytest 入口:離線快速守護(只讀封存 + parse,不連 PG)--------------------

def _archive(y: int, m: int, d: int) -> Path:
    return _RAW_DIR / f"{y}" / f"{y}_{m}_{d}.csv"


def _need(p: Path):
    import pytest
    if not p.exists() or p.stat().st_size == 0:
        pytest.skip(f"無封存原始檔 {p}")


def test_six_column_alignment_2330() -> None:
    """六欄對位:2330(2026-04-17,檔名 4_24)四個分級的每一格逐值。"""
    p = _archive(2026, 4, 24)
    _need(p)
    _, df = tdcc.parse_raw(p.read_bytes())
    got = {r["holding_tier"]: (r["num_holders"], r["num_shares"], r["pct_of_outstanding"])
           for r in df.filter(pl.col("company_code") == "2330").to_dicts()}
    assert got[1] == (1952075, 238348799, 0.91)       # 零股級距
    assert got[15] == (1502, 22222513229, 85.69)      # 千張大戶
    assert got[16] == (3, 1027, 0.00)                 # 差異數(獨立對帳列)
    assert got[17] == (2449774, 25932525515, 100.00)  # 合計


def test_num_shares_int64_no_overflow() -> None:
    """num_shares 必須 Int64:2330 合計 25.9 億張(> int32 上限 21.4 億),int32 會溢位。"""
    p = _archive(2026, 4, 24)
    _need(p)
    _, df = tdcc.parse_raw(p.read_bytes())
    assert df.schema["num_shares"] == pl.Int64
    v = df.filter((pl.col("company_code") == "2330") & (pl.col("holding_tier") == 17))
    assert v["num_shares"][0] == 25932525515 > 2_147_483_647


def test_data_date_from_content_not_filename() -> None:
    """日期取自內容『資料日期』欄而非檔名:2026_4_24.csv 內容實為 2026-04-17。"""
    p = _archive(2026, 4, 24)
    _need(p)
    cd, df = tdcc.parse_raw(p.read_bytes())
    assert cd == Date(2026, 4, 17) != Date(2026, 4, 24)
    assert df["data_date"].unique().to_list() == [Date(2026, 4, 17)]


def test_company_code_trailing_space_stripped() -> None:
    """代號尾端補空白('2330  ')被 .strip 去掉,對 daily_quote(代號 2330)join 不會斷。"""
    p = _archive(2026, 4, 24)
    _need(p)
    _, df = tdcc.parse_raw(p.read_bytes())
    codes = df["company_code"].to_list()
    assert "2330" in codes
    assert all(c == c.strip() and " " not in c for c in codes)


def test_distinct_and_internal_identity() -> None:
    """檔內去重(date,code,tier)零重複;且合計(tier17)== sum(tier1..15)(稽核 C 修正的恆等式,
    差異數 tier16 為獨立對帳列不計入)——columns 未錯位的結構性守衛。"""
    p = _archive(2026, 4, 24)
    _need(p)
    _, df = tdcc.parse_raw(p.read_bytes())
    assert df.height == df.select(["data_date", "company_code", "holding_tier"]).n_unique()
    r = df.filter(pl.col("company_code") == "2330")
    s15 = r.filter(pl.col("holding_tier").is_between(1, 15))["num_holders"].sum()
    t17 = r.filter(pl.col("holding_tier") == 17)["num_holders"][0]
    assert s15 == t17  # 人數:各級距(1..15)加總 == 合計


def test_header_guard_fail_loud_on_drift() -> None:
    """標頭欄位位移 → parse.SchemaDrift fail-loud(不靜默錯位;比照 daily_quote._guard)。"""
    import pytest
    good = ("資料日期,證券代號,持股分級,人數,股數,占集保庫存數比例%\r\n"
            "20260417,2330,17,1,2,3.00\r\n").encode("utf-8-sig")
    _, df = tdcc.parse_raw(good)  # 正常標頭:過
    assert df.height == 1
    bad = ("資料日期,證券代號,持股分級,人數,股數,錯欄名\r\n"
           "20260417,2330,17,1,2,3.00\r\n").encode("utf-8-sig")
    with pytest.raises(parse.SchemaDrift):
        tdcc.parse_raw(bad)


def test_fetch_latest_archives_before_parse(monkeypatch) -> None:
    """原始檔封存鐵律:fetch_latest 先 save_raw 才 parse;回傳 DF 形態正確。"""
    p = _archive(2026, 4, 24)
    _need(p)
    raw = p.read_bytes()
    order: list[str] = []
    monkeypatch.setattr(tdcc.http, "fetch_bytes",
                        lambda url, **k: (order.append("fetch"), raw)[1])
    monkeypatch.setattr(tdcc.archive, "save_raw",
                        lambda *a, **k: order.append("save"))
    df = tdcc.fetch_latest(Date(2026, 4, 24))
    assert order == ["fetch", "save"], "save_raw 必須在 parse 之前(封存鐵律)"
    assert df is not None and df.height == 67252
    assert df["data_date"].unique().to_list() == [Date(2026, 4, 17)]


if __name__ == "__main__":
    main()
