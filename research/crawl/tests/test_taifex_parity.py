"""taifex port 忠實度守護:Python 獨立解析既有封存原始檔
(data/taifex/futures_daily/<year>_fut.csv 年檔 + <year>/<year>_<m>.csv 月檔)必須
逐位重現 PG `taifex_futures_daily`(Scala `TradingReader.readTaifexFuturesDaily`
的產物),稽核列出的 **port 修好而 PG 錯的 row 斷言「Python 對、PG 錯」**。

## 對照基準 = PG(唯讀 ATTACH,零寫入)

DuckDB `ATTACH … (TYPE postgres, READ_ONLY)`(與稽核 A/C 腳本、其餘 port 測試同法)。
cache 已由 C-taifex 稽核證實與 PG 逐位相同(5,780,185 列三指紋零差異),取語義最完整
的 PG 為基準。

## parity 方法:同一份 raw 各自獨立重算兩份(皆不呼叫 Scala/module 的對方路徑)

  · **buggy 重演**(本測試自帶,含 A-taifex BUG):價格欄套 `> 0` 過濾,其餘忠實照搬
    Scala 欄位對位 + 值轉換 + 「同鍵取最完整列」dedup(maxBy first-wins)。
  · **fixed 規格** = `taifex.parse_text`(port 模組,價格欄移除 `> 0` 過濾)。

再驗三條:
  (1) **buggy == PG**(逐位、全 19 欄 NULL-safe;鍵集雙向零差)
      → 證「PG 就是 Scala 之產物」且我對 Scala 語義的理解正確(先紅)。
  (2) **fixed 相對 PG 的差異,只可能是「PG 該格 NULL 且 fixed ≤ 0」的價格欄**
      (價差契約的負/零報價被 `>0` 濾掉);任何非價格欄差異、或 fixed>0 卻與 PG 不同,
      即 FAIL → 證修的正是這個 bug、且沒動到別的(後綠)。
  (3) **recovered 量化 == 稽核數字**:價差契約 close 由 NULL 回填 == 573,097 列,
      其中 volume>0 == 287,394 列(A/C-taifex 全庫量化)。

## 稽核壞日期的處理(docs/data_audit/_done/{A,C}-taifex.json)

- **缺口(非汙染,連原始檔都沒有)**:2026-01-02~02-26(缺 2026_1/2.csv)、
  2026-05-22 起(缺 2026_6/7.csv、2026_5.csv 為半月檔到 05-21)。raw 與 PG **同缺**,
  parse 不到、PG 也沒有 → 兩邊皆 0 列,自然不影響逐位比對(屬整合層補抓職責)。
- **無資料汙染列**:本源 PG 乾淨(稽核 C 異常掃描全為 TAIFEX 真實結構);唯一
  來源重複鍵(2013 MTX 201312W4/201401 髒值 histLow=-9)由 dedup 依 Scala 語義
  收斂為 1 列,buggy==PG、fixed 於此列 histLow 由 NULL→-9(價差價格修復,先紅後綠)。

Run:
    uv run --project research python -m research.crawl.tests.test_taifex_parity          # 樣本(快)
    uv run --project research python -m research.crawl.tests.test_taifex_parity --full   # 全 31 檔逐位
"""
from __future__ import annotations

import csv
import io
import sys
from datetime import date as Date

import duckdb
import polars as pl

from research.crawl.sources import taifex
from research.db import DEFAULT_DSN

TABLE = "taifex_futures_daily"
KEYS = ["date", "contract_code", "contract_month", "trading_session"]
#: 受 A-taifex `>0` BUG 影響的 9 個價格欄(fixed 相對 PG 只可能在此差、且方向固定)。
PRICE_COLS = ["open", "high", "low", "close", "settlement_price",
              "best_bid", "best_ask", "historical_high", "historical_low"]
#: 不受該 bug 影響的欄(buggy 與 fixed 皆同;fixed 於此與 PG 必須逐位相等)。
NONPRICE_COLS = ["change", "change_pct", "volume", "open_interest",
                 "trading_halt", "spread_single_volume"]
ALL_COLS = KEYS + PRICE_COLS + NONPRICE_COLS


# --------------------------------------------------------------------------- #
# buggy 重演(獨立於 port 模組;忠實重現 Scala readTaifexFuturesDaily 含 BUG)      #
# --------------------------------------------------------------------------- #
def _b_opt_double(v: str) -> float | None:
    c = v.replace(",", "").replace("%", "").replace(" ", "").strip()
    if c in ("", "-", "--"):
        return None
    try:
        return float(c)
    except ValueError:
        return None


def _b_opt_price(v: str) -> float | None:
    """Scala taifexOptPrice = taifexOptDouble.filter(_ > 0.0) —— **BUG 本體**。"""
    d = _b_opt_double(v)
    return d if (d is not None and d > 0.0) else None


def _b_long(v: str) -> int:
    d = _b_opt_double(v)
    return int(d) if d is not None else 0


def _b_opt_long(v: str) -> int | None:
    d = _b_opt_double(v)
    return int(d) if d is not None else None


def _buggy_df(text: str) -> pl.DataFrame:
    """獨立重演 Scala:價格欄套 `>0` 過濾,其餘忠實照搬;再 Scala 式 dedup。"""
    cols: dict[str, list] = {name: [] for name in taifex._SCHEMA}
    for r in csv.reader(io.StringIO(text)):
        if len(r) < 16 or taifex.parse_date(r[0]) is None:
            continue
        n = len(r)
        cols["date"].append(taifex.parse_date(r[0]))
        cols["contract_code"].append(r[1].strip())
        cols["contract_month"].append(r[2].strip())
        cols["open"].append(_b_opt_price(r[3]))
        cols["high"].append(_b_opt_price(r[4]))
        cols["low"].append(_b_opt_price(r[5]))
        cols["close"].append(_b_opt_price(r[6]))
        cols["change"].append(_b_opt_double(r[7]))
        cols["change_pct"].append(_b_opt_double(r[8]))
        cols["volume"].append(_b_long(r[9]))
        cols["settlement_price"].append(_b_opt_price(r[10]))
        cols["open_interest"].append(_b_opt_long(r[11]))
        cols["best_bid"].append(_b_opt_price(r[12]))
        cols["best_ask"].append(_b_opt_price(r[13]))
        cols["historical_high"].append(_b_opt_price(r[14]))
        cols["historical_low"].append(_b_opt_price(r[15]))
        cols["trading_halt"].append((r[16].strip() or None) if n > 16 else None)
        sess = r[17].strip() if n > 17 else ""
        cols["trading_session"].append(sess if sess else "一般")
        cols["spread_single_volume"].append(_b_opt_long(r[18]) if n > 18 else None)
    return _scala_dedup(pl.DataFrame(cols, schema=taifex._SCHEMA))


def _scala_dedup(df: pl.DataFrame) -> pl.DataFrame:
    """Scala 「同鍵取最完整列」dedup(獨立複本;完整度用 buggy 值算,maxBy first-wins)。"""
    if df.height == 0:
        return df
    score = (
        pl.when(pl.col("settlement_price").is_not_null()).then(1000).otherwise(0)
        + pl.when(pl.col("open_interest").is_not_null()).then(100).otherwise(0)
        + pl.when(pl.col("close").is_not_null()).then(10).otherwise(0)
        + pl.min_horizontal(pl.col("volume"), pl.lit(9, dtype=pl.Int64))
    )
    return (df.with_row_index("_ord").with_columns(score.alias("_score"))
              .sort(["_score", "_ord"], descending=[True, False])
              .unique(subset=KEYS, keep="first", maintain_order=True)
              .drop(["_score", "_ord"]))


# --------------------------------------------------------------------------- #
# 檔案發現 + 讀取                                                                #
# --------------------------------------------------------------------------- #
def _all_files() -> list:
    """年檔 <year>_fut.csv(頂層)+ 月檔 <year>/<year>_<m>.csv;>200B(比照 reader)。"""
    d = taifex._RAW_DIR
    files = [p for p in d.glob("*.csv") if p.stat().st_size > 200]
    files += [p for p in d.glob("*/*.csv") if p.stat().st_size > 200]
    return sorted(files, key=lambda p: p.name)


def _read(path) -> str:
    return path.read_bytes().decode("Big5-HKSCS", errors="replace")


# --------------------------------------------------------------------------- #
# 比對(key-join vs PG;NULL-safe)                                               #
# --------------------------------------------------------------------------- #
def _connect():
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{DEFAULT_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    return con


def _using() -> str:
    return "USING (" + ", ".join(KEYS) + ")"


def _key_set_diffs(con, cand_view: str) -> tuple[int, int]:
    """回 (cand 有但 PG 無, PG 有但 cand 無;PG 限縮在 cand 涵蓋的日期)。"""
    on = " AND ".join(f"c.{k} IS NOT DISTINCT FROM g.{k}" for k in KEYS)
    cand_not_pg = con.execute(
        f"SELECT count(*) FROM {cand_view} c "
        f"LEFT JOIN pg.public.{TABLE} g ON {on} WHERE g.date IS NULL").fetchone()[0]
    pg_not_cand = con.execute(
        f"SELECT count(*) FROM pg.public.{TABLE} g "
        f"JOIN (SELECT DISTINCT date FROM {cand_view}) dd ON g.date = dd.date "
        f"LEFT JOIN {cand_view} c ON {on} WHERE c.date IS NULL").fetchone()[0]
    return cand_not_pg, pg_not_cand


def _cell_mismatches_buggy(con, cand_view: str) -> int:
    """buggy 與 PG 逐格全等否:回 19 欄任一 IS DISTINCT FROM 的列數(期望 0)。"""
    conds = " OR ".join(f"c.{c} IS DISTINCT FROM g.{c}" for c in PRICE_COLS + NONPRICE_COLS)
    return con.execute(
        f"SELECT count(*) FROM {cand_view} c JOIN pg.public.{TABLE} g {_using()} "
        f"WHERE {conds}").fetchone()[0]


def _fixed_unexplained(con, cand_view: str) -> int:
    """fixed 與 PG 的差異中「非修復所能解釋」的列數(期望 0)。

    可解釋 = 非價格欄全等 且 每個價格欄 (相等) 或 (PG NULL 且 fixed ≤ 0)。
    """
    nonprice = " OR ".join(f"c.{c} IS DISTINCT FROM g.{c}" for c in NONPRICE_COLS)
    price = " OR ".join(
        f"(c.{c} IS DISTINCT FROM g.{c} AND NOT (g.{c} IS NULL AND c.{c} <= 0))"
        for c in PRICE_COLS)
    return con.execute(
        f"SELECT count(*) FROM {cand_view} c JOIN pg.public.{TABLE} g {_using()} "
        f"WHERE ({nonprice}) OR ({price})").fetchone()[0]


def _recovered_counts(con, cand_view: str) -> tuple[int, int]:
    """fixed 由 PG-NULL 回填 close 的價差契約列數(全部 / volume>0)。"""
    base = (f"FROM {cand_view} c JOIN pg.public.{TABLE} g {_using()} "
            f"WHERE c.contract_month LIKE '%/%' AND g.close IS NULL AND c.close IS NOT NULL")
    total = con.execute(f"SELECT count(*) {base}").fetchone()[0]
    vol = con.execute(f"SELECT count(*) {base} AND c.volume > 0").fetchone()[0]
    return total, vol


def _check(con, buggy: pl.DataFrame, fixed: pl.DataFrame) -> tuple[bool, list[str], tuple]:
    """對一組(可跨多檔)的 buggy/fixed DF 做三條驗證。回 (ok, 訊息, recovered)。"""
    msgs: list[str] = []
    ok = True
    con.register("_buggy", buggy)
    con.register("_fixed", fixed)
    try:
        # (1) buggy == PG:鍵集雙向零差 + 逐格全等
        b_np, p_nb = _key_set_diffs(con, "_buggy")
        mm = _cell_mismatches_buggy(con, "_buggy")
        if b_np or p_nb or mm:
            ok = False
            msgs.append(f"✗ buggy≠PG:buggy-only鍵 {b_np}、PG-only鍵 {p_nb}、逐格不符 {mm}")
        else:
            msgs.append(f"✓ buggy==PG:{buggy.height} 列鍵集雙向零差 + 19 欄逐格全等")
        # (2) fixed 差異只可能是修復(PG NULL 且 fixed≤0 的價格欄)
        f_np, pf_nb = _key_set_diffs(con, "_fixed")
        unexp = _fixed_unexplained(con, "_fixed")
        if f_np or pf_nb or unexp:
            ok = False
            msgs.append(f"✗ fixed 有非修復差異:fixed-only鍵 {f_np}、PG-only鍵 {pf_nb}、"
                        f"無法以修復解釋的列 {unexp}")
        else:
            msgs.append("✓ fixed 相對 PG 僅『PG NULL 且值≤0 的價格欄』差異(非價格欄逐位相等)")
        # (3) recovered 量化
        rec_total, rec_vol = _recovered_counts(con, "_fixed")
        msgs.append(f"  · 價差契約 close 由 NULL 回填:{rec_total} 列(其中 volume>0:{rec_vol})")
        return ok, msgs, (rec_total, rec_vol)
    finally:
        con.unregister("_buggy")
        con.unregister("_fixed")


# --------------------------------------------------------------------------- #
# 先紅後綠錨(具體 row:port 對、PG 錯)                                          #
# --------------------------------------------------------------------------- #
def _anchor_checks(con) -> tuple[bool, list[str]]:
    """具體 row 級斷言:證單式忠實、價差負價回填、dedup 收斂。"""
    ok = True
    out: list[str] = []
    d13 = taifex.parse_text(_read(taifex._RAW_DIR / "2013_fut.csv"))
    d98 = taifex.parse_text(_read(taifex._RAW_DIR / "1998_fut.csv"))

    def one(df, code, month, day) -> dict:
        r = df.filter((pl.col("contract_code") == code)
                      & (pl.col("contract_month") == month)
                      & (pl.col("date") == day))
        return r.to_dicts()[0] if r.height == 1 else {}

    def pg_one(code, month, day) -> dict:
        rows = con.execute(
            f"SELECT {', '.join(ALL_COLS)} FROM pg.public.{TABLE} "
            f"WHERE contract_code=? AND contract_month=? AND date=?",
            [code, month, day]).pl().to_dicts()
        return rows[0] if len(rows) == 1 else {}

    # 錨 A:單式 TX 199809(史上第一筆)—— port 與 PG 逐位相等(fix no-op)。
    a_port = one(d98, "TX", "199809", Date(1998, 7, 21))
    a_pg = pg_one("TX", "199809", Date(1998, 7, 21))
    if a_port and a_pg and all(a_port[c] == a_pg[c] for c in ALL_COLS):
        out.append("✓ [單式] TX 199809 1998-07-21:port 與 PG 逐位相等(fix 對單式 no-op)")
    else:
        ok = False
        out.append(f"✗ [單式] TX 199809:port={a_port} PG={a_pg}")

    # 錨 B:價差 CBF 201301/201302(負價被 PG 濾成 NULL)—— port 回填負值。
    b = one(d13, "CBF", "201301/201302", Date(2013, 1, 2))
    b_pg = pg_one("CBF", "201301/201302", Date(2013, 1, 2))
    exp = dict(open=-0.03, high=-0.03, low=-0.05, close=-0.05, best_bid=-0.06,
               best_ask=-0.04, historical_high=-0.02, historical_low=-0.05)
    port_ok = b and all(b[k] == v for k, v in exp.items()) and b["volume"] == 6
    pg_null = b_pg and all(b_pg[k] is None for k in exp) and b_pg["volume"] == 6
    if port_ok and pg_null:
        out.append("✓ [價差·先紅後綠] CBF 201301/201302 2013-01-02:port 回填 open/high/low/"
                   "close/bid/ask/histH/histL = 負值,PG 全 NULL(只剩 volume=6)")
    else:
        ok = False
        out.append(f"✗ [價差] CBF:port_ok={port_ok} pg_null={pg_null} port={b} PG={b_pg}")

    # 錨 C:dedup 收斂 + 價差負 histLow 回填(來源重複鍵,Scala first-wins)。
    c = one(d13, "MTX", "201312W4/201401", Date(2013, 12, 25))
    c_pg = pg_one("MTX", "201312W4/201401", Date(2013, 12, 25))
    if (c and c_pg and c["close"] == 29.0 and c["volume"] == 29
            and c["historical_low"] == -9.0 and c_pg["historical_low"] is None
            and c_pg["close"] == 29.0):
        out.append("✓ [dedup·先紅後綠] MTX 201312W4/201401 2013-12-25:2 列來源重複鍵收斂為 1"
                   "(取 close=29 那列),histLow port=-9 / PG=NULL(負價修復)")
    else:
        ok = False
        out.append(f"✗ [dedup] MTX:port={c} PG={c_pg}")
    return ok, out


# --------------------------------------------------------------------------- #
def _run(con, paths_) -> tuple[bool, tuple[int, int]]:
    """對給定檔案列表:各檔 parse buggy/fixed → concat → 三條驗證 + 錨。

    回 (ok, recovered=(價差 close 回填總數, 其中 volume>0))。
    """
    empty = pl.DataFrame(schema=taifex._SCHEMA)
    buggy = pl.concat([_buggy_df(_read(p)) for p in paths_]) if paths_ else empty
    fixed = pl.concat([taifex.parse_text(_read(p)) for p in paths_]) if paths_ else empty
    ok, msgs, rec = _check(con, buggy, fixed)
    for m in msgs:
        print("  " + m)
    a_ok, a_msgs = _anchor_checks(con)
    print("  --- 先紅後綠錨(具體 row)---")
    for m in a_msgs:
        print("  " + m)
    return (ok and a_ok), rec


def main() -> None:
    args = sys.argv[1:]
    full = "--full" in args
    con = _connect()
    try:
        files = _all_files()
        if not full:
            # 樣本:1998(單式最舊)+ 2013(含價差 BUG + dedup 碰撞)+ 2026 月檔(近期)。
            want = {"1998_fut.csv", "2013_fut.csv"}
            files = [p for p in files if p.name in want or p.parent.name == "2026"]
        print(f"taifex parity {'FULL' if full else '樣本'}(PG: {DEFAULT_DSN});{len(files)} 檔")
        ok, (tot, vol) = _run(con, files)
        if full:
            # 全庫 recovered 必須命中稽核數字(A/C-taifex:573,097 / 287,394)。
            hit = (tot == 573097 and vol == 287394)
            print(f"\n[全庫 recovered] 價差 close 由 NULL 回填 {tot}(期望 573097)、"
                  f"volume>0 {vol}(期望 287394)—— {'✓ 命中稽核' if hit else '✗ 不符'}")
            ok = ok and hit
    finally:
        con.close()
    print(f"\n結果:{'全數通過 ✓' if ok else '有失敗 ✗'}")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
