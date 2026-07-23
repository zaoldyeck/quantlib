"""insider_holding port 忠實度守護:Python 解析既有封存原始檔
(data/insider_holding/<market>/<year>/YYYY_M_D.html)必須逐位重現 PG 表
`insider_holding`——除稽核列出的 **3 筆已知壞列**(雙方式申報 transfer_shares 被
Scala 字元黏接)外,那 3 筆反向斷言 **Python 對、PG 錯**。

## 為什麼比對 PG(且 == 比對 cache)

直接 ATTACH PostgreSQL READ_ONLY 逐欄比對(task 明訂「與 PG 該表逐欄比對」)。稽核
C-insider_holding 已證 cache 與 PG 全 771 列、15 欄雙向 EXCEPT 零差異,故結論同時
適用 cache。PG 不可達 → SKIP(cache-only 環境用 `--cache` 對照同 15 欄)。

## parity 語義:parse-of-archive vs PG(parse-of-same-archive,先紅後綠)

PG 是 Scala reader 解析**同一批**封存原始檔的產物。本測試以 port 的 `ih.parse_raw`
解析同檔,兩者對「好列」須 15 欄逐位相同——證明對位/值轉換/市場分流/去重/空白語義
與 Scala 等價。**先紅後綠**:若 port 沒修雙方式黏接 bug,那 3 筆的 transfer_shares 會
= PG 的天文數字 → `_TWO_METHOD_FIX` 斷言失敗(紅);修對(加總 = planned_shares_own)
才綠。

## 已知壞列(稽核 C-insider_holding BUG#1):3 筆雙方式申報 transfer_shares

同一張申報同時申報兩種轉讓方式時,cols[7] 是兩個 `<br>` 分隔股數,Scala cleanCell
`.replace(" ","")` 把兩數**字元黏接**成天文數字。3 筆:

    (twse 2007-01-05 2856): PG=57,000,001,000,000  → 修正 6,700,000 (=5,700,000+1,000,000)
    (twse 2026-06-29 6994): PG= 6,560,006,360,000  → 修正 7,016,000 (=  656,000+6,360,000)
    (tpex 2026-05-13 8284): PG=       580,253,000  → 修正   253,580 (=      580+  253,000)

**逐位鐵證**:三筆的修正值恰 = 同列 `planned_shares_own`(來源自己的「預定轉讓總股數
-自有」)。故對這 3 (market,report_date,code):斷言 `py.transfer_shares == 加總 ==
planned_shares_own` 且 `!= PG 的黏接值`(Python 對、PG 錯);其餘 14 欄仍須逐位相同。

## 覆蓋(對齊稽核 C):771 列 = 2007 年 6 交易日(67 列)+ 2026 年 70 交易日(704 列)

中間 2007-01-10 ~ 2026-03-30 的 19 年空窗**無原始檔**(端點反爬待 Playwright,非本 port
範圍),本測試只掃存在的 156 檔、自然覆蓋 771 列全體。3 個漏爬日(twse 2026-04-09、
tpex 2026-04-29 / 2026-05-28)無原始檔亦無 PG 列,對逐位比對無影響,僅註記。

## 定點:int32 溢位 / 表頭守衛

`_check_int64`:六股數欄 dtype Int64;PG 黏接值 5.7e13 遠超 int32(佐證欄必為 bigint);
`_parse_shares("3,000,000,000")==3_000_000_000`(>2^31 無溢位)。
`_check_guards`:缺表頭(有資料卻無『申報日期』)/ 表頭位置位移 → `SchemaDrift` fail-loud。

Run:
    uv run --project research python -m research.crawl.tests.test_insider_holding_parity
    uv run --project research python -m research.crawl.tests.test_insider_holding_parity --cache
    uv run --project research python -m pytest research/crawl/tests/test_insider_holding_parity.py -q
"""
from __future__ import annotations

import os
import sys
from datetime import date as Date
from pathlib import Path

import duckdb
import polars as pl

from research import paths
from research.crawl.parse import SchemaDrift
from research.crawl.sink import CACHE_DB
from research.crawl.sources import insider_holding as ih

#: 3 筆雙方式申報:(market, report_date, code) → (PG 黏接壞值, port 修正值 = planned_shares_own)。
_TWO_METHOD_FIX: dict[tuple[str, Date, str], tuple[int, int]] = {
    ("twse", Date(2007, 1, 5), "2856"): (57_000_001_000_000, 6_700_000),
    ("twse", Date(2026, 6, 29), "6994"): (6_560_006_360_000, 7_016_000),
    ("tpex", Date(2026, 5, 13), "8284"): (580_253_000, 253_580),
}
#: 漏爬日(無原始檔、PG 亦無列;僅註記,不影響逐位比對)。
_MISSING_CRAWL = ("twse 2026-04-09", "tpex 2026-04-29", "tpex 2026-05-28")

_SHARE_COLS = ["transfer_shares", "max_intraday_shares", "current_shares_own",
               "current_shares_trust", "planned_shares_own", "planned_shares_trust"]
#: 去重 / 對位鍵(對齊 Slick 唯一索引):code + reporter_name + method + transferee。
_ROWKEY = ("company_code", "reporter_name", "transfer_method", "transferee")


def _rowkey(rec: dict) -> tuple[str, ...]:
    return tuple(rec[c] for c in _ROWKEY)


# ---- 對照來源:PG(預設)或 cache(--cache)------------------------------------

def _connect(use_cache: bool):
    """In-memory duckdb + ATTACH 對照源 READ_ONLY(預設 catalog 可寫,故建得了 view)。"""
    con = duckdb.connect()
    if use_cache:
        con.execute(f"ATTACH '{CACHE_DB}' AS src_db (READ_ONLY)")
        table = "src_db.insider_holding"
    else:
        user = os.environ.get("USER", "zaoldyeck")
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH 'host=localhost port=5432 dbname=quantlib user={user}' "
                    "AS src_db (TYPE postgres, READ_ONLY)")
        table = "src_db.public.insider_holding"
    con.execute(f"CREATE OR REPLACE VIEW _src AS SELECT {','.join(ih.CACHE_COLS)} FROM {table}")
    return con


def _pg_by_day(con) -> dict[tuple[str, Date], dict[tuple, dict]]:
    """整表(771 列)一次拉進 {(market, report_date): {rowkey: rec}}。"""
    rows = con.execute(f"SELECT {','.join(ih.CACHE_COLS)} FROM _src").fetchall()
    out: dict[tuple[str, Date], dict[tuple, dict]] = {}
    for r in rows:
        rec = dict(zip(ih.CACHE_COLS, r))
        out.setdefault((rec["market"], rec["report_date"]), {})[_rowkey(rec)] = rec
    return out


# ---- 逐檔比對 ---------------------------------------------------------------

def _archive_files() -> list[tuple[str, Path]]:
    out: list[tuple[str, Path]] = []
    for market in ih.MARKETS:
        root = paths.RAW / ih.TABLE / market
        for yd in sorted(p for p in root.glob("*") if p.is_dir()):
            out += [(market, p) for p in sorted(yd.glob("*.html"))]
    return out


def _archive_date(p: Path) -> Date:
    y, m, d = p.stem.split("_")
    return Date(int(y), int(m), int(d))


def check_file(pg_day: dict, market: str, p: Path) -> tuple[str, str, int]:
    """回 (狀態, 訊息, 修正列數)。狀態 ∈ {OK, OK-FIX, FAIL, SKIP}。"""
    rd = _archive_date(p)
    df = ih.parse_raw(market, p.read_bytes(), rd)
    src = pg_day.get((market, rd), {})

    if df.is_empty():
        if src:
            return "FAIL", f"{market} {rd}: port 解析 0 列,PG 有 {len(src)} 列", 0
        return "SKIP", f"{market} {rd}: 雙方皆無申報(休市/查無)→ SKIP", 0

    py = {_rowkey(rec): rec for rec in df.to_dicts()}
    ps, gs = set(src), set(py)
    if ps != gs:
        return ("FAIL", f"{market} {rd}: 列鍵集合不同 PG多={sorted(ps - gs)[:3]} "
                        f"py多={sorted(gs - ps)[:3]}(PG {len(ps)} vs py {len(gs)})", 0)

    bad: list[str] = []
    fixes = 0
    for k in sorted(ps & gs):
        s, g = src[k], py[k]
        marker = (market, rd, g["company_code"])
        allowed: set[str] = set()
        if marker in _TWO_METHOD_FIX:
            glued, fixed = _TWO_METHOD_FIX[marker]
            if g["transfer_shares"] != fixed:
                bad.append(f"{marker}: port transfer_shares={g['transfer_shares']} 應={fixed}"
                           "(加總修正未生效?)")
            elif g["transfer_shares"] != g["planned_shares_own"]:
                bad.append(f"{marker}: 修正值 {fixed} != planned_shares_own "
                           f"{g['planned_shares_own']}(鐵證不成立?)")
            elif s["transfer_shares"] != glued:
                bad.append(f"{marker}: PG transfer_shares={s['transfer_shares']} 應為黏接 {glued}")
            elif s["transfer_shares"] == g["transfer_shares"]:
                bad.append(f"{marker}: Python 未與 PG 分岔(bug 沒被修?)")
            else:
                fixes += 1
            allowed = {"transfer_shares"}  # 已單獨驗證,其餘 14 欄仍須逐位相同
        for col in ih.CACHE_COLS:
            if col in allowed:
                continue
            if s[col] != g[col]:
                bad.append(f"{g['company_code']}.{col}: PG={s[col]!r} py={g[col]!r}")
        if len(bad) > 20:
            bad.append("…(截斷)")
            break

    if bad:
        return "FAIL", f"{market} {rd}({df.height} 列):" + "; ".join(bad[:6]), fixes
    tag = f",{fixes} 筆雙方式修正(Python 對/PG 黏接)" if fixes else ""
    return ("OK-FIX" if fixes else "OK",
            f"{market} {rd}: {df.height} 列 15 欄逐位一致{tag}", fixes)


def run(mode: str) -> tuple[dict[str, int], list[str], int]:
    con = _connect(mode == "--cache")
    try:
        pg_day = _pg_by_day(con)
    finally:
        con.close()
    tally = {"OK": 0, "OK-FIX": 0, "FAIL": 0, "SKIP": 0}
    fails: list[str] = []
    total_fixes = 0
    files = _archive_files()
    print(f"insider_holding parity 對照 {'cache' if mode == '--cache' else 'PG'}(15 欄);"
          f"{len(files)} 檔 / PG {sum(len(v) for v in pg_day.values())} 列")
    for market, p in files:
        status, msg, fixes = check_file(pg_day, market, p)
        tally[status] += 1
        total_fixes += fixes
        if status == "FAIL":
            fails.append(msg)
        if status in ("OK-FIX", "FAIL"):
            print(f"  {'✗' if status == 'FAIL' else '◐'} {msg}")
    print(f"\n結果:逐位一致 {tally['OK']}、含雙方式修正 {tally['OK-FIX']}、"
          f"失敗 {tally['FAIL']}、SKIP {tally['SKIP']};雙方式修正累計 {total_fixes}/3 筆")
    return tally, fails, total_fixes


# ---- 定點:int32 溢位 / 表頭守衛 --------------------------------------------

def _check_int64() -> tuple[bool, str]:
    p = paths.RAW / ih.TABLE / "twse" / "2026" / "2026_6_29.html"
    if not p.exists():
        return True, "int64 定點:找不到 twse/2026/2026_6_29.html → SKIP"
    df = ih.parse_raw("twse", p.read_bytes(), Date(2026, 6, 29))
    for col in _SHARE_COLS:
        if df.schema[col] != pl.Int64:
            return False, f"int64 定點:{col} dtype={df.schema[col]}(應 Int64)"
    # >2^31 無溢位:_parse_shares 純 Python int,且 PG 黏接值 5.7e13 佐證欄必為 bigint。
    if ih._parse_shares("3,000,000,000") != 3_000_000_000:
        return False, "int64 定點:_parse_shares 3,000,000,000 溢位"
    fixed = df.filter(pl.col("company_code") == "6994")["transfer_shares"][0]
    if fixed != 7_016_000:
        return False, f"int64 定點:6994 transfer_shares={fixed}(應 7,016,000 加總修正)"
    return True, "int64 定點:六股數欄 Int64、3e9 無溢位、6994 加總=7,016,000"


def _synthetic(header_repl: tuple[str, str] | None = None,
               drop_top: bool = False) -> bytes:
    """合成一張 18 欄 t56sb12 表(含雙方式列),供表頭守衛負向測試。padding > 1024 bytes。"""
    top = ("<th>異動情形</th><th>申報日期</th><th>公司<br>代號</th><th>公司名稱</th>"
           "<th>申報人身分</th><th>姓名</th><th>預定轉讓方式及股數</th>"
           "<th>每日於盤中交易 最大得轉讓股數</th><th>受讓人</th><th>目前持有股數</th>"
           "<th>預定轉讓總股數</th><th>預定轉讓後持股</th><th>有效轉讓期間</th>"
           "<th>是否申報持股未完成轉讓</th>")
    if drop_top:
        top = top.replace("申報日期", "XXX")
    if header_repl:
        top = top.replace(*header_repl)
    sub = ("<th>轉讓方式</th><th>轉讓股數</th><th>自有持股</th><th>信託</th>"
           "<th>自有持股</th><th>信託</th><th>自有持股</th><th>信託</th>")
    data = ("<td> </td><td>115/06/29</td><td>6994</td><td>富威電力</td><td>董事本人</td>"
            "<td>森崴能源</td><td>一般交易 鉅額逐筆交易</td><td>656,000 6,360,000</td>"
            "<td>104,162</td><td></td><td>22,357,000</td><td>0</td><td>7,016,000</td>"
            "<td>0</td><td>15,341,000</td><td>0</td><td>期間</td><td></td>")
    pad = "<!-- " + "x" * 1200 + " -->"  # 破 parse_raw 的 <1024 bytes 門檻
    html = (f"<html><body>{pad}<table class='hasBorder'>"
            f"<tr class='tblHead'>{top}</tr><tr class='tblHead'>{sub}</tr>"
            f"<tr>{data}</tr></table></body></html>")
    return html.encode("utf-8")


def _check_guards() -> tuple[bool, str]:
    # 正向:合成表解析出 1 列且雙方式加總修正生效(7,016,000)。
    df = ih.parse_raw("twse", _synthetic(), Date(2026, 6, 29))
    if df.height != 1 or df["transfer_shares"][0] != 7_016_000:
        return False, f"守衛正向:合成表 height={df.height} transfer_shares={df['transfer_shares'].to_list()}"
    # 負向 1:有資料卻無『申報日期』表頭 → fail-loud。
    for kind, raw in (("缺表頭", _synthetic(drop_top=True)),
                      ("位置位移", _synthetic(header_repl=("公司名稱", "XXX")))):
        try:
            ih.parse_raw("twse", raw, Date(2026, 6, 29))
            return False, f"守衛:{kind}未 fail-loud"
        except SchemaDrift:
            pass
    return True, "守衛:缺表頭 / 表頭位置位移 → SchemaDrift fail-loud;合成表加總修正生效"


def main() -> None:
    mode = next((a for a in sys.argv[1:] if a in ("--cache",)), "")
    print(f"排除已知壞列:3 筆雙方式申報 transfer_shares(PG 字元黏接 → port 加總修正);"
          f"3 漏爬日 {list(_MISSING_CRAWL)}(無原始檔亦無 PG 列)")
    tally, fails, fixes = run(mode)
    ok_i, msg_i = _check_int64()
    ok_g, msg_g = _check_guards()
    print(f"  {'✓' if ok_i else '✗'} {msg_i}")
    print(f"  {'✓' if ok_g else '✗'} {msg_g}")
    if fails or not ok_i or not ok_g or fixes != 3:
        for f in fails[:20]:
            print("  ✗", f)
        if fixes != 3:
            print(f"  ✗ 雙方式修正只驗到 {fixes}/3 筆(封存原始檔缺?)")
        raise SystemExit(1)


# ---- pytest 入口(離線:讀本機 PG + 封存;PG 不可達或無封存 → skip)----------

def test_insider_holding_parity() -> None:
    import pytest

    if not (paths.RAW / ih.TABLE).exists():
        pytest.skip("無 insider_holding 封存原始檔")
    try:
        tally, fails, fixes = run("")
    except duckdb.Error as exc:  # PG 不可達
        pytest.skip(f"PG 不可達,parity 需對照 PG:{exc}")
    ok_i, msg_i = _check_int64()
    ok_g, msg_g = _check_guards()
    assert not fails, f"{len(fails)} 檔 parity 失敗:{fails[:5]}"
    assert tally["OK"] + tally["OK-FIX"] > 0, "無任何逐位一致樣本(對照/封存缺失?)"
    assert fixes == 3, f"雙方式修正應驗到 3 筆,實得 {fixes}(Python 對/PG 錯 未被完整驗證)"
    assert ok_i, msg_i
    assert ok_g, msg_g


if __name__ == "__main__":
    main()
