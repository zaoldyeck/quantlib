"""income_statement port 忠實度守護:Python parser 對**已封存原始檔**的輸出必須逐位
重現 PostgreSQL `concise_income_statement_progressive`(舊 Scala reader 的產物)。

這是 port parity 的「先紅後綠」證明:PG 由 `FinancialReader.readIncomeStatement` 寫入,
若本 port 的 melt 與 PG 逐 (type,code,title)→value 完全一致,即證明
`income_statement.parse_bytes` == Scala reader(忠實移植)。**不碰任何寫入**——PG 唯讀
對照(DuckDB postgres attach,READ_ONLY)。

涵蓋:twse+tpex × 個別/合併 × 一般業/金融/證券/保險/金控各模板 × IFRS 前後(b_i/b_c/a_c)
× IFRS 過渡年併存。另含三個定點:
- 2832 台產 2011Q4:原始檔 20 值對 19 表頭 → Scala `headers.zip` 截斷 → 基本每股盈餘
  =700633(來源錯位)。port 逐位重現此「錯值」= 忠實搬運(不偷偷修來源錯,對齊 reader 契約)。
- HTML「檔案不存在」空產業模板 chunk → parse 產 0 列不炸(對齊 Scala 安全吞掉)。
- 編碼守衛:UTF-8 供檔可 fallback 解析;真表頭漂移 fail-loud(取代 Scala companyCode.get 裸拋)。

**C-is_progressive_raw 已知壞日期(明確排除,附因)**:見 `_KNOWN_INCOMPLETE`。這些季在
**源頭**(申報期限前凍結原始檔)缺公司,非解析錯——且因 PG 與磁碟原始檔同源(都出自那份
殘檔),raw-vs-PG parity 對「有到的公司」仍逐位一致;缺料在絕對完整性層面,raw-vs-PG
看不到。故本測試證明的是**解析忠實度**,不是源頭完整性(後者是 crawler 排程/update.py 的事)。

Run: uv run --project research python -m research.crawl.tests.test_income_statement_parity
需 PostgreSQL 可達(唯讀)。不需 cache.duckdb。
"""
from __future__ import annotations

import sys

import duckdb

from research import paths
from research.crawl.parse import SchemaDrift
from research.crawl.sources import income_statement as isrc
from research.db import DEFAULT_DSN

_PG_TABLE = "pg.public.concise_income_statement_progressive"

#: strict 逐位 parity 的季度(涵蓋各年代/模板;磁碟或 PG 無此季 → 該市場 SKIP)。
_STRICT_PERIODS = [
    (2025, 1),  # a_c 合併,twse+tpex(現行)
    (2020, 1),  # a_c 合併
    (2018, 3),  # a_c 合併,Q3(非年報)
    (2014, 2),  # IFRS 過渡:a_c 與 b_c/b_i 併存
    (2013, 1),  # IFRS 過渡
    (2010, 1),  # 純舊 GAAP:b_i(個體)+ b_c(合併)兩型
    (2006, 4),  # 早期,合併家數少
]

#: C-is_progressive_raw 列出的源頭殘缺季(排除於「必逐位一致」的絕對完整性判準之外;
#: 仍會被解析,但缺公司屬源頭,非 parser 錯)。
_KNOWN_INCOMPLETE = {
    (2023, 2): "缺 149 家(金控/銀行/KY 於申報期限前凍結原始檔)",
    (2025, 2): "缺 128 家(同上)",
    (2024, 3): "缺 11 家金控",
    (2026, 1): "整季僅 539 家(鄰季約 1900)",
}


def _close(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-6 * max(1.0, abs(a), abs(b))


def _connect_pg() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{DEFAULT_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    return con


def _parse_period(market: str, year: int, quarter: int) -> tuple[dict, list[str]]:
    """glob 該季所有 chunk raw → {(type,code,title): value};回 (map, 值衝突訊息)。"""
    d = paths.RAW / "income_statement" / market / f"{year:04d}"
    got: dict[tuple[str, str, str], float] = {}
    collisions: list[str] = []
    if not d.exists():
        return got, collisions
    for path in sorted(d.glob(f"{year}_{quarter}_*.csv")):
        if not isrc._FNAME_RE.match(path.name):
            continue
        for r in isrc.parse_raw_file(path):
            key = (r["type"], r["company_code"], r["title"])
            if key in got and not _close(got[key], r["value"]):
                collisions.append(f"跨 chunk 值衝突 {key}: {got[key]} vs {r['value']} @ {path.name}")
            got[key] = r["value"]
    return got, collisions


def _pg_period(con, market: str, year: int, quarter: int) -> dict:
    rows = con.execute(
        f"SELECT type, company_code, title, value FROM {_PG_TABLE} "
        "WHERE market = ? AND year = ? AND quarter = ?", [market, year, quarter]).fetchall()
    return {(t, c, ti): v for (t, c, ti, v) in rows}


def _cmp(got: dict, want: dict) -> list[str]:
    msgs: list[str] = []
    gk, wk = set(got), set(want)
    if gk - wk:
        msgs.append(f"parse 多 {len(gk - wk)} 鍵,例:{sorted(gk - wk)[:3]}")
    if wk - gk:
        msgs.append(f"PG 多 {len(wk - gk)} 鍵,例:{sorted(wk - gk)[:3]}")
    nbad = 0
    for k in sorted(gk & wk):
        if not _close(got[k], want[k]):
            if nbad < 8:
                msgs.append(f"{k}: parse={got[k]} PG={want[k]}")
            nbad += 1
    if nbad >= 8:
        msgs.append(f"…共 {nbad} 筆值不符")
    return msgs


def _check_strict(con) -> tuple[int, int, int]:
    ok = skip = fail = 0
    for (year, quarter) in _STRICT_PERIODS:
        for market in isrc.MARKETS:
            got, collisions = _parse_period(market, year, quarter)
            want = _pg_period(con, market, year, quarter)
            tag = f"{market} {year}Q{quarter}"
            if not want and not got:
                print(f"  · {tag}: 磁碟與 PG 皆無 → SKIP")
                skip += 1
                continue
            if not want:
                print(f"  · {tag}: PG 無此季 → SKIP")
                skip += 1
                continue
            if not got:
                print(f"  ✗ {tag}: 磁碟無原始檔,但 PG 有 {len(want)} 筆")
                fail += 1
                continue
            msgs = collisions + _cmp(got, want)
            if msgs:
                print(f"  ✗ {tag}: parse {len(got)} / PG {len(want)} 筆,{len(msgs)} 類差異:")
                for m in msgs[:8]:
                    print(f"       {m}")
                fail += 1
            else:
                print(f"  ✓ {tag}: {len(got)} 筆 (type,code,title)→value 逐位一致")
                ok += 1
    return ok, skip, fail


def _check_2832_source_error(con) -> tuple[int, int, int]:
    """2832 台產 2011Q4:合併報表原始檔 20 值對 19 表頭 → `headers.zip` 截斷,基本每股盈餘
    欄落到 少數股權損益 的值(=700633,源頭錯位)。port 逐位重現此「錯值」= 忠實搬運
    (不偷偷修來源錯,對齊 reader 契約)。以全季 (type,code,title) 對照 PG 之 2832 全部科目。
    """
    market, year, quarter = "twse", 2011, 4
    got, collisions = _parse_period(market, year, quarter)
    if not got:
        print("  · 2832/2011Q4 來源錯位案:磁碟無原始檔 → SKIP")
        return 0, 1, 0
    want = _pg_period(con, market, year, quarter)
    g = {k: v for k, v in got.items() if k[1] == "2832"}
    w = {k: v for k, v in want.items() if k[1] == "2832"}
    eps = g.get(("consolidated", "2832", "基本每股盈餘"))
    if eps != 700633.0:
        print(f"  ✗ 2832/2011Q4: 合併 基本每股盈餘 應忠實重現源頭錯位 700633,實得 {eps}")
        return 0, 0, 1
    bad = [k for k in set(g) & set(w) if not _close(g[k], w[k])]
    coll_2832 = [c for c in collisions if "2832" in c]
    if set(g) != set(w) or bad or coll_2832:
        print(f"  ✗ 2832/2011Q4: parse 與 PG 不一致(鍵差 {set(g) ^ set(w)},值差 {bad},衝突 {coll_2832})")
        return 0, 0, 1
    print(f"  ✓ 2832/2011Q4: 源頭錯位 合併·基本每股盈餘=700633 忠實重現,{len(g)} 科目(含個別/合併)對 PG 全等")
    return 1, 0, 0


def _check_html_swallow() -> tuple[int, int, int]:
    """HTML「檔案不存在」空產業模板 chunk → parse 0 列不炸(對齊 Scala)。"""
    html = paths.RAW / "income_statement" / "tpex" / "2012" / "2012_4_b_c_4.csv"
    if not html.exists() or html.read_bytes()[:1] != b"<":
        print("  · HTML 空 chunk 案:找不到已知 HTML chunk → SKIP")
        return 0, 1, 0
    recs = isrc.parse_raw_file(html)
    if recs:
        print(f"  ✗ HTML chunk: 應產 0 列,實得 {len(recs)} 列")
        return 0, 0, 1
    print("  ✓ HTML 空 chunk: parse 0 列不炸(安全吞掉)")
    return 1, 0, 0


def _check_encoding_guard() -> tuple[int, int, int]:
    """編碼守衛:UTF-8 供檔可 fallback 解析;真表頭漂移 fail-loud(非裸拋)。"""
    ok = fail = 0
    # (A) MOPS 若改 UTF-8 供檔:Big5 解不出「公司代號」→ fallback UTF-8 → 正常解析。
    utf8 = ("出表日期,年度,季別,公司代號,公司名稱,營業收入\n"
            "\"114/06/15\",\"114\",\"1\",\"2330\",\"台積電\",\"100.5\"\n").encode("utf-8")
    recs = isrc.parse_bytes(utf8, "twse", "consolidated", 2025, 1)
    hit = [r["value"] for r in recs if r["company_code"] == "2330" and r["title"] == "營業收入"]
    if hit == [100.5]:
        ok += 1
        print("  ✓ UTF-8 供檔:Big5 解不出 → fallback UTF-8 正常解析(2330 營業收入=100.5)")
    else:
        fail += 1
        print(f"  ✗ UTF-8 fallback 失敗:期望 2330 營業收入=100.5,實得 {hit}")
    # (B) 真表頭漂移(無「公司代號」、非 HTML、兩編碼皆然)→ SchemaDrift fail-loud。
    drift = b"code,name,revenue\n2330,tsmc,100\n"
    try:
        isrc.parse_bytes(drift, "twse", "consolidated", 2025, 1)
        fail += 1
        print("  ✗ 表頭漂移: 應 SchemaDrift fail-loud,卻靜默通過")
    except SchemaDrift:
        ok += 1
        print("  ✓ 表頭漂移: SchemaDrift fail-loud(取代 Scala companyCode.get 裸拋)")
    return ok, 0, fail


def main() -> None:
    print(f"income_statement port parity(PG 唯讀對照:{DEFAULT_DSN})")
    print("已知源頭殘缺季(排除於絕對完整性判準,見 docstring):")
    for (y, q), why in _KNOWN_INCOMPLETE.items():
        print(f"    {y}Q{q}: {why}")
    try:
        con = _connect_pg()
    except Exception as exc:  # noqa: BLE001
        print(f"\n無法連上 PostgreSQL 對照:{type(exc).__name__}: {exc}")
        raise SystemExit(2)
    tot_ok = tot_skip = tot_fail = 0
    try:
        print("\n[strict 逐位 parity]")
        for fn in (_check_strict,):
            o, s, f = fn(con)
            tot_ok, tot_skip, tot_fail = tot_ok + o, tot_skip + s, tot_fail + f
        print("\n[定點:來源錯位忠實重現]")
        o, s, f = _check_2832_source_error(con)
        tot_ok, tot_skip, tot_fail = tot_ok + o, tot_skip + s, tot_fail + f
    finally:
        con.close()
    print("\n[定點:HTML 空 chunk / 編碼守衛(不需 PG)]")
    for o, s, f in (_check_html_swallow(), _check_encoding_guard()):
        tot_ok, tot_skip, tot_fail = tot_ok + o, tot_skip + s, tot_fail + f
    print(f"\n結果:通過 {tot_ok}、SKIP {tot_skip}、失敗 {tot_fail}")
    if tot_fail:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
