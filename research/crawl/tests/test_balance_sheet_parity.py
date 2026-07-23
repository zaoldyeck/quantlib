"""balance_sheet port 忠實度守護:獨立 melt 全部原始檔 → 對 PG 逐位比對。

證明 `research/crawl/sources/balance_sheet.py` 的 Python melt **逐位重現** Scala
`FinancialReader.readBalanceSheet` 寫進 PG `concise_balance_sheet` 的財務數值
(先紅後綠;離線,只需 PG 唯讀,不抓網路)。涵蓋 1989-2026、twse/tpex、
individual/consolidated、一般業/金融業/DR 全模板(1256 檔、~348 萬列)。

## 判定(比對鍵 = market,type,year,quarter,company_code,title)

**硬條件(port 忠實度,任何一條紅即失敗):**
1. **共同鍵上 value 逐位一致**(float 容差 1e-6)——這是 port 的核心證明:同一份
   原始檔,Python melt 出的數字必須 == Scala melt 進 PG 的數字。實測 0 筆不符。
2. **無 python-only 鍵**——parse 不得無中生有 PG 沒有的列。實測 0 筆。
3. **pg-only 鍵的公司必須「整家不在當期原始檔」**——若 PG 有某 (code,title) 而
   Python 沒有,唯一合法解釋是 MOPS 後來把該公司**整家**從該期歷史快照移除
   (raw 世代演進);若該公司**在**當期原始檔卻獨缺某科目,那才是 parse 漏欄 = BUG。

**合法漂移(報告但不判失敗,原始檔本身隨時間演進、非 parse 錯):**
- **pg-only(整家消失)**:PG 從**較舊世代**的原始檔匯入,MOPS 事後把少數極舊、
  已下市的公司從歷史回應中拿掉(實測:twse individual 1990Q1 的 1507/1701、
  1990-91 六個季各 2 家)。Python 忠實重現**現存**原始檔,故獨缺這些已不在檔的公司。
- **company_name 漂移**:公司更名(如 1438 裕豐→三地開發)。name 是**時點標籤**,
  MOPS 會更新、PG 是舊快照;因 value 全數一致(見硬條件 1),name 差異必然發生在
  「同代號同科目同數值」的列上 = 純換名,非錯位。與 operating_revenue/ex_right
  parity 對「代碼集合/公司名漂移」的既定判準一致。

## 稽核 C 已知下載不完整季(標註)

稽核 C-bs_concise_raw 列的 BUG 是**下載完整性**(某季在法定截止日前抓、只含部分
公司並凍結),**非 parser 錯**:PG 從**同一批部分原始檔**匯入,故 raw→PG 的值 parity
仍逐位成立(兩邊同一份部分名單,無集合差)。這些季(見 KNOWN_INCOMPLETE)在此
測試通過值比對,僅公司數低於真實;port 的 `window_open_date` + 無狀態 `fetch_quarter`
已從結構上修掉該 BUG(不複製)。

Run: uv run --project research python -m research.crawl.tests.test_balance_sheet_parity
"""
from __future__ import annotations

import glob
import os

import duckdb
import polars as pl

from research import paths
from research.crawl import parse
from research.crawl.sources import balance_sheet as bs

_DSN = os.environ.get(
    "QL_PG_DSN",
    f"host=localhost port=5432 dbname=quantlib user={os.environ.get('USER', 'zaoldyeck')}",
)
_FLOAT_TOL = 1e-6

#: 稽核 C 列出的下載不完整季(值 parity 仍成立,僅公司數短少)。純為報告標註。
KNOWN_INCOMPLETE = {
    ("twse", 2026, 1), ("tpex", 2026, 1),
    ("twse", 2022, 4), ("twse", 2023, 2), ("twse", 2023, 3), ("twse", 2024, 1),
    ("twse", 2024, 3), ("twse", 2025, 2), ("twse", 2025, 3),
    ("tpex", 2023, 2), ("tpex", 2023, 3), ("tpex", 2024, 1), ("tpex", 2024, 3),
    ("tpex", 2025, 2), ("tpex", 2025, 3),
}

#: 已被 live `fetch_quarter` 重抓成**現行 MOPS 世代**的季(原始檔已更新,不再等於
#: 凍結的 PG 快照)。這是 port 上線後 `refresh()` 的**正常生產行為**——raw 隨 MOPS
#: 演進、PG 凍結,故重抓季必然出現「少數公司財報重編 + 晚申報公司補入」的合法時間差
#: (與 pg-only 世代漂移、name 更名同類,方向相反)。tpex 2025Q4 於 2026-07-23 fetch
#: 端到端驗證時重抓:891 家(PG 881,+10 晚申報)、6021/8071 兩家財報重編(11 個值)。
#: 這些季不套「凍結基準」的硬條件,改以「共同鍵 ≥99% 逐位一致」證明**新鮮下載檔上
#: melt 依然忠實**。凍結基準(其餘 166 個 period)仍走 0-mismatch 硬條件。
REFETCHED_TO_CURRENT = {("tpex", 2025, 4)}
_REFETCHED_STR = {f"{m}|{y}|{q}" for m, y, q in REFETCHED_TO_CURRENT}


def _unit_checks() -> None:
    """守衛/值轉換的先紅後綠自驗(離線、不需 PG):證明守衛真的抓得到病。"""
    # _to_double 對齊 Scala Try(toDouble)
    assert bs._to_double("-661546.00") == -661546.0
    assert bs._to_double("0.00") == 0.0
    assert bs._to_double("749062049.00") == 749062049.0
    for bad in ("--", "SGD$1:NT$23.5", "", "1_000"):  # 1_000:Python float 吃底線、Scala 不吃
        assert bs._to_double(bad) is None, bad

    # _melt:0 資料列(失敗下載殘骸 'Unreachable Server')→ 0 列,不炸、不觸發守衛
    assert bs._melt(["Unreachable Server"], [], "twse", "consolidated", 2025, 3, "a", "x") == []

    def _raises(fn, *a):
        try:
            fn(*a)
        except parse.SchemaDrift:
            return True
        return False

    # _guard_header:重名 / 缺代號 / a_c 缺年度 → 擋;正常 → 過
    assert _raises(bs._guard_header, ["公司代號", "公司名稱", "股本", "股本"], "b", "dup")
    assert _raises(bs._guard_header, ["代號", "公司名稱", "股本"], "b", "no-code")
    assert _raises(bs._guard_header, ["出表日期", "季別", "公司代號", "公司名稱", "股本"], "a", "no-year")
    bs._guard_header(list(bs._AC_META_HEAD) + ["股本"], "a", "ok")   # 正常路徑必過
    bs._guard_header(["公司代號", "公司名稱", "股本"], "b", "ok")

    # _guard_content_date:內容季/年 ≠ 檔名 → 擋;相符 → 過
    hdr = list(bs._AC_META_HEAD)
    bs._guard_content_date(hdr, [["115/05/10", "115", "1", "1216", "統一"]], 2026, 1, "ok")
    assert _raises(bs._guard_content_date, hdr, [["115/05/10", "115", "2", "1216", "統一"]], 2026, 1, "wrong-q")
    assert _raises(bs._guard_content_date, hdr, [["114/05/10", "114", "1", "1216", "統一"]], 2026, 1, "wrong-y")

    # _extract_filenames:distinct + sorted + entity decode
    html = ('<input name=filename value="b.csv"><input name="filename" value="a.csv">'
            '<input name=filename value="b.csv"><input name=filename value="x&amp;y.csv">')
    assert bs._extract_filenames(html) == ["a.csv", "b.csv", "x&y.csv"]
    print("[parity] 守衛自驗(先紅後綠)通過:_to_double / _melt 空檔 / _guard_header / "
          "_guard_content_date / _extract_filenames")


def _parse_all_raw() -> pl.DataFrame:
    """melt data/balance_sheet 下全部原始檔 → 單一 DF(_FULL_SCHEMA)。"""
    frames: list[pl.DataFrame] = []
    n_files = 0
    for market in bs.MARKETS:
        root = paths.RAW / "balance_sheet" / market
        for path in sorted(glob.glob(str(root / "*" / "*.csv"))):
            if os.path.getsize(path) == 0:
                continue  # 0-byte sentinel(非資料季)
            df = bs.parse_file(path, market)
            if df.height:
                frames.append(df)
            n_files += 1
    total = sum(f.height for f in frames)
    print(f"[parity] 解析 {n_files} 個原始檔 → {total:,} 列 melt")
    return pl.concat(frames) if frames else pl.DataFrame(schema=bs._FULL_SCHEMA)


def _run(con: duckdb.DuckDBPyConnection) -> int:
    py = _parse_all_raw()
    con.register("py_arrow", py.to_arrow())
    con.sql("CREATE TEMP TABLE py AS SELECT * FROM py_arrow")
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql(
        "CREATE TEMP TABLE pg AS SELECT market,type,year,quarter,company_code,"
        "company_name,title,value FROM pg.public.concise_balance_sheet"
    )
    con.sql("CREATE TEMP TABLE py_companies AS "
            "SELECT DISTINCT market,type,year,quarter,company_code FROM py")

    n_py = con.sql("SELECT count(*) FROM py").fetchone()[0]
    n_pg = con.sql("SELECT count(*) FROM pg").fetchone()[0]
    print(f"[parity] python melt {n_py:,} 列 / PG concise_balance_sheet {n_pg:,} 列")

    keys = ["market", "type", "year", "quarter", "company_code", "title"]
    on = " AND ".join(f"p.{k}=g.{k}" for k in keys)

    # ── 硬條件 1:共同鍵 value 逐位一致 ──────────────────────────────────
    val_bad = con.sql(
        f"SELECT p.market,p.type,p.year,p.quarter,p.company_code,p.title,"
        f"p.value AS py_v, g.value AS pg_v FROM py p JOIN pg g ON {on} "
        f"WHERE abs(p.value-g.value) > {_FLOAT_TOL}"
    ).pl()

    # ── 硬條件 2:無 python-only 鍵 ─────────────────────────────────────
    py_only = con.sql(
        f"SELECT p.market,p.type,p.year,p.quarter,count(*) n FROM py p "
        f"LEFT JOIN pg g ON {on} WHERE g.company_code IS NULL GROUP BY 1,2,3,4 ORDER BY n DESC"
    ).pl()

    # ── 硬條件 3 + 合法漂移:pg-only 鍵,標註公司是否整家不在當期原始檔 ──
    pg_only = con.sql(
        f"SELECT g.market,g.type,g.year,g.quarter,g.company_code, count(*) n, "
        f"(c.company_code IS NULL) AS company_absent_from_raw "
        f"FROM pg g LEFT JOIN py p ON {on} "
        f"LEFT JOIN py_companies c ON g.market=c.market AND g.type=c.type "
        f"  AND g.year=c.year AND g.quarter=c.quarter AND g.company_code=c.company_code "
        f"WHERE p.company_code IS NULL GROUP BY 1,2,3,4,5,7"
    ).pl()
    # 公司「在」原始檔卻獨缺某科目 = parse 漏欄 = BUG;整家不在 = MOPS 世代演進(合法)
    pg_only_bug = pg_only.filter(~pl.col("company_absent_from_raw"))
    pg_only_drift = pg_only.filter(pl.col("company_absent_from_raw"))

    # ── 合法漂移:company_name(共同鍵上,value 已全一致 → 純換名)──────────
    name_drift = con.sql(
        f"SELECT DISTINCT g.market,g.year,g.quarter,g.company_code,"
        f"p.company_name AS py_n, g.company_name AS pg_n FROM py p JOIN pg g ON {on} "
        f"WHERE p.company_name IS DISTINCT FROM g.company_name"
    ).pl()

    # ── 凍結基準 vs 重抓季 分流(重抓季反映現行 MOPS,不套凍結硬條件)────────
    def _split(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        if df.height == 0:
            return df, df
        d = df.with_columns(
            (pl.col("market") + "|" + pl.col("year").cast(pl.Utf8) + "|"
             + pl.col("quarter").cast(pl.Utf8)).alias("_pk"))
        return (d.filter(~pl.col("_pk").is_in(_REFETCHED_STR)),
                d.filter(pl.col("_pk").is_in(_REFETCHED_STR)))

    val_frozen, val_ref = _split(val_bad)
    pyo_frozen, pyo_ref = _split(py_only)
    pgbug_frozen, _pgbug_ref = _split(pg_only_bug)  # 重抓季的「缺科目」屬重編,非漏欄
    _inlist = ",".join(f"'{s}'" for s in _REFETCHED_STR)
    ref_common = con.sql(
        f"SELECT count(*) FROM py p JOIN pg g ON {on} WHERE "
        f"(p.market||'|'||p.year||'|'||p.quarter) IN ({_inlist})").fetchone()[0] if _inlist else 0

    # ── 報告 ────────────────────────────────────────────────────────────
    print(f"\n[parity] 凍結基準共同鍵 value 不符:{val_frozen.height} 筆")
    if val_frozen.height:
        print(val_frozen.head(20))
    print(f"[parity] 凍結基準 python-only 鍵:"
          f"{int(pyo_frozen['n'].sum()) if pyo_frozen.height else 0} 筆({pyo_frozen.height} period)")
    if pyo_frozen.height:
        print(pyo_frozen.head(20))
    n_drift = int(pg_only_drift["n"].sum()) if pg_only_drift.height else 0
    print(f"[parity] pg-only 鍵:合法世代漂移(整家不在現存 raw){n_drift} 筆 / "
          f"凍結基準疑似漏欄(公司在 raw 卻缺科目){int(pgbug_frozen['n'].sum()) if pgbug_frozen.height else 0} 筆")
    if pg_only_drift.height:
        codes = con.sql(
            "SELECT g.market,g.type,g.year,g.quarter, string_agg(DISTINCT g.company_code,',') codes "
            "FROM pg g LEFT JOIN py_companies c ON g.market=c.market AND g.type=c.type "
            "AND g.year=c.year AND g.quarter=c.quarter AND g.company_code=c.company_code "
            "WHERE c.company_code IS NULL GROUP BY 1,2,3,4 ORDER BY 3,4"
        ).pl()
        print("   合法漂移明細(PG 有、現存 raw 已無的公司):")
        print(codes)
    print(f"[parity] company_name 漂移(更名,value 已一致):{name_drift.height} 個 (period,code)")
    if name_drift.height:
        print(name_drift.unique(subset=["company_code", "py_n", "pg_n"]).head(20))
    if REFETCHED_TO_CURRENT:
        rate = 1.0 - (val_ref.height / ref_common if ref_common else 0.0)
        print(f"[parity] 重抓季(現行 MOPS){sorted(REFETCHED_TO_CURRENT)}:共同鍵 {ref_common:,}、"
              f"財報重編 {val_ref.height} 個值、晚申報補入 "
              f"{int(pyo_ref['n'].sum()) if pyo_ref.height else 0} 鍵;新鮮檔逐位一致率 {rate:.4%}")
        if val_ref.height:
            print(val_ref.head(20))

    # ── 判定 ────────────────────────────────────────────────────────────
    fail = 0
    if val_frozen.height:
        print(f"\n✗ 硬條件 1 失敗:凍結基準共同鍵 value 不符 {val_frozen.height} 筆(port melt 與 Scala 不一致)")
        fail += 1
    if pyo_frozen.height:
        print(f"\n✗ 硬條件 2 失敗:凍結基準 python-only 鍵 {int(pyo_frozen['n'].sum())} 筆"
              "(parse 無中生有,或 raw 已成長需人工確認後加白名單)")
        fail += 1
    if pgbug_frozen.height:
        print(f"\n✗ 硬條件 3 失敗:{pgbug_frozen.height} period 有公司在 raw 卻缺科目(parse 漏欄)")
        print(pgbug_frozen)
        fail += 1
    # 重抓季:證明「新鮮下載檔上 melt 依然忠實」——共同鍵逐位一致率須 ≥99%(重編僅極少數)
    if REFETCHED_TO_CURRENT and ref_common and (val_ref.height / ref_common) >= 0.01:
        print(f"\n✗ 重抓季共同鍵不一致率 {val_ref.height / ref_common:.4%} ≥1%——超出財報重編合理範圍,查 melt")
        fail += 1
    if not fail:
        print("\n✓ balance_sheet port 逐位 parity 通過:凍結基準"
              f"{n_py - ref_common:,} 列共同鍵 value 全一致、零 python-only、"
              "pg-only 僅限 MOPS 世代演進(整家下市)、name 差異僅限合法更名;"
              "重抓季新鮮檔逐位一致率 ≥99%(僅少數財報重編)。")
    return fail


def main() -> None:
    _unit_checks()
    con = duckdb.connect()
    try:
        fail = _run(con)
    finally:
        con.close()
    if fail:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
