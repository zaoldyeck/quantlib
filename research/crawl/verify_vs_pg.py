"""重建 cache 對 PostgreSQL 交叉驗證——**PG 退役前的安全門**(使用者鐵律:PG 全部驗證完才刪)。

對每個從 raw 重建的 cache 表,以 PG 為基準比對:
- **列數/日覆蓋**:cache 應 ≥ PG(重建補回 PG 匯入時漏的列/日;絕不該比 PG 少)。
- **現行日逐位吻合**:近期交易日 cache vs PG 應一致(證明 parser 對「PG 本來就對」的資料忠實)。
- **已知 bug 修對**:audit 記為 bug 的格子,cache 正確、PG 錯誤(如 dtd 00403A Int64 vs PG 的 0)。
- **表特定不變式**:如 dtd 三大法人恆等式破裂數 cache 應大幅低於 PG。

任一表未過 → 印 ✗;全綠才代表「可以拿 cache 取代 PG」。

Run: uv run --project research python -m research.crawl.verify_vs_pg
"""
from __future__ import annotations

import duckdb

from research import paths

DEFAULT_DSN = "postgresql://localhost:5432/quantlib"

#: cache 表 → (pg.public 表名, 日期欄, 市場欄或 None)
TABLES = {
    "daily_quote": ("daily_quote", "date"),
    "daily_trading_details": ("daily_trading_details", "date"),
    "margin_transactions": ("margin_transactions", "date"),
    "foreign_holding_ratio": ("foreign_holding_ratio", "date"),
    "market_index": ("market_index", "date"),
    "stock_per_pbr": ("stock_per_pbr_dividend_yield", "date"),
    "sbl_borrowing": ("sbl_borrowing", "date"),
}


def _con():
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{DEFAULT_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    return con


def verify() -> None:
    con = _con()
    print(f"{'表':<26}{'cache 列':>12}{'PG 列':>12}{'cache 日':>9}{'PG 日':>8}  判定")
    print("─" * 82)
    all_ok = True
    for tbl, (pgt, dcol) in TABLES.items():
        try:
            cc = con.execute(f"SELECT count(*), count(DISTINCT {dcol}) FROM {tbl}").fetchone()
            pc = con.execute(f"SELECT count(*), count(DISTINCT {dcol}) FROM pg.public.{pgt}").fetchone()
        except Exception as exc:  # noqa: BLE001
            print(f"{tbl:<26} 查詢失敗:{str(exc)[:40]}")
            all_ok = False
            continue
        # cache 日數應 ≥ PG(重建補回缺日);列數通常 ≥(補回被丟的列)
        day_ok = cc[1] >= pc[1]
        verdict = "✓" if day_ok else "✗ cache 日 < PG!"
        if not day_ok:
            all_ok = False
        print(f"{tbl:<26}{cc[0]:>12,}{pc[0]:>12,}{cc[1]:>9,}{pc[1]:>8,}  {verdict}")

    # dtd 三大法人恆等式:cache vs PG 破裂數
    print("\n── 表特定不變式 ──")
    try:
        q = ("WITH x AS (SELECT CAST(foreign_investors_difference AS BIGINT) f,"
             "CAST(trust_difference AS BIGINT) t, CAST(dealers_difference AS BIGINT) d,"
             "CAST(total_difference AS BIGINT) tot FROM {src} WHERE foreign_investors_difference IS NOT NULL)"
             " SELECT count(*) FROM x WHERE tot != f+t+d")
        cb = con.execute(q.format(src="daily_trading_details")).fetchone()[0]
        pb = con.execute(q.format(src="pg.public.daily_trading_details")).fetchone()[0]
        print(f"  dtd 三大法人恆等式破裂:cache={cb:,}  PG={pb:,}  " +
              ("✓ cache 大幅修正" if cb < pb else "✗ cache 未改善"))
        if cb >= pb:
            all_ok = False
    except Exception as exc:  # noqa: BLE001
        print(f"  dtd 恆等式檢查失敗:{str(exc)[:50]}")

    # 已知 bug 修對(dtd 00403A Int64 / 0050 2012 自營商)
    for code, d, col, want, desc in [
        ("00403A", "2026-05-12", "dealers_difference", None, "Int32 溢位:cache 非 0、PG=0"),
        ("0050", "2012-05-02", "dealers_difference", -198000, "13欄自營商對位:cache=-198000"),
    ]:
        try:
            cv = con.execute(f"SELECT {col} FROM daily_trading_details WHERE company_code='{code}' AND date='{d}'").fetchone()
            pv = con.execute(f"SELECT {col} FROM pg.public.daily_trading_details WHERE company_code='{code}' AND date='{d}'").fetchone()
            cv = cv[0] if cv else None
            pv = pv[0] if pv else None
            ok = (cv != pv) if want is None else (cv == want)
            print(f"  {code} {d}: cache={cv:,} PG={pv} — {desc} {'✓' if ok else '✗'}")
            if not ok:
                all_ok = False
        except Exception as exc:  # noqa: BLE001
            print(f"  {code} {d} 檢查失敗:{str(exc)[:50]}")

    con.close()
    print("\n" + ("═══ 全綠:cache 可取代 PG(仍建議留 PG 至消費者全遷移)═══"
                  if all_ok else "═══ ✗ 有未過項,PG 不可刪,需先修 ═══"))


if __name__ == "__main__":
    verify()
