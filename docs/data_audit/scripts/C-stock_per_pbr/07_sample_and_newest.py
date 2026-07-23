"""C-stock_per_pbr 稽核 07:隨機抽樣逐欄比對(3 日 x 5 檔)+ cache 獨有日的合理性。

Run: uv run --project research python docs/data_audit/scripts/C-stock_per_pbr/07_sample_and_newest.py
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"
COLS = ["price_book_ratio", "dividend_yield", "price_to_earning_ratio"]


def main() -> None:
    random.seed(20260722)
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")

    dates = [r[0] for r in con.sql("""
      SELECT DISTINCT date FROM stock_per_pbr WHERE date <= DATE '2026-07-17'
      USING SAMPLE 3 ROWS (reservoir, 20260722)
    """).fetchall()]
    print(f"抽樣日期: {dates}")
    ok = bad = 0
    for d in dates:
        allc = [r[0] for r in con.execute(
            "SELECT DISTINCT company_code FROM stock_per_pbr WHERE date = ? ORDER BY 1",
            [d]).fetchall()]
        codes = random.sample(allc, 5)
        for code in codes:
            c = con.execute(
                "SELECT market, date, company_code, price_book_ratio, dividend_yield, "
                "price_to_earning_ratio FROM stock_per_pbr WHERE date=? AND company_code=?",
                [d, code]).fetchall()
            p = con.execute(
                "SELECT market, date, company_code, price_book_ratio, dividend_yield, "
                "price_to_earning_ratio FROM pg.public.stock_per_pbr_dividend_yield "
                "WHERE date=? AND company_code=?", [d, code]).fetchall()
            same = sorted(map(str, c)) == sorted(map(str, p))
            ok, bad = (ok + 1, bad) if same else (ok, bad + 1)
            print(f"  {d} {code:>6}  cache={c}  pg={p}  {'OK' if same else '*** 不同 ***'}")
    print(f"\n抽樣結果: 相同 {ok} / 不同 {bad}")

    print("\n== cache 獨有日 2026-07-20 的合理性(對照前一交易日 2026-07-17)==")
    print(con.sql("""
      SELECT date, market, COUNT(*) n,
             ROUND(median(price_book_ratio),3) pb_med,
             ROUND(median(dividend_yield),3) dy_med,
             COUNT(*) FILTER (WHERE price_to_earning_ratio IS NULL) pe_null
      FROM stock_per_pbr WHERE date IN (DATE '2026-07-17', DATE '2026-07-20')
      GROUP BY 1,2 ORDER BY 2,1
    """).df().to_string())
    print("\n-- 兩日皆有的代號,PB 相對變動的分位數(應接近 0,不應恆等於 0)--")
    print(con.sql("""
      WITH a AS (SELECT market, company_code, price_book_ratio pb FROM stock_per_pbr WHERE date=DATE '2026-07-17'),
           b AS (SELECT market, company_code, price_book_ratio pb FROM stock_per_pbr WHERE date=DATE '2026-07-20')
      SELECT COUNT(*) n,
             ROUND(quantile_cont(b.pb/a.pb, 0.05),4) p05,
             ROUND(quantile_cont(b.pb/a.pb, 0.50),4) p50,
             ROUND(quantile_cont(b.pb/a.pb, 0.95),4) p95,
             COUNT(*) FILTER (WHERE b.pb = a.pb) identical
      FROM a JOIN b USING (market, company_code) WHERE a.pb > 0
    """).df().to_string())


if __name__ == "__main__":
    main()
