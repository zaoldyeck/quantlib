"""C-stock_per_pbr 稽核 05:量化「檔名≠內容」造成的 DB 汙染。

對 04 掃出的 19 個 twse 錯位日,查它們在 cache 裡的列數,並與「內容日」當天的
資料逐欄比對——若逐欄相同即證明 DB 內存的是別日的值。同時標出哪些錯位日
本身是交易日(daily_quote 有該日)→ 實際會被策略取用。

Run: uv run --project research python docs/data_audit/scripts/C-stock_per_pbr/05_phantom_impact.py
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
from research import paths  # noqa: E402

# (檔名日, 內容日) —— 由 04_filename_vs_content_date.py 產出
MISMATCH = [
    (date(2008, 12, 4), date(2008, 12, 18)),
    (date(2008, 3, 3), date(2017, 12, 18)),
    (date(2008, 3, 6), date(2008, 12, 18)),
    (date(2009, 8, 2), date(2017, 12, 18)),
    (date(2011, 7, 10), date(2017, 12, 18)),
    (date(2012, 10, 21), date(2012, 10, 18)),
    (date(2012, 11, 11), date(2012, 12, 18)),
    (date(2012, 11, 25), date(2012, 12, 18)),
    (date(2012, 3, 31), date(2017, 12, 18)),
    (date(2012, 8, 7), date(2017, 12, 18)),
    (date(2013, 5, 13), date(2017, 12, 18)),
    (date(2014, 10, 12), date(2017, 12, 12)),
    (date(2014, 12, 16), date(2014, 12, 18)),
    (date(2016, 10, 6), date(2017, 12, 18)),
    (date(2017, 4, 3), date(2017, 12, 18)),
    (date(2018, 12, 13), date(2017, 12, 18)),
    (date(2025, 8, 12), date(2017, 12, 18)),
    (date(2026, 2, 25), date(2017, 12, 18)),
    (date(2026, 4, 12), date(2017, 12, 18)),
]
WD = ["一", "二", "三", "四", "五", "六", "日"]


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    print(f"{'檔名日':>12} {'週':>2} {'內容日':>12} {'偏移天':>7} {'cache列':>7} "
          f"{'內容日列':>8} {'逐欄同':>7} {'該日有報價':>9} {'鄰日列數'}")
    tot_rows = tot_trading = 0
    for fn, hd in MISMATCH:
        n = con.execute(
            "SELECT COUNT(*) FROM stock_per_pbr WHERE market='twse' AND date=?",
            [fn]).fetchone()[0]
        nh = con.execute(
            "SELECT COUNT(*) FROM stock_per_pbr WHERE market='twse' AND date=?",
            [hd]).fetchone()[0]
        # 逐欄比對:同 company_code 上三個數值欄是否完全一致
        same = con.execute("""
          SELECT COUNT(*) FROM stock_per_pbr a JOIN stock_per_pbr b USING (company_code)
          WHERE a.market='twse' AND a.date=? AND b.market='twse' AND b.date=?
            AND a.price_book_ratio IS NOT DISTINCT FROM b.price_book_ratio
            AND a.dividend_yield IS NOT DISTINCT FROM b.dividend_yield
            AND a.price_to_earning_ratio IS NOT DISTINCT FROM b.price_to_earning_ratio
        """, [fn, hd]).fetchone()[0]
        has_q = con.execute(
            "SELECT COUNT(*) FROM daily_quote WHERE market='twse' AND date=?",
            [fn]).fetchone()[0]
        neigh = con.execute("""
          SELECT COUNT(*) FROM stock_per_pbr WHERE market='twse'
            AND date = (SELECT MAX(date) FROM stock_per_pbr WHERE market='twse' AND date < ?)
        """, [fn]).fetchone()[0]
        tot_rows += n
        if has_q:
            tot_trading += n
        print(f"{fn!s:>12} {WD[fn.weekday()]:>2} {hd!s:>12} {(hd-fn).days:>7} "
              f"{n:>7} {nh:>8} {same:>7} {has_q:>9} {neigh:>8}")
    print(f"\n受汙染列數合計 {tot_rows};其中落在「有報價的交易日」= {tot_trading} 列")


if __name__ == "__main__":
    main()
