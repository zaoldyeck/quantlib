"""重抓某季財報(bs/is/cf)→ upsert cache → 重生 raw_quarterly。

**修「季報開窗早於申報截止致凍結缺料」**:某季在申報截止前被封季(只抓到部分公司,
如 2026Q1 僅 539/~1800 家),之後**不會自癒**——每日 loop 不刷財報、rebuild_financials
只重解析既有 archive(archive 本身就缺)。本工具重抓 bulk/form(idempotent,覆寫封存)
補齊該季全部公司,更新 S 的 cfo_ni 閘門源 raw_quarterly。

fetch 簽章差異:cf = `fetch_quarter(year, quarter, force=True)`(combined bulk ZIP);
bs/is = `fetch_quarter(market, year, quarter)`(per-market form,twse+tpex 各一)。

Run: uv run --project . python -m quantlib.crawl.refetch_quarter --year 2026 --quarter 1
"""
from __future__ import annotations

import argparse

import duckdb
import polars as pl

from quantlib import paths
from quantlib.crawl.sources import balance_sheet, cash_flows, income_statement

#: cache 表欄(upsert 對齊;fetch 輸出與此同構,多餘欄剔除、缺欄由 INSERT BY NAME 補 null)
_COLS = {
    "cf_progressive_raw": ["market", "year", "quarter", "company_code", "title", "value"],
    "bs_concise_raw": ["market", "type", "year", "quarter", "company_code", "title", "value"],
    "is_progressive_raw": ["market", "type", "year", "quarter", "company_code", "title", "value"],
}


def _cov(con: duckdb.DuckDBPyConnection, table: str, year: int, quarter: int) -> int:
    return con.execute(
        f"SELECT count(DISTINCT company_code) FROM {table} "
        f"WHERE year={year} AND quarter={quarter}").fetchone()[0]


def _upsert(con: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame,
            year: int, quarter: int) -> int:
    """刪該季 + 插入(idempotent);欄位對齊 cache 表。"""
    df2 = df.select([c for c in _COLS[table] if c in df.columns]).unique()
    con.register("_q", df2)
    con.execute(f"DELETE FROM {table} WHERE year={year} AND quarter={quarter}")
    con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM _q")
    con.unregister("_q")
    return df2.height


def refetch(year: int, quarter: int) -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=False)
    try:
        # cf:combined bulk ZIP
        print(f"[refetch] cf {year}Q{quarter} before: {_cov(con, 'cf_progressive_raw', year, quarter)} 家", flush=True)
        cf_df = cash_flows.fetch_quarter(year, quarter, force=True)
        n = _upsert(con, "cf_progressive_raw", cf_df, year, quarter)
        print(f"[refetch] cf after: {_cov(con, 'cf_progressive_raw', year, quarter)} 家({n:,} rows)", flush=True)

        # bs/is:per-market form(twse + tpex)
        for name, mod, table in [("bs", balance_sheet, "bs_concise_raw"),
                                 ("is", income_statement, "is_progressive_raw")]:
            print(f"[refetch] {name} {year}Q{quarter} before: {_cov(con, table, year, quarter)} 家", flush=True)
            frames = []
            for market in ("twse", "tpex"):
                d = mod.fetch_quarter(market, year, quarter)
                if d is not None and not d.is_empty():
                    frames.append(d)
            if frames:
                full = pl.concat(frames, how="vertical_relaxed")
                n = _upsert(con, table, full, year, quarter)
                print(f"[refetch] {name} after: {_cov(con, table, year, quarter)} 家({n:,} rows)", flush=True)
    finally:
        con.close()

    # 重生 raw_quarterly(S 的 cfo_ni/F-Score 閘門源)
    from quantlib.crawl.rebuild_financials import regen_raw_quarterly
    regen_raw_quarterly()


def main() -> None:
    ap = argparse.ArgumentParser(description="重抓某季財報補齊缺料 + 重生 raw_quarterly")
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--quarter", type=int, required=True)
    args = ap.parse_args()
    refetch(args.year, args.quarter)


if __name__ == "__main__":
    main()
