"""C-ex_right_dividend 稽核 06:把磁碟上的 MOPS 月報檔重新解析,和 PG / cache 對帳。

為什麼要做:Scala 端有兩層「已有就跳過」——
  * Task.pullExRightDividend:359-375「檔案存在就不重抓」(當月除外);
  * TradingReader.readExRightDividend:302-303 + 350「(market,date,code) 已在 DB 就不寫」。
兩層疊起來 → 公告事後被更正(MOPS 常見:先報整數股利,再依參加分派股數調整成
小數)時,PG 永遠停在第一次匯入的舊值。本腳本用「現在磁碟上的檔案」當真值重算,
把停在舊值的列全部抓出來。

Run: uv run --project research python docs/data_audit/scripts/C-ex_right_dividend/06_reparse_vs_db.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import duckdb  # noqa: E402
import polars as pl  # noqa: E402
from research import paths  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from importlib import import_module  # noqa: E402

_m = import_module("04_mops_file_semantics")

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"
ROOT = Path(__file__).resolve().parents[4]
DATA = ROOT / "data" / "ex_right_dividend"


def rebuild() -> pl.DataFrame:
    """依 Scala parseMopsRows 的語義重建「檔案應該產生的列」。

    同 key 多列時保留第一列(Scala: dividendRow ++ rightRow 後 distinctBy)。
    跨檔同 key 時,以「公告日期較晚」者為準(較晚的公告是更正後版本)。
    """
    recs = []
    for market in ("twse", "tpex"):
        for year_dir in sorted((DATA / market).iterdir()):
            if not year_dir.is_dir():
                continue
            for f in sorted(year_dir.glob("*.csv")):
                if f.stem.count("_") != 1:      # 只處理 MOPS 月報 YYYY_M.csv
                    continue
                recs.extend(_m.parse_rows(_m.read_mops(f), market))
    if not recs:
        return pl.DataFrame()
    df = pl.DataFrame(recs)
    # 同檔同 key:息 在前(parse_rows 的產生順序即 Scala 的順序)
    df = df.with_row_index("ord")
    df = (df.sort(["ann", "ord"], descending=[True, False])
            .unique(subset=["market", "date", "code"], keep="first"))
    return df.select(["market", "date", "code", "cash", "kind", "ann"])


def main() -> None:
    files = rebuild()
    print(f"重建列數(MOPS 月報,去重後):{len(files):,}")

    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.register("f", files.rename({"code": "company_code", "cash": "file_cash"}))

    print("\n== 檔案 vs PG:同 key 的現金股利差異(相對差 > 1e-6)==")
    print(con.sql("""
      SELECT f.market, f.date, f.company_code, f.kind, f.ann AS announce_date,
             f.file_cash, e.cash_dividend AS pg_cash,
             round(f.file_cash - e.cash_dividend, 6) AS delta
      FROM f JOIN pg.public.ex_right_dividend e
        ON e.market=f.market AND e.date=f.date AND e.company_code=f.company_code
      WHERE abs(f.file_cash - e.cash_dividend) > 1e-6
      ORDER BY f.date
    """).df().to_string())

    print("\n== 檔案有、PG 沒有的 key(依年月彙總)==")
    print(con.sql("""
      SELECT f.market, year(f.date) y, month(f.date) m, COUNT(*) n
      FROM f WHERE NOT EXISTS (
        SELECT 1 FROM pg.public.ex_right_dividend e
        WHERE e.market=f.market AND e.date=f.date AND e.company_code=f.company_code)
      GROUP BY 1,2,3 ORDER BY 1,2,3
    """).df().to_string())

    print("\n== 檔案 vs cache:同 key 的現金股利差異(只看 cash>0 者)==")
    print(con.sql("""
      SELECT f.market, f.date, f.company_code, f.file_cash, c.cash_dividend AS cache_cash
      FROM f JOIN ex_right_dividend c
        ON c.market=f.market AND c.date=f.date AND c.company_code=f.company_code
      WHERE f.file_cash > 0 AND abs(f.file_cash - c.cash_dividend) > 1e-6
      ORDER BY f.date
    """).df().to_string())


if __name__ == "__main__":
    main()
