"""C-ex_right_dividend 稽核 07:cash_dividend 欄的語義在 2024-07(twse)/2026-05(tpex)換源後改變。

兩種語義:
  * legacy(TWT49U / exDailyQ):TradingReader 取的是「權值+息值」欄
    (twse values(5)、tpex values(7)),等於 除權息前收盤價 − 除權息參考價,
    也就是**除權息當日的全部價格扣除**(含股票股利造成的稀釋)。
  * MOPS t108sb27(parseMopsRows,TradingReader.scala:378-380):取的是
    「盈餘分配 + 法定盈餘公積 + 特別股」= **純現金股利**,股票股利另開一列
    且 cash_dividend = 0。

research/prices.py:25 的還原公式 factor = (close_pre − cash_div)/close_pre:
  * 餵 legacy 值 → factor = 參考價/前收盤 = 正確的「現金+配股」合併還原因子。
  * 餵 MOPS 值 → 只還原現金部分,**配股完全沒還原**(而且純配股列因
    prices.py:143 的 cash_dividend > 0 被整列丟掉)。

本腳本:
  (1) 全史驗證 legacy 列的 cash_dividend == pre_close − ref_px(證明語義是「權值+息值」);
  (2) 盤點換源後有股票股利的事件(= 被漏還原的),並用 daily_quote 實際價格落差量化。

Run: uv run --project research python docs/data_audit/scripts/C-ex_right_dividend/07_semantic_drift.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402
import polars as pl  # noqa: E402
from importlib import import_module  # noqa: E402

from research import paths  # noqa: E402

_m = import_module("04_mops_file_semantics")

PG_DSN = "host=localhost port=5432 dbname=quantlib user=zaoldyeck"
ROOT = Path(__file__).resolve().parents[4]
DATA = ROOT / "data" / "ex_right_dividend"


def mops_events() -> pl.DataFrame:
    """從 MOPS 月報檔重建「公司-事件」層級資料(含股票股利金額)。"""
    recs = []
    for market in ("twse", "tpex"):
        for year_dir in sorted((DATA / market).iterdir()):
            if not year_dir.is_dir():
                continue
            for f in sorted(year_dir.glob("*.csv")):
                if f.stem.count("_") != 1:
                    continue
                for r in _m.read_mops(f):
                    if len(r) < 17 or not r[0].strip() or r[0].strip() == "公司代號":
                        continue
                    stock = _m._d(r[4]) + _m._d(r[5])
                    cash = _m._d(r[7]) + _m._d(r[8]) + _m._d(r[9])
                    recs.append({
                        "market": market, "company_code": r[0].strip(),
                        "ex_right": _m._slash(r[6]), "ex_div": _m._slash(r[10]),
                        "stock_div": stock, "cash_div": cash,
                        "ann": _m._slash(r[16]),
                    })
    return pl.DataFrame(recs)


def main() -> None:
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)
    con.sql("INSTALL postgres; LOAD postgres;")
    con.sql(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres, READ_ONLY)")
    con.sql("""
      CREATE OR REPLACE TEMP VIEW e AS
      SELECT market, date, company_code, cash_dividend, right_or_dividend,
             closing_price_before_ex_right_ex_dividend AS pre_close,
             ex_right_ex_dividend_reference_price AS ref_px,
             CASE WHEN closing_price_before_ex_right_ex_dividend = 0
                   AND ex_right_ex_dividend_reference_price = 0
                  THEN 'mops' ELSE 'legacy' END AS src
      FROM pg.public.ex_right_dividend
    """)

    print("== (1) legacy 列:cash_dividend 是否等於 pre_close − ref_px ==")
    print(con.sql("""
      SELECT right_or_dividend, COUNT(*) n,
             COUNT(*) FILTER (WHERE abs(cash_dividend - (pre_close - ref_px)) <= 0.005) n_match,
             COUNT(*) FILTER (WHERE abs(cash_dividend - (pre_close - ref_px)) > 0.005) n_mismatch,
             round(max(abs(cash_dividend - (pre_close - ref_px))), 4) max_abs_err
      FROM e WHERE src='legacy' GROUP BY 1 ORDER BY 2 DESC
    """).df().to_string())

    print("\n== (1b) legacy 不吻合的樣本 ==")
    print(con.sql("""
      SELECT * FROM e WHERE src='legacy'
        AND abs(cash_dividend - (pre_close - ref_px)) > 0.005
      ORDER BY abs(cash_dividend - (pre_close - ref_px)) DESC LIMIT 20
    """).df().to_string())

    ev = mops_events()
    con.register("mv", ev)

    print("\n== (2) MOPS 期間有股票股利的事件數(= prices.py 漏還原的配股)==")
    print(con.sql("""
      SELECT market, year(ex_right) y, COUNT(*) n,
             COUNT(*) FILTER (WHERE cash_div > 0) n_also_cash,
             round(median(stock_div), 3) med_stock_div
      FROM mv WHERE stock_div > 0 AND ex_right IS NOT NULL
      GROUP BY 1,2 ORDER BY 1,2
    """).df().to_string())

    print("\n== (2b) 逐筆:配股事件在 DB 的 cash_dividend、以及當日 raw 價格落差 ==")
    print(con.sql("""
      WITH ev AS (
        SELECT market, company_code, ex_right AS date, stock_div, cash_div
        FROM mv WHERE stock_div > 0 AND ex_right IS NOT NULL
      ),
      px AS (
        SELECT market, company_code, date, closing_price,
               lag(closing_price) OVER (PARTITION BY market, company_code ORDER BY date) prev_close
        FROM daily_quote
      )
      SELECT ev.market, ev.date, ev.company_code, ev.stock_div, ev.cash_div,
             e.cash_dividend AS db_cash, e.right_or_dividend AS db_kind,
             px.prev_close, px.closing_price AS close,
             round(px.closing_price / nullif(px.prev_close,0) - 1, 4) AS raw_ret,
             round(1.0 / (1.0 + ev.stock_div/10.0) - 1, 4) AS implied_dilution
      FROM ev
      LEFT JOIN e ON e.market=ev.market AND e.date=ev.date AND e.company_code=ev.company_code
      LEFT JOIN px ON px.market=ev.market AND px.company_code=ev.company_code AND px.date=ev.date
      ORDER BY ev.date
    """).df().to_string())


if __name__ == "__main__":
    main()
