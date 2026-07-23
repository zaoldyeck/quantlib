"""C-sbl_borrowing ⑥:把前五支腳本的結果收斂成兩張「可以直接照做」的清單。

  A. 錯日資料(檔名日期 ≠ 內容日期):要刪的 DB 列 + 要刪的原始檔 + 補抓 URL。
  B. 真的漏抓(0-byte sentinel 蓋在真交易日上,或整個檔案不存在):同上。
  C. 附帶量化:跨市場重複 (date, company_code) 有幾對落在錯日上、缺漏日的估計列數。

Run: PYTHONPATH=<repo> uv run --project . python docs/data_audit/scripts/C-sbl_borrowing/06_actionables.py
"""
from __future__ import annotations

import datetime as dt
import os

import duckdb
import pandas as pd

from research import paths
from quantlib.data_calendar import is_trading_day

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))))
SCAN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "content_date_scan.csv")

TWSE_URL = "https://www.twse.com.tw/exchangeReport/TWT93U?response=csv&date={ymd}"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/margin/sbl?date={roc}"


def roc(d: dt.date) -> str:
    return f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"


def main() -> None:
    pd.set_option("display.width", 260)
    pd.set_option("display.max_rows", 300)
    con = duckdb.connect(str(paths.CACHE_DB), read_only=True)

    scan = pd.read_csv(SCAN, parse_dates=["fn_date", "content_date"])
    scan["fn_date"] = scan.fn_date.dt.date
    scan["content_date"] = scan.content_date.dt.date

    bad = scan[scan.content_date.notna() & (scan.fn_date != scan.content_date)].copy()
    bad["is_trading_day"] = bad.fn_date.map(is_trading_day)
    bad["wd"] = bad.fn_date.map(lambda d: d.strftime("%a"))
    bad["lag_days"] = [(c - f).days for f, c in zip(bad.fn_date, bad.content_date)]
    rows = []
    for _, r in bad.iterrows():
        n = con.execute("SELECT count(*) FROM sbl_borrowing WHERE market=? AND date=?",
                        [r.market, r.fn_date]).fetchone()[0]
        rows.append(n)
    bad["db_rows"] = rows
    bad["kind"] = ["真交易日(內容錯)" if t else "非交易日(幽靈)" for t in bad.is_trading_day]
    bad["url"] = [TWSE_URL.format(ymd=d.strftime("%Y%m%d")) if m == "twse"
                  else TPEX_URL.format(roc=roc(d)) for m, d in zip(bad.market, bad.fn_date)]

    print("=== A. 錯日資料(檔名 ≠ 內容),依類別 ===")
    print(bad.groupby(["kind", bad.lag_days > 0]).agg(
        n=("fn_date", "size"), db_rows=("db_rows", "sum")).to_string())
    print("\nA-1 真交易日但內容是別天(要刪 DB 列 + 刪檔 + 重抓):")
    t = bad[bad.is_trading_day].sort_values(["market", "fn_date"])
    print(t[["market", "fn_date", "wd", "content_date", "lag_days", "db_rows"]].to_string())
    print(f"合計 {len(t)} 天 / {t.db_rows.sum()} 列")
    print("\nA-2 非交易日卻有整天資料(幽靈日,要刪 DB 列 + 刪檔,不必重抓):")
    g = bad[~bad.is_trading_day].sort_values(["market", "fn_date"])
    print(g[["market", "fn_date", "wd", "content_date", "lag_days", "db_rows"]].to_string())
    print(f"合計 {len(g)} 天 / {g.db_rows.sum()} 列")

    print("\n\n=== B. 真的漏抓 ===")
    lo, hi = con.sql("SELECT min(date), max(date) FROM sbl_borrowing").fetchone()
    have = {(m, d) for m, d in con.sql(
        "SELECT DISTINCT market, date FROM sbl_borrowing").fetchall()}
    # B-1:is_trading_day 判為交易日、sbl 無列
    miss = []
    for market in ("twse", "tpex"):
        d = lo
        while d <= hi:
            if d.weekday() < 5 and (market, d) not in have and is_trading_day(d):
                miss.append((market, d))
            d += dt.timedelta(days=1)
    # B-2:daily_quote sentinel 說「非交易日」但 margin/dtd/per 有資料 → 真交易日
    extra = con.sql(f"""
    WITH w AS (
      SELECT market, date FROM margin_transactions WHERE date BETWEEN DATE '{lo}' AND DATE '{hi}'
      INTERSECT
      SELECT market, date FROM daily_trading_details WHERE date BETWEEN DATE '{lo}' AND DATE '{hi}'
      INTERSECT
      SELECT market, date FROM stock_per_pbr WHERE date BETWEEN DATE '{lo}' AND DATE '{hi}'),
      s AS (SELECT DISTINCT market, date FROM sbl_borrowing)
    SELECT w.market, w.date FROM (SELECT DISTINCT market, date FROM w) w
    LEFT JOIN s USING (market, date) WHERE s.date IS NULL ORDER BY 1,2""").df()
    extra_set = {(r.market, r.date.date() if hasattr(r.date, "date") else r.date)
                 for _, r in extra.iterrows()}
    allmiss = sorted(set(miss) | extra_set)

    out = []
    for market, d in allmiss:
        f = os.path.join(REPO, "data", "sbl_borrowing", market, str(d.year),
                         f"{d.year}_{d.month}_{d.day}.csv")
        sz = os.path.getsize(f) if os.path.exists(f) else None
        prev = con.execute(
            "SELECT count(*) FROM sbl_borrowing WHERE market=? AND date=(SELECT max(date) "
            "FROM sbl_borrowing WHERE market=? AND date<?)", [market, market, d]).fetchone()[0]
        url = (TWSE_URL.format(ymd=d.strftime("%Y%m%d")) if market == "twse"
               else TPEX_URL.format(roc=roc(d)))
        out.append((market, d, d.strftime("%a"), "0-byte sentinel" if sz == 0
                    else ("檔案不存在" if sz is None else f"{sz}B"), prev, url))
    b = pd.DataFrame(out, columns=["market", "date", "wd", "raw", "est_rows", "url"])
    print(b.to_string())
    print(f"\n合計 {len(b)} 個 (market,date),估計缺 {b.est_rows.sum()} 列")
    print(b.groupby(["market", "raw"]).agg(n=("date", "size"), est_rows=("est_rows", "sum")).to_string())

    print("\n\n=== C. 跨市場重複 (date, company_code) 與錯日的關係 ===")
    dup = con.sql("""
    SELECT date, company_code FROM sbl_borrowing GROUP BY 1,2 HAVING count(*)>1""").df()
    baddates = set(bad[bad.market == "twse"].fn_date)
    dup["on_bad_day"] = dup.date.map(
        lambda d: (d.date() if hasattr(d, "date") else d) in baddates)
    print(dup.groupby("on_bad_day").size().to_string())
    print("不在錯日上的(真轉市日):")
    print(dup[~dup.on_bad_day].to_string())


if __name__ == "__main__":
    main()
