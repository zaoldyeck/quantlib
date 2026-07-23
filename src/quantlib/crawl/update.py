"""每日更新編排:抓「已齊備」交易日的新資料,增量 upsert 進 cache.duckdb。

齊備語義沿用 `quantlib.data_calendar`:D 的資料自 D+1 00:30 起才齊備,只抓
`latest_complete_trading_day()` 以前的交易日;TWSE 回無資料就寫 0-byte sentinel
維護休市日曆(`is_trading_day` 讀它)。

覆蓋範圍:
- **日頻**(每交易日必有):daily_quote、daily_trading_details、stock_per_pbr
  —— S 決策熱路徑,已 parity 驗證。
- **月頻/不定期**:operating_revenue(S 進場訊號,月頻)、ex_right_dividend、
  capital_reduction —— 見 `_refresh_monthly`。

Run: uv run --project . python -m quantlib.crawl.update [--upto YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
from datetime import date as Date, timedelta

from quantlib.crawl.sink import Sink
from quantlib.crawl.sources import (daily_quote, daily_trading_details,
                                    foreign_holding_ratio, insider_holding,
                                    margin_transactions, sbl_borrowing,
                                    stock_per_pbr)
from quantlib.crawl.sources import index as market_index
from quantlib.data_calendar import (QUOTE_DIR, is_trading_day,
                                    latest_complete_trading_day)

#: 日頻源(每交易日必有資料;以 cache 最新日 + 1 起補)。完整替代 Scala `Main update`
#: 的日頻爬取——含籌碼面(margin/foreign/sbl)、指數、內部人,消費者(Serenity/S/
#: auto_trader)全需,不得只留 S 熱路徑造成籌碼訊號靜默降級。
DAILY_SOURCES = [daily_quote, daily_trading_details, stock_per_pbr,
                 margin_transactions, foreign_holding_ratio, sbl_borrowing,
                 market_index, insider_holding]


def _missing_days(sink: Sink, src, market: str, upto: Date) -> list[Date]:
    """cache 中該 (源,市場) 最新日之後、直到 upto 的交易日(跳週末/sentinel)。
    源可宣告 `DATE_COL`(預設 'date';insider_holding 用 'report_date',無 date 欄)。"""
    dcol = getattr(src, "DATE_COL", "date")
    mx = sink.con.execute(
        f"SELECT max({dcol}) FROM {src.TABLE} WHERE market = ?", [market]).fetchone()[0]
    if mx is None:
        return []  # 空表 → 應先 scp 種歷史,不從零下載
    out, d = [], mx + timedelta(days=1)
    while d <= upto:
        if is_trading_day(d):
            out.append(d)
        d += timedelta(days=1)
    return out


def _write_sentinel(day: Date) -> None:
    """TWSE 回無資料 → 寫 0-byte 休市 sentinel(daily_quote/twse 為日曆真源)。"""
    p = QUOTE_DIR / str(day.year) / f"{day.year}_{day.month}_{day.day}.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        p.write_bytes(b"")


def _refresh_daily(sink: Sink, upto: Date) -> int:
    total = 0
    for src in DAILY_SOURCES:
        dcol = getattr(src, "DATE_COL", "date")
        for market in src.MARKETS:
            for day in _missing_days(sink, src, market, upto):
                df = src.fetch_day(market, day)
                if df is None:
                    # TWSE 無資料 = 休市 → 寫 sentinel;其餘源跟隨日曆,不寫
                    if src is daily_quote and market == "twse":
                        _write_sentinel(day)
                        print(f"[crawl] {day} TWSE 無資料 → 休市 sentinel")
                    continue
                n = sink.upsert_day(src.TABLE, market, day, df, date_col=dcol)
                total += n
                print(f"[crawl] {src.TABLE}/{market} {day}: {n} 列")
    return total


def _refresh_monthly(sink: Sink, upto: Date) -> None:
    """月頻/不定期源:operating_revenue(S 進場訊號)、ex_right_dividend、
    capital_reduction、treasury_stock_buyback(庫藏股,Serenity 消費)——
    全 Python 直抓,VM 完全自主、零 Mac 依賴。
    operating_revenue 更新後重算 industry_taxonomy_pit(唯一真源)。
    註:財報 bs/is/cf(季頻)由 quantlib.crawl.rebuild_financials 另跑(季度一次),
    不在每日路徑——季報資料 stale-tolerant,每日 loop 不需刷新。
    """
    from quantlib.crawl.sources import (capital_reduction, ex_right_dividend,
                                        operating_revenue, treasury_stock_buyback)

    n = operating_revenue.refresh(sink, upto)
    if n:
        operating_revenue.rebuild_industry_taxonomy(sink)
    ex_right_dividend.refresh(sink, upto)
    capital_reduction.refresh(sink, upto)
    treasury_stock_buyback.refresh(sink, upto)


def ensure_fresh(upto: Date | None = None, *, monthly: bool = True) -> None:
    """齊備自檢 + 增量更新。premarket 於 07:20 呼叫。"""
    upto = upto or latest_complete_trading_day()
    print(f"[crawl] 更新至齊備日 {upto}")
    with Sink() as sink:
        n = _refresh_daily(sink, upto)
        if monthly:
            _refresh_monthly(sink, upto)
    print(f"[crawl] 日頻新增 {n} 列,更新完成")


def main() -> None:
    ap = argparse.ArgumentParser(description="台股 cache 每日增量更新(Python 直寫 DuckDB)")
    ap.add_argument("--upto", default=None, help="更新到此交易日(預設最新齊備日)")
    ap.add_argument("--no-monthly", action="store_true", help="只跑日頻(跳過月頻源)")
    args = ap.parse_args()
    upto = Date.fromisoformat(args.upto) if args.upto else None
    ensure_fresh(upto, monthly=not args.no_monthly)


if __name__ == "__main__":
    main()
