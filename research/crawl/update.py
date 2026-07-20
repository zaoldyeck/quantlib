"""每日更新編排:抓「已齊備」交易日的新資料,增量 upsert 進 cache.duckdb。

齊備語義沿用 `research.data_calendar`:D 的資料自 D+1 00:30 起才齊備,只抓
`latest_complete_trading_day()` 以前的交易日;TWSE 回無資料就寫 0-byte sentinel
維護休市日曆(`is_trading_day` 讀它)。

覆蓋範圍:
- **日頻**(每交易日必有):daily_quote、daily_trading_details、stock_per_pbr
  —— S 決策熱路徑,已 parity 驗證。
- **月頻/不定期**:operating_revenue(S 進場訊號,月頻)、ex_right_dividend、
  capital_reduction —— 見 `_refresh_monthly`。

Run: uv run --project research python -m research.crawl.update [--upto YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
from datetime import date as Date, timedelta

from research.crawl.sink import Sink
from research.crawl.sources import (daily_quote, daily_trading_details,
                                    stock_per_pbr)
from research.data_calendar import (QUOTE_DIR, is_trading_day,
                                    latest_complete_trading_day)

#: 日頻源(每交易日必有資料;以 cache 最新日 + 1 起補)
DAILY_SOURCES = [daily_quote, daily_trading_details, stock_per_pbr]


def _missing_days(sink: Sink, table: str, market: str, upto: Date) -> list[Date]:
    """cache 中該 (表,市場) 最新日之後、直到 upto 的交易日(跳週末/sentinel)。"""
    mx = sink.con.execute(
        f"SELECT max(date) FROM {table} WHERE market = ?", [market]).fetchone()[0]
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
        for market in src.MARKETS:
            for day in _missing_days(sink, src.TABLE, market, upto):
                df = src.fetch_day(market, day)
                if df is None:
                    # TWSE 無資料 = 休市 → 寫 sentinel;其餘源跟隨日曆,不寫
                    if src is daily_quote and market == "twse":
                        _write_sentinel(day)
                        print(f"[crawl] {day} TWSE 無資料 → 休市 sentinel")
                    continue
                n = sink.upsert_day(src.TABLE, market, day, df)
                total += n
                print(f"[crawl] {src.TABLE}/{market} {day}: {n} 列")
    return total


def _refresh_monthly(sink: Sink, upto: Date) -> None:
    """月頻源:operating_revenue(S 進場訊號)+ 其後重算 industry_taxonomy_pit。

    ex_right_dividend / capital_reduction(不定期價格調整表)首版由月度 scp 橋接
    —— 兩者日影響低(cache 已含前瞻除權息資料),Python adapter 為 fast-follow
    (見 crawl 模組 docstring 與部署計畫 Part B)。此處不靜默略過:明文標記。
    """
    from research.crawl.sources import operating_revenue

    n = operating_revenue.refresh(sink, upto)
    if n:
        operating_revenue.rebuild_industry_taxonomy(sink)
    print("[crawl] ex_right_dividend / capital_reduction:首版由月度 scp 橋接"
          "(adapter fast-follow;cache 已含前瞻資料)")


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
