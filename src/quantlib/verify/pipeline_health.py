"""資料 pipeline 健康度 + 覆蓋驗證(面向①資料正確性 + ②有無缺口)。

逐源檢查:
- **覆蓋**:min/max date、distinct 交易日、列數;起始 vs 真正最早可得日。
- **新鮮度**:latest vs latest_complete_trading_day(D+1 齊備政策);跨源對齊。
- **缺日**:近窗每交易日(vs data_calendar 交易日曆)是否都有資料。
- **端點健康**(--probe):每日頻源 fetch_day(最新齊備日)是否回傳資料——抓死端點
  (如 2026-07 capital_reduction strDate→startDate 事故:端點回空、資料靜默停更)。

Run:
  uv run python -m quantlib.verify.pipeline_health            # 覆蓋 + 新鮮度 + 缺日(不連網)
  uv run python -m quantlib.verify.pipeline_health --probe    # 加端點健康(連網,每源抓一天)
依賴 cache:是。
"""
from __future__ import annotations

import argparse
from datetime import date as Date, timedelta

from quantlib import db
from quantlib.data_calendar import is_trading_day, latest_complete_trading_day

#: (cache 表, 日期欄, 頻率, 真正最早可得日〔None=未知/不檢〕)。
#: 真正最早日來源:各 source docstring / 交易所開站日 / 既有封存最早檔。
_SOURCES: list[tuple[str, str, str, Date | None]] = [
    ("daily_quote", "date", "daily", Date(2004, 2, 11)),          # twse 2004-02-11 起零漂移
    ("daily_trading_details", "date", "daily", Date(2007, 4, 23)),
    ("stock_per_pbr", "date", "daily", Date(2005, 9, 1)),
    ("margin_transactions", "date", "daily", Date(2001, 1, 2)),
    ("foreign_holding_ratio", "date", "daily", Date(2004, 2, 11)),
    ("sbl_borrowing", "date", "daily", Date(2016, 1, 4)),
    ("market_index", "date", "daily", Date(2009, 1, 5)),  # 真資料 2009 起(2005/2007 僅 header probe 檔)
    ("insider_holding", "declare_date", "sparse", Date(2007, 1, 1)),
    ("operating_revenue", "year", "monthly", None),
    ("ex_right_dividend", "date", "event", Date(2003, 5, 5)),
    ("capital_reduction", "date", "event", None),
    ("treasury_stock_buyback", "announce_date", "event", None),
    ("tdcc_shareholding", "data_date", "weekly", None),
    ("taifex_futures_daily", "date", "daily", Date(1998, 7, 21)),
    ("taifex_futures_institutional", "date", "daily", None),
    ("taifex_futures_final_settlement", "date", "event", None),
]

#: 新鮮度容忍(交易日源落後齊備日超過此天數 → 警示)
_STALE_DAYS = {"daily": 3, "sparse": 30, "monthly": 40, "event": 40, "weekly": 10}


def _q1(con, sql: str):
    return con.execute(sql).fetchone()


def check_coverage(con) -> list[str]:
    """回警示行(空=全綠)。"""
    warns: list[str] = []
    complete = latest_complete_trading_day()
    print(f"=== Pipeline 覆蓋 + 新鮮度(齊備日 {complete})===")
    print(f"  {'表':30s}{'最早':>12}{'最新':>12}{'交易日':>8}{'列數':>13}  狀態")
    for table, dcol, cadence, earliest in _SOURCES:
        try:
            mn, mx, nd, n = _q1(
                con, f"SELECT min({dcol}), max({dcol}), count(DISTINCT {dcol}), count(*) FROM {table}")
        except Exception as exc:  # noqa: BLE001
            warns.append(f"{table}: 查詢失敗 {exc}")
            continue
        flags = []
        # 新鮮度(僅 date 型日源;year/月頻另計)
        if dcol == "date" and mx is not None and cadence in _STALE_DAYS:
            lag = (complete - mx).days
            if lag > _STALE_DAYS[cadence]:
                flags.append(f"落後齊備日 {lag} 天")
        # 起始 vs 真正最早可得日(晚於預期 → 可能缺歷史)
        if earliest is not None and mn is not None and isinstance(mn, Date) and mn > earliest + timedelta(days=45):
            flags.append(f"起始 {mn} 晚於預期 {earliest}(疑缺早期歷史)")
        status = "⚠ " + "；".join(flags) if flags else "✓"
        if flags:
            warns.append(f"{table}: {'；'.join(flags)}")
        print(f"  {table:30s}{str(mn):>12}{str(mx):>12}{nd or 0:>8}{n or 0:>13,}  {status}")
    return warns


def check_recent_gaps(con, window: int = 60) -> list[str]:
    """日頻源近 window 交易日是否每日都有資料(vs 交易日曆)。"""
    warns: list[str] = []
    complete = latest_complete_trading_day()
    tdays = []
    d = complete
    while len(tdays) < window:
        if is_trading_day(d):
            tdays.append(d)
        d -= timedelta(days=1)
    lo = min(tdays)
    print(f"\n=== 日頻源近 {window} 交易日缺日({lo} ~ {complete})===")
    for table, dcol, cadence, _ in _SOURCES:
        if cadence != "daily" or dcol != "date":
            continue
        have = {r[0] for r in con.execute(
            f"SELECT DISTINCT date FROM {table} WHERE date >= DATE '{lo}'").fetchall()}
        missing = [d for d in tdays if d not in have]
        if missing:
            warns.append(f"{table}: 近窗缺 {len(missing)} 交易日(最近缺 {max(missing)})")
            print(f"  ⚠ {table:30s} 缺 {len(missing)} 日;最近缺 {max(missing)}")
        else:
            print(f"  ✓ {table:30s} 近 {window} 交易日齊備")
    return warns


def probe_endpoints() -> list[str]:
    """每日頻源 fetch_day(最新齊備日)→ 是否回資料(抓死端點)。連網、每源一天。"""
    from quantlib.crawl import update
    warns: list[str] = []
    day = latest_complete_trading_day()
    print(f"\n=== 端點健康(fetch_day {day},每源一天)===")
    for src in update.DAILY_SOURCES:
        table = src.TABLE
        ok_any = False
        for market in getattr(src, "MARKETS", ("twse", "tpex")):
            try:
                dfr = src.fetch_day(market, day)
                if dfr is not None and not dfr.is_empty():
                    ok_any = True
            except Exception as exc:  # noqa: BLE001
                warns.append(f"{table}/{market}: fetch 例外 {type(exc).__name__}")
        # insider 稀疏、market_index 休市可能空;只對「該日該有資料」的源警示
        if not ok_any and table not in ("insider_holding",):
            warns.append(f"{table}: 端點對齊備日 {day} 回空(疑失效,如 strDate 事故)")
            print(f"  ⚠ {table:30s} {day} 回空")
        else:
            print(f"  ✓ {table:30s} 端點健康")
    return warns


def main() -> None:
    ap = argparse.ArgumentParser(description="資料 pipeline 健康度 + 覆蓋驗證")
    ap.add_argument("--probe", action="store_true", help="加端點健康檢查(連網)")
    ap.add_argument("--window", type=int, default=60, help="缺日檢查的近窗交易日數")
    args = ap.parse_args()
    con = db.connect()
    warns = check_coverage(con)
    warns += check_recent_gaps(con, args.window)
    if args.probe:
        warns += probe_endpoints()
    print("\n=== 總結 ===")
    if warns:
        print(f"  ⚠ {len(warns)} 項需檢視:")
        for w in warns:
            print(f"    - {w}")
    else:
        print("  ✓ 全綠:覆蓋/新鮮度/缺日皆正常")


if __name__ == "__main__":
    main()
