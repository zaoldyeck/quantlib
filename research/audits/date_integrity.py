"""日期完整性偵測器:找出「別天的資料(幽靈日)」與「真交易日整天缺漏」。

這是資料稽核 FC2(別天資料)+ FC3(缺交易日)的**權威偵測器**,取代從稽核報告
文字裡 regex 抓日期的脆弱做法——它用與稽核 agent 同款、可重現的方法直接算:

  幽靈日 = 某日的「整日內容指紋」(排除日期欄)與另一日完全相同。
           台股交易所對非交易日/某些請求會回「別天」或固定快照(常是 2017-12-18),
           爬蟲照檔名日期存檔 → 兩個日期鍵下裝著同一份內容。
  缺交易日 = 該日**其他日頻表有健康資料**(證明市場有開市),但本表整天 0 列。
           成因多是空回應被寫成 0-byte sentinel,data_calendar 讀它當休市 → 永不補。

輸出即**重抓清單**;同時可當每日守護(CI 跑一次,新汙染即紅)。

用法:
  uv run --project research python -m research.audits.date_integrity            # 全表報告
  uv run --project research python -m research.audits.date_integrity --table daily_quote
  uv run --project research python -m research.audits.date_integrity --json worklist.json
依賴:PostgreSQL(權威來源;不走 cache,cache 只是 PG 的複本)。
"""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass

import duckdb

# (表名, 內容欄位——算指紋用,排除 market/date)。跨表「市場有無開市」以列數為證。
DAILY_TABLES = {
    "daily_quote": ["company_code", "closing_price", "trade_volume", "trade_value"],
    "daily_trading_details": ["company_code", "foreign_investors_difference",
                              "total_difference"],
    "margin_transactions": ["company_code", "margin_balance_of_the_day",
                            "short_balance_of_the_day"],
    "stock_per_pbr_dividend_yield": ["company_code", "price_book_ratio", "dividend_yield"],
    "foreign_holding_ratio": ["company_code", "foreign_held_ratio"],
    "sbl_borrowing": ["company_code", "daily_balance"],
    "index": ["name", "close", "change"],
}
#: 判定「市場有開市」的證人表(任一有健康列數即為交易日)
WITNESS_TABLES = ["daily_quote", "daily_trading_details", "margin_transactions",
                  "stock_per_pbr_dividend_yield", "index"]
#: 健康列數下限(低於此視為半截/空,不算證人)——市場最少也有數百檔
MIN_HEALTHY_ROWS = 50


@dataclass
class DateIssue:
    table: str
    market: str
    date: str
    kind: str            # "phantom"(別天資料) | "missing"(缺交易日)
    detail: str


def _pg(con) -> None:
    con.execute("SET enable_progress_bar=false;")   # 否則進度條洗版 stdout
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute("ATTACH 'dbname=quantlib host=localhost port=5432' AS pg "
                "(TYPE postgres, READ_ONLY);")


def _markets(con, table: str) -> list[str]:
    return [r[0] for r in con.execute(
        f"SELECT DISTINCT market FROM pg.public.\"{table}\" ORDER BY 1").fetchall()]


def phantom_days(con, table: str, cols: list[str]) -> list[DateIssue]:
    """整日內容指紋與另一日相同的日期(別天的資料)。"""
    issues: list[DateIssue] = []
    fp = " || '|' || ".join(f"COALESCE(CAST({c} AS VARCHAR),'')" for c in cols)
    for market in _markets(con, table):
        # 每個 (market,date) 算一個整日指紋 = 排序後所有列指紋的雜湊
        rows = con.execute(f"""
            WITH per_row AS (
              SELECT date, md5({fp}) AS rh
              FROM pg.public."{table}" WHERE market = ?
            ),
            per_day AS (
              SELECT date, count(*) AS n, md5(string_agg(rh, ',' ORDER BY rh)) AS day_fp
              FROM per_row GROUP BY date
            )
            SELECT day_fp, count(*) AS dup, min(date) AS keep, list(date ORDER BY date) AS dates
            FROM per_day WHERE n >= ? GROUP BY day_fp HAVING count(*) > 1
        """, [market, MIN_HEALTHY_ROWS]).fetchall()
        for day_fp, dup, keep, dates in rows:
            # 指紋相同的多天:最早那天多半是真的,其餘是「複製過去/未來」的幽靈
            for d in dates:
                if d == keep:
                    continue
                issues.append(DateIssue(
                    table, market, str(d), "phantom",
                    f"整日內容與 {keep} 逐列相同({dup} 天共用同指紋)"))
    return issues


def missing_trading_days(con, table: str) -> list[DateIssue]:
    """本表整天 0 列、但其他日頻表證明市場有開市的日期。"""
    issues: list[DateIssue] = []
    # 建全市場交易日集合(任一證人表健康列數 ≥ 門檻)
    witness_sql = " UNION ".join(
        f"SELECT market, date FROM pg.public.\"{t}\" GROUP BY market, date "
        f"HAVING count(*) >= {MIN_HEALTHY_ROWS}" for t in WITNESS_TABLES)
    for market in _markets(con, table):
        # **只在本表自己的覆蓋範圍 [min,max] 內找洞**:表的起點比別表晚(如 tpex
        # daily_quote 2007-07 起、stock_per_pbr 2007-01 起)是覆蓋差異,不是缺漏,
        # 不能算進去(否則會誤報數千天要重抓)。
        rng = con.execute(
            f"SELECT min(date), max(date) FROM pg.public.\"{table}\" WHERE market = ?",
            [market]).fetchone()
        if rng[0] is None:
            continue
        lo, hi = rng
        rows = con.execute(f"""
            WITH witness AS ({witness_sql}),
            trading AS (SELECT DISTINCT date FROM witness
                        WHERE market = ? AND date BETWEEN ? AND ?),
            have AS (SELECT DISTINCT date FROM pg.public."{table}" WHERE market = ?)
            SELECT t.date FROM trading t
            LEFT JOIN have h ON t.date = h.date
            WHERE h.date IS NULL ORDER BY t.date
        """, [market, lo, hi, market]).fetchall()
        for (d,) in rows:
            issues.append(DateIssue(table, market, str(d), "missing",
                                    "其他日頻表證明有開市,本表整天 0 列"))
    return issues


def scan(con, tables: list[str] | None = None) -> list[DateIssue]:
    issues: list[DateIssue] = []
    for table, cols in DAILY_TABLES.items():
        if tables and table not in tables:
            continue
        issues += phantom_days(con, table, cols)
        issues += missing_trading_days(con, table)
    return issues


def main() -> None:
    ap = argparse.ArgumentParser(description="日期完整性偵測(幽靈日 + 缺交易日)")
    ap.add_argument("--table", default=None, help="只掃某表")
    ap.add_argument("--json", default=None, help="輸出重抓清單 JSON")
    args = ap.parse_args()

    con = duckdb.connect()
    _pg(con)
    issues = scan(con, [args.table] if args.table else None)
    con.close()

    from collections import Counter
    by = Counter((i.table, i.kind) for i in issues)
    print(f"日期完整性:{len(issues)} 個問題")
    for (table, kind), n in sorted(by.items()):
        print(f"  {table:<30} {kind:<8} {n}")
    for i in issues:
        print(f"  ✗ {i.table} {i.market} {i.date} [{i.kind}] {i.detail}")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump([asdict(i) for i in issues], fh, ensure_ascii=False, indent=1)
        print(f"→ 重抓清單 {args.json}")


if __name__ == "__main__":
    main()
