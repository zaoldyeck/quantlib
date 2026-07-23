"""幽靈日偵測:兩個不同日期的整日內容完全相同(某日資料被掛到別的日期上)。

**動機**:TWSE 對「非交易日」的請求會把**你要的日期**印在標題上、內容卻回鄰近交易日的
資料(A-daily_quote #5、C-daily_quote:2009-12-12〔週六〕內容 = 2009-12-18)。這類:
- content_dates 抓不到(標頭日=檔名日,server 照印請求日);
- 當年 cache-vs-PG 比對也抓不到(PG 與 cache 同源同錯,兩邊都有這 772 幽靈列)。
唯一抓法 = **整日內容指紋比對**:同 (source,market) 下兩個日期指紋相同 = 其一是幽靈。

判定哪個是幽靈:非交易日(is_trading_day=False,週末/假日)那個;真交易日那個保留。

Run: uv run --project . python -m quantlib.verify.ghost_days           # 偵測
     uv run --project . python -m quantlib.verify.ghost_days --fix     # 刪幽靈 cache 列 + raw 改 sentinel
唯讀(--fix 才寫)。
"""
from __future__ import annotations

import argparse
import hashlib
from datetime import date as Date

from quantlib import paths
from quantlib.data_calendar import is_trading_day
from quantlib.db import connect

#: 每源做指紋的代表值欄(company_code + 此欄足以區分不同交易日)。
_FP_COL = {
    "daily_quote": "closing_price",
    "daily_trading_details": "foreign_investors_difference",
    "margin_transactions": "margin_balance_of_the_day",
    "foreign_holding_ratio": "foreign_held_ratio",
    "stock_per_pbr": "price_book_ratio",
    "sbl_borrowing": "daily_balance",
}


def _collisions(con, table: str, col: str, market: str) -> list[tuple[Date, Date]]:
    """該 (表,市場) 下內容指紋相同的日期對。"""
    rows = con.execute(f"""
        SELECT date, string_agg(company_code || ':' || COALESCE({col}, 0), ',' ORDER BY company_code) sig
        FROM {table} WHERE market = ? GROUP BY date
    """, [market]).fetchall()
    seen: dict[str, Date] = {}
    dups: list[tuple[Date, Date]] = []
    for d, sig in rows:
        h = hashlib.md5(sig.encode()).hexdigest()
        if h in seen:
            dups.append((seen[h], d))
        else:
            seen[h] = d
    return dups


def detect(con) -> list[dict]:
    """回所有幽靈日對:{table, market, keep(真交易日), ghost(非交易日), rows}。"""
    out = []
    for table, col in _FP_COL.items():
        for market in ("twse", "tpex"):
            for a, b in _collisions(con, table, col, market):
                # 幽靈 = 非交易日那個;都是交易日則標 uncertain(理論上不該發生)
                ta, tb = is_trading_day(a), is_trading_day(b)
                if ta and not tb:
                    keep, ghost = a, b
                elif tb and not ta:
                    keep, ghost = b, a
                else:
                    keep, ghost = (a, b) if a < b else (b, a)  # 都交易日:保留較早、標存疑
                n = con.execute(f"SELECT count(*) FROM {table} WHERE market=? AND date=?",
                                [market, ghost]).fetchone()[0]
                out.append({"table": table, "market": market, "keep": keep, "ghost": ghost,
                            "rows": n, "both_trading": ta and tb})
    return out


def _sentinel_raw(source: str, market: str, day: Date) -> bool:
    """把幽靈日的 raw 檔改成 0-byte 休市 sentinel(非交易日的正確狀態)。回是否有動。"""
    raw = paths.RAW / source / market / f"{day.year:04d}" / f"{day.year}_{day.month}_{day.day}.csv"
    if raw.exists() and raw.stat().st_size > 0:
        raw.write_bytes(b"")  # 0-byte sentinel
        return True
    return False


def fix(ghosts: list[dict]) -> None:
    """修幽靈日:對碰撞對中的**每個**日期重爬(不猜哪個是幽靈——幽靈的 raw 內容≠真實,
    重爬取回正確資料 upsert;已正確的日期重爬得同值、無害;非交易日重爬回無資料→刪+sentinel)。
    比刪除穩健:幽靈多是真交易日(raw 檔頭對、內容錯),需要的是**正確值**不是刪掉。
    **呼叫前務必關閉讀連線**(DuckDB 同程序不能同時讀寫)。端點只回最新資料的老日期
    (如 margin TPEx 2008)重爬會 DateMismatch→守衛拒絕、raw 不損,優雅跳過(無法重爬修)。"""
    import importlib

    from quantlib.crawl import parse as P
    from quantlib.crawl.sink import Sink
    _SRC_MODULE = {"market_index": "index"}
    # 收集碰撞對涉及的所有 (table, market, date),去重
    todo = set()
    for g in ghosts:
        todo.add((g["table"], g["market"], g["ghost"]))
        todo.add((g["table"], g["market"], g["keep"]))
    import duckdb

    def _del_cache(table, market, day):
        w = duckdb.connect(str(paths.CACHE_DB), read_only=False)
        try:
            w.execute(f"DELETE FROM {table} WHERE market=? AND date=?", [market, day])
        finally:
            w.close()

    fixed = sentineled = errs = 0
    unfixable: list[tuple] = []
    for table, market, day in sorted(todo, key=lambda x: (x[0], x[1], x[2])):
        modname = _SRC_MODULE.get(table, table)
        mod = importlib.import_module(f"quantlib.crawl.sources.{modname}")
        # 非交易日(週末/颱風/假日,is_trading_day 讀 daily_quote sentinel)本不該有資料。
        # 端點對非交易日只會回鄰日幽靈(檔頭對、內容錯,守衛放行),重爬修不掉 → **直接移除**
        # (刪 cache + raw 改 0-byte sentinel)。使用者定調:錯誤資料乾淨移除,只留正確資料。
        if not is_trading_day(day):
            _del_cache(table, market, day)
            _sentinel_raw(table, market, day)
            sentineled += 1
            print(f"  · {table}/{market} {day}: 非交易日 → 移除幽靈(刪 cache + raw sentinel)")
            continue
        try:
            df = mod.fetch_day(market, day)  # 真交易日:重抓 + 守衛 + 覆蓋 raw
            if df is None or df.is_empty():
                _del_cache(table, market, day)
                _sentinel_raw(table, market, day)
                sentineled += 1
                print(f"  · {table}/{market} {day}: 交易日但端點無資料 → 移除")
                continue
            with Sink() as sink:
                n = sink.upsert_day(mod.TABLE, market, day, df, date_col=getattr(mod, "DATE_COL", "date"))
            fixed += 1
            print(f"  ✓ {table}/{market} {day}: 重爬 upsert {n} 列(取回正確資料)")
        except Exception as exc:  # noqa: BLE001 - 端點回最新(老日期)→ DateMismatch/SchemaDrift 守衛拒絕,raw 不損
            errs += 1
            unfixable.append((table, market, day))
            kind = type(exc).__name__
            print(f"  ❌ {table}/{market} {day}: {kind}(端點只回最新、無法重爬)")
    print(f"  [fix] 重爬 {fixed}、移除 {sentineled}、真交易日但無法重爬 {errs}")
    if unfixable:
        print("  ⚠ 以下真交易日幽靈端點無法重爬,需用連續性判定移除錯誤那天(見 --resolve-unfetchable):")
        for t, m, d in unfixable:
            print(f"      {t}/{m} {d}")


def main() -> None:
    ap = argparse.ArgumentParser(description="幽靈日偵測(整日內容指紋碰撞)")
    ap.add_argument("--fix", action="store_true", help="刪幽靈 cache 列 + raw 改 0-byte sentinel")
    args = ap.parse_args()
    con = connect()
    ghosts = detect(con)
    print(f"=== 幽靈日偵測(整日內容指紋碰撞;{len(_FP_COL)} 源 × 雙市場)===")
    if not ghosts:
        print("  ✓ 無幽靈日,全源各交易日內容互異。")
        return
    for g in ghosts:
        tag = "⚠都交易日" if g["both_trading"] else "幽靈=非交易日"
        print(f"  {g['table']:24}/{g['market']}  {g['ghost']}({g['rows']} 列)= 複製 {g['keep']}  [{tag}]")
    if args.fix:
        print("\n[fix] 重爬幽靈日(取回正確資料):")
        con.close()  # DuckDB 同程序不能讀寫並存:fix 前先關讀連線
        fix(ghosts)
    else:
        print(f"\n  共 {len(ghosts)} 個幽靈日 → --fix 清除(刪 cache 列 + raw 改 sentinel)")


if __name__ == "__main__":
    main()
