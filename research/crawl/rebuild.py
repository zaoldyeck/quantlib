"""從 raw 全量重建 cache 表:用各源(已擴充吃全世代)的 parser 重解析封存 raw。

**取代 `cache_tables.py`(PG→DuckDB)**——這是 PostgreSQL 退役後 cache 的建法,也是
「所有歷史資料正確」的執行器:cache 原本從 PG(Scala 匯入、含 162 個 parser bug)抄來;
本模組改為**直接從封存 raw 用修好的 Python parser 重建**,一次修掉解析層 bug + 解除 PG 依賴。

機制(parser-agnostic):monkeypatch `http.fetch_bytes/fetch_text`,讓每個源自己的
`fetch_day(market, day)` **讀封存 raw(不連網)** → 走它完整的 decode/格式世代分派/parse
→ 收集全歷史 → 寫回 cache 表。重用 fetch_day 的真實路徑,故源內部怎麼擴充世代都自動生效。

**只重建、不改 raw**:rebuild 期間 archive.save_raw 被 no-op 掉(raw 是不可重生地基,重建
只讀不寫)。休市 sentinel(0-byte)→ fetch_day 回 None → 跳過。

Run:
  uv run --project research python -m research.crawl.rebuild --source daily_trading_details
  uv run --project research python -m research.crawl.rebuild --all
  uv run --project research python -m research.crawl.rebuild --source daily_quote --dry-run  # 只解析算列數不寫
"""
from __future__ import annotations

import argparse
import contextlib
import importlib
from datetime import date as Date

import polars as pl

from research import paths
from research.crawl import archive, http

#: 日頻源:raw 在 data/<source>/<market>/<year>/<y>_<m>_<d>.csv,fetch_day(market, day) 單次抓取。
DAILY_SOURCES = [
    "daily_quote", "daily_trading_details", "margin_transactions",
    "foreign_holding_ratio", "index", "stock_per_pbr", "sbl_borrowing",
]

#: 源模組名 → raw 目錄名(過渡對映)。2026-07-23 raw 集中統一:stock_per_pbr 歷史已從
#: Scala 時代的 stock_per_pbr_dividend_yield/ 併入 canonical stock_per_pbr/(不再需對映);
#: `index` 模組的 raw(TABLE=market_index)歷史已從 index/ 併入 market_index/。
#: 模組檔正名 index→market_index 留 Phase 1(架構重整)。
_RAW_DIR = {"index": "market_index"}


def _raw_path(source: str, market: str, day: Date) -> "object":
    """該(源,市場,日)的封存 raw 路徑,套用舊路徑對映(stock_per_pbr → …_dividend_yield)。"""
    d = _RAW_DIR.get(source, source)
    return paths.RAW / d / market / f"{day.year:04d}" / f"{day.year:04d}_{day.month}_{day.day}.csv"


def _archived_days(source: str, market: str) -> list[Date]:
    """該源該市場封存了哪些日子(含 sentinel;由檔名 <y>_<m>_<d>.csv 推得),排序。"""
    base = paths.RAW / _RAW_DIR.get(source, source) / market
    if not base.exists():
        return []
    out: list[Date] = []
    for f in base.rglob("*.csv"):
        try:
            y, m, d = (int(x) for x in f.stem.split("_"))
            out.append(Date(y, m, d))
        except (ValueError, TypeError):
            continue  # 非 <y>_<m>_<d> 命名(季頻多檔源)→ 不走日頻路徑
    return sorted(set(out))


@contextlib.contextmanager
def _read_from_archive(source: str, market: str, day: Date):
    """讓源的 fetch_day 改讀封存 raw(不連網、不重寫 raw)。"""
    p = _raw_path(source, market, day)
    raw = p.read_bytes()
    o_bytes, o_text, o_save = http.fetch_bytes, http.fetch_text, archive.save_raw
    http.fetch_bytes = lambda *a, **k: raw
    http.fetch_text = lambda *a, encoding="Big5-HKSCS", **k: raw.decode(encoding, errors="replace")
    archive.save_raw = lambda *a, **k: p  # no-op:重建只讀 raw,不動不可重生地基
    try:
        yield
    finally:
        http.fetch_bytes, http.fetch_text, archive.save_raw = o_bytes, o_text, o_save


def rebuild_daily_source(source: str, *, dry_run: bool = False) -> dict:
    """重建一個日頻源的 cache 表。回 {rows, days, empty, errors}。"""
    mod = importlib.import_module(f"research.crawl.sources.{source}")
    table = mod.TABLE
    markets = getattr(mod, "MARKETS", ("twse", "tpex"))
    frames: list[pl.DataFrame] = []
    n_days = n_empty = n_err = 0
    errs: list[str] = []
    for market in markets:
        for day in _archived_days(source, market):
            n_days += 1
            try:
                with _read_from_archive(source, market, day):
                    df = mod.fetch_day(market, day)
            except Exception as exc:  # noqa: BLE001 - 收集,不中斷整源
                n_err += 1
                if len(errs) < 20:
                    errs.append(f"{market} {day}: {type(exc).__name__}: {str(exc)[:80]}")
                continue
            if df is None or df.is_empty():
                n_empty += 1
                continue
            frames.append(df)
    rows = 0
    if frames and not dry_run:
        full = pl.concat(frames, how="vertical_relaxed")
        rows = full.height
        con = __import__("duckdb").connect(str(paths.CACHE_DB), read_only=False)
        try:
            con.register("_new", full)
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM _new")
        finally:
            con.unregister("_new")
            con.close()
    elif frames:
        rows = sum(f.height for f in frames)
    return {"source": source, "table": table, "rows": rows, "days": n_days,
            "empty": n_empty, "errors": n_err, "err_sample": errs}


def _write(table: str, df: "pl.DataFrame") -> int:
    con = __import__("duckdb").connect(str(paths.CACHE_DB), read_only=False)
    try:
        con.register("_new", df)
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM _new")
    finally:
        con.unregister("_new")
        con.close()
    return df.height


def rebuild_operating_revenue() -> int:
    """oprev 全 raw(CSV+HTML,3 世代)→ operating_revenue + 重算 industry_taxonomy_pit(FC8)。"""
    import glob
    from research.crawl.sources import operating_revenue as opr
    from research.crawl.sink import Sink
    files = (glob.glob("data/operating_revenue/**/*.csv", recursive=True)
             + glob.glob("data/operating_revenue/**/*.html", recursive=True))
    frames = [d for f in files if (d := _safe(opr.parse_file, f)) is not None and not d.is_empty()]
    full = pl.concat(frames, how="vertical_relaxed").unique(
        subset=["market", "type", "year", "month", "company_code"], keep="last")
    with Sink() as s:
        s.con.register("_o", full)
        s.con.execute("DROP TABLE IF EXISTS operating_revenue")
        s.con.execute("CREATE TABLE operating_revenue AS SELECT * FROM _o")
        s.con.unregister("_o")
        opr.rebuild_industry_taxonomy(s)  # FC8:用出表日期當生效錨
    print(f"[rebuild] operating_revenue: {full.height:,} 列 + industry_taxonomy_pit")
    return full.height


#: 除權息 cache 目標欄——**含 FC1 參考價欄**(closing_price_before / reference_price)。
#: prices.py 以「參考價/前收盤」為首選還原因子(純配股不再幽靈崩跌);絕不可沿用舊 4 欄
#: 瘦身 schema(cash_dividend only),否則 prices.py 退回 cash-only fallback → FC1 失效。
_EXD_KEEP = ["market", "date", "company_code", "cash_dividend", "right_or_dividend",
             "closing_price_before_ex_right_ex_dividend",
             "ex_right_ex_dividend_reference_price"]


def rebuild_ex_right_dividend() -> int:
    """除權息全世代(parse_raw 標頭判世代:MOPS / twse 舊制 / tpex 舊制)。"""
    import glob
    from research.crawl.sources import ex_right_dividend as exd
    frames = []
    for market in exd.MARKETS:
        for f in glob.glob(f"data/ex_right_dividend/{market}/**/*.csv", recursive=True):
            d = _safe(exd.parse_raw, market, open(f, "rb").read())
            if d is not None and not d.is_empty():
                frames.append(d.select([c for c in _EXD_KEEP if c in d.columns]))
    full = pl.concat(frames, how="vertical_relaxed").unique(
        subset=["market", "date", "company_code"], keep="last")
    print(f"[rebuild] ex_right_dividend: {_write('ex_right_dividend', full):,} 列"
          f"(欄:{', '.join(full.columns)})")
    return full.height


def rebuild_capital_reduction() -> int:
    import glob
    from research.crawl.sources import capital_reduction as cr
    frames = []
    for market in cr.MARKETS:
        for f in glob.glob(f"data/capital_reduction/{market}/**/*.csv", recursive=True):
            recs = _safe(lambda m, x: cr._parse(m, x), market,
                         open(f, "rb").read().decode("Big5-HKSCS", errors="replace"))
            if recs:
                frames.append(pl.DataFrame(recs))
    full = pl.concat(frames, how="vertical_relaxed").unique()
    print(f"[rebuild] capital_reduction: {_write('capital_reduction', full):,} 列")
    return full.height


def rebuild_taifex_daily() -> int:
    """期貨日資料:parse_text over 年檔+月檔;**全列去重(非只 date——同日多合約)**。"""
    import glob
    from research.crawl.sources import taifex
    files = sorted(glob.glob("data/taifex/futures_daily/*_fut.csv")
                   + glob.glob("data/taifex/futures_daily/*/*.csv"))
    frames = [d for f in files
              if (d := _safe(taifex.parse_text,
                             open(f, encoding="Big5-HKSCS", errors="replace").read())) is not None
              and not d.is_empty()]
    full = pl.concat(frames, how="vertical_relaxed").unique(keep="first")
    print(f"[rebuild] taifex_futures_daily: {_write('taifex_futures_daily', full):,} 列")
    return full.height


def _safe(fn, *a):
    try:
        return fn(*a)
    except Exception:  # noqa: BLE001 - 收集,單檔錯不中斷整源(rebuild 對汙染/漂移 fail-soft)
        return None


def rebuild_insider_holding() -> int:
    import glob
    import re
    from datetime import date as _D
    from pathlib import Path
    from research.crawl.sources import insider_holding as ins
    frames = []
    for market in ins.MARKETS:
        for f in glob.glob(f"data/insider_holding/{market}/**/*.html", recursive=True):
            m = re.match(r"(\d+)_(\d+)_(\d+)", Path(f).stem)
            if not m:
                continue
            rd = _D(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            d = _safe(ins.parse_raw, market, open(f, "rb").read(), rd)
            if d is not None and not d.is_empty():
                frames.append(d)
    full = pl.concat(frames, how="vertical_relaxed").unique() if frames else None
    if full is None or full.is_empty():
        print("[rebuild] insider_holding: 0 列(保護舊表不寫)")
        return 0
    print(f"[rebuild] insider_holding: {_write('insider_holding', full):,} 列")
    return full.height


def rebuild_treasury_buyback() -> int:
    import glob
    from research.crawl.sources import treasury_stock_buyback as tsb
    frames = []
    for market in tsb.MARKETS:
        for f in glob.glob(f"data/treasury_stock_buyback/{market}/**/*.html", recursive=True):
            d = _safe(tsb.parse_raw, market, open(f, "rb").read())
            if d is not None and not d.is_empty():
                frames.append(d)
    full = pl.concat(frames, how="vertical_relaxed").unique() if frames else None
    if full is None or full.is_empty():
        print("[rebuild] treasury_stock_buyback: 0 列(保護舊表不寫)")
        return 0
    print(f"[rebuild] treasury_stock_buyback: {_write('treasury_stock_buyback', full):,} 列")
    return full.height


#: 非日頻源 → rebuild 函式(財報 bs/is/cf + raw_quarterly 見 rebuild_financials.py,量大另跑)。
QUARTERLY_REBUILDS = {
    "operating_revenue": rebuild_operating_revenue,
    "ex_right_dividend": rebuild_ex_right_dividend,
    "capital_reduction": rebuild_capital_reduction,
    "taifex_futures_daily": rebuild_taifex_daily,
    "insider_holding": rebuild_insider_holding,
    "treasury_stock_buyback": rebuild_treasury_buyback,
}


def main() -> None:
    ap = argparse.ArgumentParser(description="從 raw 全量重建 cache 表(全世代 parser)")
    ap.add_argument("--source", help="單一源(見 DAILY_SOURCES)")
    ap.add_argument("--all", action="store_true", help="全部日頻源")
    ap.add_argument("--quarterly", action="store_true", help="季頻/特殊源(oprev/除權息/減資/期貨)")
    ap.add_argument("--dry-run", action="store_true", help="只解析算列數/錯誤,不寫 cache")
    args = ap.parse_args()
    if args.quarterly:
        for name, fn in QUARTERLY_REBUILDS.items():
            fn()
        print("[rebuild] 季頻/特殊源完成;財報鏈另跑 research.crawl.rebuild_financials")
        return
    todo = DAILY_SOURCES if args.all else ([args.source] if args.source else [])
    if not todo:
        ap.error("需 --source <name> / --all / --quarterly")
    for s in todo:
        r = rebuild_daily_source(s, dry_run=args.dry_run)
        tag = "(dry)" if args.dry_run else ""
        print(f"[rebuild]{tag} {r['source']}: {r['rows']:,} 列 / {r['days']} 日"
              f"(空 {r['empty']}、錯 {r['errors']})")
        for e in r["err_sample"]:
            print(f"    ⚠ {e}")


if __name__ == "__main__":
    main()
