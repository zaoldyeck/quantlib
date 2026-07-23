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


def _archived_days(source: str, market: str) -> list[Date]:
    """該源該市場封存了哪些日子(含 sentinel;由檔名 <y>_<m>_<d>.csv 推得),排序。"""
    base = paths.RAW / source / market
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
    p = archive.raw_path(source, market, day)
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


def main() -> None:
    ap = argparse.ArgumentParser(description="從 raw 全量重建 cache 表(全世代 parser)")
    ap.add_argument("--source", help="單一源(見 DAILY_SOURCES)")
    ap.add_argument("--all", action="store_true", help="全部日頻源")
    ap.add_argument("--dry-run", action="store_true", help="只解析算列數/錯誤,不寫 cache")
    args = ap.parse_args()
    todo = DAILY_SOURCES if args.all else ([args.source] if args.source else [])
    if not todo:
        ap.error("需 --source <name> 或 --all")
    for s in todo:
        r = rebuild_daily_source(s, dry_run=args.dry_run)
        tag = "(dry)" if args.dry_run else ""
        print(f"[rebuild]{tag} {r['source']}: {r['rows']:,} 列 / {r['days']} 日"
              f"(空 {r['empty']}、錯 {r['errors']})")
        for e in r["err_sample"]:
            print(f"    ⚠ {e}")


if __name__ == "__main__":
    main()
