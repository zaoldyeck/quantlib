"""從 raw **並行**重建財報三表(bs/is/cf)+ 重生 raw_quarterly.parquet(S cfo_ni/F-Score 源)。

財報是量化交易的地基,故 authoritative:cache = parser(raw),且**多程序並行**(使用者定調
「極速並行化」)。三源各自的 raw:
- bs_concise_raw  ← data/balance_sheet/ 簡明資產負債表 CSV(1,260 檔;balance_sheet.parse_file)
- is_progressive_raw ← data/income_statement/ 綜合損益表 CSV(1,158 檔;income_statement.parse_raw_file)
- cf_progressive_raw ← data/financial_statements/ tifrs HTML(**120K 檔**;cash_flows.parse_quarter,按季)

bs/is 檔少 → 分塊並行 concat 一次寫;cf 檔多且大 → **按季並行 parse + 串流 INSERT**(避 OOM)。

Run: uv run --project . python -m quantlib.crawl.rebuild_financials
依賴 cache:寫入(DROP+重建 3 表 + raw_quarterly)。
"""
from __future__ import annotations

import glob
import os
import re
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from pathlib import Path

import polars as pl

from quantlib import paths

_BS_SEL = ["market", "type", "year", "quarter", "company_code", "title", "value"]
_IS_SCHEMA = {"market": pl.Utf8, "type": pl.Utf8, "year": pl.Int32, "quarter": pl.Int32,
              "company_code": pl.Utf8, "title": pl.Utf8, "value": pl.Float64}


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _workers() -> int:
    return max(1, (os.cpu_count() or 4) - 1)


# ── bs(簡明資產負債表 CSV)────────────────────────────────────────────────────
def _parse_bs_chunk(jobs: list) -> "pl.DataFrame | None":
    from quantlib.crawl.sources import balance_sheet
    frames = []
    for path, market in jobs:
        try:
            df = balance_sheet.parse_file(path, market)
            if df is not None and not df.is_empty():
                frames.append(df.select(_BS_SEL))
        except Exception:  # noqa: BLE001 - 單檔錯不擋整源
            pass
    return pl.concat(frames, how="vertical_relaxed") if frames else None


def _parse_is_chunk(paths: list) -> "pl.DataFrame | None":
    from quantlib.crawl.sources import income_statement
    recs = []
    for p in paths:
        try:
            recs.extend(income_statement.parse_raw_file(Path(p)))
        except Exception:  # noqa: BLE001
            pass
    return pl.DataFrame(recs, schema=_IS_SCHEMA) if recs else None


def _rebuild_csv_source(table: str, files: list, worker, dedup_subset: list) -> int:
    """bs/is 共用:分塊並行 parse → concat → 去重 → DROP+重建。回列數。"""
    import duckdb
    n_workers = _workers()
    chunks = list(_chunks(files, max(1, len(files) // (n_workers * 4) + 1)))
    frames = []
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        for df in ex.map(worker, chunks):
            if df is not None:
                frames.append(df)
    full = pl.concat(frames, how="vertical_relaxed").unique(subset=dedup_subset, keep="first")
    con = duckdb.connect(str(paths.CACHE_DB), read_only=False)
    try:
        con.register("_new", full)
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM _new")
    finally:
        con.unregister("_new")
        con.close()
    print(f"[rebuild] {table}: {full.height:,} 列(並行 {n_workers} 程序)", flush=True)
    return full.height


def rebuild_bs() -> int:
    files = [(str(f), m) for m in ("twse", "tpex")
             for f in glob.glob(f"data/balance_sheet/{m}/**/*.csv", recursive=True)]
    return _rebuild_csv_source("bs_concise_raw", files, _parse_bs_chunk,
                               ["market", "type", "year", "quarter", "company_code", "title"])


def rebuild_is() -> int:
    files = [str(f) for m in ("twse", "tpex")
             for f in glob.glob(f"data/income_statement/{m}/**/*.csv", recursive=True)]
    return _rebuild_csv_source("is_progressive_raw", files, _parse_is_chunk,
                               ["market", "type", "year", "quarter", "company_code", "title"])


# ── cf(tifrs HTML,120K 檔;按季並行 + 串流 INSERT 避 OOM)──────────────────────
def _parse_cf_quarter(yq: tuple) -> "pl.DataFrame | None":
    from quantlib.crawl.sources import cash_flows
    try:
        return cash_flows.parse_quarter(yq[0], yq[1])
    except Exception:  # noqa: BLE001
        return None


def rebuild_cf() -> int:
    import duckdb
    qdirs = sorted(glob.glob("data/financial_statements/*_*"))
    yq = []
    for d in qdirs:
        m = re.match(r"(\d+)_(\d+)$", os.path.basename(d))
        if m:
            yq.append((int(m.group(1)), int(m.group(2))))
    n_workers = _workers()
    print(f"[rebuild-cf] {len(yq)} 季(按季並行 {n_workers} 程序 + 串流 INSERT)", flush=True)
    con = duckdb.connect(str(paths.CACHE_DB), read_only=False)
    con.execute("DROP TABLE IF EXISTS cf_rebuild_tmp")
    created = False
    total = err = done = 0
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        for df in ex.map(_parse_cf_quarter, yq):  # 保序;每季回一 DF
            done += 1
            if df is None or df.is_empty():
                if df is None:
                    err += 1
                continue
            con.register("_q", df)
            if not created:
                con.execute("CREATE TABLE cf_rebuild_tmp AS SELECT * FROM _q")
                created = True
            else:
                con.execute("INSERT INTO cf_rebuild_tmp SELECT * FROM _q")
            con.unregister("_q")
            total += df.height
            if done % 20 == 0:
                print(f"  ...{done}/{len(yq)} 季,累計 {total:,} 列", flush=True)
    con.execute("DROP TABLE IF EXISTS cf_progressive_raw")
    con.execute("CREATE TABLE cf_progressive_raw AS SELECT DISTINCT * FROM cf_rebuild_tmp")
    n = con.execute("SELECT count(*) FROM cf_progressive_raw").fetchone()[0]
    con.execute("DROP TABLE IF EXISTS cf_rebuild_tmp")
    con.close()
    print(f"[rebuild] cf_progressive_raw: {n:,} 列({err} 季錯)", flush=True)
    return n


def regen_raw_quarterly() -> None:
    from quantlib.db import RAW_QUARTERLY_PARQUET, connect
    from quantlib.strat_lab.raw_quarterly import build_raw_quarterly

    con = connect()  # 用重建後的 bs/is/cf
    rq = build_raw_quarterly(con, date(2001, 1, 1), date(2026, 8, 1))
    out = RAW_QUARTERLY_PARQUET
    tmp = out + ".tmp"
    rq.write_parquet(tmp)
    os.replace(tmp, out)  # 原子換名:避免中止留半檔汙染 S live 源
    key = rq.filter((pl.col("company_code") == "2330") & (pl.col("year") == 2024)
                    & (pl.col("quarter") == 4))
    fs = key.select("f_score_raw").to_series().to_list() if not key.is_empty() else None
    print(f"[rebuild] raw_quarterly.parquet: {rq.height:,} 列 → {out}", flush=True)
    print(f"  金鑰 2330 2024Q4 F-Score = {fs}(稽核基準 8)", flush=True)


def main() -> None:
    rebuild_bs()
    rebuild_is()
    rebuild_cf()
    regen_raw_quarterly()
    print("[rebuild] 財報鏈完成:bs + is + cf + raw_quarterly(全並行)", flush=True)


if __name__ == "__main__":
    main()
