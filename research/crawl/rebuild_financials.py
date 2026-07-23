"""從 raw 重建 cf_progressive_raw + 重生 raw_quarterly.parquet(S 的 cfo_ni 閘門/F-Score 源)。

cf 慢(118K 檔跨 ~100 季),故獨立腳本背景跑。bs/is 已由日源 rebuild 流程外的
one-shot 完成;本腳本補 cf + 重生 raw_quarterly。

Run: uv run --project research python -m research.crawl.rebuild_financials
"""
from __future__ import annotations

import glob
import os
import re
from datetime import date

import polars as pl

from research import paths
from research.crawl.sink import Sink
from research.crawl.sources import cash_flows as cf


def rebuild_cf() -> int:
    """逐季**串流 INSERT**(不 concat 全部,避免 118K 檔 melt 的記憶體壓力/OOM)。"""
    import duckdb
    qdirs = sorted(glob.glob("data/financial_statements/*_*"))
    yq = []
    for d in qdirs:
        m = re.match(r"(\d+)_(\d+)$", os.path.basename(d))
        if m:
            yq.append((int(m.group(1)), int(m.group(2))))
    print(f"[rebuild-cf] {len(yq)} 季(逐季串流)", flush=True)
    con = duckdb.connect(str(paths.CACHE_DB), read_only=False)
    con.execute("DROP TABLE IF EXISTS cf_rebuild_tmp")
    created = False
    total = err = 0
    for i, (y, q) in enumerate(yq):
        try:
            df = cf.parse_quarter(y, q)
        except Exception as exc:  # noqa: BLE001
            err += 1
            if err <= 3:
                print(f"  ⚠ {y}Q{q}: {str(exc)[:60]}", flush=True)
            continue
        if df is None or df.is_empty():
            continue
        con.register("_q", df)
        if not created:
            con.execute("CREATE TABLE cf_rebuild_tmp AS SELECT * FROM _q")
            created = True
        else:
            con.execute("INSERT INTO cf_rebuild_tmp SELECT * FROM _q")
        con.unregister("_q")
        total += df.height
        del df
        if i % 10 == 0:
            print(f"  ...{y}Q{q} ({i}/{len(yq)}) 累計 {total:,} 列", flush=True)
    # 去重後換上正式表
    con.execute("DROP TABLE IF EXISTS cf_progressive_raw")
    con.execute("CREATE TABLE cf_progressive_raw AS SELECT DISTINCT * FROM cf_rebuild_tmp")
    n = con.execute("SELECT count(*) FROM cf_progressive_raw").fetchone()[0]
    con.execute("DROP TABLE IF EXISTS cf_rebuild_tmp")
    con.close()
    print(f"[rebuild] cf_progressive_raw: {n:,} 列({err} 錯)", flush=True)
    return n


def regen_raw_quarterly() -> None:
    from research.db import RAW_QUARTERLY_PARQUET, connect
    from research.strat_lab.raw_quarterly import build_raw_quarterly

    con = connect()  # 用重建後的 cache(bs/is/cf;PG 已退役)
    rq = build_raw_quarterly(con, date(2001, 1, 1), date(2026, 8, 1))
    out = RAW_QUARTERLY_PARQUET  # canonical:research/raw_quarterly.parquet(S cfo_ni 閘門讀這個)
    # 原子換名:先寫 tmp 再 os.replace,避免中途被中止留半檔汙染 S 的 live 源
    tmp = out + ".tmp"
    rq.write_parquet(tmp)
    os.replace(tmp, out)
    key = rq.filter((pl.col("company_code") == "2330") & (pl.col("year") == 2024)
                    & (pl.col("quarter") == 4))
    fs = key.select("f_score_raw").to_series().to_list() if not key.is_empty() else None
    print(f"[rebuild] raw_quarterly.parquet: {rq.height:,} 列 → {out}", flush=True)
    print(f"  金鑰 2330 2024Q4 F-Score = {fs}(稽核基準 8)", flush=True)


if __name__ == "__main__":
    rebuild_cf()
    regen_raw_quarterly()
    print("[rebuild] 財報鏈完成:cf + raw_quarterly", flush=True)
