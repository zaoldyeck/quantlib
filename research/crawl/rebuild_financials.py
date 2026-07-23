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
    qdirs = sorted(glob.glob("data/financial_statements/*_*"))
    yq = []
    for d in qdirs:
        m = re.match(r"(\d+)_(\d+)$", os.path.basename(d))
        if m:
            yq.append((int(m.group(1)), int(m.group(2))))
    print(f"[rebuild-cf] {len(yq)} 季", flush=True)
    frames = []
    err = 0
    for i, (y, q) in enumerate(yq):
        try:
            df = cf.parse_quarter(y, q)
            if df is not None and not df.is_empty():
                frames.append(df)
        except Exception as exc:  # noqa: BLE001
            err += 1
            if err <= 3:
                print(f"  ⚠ {y}Q{q}: {str(exc)[:60]}", flush=True)
        if i % 20 == 0:
            print(f"  ...{y}Q{q} ({i}/{len(yq)})", flush=True)
    full = pl.concat(frames, how="vertical_relaxed").unique(
        subset=["market", "year", "quarter", "company_code", "title"], keep="last")
    with Sink() as s:
        s.con.register("_x", full)
        s.con.execute("DROP TABLE IF EXISTS cf_progressive_raw")
        s.con.execute("CREATE TABLE cf_progressive_raw AS SELECT * FROM _x")
        s.con.unregister("_x")
    print(f"[rebuild] cf_progressive_raw: {full.height:,} 列({err} 錯)", flush=True)
    return full.height


def regen_raw_quarterly() -> None:
    from research.db import connect
    from research.strat_lab.raw_quarterly import build_raw_quarterly

    con = connect(use_cache=True)  # 用重建後的 cache
    rq = build_raw_quarterly(con, date(2001, 1, 1), date(2026, 8, 1))
    out = paths.REPO / "research" / "records" / "raw_quarterly.parquet"
    rq.write_parquet(str(out))
    key = rq.filter((pl.col("company_code") == "2330") & (pl.col("year") == 2024)
                    & (pl.col("quarter") == 4))
    fs = key.select("f_score_raw").to_series().to_list() if not key.is_empty() else None
    print(f"[rebuild] raw_quarterly.parquet: {rq.height:,} 列 → {out}", flush=True)
    print(f"  金鑰 2330 2024Q4 F-Score = {fs}(稽核基準 8)", flush=True)


if __name__ == "__main__":
    rebuild_cf()
    regen_raw_quarterly()
    print("[rebuild] 財報鏈完成:cf + raw_quarterly", flush=True)
