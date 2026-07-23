# transcript 逐字復原(零改動)。
#
# 來源:3d5413eb-b7db-45c8-bf62-efdef11c1375.jsonl @ 2026-07-08T23:51:54.150Z(工具 Write:/Users/zaoldyeck/Documents/scala/quantlib/src/quantlib/apex/experiments/smoke_momentum.py)
# 涵蓋 trials(1):mom126_top10_monthly
"""Smoke run — 驗證 apex 管線端到端(非假設檢定;記帳 batch=B00-smoke)。

策略(故意樸素):月頻,決策日 = 每月最後交易日,買 126d 動能(跳過近 5d)
top 10,跌出 top 30 訊號出場 + trailing 25%。dev 窗 2012-2023。

Requires: var/cache/cache.duckdb(cache_tables.py 最新即可)。
Run: uv run --project . python src/quantlib/apex/experiments/smoke_momentum.py
"""
from __future__ import annotations

import time

import polars as pl

from quantlib.apex import data, ledger, metrics
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate

DEV_START, DEV_END = "2012-01-02", "2023-12-29"
TOPN, EXIT_RANK = 10, 30

t0 = time.time()
con = data.connect()
panel = data.common_stocks(data.load_panel(con, DEV_START, DEV_END, warmup_days=300))
elig = data.eligibility(panel)
t1 = time.time()

feat = (
    panel.sort(["company_code", "date"])
    .with_columns(
        (pl.col("close").shift(5) / pl.col("close").shift(126) - 1)
        .over("company_code")
        .alias("mom")
    )
    .select(["date", "company_code", "mom"])
    .drop_nulls()
)

month_last = (
    panel.select(pl.col("date").unique().sort())
    .group_by_dynamic("date", every="1mo")
    .agg(pl.col("date").last().alias("decision"))
    .get_column("decision")
)

scored = (
    feat.filter(pl.col("date").is_in(month_last.implode()))
    .join(elig.filter(pl.col("eligible")), on=["date", "company_code"], how="semi")
    .with_columns(pl.col("mom").rank("ordinal", descending=True).over("date").alias("rk"))
)
entries = scored.filter(pl.col("rk") <= TOPN).select(
    ["date", "company_code", pl.col("mom").alias("score")]
)
# 訊號死亡:決策日不在 top EXIT_RANK 的「全 universe」都掛 flag(引擎只對持倉生效)
in_top = scored.filter(pl.col("rk") <= EXIT_RANK).select(["date", "company_code"])
all_on_days = panel.filter(pl.col("date").is_in(month_last.implode())).select(
    ["date", "company_code"]
)
exit_flags = all_on_days.join(in_top, on=["date", "company_code"], how="anti")
t2 = time.time()

res = simulate(
    panel.filter(pl.col("date") >= pl.lit(DEV_START).str.to_date()),
    entries,
    exit_flags=exit_flags,
    exec_spec=ExecSpec(),
    port_spec=PortSpec(n_slots=TOPN),
    exit_spec=ExitSpec(trailing_stop=0.25),
)
t3 = time.time()

bench = data.benchmark_nav(con, DEV_START, DEV_END)
print(metrics.fmt_report("smoke: 126d momentum top10 monthly", res.nav, res.trades, bench))
print(f"\ntiming: load {t1-t0:.1f}s | signals {t2-t1:.1f}s | engine {t3-t2:.1f}s")

trial_id = ledger.log_trial(
    family="smoke",
    name="mom126_top10_monthly",
    hypothesis="管線端到端驗證(非假設檢定)",
    config={
        "topn": TOPN, "exit_rank": EXIT_RANK, "mom_window": 126, "mom_skip": 5,
        "trailing": 0.25, "fill": "next_open", "universe": "twse+tpex common",
    },
    window=f"{DEV_START}..{DEV_END}",
    metrics=metrics.summarize(res.nav, res.trades, bench),
    batch="B00-smoke",
    curve=res.nav,
)
print(f"logged: {trial_id}")

