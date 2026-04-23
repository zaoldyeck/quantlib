---
name: quantlib-data-refresh
description: Use this skill when the user asks to update / sync / refresh TWSE data (e.g. "更新資料", "sync data", "資料是不是最新的", "refresh cache"). Runs the mandatory two-step Data Refresh Workflow (Scala crawler + PostgreSQL → DuckDB cache sync) and verifies v4 baseline has not regressed. Blocks proceeding if regression > 2pp CAGR.
---

# Data refresh workflow (MANDATORY order)

Two-step pipeline, never skip step 2:

## Step 0: Freshness gate

Check when cache was last synced:
```bash
ls -la research/cache.duckdb 2>&1 | awk '{print $6, $7, $8}'
```
- If mtime within 24h and user didn't explicitly say "force refresh" → ask "cache was synced X hours ago, refresh anyway?"
- If mtime > 24h OR user said "force" → proceed

## Step 1: Crawl + import to PostgreSQL

```bash
cd /Users/zaoldyeck/Documents/scala/quantlib && \
  sbt "runMain Main update"
```

- Bash timeout ≥ 1200000ms (this can take 10-30 min)
- Watch for "[giveup]" errors → flag them (usually TWSE holiday or weekend — benign)
- Watch for new CSV schema issues → `quantlib-data-auditor` handles

## Step 2: Sync PostgreSQL → local DuckDB cache

```bash
cd /Users/zaoldyeck/Documents/scala/quantlib && \
  uv run --project research python research/cache_tables.py
```

- Takes 3-4 min (pulls ~10M rows)
- Output must show all 5 tables with expected row counts:
  - daily_quote ~5.2M rows
  - stock_per_pbr ~4.3M rows
  - growth_analysis_ttm ~170K rows
  - ex_right_dividend ~17K rows
  - etf ~220 rows
- If any table drops >5% vs expected → stop, invoke quantlib-data-auditor

## Step 3: Baseline regression check

Run v4 backtest; compare against memory baseline (`project_v4_baseline.md`):
```bash
cd /Users/zaoldyeck/Documents/scala/quantlib && \
  uv run --project research python research/v4.py \
    --start 2018-01-02 --end 2026-04-17 --capital 1000000
```

Expected Python baseline: CAGR +27.09% / Sharpe 1.02 / MDD -39.8% / Excess +216pp.

- |ΔCAGR| <= 1pp → ✅ report success
- 1pp < |ΔCAGR| <= 2pp → ⚠️ warn user, possible factor drift
- |ΔCAGR| > 2pp → 🚨 **STOP**, advise user to investigate before any new research (likely data bug introduced)

## Output (Traditional Chinese)

- Step 1 耗時 + TWSE 有無失敗日期
- Step 2 五表 row count 對照
- Step 3 CAGR / Sharpe / MDD / Excess vs baseline delta
- 結論：Green / Yellow / Red + 下一步建議

## Anti-patterns

- Never skip Step 2 — research scripts would read stale cached data
- Never run Step 2 without Step 1 — cache would be outdated
- Don't auto-fix data bugs here — delegate to `quantlib-data-auditor`
- Don't hide regressions — always report exact ΔCAGR vs baseline
