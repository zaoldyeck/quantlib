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

- Takes 3-5 min (pulls ~30M rows)
- Output must show all tables with expected row-count order of magnitude (growing over time — use lower bound not exact):
  - daily_quote ≥ 8.9M rows
  - stock_per_pbr ≥ 7.6M rows
  - growth_analysis_ttm ≥ 170K rows
  - ex_right_dividend ≥ 29K rows
  - capital_reduction ≥ 640 rows
  - operating_revenue ≥ 475K rows
  - daily_trading_details ≥ 5.7M rows
  - margin_transactions ≥ 8.2M rows
  - etf ≥ 200 rows
  - tdcc_shareholding ≥ 67K rows per week accumulated（只抓當週→線性增長）
  - sbl_borrowing ≥ 6.5K rows per crawled day × 2 markets（TWSE from 2016, TPEx from 2013）
  - foreign_holding_ratio ≥ 6.6K rows per crawled day × 2 markets（TWSE from 2005, TPEx from 2010）
- If any table drops >5% vs last-known-good → stop, invoke quantlib-data-auditor

**Caveat for TDCC / SBL / QFII first-time backfill**:
- `Main update` will try to crawl `sbl_borrowing` from 2016-01-04 and `foreign_holding_ratio` from 2005-01-03 to today on first run → takes 30-40 hours due to 20s rate-limit per date
- TDCC opendata endpoint has no history, only returns current week snapshot
- Recommendation: do first-time backfill via explicit `--since` flag (overnight), then daily `Main update` becomes < 1 min incremental:
  ```bash
  sbt "runMain Main pull sbl  --since 2016-01-04"   # ~19-28h
  sbt "runMain Main pull qfii --since 2005-01-03"   # ~30-38h
  sbt "runMain Main read sbl" && sbt "runMain Main read qfii"
  ```

## Step 3: Baseline regression check

Run v4 backtest; compare against memory baseline (`project_v4_baseline.md`):
```bash
cd /Users/zaoldyeck/Documents/scala/quantlib && \
  uv run --project research python research/strat_lab/v4.py \
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
