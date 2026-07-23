# AGENTS.md

This file defines repository-level operating rules for Codex agents working on
this quantlib workspace.

## Codex Persistence Source Of Truth

Codex-facing persistent instructions live in this file, `.claude/skills/`, and
`.codex/agents/`. Treat those paths as the canonical Codex layer for this
repository.

- Do not infer Codex policy from Claude-specific files.
- Keep broker, data freshness, research validation, and automated-trading rules
  centralized here; skills and agents should reference these rules instead of
  duplicating conflicting policy.
- Codex command persistence is not currently a separate source of truth in this
  repository. Reusable operations should live in package-manager commands,
  scripts, `.claude/skills/`, or `.codex/agents/`, not in Claude command files.
- Durable project facts belong in repository artifacts such as `docs/`,
  `src/quantlib/trading/strategy_registry.py`, and `var/out/strat_lab/`.
- External Codex memory is read-only context unless the user explicitly asks to
  remember something. Do not write or reference non-existent `project_*.md`
  memory files as the source of truth.

## Mandatory Data Freshness

Before any data-related conclusion, ranking, backtest, KPI table, or investment
analysis:

1. Check the latest date in `var/cache/cache.duckdb`（唯一結構化真源;Python 研究一律讀它,
   PostgreSQL 已退役 2026-07-23）。
2. If stale, update data first, then recompute results.
3. State the actual data cutoff used in the output or document.

If the standard refresh has already completed on the same Taiwan calendar day and
the cache cutoff is still verified, do not rerun merely because another research
task starts. Refresh again after a new market close, date change, stale cutoff,
or an explicit user request to force-refresh.

Standard refresh（Python 爬蟲直寫 cache,一步到位取代舊 Scala Main update + cache_tables）:

```bash
uv run --project . python -m quantlib.crawl.update
```

Do not publish rankings or backtest conclusions from stale cached data.

### TAIFEX Intraday Partial-Data Guard

For TAIFEX free recent tick / intraday archives, never let an in-progress
current trading day enter research or backtests.

- Default latest safe intraday date is Taiwan today minus one calendar day until
  `16:00:00` Asia/Taipei.
- After `16:00:00` Asia/Taipei, today may be downloaded or parsed only if the
  official archive exists and downstream daily contract metadata is consistent.
- `QL_TAIFEX_INTRADAY_SAFE_AFTER` may move the cutoff later, but not earlier
  for research runs unless the user explicitly asks for live/paper intraday
  capture.
- `QL_TAIFEX_INTRADAY_ALLOW_TODAY=true` is only for live capture or manual
  diagnostics; never use it for historical backtests.
- Strategy research must state the actual intraday cutoff, and it must be the
  minimum completed cutoff across official daily data and tick-derived features.

## Performance First

Research and backtests must be implemented with production-grade performance in
mind:

- Prefer vectorized Polars/DuckDB operations over Python row loops.
- Prefer local DuckDB cache reads (`quantlib.db.connect()`) over repeated remote fetches.
- Use cache tables and reusable intermediate artifacts when iterations are
  expensive.
- Use DuckDB threading / local columnar execution for broad scans.
- Avoid recomputing unchanged panels, factors, and adjusted prices.
- Keep slow brute-force sweeps bounded, resumable, and auditable.

The default expectation is fast, local, reproducible research, not ad hoc manual
calculation.

## Root-Cause Fixes

When a real defect, data-integrity issue, stale-cache problem, or parallelism
limitation is found, fix the root cause immediately when feasible. Do not leave
known issues as future work merely to preserve momentum.

Workarounds are acceptable only as temporary diagnostics. They must not become
the final solution unless explicitly documented as the correct long-term design.

## Price And Return Semantics

All equity or ETF performance analysis must use total-return-equivalent adjusted
prices through `src/quantlib/prices.py` unless the task explicitly asks for raw price
behavior. Do not run NAV simulations directly on raw `daily_quote.closing_price`
because that omits dividends and distorts CAGR.

## Broker SDK And Secrets

The active broker integration for quote, account, and order automation is
Fubon Neo API through the official `fubon-neo` SDK wheels vendored under
`vendor/fubon_neo/`.

- Manage the SDK through `uv` and `pyproject.toml(repo 根)`; do not install it
  globally.
- Keep both supported wheels available: macOS arm64 for the local machine and
  manylinux x86_64 for future cloud deployment.
- Store broker credentials only in ignored local files such as `.env`
  and `secrets/`.
- Never commit API keys, person IDs, certificate passwords, `.p12`, or `.pfx`
  files.
- Use read-only smoke tests first. Do not place, modify, or cancel orders unless
  the user explicitly asks for live order execution and the runner is not in
  dry-run mode.

## Strategy Stage Model

Use explicit deployment stages instead of a single overloaded "production"
label:

1. `research_candidate`: idea or exploratory result.
2. `backtest_validated`: strict professional backtest passed.
3. `execution_ready`: target-book/order-level implementation is reproducible
   and reconciles to the validated strategy.
4. `live_pilot`: approved for small-capital live testing.
5. `production_scaled`: approved for unattended large-capital deployment.

A strong NAV-lineage backtest can promote a strategy to `backtest_validated`,
but broker order planning requires `execution_ready`. Large unattended capital
requires `production_scaled`.

By default, "the strongest strategy" means the highest-stage strategy registered
in `src/quantlib/trading/strategy_registry.py`. For current holdings or automated
trading targets, only use strategies at `execution_ready` or above. If the best
available strategy is only `backtest_validated`, say that clearly and do not use
it to drive live broker orders.

## Automated Trading Policy

The automated trading system must use only the single currently strongest
registered strategy. Do not blend multiple strategies, add discretionary picks,
or use lower-ranked backup strategies unless the user explicitly asks for that
design.

`src/quantlib/trading/auto_trader.py` must remain fail-closed:

- status and smoke-test commands may run read-only broker checks;
- daily trading must block when no `execution_ready` strategy exists;
- live execution requires both an explicit live command and `FUBON_DRY_RUN=false`;
- all generated plans and broker calls must record whether any order was placed.

## Strategy Reporting

When reporting any trading strategy result, always include the strategy's most
recent 1-year CAGR in addition to full-window and OOS metrics. If the latest
available data end date is not today, state the exact 1-year window used.

Strategy research, rankings, backtests, challenger comparisons, and deployment
candidate reports must include visual charts, not only tables. At minimum,
include an equity/NAV or P&L curve, a drawdown curve, and benchmark comparisons
against the relevant buy-and-hold alternatives. When the strategy involves
rotation, regime switching, changing exposure, or execution costs, add the
appropriate supporting charts such as rolling return/CAGR, rolling drawdown,
exposure, turnover, position count, or cost/fill diagnostics. Prefer HTML or a
similarly chart-friendly format when Markdown would make the report hard to
review.

