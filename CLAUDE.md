# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build & Run
```bash
# Compile the project
sbt compile

# Clean build artifacts
sbt clean

# Run the main application
sbt run

# Run with specific class
sbt "runMain Main"

# Package into JAR
sbt package

# Start SBT interactive shell
sbt
```

### Testing
```bash
# ScalaTest-based web controller specs (src/test/scala/web/):
sbt "test"

# Strategy unit tests live as *Test.scala inside src/main/scala/strategy/
# (run directly via runMain — they have `def main`):
sbt "runMain strategy.PublicationLagTest"
sbt "runMain strategy.UniverseTest"
sbt "runMain strategy.BacktesterTest"
sbt "runMain strategy.MomentumValueStrategyTest"
```

### Dependency Management
```bash
# Reload build configuration after changes
sbt reload

# Show dependency tree
sbt dependencyTree

# Update dependencies (modify build.sbt and reload)
sbt reload
```

### Database Operations
```bash
# Start PostgreSQL service (adjust for your system)
# macOS: brew services start postgresql
# Linux: sudo systemctl start postgresql

# Connect to database
psql -h localhost -p 5432 -d quantlib
```

### Data Migration
```bash
# Migrate existing data files into year-based subdirectories
./migrate_data.sh
```

### Data Refresh Workflow (MANDATORY order)

Whenever Taiwan-market data needs to be up-to-date (daily routine, before
starting a research session, after a long break), run these **in order**:

```bash
# 1. Crawl latest from TWSE/TPEx/MOPS + import to PostgreSQL
sbt "runMain Main update"

# 2. Sync PostgreSQL → local DuckDB cache (3-4 min one-time)
uv run python research/cache_tables.py
```

Why both steps are required:

- Step 1 updates PostgreSQL with newly crawled rows (daily quotes, monthly
  revenue, quarterly financials, etc.).
- Step 2 rebuilds `research/cache.duckdb` as a columnar snapshot.
  Without step 2, Python research scripts still read **stale cached data**,
  so new bugs found by freshly-imported rows won't show up.
- Skipping step 1 but doing step 2 fills the cache with outdated pg data.
- Skipping step 2 but doing step 1 means Python scripts (`research/v4.py`,
  `research/01_find_spikes.py`, etc.) run ~15x slower (~87s vs 5.6s) because
  they fall back to live pg scan instead of local DuckDB.

**Rule**: any new research script under `research/` **MUST** document at
the top whether it needs `cache_tables.py` to be current.

### Research Scripts (Python + Polars + DuckDB)

Fast iteration harness under `research/`. Reads from local DuckDB cache for
ms-level queries.

```bash
# Prerequisite: ensure cache is fresh (see "Data Refresh Workflow" above)

# Find stocks with N-day >X% price surges
uv run python research/01_find_spikes.py --min-gain 0.80 --window 60

# Detect market-wide data anomalies (row counts, extreme values)
uv run python research/02_anomaly_scan.py --min-stocks 20

# Full data-integrity audit across all tables
uv run python research/03_full_data_audit.py

# Verify filename-date matches CSV content-date
uv run python research/04_cross_verify.py

# Audit operating_revenue for zero/negative/extreme-YoY rows
uv run python research/05_revenue_audit.py

# Python port of v4 RegimeAwareStrategy (~5s vs Scala ~10-15 min)
uv run python research/v4.py
```

Speed comparison (2018-2026 v4 backtest):

| Stack                             | Runtime |
|-----------------------------------|---------|
| Scala + Slick                     | 10-15 min |
| Python + DuckDB attached-pg       | 87 s |
| **Python + DuckDB local cache**   | **5.6 s** |

Use the Python harness for **research iteration** (trying variants, tuning
thresholds, scanning for bugs). Use the Scala `Main strategy` command for
**production-grade validation** since its per-trade cost accounting is
exact while the Python version approximates turnover via pick-overlap.

## Research Tooling (Python ecosystem under `research/`)

Detailed usage + adapters in memory file `project_research_tooling.md`. Quick picker:

| Need | Tool | One-liner entry |
|---|---|---|
| Factor IC / quantile / turnover analysis | **alphalens-reloaded** | `al.tears.create_full_tear_sheet(factor_data)` |
| Portfolio tear-sheet (Sharpe/MDD/heatmap) | **pyfolio-reloaded** | `pf.create_returns_tear_sheet(daily_returns)` |
| Standalone performance metric | **empyrical-reloaded** | `ep.sharpe_ratio(returns)`, `ep.max_drawdown(returns)` |
| Grid search over strategy params | **vectorbt** | `vbt.Portfolio.from_signals(prices, entries, exits)` |
| Technical indicators (RSI/MACD/KD/布林) | **stockstats** | `wrap(df)['macd']` auto-computes |
| ML-predicted alpha (LightGBM/LSTM) | **Qlib** | see `research_tooling.md` for TW data converter |
| Intraday quote / real order / tick data | **shioaji** (永豐 API) | needs 大戶投 account + API key, stored in `.env` |

**Agents in `.claude/agents/`** (12 agents, 9 from TradingAgents + 3 quantlib-specific):

| Agent | 適用場景 |
|---|---|
| `twstock-fundamental-analyst` | 個股基本面 |
| `twstock-technical-analyst` | 技術面 / 資金流 |
| `twstock-news-analyst` | 新聞 + 公告事件 |
| `twstock-sentiment-analyst` | PTT / 散戶情緒 |
| `twstock-bull-researcher` | 看多論述 |
| `twstock-bear-researcher` | 看空論述 |
| `twstock-trader` | 綜合進出場決策 |
| `twstock-risk-manager` | 風險否決 |
| `twstock-portfolio-manager` | 最終批准 + mandate 對照 |
| `quantlib-factor-researcher` | 新因子設計 + IC 測試 |
| `quantlib-data-auditor` | 資料完整性 audit |
| `quantlib-backtest-runner` | Backtest + baseline 對比 |

Agents are on-demand subagents (`$\_\_ 呼叫 agent "..."`), use Claude Code subscription (zero API cost vs TradingAgents' per-query LLM spend).

## Gotchas & Contracts

### Bash & JVM

- **Bash tool `cd` persists between calls** — always use absolute paths, e.g. `cd /Users/zaoldyeck/Documents/scala/quantlib/research && ...`.
- **Run Python research from repo root**: `uv run --project research python research/v4.py` (NOT `cd research && python v4.py`).
- **`Main.run()` must `sys.exit(0)` in `finally`** — Akka ForkJoinPool + non-daemon workers keep JVM alive 30-60s otherwise.
- **Long Scala commands need generous timeout** — `Main update` / `Main strategy` can take 10+ min; Bash timeout ≥ 1200000ms.

### Data Judgement Rules (NOT bugs)

- **Negative `monthly_revenue` in financial/securities sector** is real — FX / valuation losses; always has `備註` field. Check before flagging.
- **Zero `monthly_revenue` for construction stocks** is real — revenue recognized on project handover (completed-project basis).
- **Saturday sessions with <50% row count** are real TWSE makeup trading days, not partial CSV.
- **`concise_*` tables have no `market` column** — filter via `company_code` prefix or join with a table that has market.

### Known Bug Patterns (Watch for Recurrence)

- **TWSE CSV schema drift** — columns get added silently. Readers with `case _` fall-through will silently misalign new-format rows; always use explicit `case 7 / case 8 / ...` dispatch.
- **TWSE partial/stale daily publish** — cross-date close-ratio > 2 or < 0.5 with no corporate-action = stale CSV. Fix: delete local CSV + DB row, re-curl, re-read.
- **Crawler filename-content date mismatch** — run `research/04_cross_verify.py` after any crawler change.
- **Quarterly partial import** — after `Main update`, sanity-check `SELECT year, quarter, COUNT(DISTINCT company_code) FROM concise_balance_sheet GROUP BY year, quarter` against ~1800 expected.
- **`Crawler.downloadFile` regex** stays `^\d{4}_.*\.csv$` — daily (YYYY_M_D.csv) AND quarterly (YYYY_Q_a_c_idx.csv) both need year-subdir routing.

### Strategy Semantics Contract

- **`ValueRevertStrategy.rebalanceDates` MUST use `minDay=1`** (month-start) — month-mid rebalance loses ~8pp CAGR on pbBand factor.
- **Python `research/v4.py` must match Scala within 1pp CAGR** — drift >1pp → check (a) asof-join +1-day shift (new picks effective T+1), (b) per-rebal turnover = `|prev ∩ cur| / TOPN`.
- **`Backtester.CommissionRate` default `0.000285`** (2-折 e-trading) — don't reset to 0.001425 without reason.
- **`Output.writeNavChart` must pass `openInBrowser = false`** — never auto-open Chrome on backtest.

## Architecture Overview

This is a Taiwan stock market financial data crawler and quantitative analysis system written in Scala. The system follows a layered architecture with four main stages:

### 1. Setting Layer (`setting/` package)
- **Purpose**: Configuration management and URL construction
- **Key Components**:
  - `Setting` trait: Base interface for all data source configurations
  - `Detail` abstract class: Handles URL construction, file path management, and duplicate detection logic
  - Concrete implementations: `DailyQuoteSetting`, `FinancialAnalysisSetting`, etc.
- **Data Sources**: Reads from `application.conf` to configure API endpoints for TWSE (Taiwan Stock Exchange) and TPEx (Taipei Exchange)

### 2. Task Layer (`Task.scala`)
- **Purpose**: High-level coordination and scheduling of data collection
- **Key Methods**:
  - `pullAllData()`: Orchestrates downloading of all data types
  - `pullDailyFiles()`: Handles daily data with duplicate avoidance
  - `pullQuarterlyFiles()`: Manages quarterly financial reports
  - `createTables()`: Database schema initialization
- **Smart Features**: Automatically skips existing files and handles date ranges

### 3. Crawler Layer (`Crawler.scala`)
- **Purpose**: Low-level HTTP operations and file management
- **Features**:
  - Handles both CSV downloads and form-based data retrieval
  - Automatic year-based directory organization
  - Special handling for bulk data files (capital reduction, ex-dividend)
  - Retry logic and rate limiting

### 4. Reader Layer (`reader/` package)
- **Purpose**: Parse downloaded files and load data into PostgreSQL database
- **Key Components**:
  - `TradingReader`: Handles market data (quotes, indices, trading details)
  - `FinancialReader`: Processes financial statements and analysis data
  - Uses Slick ORM for database operations with parallel processing

### 5. Strategy Layer (`strategy/` package)
- **Purpose**: Backtest framework + factor research + strategy implementations
- **Engine**: `Backtester.scala` (daily NAV walk, DRIP, split detection, delta rebalance)
- **Signals**: `Signals.scala` (32 factor functions), `PublicationLag.scala` (PIT filing dates)
- **Universe**: `Universe.scala` (TWSE common stocks, ADV >= NT$50M)
- **Metrics**: `Metrics.scala` (CAGR / Sharpe / MDD), `RankMetrics.scala` (IC / t-stat)
- **Research harness**: `FactorResearch.scala` (batch IC + pairwise correlation)
- **Strategies** (15 variants): `MomentumValueStrategy`, `ValueRevertStrategy` (v3), `RegimeAwareStrategy` (v4 champion), `MagicFormulaPiotStrategy`, `MultiFactorStrategy`, `DividendYieldStrategy`, `ValueMomentumStrategy`, `Hold0050Strategy` (benchmark)
- **Output**: `Output.scala` (Plotly HTML + CSV trades/monthly-NAV; `openInBrowser=false`)

### 6. Python Research Layer (`research/` directory)
- **Purpose**: Fast iteration harness (5s backtest vs Scala 10-15 min)
- **Stack**: Polars (columnar DataFrame) + DuckDB (embedded OLAP) + uv (deps)
- **Cache**: `research/cache.duckdb` (local columnar copy of pg, 3-4 min sync)
- **Scripts**: `v4.py` (v4 port), `01_find_spikes.py`, `02_anomaly_scan.py`, `03_full_data_audit.py`, `04_cross_verify.py`, `05_revenue_audit.py`
- **Entry**: `uv run --project research python research/<script>.py`

## Data Organization

The system manages two types of data storage:

### Year-Based Storage (Most Data)
- Path pattern: `data/{category}/{market}/{year}/`
- Categories: `daily_quote`, `daily_trading_details`, `index`, `margin_transactions`, `stock_per_pbr_dividend_yield`, `operating_revenue`, `balance_sheet`, `income_statement`, `capital_reduction`, `ex_right_dividend`
- Markets: `twse` (Taiwan Stock Exchange), `tpex` (Taipei Exchange)

### Direct Storage (Small Datasets)
- Path pattern: `data/{category}/{market}/`
- Categories: `financial_analysis`, `etf`, `financial_statements`

## Database Schema

### Core Tables (Slick-managed)
- Tables defined in `db/table/` package using Slick table definitions
- Primary tables: `DailyQuote`, `FinancialAnalysis`, `BalanceSheet`, `IncomeStatement`, `OperatingRevenue`, etc.
- Database: PostgreSQL (configurable to MySQL or H2 in `application.conf`)

### Views & Materialized Views
- SQL files in `src/main/resources/sql/`
- `materialized_view/`: Preprocessed financial statement views
- `view/`: Analysis views for growth, valuation, and financial metrics
- Auto-created during `task.createTables()`

### Derived Tables / Views Used by Strategy Layer
- `concise_balance_sheet` + `concise_income_statement_progressive` — wide-form financial statements (populated by `readBalanceSheet` / `readIncomeStatement`; REFRESH mat views after import)
- `growth_analysis_ttm` (view) — Piotroski F-Score + drop_score + 5y growth/decline flags
- `financial_index_ttm` (view) — ROIC, ROE, margin, cash-flow ratios (trailing twelve months)
- `stock_per_pbr_dividend_yield` (table) — daily P/E, P/B, dividend yield from TWSE
- **Query pattern**: strategies use Plain SQL (`sql"""..."""`) not Slick lifted DSL — views have Chinese + `%` column names that lifted DSL can't map cleanly

## Entry Points

### Main Application (`Main.scala`)

Main is a scopt CLI with 5 subcommands (each copy-paste ready):

```bash
sbt "runMain Main update"                               # crawl + read all
sbt "runMain Main pull daily_trading_details"           # crawl one target
sbt "runMain Main read stock_per_pbr"                   # re-import one target
sbt "runMain Main research --start 2018-01-02"          # factor IC scan
sbt "runMain Main strategy regime_aware --start 2018-01-02 --end 2026-04-17"
```

Valid `<target>` for pull/read: `daily_quote | daily_trading_details | index | margin | stock_per_pbr | capital_reduction | ex_right_dividend | operating_revenue | balance_sheet | income_statement | financial_analysis | financial_statements | etf | all`

Valid `<variant>` for strategy: `momentum_value | alpha_stack | value_revert | regime_aware | regime_hyst | value_momentum | regime_value_momentum | multi_factor | regime_multi | dividend_yield | regime_yield | mf_piot | mf_piot_norsv | mf_raw | regime_mf_piot`

### Job Orchestration (`Job.scala`)
Provides high-level workflows combining multiple operations across different data types.

## Key Configuration

### Database Connection (`application.conf`)
- Default: PostgreSQL on localhost:5432, database 'quantlib'
- Alternate configurations available for MySQL and H2
- Connection pooling via HikariCP

### Data Source URLs
- All API endpoints configured in `application.conf` under `data` section
- Supports both direct download URLs and form-based endpoints
- Date parameters are dynamically inserted by `Detail` implementations

## Environment Setup

### Prerequisites
- Java 8+, SBT 1.10.5 (see `project/build.properties`), PostgreSQL on localhost:5432, database `quantlib`

### Initial Setup
```bash
createdb quantlib
sbt compile
sbt "runMain Main update"   # first run: creates tables, then crawls + imports
```

### Key Config Files
- `src/main/resources/application.conf` — DB connection, data source URLs, pool sizes
- `build.sbt` — Scala 2.13.15 + Slick 3.3.3 + Play WS + scopt
- `project/build.properties` — SBT version pin

---

## Web Application Extension (scaffolded, not active focus)

- `backend/` — Play Framework 2.9 + Scala scaffold (`AuthController`, `StockController`, `PortfolioController`, `WatchlistController`, `MarketController` + service layer)
- `frontend/` — Next.js 14 + TypeScript scaffold
- `docker-compose.yml` — PostgreSQL + Redis + backend + frontend full stack
- `specs/001-src-main-scala/` — full spec (plan.md, research.md, data-model.md, contracts/openapi.yaml, quickstart.md)
- **Current status**: scaffolded but inactive — active work focuses on `strategy/` + `research/`
- **When resuming web work**: read `specs/001-src-main-scala/plan.md` for Play/Next.js/JWT/Redis design decisions before changing scaffold

```bash
# Backend dev:     cd backend && sbt run              (port 9000)
# Frontend dev:    cd frontend && npm run dev         (port 3000)
# Full stack:      docker-compose up
```
