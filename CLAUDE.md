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
- Skipping step 2 but doing step 1 means Python scripts (`research/strat_lab/v4.py`,
  `research/audits/01_find_spikes.py`, etc.) run ~15x slower (~87s vs 5.6s) because
  they fall back to live pg scan instead of local DuckDB.

**Rule**: any new research script under `research/` **MUST** document at
the top whether it needs `cache_tables.py` to be current.

### Research Scripts (Python + Polars + DuckDB)

Fast iteration harness under `research/`. Reads from local DuckDB cache for
ms-level queries.

```bash
# Prerequisite: ensure cache is fresh (see "Data Refresh Workflow" above)

# Find stocks with N-day >X% price surges
uv run python research/audits/01_find_spikes.py --min-gain 0.80 --window 60

# Detect market-wide data anomalies (row counts, extreme values)
uv run python research/audits/02_anomaly_scan.py --min-stocks 20

# Full data-integrity audit across all tables
uv run python research/audits/03_full_data_audit.py

# Verify filename-date matches CSV content-date
uv run python research/audits/04_cross_verify.py

# Audit operating_revenue for zero/negative/extreme-YoY rows
uv run python research/audits/05_revenue_audit.py

# Python port of v4 RegimeAwareStrategy (~5s vs Scala ~10-15 min)
uv run python research/strat_lab/v4.py
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
| Intraday quote / real order / tick data | **shioaji 1.3.3** (永豐 API) + Sinotrade 官方 `/shioaji` Skill | needs 大戶投 account + API key, stored in `.env`；Skill 提供 reference / runtime 由 py 套件執行 |

**Agents in `.claude/agents/`** (22 agents):

### 個股研究

| Agent | 適用場景 |
|---|---|
| `twstock-fundamental-analyst` | 個股基本面 |
| `twstock-technical-analyst` | 技術面 / 資金流 |
| `twstock-news-analyst` | 新聞 + 公告事件 |
| `twstock-sentiment-analyst` | PTT / 散戶情緒 |
| `twstock-confcall-analyzer` | 法說會內容 NLP 分析 |
| `twstock-eps-revision-tracker` | 分析師 EPS 上修 / 下修動量 |
| `twstock-news-classifier` | 新聞 catalyst 分類 + 影響股票 |
| `twstock-industry-analyst` | 產業景氣循環 + 上下游 + 供應鏈 |

### 多空辯論 + 決策

| Agent | 適用場景 |
|---|---|
| `twstock-bull-researcher` | 看多論述 |
| `twstock-bear-researcher` | 看空論述 |
| `twstock-trader` | 綜合進出場決策 |
| `twstock-risk-manager` | 風險否決 |
| `twstock-portfolio-manager` | 最終批准 + mandate 對照 |

### 持倉 / 部位管理

| Agent | 適用場景 |
|---|---|
| `twstock-position-reviewer` | 持倉檢查 + hold / trim / exit 建議 |
| `twstock-rebalance-recommender` | 產出 rebalance buy/sell orders |
| `twstock-scenario-tester` | Forward 情境壓力測試 |
| `twstock-playbook-generator` | 個股化 entry/exit 規則 |
| `twstock-forward-predictor` | LightGBM 預測 1y forward 報酬 |

### 量化研究

| Agent | 適用場景 |
|---|---|
| `quantlib-factor-researcher` | 新因子設計 + IC 測試 |
| `quantlib-data-auditor` | 資料完整性 audit |
| `quantlib-backtest-runner` | Backtest + baseline 對比 |
| `quantlib-strategy-validator` | **Walk-forward + MC + DSR + PBO professional validation**（策略出廠前必跑）|
| `quantlib-emerging-leader-scan` | **季度 scan TWSE/TPEx 找新興利基龍頭**（量化 pre-screen + WebSearch 驗證 + 對 `docs/leaders_by_domain.md` 提建議）|

Agents are on-demand subagents, use Claude Code subscription (zero API cost vs TradingAgents' per-query LLM spend).

**Skills in `.claude/skills/`** (6 auto-triggered workflows; Claude invokes on matching keywords):

| Skill | 觸發時機 |
|---|---|
| `quantlib-data-refresh` | 「更新資料」「sync data」「refresh cache」 |
| `quantlib-backtest` | 「跑 X 策略」「compare A vs B」「threshold sweep」 |
| `quantlib-factor-test` | 「測試 XXX 因子」「看 YYY 有沒有 IC」 |
| `quantlib-stock-deepdive` | 「分析 XXXX」「深入看 YYYY」（並行呼叫 6 個 agent）|
| `quantlib-data-health` | 「檢查資料」「audit」「這 anomaly 是 bug 嗎」 |
| `quantlib-spike-study` | 「暴漲股研究」「飆股」「為什麼這支漲」「spike event study」 |

**Slash commands in `.claude/commands/`**:

| Command | 用途 |
|---|---|
| `/spike-dossier <ticker> <date>` | 單一暴漲股完整 dossier（quant + news trace + 類比案例） |

## Gotchas & Contracts

### Bash & JVM

- **Bash tool `cd` persists between calls** — always use absolute paths, e.g. `cd /Users/zaoldyeck/Documents/scala/quantlib/research && ...`.
- **Run Python research from repo root**: `uv run --project research python research/strat_lab/v4.py` (NOT `cd research && python v4.py`).
- **`Main.run()` must `sys.exit(0)` in `finally`** — Akka ForkJoinPool + non-daemon workers keep JVM alive 30-60s otherwise.
- **Long Scala commands need generous timeout** — `Main update` / `Main strategy` can take 10+ min; Bash timeout ≥ 1200000ms.

### Database Schema Contract (Slick FRM)

- **ALL PG tables MUST be created via Slick** (`TableQuery[X].schema.createIfNotExists`), never raw `psql CREATE TABLE`. Slick `class X extends Table[Tuple]` is single source of truth for columns + types + unique indexes — compiler validates type-safe queries (`x.filter(_.date === d).result`) against this definition. Raw DDL drifts from Slick at runtime.
- **When adding a new data source**:
  1. Write `src/main/scala/db/table/XxxTable.scala` (Slick definition)
  2. Register in `Task.createTables()` with `xxx.schema.createIfNotExists`
  3. Run `sbt "runMain Main init"` to materialize (idempotent)
  4. **Never** CREATE TABLE via psql, even for "quick test" — it poisons FRM parity
- **`Main init`** is idempotent (tables only). Views/matviews are NOT — their `.sql` files use plain `CREATE VIEW` and will fail on re-run. Use `Task.createTablesAndViews()` only on fresh DB setup.
- **If schema changes**: modify Slick `Table[Tuple]` first, then `ALTER TABLE` in psql to match (Slick has no migration framework — maintain parity manually). Drop+recreate acceptable when table has <100K rows and data is re-importable from `data/`.
- **`createIfNotExists` gotcha**: works for tables only; indexes are re-attempted each run. If an index name collides on re-run, `DROP INDEX` first. Use unique index names prefixed with table camelCase (e.g. `idx_ForeignHoldingRatio_market_date_code`).

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

- **Ship-ready strategy ranking + execution runbook** — see [`docs/strategy_ranking.md`](docs/strategy_ranking.md). 主策略 `Quality + Catalyst Hybrid (5+5, NAV 85/15, ATR trailing, TWSE+TPEx)`（iter_13 monthly mcap TPEx + iter_24 max=5 ATR）通過 **6/6 OOS PASS real alpha** (OOS CAGR +24.39% / Sortino 1.535 / Boot LB +11.74% / multi-config PBO 0.408)。同時持倉硬上限 = 10 檔（5+5）。Cross-validation 5 個 ranker 證實非賭 TSMC（4 sensible ranker max gap 0.933，composite 是 outlier 失敗）。跨 cycle 切片：2008 GFC -23.9%、2009 +27.1%、2008-09 雙年合計 -1.6%、2022 growth crash -27.9%（系統性熊市 vs 2330 同 beta）。
- **Canonical pricing module (`research/prices.py`, 2026-04-30+)** — ALL NAV simulation MUST go through `prices.fetch_adjusted_panel` (or helpers `daily_returns_from_panel` / `fetch_daily_returns` / `total_return_series`). Reading raw `daily_quote.closing_price` and running daily NAV systematically under-counts cash dividend reinvestment and ignores capital reduction reference resets — historical bug that under-stated iter_20 / iter_24 NAV ~3-6pp CAGR over 21y. See `feedback_canonical_prices_module.md` memory and `research/tests/test_prices.py` for the 10-test parity suite (incl. cross-implementation check vs `research/analyses/active_etf_metrics.py`).
- **Python is the canonical research engine** (`research/strat_lab/v4.py` + `vectorbt` + `alphalens`). Scala `strategy/` package is **frozen** as historical reference — no new strategies, no factor research, no backtesting there.
- **Month-start rebalance** (`minDay=1`) is **v4 legacy only**. New strategies應該傾向 **event-driven daily**（見下一節）。月頻只在證明比事件驅動好時才用。
- **Commission 0.0285%** (2-折 e-trading via 國泰/富邦/永豐), **sell tax 0.3%** — hardcoded in `research/strat_lab/v4.py`; copy these constants to any new strategy. 若用 vectorbt，symmetric-baked `fees=0.001785` per side（round-trip = 0.00357）。
- **Per-rebal turnover cost** = `|prev ∩ cur| / TOPN × (SELL_TAX + 2 × COMMISSION)` — avoid flat 100% turnover assumption; match research/strat_lab/v4.py formula.
- **Asof-join +1 day shift**: new picks effective T+1, not T (trade at today's close). Any Python backtest doing monthly rebalance must offset the pick assignment by +1 trading day.
- **Speed over bit-exact accuracy** — 5-second iterations are worth >1pp CAGR approximation noise. Do not add Python-side complexity to shave final % points.

### Strategy Design Principles (新策略開發必讀)

**1. Event-driven 優於 calendar-driven**
- V4 月初換股是 Scala 時期的妥協，**不是最佳設計**。新策略預設**每日掃描 + signal-triggered entry/exit**。
- Legacy v4 月頻 rebalance 反應延遲 ~15 天（月中資訊要等下月 1 日才生效）；event-driven < 1 天。
- 實作：vectorbt `Portfolio.from_signals` 或自訂 daily loop。每日產生 candidate pool + 風控出場 → 對 TWSE ~1,144 支 × 2,746 天在 cache 環境下 < 60 秒可完成 full backtest。

**2. 資金分配預設 `TargetPercent` 固定比例（自然複利）**
- 預設：`size=1/TOPN, size_type="TargetPercent"`，例如 TOPN=10 即每檔目標 10% NAV
- 避免 `fixed dollar`（不複利）、避免 `percent of cash`（位置越多單筆越小）
- 進階：vol-targeted sizing（每檔目標 1% daily NAV σ）—等 strategy 第一版有效再升級

**3. 出場必須多條件 OR，不能只 trailing stop**
- Trailing stop 15%（從持倉高點回跌）
- Factor signal reversal（原始進場理由消失，例 pbBand pct 漲到後 30%）
- Time stop 120 交易日（避免 grind）
- Absolute stop -20%（防止單筆爆炸）
- Regime flip（整體回撤 > 25% 減碼）

**4. 所有新策略必須通過 `quantlib-strategy-validator` agent 驗證才能 ship**
- Walk-forward OOS（5 年 train / 1 年 test，滾動）
- Monte Carlo permutation p-value < 0.05
- Bootstrap 95% CI lower bound > 10% CAGR
- DSR > 0.95（若測過多個 config）
- PBO < 0.5（overfit 機率）
- Robustness grid（參數 ±20% CAGR spread < 15pp）
- **禁止**：只報 in-sample number 就聲稱「有 alpha」

### Data Roadmap

#### Pipeline gap — TPEx 在 PG 卻被 cache filter 擋掉（高優先修）

**Scala crawler 已下載 TPEx 全套資料到 PG**（7 tables × 2007 起），但 `research/cache_tables.py` 所有 `CREATE TABLE ... WHERE market='twse'` 把 TPEx 整個 filter 掉，research pipeline 系統性漏掉 ~75% 樣本。

**修正步驟（執行前必照此順序）**：
1. 先在 `research/strat_lab/v4.py` 等既存 query 加顯式 `WHERE market='twse'` filter（否則 cache 加 TPEx 後 baseline 會改）
2. 修 `research/cache_tables.py` 移除 `WHERE market='twse'`（保留所有市場）
3. 重跑 `uv run python research/cache_tables.py`（~5 分鐘）
4. 新實驗腳本明確選擇 `market='twse'` / `'tpex'` / 兩者

**TPEx 資料量**（vs TWSE 同表）：
- daily_quote: 3.81M rows (73% of TWSE)，2007-07-02 起
- margin_transactions: 2.88M rows，2007-01-02 起
- stock_per_pbr: 3.29M rows，2007-01-02 起
- operating_revenue: 208K rows（TPEx 無工業分類時也有營收）

暴漲股多在 TPEx（小型股、生技、IC 設計），chase 策略納入 TPEx 後 universe 擴大、訊號樣本翻倍。

#### 資料下載狀態 (Sprint A 完成 2026-04-24)

| 優先 | 資料 | 狀態 | 說明 |
|---|---|---|---|
| ⭐⭐⭐ | **TDCC 集保股權分散（17 級距）** | ✅ Sprint A | weekly，`tdcc_shareholding` table。Endpoint 只給當週，歷史回補 pending (Task #20) |
| ⭐⭐ | **TWSE 借券賣出餘額 (TWT93U)** | ✅ Sprint A | daily，`sbl_borrowing` table。從 2016-01-04 起可回溯 |
| ⭐⭐ | **TPEx 借券賣出餘額** | ⏸ pending | endpoint 未找到 (Task #19)，需 Playwright MCP 探查 |
| — | **MOPS 重大訊息公告** | 歷史不做 | 資料量大；改為 live-monitor 策略啟動時才補（Task #14）|
| ⭐⭐ | **內部人持股轉讓** (事前申報日報) | ✅ **2026-04-29** | MOPS t56sb12_q1/q2 daily bulk; 2-step ajax (step1 → step2 with report=SY\|OY); TWSE+TPEx 各 4-6 筆/日，2007+ 可回溯 (Task #21) |
| ⭐⭐ | **庫藏股** | ✅ **2026-04-29** | MOPS t35sc09，TWSE 1637 + TPEx 1239 rows，single-shot snapshot (Task #22) |
| ❌ | **現金增資 / 可轉債** | **取消** | MOPS 沒有結構化 bulk announcement endpoint；可行 endpoint 都需 LLM/keyword 分類 free-text 事由，違反「不用 LLM 分類」原則 (Task #23 deleted) |
| ⭐ | **個股外資持股比例** | 可選 | 和 daily_trading_details flow 重疊（Task #24） |

**Sprint B 啟動條件**：Sprint A 的 TDCC + SBL 數據回測證明籌碼面訊號有效（longest-window CAGR ≥ 22%，beat 0050 by ≥ 3pp），才投入 MOPS anti-bot 爬蟲。

**2026-04-29 Sprint B 階段成果**：
- 庫藏股 working — 全 db schema (`treasury_stock_buyback`)、cache view、reader、CLI (`Main pull buyback` / `read buyback`) 全 done
- 內部人 / 現增 — 全套 code 寫好（Setting / Table / Reader / Crawler / CLI）但 endpoint server 對 IP/TLS fingerprint 直接 close，需 Playwright 階段
- 取消法說會 (Task #86/87) — LLM-only / 無 cross-section ranking 價值，改 on-demand `twstock-confcall-analyzer` agent 查詢

補資料時務必 **加到 `research/cache_tables.py`** 讓 cache 重建帶入，並 **加到 `research/db.py`** 讓 pg-attach 模式有對應 view（parity）。

#### CLI Targets for Sprint A 新資料

```bash
# TDCC (weekly 集保股權分散 — 每週五/六跑一次即可，opendata 只給當週)
sbt "runMain Main pull tdcc"          # download current week snapshot
sbt "runMain Main read tdcc"          # parse + insert (data_date dedupe)

# SBL (daily 借券賣出餘額 — 可指定 since)
sbt "runMain Main pull sbl --since 2026-04-21"   # 近期補抓
sbt "runMain Main pull sbl --since 2016-01-04"   # 完整歷史（~2500 交易日 × 20s sleep ≈ 14h，建議過夜）
sbt "runMain Main read sbl"
```

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
