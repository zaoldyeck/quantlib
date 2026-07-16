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

**「更新」= 當日全表齊備,不是抓到多少算多少(2026-07-15 定案,第一手蒐證見
`docs/data_ops/twse_publish_times.md`)**:

> **D 日的資料自 D+1 00:30 起才算齊備 → 一天只跑一次完整更新,排在 D+1 盤前
> (每日 loop 07:20)。D 日盤中/傍晚不期待 D 的資料。**

依據(不是慣例,是官方明文):① 融資融券 `MI_MARGN` 的官方保證只到「**次一營業日
開市前公告**」(操作辦法 §69),D 日晚間抓得到是實務不是承諾;② 借券 `TWT93U`/TPEx
`sbl` 官方明文「每日晚間**二次**更新(約 20:30、22:30)」且時間隨日結浮動——20:30~22:30
之間抓到的是**部分更新**,檔案看起來完整卻會被改寫(無聲汙染,舊設定 21:30 正踩中);
③ 實證:margin/sbl/foreign/insider 四表全史零次同日成功,唯一「全表齊備」紀錄在 D+1 凌晨。

**為什麼重要**:抓一半 → **表間日期錯位** → 策略閘門查無資料就 fail-closed 把候選
靜靜砍光(2026-07-15 事故:報價 7/14、法人 7/13 → Evergreen「濾後候選 0」)。
`Task.dailyEndExclusive` 的各源時刻(quote/index 15:30、T86 16:00、stock_per_pbr 18:30、
margin/foreign 21:30、sbl **22:30**、insider 22:00)只用來**避免白跑的請求**,不是正確性
的來源;每個時刻的證據等級都註記在呼叫點。

**sentinel(休市日)規則**:只有「已過該日齊備時刻(D+1 00:30)」之後收到的乾淨無資料
才寫 0-byte sentinel;之前一律 `[deferred]` 刪檔重抓。sentinel 同時是我們的**休市日曆**
(`research/data_calendar.py::is_trading_day` 讀它)——颱風假無法從星期幾推得
(2026-07-10 即是颱風休市,平日 ≠ 交易日)。

**Python 側入口**:`research/data_calendar.py`(`latest_complete_trading_day()` /
`stale_tables()`);`research.tri.daily` 已內建齊備自檢——不齊備才跑一次完整更新。

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

**Rule(研發代碼永久留存,2026-07-10;上位通則見全域 CLAUDE.md §2.4
「有價值的工作產出必須落地 repo」)**: 本專案的具體化——策略研發程式碼
(回測、表生成、驗證、對比分析、引擎收割)一律先寫成 `research/<campaign>/`
下的正式檔案(docstring 註明用途、run 指令、是否依賴 cache)再以
`uv run --project research python -m ...` 執行;**完成即 commit(含負結果
harness——負結果是防重複試錯的資產)**。**禁止 Bash heredoc 一次性執行
研發邏輯**——heredoc 只准 ≤10 行的即拋查詢(查 schema、看幾筆資料)。
教訓(2026-07):apex/EV 系大量實驗曾以 heredoc 執行未落檔,592 筆 trial
代碼事後靠對話 transcript 逐字搶救(`research/apex/rebuild/recovered/`);
transcript 復原是最後保險,不是流程。LLM 標記產出依同等規格留存:標記
原始輸出 `label_runs/{month}.json`、搜尋材料 `ev28_news/{month}/`(邊搜
邊存 jsonl)、提示詞 `prompts/{month}.txt`——全部零 token 可重放。

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

**Skills in `.claude/skills/`** (auto-triggered workflows; Claude invokes on matching keywords):

| Skill | 觸發時機 |
|---|---|
| `serenity-trading-system` | 「**跑每日 loop**」「每日管理/檢查持倉」「產生下單計畫」「Serenity 選股/觀點/交易系統」「結構性瓶頸股」「找下一個 AXTI」— 現役單一交易策略的完整系統(選股+估值+出場+每日營運),自包含於 `.claude/skills/serenity-trading-system/`;每日 loop 見 `references/daily-ops.md` |
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

### 資料流極速鐵律(2026-07-17 定案;任何 session、任何新程式一律遵守,違者重寫)

**cache 是毫秒級的,慢永遠是程式的錯。**歷史事故:引擎單次回測拖到 ~10 分鐘,
逐段剖析後 5.1 秒(~120x)——五個病灶全是載入層寫法,不是資料層。硬性規則:

1. **禁止全表載入**:任何從 cache/PG 拉表的查詢,能按池/codes/日期過濾就必須過濾
   (`WHERE company_code IN (...)`)。全市場大表(daily_quote 900 萬、
   daily_trading_details 590 萬、stock_per_pbr 770 萬)原封進 pandas = 直接違規。
2. **rolling/groupby 留在 polars 或 SQL**,禁止 pandas `groupby().transform(lambda)`
   跑百萬行。
3. **重算物必須快取**:調整價格面板(back-adjustment)等昂貴衍生物一律磁碟快取,
   key 含 `cache.duckdb` mtime(資料世代一變即失效)。
4. **迴圈內禁止重複預處理**:對同一 DataFrame 反覆 as-of 篩選時,轉型/排序只做一次
   (預處理快取 + searchsorted),不准每次迭代 copy+sort。
5. **不用的資源不初始化**(如用不到的 view 註冊、備用通道的面板)。
6. **新 harness 出廠前必跑一次 `time` 實測**:單次回測 >30 秒即視為有病灶,cProfile
   剖到見骨才准交付;修完必附逐位等價驗證。

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
- **產業別一律用 `industry_taxonomy_pit`(2026-07-10 鐵律)** — 正規化 + PIT 的官方產業分類(修歷史舊名、記錄生效日防前視;`research/industry_taxonomy.py` 為建表源)。查詢模式:`asof` 取 `effective_date <= 觀察日` 的最新一筆。**禁止**直接用 `operating_revenue.industry`(舊檔含 legacy 名稱且無 PIT 語義)。

### 執行紀律事故記錄(永不再犯)

- **2026-07 Evergreen 偷工事故**:使用者明確指示「Agent 標記/蒸餾時要
  去搜尋消息面、題材資料」,被實作偷工成「憑訓練記憶回顧」且未告知,
  26 輪實驗(EV1-EV26)建立在偷工地基上,覆盤時還將偷工包裝成「回測
  形態天花板」。教訓:任何 Evergreen/標記/蒸餾 agent **不得限制工具**
  (WebSearch/WebFetch 全開),判斷輸入須與使用者指定的完整形態一致;
  對照全域天條第 00 條。

### Known Bug Patterns (Watch for Recurrence)

- **TWSE CSV schema drift** — columns get added silently. Readers with `case _` fall-through will silently misalign new-format rows; always use explicit `case 7 / case 8 / ...` dispatch.
- **TWSE partial/stale daily publish** — cross-date close-ratio > 2 or < 0.5 with no corporate-action = stale CSV. Fix: delete local CSV + DB row, re-curl, re-read.
- **Crawler filename-content date mismatch** — run `research/04_cross_verify.py` after any crawler change.
- **Quarterly partial import** — after `Main update`, sanity-check `SELECT year, quarter, COUNT(DISTINCT company_code) FROM concise_balance_sheet GROUP BY year, quarter` against ~1800 expected.
- **`Crawler.downloadFile` regex** stays `^\d{4}_.*\.csv$` — daily (YYYY_M_D.csv) AND quarterly (YYYY_Q_a_c_idx.csv) both need year-subdir routing.

### Exit Semantics Contract(2026-07-16 定調)

- **出場一律逐日重放價格路徑,禁止今日快照評估**(`research/trading/exit_replay.py`)。
  回測逐交易日評估、觸發當天出場;live 只看「今天的價格 vs 今天的止損線」會把
  「沒跑報告那幾天已觸發的出場」變成沒發生——**只在最沒紀律的時候放寬規則**。
  使用者定調:「我就算延遲了,該賣還是得賣,不能過時間了就當作沒發生」。
- **峰值(trailing 錨)由價格歷史重算**,不得用「跑報告時才更新」的增量 state
  (漏跑 → 峰值偏低 → 止損線偏低 → 該賣的沒賣)。峰值下限 = 該筆成交價
  (回測 `peak_close = entry_close` 的忠實對應)。
- **成本是帳戶的屬性,不是策略的屬性**(`research/trading/cost_basis.py` 單一
  來源):富邦 inventories 無成本欄位、filled_history 全回空 → 跨日成交價只能靠
  成交當天自己記帳(執行器 TCA jsonl 必須永久保存)。收養部位的「成本」是收養日
  收盤的**代理值**,報告必須標示 `(收養價)` 而非假裝是真成本。
- **報告零 LLM**:LLM 的判斷早就被壓成帶時間戳的資料(Serenity `thesis_registry`
  的 source_note/evidence_url/invalidation_criteria;Evergreen `registry_v3.parquet`
  + `ev28_news/{month}/materials.json` + `prompts/{month}.txt`),報告只做 join + render。

### Strategy Semantics Contract

- **現役交易策略(live,使用者指定單一策略)= Serenity 事件引擎 `ev_v3_wf`(2026-07-17 戰役十八換帥:walk-forward 驗證〔train 2022-07~2025-07 凍結 → OOS 四方對決乾淨勝 Evergreen P5 274.5% vs 172.2%〕+ EV43 式 refit 上場參數 = fresh12_nofilt 計分 × tp40/trail25/abs15/td30 × inst_neg × 10 席等權;血統審計 `research/serenity/data/live_config.json`;下次 refit 2027-01/07)。前任 `ev_v2_thesis_inst`** — 論點註冊表策展 × 事件紀律(止盈 +60% 回收 / trail -20% / abs -15% / time50 / **法人分佈出場** / 營收論點停損 + 雙 regime guard + live-book 收養協定)。驗證(cutoff 2026-07-06,計分經戰役十一~十三逐項消融驗證=8 成分全背書):lag0 CAGR 253.3% / MDD -18.0% / Sharpe 6.84 / Lo-t 4.29;置換檢定 p=0.000、DSR 1.00、bootstrap 5% 下界 +102.5%;富邦 realistic CAGR 271.6% / MDD -17.2% / 成交率 96.9%(摩擦 ~0.25% 名目)。**2026-07-14 戰役十五 role purity**:成員層三測試入法(SOP §1.5),beneficiary 14 檔除名(池 58→44,`member_roles.csv` 為姊妹檔);新池重驗證(cutoff 07-14):lag0 288.8% / MDD -16.9% / 置換 p=0.000 / DSR 1.00 / bootstrap 5% 下界 +111.4%;**PBO lag0 0.526(fold 稀疏 caveat 未解,回溯標記先導評估中)**;82 預註冊 trials;live OOS 對照帳 `docs/serenity/serenity_forward_track.md` 月結。文件:skill `serenity-trading-system`(`references/daily-ops.md` = 每日營運、`tw-event-engine.md` = 策略規格)+ `docs/serenity/serenity_event_engine_v1.md`。**鐵律:任何引擎變更必須先在 `docs/serenity/serenity_engine_trials_ledger.md` 預註冊假設與判準再跑**;策展維護依 `serenity_curation_sop.md`(註冊表是 alpha 源頭)。每日營運入口:`uv run --project research python -m research.serenity.daily run`(送單永遠是使用者的人工步驟)。**架構主權(血統原則,2026-07-07)**:Serenity 需求與 quantlib 舊慣例衝突時,一律改造專案服務 Serenity,不反向遷就(先例:事件驅動營收爬蟲、`research/serenity/` 獨立家);文字/質性資訊 fetch-on-demand 當下抓當下判斷(WebFetch → 使用者 Chrome),只存首見時間戳+策展蒸餾+量化資料,不建原文語料庫。
- **純量化冠軍(參考)= Iter95** — see [`docs/strategy_ranking.md`](docs/strategy_ranking.md)(同窗 2018-26 realistic 70.8% / MDD -22.1%;2026-07 純量化戰役確認無挑戰者勝出,見 `quant_campaign_ledger.md`)。歷代 ship 策略(Quality+Catalyst Hybrid 等)退役為歷史參考。
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

#### TPEx cache 覆蓋 — 已修復（2026-07-16 查證定案，舊「pipeline gap」記載作廢）

**cache 為全市場**：`research/cache_tables.py` 無 market filter（docstring 明寫
「Cache holds BOTH TWSE + TPEx rows」），daily_quote（TPEx 3.86M rows）、
stock_per_pbr、operating_revenue、ex_right_dividend、daily_trading_details 等
全表雙市場俱備、毫秒級。**規則**：research 腳本自行下顯式 `WHERE market=...`
選市場;`prices.fetch_adjusted_panel` 以 `market='twse'|'tpex'` 分拉再 concat。
**教訓（2026-07-16 池品質對決時發現）**：本段舊文宣稱「cache 只有 TWSE、高優先修」
已過時多時,導致回溯標記提示詞誤加「僅 TWSE」限制、對決腳本多繞 pg-attach——
**外部行為查文件,自家資料查 cache 本身**（`SELECT market, count(*) GROUP BY market`
十秒定案），文件陳述一律以實測為準。

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
- 法說會 (Task #86/87) — **2026-07-07 翻案(部分)**:行事曆+簡報連結已接入 Serenity 每日 loop(MOPS t100sb02_1,`research/serenity/daily.py`;事件庫 `research/data/confcall_events.parquet` 累積首見日供未來 event study);逐字稿 NLP 因子維持不做(舊結論仍成立:無截面排名價值),內容解讀走策展層 read-through + on-demand `twstock-confcall-analyzer`

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
