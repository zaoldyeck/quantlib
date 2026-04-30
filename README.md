# QuantLib — 台股量化研究系統

雙層架構：

1. **資料層 (Scala)** — 從 TWSE / TPEx / MOPS / TDCC 爬取股價、財報、籌碼、MOPS 結構化公告，存入 PostgreSQL
2. **研究層 (Python)** — 在本地 DuckDB cache 上跑策略 backtest、OOS 驗證、event study

**Ship-ready 主策略**：[`strict 5+5 NAV 85/15 with C+B`](docs/strategy_ranking.md)
- 子策略 A: iter_13 monthly mcap top 5 quality pool (TWSE+TPEx) — 5 檔
- 子策略 B: iter_24 max=5 catalyst breakout + ATR trailing (TWSE+TPEx) — 5 檔
- 持倉硬上限 = 10 檔（5+5），每年初 rebal 回 85/15
- 21y in-sample CAGR +22.87% / Sortino 1.416 / MDD -44.85%
- 16 fold walk-forward **OOS** CAGR **+24.39%** / Sortino **1.535** / Sharpe 1.032
- Lo p=1.13e-5、Boot CAGR LB +11.74%、DSR 0.954
- **三維全勝 2330 hold**（CAGR +0.16pp / Sortino +0.20 / MDD +0.86pp）
- Cross-validation 證實非賭 TSMC（mcap 1.512 vs roa_med 0.635 合理差距）
- ✅ Verdict: 6/6 OOS PASS real alpha（multi-config PBO 0.408 < 0.5）
- ✅ 跨 cycle 切片驗證：2008 GFC -23.9% / 2009 +27.1% / 雙年合計 -1.6%（系統性熊市 vs 反彈互抵）

---

## 文件入口

| 想找 | 去哪 |
|---|---|
| **最終策略排行 + 執行手冊** | [`docs/strategy_ranking.md`](docs/strategy_ranking.md) |
| **主動 ETF 同窗口比較** | [`docs/active_etf_analysis.md`](docs/active_etf_analysis.md) |
| **各領域龍頭股 master 清單** | [`docs/leaders_by_domain.md`](docs/leaders_by_domain.md) |
| **Python 研究目錄結構** | [`research/README.md`](research/README.md) |
| **開發鐵則 / data / coding 規範** | [`CLAUDE.md`](CLAUDE.md) |

---

## 專案架構

```
.
├── src/main/scala/                Scala 資料爬蟲 + DB schema
│   ├── Main.scala                 CLI 入口（scopt：update / pull / read / strategy）
│   ├── Crawler.scala              HTTP 下載 + 年份目錄分流
│   ├── Task.scala                 高階任務協調
│   ├── Job.scala                  跨任務組合
│   ├── reader/                    CSV / HTML / JSON 解析 → DB
│   ├── setting/                   各資料源 URL / 目錄設定
│   ├── db/table/                  Slick 表格定義（22 張表）
│   └── strategy/                  歷史 Scala 策略（frozen，研究全面 Python）
│
├── research/                      Python 量化研究（uv 管理）
│   ├── prices.py                  ⭐ canonical 還原 OHLCV（cash_div + cap_red）
│   ├── db.py                      DuckDB 連線（attach PG 或讀 cache.duckdb）
│   ├── cache_tables.py            PG → DuckDB cache 同步
│   ├── strat_lab/                 策略 + validator + tools
│   │   ├── v4.py                  v4 RegimeAware baseline
│   │   ├── iter_13.py             quality pool mcap-weighted（iter_21 子策略 80%）
│   │   ├── iter_20.py             catalyst-confirmed breakout（iter_21 子策略 20%）
│   │   ├── iter_21.py             🎯 80/20 hybrid 合成器（ship-ready）
│   │   ├── validate_iter21_v5.py  OOS validator（30s 完跑）
│   │   ├── validate_all.py        multi-strategy 驗證 sweep
│   │   ├── plot_strategies.py     NAV 對比圖
│   │   └── _engine.py / _types.py shared backtest infra
│   ├── audits/                    一次性資料 audit (01-05)
│   ├── analyses/                  一次性分析（active_etf_metrics）
│   ├── experiments/               prototype 沙箱
│   ├── tests/                     pytest 單元測試
│   └── README.md                  研究目錄詳細說明
│
├── docs/                          User-facing canonical 文件
│   ├── strategy_ranking.md        策略排行 + iter_21 執行手冊
│   ├── active_etf_analysis.md     vs 11 主動 ETF 比較
│   └── leaders_by_domain.md       Tier 1-5 龍頭清單
│
├── .claude/                       Claude Code agents / skills / commands
│   ├── agents/                    13 個 on-demand subagent
│   ├── skills/                    6 個 keyword auto-trigger workflow
│   └── commands/                  自訂 slash command
│
├── data/                          [gitignored] 爬蟲下載的原始 CSV / HTML / JSON
└── src/main/resources/
    ├── application.conf           DB 連線 + 資料源 URL 配置
    └── sql/                       SQL views / materialized views
```

---

## Quick start

### 0. 前置

- Java 8+ / SBT 1.10.5 / PostgreSQL on `localhost:5432`，DB 名稱 `quantlib`
- Python 3.11+ via [`uv`](https://github.com/astral-sh/uv)（research/ 內全部依賴 lock 在 `uv.lock`）

```bash
createdb quantlib
sbt compile          # 第一次會解 dep
```

### 1. 資料更新（每日例行）

兩步驟必照順序：

```bash
# Scala 端：crawl + import to PG
sbt "runMain Main update"

# Python 端：PG → DuckDB cache 同步（3-5 min）
uv run --project research python research/cache_tables.py
```

跳過 step 2 → Python 跑舊資料；跳過 step 1 → cache 灌過時 PG。

### 2. 跑主策略（strict 5+5 NAV 85/15 with C+B）

```bash
# Step 1: 子策略 NAV 各自先跑
uv run --project research python research/strat_lab/iter_13.py \
    --freq monthly --ranker mcap --universe twse_tpex --mode mcap
uv run --project research python research/strat_lab/iter_24.py \
    --max-positions 5 --atr-trailing

# Step 2: 全 hybrid sweep + cross-validation（包含 5+5_w85_atr 主策略）
uv run --project research python research/strat_lab/sweep_hybrid.py
```

### 3. OOS 全套驗證（每次重大改動必跑）

```bash
uv run --project research python research/strat_lab/validate_hybrid.py --top 5
```

期望輸出 5+5_w85_atr_mcap 為 **5/6 PASS borderline real alpha**：
- CAGR retention ≥ 50% ✓ / Sharpe retention ≥ 70% ✓
- Lo (2002) p < 0.05 ✓ / Bootstrap CAGR LB > 10% ✓
- DSR > 0.95 ✓ / PBO < 0.5 ⚠️（single-config CSCV 過嚴 caveat）

### 4. 單元測試

```bash
uv run --project research python -m pytest research/tests/ -v
```

---

## 資料庫 Schema（22 張 Slick-managed table）

| 類別 | Tables | 起始日 |
|---|---|---|
| **價量** | `daily_quote`、`ex_right_dividend`、`capital_reduction`、`index` | 2003+ |
| **三大法人** | `daily_trading_details` | 2007+ |
| **融資融券** | `margin_transactions` | 2001+ |
| **估值** | `stock_per_pbr_dividend_yield` | 2005+ |
| **財報（原始）** | `balance_sheet`、`income_statement_progressive`、`cash_flows_progressive` | 2010+ |
| **財報（簡明）** | `concise_balance_sheet`、`concise_income_statement_progressive` | 2010+ |
| **基本面** | `financial_analysis`、`operating_revenue` | 2001+ |
| **籌碼面（Sprint A）** | `tdcc_shareholding`（週）、`sbl_borrowing`（日，2016+/2013+）、`foreign_holding_ratio`（日，2005+/2010+） | — |
| **MOPS 結構化（Sprint B）** | `treasury_stock_buyback`（庫藏股）、`insider_holding`（內部人轉讓事前申報，2007+） | — |
| **靜態** | `etf` | — |

兩市場全覆蓋（TWSE + TPEx，用 `market` 欄區分）。

---

## CLI 命令參考

`Main.scala` 是 scopt CLI，5 個 subcommand：

```bash
sbt "runMain Main update"                               # crawl + read 全部
sbt "runMain Main pull <target>"                        # 單一資料源 crawl
sbt "runMain Main read <target>"                        # 單一資料源 import
sbt "runMain Main research --start 2018-01-02"          # Scala factor IC scan（已凍結）
sbt "runMain Main strategy regime_aware --start 2018-01-02 --end 2026-04-17"
```

`<target>` ∈ `daily_quote | daily_trading_details | index | margin | stock_per_pbr | capital_reduction | ex_right_dividend | operating_revenue | balance_sheet | income_statement | financial_analysis | financial_statements | etf | tdcc | sbl | qfii | buyback | insider | all`

---

## 設計原則

完整鐵則見 [`CLAUDE.md`](CLAUDE.md)。摘要：

- **Python is canonical research engine** — Scala `strategy/` package frozen as historical reference
- **NAV 模擬必經 `prices.py`** — 直接讀 raw `daily_quote.closing_price` 跑 daily NAV 系統性低估 ~3-6pp CAGR over 21y
- **PIT-fair 選股** — 不可 hardcode ticker；mcap ranker 也算（21y TSMC 從沒掉第一）
- **Long-only / 不開槓桿 / 不做空** — 用戶風險偏好
- **新策略 ship 前必跑 `quantlib-strategy-validator` agent**（walk-forward + MC + DSR + PBO）
- **必勝 2330 hold**（CAGR 24.23% / Sortino 1.333 / MDD -45.86%）才算真 alpha

---

## License

Private, 個人研究用途。
