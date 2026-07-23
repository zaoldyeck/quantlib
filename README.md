# QuantLib — 台股量化研究系統

**全 Python 單層架構**（2026-07-23 退役 Scala + PostgreSQL）：

```
Python 爬蟲(research/crawl)→ data/ raw 原子封存 → parser → cache.duckdb → 研究 + 實盤
```

- **爬取**：`research/crawl/` 15+ 源(價量/財報/籌碼/期貨)，原始檔先封存 `data/` 才 parse。
- **資料**：`cache.duckdb`(DuckDB,唯一結構化真源)由 raw 重建、每日增量更新;毫秒查詢。
- **研究 + 實盤**：策略回測(vectorbt/alphalens)、OOS 驗證、event study、富邦/永豐實盤下單。

> 舊 Scala 爬蟲 + 策略引擎已封存 `legacy/scala/`(見該目錄 README);PostgreSQL 已 `dropdb`。

**策略狀態**：純量化冠軍 = apex_revcycle_S(GCP 實盤,1 股營運);現役事件策略 = Serenity。
策略研究/驗證/升級標準以 [`docs/strategy_research/research_sop.md`](docs/strategy_research/research_sop.md)
為準;狀態與交易規則見 [`docs/strategy_ranking.md`](docs/strategy_ranking.md)。

---

## 文件入口

| 想找 | 去哪 |
|---|---|
| **文件索引** | [`docs/README.md`](docs/README.md) |
| **台股量化策略研發 SOP** | [`docs/strategy_research/research_sop.md`](docs/strategy_research/research_sop.md) |
| **策略 production 狀態 + 交易規則** | [`docs/strategy_ranking.md`](docs/strategy_ranking.md) |
| **產業分類資料層** | [`docs/data/industry_taxonomy.md`](docs/data/industry_taxonomy.md) |
| **各領域龍頭股 master 清單** | [`docs/leaders_by_domain.md`](docs/leaders_by_domain.md) |
| **Python 研究目錄結構** | [`research/README.md`](research/README.md) |
| **開發鐵則 / data / coding 規範** | [`AGENTS.md`](AGENTS.md) |
| **Scala 退役封存** | [`legacy/scala/README.md`](legacy/scala/README.md) |

---

## 專案架構

```
.
├── research/                      Python 全棧（uv 專案,pyproject.toml 在此）
│   ├── crawl/                     ⭐ 爬取層（取代 Scala 爬蟲）
│   │   ├── sources/               每源一 adapter：fetch_day / refresh + parser（判世代）
│   │   ├── archive.py             原始檔封存（save_raw / save_sentinel,tmp→os.replace）
│   │   ├── sink.py                DuckDB upsert sink（刪該日 + 插入,idempotent）
│   │   ├── update.py              每日增量編排（齊備自檢 → 逐源 upsert）
│   │   └── rebuild.py / rebuild_financials.py   從 raw 全量重建 cache
│   ├── db.py                      ⭐ cache-only DuckDB 連線（無 PG fallback）
│   ├── prices.py                  ⭐ canonical 還原 OHLCV（DRIP + 除權息 FC1 + 減資）
│   ├── industry_taxonomy.py       ⭐ canonical PIT 產業分類
│   ├── raw_quarterly.parquet      財務品質因子（Piotroski F-Score / cfo_ni）唯一真源
│   ├── apex/                      apex_revcycle_S（純量化冠軍,GCP 實盤）
│   ├── serenity/                  Serenity 事件引擎（現役）+ 每日 loop
│   ├── evergreen/                 Evergreen（參考策略 + 引擎 parity 守護）
│   ├── strat_lab/                 因子研究 + validator + 回測 harness
│   ├── trading/                   實盤下單（execution）+ S 雲端編排（live）
│   ├── execsim/                   回測成交模擬（broker_fee 費率唯一真源）
│   ├── audits/                    資料 audit（01-05,cache-first）
│   ├── intraday/                  日內 1 分 K 回補（永豐 shioaji）
│   └── paths.py                   ⭐ 路徑唯一真源（data/ raw、var/ 產物、cache）
│
├── data/                          [gitignored,51 GB] 爬蟲原始封存（不可重生的事實地基）
├── var/                           [gitignored] 可重生產物（cache / 回測輸出 / 報告 / log）
├── docs/                          User-facing canonical 文件
├── legacy/scala/                  封存的 Scala 爬蟲 + 策略引擎（2026-07-23 退役）
└── .claude/                       Claude Code agents / skills / commands / workflows
```

---

## Quick start

### 0. 前置

- **[`uv`](https://github.com/astral-sh/uv)**（Python 套件/環境管理）。**無 Java/SBT/PostgreSQL 依賴。**

```bash
uv sync --project research          # 裝依賴（含 fubon_neo / shioaji,lock 在 uv.lock）
```

### 1. 資料更新（每日例行,一步到位）

```bash
# Python 爬蟲抓已齊備交易日的新資料,增量直寫 cache.duckdb
uv run --project research python -m research.crawl.update
```

從 raw 全量重建（歷史回補 / cache 遺失）：

```bash
uv run --project research python -m research.crawl.rebuild --all
uv run --project research python -m research.crawl.rebuild_financials   # 財報鏈 + raw_quarterly
```

### 1b. 日內 1 分 K 回補（每天跑一次）

```bash
uv run --project research python -m research.intraday.pull_kbars
uv run --project research python -m research.intraday.pull_kbars --status   # 看進度,不連線、不吃額度
```

- **隨時 Ctrl-C 或關機都能續傳**（進度即磁碟上的檔案,無 state 檔）。
- **會自己停**：永豐每日 2 GB 流量用完即停（不是錯誤）,交易日 08:00 重置。
- **全部補完約 30 天**（受 2 GB/日 上限綁住,瓶頸是流量不是速度）。

### 2. 策略研究 SOP

正式流程見 [`docs/strategy_research/research_sop.md`](docs/strategy_research/research_sop.md)。

### 3. 測試

```bash
uv run --project research python -m pytest -q          # 全套件（251 tests）
```

---

## Cache Schema（cache.duckdb 24 表,雙市場 TWSE + TPEx,用 `market` 欄區分）

| 類別 | Tables | 起始日 |
|---|---|---|
| **價量** | `daily_quote`、`ex_right_dividend`（含 FC1 參考價欄）、`capital_reduction`、`market_index` | 2003+ |
| **三大法人** | `daily_trading_details` | 2007+ |
| **融資融券** | `margin_transactions` | 2001+ |
| **估值** | `stock_per_pbr` | 2005+ |
| **財報 raw base** | `is_progressive_raw`、`bs_concise_raw`、`cf_progressive_raw`（長表逐科目） | 2010+ |
| **基本面** | `operating_revenue`（含 report_date FC8） | 2001+ |
| **籌碼面** | `tdcc_shareholding`（週）、`sbl_borrowing`（日）、`foreign_holding_ratio`（日） | — |
| **MOPS 結構化** | `treasury_stock_buyback`、`insider_holding` | 2007+ |
| **期貨** | `taifex_futures_daily` / `institutional` / `final_settlement` + 衍生表 | — |
| **衍生/策展** | `industry_taxonomy_pit`（PIT 產業）、`etf` | — |

財務品質因子（F-Score / cfo_ni）由 `research/raw_quarterly.parquet` 提供,`db.connect()`
自動註冊為 `raw_quarterly` view（取代舊 PG view）。

---

## 設計原則

完整鐵則見 [`AGENTS.md`](AGENTS.md),策略研發流程見
[`docs/strategy_research/research_sop.md`](docs/strategy_research/research_sop.md)。摘要：

- **全 Python、零 Scala/JVM/PostgreSQL** — cache.duckdb 為唯一結構化真源,從 raw 可重建。
- **原始檔封存鐵律** — 爬蟲先把 raw 原子落地 `data/` 才 parse;`data/` 是不可重生的事實地基。
- **NAV 模擬必經 `prices.py`** — 直接讀 raw `daily_quote.closing_price` 系統性低估 ~3-6pp CAGR。
- **產業分類必經 `industry_taxonomy_pit`** — 不可把最新分類套回全歷史（前視）。
- **讀取一律 `research.db.connect()`** — cache-only,查詢自帶 `WHERE market=...`。
- **PIT-fair 選股** — 不可 hardcode ticker;mcap ranker 也算。
- **Long-only / 不開槓桿 / 不做空** — 用戶風險偏好。
- **新策略 ship 前必跑 `quantlib-strategy-validator`**（walk-forward + MC + DSR + PBO）。

---

## License

Private, 個人研究用途。
