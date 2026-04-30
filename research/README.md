# research/ — Python 量化研究目錄

> **快速 onboard**：要找最終策略 → [`../docs/strategy_ranking.md`](../docs/strategy_ranking.md)。要還原股價 → [`prices.py`](prices.py)。要跑 backtest → [`strat_lab/`](strat_lab/)。

## 目錄結構

```
research/
├── 資料層（infra）
│   ├── db.py              DuckDB 連線（attach PG 或讀 cache.duckdb）
│   ├── prices.py          ★ canonical OHLCV 還原模組（cash_div + cap_red back-adjust）
│   ├── cache_tables.py    PG → DuckDB cache 同步（每次 data refresh 必跑）
│   └── constants.py       共用常數（commission / sell_tax / TDPY 等）
│
├── strat_lab/             策略 + validator + tools
│   ├── v4.py              v4 RegimeAware（TWSE-only, monthly rebal）
│   ├── iter_13.py         quality pool mcap-weighted top 5（iter_21 子策略 80%）
│   ├── iter_20.py         catalyst-confirmed breakout（iter_21 子策略 20%）
│   ├── iter_21.py         🎯 80/20 hybrid 合成器（ship-ready）
│   ├── iter_24.py         pyramid scale-in 變體（參考）
│   ├── _engine.py         shared backtest engine（dollar-tracking simulator）
│   ├── _types.py          shared dataclasses
│   ├── raw_quarterly.py   first-principles 因子 panel 建構
│   ├── validate_iter21_v5.py  ★ iter_21 OOS validator（30s 完跑，6/6 PASS）
│   ├── validate_all.py    multi-strategy 驗證 sweep
│   ├── plot_strategies.py NAV 對比圖（plotly HTML）
│   └── results/           [gitignored] daily NAV / picks / trades CSV
│
├── tests/
│   ├── test_prices.py     10 tests（含 cross-impl parity vs active_etf_metrics）
│   └── test_engine.py     backtest engine smoke
│
├── audits/                一次性資料 audit 腳本（CLI 個別執行）
│   ├── 01_find_spikes.py        TWSE/TPEx N-day >X% surge 偵測
│   ├── 02_anomaly_scan.py       row-count / price-jump anomaly
│   ├── 03_full_data_audit.py    跨 table integrity
│   ├── 04_cross_verify.py       filename-date vs CSV-content-date
│   └── 05_revenue_audit.py      operating_revenue 零負值 / 異常 YoY
│
├── analyses/              一次性分析 scripts
│   └── active_etf_metrics.py    11 主動 ETF + 0050/0052 跟 iter_21 對齊比較
│
├── experiments/           Prototype 沙箱（spike research, chase trailing, ensemble）
│   ├── build_spike_dataset.py   spike event 數據集建構
│   ├── chase_trailing_stop.py   chase 策略 prototype
│   ├── ensemble_v4_chase.py     v4 + chase blend prototype
│   ├── event_driven_v[12].py    event-driven prototypes（已被 iter_19/20 取代）
│   ├── robustness_check.py      vectorbt 穩健性測試
│   ├── sprint_a_signal_prototype.py  Sprint A 籌碼訊號 prototype
│   └── spike_dataset.parquet    spike features dataset
│
├── 文件
│   └── README.md          本檔（策略排行 → docs/strategy_ranking.md，rationale → memory）
│
├── 配置 / 套件
│   ├── pyproject.toml     uv project 配置
│   ├── uv.lock            鎖定套件版本
│   ├── __init__.py
│   └── .venv/             [gitignored] virtual env
│
└── 資料 artefact（gitignored, regenerable）
    ├── cache.duckdb       PG → DuckDB cache（1.6 GB；regen via cache_tables.py）
    ├── raw_quarterly.parquet  first-principles factor panel（regen via strat_lab/raw_quarterly.py）
    └── out/               ad-hoc outputs（spike inventories, ad-hoc CSVs）
```

## 常用命令

### 跑主策略（執行手冊見 [`docs/strategy_ranking.md`](../docs/strategy_ranking.md)）
```bash
# Quality + Catalyst Hybrid (5+5, NAV 85/15, ATR trailing, TWSE+TPEx) ship-ready
uv run --project research python research/strat_lab/iter_13.py \
    --freq monthly --ranker mcap --universe twse_tpex --mode mcap
uv run --project research python research/strat_lab/iter_24.py \
    --max-positions 5 --atr-trailing
uv run --project research python research/strat_lab/sweep_hybrid.py
# 取得 5+5_w85_atr_mcap 結果與全 sweep 排行

# v4 baseline regression
uv run --project research python research/strat_lab/v4.py \
    --start 2018-01-02 --end 2026-04-17 --capital 1000000
```

### OOS 驗證（每次重大改動必跑）
```bash
uv run --project research python research/strat_lab/validate_iter21_v5.py
```
期望輸出：6/6 PASS（CAGR retention ≥ 50% / Sharpe retention ≥ 70% / Lo p < 0.05 / Boot LB > 10% / DSR > 0.95 / PBO < 0.5）

### 單元測試
```bash
uv run --project research python -m pytest research/tests/ -v
```

### 資料 audit（CLI 個別執行）
```bash
uv run --project research python research/audits/01_find_spikes.py --min-gain 0.80 --window 60
uv run --project research python research/audits/02_anomaly_scan.py --min-stocks 20
# 03/04/05 同樣模式
```

### 主動 ETF 績效分析
```bash
uv run --project research python research/analyses/active_etf_metrics.py
```

### 重建 cache（資料更新後必做）
```bash
# 1. Scala 端先把資料抓進 PG
sbt "runMain Main update"
# 2. PG → DuckDB cache 同步（3-5 分鐘）
uv run --project research python research/cache_tables.py
```

## 關鍵 contract（永久原則）

1. **NAV 模擬必經 `prices.py`**（不准直接讀 `daily_quote.closing_price` 跑 daily NAV）
   — 否則漏除息 ~3-6pp CAGR over 21y
2. **新策略放 `strat_lab/`**，命名 `iter_NN_xxx.py`，import `_engine.py` + `_types.py`
3. **一次性 audit 放 `audits/`**，一次性 analysis 放 `analyses/`
4. **Prototype 放 `experiments/`**，與 production strategy 隔離
5. **不要 revive 已驗證失敗的方向** — 失敗 list 見 [`../docs/strategy_ranking.md`](../docs/strategy_ranking.md) §七
6. **新策略 ship 前必跑 `quantlib-strategy-validator` agent**（walk-forward + MC + DSR + PBO 全套）

## 文件對照

| 想找 | 去哪 |
|---|---|
| 最終策略排行 + 執行手冊 | [`../docs/strategy_ranking.md`](../docs/strategy_ranking.md) |
| 主動 ETF 同窗口比較 | [`../docs/active_etf_analysis.md`](../docs/active_etf_analysis.md) |
| 各領域龍頭股清單 | [`../docs/leaders_by_domain.md`](../docs/leaders_by_domain.md) |
| TW data endpoint reference + MOPS gotchas | memory `reference_tw_data_endpoints.md` |
| 失敗的研究方向（GRR / regime gate / contrarian etc.）| memory `project_grr_v1_research.md` 等 + `docs/strategy_ranking.md §七` |
| 取消的 crawler 決策 rationale | memory `feedback_data_acquisition_strategy.md` |
| Strategy / data / coding 鐵則 | [`../CLAUDE.md`](../CLAUDE.md) |
| 失敗實驗為何失敗 | memory `~/.claude/projects/.../memory/MEMORY.md` 索引 |

---

_最後更新：2026-04-30 — v6 全面重驗 + Quality + Catalyst Hybrid (5+5, NAV 85/15, ATR trailing, TWSE+TPEx) ship verdict_
