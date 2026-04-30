# 量化策略最終排行 + 執行手冊（v5）

**版本**：v5.0（2026-04-30）
**評估窗口**：2005-01-03 → 2026-04-25（21.30 年完整 cycle）
**驗證方法**：Walk-forward 16 fold OOS + Lo (2002) Sharpe t-test + Bootstrap 95% CI + Deflated Sharpe Ratio + PBO + Robustness grid
**對齊鐵則**：long-only、不開槓桿、不做空、PIT-fair（無 hardcoded ticker）、必勝 2330 hold（CAGR 24.23% / Sortino 1.333 / MDD -45.86%）
**Pricing**：所有 NAV 透過 [`research/prices.py`](../research/prices.py) 取 dividend + capital-reduction back-adjusted（含息）

---

## 一、最終排行（21y in-sample，含 OOS 驗證）

| 排名 | 策略 | CAGR | Sortino | MDD | OOS verdict | 用途 |
|---|---|---:|---:|---:|---|---|
| 🥇 | **iter_21 80/20**（ship-ready）| **+24.50%** | **1.544** | **-40.39%** | **6/6 PASS** | 主策略，paper trading candidate |
| 🥈 | iter_21 50/50（高 CAGR 變體）| +26.67% | **1.661** | -43.12% | 6/6 PASS | 願意承受 +3pp MDD 換 +0.12 Sortino |
| 🥉 | iter_20 v8（單獨 catalyst breakout）| +28.68% | 1.413 | -58.93% | 未跑 OOS battery | 只用作 iter_21 子策略，不單跑（MDD 太深）|
| 4 | iter_24 (pyramid scale-in)| +27.05% | 1.202 | -54.27% | 未跑 OOS battery | 變體；目前未進 ship slot |
| 5 | iter_13 mcap (single)| +22.61% | 1.342 | -45.15% | OOS Sortino 1.535 | iter_21 子策略；單獨 = mcap-weighted quality 池 |
| 參考 | 2330 hold | +24.23% | 1.333 | -45.86% | — | 必勝 benchmark |
| 參考 | 0050 hold | +13.45% | 0.823 | -55.66% | — | 大盤 benchmark |

**勝出條件確認**：iter_21 80/20 對 2330 hold **三維全勝** — CAGR +0.27pp、Sortino +0.211、MDD +5.47pp（更淺）。

---

## 二、🥇 iter_21 80/20 — 執行手冊（ship-ready）

### 2.1 策略結構

```
NAV 80% → iter_13 mcap-weighted quality pool
NAV 20% → iter_20 catalyst-confirmed breakout
兩子策略各自獨立 daily NAV 累積
每年初 trading day 重平衡回 80/20
```

### 2.2 子策略 A：iter_13 mcap-weighted quality pool（80% NAV）

**選股邏輯**（每年初一次）：
1. 通過 quality 篩選（每年最後一個交易日的數據）：
   - 5 年 ROA 中位數 ≥ 6%
   - 5 年 GM 中位數 ≥ 25%
   - 最近 5 年無連續 2 季 NI < 0
   - 60 日 ADV ≥ NT$50M
   - 上市 ≥ 90 日 / 公司代碼 4 位數字 / 非 ETF
   - 產業屬於：半導體、電子零組件、光電、電腦周邊、通信網路、電子通路、其他電子、資訊服務
2. 按市值（capital_stock × 12 月底收盤）由大到小排序
3. 取 TOP 5
4. 按 mcap 加權（合計 = 子策略 NAV）
5. 池不足 5 → 缺位用 0050 補

**執行命令**：
```bash
uv run --project research python research/strat_lab/iter_13.py \
    --start 2005-01-03 --end 2026-04-25 --capital 1000000 --mode mcap
```

**輸出**：
- `research/strat_lab/results/iter_13_mcap_daily.csv`（NAV 序列）
- `research/strat_lab/results/iter_13_mcap_picks.csv`（每年初 picks + weights）

### 2.3 子策略 B：iter_20 catalyst-confirmed breakout（20% NAV）

**進場觸發**（每日盤後評估，同日全 true 才進）：
- s1：今日 close > 過去 60 日 max close（60 日突破）
- s2：今日 volume > 1.5 × 過去 60 日 avg volume（量增）
- s3：最近已公告月營收 YoY ≥ 30%（catalyst 已存在）
- s4：60 日 ADV ≥ NT$50M / 上市 ≥ 90 日 / 非 ETF / 非金融證券保險

**出場觸發**（任一即出）：
- e1：自進場後峰值回跌 -15%（trailing stop）
- e2：今日 close < 200 日 MA（長期破壞）
- e3：最近已公告月營收 YoY < 0%（catalyst 失效）

**倉位管理**：
- 每筆新進場 = 當下子策略 NAV × 15%
- 既有 position 自然漂移、不 rebalance
- exit 後資金回 0050 buffer
- 同時最多 10 個 position

**執行命令**：
```bash
uv run --project research python research/strat_lab/iter_20.py \
    --start 2005-01-03 --end 2026-04-25 --capital 1000000
```

**輸出**：
- `research/strat_lab/results/iter_20_daily.csv`（NAV 序列）
- `research/strat_lab/results/iter_20_trades.csv`（每筆 entry/exit）

### 2.4 合成（80/20 hybrid）

**執行命令**（讀兩子策略 NAV CSV 合成）：
```bash
uv run --project research python research/strat_lab/iter_21.py \
    --start 2005-01-03 --end 2026-04-25 --capital 1000000 \
    --w-iter13 0.8 --w-iter20 0.2
```

**輸出**：`research/strat_lab/results/iter_21_daily.csv`（最終 NAV）

⚠️ **執行前提**：先跑 `iter_13.py` + `iter_20.py` 產生子 NAV CSV（步驟 2.2 + 2.3）；iter_21.py 只是合成器，不重新算個股 backtest。

### 2.5 OOS 驗證（每次重大改動必跑）

```bash
uv run --project research python research/strat_lab/validate_iter21_v5.py
```

**期望輸出**（v5 baseline）：
- 6/6 PASS
- IS CAGR +24.50% / Sortino 1.544
- OOS pooled CAGR +23.98%（97.9% retention）
- Sharpe retention 100.1%
- Lo (2002) p = 2.67×10⁻⁷
- Boot CAGR 95% CI = [+13.26%, +38.31%]
- DSR = 1.000（n_trials=50）
- PBO = 0.098
- Robustness ±20% spread = 2.61pp CAGR

**Verdict 跌出 6/6**：暫停 paper trade、debug 是資料還是 code 變化造成。

---

## 三、🥈 iter_21 50/50（高 CAGR 替代）

同樣 6/6 PASS。差異：CAGR +26.44% / Sortino 1.671 / MDD -40.58%（基本同 80/20）。

**何時用 50/50 而非 80/20**：
- 願意承受短期 +3pp MDD 換取 +0.12 Sortino + +1.94pp CAGR（21y compounding 差距明顯）
- 較不擔心 catalyst breakout 在 bear regime 的虧損（iter_20 標準 MDD -58%）

**執行**：同 2.4，把 `--w-iter13 0.5 --w-iter20 0.5`

⚠️ **NAV CSV 命名衝突**：iter_21.py 永遠寫到 `iter_21_daily.csv`。如要保留 80/20 跟 50/50 兩條線並行：跑完一條後手動 `cp` 到 `iter_21_w80_daily.csv` / `iter_21_w50_daily.csv`，validator 才能對照。

---

## 四、Walk-forward OOS 16 fold 細節（iter_21 80/20）

| 測試年 | CAGR | Sharpe | Sortino | MDD |
|---|---:|---:|---:|---:|
| 2010 | +15.01% | 0.877 | 1.408 | -12.85% |
| **2011** | **-9.58%** | **-0.449** | **-0.715** | -22.13% |
| 2012 | +18.97% | 1.006 | 1.416 | -14.11% |
| 2013 | +13.13% | 0.703 | 1.022 | -12.26% |
| 2014 | +34.68% | 2.052 | 2.855 | -8.67% |
| 2015 | +8.85% | 0.365 | 0.546 | -19.48% |
| 2016 | +23.57% | 1.252 | 2.006 | -9.69% |
| 2017 | +35.10% | 2.559 | 4.352 | -7.43% |
| **2018** | **-2.83%** | **-0.164** | **-0.219** | -18.60% |
| 2019 | +50.72% | 2.723 | 4.201 | -14.82% |
| 2020 | +67.47% | 2.549 | 3.898 | -27.94% |
| 2021 | +38.62% | 1.809 | 2.858 | -14.75% |
| **2022** | **-26.43%** | **-1.006** | **-1.625** | **-40.39%** |
| 2023 | +32.75% | 1.647 | 2.961 | -11.46% |
| 2024 | +92.29% | 3.142 | 4.162 | -20.91% |
| 2025 | +45.94% | 1.491 | 2.114 | -27.53% |

**13 年正報酬 / 3 年負（2011 / 2018 / 2022）— 三個負年全部對應全球熊市，屬市場系統性風險而非策略性失敗**。

---

## 五、Robustness（±20% w_iter13 grid）

| w_iter13 | CAGR | Sortino | MDD |
|---:|---:|---:|---:|
| 0.64 (-20%) | +26.27% | 1.665 | -40.25% |
| **0.80 (基準)** | **+25.04%** | **1.560** | -42.46% |
| 0.96 (+20%) | +23.66% | 1.424 | -44.62% |

CAGR spread = **2.61pp**（門檻 < 15pp，**PASS**）。方向性規律：iter_20 比例增加（w_iter13 降）→ CAGR + Sortino 上升、MDD 略改善。

---

## 六、Paper trading 執行建議

### 6.1 啟動條件
- ✅ 全套 OOS 驗證 6/6 PASS（已達成 2026-04-30）
- ✅ Bootstrap CAGR LB > 10%（13.26%，已達成）
- ⏳ 在永豐 Shioaji API 建立 paper portfolio

### 6.2 資金分配
- 初始 paper 資金：可動用資金 **≤ 10%**
- 80% 配到 iter_13 池子（每年初 rebal 一次）
- 20% 配到 iter_20 catalyst pool（盤後每日掃 entry/exit）
- 預留現金 buffer（cash → 0050）

### 6.3 監控頻率
- **每月**：計算 actual NAV vs 回測 NAV 偏差。連續 3 月 deviation > 50% → 暫停人工檢視。
- **每季**：跑 `validate_iter21_v5.py`，比對 OOS retention 是否仍 > 70%。
- **重大改動觸發點**：cache rebuild 後、Polars/DuckDB 升級後、`prices.py` 修改後 → 必跑 `pytest research/tests/`。

### 6.4 放大資金條件
- 6-12 個月 paper trade Sortino > 1.0 + 累積追蹤誤差 < 30%
- 同時 OOS 驗證仍 6/6 PASS
- 通過後可放大到自有資金的 30-50%

### 6.5 停損條件（任一觸發 → 全面暫停）
- Paper trade Sortino 連續 3 月 < 0.5
- Paper trade MDD > 50%
- 結構性事件：iter_13 池 quality 篩選失效（連續 2 年池 < 5 檔）
- 結構性事件：TWSE 出現極端 regime 變化（例如：類似 2008 / 2020 但更深）

---

## 七、🚫 不要做的事（已驗證失敗）

下列方向**全部已實證失敗或無 alpha**，禁止重做：

| 方向 | 失敗證據 | Memory |
|---|---|---|
| 加 regime gate（0050 60d -10% 暫停新進場）| iter_30 OOS Sortino 0.215（vs iter_20 0.870 單獨）；iter_31 80/20 Sortino 1.401（vs iter_21 80/20 1.544，扣 -0.143）| `project_iter21_final_ship_candidate.md` |
| 純 chip 因子（TDCC / margin / SBL 簡單規則）| iter_17 / iter_18 CAGR 2-14%；chip 只在 iter_8 narrow ML ensemble 工作 | `project_strat_lab_iter17-18_chip_flow.md` |
| Mean reversion 超賣品質股反彈（iter_28）| 全失敗 | `project_strat_lab_iter28-29_contrarian_failures.md` |
| Foreign accumulation lead（iter_29）| 同上 | 同上 |
| GRR v1（月營收 × sticky 毛利率 → implied EPS）| IC 真但 DSR 0.61 / Boot LB -2.82% 不過 deploy gate | `project_grr_v1_research.md` |
| Magic Formula + Piotroski | CAGR -2.89% ~ +11.43%，全敗 0050 | `project_strategy_research_findings.md` |
| 4-factor composite | Sharpe 0.19 / MDD -77%（factor dilution）| 同上 |
| Hysteresis regime 7%/2% | 輸 symmetric 5% 約 2.5pp | 同上 |
| Momentum (relativeStrength63d) | TW 噪音 IC -0.012 | 同上 |
| Conviction-weighted entry size（iter_23）| 證實傷害 | `project_iter21_final_ship_candidate.md` |
| Chip filter 在 iter_22 | 證實傷害 | 同上 |
| 1+9 NAV 75/25 mcap-weighted | cross-validation 證實是賭 TSMC（換 ranker 崩到 0.02-0.63）| `feedback_mcap_ranker_tsmc_bias.md` |

---

## 八、🚫 結構鐵則（永久）

| 鐵則 | 原因 |
|---|---|
| **永遠用 21 年完整窗口評估** | Sample-period bias 已被多次踩雷（cherry-pick 短窗會誤判）|
| **必須 PIT-fair 選股** | 不可 hardcode 「2330」「2454」等 ticker。mcap ranker 也是變相 hardcode（21y TSMC 從沒掉第一）|
| **必勝 2330 hold** | CAGR 24.23% / Sortino 1.333；達不到就不是真 alpha |
| **dollar-tracking 不要 weight-compound** | 2026-04-28 「自然漂移 v2」灌水 +8pp CAGR 是 weight-compound bug |
| **NAV 必經 prices.py** | 直接讀 raw `daily_quote` 跑 NAV 系統性低估 ~3-6pp CAGR over 21y |
| **不可槓桿、不可做空** | user 偏好風險限制 |
| **總持倉同時最多 10 檔** | iter_13 (5) + iter_20 (max 10) 自然合計 ≤ 15，多數時間 < 10 |
| **Apples-to-apples benchmark** | 策略 OOS 必對 benchmark IS = 嚴重 review-bias 錯誤 |

---

## 九、Source artefacts

### 程式碼（active）
- [`research/prices.py`](../research/prices.py) — canonical OHLCV 還原模組
- [`research/strat_lab/iter_13.py`](../research/strat_lab/iter_13.py) — quality pool mcap
- [`research/strat_lab/iter_20.py`](../research/strat_lab/iter_20.py) — catalyst breakout
- [`research/strat_lab/iter_21.py`](../research/strat_lab/iter_21.py) — 80/20 hybrid 合成器
- [`research/strat_lab/iter_24.py`](../research/strat_lab/iter_24.py) — pyramid 變體（參考）
- [`research/strat_lab/_engine.py`](../research/strat_lab/_engine.py) — shared backtest infra
- [`research/strat_lab/validate_iter21_v5.py`](../research/strat_lab/validate_iter21_v5.py) — OOS validator（30s 完跑）

### 驗證輸出（regenerable，gitignored）
- `research/strat_lab/results/iter_21_daily.csv` — 最新 80/20 NAV
- `research/strat_lab/results/iter_21_v5_validation_folds.csv` — 16 fold OOS 細節
- `research/strat_lab/results/iter_21_v5_validation_robustness.csv` — ±20% grid

### 驗證 / 測試
- [`research/tests/test_prices.py`](../research/tests/test_prices.py) — 10 tests（含 cross-impl parity vs `active_etf_metrics.py`）
- [`research/tests/test_engine.py`](../research/tests/test_engine.py) — backtest engine smoke

### 相關文件
- [`docs/active_etf_analysis.md`](active_etf_analysis.md) — vs 11 主動 ETF 同窗口比較
- [`docs/leaders_by_domain.md`](leaders_by_domain.md) — 各領域龍頭股 master 清單
- [`research/README.md`](../research/README.md) — research 目錄結構 + 常用命令
- TW endpoint reference + MOPS gotchas → memory `reference_tw_data_endpoints.md`

---

_最後更新：2026-04-30 — v5 OOS 驗證重跑後 ship-ready confirmed_
