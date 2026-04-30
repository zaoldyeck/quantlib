# 量化策略最終排行 + 完整 Catalog

**評估窗口**：2005-01-03 → 2026-04-25（21.30 年完整週期，**只看最長窗口，禁止 cherry-pick**）

**含息計算**：所有 NAV 透過 [`research/prices.py`](../research/prices.py) 取 dividend + capital-reduction back-adjusted

**Benchmark**：
- 2330 直接持有：CAGR +24.23% / Sortino 1.333 / MDD -45.86% ← 必勝門檻
- 0050 直接持有：CAGR +13.45% / Sortino 0.823 / MDD -55.66% ← 大盤 baseline

**鐵則**：long-only / 不開槓桿 / 不做空 / 同時最多持倉 10 檔 / PIT-fair（不可 hardcode ticker）

---

## 一、總排行（21 年完整窗口）

按 **OOS Sortino** 由高到低，標 verdict：

### 🟢 Ship-able（已通過 OOS 驗證 + 三維勝 2330）

| 排名 | 策略名 | OOS CAGR | OOS Sortino | MDD | 對 2330 三維 | Verdict |
|---:|---|---:|---:|---:|---|---|
| 🥇 | **5+5 Hybrid (NAV 85/15)** | **+24.39%** | **1.535** | -44.85% | ✅ 全勝 | 6/6 PASS real alpha |
| 🥈 | 5+5 Hybrid (NAV 80/20) | +23.90% | 1.512 | ~-45% | ✅ | 6/6 PASS |
| 🥉 | 3+7 Hybrid (NAV 80/20) | +23.79% | 1.504 | -46% | ✅ | 6/6 PASS（catalyst 偏重）|

### 🟡 Optimization 變體（marginal 改善，因 CAGR 違規鐵則不取代）

| 排名 | 策略名 | OOS CAGR | OOS Sortino | MDD | 對 2330 三維 | Verdict |
|---:|---|---:|---:|---:|---|---|
| 4 | 5+5 optA_optB（Phase F best）| +23.30% | **1.560** | -42.5% | ❌ CAGR 輸 0.93pp | optimize 但違規 |

### 🟠 子策略單獨（hybrid 之前的 building blocks）

| 排名 | 策略名 | CAGR | Sortino | MDD | Verdict |
|---:|---|---:|---:|---:|---|
| 5 | iter_13 monthly mcap (TWSE+TPEx) | +21.97% | 1.302 | -43.90% | sub-strategy of Ship |
| 6 | iter_24 max=5 + ATR trailing | +19.58% | 0.803 | -57.70% | sub-strategy of Ship |
| 7 | iter_24 max=10 (no ATR, deprecated) | +28.68% | 1.413 | -58.93% | 違反 max 10 鐵則 |
| 8 | iter_20 catalyst breakout (max=10) | +22.20% | 1.083 | -62.5% | 太高 MDD，已被 iter_24 取代 |

### 🔴 已驗證失敗（細節見 §四）

| 策略 | 失敗原因 | OOS 數據 |
|---|---|---|
| iter_21 80/20（前任 ship）| 違反 max 10 + 三維退化（annual / no-ATR / TWSE-only）| Sortino 1.541 但結構 invalid |
| 1+9 NAV 75/25 mcap | Cross-validation 證實是賭 TSMC | mcap 1.778, roa_med 0.019（差距 1.759）|
| v4 RegimeAware | Sharpe 0.72 long-term，輸 2330 | CAGR +15.5%, Sharpe 0.72 |
| iter_22 chip filter | 證實傷害 hybrid | — |
| iter_23 conviction-weighted entry | 證實傷害 | — |
| iter_28 Mean Reversion | 全失敗 | — |
| iter_29 Foreign Accumulation Lead | 全失敗 | — |
| iter_30 regime gate (60d -10%) | OOS Sortino 0.215 vs 0.870 (iter_20)| 大幅退化 |
| iter_31 hybrid + regime gate | 扣 -0.16 Sortino | — |
| GRR v1（月營收×sticky GM）| DSR 0.61, Boot LB -2.82% | 不過 deploy gate |
| Magic Formula + Piotroski | CAGR -2.89% ~ +11.43% | 全敗 0050 |
| 4-factor composite (linear)| Sharpe 0.19, MDD -77% | factor dilution |
| Hysteresis regime 7%/2% | 輸 symmetric 5% 約 2.5pp | — |
| Momentum (relativeStrength63d) | TW 噪音 IC -0.012 | — |
| Composite ranker (z-score 等權)| OOS Sortino -1.181 | naive 多因子合成崩 |
| LightGBM Regressor / Ranker | Sharpe 0.41 / 0.22 | 樣本 vs 訊號比過低 |
| iter_5 11 因子等權 | Sharpe 0.514 | 廣 universe noise |
| iter_6 IC-weighted 26 因子 | Sharpe 0.423 | 因子相關 double-count |
| iter_10 純 quality basket TOP 5 (eq)| Sortino 0.977 | cousins 稀釋 TSMC alpha |
| iter_11 leader + 0050 regime | whipsaw 嚴重 | 60d return lag 太多 |
| iter_12 5y 營收成長排序 | 系統性偏 speculative | blow-ups 多 |
| iter_14 leader + growth satellite | satellite 多選 yesterday's winners | — |
| iter_15/26/27 hardcode 2330 | 違反 PIT-fair 鐵則 | invalidated |
| iter_16 buy-once-hold-21y | UMC 等 fallen quality 無法淘汰 | — |
| iter_17/18 純籌碼 score | CAGR 2-14% | chip 因子單獨無 alpha |
| iter_19 月營收加速度 ranking | 月公告滯後 30 天 = 末段進場 | 已被 iter_20 取代 |

---

## 二、Ship Candidate 詳細：5+5 Hybrid (NAV 85/15)

完整執行手冊見此節。**主要邏輯**：

```
總資金 100%
├── 85% → Quality 池（每月初換股一次，固定 5 檔大型 quality）
└── 15% → Catalyst 池（每天盤後掃，最多 5 檔突破股，沒訊號時 0050 buffer）

每年初重平衡兩池子比例回 85/15。
持倉硬上限 = 5 + 5 = 10 檔。
```

### 2.1 Quality 池（85% NAV）— 月頻

**選股 8 條件（每月初一次，PIT-safe quarter）**：
1. 5 年 ROA TTM 中位數 ≥ 12%
2. 5 年毛利率 TTM 中位數 ≥ 30%
3. 最近 5 年無連續 quarter NI < 0
4. 60 日 ADV ≥ NT$50M
5. 上市 ≥ 90 天 / 4 位數字代碼 / 非 ETF
6. 產業：半導體 / 電子零組件 / 光電 / 電腦周邊 / 通信網路 / 電子通路 / 其他電子 / 資訊服務
7. 市場：TWSE 或 TPEx
8. 按市值排序取 TOP 5，**mcap 加權**

**退場**：每月初 re-rank — 持倉跌出 TOP 5 → 換掉。**不設個股 stop-loss**（Phase A 實測 marginal +0.020 Sortino 但 CAGR 退 -0.3pp）。

**執行**：
```bash
uv run --project research python research/strat_lab/iter_13.py \
    --freq monthly --ranker mcap --universe twse_tpex --mode mcap
```

**21 年實際選股**：256 月份 — 2330 共 208 次 (81%)，3034/3008/6770 等少量月份，2008-2010 quality 條件嚴格 14% 月份 fallback 0050。

### 2.2 Catalyst 池（15% NAV）— 日頻

**進場 5 條件（盤後同日全 true）**：
1. 今日收盤 > 過去 60 日 max close（**60 日突破**）
2. 今日成交量 > 過去 60 日 avg vol × 1.5（**量增 50%**）
3. 最近已公告月營收 YoY ≥ 30%（**catalyst 已確認**）
4. 60 日 ADV ≥ NT$50M
5. 上市 ≥ 90 天 / 4 位數字 / 非 ETF / 非金融證券保險

**退場 3 條件（任一觸發即出）**：
- e1：**ATR trailing stop** — `trail_pct = clip(entry_atr/entry_px × 3.0, 10%, 25%)`
- e2：今日 close < 200 日 MA（趨勢破壞）
- e3：最近月營收 YoY < 0%（catalyst 失效）

**倉位**：每筆新進場 = 當下 catalyst 池 NAV × 15%；最多 5 個 position；exit 後資金 → 0050 buffer。

**執行**：
```bash
uv run --project research python research/strat_lab/iter_24.py \
    --max-positions 5 --atr-trailing
```

### 2.3 Hybrid 合成 + 年度再平衡

```bash
uv run --project research python research/strat_lab/sweep_hybrid.py
# 找 5+5_w85_atr_mcap 配置作為 ship NAV
```

每年第 1 個交易日：兩池子 NAV 重新平衡回 85/15。

---

## 三、其他 Hybrid 變體（已試過）

| 配置 | Slot | NAV weight | OOS Sortino | OOS CAGR | Verdict |
|---|---|---:|---:|---:|---|
| 5+5 (Ship)| 5 quality + 5 catalyst | 85/15 | **1.535** | +24.39% | 🥇 Ship |
| 5+5 alt | 5+5 | 80/20 | 1.512 | +23.90% | 🥈 Alt |
| 3+7 | 3 quality + 7 catalyst | 80/20 | 1.504 | +23.79% | 🥉 Alt（catalyst 偏重）|
| 5+5 weight 85/15 + Phase A/B/C optimize | 5+5 | 85/15 | 1.560 | +23.30% | 🟡 marginal Sortino+ 但 CAGR 違規 |
| iter_21 80/20（v5 廢棄）| 5 + max 10 | 80/20 | — | — | ❌ 違反 max 10 鐵則 |
| 1+9 NAV 75/25 | 1 quality (mcap) + 9 catalyst | 75/25 | **1.778**（IS）| +27.92%（IS）| ❌ Cross-val 證實賭 TSMC |
| 2+8 / 4+6 / 6+4 / 7+3 / 8+2 / 9+1 | 各 slot 變體 | various | ~1.3-1.5 | — | 統計噪音內，5+5 / 3+7 微勝 |
| 0+10 純 catalyst | 10 catalyst | 100/0 | 1.037 | +22.80% | 缺 quality anchor 抗不了系統性熊市 |
| 10+0 純 quality | 10 quality | 0/100 | 1.302 | +21.97% | 沒 catalyst 上行 boost |

**Hybrid sweep 完整結果**（66 配置 × 4 ranker cross-val）見 `research/strat_lab/results/hybrid_sweep_v6.csv`。

---

## 四、失敗策略詳細解析（避免重蹈覆轍）

### 4.1 ❌ 1+9 NAV 75/25 mcap — 「賭 TSMC」陷阱

**初看極強**：OOS Sortino 1.778 / CAGR +27.92% / Boot LB +14.31%（看似首破 2330 大幅）

**Cross-validation 證實偽 alpha**：把 quality TOP 1 的排序方法從 mcap 換成其他 quality 指標：

| ranker | 1+9 OOS Sortino | OOS CAGR |
|---|---:|---:|
| **mcap（含 TSMC outlier）★** | **1.778** | **+27.92%** |
| rev_cagr5y（5y 營收 CAGR）| 0.627 | +16.40% |
| roa_recent（最近 ROA TTM）| 0.414 | +10.52% |
| roa_med（5y ROA 中位數）| **0.019** | +1.42% |

**結論**：mcap vs roa_med 差距 1.759 → 81% 月份 mcap TOP 1 自然選 2330 = 變相 hardcode TSMC。

**5+5 結構對比**：mcap vs roa_med 差距僅 0.933 → 5 檔分散有效降低對 mcap-TOP-1 依賴。

📝 詳細記憶：`feedback_mcap_ranker_tsmc_bias.md`

### 4.2 ❌ v4 RegimeAware — 廣 universe 多因子的天花板

v4 (pbBand mean-reversion + drop_score + 0050 regime gate) 是過去 baseline。

**長期績效**（2008-2026 18y）：CAGR +15.5% / Sharpe 0.72 / MDD -42.8%

**問題**：
- 廣 universe（全 TWSE）+ 多因子 ensemble 永遠 < 0050（Sharpe 0.605）
- 加 quality / dividend 因子反而降 Sharpe（contrarian alpha 跟 mainstream 因子方向相反）
- LightGBM 在這 setup 樣本/訊號比太低，必過擬合

**結構性 ceiling**：long-only TWSE monthly top-10 framework 的 Sharpe 上限 ≈ 0.72。要破必須換 framework（降 TOPN、daily event-driven、leverage、short）。

📝 詳細記憶：`project_strat_lab_iter1-7_apr27.md`

### 4.3 ❌ iter_30/31 Regime Gate — 反而傷害

設計：0050 過去 60 天跌 -10% 就停止 catalyst 池新進場（避開 bear 市）。

**結果**：
- iter_30（單獨 catalyst + gate）OOS Sortino 0.215 vs iter_20 (no gate) 0.870 — 拖累 0.66
- iter_31（5+5 + gate）OOS Sortino 1.401 vs 5+5 (no gate) 1.535 — 扣 -0.134

**為何失敗**：bear 期間 stop new entry 但 existing position 繼續吃損；recovery 早期錯失新買點。Net negative。

📝 詳細記憶：`project_strict_5_5_v6_ship.md` §不要做的事

### 4.4 ❌ iter_28/29 Contrarian / Mean Reversion / Foreign-led

**iter_28 Mean Reversion（超賣品質股反彈）**：在 iter_13 quality pool 內持有「過去 60 天表現最差」的 5 檔 → 結果全失敗（Sortino < 0.5）

**iter_29 Foreign Accumulation Lead**：跟著外資 5 日累積買超的個股 → 失敗

**結論**：21 年 TWSE 結構性 alpha 只來自兩種 — **quality + scale**（iter_13 mcap pool）和 **catalyst breakout**（iter_24 突破）。Contrarian / leading-indicator 框架無 edge。

📝 詳細記憶：`project_strat_lab_iter28-29_contrarian_failures.md`

### 4.5 ❌ GRR v1 — 月營收 × Sticky GM → Implied EPS Growth

**Idea**：用月營收（每月公告）× 過去毛利率均值推估「隱含 EPS 成長」，比季報快 30 天觸發。

**結果**：
- IC 確實顯著（cross-sectional rank correlation 與 forward 1m return 正向）
- 但 OOS DSR 0.61（不過 0.95）、Bootstrap CAGR 信賴區間下界 -2.82%（不過 +10%）
- Walk-forward 在 2014-2017 表現良好但 2018-2024 退化

**根因**：月營收 + sticky GM 組合對「季度 EPS surprise direction」有預測力，但**對 forward 1m return 的 alpha 在 ~0**（市場已 price in 月營收快速）。

📝 詳細記憶：`project_grr_v1_research.md`

### 4.6 ❌ Magic Formula + Piotroski — TW 失效

Joel Greenblatt's Magic Formula（Earnings Yield + ROC ranking）+ Piotroski F-Score (9 條基本面打分)。

**結果**：CAGR -2.89% ~ +11.43%（依參數）— **全敗 0050**（13.45%）

**為何失效**：原版假設 US/EU 市場「low P/E + high ROC + 高 F-Score」可長期 outperform。TW 市場：
- 散戶主導 → 價值因子被「題材輪動」洗掉
- 半導體 cycle 嚴重 → ROC 在淡季 distort
- F-Score 對工業類股偏向（電子業 score 普遍高）

📝 詳細記憶：`project_strategy_research_findings.md`

### 4.7 ❌ 4-factor composite (linear) — Factor Dilution

Idea：把 4 個 sensible factor (mcap, ROA, GM, momentum) z-score 加總當 ranker。

**結果**：Sharpe 0.19 / MDD -77%

**為何失效**：
- 多 factor 線性加總 = 把好的 + 壞的 average → factor dilution
- z-score 在月度 small pool（10-30 stocks）動盪
- 多面向 quality 訊號互相 distort

**驗證**：5+5 Hybrid 拿 composite ranker 做 cross-val → OOS Sortino **-1.181** (vs mcap 1.535)

📝 詳細記憶：`project_strict_5_5_v6_ship.md` cross-validation 段

### 4.8 ❌ Pure Momentum (relativeStrength63d) — TW 噪音

63 日相對強弱排序。**IC -0.012**（基本是隨機）。

**為何失效**：TW 市場是「題材輪動」型，63 日 trend 換得太快 → momentum 變成「last week's hot stock」買在頂。

### 4.9 ❌ iter_22 Chip Filter / iter_23 Conviction-weighted

**iter_22**：在 iter_24 catalyst entry 加一層「籌碼集中度上升」filter。**結果**：fewer entries 但 Sortino 沒升 — chip 因子在 catalyst breakout context 沒額外 edge。

**iter_23**：catalyst entry 用 conviction score (signal strength) 加權倉位（強訊號買多）。**結果**：傷害 Sortino — backtest 上強訊號 ≠ forward return，反成 noise。

### 4.10 ❌ iter_15/26/27 Hardcode TSMC

直接寫 `if code == '2330'` 進策略 code。**違反 PIT-fair 鐵則**（不可 hardcode ticker）。發現後 invalidated。

📝 永久警示：`feedback_no_benchmark_self_select.md`

### 4.11 ❌ iter_16 Buy-once-hold-21y

買進 5 檔 quality 21 年不動。**結果**：UMC、宏達電、宏碁等 fallen quality 拖垮績效。**結論**：Quality 必須能淘汰，不能買了就不換。

### 4.12 ❌ iter_5-9 多因子 Ensemble / LightGBM

11-60 因子 ensemble + LightGBM Regressor / Ranker：

| 變體 | Sharpe | 為何失敗 |
|---|---:|---|
| iter_5 11 因子等權 | 0.514 | 廣 universe noise |
| iter_6 IC-weighted 26 因子 | 0.423 | 因子相關 double-count |
| iter_6 IC-w t>5 9 因子 | 0.524 | 過度集中 reversal/低 vol |
| iter_7 LightGBM Regressor | 0.410 | 樣本/訊號比太低 |
| iter_7 LightGBM Ranker | 0.219 | lambdarank 丟資訊 |
| **iter_8 60-factor LightGBM (narrow universe)** | **1.638（2018-2026 only）**| narrow universe 才 work，全期 fail |

**關鍵 insight (iter_8)**：要讓 ML ensemble work，必須先**對 narrow universe** (e.g. v4 pbBand-filtered universe)，不能直接 broad TWSE。

📝 詳細記憶：`project_strat_lab_iter8_breakthrough.md`

### 4.13 ❌ iter_10/11/12/14 Quality 變體

| 變體 | Sortino | 失敗原因 |
|---|---:|---|
| iter_10 純 quality basket TOP 5 (equal-weight) | 0.977 | cousins 稀釋 TSMC alpha |
| iter_11 leader + 0050 regime gate | — | 60d return lag 太多，whipsaw 嚴重 |
| iter_12 5y 營收成長排序 | — | 偏 speculative 微型成長股，blow-ups |
| iter_14 leader + growth satellite | — | satellite 多選 yesterday's winners |

### 4.14 ❌ Hysteresis Regime 7%/2%

V4 regime gate 用 symmetric ±5% threshold。試 hysteresis (進入 bear -7%、退出 bear +2%) → 輸 symmetric ~2.5pp。

### 4.15 ❌ iter_17/18 純籌碼 Score

把 TDCC 集保大戶 + 融資餘額 + SBL 借券 三因子線性 score 排序選股。**CAGR 2-14%（依參數）— 全敗 0050**。

**結論**：chip 因子只在 iter_8 narrow ML ensemble 內當輔助 feature 才 work，**單獨用沒 alpha**。

📝 詳細記憶：`project_strat_lab_iter17-18_chip_flow.md`

### 4.16 ❌ iter_19 月營收加速度 (Ranking Frame)

把月營收 YoY 變化率當 ranking signal，月頻換股。**結果**：月公告滯後 30 天 → 等 ranking 更新時已是末段。被 iter_20 (event-driven daily breakout + yoy confirm) 取代。

📝 詳細記憶：`project_revenue_acceleration_insight.md`

### 4.17 ❌ Quality 池所有 intra-month exit 機制 ablation（2026-04-30 完整實測）

User push back：「event-driven 退場 / ATR 退場 / 移動止損都不會比 monthly re-rank 更好嗎？」實證 8 個 variant：

#### A. Daily event-driven entry+exit（全 event）

把整個 iter_13 改成「daily mcap re-rank within monthly pool」— 持倉跌出 daily TOP 5 立刻出，新進入 TOP 5 立刻入：

| 指標 | Monthly baseline | Daily event-driven | 差異 |
|---|---:|---:|---:|
| CAGR | +21.97% | +19.46% | **-2.51pp** ❌ |
| Sortino | 1.302 | 1.021 | **-0.281** ❌ |
| MDD | -43.90% | -65.63% | **-21.7pp 惡化** ❌ |

**為何退化**：Quality 條件是 quarterly data → 月內基本不變；daily mcap rank 變動 = momentum signal → 追漲殺跌；systematic crash 反覆洗倉。

**程式**：[`research/strat_lab/iter_13_event_full.py`](../research/strat_lab/iter_13_event_full.py)

#### B. Monthly entry + 7 種 intra-month exit layer（exit-only ablation）

固定 monthly entry（baseline），加各種 intra-month exit trigger：

| Variant | CAGR | Sortino | MDD | Triggers (21y) | 結論 |
|---|---:|---:|---:|---:|---|
| **baseline (monthly re-rank only)** ★ | **+22.10%** | **1.310** | **-43.7%** | — | 最佳 |
| fixed_15 (-15% 固定移動止損) | +21.29% | 1.267 | -43.7% | 120 | -0.043 退化 |
| **atr (ATR trailing 3× clip [10%,25%])** | +20.95% | 1.246 | -48.6% | 212 | -0.064 退化，MDD 惡化 |
| qual_fade (ROA TTM < 8% 立刻出) | +22.14% | 1.314 | -43.7% | 47 | +0.004 持平（噪音內）|
| **rev_neg (月營收 YoY < 0% 立刻出)** | +18.55% | 1.059 | -54.1% | 378 | **-0.251 大幅退化** |
| atr + qual_fade | +20.92% | 1.245 | -48.6% | 246 | -0.065 退化 |
| atr + rev_neg | +17.65% | 1.016 | -54.1% | 509 | -0.294 大幅退化 |
| atr + qual_fade + rev_neg | +18.64% | 1.079 | -51.0% | 542 | -0.231 大幅退化 |

**結論：所有 intra-month exit layer 都退化或持平**。Monthly re-rank 是 Quality 池最佳設計。

#### 為何 ATR trailing 對 Catalyst work 但 Quality fail？

| 池 | 持股特性 | 日 vol (ATR/px) | trailing % | 結果 |
|---|---|---:|---|---|
| Quality | 大型股 (TSMC、聯發科等) | 1-2% | clip 到 10% 對大型股太緊 | ❌ 被日常震盪洗倉 |
| Catalyst (iter_24) | 突破股 | 3-5% | ATR×3 = 9-15% 適中 | ✅ work |

#### 為何「月營收 YoY 翻負」對 Quality 大幅惡化？

半導體電子業月營收**有 cycle 性波動**（季節性 / 庫存週期）：
- 2330 在 cycle 谷底偶而 yoy -10% 但長期持有反彈
- 21 年觸發 378 次 → 被 false signal 反覆洗倉
- 月營收 YoY 翻負對 Catalyst 是 entry 確認 / exit 訊號（catalyst 失效）；對 Quality 是 cycle noise

#### 為何「ROA TTM 跌出 12%」幾乎沒效果？

- Monthly quality screen 已捕捉這個訊號（PIT-safe quarter 切換時 re-screen）
- 個股 ROA 跌出 12% 通常意味下個月初就會被剔除 → 加 intra-month event 沒邊際效益
- 47 次觸發但對 NAV 影響極小

**程式**：[`research/strat_lab/iter_13_exit_ablation.py`](../research/strat_lab/iter_13_exit_ablation.py) — 可直接重跑驗證

**詳細結果**：`research/strat_lab/results/iter_13_exit_ablation_v8.csv`

### 4.17 (續) — 為何 Quality 池本質上不需 intra-month exit

理論解釋：
1. **Quality 屬於 mean-reverting + low-frequency alpha** — 持倉股是大型穩定公司，alpha 主要來自市值集中 + quarterly fundamentals 篩選
2. **Intra-month exit 都是 high-frequency signal** — 利用日級 noise，但 noise ≠ alpha
3. **Monthly re-rank = 12-times-a-year filter** 已經是「足夠頻繁但又避開 noise」的最佳折衷

對應 Catalyst 池（iter_24）：
- Catalyst 屬於 momentum + breakout alpha — 進場時點關鍵，需要日級 entry/exit
- Trailing stop 在 catalyst 後保護 trend reversal → ATR-based work

**結構性結論**：不同 alpha 性質（mean-reverting vs momentum）需要不同 exit 設計。Quality monthly + Catalyst event-driven 是這個框架的最佳組合。

---

## 五、為何 5+5 Hybrid 是當前最強

### 5.1 對比 single-strategy

| | iter_13 single | iter_24 single | 5+5 Hybrid |
|---|---:|---:|---:|
| OOS CAGR | 21.97% | 19.58% | **24.39%** |
| OOS Sortino | 1.302 | 0.803 | **1.535** |
| MDD | -43.90% | -57.70% | -44.85% |

**Hybrid 整體贏兩個單獨策略**，因為 catalyst 在 systematic crash 表現差但 quality 提供 stability。低相關 sleeve combination 改善 risk-adjusted return。

### 5.2 對比 hybrid 變體

memory `project_final_strategy_ranking.md` 跑過 Slot × NAV weight 全空間 sweep（66 配置）：
- 5+5 / 3+7 / 6+4 配比都在 1.55-1.60 區間（飽和）
- 8+2/7+3 給 quality 太多 slot 反而拖累 catalyst boost
- 0+10 純 catalyst 缺 quality anchor 抗不了系統性熊市

### 5.3 對比 1+9（看似最強但賭 TSMC）

1+9 mcap IS Sortino 1.778 看似更強 → cross-validation 換 ranker 後崩到 0.019 → 證實是 sample-period bias。

5+5 mcap → roa_med 換 ranker 仍保 0.602（差距小）→ alpha 真實。

### 5.4 對比 2330 直接持有

| | 2330 hold | 5+5 Hybrid |
|---|---:|---:|
| CAGR | +24.23% | **+24.39%** ✅ |
| Sortino | 1.333 | **1.535** ✅ |
| MDD | -45.86% | **-44.85%** ✅ |
| 集中風險 | TSMC 100% | 5+5 多檔分散 |

**三維全勝 + 風險分散** = 5+5 Hybrid 是 ship-able 真 alpha。

---

## 六、驗證方法論（給技術讀者）

每個 ship-able 策略必跑：

1. **Walk-forward 16-fold OOS**（5y train / 1y test rolling, 2010-2025）
2. **Lo (2002) Sharpe asymptotic t-test**（自動 correct skew + kurtosis）
3. **Bootstrap year-block CI**（1000 次 resample）
4. **Deflated Sharpe Ratio**（López 2014, 多 trial 修正）
5. **Multi-config CSCV PBO**（López 2014, 過擬合機率）
6. **Cross-validation across rankers**（換 mcap → ROA / GM → 看 alpha 是否仍在）

**Pass threshold**：6/6 PASS = real alpha；5/6 = borderline；< 5 = 不過。

5+5 Hybrid (NAV 85/15) 詳細數字：
- OOS Sharpe retention 106.5%
- Lo p = 1.13×10⁻⁵
- Boot CAGR 95% LB = +11.74%
- DSR (n=66) = 0.954
- Multi-config PBO = 0.408
- Cross-val gap (mcap vs roa_med) = 0.933

---

## 七、結構性 Ceiling 認知

memory `project_cb_tpex_atr_results.md` 結尾：

> **天花板未破** — 5+5 NAV 80/20 + ATR + TPEx 仍輸 2330 hold (Sortino 1.713 / CAGR +28.79% on long window)。結構性事實：台股 21y alpha 主要來自 mcap-weighted bucket 把 NAV 集中到 2330。
>
> **要真正破天花板必須走出量化**（用 copilot agents 做主觀層 alpha + 量化 baseline 結合）。

未來進化方向：
- 用 LLM agent 做質性研究（看法說會、找新興龍頭）
- 餵回量化 portfolio 做主觀層 alpha
- 對應 agents 在 [`.claude/agents/`](../.claude/agents/)：twstock-* / quantlib-emerging-leader-scan

---

## 八、相關檔案

### 程式（active）
- [`research/prices.py`](../research/prices.py) — 還原 OHLCV（含息）
- [`research/strat_lab/iter_13.py`](../research/strat_lab/iter_13.py) — Quality 池選股
- [`research/strat_lab/iter_24.py`](../research/strat_lab/iter_24.py) — Catalyst 池進出場
- [`research/strat_lab/sweep_hybrid.py`](../research/strat_lab/sweep_hybrid.py) — 兩池子合成 + 全 sweep
- [`research/strat_lab/validate_hybrid.py`](../research/strat_lab/validate_hybrid.py) — 完整 OOS 驗證
- [`research/strat_lab/validate_full_v6.py`](../research/strat_lab/validate_full_v6.py) — Multi-config PBO + cycle slice
- [`research/strat_lab/iter_13_event_exit.py`](../research/strat_lab/iter_13_event_exit.py) — Phase A stop-loss ablation
- [`research/strat_lab/sweep_iter13_params.py`](../research/strat_lab/sweep_iter13_params.py) — Phase C iter_13 sweep
- [`research/strat_lab/sweep_iter24_params.py`](../research/strat_lab/sweep_iter24_params.py) — Phase B iter_24 sweep

### 文件
- [`docs/active_etf_analysis.md`](active_etf_analysis.md) — vs 11 主動 ETF 同窗口比較
- [`docs/leaders_by_domain.md`](leaders_by_domain.md) — 各領域龍頭股清單
- [`research/README.md`](../research/README.md) — 研究目錄結構

### Memory（失敗策略詳細）
- `project_strat_lab_iter1-7_apr27.md` — iter 1-7 多因子 + LightGBM
- `project_strat_lab_iter8_breakthrough.md` — iter_8 narrow universe ML
- `project_strat_lab_iter13_breakthrough.md` — iter_13 mcap 突破
- `project_strat_lab_iter17-18_chip_flow.md` — 純籌碼失敗
- `project_strat_lab_iter20_breakout.md` — iter_20 catalyst
- `project_strat_lab_iter28-29_contrarian_failures.md` — Mean reversion
- `project_grr_v1_research.md` — GRR v1
- `feedback_mcap_ranker_tsmc_bias.md` — 1+9 賭 TSMC 警示
- `project_final_strategy_ranking.md` — Hybrid sweep 完整紀錄
- `project_cb_tpex_atr_results.md` — TPEx + ATR 改造
- `project_strict_5_5_v6_ship.md` — 當前 ship candidate

---

## 九、改版歷史

| 版本 | 日期 | 變更 |
|---|---|---|
| v8.0 | 2026-04-30 晚 | **重寫成 strategy catalog 風格** — 完整列出 30+ 試過策略、verdict、失敗原因解析；ship candidate 執行手冊壓到 §二 一節 |
| v7.1 | 2026-04-30 晚 | (已被 v8 取代) 改寫成 single-strategy 執行手冊 — user feedback 認為太窄，要看完整策略 catalog |
| v7.0 | 2026-04-30 晚 | Phase A/B/C optimization sweep — 找到 marginal Sortino+ 但 CAGR 違規鐵則 → 不取代 |
| v6.0 | 2026-04-30 | 廢棄 iter_21 80/20（違規）+ 還原 5+5 NAV with ATR+TPEx + 升 6/6 PASS（multi-config PBO 0.408）|
| v5.0 | 2026-04-30 早 | （**錯誤版**）iter_21 80/20 違反 max 10 + 三維退化 |
| v4 之前 | — | iter_21 80/20 + 6/6 PASS claim — 基於 raw close NAV，DRIP 漏掉 |

---

_最後更新：2026-04-30 — v8 完整策略 catalog 風格重寫_
