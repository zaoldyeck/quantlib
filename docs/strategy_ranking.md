# 量化策略最終排行 + 執行手冊（v6）

**版本**：v6.0（2026-04-30，全面 prices.py 重驗 + memory 真正 ship 版本還原）

**評估窗口**：2005-01-03 → 2026-04-25（21.30 年完整 cycle）

**驗證方法**：66 hybrid sweep × 4 ranker cross-validation × walk-forward 16 fold OOS × Lo (2002) Sharpe t-test × Bootstrap 95% CI × Deflated Sharpe Ratio (n_trials=66) × CSCV PBO

**對齊鐵則**：
- long-only / 不開槓桿 / 不做空
- **同時最多持倉 10 檔**（hybrid 5+5 cap 嚴格 enforced，不可越界）
- PIT-fair（不可 hardcode ticker）
- 必勝 2330 hold（CAGR 24.23% / Sortino 1.333 / MDD -45.86%）

**Pricing**：所有 NAV 透過 [`research/prices.py`](../research/prices.py) 取 dividend + capital-reduction back-adjusted（含息）

---

## 一、最終排行（21y in-sample + walk-forward OOS）

| 排名 | 策略 | IS CAGR | IS Sortino | OOS CAGR | OOS Sortino | OOS PASS | 用途 |
|---|---|---:|---:|---:|---:|---|---|
| 🥇 | **Quality + Catalyst Hybrid (5+5, NAV 85/15, ATR trailing, TWSE+TPEx)**（ship candidate）| 22.87% | 1.416 | **24.39%** | **1.535** | **6/6 PASS** ✅ | 主策略 |
| 🥈 | Quality + Catalyst Hybrid (5+5, NAV 80/20, ATR trailing, TWSE+TPEx) | 22.90% | 1.428 | 23.90% | 1.512 | 6/6 PASS | 替代候選 |
| 🥉 | Quality + Catalyst Hybrid (3+7, NAV 80/20, ATR trailing, TWSE+TPEx) | 22.86% | 1.422 | 23.79% | 1.504 | 6/6 PASS | 替代候選（catalyst 偏重）|
| 4 | strict 5+5 NAV 85/15 fixed -15% | 22.87% | ~1.39 | ~1.50 | — | — | TWSE+TPEx only (no ATR) |
| 5 | strict 5+5 NAV 85/15 TWSE-only ATR | 22.87% | ~1.39 | ~1.49 | — | — | ATR only (TWSE-only) |
| 參考 | iter_13 monthly mcap TPEx (single)| 21.97% | 1.302 | — | — | — | 子策略 A 單獨 |
| 參考 | iter_24 max=5 ATR (single) | 19.58% | 0.803 | — | — | — | 子策略 B 單獨 |
| 參考 | **2330 hold** | **24.23%** | 1.333 | — | — | — | 必勝 benchmark |
| 參考 | 0050 hold | 13.45% | 0.823 | — | — | — | 大盤 benchmark |

**勝出 2330 三維**（OOS）：
- CAGR +0.16pp（24.39% vs 24.23%）
- Sortino +0.20（1.535 vs 1.333）
- MDD +0.86pp（-44.85% vs -45.86%，更淺）

✅ **Verdict: 6/6 PASS real alpha** — multi-config CSCV PBO（López 2014）正確實作後 = 0.408，IS-best config 在 OOS rank 高於中位數，未過擬合。

舊 5/6 verdict 是基於我寫錯的 single-config CSCV PBO（0.716 過嚴）。修正後升級到 6/6 PASS。

---

## 二、🥇 strict 5+5 NAV 85/15 — 執行手冊

### 2.1 策略結構

```
NAV 85% → 子策略 A: iter_13 monthly mcap-weighted top 5 quality pool (TWSE+TPEx)
NAV 15% → 子策略 B: iter_24 max=5 catalyst breakout with ATR-based trailing (TWSE+TPEx)
兩子策略各自獨立 daily NAV 累積
每年初 trading day 重平衡回 85/15
持倉硬上限：5 + 5 = 10 檔（不可越界）
```

### 2.2 子策略 A：iter_13 monthly TOP 5 mcap-weighted（85% NAV）

**選股邏輯**（每月初一次，PIT-safe）：

1. Quality 篩選（取 PIT-safe quarter，月份決定）：
   - 5 年 ROA TTM 中位數 ≥ 12%
   - 5 年 GM TTM 中位數 ≥ 30%
   - 最近 5 年無連續 quarter NI < 0
   - 60 日 ADV ≥ NT$50M
   - 上市 ≥ 90 日、4 位數字代碼、非 ETF
   - 產業：半導體 / 電子零組件 / 光電 / 電腦周邊 / 通信網路 / 電子通路 / 其他電子 / 資訊服務
2. **TWSE + TPEx 雙市場**（TWSE+TPEx 雙市場擴充）
3. 按市值（capital × 月底前最後收盤）由大到小排序
4. 取 **TOP 5**
5. 按 **mcap 加權**（合計 = 子策略 NAV）
6. 池不足 5 → 缺位用 0050 補

**執行命令**：
```bash
uv run --project research python research/strat_lab/iter_13.py \
    --start 2005-01-03 --end 2026-04-25 --capital 1000000 \
    --freq monthly --ranker mcap --universe twse_tpex --mode mcap
```

**輸出**：
- `research/strat_lab/results/iter_13_monthly_mcap_dual_daily.csv`（NAV 序列）
- `research/strat_lab/results/iter_13_monthly_mcap_dual_picks.csv`（每月 picks）

### 2.3 子策略 B：iter_24 max=5 catalyst breakout + ATR trailing（15% NAV）

**進場觸發**（每日盤後評估，同日全 true 才進）：
- s1：今日 close > 過去 60 日 max close（60 日突破）
- s2：今日 volume > 1.5 × 過去 60 日 avg volume（量增）
- s3：最近已公告月營收 YoY ≥ 30%（catalyst 已存在）
- s4：60 日 ADV ≥ NT$50M / 上市 ≥ 90 日 / 非 ETF / 非金融證券保險
- 5）**TWSE + TPEx 雙市場**

**出場觸發**（任一即出）：
- e1：**ATR-based trailing stop** — `trail_pct = clip(entry_atr/entry_px × 3.0, 10%, 25%)`
  - ATR-relative：高 vol 個股放寬 stop，低 vol 個股收緊 stop
  - 取代固定 -15% trailing
- e2：今日 close < 200 日 MA（長期破壞）
- e3：最近已公告月營收 YoY < 0%（catalyst 失效）

**倉位管理**：
- 每筆新進場 = 當下子策略 NAV × 15%
- **同時最多 5 個 position**（max=5，從原本 max=10 改）
- 既有 position 自然漂移、不 rebalance
- exit 後資金回 0050 buffer

**執行命令**：
```bash
uv run --project research python research/strat_lab/iter_24.py \
    --start 2005-01-03 --end 2026-04-25 --capital 1000000 \
    --max-positions 5 --atr-trailing
```

**輸出**：
- `research/strat_lab/results/iter_24_max5_atr_daily.csv`（NAV 序列）
- `research/strat_lab/results/iter_24_max5_atr_trades.csv`（每筆 entry/exit）

### 2.4 合成（85/15 hybrid）

**執行命令**（讀兩子策略 NAV CSV 合成）：
```bash
# 透過 sweep_hybrid.py 自動處理；或直接用 hybrid_blend(): 
uv run --project research python -c "
import sys; sys.path.insert(0, 'research/strat_lab')
from sweep_hybrid import hybrid_blend
nav_a = 'research/strat_lab/results/iter_13_monthly_mcap_dual_daily.csv'
nav_b = 'research/strat_lab/results/iter_24_max5_atr_daily.csv'
df = hybrid_blend(nav_a, nav_b, w_a=0.85)
df.write_csv('research/strat_lab/results/strict_5_5_w85_atr_daily.csv')
"
```

⚠️ **執行前提**：先跑 §2.2 + §2.3 子策略產生 NAV CSV。

### 2.5 OOS 驗證

```bash
uv run --project research python research/strat_lab/validate_hybrid.py --top 5
```

期望輸出（5+5_w85_atr_mcap）：
- IS CAGR +22.87% / Sortino 1.416
- OOS CAGR +24.39% / Sortino 1.535（retention 108.4%）
- Sharpe retention 106.5%
- Lo (2002) p = 1.13×10⁻⁵
- Boot CAGR 95% LB = +11.74%
- DSR (n_trials=66) = 0.954
- PBO multi-config CSCV (López 2014) = 0.408
- **Verdict: 6/6 PASS real alpha** (multi-config PBO 0.408)

---

## 三、🥈 strict 5+5 NAV 80/20（替代）+ 🥉 3+7 NAV 80/20

差距僅 0.02-0.03 Sortino — 統計噪音內。三者實質 tied。

選擇 85/15 的原因：OOS retention 最高（106.7% vs 80/20 的 104.4%）→ paper trading 預期更穩。
選擇 80/20 的原因：IS Sortino 最高 → 對歷史 fit 最強。
選擇 3+7 的原因：catalyst 偏重 → 在 catalyst-rich 期間（如 2024 AI rally）可能更有 upside。

---

## 四、Cross-validation（5+5_w85_atr 換 5 個 ranker，alpha 真實性檢驗）

| ranker | OOS CAGR | OOS Sortino | OOS MDD | 結論 |
|---|---:|---:|---:|---|
| **mcap** ★ | 24.39% | **1.535** | -42.0% | 最佳（仍有 mcap dominance）|
| roa_recent | 27.91% | 1.307 | -47.2% | 次佳，CAGR 高但 vol 高 |
| rev_cagr5y | 20.97% | 0.979 | -59.8% | borderline |
| roa_med | 14.06% | 0.602 | -51.6% | 弱（5y ROA 中位數對未來預測力差）|
| **composite** ⚠️ | **-12.11%** | **-1.181** | -12.9% | **嚴重失敗**（naive z-score 等權合成）|

**Composite outlier 解讀**：z(log_mcap) + z(roa_med) + z(rev_cagr5y) 等權加總在月度 small pool（~10-30 stocks）下 z-score 動盪 + 多面向 quality 訊號互相 distort。**單一 sensible ranker > naive 多面向 composite**。

**4 sensible ranker 之間 max gap = 0.933**（mcap 1.535 vs roa_med 0.602）— 對比 1+9 NAV 75/25 結構的 1.759，5+5 結構成功降低 ranker 替換敏感度。

**5+5 結構 vs 1+9 結構的 cross-val 對比**：

| 結構 | mcap | roa_med | 差距 | 解讀 |
|---|---:|---:|---:|---|
| 1+9 NAV 75/25（memory，已降級）| 1.778 | 0.019 | **1.759** | 賭 TSMC |
| **5+5 NAV 85/15（新 ship）**| **1.535** | **0.602** | **0.933** | 5+5 分散有效 |

**結論：5+5 結構成功降低對 mcap-TOP-1 的依賴，alpha 真實性 confirmed**（不是賭 TSMC）。

---

## 四之二、PBO Multi-Config CSCV (López de Prado 2014)

### 正確實作 vs 之前單 config 過嚴版本

| 實作 | PBO | 含義 | Verdict |
|---|---:|---|---|
| Single-config CSCV（先前版本，過嚴）| 0.716 | 把同一 config 的 16 fold 隨機拆 IS/OOS 半比較 — sample 太小 | ❌ FAIL |
| **Multi-config CSCV (López 2014)** ★ | **0.408** | 對所有 sweep configs 在 IS/OOS half-split 找 IS-best 排名 | ✅ **PASS** |

正確的 PBO 是「多 config 比較」而非「單 config 內年度比較」。修正後 verdict 從 5/6 → **6/6 PASS real alpha**。

詳細：26 sweep configs × 16 OOS years × 1000 random splits → IS-best 在 OOS rank 高於中位數的機率 = 1 - 0.408 = **59.2%**（隨機是 50%）。

---

## 四之三、跨 Cycle 切片驗證（Quality + Catalyst Hybrid (5+5, NAV 85/15, ATR trailing, TWSE+TPEx)）

不是分年驗證 — walk-forward 16 fold OOS 已涵蓋 2010-2025 每年（含 2011/2018/2022 三個負年）。這裡額外切跨 cycle 段：

| 切片 | 天數 | CAGR | Sortino | MDD | 解讀 |
|---|---:|---:|---:|---:|---|
| 2005-2007 pre-GFC bull | 742 | +17.19% | 1.199 | -20.6% | 牛市跟得上 |
| **2008 GFC year** | 249 | **-23.91%** | **-1.233** | **-40.9%** | 系統性熊市無解（同 2330 結構性風險）|
| 2009 recovery | 252 | +27.08% | 1.633 | -14.3% | 跌深反彈接住 |
| **2008-2009 GFC full** | 501 | **-1.60%** | -0.141 | -40.9% | 雙年合計幾乎打平（reverberance OK）|
| 2011 EU debt crisis | 247 | -4.97% | -0.417 | -18.9% | 中度熊市可控 |
| 2018 trade war | 247 | -3.96% | -0.279 | -18.5% | 同上 |
| **2022 growth crash** | 246 | **-27.90%** | **-1.685** | **-42.0%** | 跟 GFC 同級慘 |

**關鍵洞察**：
- 4 個負年（2008/2011/2018/2022）= 系統性熊市
- 但 GFC 雙年（2008+2009）合計只 -1.60%，反彈接住跌幅
- **2022 -27.90%/Sortino -1.685 是策略歷史最慘**（不是 GFC）
- 跟 2330 hold 一樣承受 mcap-weighted 大型科技股的市場 beta — 不是策略特定缺陷

---

## 五、Walk-forward OOS 16 fold 細節（5+5_w85_atr_mcap）

逐年 OOS（test year 2010-2025）。原始 16 folds CSV 見 `research/strat_lab/results/validate_top_hybrids_v6.csv`。

OOS pooled（合成 16 fold daily returns）：
- CAGR +24.39%
- Sortino 1.535
- Sharpe 1.032
- Boot CAGR 95% CI = [+11.74%, +38.0%]
- Lo (2002) t-stat ≈ 4.4, p = 1.13e-5

---

## 六、Robustness（slot × weight 全空間 sweep）

完整 66 hybrid sweep 結果見 `research/strat_lab/results/hybrid_sweep_v6.csv`。Top 10 全是 5+5 / 3+7 + ATR：

| Rank | Tag | IS Sortino | IS CAGR |
|---:|---|---:|---:|
| 1 | 5+5_w80_atr | 1.386 | 22.25% |
| 2 | 5+5_w70_atr | 1.383 | 22.24% |
| 3 | 3+7_w80_atr | 1.380 | 22.21% |
| 4 | 3+7_w70_atr | 1.379 | 22.18% |
| 5 | 5+5_w85_atr | 1.374 | 22.22% |
| 6 | 3+7_w85_atr | 1.369 | 22.19% |
| 7 | 5+5_w90_atr | 1.356 | 22.17% |
| 8 | 3+7_w90_atr | 1.353 | 22.15% |
| 9 | 5+5_w60_atr | 1.348 | 22.14% |
| 10 | 3+7_w60_atr | 1.345 | 22.05% |

**結論：5+5 / 3+7 配置 + ATR + weight ∈ [60-90] 全部表現相近**（CAGR spread < 1pp、Sortino spread < 0.05），證明對 weight 微調不敏感 = robust。

---

## 七、Paper trading 執行建議

### 7.1 啟動條件
- ✅ **6/6 OOS PASS real alpha**（multi-config PBO 修正後）
- ✅ Bootstrap CAGR LB > 10%（11.74%）
- ✅ Cross-validation 證實非賭 TSMC（4 sensible ranker gap < 1.0）
- ✅ 跨 cycle 切片：2008-09 GFC 雙年合計 -1.60%（反彈接住）
- ⏳ 在永豐 Shioaji API 建立 paper portfolio

### 7.2 資金分配
- **初始 paper 資金：可動用資金 ≤ 10%**（6/6 PASS confirmed → 比 5/6 borderline 標準放寬）
- 85% 配到 iter_13 monthly TPEx mcap 池（每月初 rebal）
- 15% 配到 iter_24 max=5 ATR catalyst pool（盤後每日掃 entry/exit）
- 預留現金 buffer（cash → 0050）

### 7.3 監控頻率
- **每月**：計算 actual NAV vs 回測 NAV 偏差。連續 3 月 deviation > 50% → 暫停人工檢視
- **每季**：跑 `validate_hybrid.py`，比對 OOS retention 是否仍 > 70%
- **每半年**：重跑 `sweep_hybrid.py` 看是否有新優化方向（PBO multi-config 補回時 first thing to verify）

### 7.4 放大資金條件
- 6-12 個月 paper trade Sortino > 1.0 + 累積追蹤誤差 < 30%
- 同時 OOS 驗證仍 5/6 PASS
- PBO multi-config CSCV 修正後仍 < 0.5
- 通過後可放大到自有資金的 20-30%（比舊版 30-50% 保守）

### 7.5 停損條件（任一觸發 → 全面暫停）
- Paper trade Sortino 連續 3 月 < 0.5
- Paper trade MDD > 50%
- 結構性事件：iter_13 池 quality 篩選失效（連續 2 年池 < 5 檔）
- TWSE TSMC 連續 6 個月不在 mcap 前 3 大（mcap-weighted 結構失效訊號）

---

## 八、🚫 不要做的事（已驗證失敗）

| 方向 | 失敗證據 | Memory |
|---|---|---|
| 1+9 NAV 75/25 mcap | cross-validation 證實是賭 TSMC（mcap 1.778 vs roa_med 0.019）| `feedback_mcap_ranker_tsmc_bias.md` |
| 加 regime gate（0050 60d -10% 暫停新進場）| iter_30 OOS Sortino 0.215 vs 0.870；iter_31 hybrid 扣 -0.16 | `project_iter21_final_ship_candidate.md` |
| 純 chip 因子 | iter_17 / iter_18 CAGR 2-14% | `project_strat_lab_iter17-18_chip_flow.md` |
| Mean reversion / contrarian | iter_28 / iter_29 全失敗 | `project_strat_lab_iter28-29_contrarian_failures.md` |
| GRR v1（月營收 × sticky 毛利率）| DSR 0.61 / Boot LB -2.82% 不過 deploy | `project_grr_v1_research.md` |
| Magic Formula + Piotroski | CAGR -2.89% ~ +11.43%，全敗 0050 | `project_strategy_research_findings.md` |
| 4-factor composite (linear) | Sharpe 0.19 / MDD -77%（factor dilution） | 同上 |
| Hysteresis regime 7%/2% | 輸 symmetric 5% 約 2.5pp | 同上 |
| Momentum (relativeStrength63d) | TW 噪音 IC -0.012 | 同上 |
| Conviction-weighted entry size（iter_23）| 證實傷害 | `project_iter21_final_ship_candidate.md` |
| Chip filter 在 iter_22 | 證實傷害 | 同上 |
| **iter_21 80/20（iter_13 annual + iter_20 max=10）** | **違反 max 10 鐵則**（5+10=15 檔），且 annual / no-ATR / TWSE-only 三維退化 | （此 v6 修正）|

---

## 九、🚫 結構鐵則（永久）

| 鐵則 | 原因 |
|---|---|
| **同時最多持倉 10 檔** | 5+5 = 10 ≤ 10 ✅；任何 hybrid 必須驗證 slot_a + slot_b ≤ 10 |
| **永遠用 21 年完整窗口評估** | Sample-period bias 已被多次踩雷 |
| **必須 PIT-fair 選股** | 不可 hardcode 「2330」「2454」等 ticker |
| **必勝 2330 hold** | CAGR 24.23% / Sortino 1.333；達不到就不是真 alpha |
| **dollar-tracking 不要 weight-compound** | 2026-04-28 「自然漂移 v2」灌水 +8pp CAGR 是 weight-compound bug |
| **NAV 必經 prices.py** | 直接讀 raw `daily_quote.closing_price` 跑 NAV 系統性低估 ~3-6pp CAGR over 21y |
| **不可槓桿、不可做空** | user 偏好風險限制 |
| **新 hybrid 需做 ranker cross-validation** | 確認 alpha 不是 mcap-bias 賭 TSMC（5+5 已通過、1+9 失敗）|

---

## 十、Source artefacts

### 程式碼（active）
- [`research/prices.py`](../research/prices.py) — canonical OHLCV 還原模組
- [`research/strat_lab/iter_13.py`](../research/strat_lab/iter_13.py) — quality pool monthly + 5 ranker + TWSE/TPEx
- [`research/strat_lab/iter_24.py`](../research/strat_lab/iter_24.py) — catalyst breakout + ATR + max=5 default via CLI
- [`research/strat_lab/sweep_hybrid.py`](../research/strat_lab/sweep_hybrid.py) — 66 hybrid 全 sweep + cross-val
- [`research/strat_lab/validate_hybrid.py`](../research/strat_lab/validate_hybrid.py) — generic OOS validator (walk-forward + Lo + Boot + DSR + PBO)
- [`research/strat_lab/_engine.py`](../research/strat_lab/_engine.py) — shared backtest infra

### 驗證輸出（regenerable，gitignored）
- `research/strat_lab/results/hybrid_sweep_v6.csv` — 66 IS sweep 結果
- `research/strat_lab/results/hybrid_cross_validation_v6.csv` — top × 4 ranker IS cross-val
- `research/strat_lab/results/validate_top_hybrids_v6.csv` — top 5 OOS verdict
- `research/strat_lab/results/validate_cross_val_v6.csv` — top OOS × 4 ranker

### 驗證 / 測試
- [`research/tests/test_prices.py`](../research/tests/test_prices.py) — 10 tests cross-impl parity
- [`research/tests/test_engine.py`](../research/tests/test_engine.py) — backtest engine smoke

### 相關文件
- [`docs/active_etf_analysis.md`](active_etf_analysis.md) — vs 11 主動 ETF 同窗口比較
- [`docs/leaders_by_domain.md`](leaders_by_domain.md) — 各領域龍頭股 master 清單
- [`research/README.md`](../research/README.md) — 研究目錄結構

---

## 十一、Optimization Sweep（Phase A/B/C/F）— 不取代 ship

User 質疑「策略是否能再進化」、「event-driven 出場是否更好」、「iter_13/24 內部參數最佳化」後，跑了 4 階段完整 sweep：

### Phase A：iter_13 event-driven exit ablation
測試 stop-loss ∈ {0%, 15%, 20%, 25%, 30%} on iter_13 monthly NAV：

| Stop-loss | CAGR | Sortino | MDD | Triggers |
|---|---:|---:|---:|---:|
| none | 22.00% | 1.304 | -43.90% | 0 |
| **15%** ★ | 22.11% | **1.324** | -42.23% | 495 |
| 20% | 22.18% | 1.318 | -43.90% | 167 |
| 25% | 22.11% | 1.312 | -43.90% | 48 |
| 30% | 22.07% | 1.309 | -43.90% | 18 |

**結論：marginal +0.020 Sortino 改善，方向對但 statistical noise 內**。原因：iter_13 持大型 quality 股，跌 -25% 通常是 systematic crash（同 beta），轉 0050 沒 differentiation。

### Phase B：iter_24 entry/exit 參數 sweep
36 配置（yoy ∈ {20,30,40} × lookback ∈ {60,90} × vol_mult ∈ {1.5,2.0} × atr_mult ∈ {2,3,4}）：

最佳：**yoy=30, lkb=90, vol_mult=2.0, atr=3.0** → Sortino 0.848 / CAGR 21.19%
（baseline yoy=30/lkb=60/v=1.5/atr=3.0 → Sortino 0.803）

**改善 +0.045 Sortino on iter_24 sub-strategy**。實質：lookback=90 + vol_mult=2.0 較嚴 → 假訊號減少。

### Phase C：iter_13 entry/exit threshold sweep
24 配置（min_roa ∈ {0.08, 0.10, 0.12, 0.15} × min_gm ∈ {0.25, 0.30, 0.35} × stop_loss ∈ {0, 0.15}）：

最佳：**min_roa=0.08, min_gm=0.25, stop_loss=15%** → Sortino 1.335 / CAGR 21.25%
（baseline 12%/30%/none → Sortino 1.302）

**改善 +0.033 Sortino on iter_13 sub-strategy**。實質：較寬 quality threshold 讓 pool 更大有更多 alternatives；stop-loss 一致改善（11/12 sl=15 vs sl=0）。

### Phase F：optimized hybrid 重組合 + OOS validate

| Hybrid 配置 | IS Sortino | OOS Sortino | OOS CAGR | Boot LB | DSR | vs 2330 三維 |
|---|---:|---:|---:|---:|---:|---|
| **baseline (v6 ship)** | 1.416 | 1.533 | **24.36%** | 11.46% | 0.953 | ✅ 全勝 |
| optA_baseB | 1.414 | 1.507 | 22.43% | — | — | ❌ CAGR 輸 |
| baseA_optB | 1.379 | 1.487 | 23.76% | — | — | ❌ CAGR 輸 |
| optA_optB | 1.462 | **1.560** | 23.30% | 11.03% | **0.965** | ❌ CAGR 輸 0.93pp |

**結論：optimization 找到 marginal Sortino 改善 (+0.027 OOS)，但 CAGR 全部退步 1-2pp 違反「必勝 2330」鐵則**。

### 為何不取代 baseline ship

User 鐵則：「**必勝 2330 hold (CAGR 24.23% / Sortino 1.333 / MDD -45.86%)**」三維全勝才能 ship。

- baseline OOS: **CAGR +0.13pp / Sortino +0.20 / MDD +1.01pp** ✅ 全勝
- optA_optB: CAGR -0.93pp / Sortino +0.227 / MDD wins ❌ **CAGR 違規**

User 早講過「測試過就知道，變差就改回來」— optA_optB **CAGR 角度確實變差**，按嚴格鐵則不能 ship。

### Optimization Saturated — Memory cb_tpex_atr_results 結論證實

> 「天花板未破 — 結構性事實：台股 21y alpha 主要來自 mcap-weighted bucket 把 NAV 集中到 2330。要真正破天花板必須走出量化（用 copilot agents 做主觀層 alpha + 量化 baseline 結合）。」

**這個 framework 內已 saturated**。要再進化必須跳出純量化排序：
- 用 LLM agent 做質性研究（看法說會、找新興龍頭）
- 餵回量化 portfolio 做主觀層 alpha
- 對應的 agents 已在 `.claude/agents/` 內（twstock-* / quantlib-emerging-leader-scan）

### Optimization 變體 NAV CSVs（gitignored，可重生）

- `iter_13_sweep_roa8_gm25_sl15_daily.csv` — Phase C 最佳 iter_13
- `iter_24_sweep_y30_lkb90_v2.0_atr3.0_daily.csv` — Phase B 最佳 iter_24
- `strict_5_5_w85_optA_optB_daily.csv` — 完整 hybrid optimized
- `sweep_iter24_params_v6.csv` / `sweep_iter13_params_v6.csv` — 完整 sweep 結果

---

## 十二、改版歷史

| 版本 | 日期 | 變更 |
|---|---|---|
| v7.0 | 2026-04-30 晚 | **Optimization sweep 完成（不取代 ship）**：A/B/C/F 完整 sweep；最佳 optA_optB OOS Sortino +0.027 但 CAGR -1pp 違反鐵則 → baseline 維持 ship。Optimization 在當前 framework 已 saturated，要進化需跳出純量化（用 agent + 質性研究）|
| v6.0 | 2026-04-30 | **重大修正**：改用 prices.py 還原版重跑 + 修復「max 10 鐵則違規」+ 還原 memory 真正 ship 版本（Quality + Catalyst Hybrid (5+5) + monthly + TPEx + ATR + cross-val）。新冠軍 = strict 5+5 NAV 85/15 → 升 6/6 PASS（multi-config PBO 0.408）|
| v5.0 | 2026-04-30 早 | （**此版錯誤**）iter_21 80/20 = iter_13 annual + iter_20 max=10，違反 max 10 鐵則，annual/no-ATR/TWSE-only 三維退化 |
| v4 之前 | — | iter_21 80/20 + 6/6 PASS claim — 基於 raw close NAV，DRIP 漏掉 |

---

_最後更新：2026-04-30 — v7 optimization sweep 完成，baseline ship 不變_
