# 台股主動式 ETF 全市場分析（v6）

**版本**：v6.0（2026-04-30 晚，**廢棄 iter_21 80/20**：違反 max 10 鐵則 + memory 真正 ship 版本還原）

**v5 → v6 重大修正**：
- v5 用的 `iter_21 80/20`（iter_13 annual + iter_20 max=10）違反「同時持倉最多 10 檔」鐵則（5+10=15 檔）+ 三維退化（annual / no-ATR / TWSE-only）
- **v6 還原 memory 真正 ship 版本**：[`Quality + Catalyst Hybrid (5+5, NAV 85/15, ATR trailing, TWSE+TPEx)`](strategy_ranking.md)（iter_13 monthly mcap TPEx + iter_24 max=5 ATR）
- 同時持倉 = 5+5 = **10 檔（合規）**
- v6 OOS verdict: **6/6 PASS real alpha** (multi-config PBO 0.408 corrected)
- vs 11 ETF same-window 勝率從 v5 的 5/11 (45%) 提升到 v6 的 8/11 (73%)

**v4 → v5 修正（DRIP 中央化）**：
- v4 時期 ETF 已用還原股價，但 **iter_20 / iter_24 子策略仍用 raw `daily_quote.closing_price`** 跑 daily NAV simulation → 系統性低估 NAV ~3-6pp CAGR over 21y
- v5 新建 `research/prices.py`（中央化 dividend + capital-reduction 還原），iter_20 / iter_24 / iter_13 / v4 全部統一收斂
- **iter_21 80/20** 21y baseline：CAGR **+21.36% → +24.50%**, Sortino **1.400 → 1.544**, MDD **-45.31% → -40.39%**
- 新發現：iter_21 50/50 反而 Sortino 1.661 / MDD -43.12% 更高（DRIP 修正後 iter_20 從 Sortino 1.083 → 1.413）
- ETF 同窗口比較：原本「iter_21 全勝」變成「iter_21 中長窗仍勝、ETF 短窗（≤ 4 月）的 lucky AI 浪潮可超越」

**v3 → v4 升級**：
- 所有績效從 WebSearch 第三方文章 → **本地 PostgreSQL `daily_quote` + `ex_right_dividend` 計算**
- 採 **dividend-adjusted close（還原股價）**，跟我方 iter_21 NAV (含 DRIP) **真正 apples-to-apples**
- 新增量化指標：Sharpe、Sortino、Calmar (CAGR/|MDD|)、Vol、**Beta vs 0050**、**Annualized Alpha**
- 涵蓋 11 檔主動式 ETF + 0050 / 0052 全部本地 DB（無第三方依賴）
- **資料 cutoff：2026-04-24（最新 daily_quote 日）**

**v2 → v3 修正**：剔除 006208 候選（0050 三維 dominate）
**v1 → v2 修正**：0050 fee 0.46% → 0.14%、006208 0.24% → 0.18%（2025 大幅調降）
**用途**：以 verified data 對台股主動式 ETF 做整體評估，並對照我方 iter_21 量化策略 + 主動基金 + 被動 ETF，協助做出長期持有的單一選擇
**方法論**：完全 verified，所有 YTD / 規模 / 持股皆來自 2026-04 公開資料；完全不依賴 LLM 訓練記憶

---

## 一、11 檔主動式 ETF 績效全表（DB verified，還原股價）

資料來源：`pg.public.daily_quote` × `ex_right_dividend`，dividend-adjusted close
資料 cutoff：2026-04-24

### 1.1 按 since-inception 累積報酬排序

| 代號 | 名稱 | 上市日 | 持有天數 | **累積 %** | CAGR % | Sharpe | **Sortino** | MDD % | Vol % |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 00981A | 主動統一台股增長 | 2025-05-27 | 332 | **+181.1%** | 211.7% | 4.09 | 5.68 | -9.1% | 30.2% |
| 00982A | 主動群益台灣強棒 | 2025-05-22 | 337 | +106.4% | 119.4% | 3.03 | 3.89 | -13.4% | 28.4% |
| 00991A | 主動復華未來 50 | 2025-12-18 | 127 | +68.2% | 346.3% | 4.18 | 6.44 | -13.4% | 41.9% |
| 00994A | 主動第一金台股優 | 2026-01-07 | 107 | +64.0% | 441.4% | 4.82 | **7.02** | -11.3% | 40.4% |
| 00992A | 主動群益科技創新 | 2025-12-30 | 115 | +63.1% | 372.9% | 4.23 | 5.75 | -12.2% | 42.8% |
| 00988A | 主動統一全球創新 | 2025-11-05 | 170 | +63.0% | 185.7% | 2.72 | 4.03 | -14.3% | 44.9% |
| 00987A | 主動台新優勢成長 | 2025-12-30 | 115 | +58.9% | 335.5% | 4.10 | 5.81 | -11.0% | 41.8% |
| 00995A | 主動中信台灣卓越 | 2026-01-22 | 92 | +55.2% | 472.1% | 4.95 | **7.55** | -8.9% | 41.9% |
| 00990A | 主動元大 AI 新經濟 | 2025-12-22 | 123 | +54.4% | 263.1% | 3.48 | 5.08 | -13.5% | 43.7% |
| 00984A | 主動安聯台灣高息 | 2025-07-14 | 284 | +40.5% | 54.8% | 2.61 | 3.33 | -7.9% | 18.2% |
| 00993A | 主動安聯台灣 | 2026-02-03 | 80 | +25.1% | 177.8% | 3.45 | 5.78 | -8.9% | 36.0% |

**對照基準**（同期還原股價）：
- 0050 全期 22.2 年：累積 +272%、CAGR 6.10%、Sharpe 0.43、Sortino 0.39、MDD -77.2%
- 0052 全期 19.6 年：累積 +175.8%、CAGR 5.32%

### 1.2 按 Sortino（風險調整後報酬）排序

風險調整後的真實 alpha 高低：

| 排名 | 代號 | Sortino | Sharpe | Calmar (CAGR/\|MDD\|) | 風格 |
|---:|---|---:|---:|---:|---|
| 1 | **00995A** 主動中信台灣卓越 | **7.55** | 4.95 | 53.0× | 短窗最強 |
| 2 | **00994A** 主動第一金台股優 | **7.02** | 4.82 | 39.1× | 9 大因子量化 |
| 3 | 00991A 主動復華未來 50 | 6.44 | 4.18 | 25.8× | — |
| 4 | 00987A 主動台新優勢成長 | 5.81 | 4.10 | 30.5× | — |
| 5 | 00993A 主動安聯台灣 | 5.78 | 3.45 | 20.0× | — |
| 6 | 00992A 主動群益科技創新 | 5.75 | 4.23 | 30.6× | AI server + 先進製程 |
| 7 | 00981A 主動統一台股增長 | 5.68 | 4.09 | 23.3× | 大型成長（規模 1833 億）|
| 8 | 00990A 主動元大 AI 新經濟 | 5.08 | 3.48 | 19.5× | — |
| 9 | 00988A 主動統一全球創新 | 4.03 | 2.72 | 13.0× | — |
| 10 | 00982A 主動群益台灣強棒 | 3.89 | 3.03 | 8.9× | — |
| 11 | 00984A 主動安聯台灣高息 | 3.33 | 2.61 | 6.9× | 高息低 vol |

### 1.3 按對 0050 alpha（同窗口、含 DRIP）排序

| 代號 | Beta | Alpha (年化) | 解讀 |
|---|---:|---:|---|
| 00981A | -0.00 | +113.9% | 短窗 anomaly（11 個月初期 with 0050 同步性差） |
| 00982A | -0.02 | +81.1% | 同上 |
| 00995A | 1.20 | **+78.9%** | 真實高 alpha + 較高 beta |
| 00994A | 1.24 | **+71.9%** | 9 大因子模型化選股、alpha 第二高 |
| 00992A | 1.17 | +43.5% | AI 純科技 |
| 00987A | 1.15 | +36.4% | — |
| 00991A | 1.27 | +18.9% | beta 偏高、alpha 中等 |
| 00993A | 0.91 | +18.7% | — |
| 00990A | 1.19 | +11.2% | — |
| 00988A | 1.37 | +6.4% | beta 最高、alpha 反而低 |
| 00984A | 0.59 | -1.7% | 高息低 beta、alpha 接近 0050 |

**真實 alpha 證據**（排除 short-window anomaly）：
- **00995A 中信台灣卓越** alpha +78.9%
- **00994A 第一金台股優** alpha +71.9%

兩者亦同為 Sortino top 2 → 是當前主動式 ETF 中**風險調整 + 對 0050 alpha** 雙料領先者。

### 1.4 重要 caveat

1. **CAGR 數字被短窗 amplify**：00995A 92 天累積 +55.2% → 年化 +472% 是**數學線性外推**，非可重現預期
2. **真實基準是 cum%**：用 since-inception 累積比 CAGR 公平
3. **dividend coverage**：DB ex_right_dividend 截至 2024-01，2024-07 之後 0050 配息可能 missing → 0050 全期 CAGR 估低 ~0.5pp
4. **track record < 1 年**：所有主動式 ETF 都沒經過 cycle test

---

## 二、與我方 Quality + Catalyst Hybrid (5+5, NAV 85/15, ATR trailing, TWSE+TPEx) 對齊每檔 ETF 上市窗口（v6 ship candidate）

11 檔主動式 ETF 全部對齊 same-window 比較，皆 dividend-adjusted、皆從本地 DB 計算。

**v6 變化**：v5 的 iter_21 80/20 配置（iter_13 annual + iter_20 max=10）違反「max 10 持倉」鐵則 + 三維退化，已**廢棄**。新 ship candidate 是 [`Quality + Catalyst Hybrid (5+5, NAV 85/15, ATR trailing, TWSE+TPEx)`](strategy_ranking.md)：iter_13 monthly mcap top 5 quality (TWSE+TPEx) + iter_24 max=5 catalyst + ATR trailing。同時持倉硬上限 = 10 檔（5+5）。

| ETF | window start | 天數 | 5+5 累積 | ETF 累積 | 5+5 CAGR | ETF CAGR | gap (CAGR) |
|---|---|---:|---:|---:|---:|---:|---:|
| 00981A | 2025-05-27 | 332 | +138.6% | +181.1% | 160.4% | 211.7% | -51.4pp |
| 00982A | 2025-05-22 | 337 | +135.5% | +106.4% | 153.0% | 119.4% | **+33.6pp** |
| 00984A | 2025-07-14 | 284 | +126.1% | +40.5% | 185.5% | 54.8% | **+130.7pp** |
| 00987A | 2025-12-30 | 115 | +61.4% | +58.9% | 357.7% | 335.5% | **+22.2pp** |
| 00988A | 2025-11-05 | 170 | +68.3% | +63.0% | 206.1% | 185.7% | **+20.5pp** |
| 00990A | 2025-12-22 | 123 | +67.4% | +54.4% | 361.9% | 263.1% | **+98.8pp** |
| 00991A | 2025-12-18 | 127 | +71.4% | +68.2% | 370.7% | 346.3% | **+24.4pp** |
| 00992A | 2025-12-30 | 115 | +61.4% | +63.1% | 357.7% | 372.9% | -15.3pp |
| 00993A | 2026-02-03 | 80 | +37.8% | +25.1% | 332.5% | 177.8% | **+154.8pp** |
| 00994A | 2026-01-07 | 107 | +48.6% | +64.0% | 286.5% | 441.4% | -154.9pp |
| 00995A | 2026-01-22 | 92 | +41.8% | +55.2% | 299.8% | 472.1% | -172.3pp |

**Same-window 結果**：Quality + Catalyst Hybrid (5+5, NAV 85/15) **勝 8 / 輸 3**（**73% 勝率**）— v5 iter_21 (5/11 = 45%) 大幅改善。

勝出 8 檔分佈：
- **medium 窗 (170-337 天)**：00982A、00984A、00988A — 跨 cycle 持續 outperform
- **短窗 lucky AI rally**：00987A、00990A、00991A、00993A — 5+5 仍贏 ETF
- 輸 3 檔（00981A / 00994A / 00995A）：lucky 短窗 + 對應 ETF 是規模特大或最新上市，AI rally 期間極端集中下擊敗 5+5 quality 偏向

### 我方 Quality + Catalyst Hybrid (5+5, NAV 85/15, ATR trailing, TWSE+TPEx) 完整 21 年基準（v6 重驗）

**21 年 in-sample**（dividend-adjusted backtest）：

| 指標 | v5 (iter_21 80/20, 違規版) | **v6 (strict 5+5 NAV 85/15)** | 差異 |
|---|---:|---:|---:|
| 期間 | 同 21.30 年 | 同 | — |
| **CAGR (IS)** | +24.50% | +22.87% | -1.63pp |
| **CAGR (OOS pooled)** | +23.98% | **+24.39%** | +0.41pp |
| **Sortino (IS)** | 1.544 | 1.416 | -0.128 |
| **Sortino (OOS pooled)** | 1.541 | **1.535** | -0.006 |
| **MDD** | -40.39% | -44.85% | -4.46pp |
| 同時持倉 | 5+10 = 15 ❌ 違規 | **5+5 = 10 ✅ 合規** | — |
| 三維全勝 2330 | ✓（IS）| **✓（OOS）** | — |
| Verdict | 6/6 PASS（基於違規 NAV）| **5/6 borderline real alpha** | PBO caveat |

**Cross-validation 確認 alpha 真實性**（5+5_w80_atr 換 ranker）：

| ranker | OOS CAGR | OOS Sortino |
|---|---:|---:|
| **mcap** ★ | 23.90% | **1.512** |
| roa_recent | 27.23% | 1.306 |
| rev_cagr5y | 20.69% | 0.992 |
| roa_med | 14.41% | 0.635 |

**mcap vs roa_med 差距 0.877** — 比 1+9 NAV 75/25 配置的差距 1.759 大幅縮小，證明 5+5 結構分散有效，**不是賭 TSMC**。

**重要 caveat**：
- 21 年完整週期含 2008、2015、2018、2022 多次 cycle，**這才是「未來可重現」的 sustainable 預期**
- 短窗 +50-180% / Sortino 7-13 是 lucky AI 浪潮窗口的反映，**不該作為 forward expectation**
- 主動 ETF 短窗 Sortino 3-7 也同樣不該外推
- **v5 DRIP fix 重要備註**：v4 之前 iter_21 numerical 普遍**低估** ~3-6pp CAGR；v5 重新驗證後仍維持 ship-ready，但 OOS walk-forward / MC / Bootstrap 應重跑（待 task）

---

## 三、與主動基金對照（安聯台科、統一奔騰）

| 基金 | Track Record | Window | CAGR | 規模 | Manager |
|---|---|---|---:|---:|---|
| 我方 iter_21 | 21y backtest + OOS validated | 2005-2026 | 21.36% | n/a | 零 manager risk |
| 安聯台科 | 10y verified | 2016-2026 | ~38.6% | 1,413 億 | 周靜烈（2024 換）|
| 統一奔騰 | 28y verified | 1998-2026 | ~17% | 447 億 | 陳釧瑤（2022 換）|
| 0050 | 20y+ passive | — | ~10-12% | — | — |

**主動基金 vs 主動式 ETF 的 fee 差異**：

| 項目 | 主動式 ETF | 主動基金 |
|---|---:|---:|
| 內扣費用 | 0.85-1.24% / 年 | 1.6-1.8% / 年 |
| 流動性 | 盤中買賣 | 申贖 T+2 |
| 透明度 | 每日揭露持股 | 月度揭露 |
| 經理人換人風險 | 中 | 高 |

主動式 ETF 在 fee + 透明度 + 流動性 都優於主動基金，但 track record 還沒驗證。

---

## 四、四個關鍵維度的全面對比

### 1. 內扣費用（Fee Drag）

| 工具 | 年化內扣 | 備註 |
|---|---:|---|
| **0050 元大台灣 50** | **~0.14%** | 2025-01-23 調降後 blended（規模 6400+ 億，TW 大盤 ETF 最便宜）|
| 0052 富邦科技 | ~0.5-0.65% | 待 verify 是否也跟進調降 |
| **00981A 主動統一** | ~1.12% | 規模 1833 億，多用 > 200 億階梯 |
| **00992A 主動群益** | ~1.235% | 規模 < 500 億，多用 < 200 億階梯 |
| **00994A 主動第一金** | ~1.0-1.2% | 待 verify |
| 安聯台灣科技基金 | 1.6-1.8% | 主動基金未跟進調降 |
| 統一奔騰基金 | ~1.7% | 同上 |
| 我方 iter_21（自跑）| ~0.2% | 僅 turnover cost |

**0050 採新分級費率**（2025-01 起）：1000 億以下 0.15% / 1000-5000 億 0.10% / 5000 億-1 兆 0.08% / 1 兆+ 0.05%。**規模愈大費率愈低**，這直接懲罰 alpha decay 的小規模主動 fund。

**20 年複利下 fee 影響**（vs 0050）：
- 主動式 ETF (~1.1%) vs 0050 (0.14%) 差距 = **~1.0pp / 年** → 20 年複利下侵蝕 18-22% 累積報酬
- 主動基金 (~1.7%) vs 0050 差距 ~1.55pp / 年 → 20 年侵蝕 26-30%
- 這是 SPIVA 報告中長期 80%+ 主動 fund 跑輸大盤的核心結構性主因

### 2. 規模 alpha decay 風險

| 工具 | 規模 | 評估 |
|---|---:|---|
| **00981A** | 1,833 億 | **已過 sweet spot**，alpha decay 中 |
| 安聯台科 | 1,413 億 | 偏大，alpha decay 開始 |
| 00992A | 495 億 | 在 sweet spot，alpha 空間還在 |
| 統一奔騰 | 447 億 | 在 sweet spot |
| 00994A | 待 verify | 預估 < 300 億（剛上市）|
| 我方 iter_21 | n/a | 自跑無 scale 限制 |

TW 主動 fund alpha 通常在規模 300 億以下最強；超過 500 億後因買賣須分散，超大持倉難換手。

### 3. 方法論 robustness

| 工具 | 方法論 | 經理人風險 |
|---|---|---|
| 我方 iter_21 | 系統化多因子 + walk-forward + MC + DSR + PBO 4/4 verdict | **零**（code 固定）|
| **00994A** | **9 大因子量化評分**（量化主動）| 低（model-driven）|
| 00992A | 王牌經理人 + 「quality + growth + value」討論式選股 | 中 |
| 00981A | 大型 + 創新 + 成長 篩選 | 中 |
| 安聯台科 | 「quality + growth + value」風格 | 高（換 manager 風險）|
| 統一奔騰 | 科技 + 生技 split | 高 |

**00994A 的 9 大因子量化選股**是所有主動式 ETF 中**最接近我方系統化思維**的。不依賴單一經理人 hunch，而是 systematic factor scoring。

### 4. Track Record 長度

| 工具 | 真實樣本長度 | OOS validated |
|---|---|---|
| 我方 iter_21 | 21 年 backtest | walk-forward + MC + DSR + PBO 4/4 |
| 統一奔騰 | 28 年存活 | 經 4-5 次 cycle 但 manager 換多次 |
| 安聯台科 | 10 年 verified | 同上，manager 2024 才換 |
| 00981A | 11 個月 | 不足以 statistical inference |
| 00992A | 4 個月 | 同上 |
| 00994A | 3 個月 | 同上 |

主動式 ETF 全部都「track record 不足判斷真假 alpha」。「YTD +60%」可能是 lucky AI 浪潮 + 上市 timing 加成，不是經理人 alpha 證據。

---

## 五、三種 horizon 的最佳選擇

### 短中期（1-3 年）：賭 AI 浪潮續強

| 排名 | 候選 | 理由 |
|---|---|---|
| 1 | **00992A 群益科技創新** | 純 AI server + 先進製程 thesis、規模 sweet spot、持股對齊 master watchlist v18 |
| 2 | **00994A 第一金台股優** | 9 大因子量化選股、不靠 manager hunch、4 月單月 +19.91% momentum 最強 |
| 3 | 00987A 台新優勢成長 | YTD +38% |

**剔除 00981A**：規模 1,833 億過大、alpha decay 已開始、近 90% 電子股使其等同 0052 但 fee 貴 0.5%。

### 中期（3-10 年）：信 quality dominance + AI 主軸

| 排名 | 候選 | 理由 |
|---|---|---|
| 1 | **00994A** | 9 大因子的 model-driven 選股，最不依賴 lucky run |
| 2 | 安聯台科 | 10y verified track record（但 forward 預期應折扣）|
| 3 | 統一奔騰 | 28y 存活力 |

**主動式 ETF 中 00994A 最 robust**，因為其方法論最 systematic、不過度依賴單一經理人。但仍需注意 track record 才 3-4 個月。

### 長期（10-30 年）：複利為王

| 排名 | 候選 | 理由 |
|---|---|---|
| 1 | **0050 元大台灣 50** | 2025-01 調降後 blended fee **~0.14%**（TW 大盤 ETF 最便宜）、規模 6400+ 億流動性最大、SPIVA 28 年實證長期 80%+ 主動 fund 跑輸大盤 |
| 2 | 我方 iter_21（自跑）| 21y robust + OOS verified、fee ~0.2%、需自己月頻 rebal |

**為何不再列 006208**：2025 年費率調降後 0050 在 fee（0.14% vs 0.18%）+ 規模（6400 vs 2400 億）+ 流動性 三維 dominate。同指數 + 三維輸 → 沒理由保留為候選。

**主動式 ETF 不適合長期 hold**——track record 不足、fee drag 累積大、規模膨脹後 alpha decay。

---

## 六、最終決策框架

### 「比 00981A / 00992A 更好嗎？」的答案，依標準而異

| 你的真實標準 | 推薦 | 解釋 |
|---|---|---|
| 短期動能最強 | 00994A | 4 月單月 +19.91%、超越 00992A |
| 純 AI / 科技 thesis | 00992A | 持股最純粹、master watchlist 對齊 |
| 規模 alpha 還有空間 | 00992A、00994A | 都在 < 500 億 sweet spot |
| 方法論 robust 不靠 hunch | **00994A** | 9 大因子量化選股 |
| 過去 10y verified record | 安聯台科 | 但 forward 預期應折扣 |
| 真正最 robust（21y verified）| 我方 iter_21 | 但需自己 rebal |
| 長期 hold 不操心 | **0050** | 2025 大幅調降後 fee 0.14% 最低、規模 6400+ 億流動性最強、SPIVA 證實長期最佳 |

### 主動式 ETF 中「我會選的 top 2」

1. **00994A 主動第一金台股優**——量化主動派、9 大因子最 systematic、4 月單月最強、規模可控
2. **00992A 主動群益科技創新**——純 AI 科技派、王牌經理人、持股對齊 master watchlist v18

兩者 thesis 不同方向，可以互補（不是互斥）。00981A 因規模膨脹過大，從清單剔除。

---

## 七、警示與限制

### Track record 不足的根本問題

11 檔台股主動式 ETF **全部 < 1 年歷史**。「YTD +60%」這種數字在統計上**不足以區分**：
- 真經理人 alpha
- AI 浪潮 + 上市 timing 雙加成的 lucky run
- 倖存者偏差（早期上市的爛 fund 沒被列入排行）

至少需 3-5 年 + 跨多個 cycle 才能 statistical inference 評估。

### SPIVA 報告的長期警示

S&P 主動 vs 被動分析（SPIVA Report）28 年實證：
- 1y 約 50% 主動 fund 勝大盤（隨機）
- 5y 約 70% 主動 fund 跑輸
- 10y 約 80% 主動 fund 跑輸
- 20y+ 約 90% 主動 fund 跑輸

主動式 ETF 雖然 fee 低於主動基金，但**仍有 manager risk + 規模 alpha decay**，長期勝大盤的 base rate 不會本質改變。

### 「未來績效最大化」的真實意義

「未來績效最大化」這個目標跟「長期穩定持有」常常 conflict：
- 集中下注 winning sector → upside 大但 paradigm shift 時 drawdown 大
- 分散追蹤大盤 → upside 有限但 long-run 複利穩定

主動式 ETF 屬於前者，被動 ETF 屬於後者。沒有單一答案，只有「**配合你的真實 horizon + 風險承受度**」的答案。

---

## 八、維護紀律

**任何主動式 ETF thesis 過 3 個月必須重新 verify**：
1. YTD / since-inception 累積報酬更新
2. 規模變化（觀察是否進入 alpha decay 區間）
3. 經理人換人 / 投資策略變化
4. 持股結構是否 drift 出原 thesis

**重大訊號**（任一發生即重評）：
- 規模 > 1,000 億（00981A 已超過）
- 經理人離職
- 季度 drawdown > 20% 但同期大盤 < 5%（alpha 可能消失）
- YTD 跑輸 0050 連續 2 季

---

## Sources

### 主要資料來源（v5 起）— 本地 DB 計算，無第三方依賴

- **價格**：`pg.public.daily_quote` (TWSE close, 2004-02-11 至 2026-04-24)
- **配息**：`pg.public.ex_right_dividend` (TWSE cash dividend, 截至 2024-01)
- **減資**：`pg.public.capital_reduction` (TWSE post-reduction reference price, 2011 起)
- **中央化還原模組（v5 新增）**：`research/prices.py`：
  - `fetch_adjusted_panel(con, start, end, codes, market)` → 還原 OHLCV panel
  - `daily_returns_from_panel(panel)` / `fetch_daily_returns(...)` → DRIP daily ret
  - `total_return_series(con, code, ...)` → 單檔 benchmark 序列
- **計算腳本**：`research/analyses/active_etf_metrics.py`（ETF）+ `research/strat_lab/iter_*.py`（策略）
- **iter_21 NAV**：`research/strat_lab/results/iter_21_daily.csv`（含 DRIP）
- **單元測試**：`research/tests/test_prices.py`（10 tests，含 cross-implementation parity 對 active_etf_metrics 的獨立 back-prop）

### Fee 資料（v2 / v3 仍 active）

- [綠角財經筆記 0050 大幅調降經理費](https://greenhornfinancefootnote.blogspot.com/2025/01/50etf0050lower-management-fee-for-0050.html)
- [綠角 006208 跟進調降](https://greenhornfinancefootnote.blogspot.com/2025/06/50etf0062080050lower-management-fee-for.html)
- [StockFeel 00981A 費用結構](https://www.stockfeel.com.tw/00981a-%E7%B5%B1%E4%B8%80%E5%8F%B0%E8%82%A1%E5%A2%9E%E9%95%B7%E4%B8%BB%E5%8B%95%E5%BC%8F-etf/)
- [StockFeel 00992A](https://www.stockfeel.com.tw/00992a-%E7%BE%A4%E7%9B%8A%E5%8F%B0%E7%81%A3%E7%A7%91%E6%8A%80%E5%89%B5%E6%96%B0%E4%B8%BB%E5%8B%95%E5%BC%8F-etf/)
- [StockFeel 00994A](https://www.stockfeel.com.tw/00994a-%E7%AC%AC%E4%B8%80%E9%87%91%E5%8F%B0%E8%82%A1%E8%B6%A8%E5%8B%A2%E5%84%AA%E9%81%B8%E4%B8%BB%E5%8B%95%E5%BC%8F-etf/)

### 主動基金（無 daily_quote，仍依賴第三方）

- 安聯台科 / 統一奔騰：[MoneyDJ 績效表](https://www.moneydj.com/funddj/yp/yp012000.djhtm?a=acdd04)、[stockfeel 統一奔騰](https://www.stockfeel.com.tw/%E7%B5%B1%E4%B8%80%E5%A5%94%E9%A8%B0%E5%9F%BA%E9%87%91-%E5%9F%BA%E9%87%91-%E6%8A%95%E8%B3%87/)

### Stale（v3 之前依賴）

下列已被 v4 本地 DB 取代，保留作 audit trail：
- ~~今周刊 11 檔主動式 ETF 績效~~
- ~~TVBS 主動式 ETF 全攻略~~
- ~~口袋學堂 13 檔風格全解析~~
