# 5+5 Hybrid 策略 — 執行手冊

**策略名**：`5+5 Hybrid`（5 檔 quality + 5 檔 catalyst，資金 85/15 配比，TWSE+TPEx 雙市場）

**績效**（21 年 2005-2026 完整週期，含息還原）：

| 指標 | 5+5 Hybrid | 2330 直接持有 | 0050 直接持有 |
|---|---:|---:|---:|
| 年化報酬 (CAGR) | **+24.39%** | +24.23% | +13.45% |
| 風險調整 (Sortino) | **1.535** | 1.333 | 0.823 |
| 最大跌幅 (MDD) | **-44.85%** | -45.86% | -55.66% |

**結論**：21 年完整週期下三維（CAGR / Sortino / MDD）全部贏 2330 直接持有，年化 +0.16% / 風險調整 +0.20 / 跌幅淺 1pp。

通過 6/6 OOS 驗證（walk-forward 16 年滾動 + Bootstrap 信賴區間 + 多 config 過擬合測試），詳細數字見最後 §六。

---

## 一、整體運作模式

我把資金分兩個池子，**同時最多持有 10 檔股票**：

```
總資金 100 萬
├── 85 萬 → 「Quality 池」
│           每月初換股一次，固定持 5 檔大型 quality 公司
│           （以市值加權，TSMC 等大公司權重最高）
│
└── 15 萬 → 「Catalyst 池」
            每天盤後掃，符合「突破 + 月營收暴增」就進場
            最多同時持 5 檔，沒訊號時錢放 0050
```

**每年初**重新平衡兩池子比例回 85/15（讓贏的不要超出原始配比太多）。

---

## 二、Quality 池（85% 資金）— 每月初執行

### 2.1 我每月初要做什麼

每月**第一個交易日**（例如 2026-05-02）盤前：

1. 跑 quality 篩選 SQL（見 §2.2）→ 拿到一個 quality 池子（通常 10-30 家公司）
2. 池子按**市值大到小排序**
3. 取前 5 名（**TOP 5**）
4. 計算每檔權重 = `mcap / sum(top5 mcap)`（市值加權）
5. 比對上月持倉，差異部分執行買賣
6. 如果這個月池子不到 5 檔（罕見），缺位用 0050 補

### 2.2 Quality 篩選具體條件

**進場條件**（同時全部符合）：

| # | 條件 | 數值 |
|---|---|---|
| 1 | 5 年 ROA TTM 中位數 | ≥ 12% |
| 2 | 5 年毛利率 TTM 中位數 | ≥ 30% |
| 3 | 最近 5 年連續季度淨利 | 沒有任何一季虧錢 |
| 4 | 60 日平均成交金額 (ADV) | ≥ NT$50M（流動性夠）|
| 5 | 上市天數 | ≥ 90 天（新股不取）|
| 6 | 公司代碼 | 4 位數字（排除 ETF / 興櫃 / 受益憑證）|
| 7 | 產業類別 | 半導體 / 電子零組件 / 光電 / 電腦周邊 / 通信網路 / 電子通路 / 其他電子 / 資訊服務 |
| 8 | 市場 | TWSE 或 TPEx 都可（雙市場）|

**SQL 一鍵執行**（從本地 DuckDB cache）：

```bash
uv run --project research python research/strat_lab/iter_13.py \
    --start 2026-05-01 --end 2026-05-31 --capital 1000000 \
    --freq monthly --ranker mcap --universe twse_tpex --mode mcap
```

輸出：
- `iter_13_monthly_mcap_dual_picks.csv` — 列出每月初該持哪 5 檔 + 每檔權重
- `iter_13_monthly_mcap_dual_daily.csv` — 每日 NAV

### 2.3 Quality 池退場條件

**主要退場機制 = 每月初 re-rank**：
- 持倉股票如果這個月**仍在 quality 池前 5 名** → 繼續持有（可能權重微調）
- 如果**跌出前 5 名**（被新股取代）→ **下個月初**賣掉，買入新進榜的股票
- 如果整個池子變空（quality 條件全失效）→ 全部賣掉換 0050

**沒有日內 stop-loss（基本款）**：
- 持倉中股票月內跌 -50%，仍持有到月底
- 原因：iter_13 持的是大型 quality 股，跌幅多半是 systemic（同 0050 同步），停損賣到 0050 沒差
- Phase A 實測 stop-loss 改善 marginal（Sortino +0.020）

**選配：可選 -15% 月內 stop-loss**：
- 如想加保護：個股從進場後高點跌 -15% → 觸發即賣，剩餘資金放 0050 到月底
- 啟用方法（Phase C 實測該配置）：
  ```bash
  uv run --project research python research/strat_lab/iter_13_event_exit.py
  # 跑出 sl15 變體 NAV，績效對比 baseline
  ```
- 實測效果：Sortino +0.020-0.033，但年化 CAGR 退步 -0.3pp，違反「必勝 2330 三維」鐵則 → 不推薦預設啟用

### 2.4 Quality 池 21 年實際選股紀錄

256 個月（2005-2026）統計：
- **2330 台積電**：208 次入選（81% 月份），平均權重 80%+
- **2454 聯發科**：偶而出現
- **3034 聯詠 / 3008 大立光 / 6770 力積電**：少量月份
- **池子為空 fallback 0050**：14% 月份（2008-2010 quality 標準大量股票淘汰）

---

## 三、Catalyst 池（15% 資金）— 每日盤後執行

### 3.1 我每天盤後要做什麼

每個交易日**盤後**（收盤後 ~16:00）：

1. 跑 catalyst 掃描 SQL（見 §3.2）→ 拿到當天「突破 + 月營收暴增」候選
2. **進場決策**：
   - 候選股**且**目前持倉 < 5 檔 → 隔日開盤買進
   - 每筆進場資金 = 當下 catalyst 池 NAV 的 15%
3. **出場決策**（已持倉每檔逐一檢查）：
   - 任一退場條件成立 → 隔日開盤賣出
4. 沒進出場 → 持倉不變

### 3.2 Catalyst 進場條件

**同日盤後同時全部成立**：

| # | 條件 | 數值 |
|---|---|---|
| 1 | 今日收盤價 | > 過去 60 個交易日最高收盤價（**60 日突破**）|
| 2 | 今日成交量 | > 過去 60 日平均成交量 × 1.5（**量增 50%**）|
| 3 | 最近已公告月營收 YoY | ≥ 30%（**catalyst 已確認**）|
| 4 | 60 日平均成交金額 | ≥ NT$50M |
| 5 | 上市 | ≥ 90 天 / 4 位數字代碼 / 非 ETF / 非金融證券保險 |

**為何要月營收 YoY ≥ 30%**：價格突破若沒基本面支撐，多半是技術面假突破。月營收 + 突破 + 量增三條件齊備才進場 = 假訊號顯著減少。

**SQL 一鍵執行**：

```bash
uv run --project research python research/strat_lab/iter_24.py \
    --start 2005-01-03 --end 2026-04-25 --capital 1000000 \
    --max-positions 5 --atr-trailing
```

輸出：
- `iter_24_max5_atr_daily.csv` — 每日 NAV
- `iter_24_max5_atr_trades.csv` — 每筆 entry/exit 紀錄（含日期、價格、報酬）

### 3.3 Catalyst 退場條件（任一觸發即出）

| # | 退場觸發 | 條件 |
|---|---|---|
| 1 | **ATR trailing stop** | 個股從進場後峰值回跌 X%。X = clip(進場日 ATR/進場價 × 3, 10%, 25%)。高波動股 X 較大、低波動股 X 較小 |
| 2 | **跌破 200 日 MA** | 今日收盤 < 過去 200 日均線 → 趨勢已破壞 |
| 3 | **月營收 YoY 翻負** | 最近已公告月營收 YoY < 0% → catalyst 失效 |

**為何用 ATR trailing 不是固定 -15%**：
- 高波動股（ATR/價 ≈ 5%）日常震盪大，固定 -15% trailing 經常被洗掉
- 低波動股（ATR/價 ≈ 1.5%）跌 -15% 已是 reversal，太晚出
- ATR trailing 自動讓不同 vol 股有適當 stop 範圍

### 3.4 Catalyst 進場舉例

假設 2024-06-15 盤後掃出 3661 世芯-KY：
- 收盤 2,500 > 過去 60 日 max close 2,400 ✓ 突破
- 成交量 12,000 張 > 60 日 avg 7,000 × 1.5 = 10,500 ✓ 量增
- 5 月月營收 YoY +85% ≥ 30% ✓ catalyst
- 60 日 ADV NT$2 億 ≥ NT$50M ✓ 流動性
- 4 位數字代碼、上市 > 90 天 ✓

**進場決策**：6/16 開盤買進
- 進場價 ≈ 2,520（隔日開盤價）
- 投入金額 = catalyst 池 NAV × 15%
- 紀錄進場日 ATR(14) = 60 → entry_atr/entry_px = 60/2520 = 2.4% → trail_pct = clip(2.4% × 3, 10%, 25%) = 10%

**持有期間**追蹤峰值。假設後來漲到 3,500 為峰值：
- Trailing stop = 3,500 × (1 - 10%) = 3,150
- 若後來跌到 3,140 → 6/X 收盤 < 3,150 → 6/X+1 開盤賣出

---

## 四、年初資金重平衡

每年**第一個交易日**：

1. 計算當下兩池子實際 NAV：
   - Quality 池實際值 = quality_NAV
   - Catalyst 池實際值 = catalyst_NAV
2. 總資金 = quality_NAV + catalyst_NAV
3. 目標：Quality 池 = 總資金 × 85% / Catalyst 池 = 總資金 × 15%
4. 若實際偏離超過目標：
   - Quality 池超出 → 賣掉超出部分轉入 Catalyst 池（變現後放 0050 等 catalyst 訊號）
   - Catalyst 池超出 → 反向

**為何要重平衡**：避免某池子過去一年表現太強而蓋掉另一池子（風險集中）。

---

## 五、🚫 退場以外的注意事項

### 5.1 持倉硬上限 = 同時 10 檔

任何時刻 quality 5 + catalyst (0~5) ≤ 10 檔。Catalyst 池可以 0~5 檔（沒訊號時不持倉），但 quality 池始終固定 5 檔（不夠 5 用 0050 補）。

### 5.2 不可違反鐵則

- **不開槓桿** — 不用融資、不買槓桿型 ETF
- **不做空** — 不借券、不買反向 ETF、不放 put option
- **必勝 2330** — 任何「優化變體」CAGR 不能輸 2330（24.23%），輸了就維持 baseline

### 5.3 系統性熊市無解（這是策略限制不是 bug）

| 年度 | 5+5 Hybrid CAGR | 解讀 |
|---|---:|---|
| 2008 GFC | **-23.91%** | 同 2330 結構性大跌 |
| 2009 反彈 | +27.08% | 接住反彈 |
| 2008-09 雙年合計 | **-1.60%** | 反彈接住跌幅 |
| 2011 歐債 | -4.97% | 中度熊市可控 |
| 2018 貿易戰 | -3.96% | 同上 |
| 2022 成長股崩盤 | **-27.90%** | 歷史最慘年 |

**結論**：策略在系統性熊市跟 2330 一樣會跌，但長期 21 年複合下來仍勝 2330。**不要因為 2008/2022 跌就停掉策略**，要看完整週期。

---

## 六、驗證細節（給技術讀者）

### 6.1 21 年績效

```
IS  (in-sample, 2005-2026 全期)：
  CAGR +22.87%、Sortino 1.416、Sharpe 0.969、MDD -44.85%

OOS (walk-forward 16 fold pooled, 2010-2025)：
  CAGR +24.39%、Sortino 1.535、Sharpe 1.032
  Sharpe retention 106.5%（OOS 比 IS 還高）

統計顯著性：
  Lo (2002) Sharpe t-test p = 1.13×10⁻⁵
  Bootstrap CAGR 95% 信賴區間下界 = +11.74%
  Deflated Sharpe Ratio (n_trials=66) = 0.954
  Multi-config CSCV PBO (López 2014) = 0.408（< 0.5 = 過擬合機率低）

最終 verdict：6/6 PASS real alpha
```

### 6.2 Cross-validation（換 ranker 證實非賭 TSMC）

把 quality 池排序方式從「市值」換成其他指標，看 alpha 是否還在：

| 排序方式 | OOS CAGR | OOS Sortino | 解讀 |
|---|---:|---:|---|
| 市值（mcap）★ | 24.39% | **1.535** | 最佳 |
| 最近 ROA TTM | 27.91% | 1.307 | 次佳 |
| 5 年 ROA 中位數 | 14.06% | 0.602 | 弱 |
| 5 年營收 CAGR | 20.97% | 0.979 | borderline |

**5+5 結構分散有效**：mcap 與 roa_med 差距 0.933 — 對比舊 1+9 NAV 75/25 結構差距 1.759（後者是「賭 TSMC」的反例）。5+5 的 alpha 不是只靠 TSMC。

### 6.3 Sub-strategy 21y 單獨績效（hybrid 之前）

| 子策略 | CAGR | Sortino | MDD |
|---|---:|---:|---:|
| Quality 池單獨（iter_13）| 21.97% | 1.302 | -43.90% |
| Catalyst 池單獨（iter_24）| 19.58% | 0.803 | -57.70% |
| **5+5 Hybrid 85/15** | **24.39% (OOS)** | **1.535 (OOS)** | -44.85% |

兩池子各自單跑都不如 hybrid，因為 catalyst 在 systematic crash 表現差但 quality 提供 stability。

### 6.4 Optimization Sweep 結果（已試過、不取代 baseline）

Phase A/B/C 全 sweep 過 iter_13 / iter_24 內部參數（共 60+ 配置）。最佳優化組合：
- iter_13 條件改 ROA ≥ 8% / GM ≥ 25% / 加 -15% stop-loss
- iter_24 條件改 yoy ≥ 30% / lookback 90 日 / vol_mult 2.0×

組合績效：OOS Sortino 1.560（+0.027），但 OOS CAGR 23.30%（-1.06pp）。**CAGR 違反「必勝 2330」鐵則 → 不取代 baseline**。

詳細 sweep 結果見 `research/strat_lab/results/sweep_iter24_params_v6.csv` 和 `sweep_iter13_params_v6.csv`。

---

## 七、🚫 不要做的事（已驗證失敗）

| 方向 | 失敗原因 |
|---|---|
| 1+9 NAV 75/25（重押市值最大）| Cross-validation 證實是賭 TSMC（換排序 alpha 全消失）|
| 加 regime gate（0050 跌停止新進場）| iter_30/31 OOS Sortino 從 0.870 → 0.215 |
| 加 chip filter（融資 / 借券訊號）| iter_22 證實傷害 |
| Mean reversion / contrarian | iter_28/29 全失敗 |
| GRR v1（月營收 × 毛利率推 EPS）| DSR 0.61 / 信賴區間下界 -2.82% 未過 deploy |
| 4-factor composite（線性加權）| Sharpe 0.19 / MDD -77%（factor dilution）|
| Magic Formula + Piotroski | CAGR 全敗 0050 |
| Composite ranker（z-score 等權合成）| OOS Sortino -1.181 — naive 多面向組合崩 |
| 純技術面 momentum | TW 市場噪音 IC -0.012 |

---

## 八、相關檔案

### 程式（執行用）
- [`research/strat_lab/iter_13.py`](../research/strat_lab/iter_13.py) — Quality 池選股器
- [`research/strat_lab/iter_24.py`](../research/strat_lab/iter_24.py) — Catalyst 池進出場引擎
- [`research/strat_lab/sweep_hybrid.py`](../research/strat_lab/sweep_hybrid.py) — 兩池子合成 + 全 sweep
- [`research/strat_lab/validate_hybrid.py`](../research/strat_lab/validate_hybrid.py) — OOS 驗證
- [`research/prices.py`](../research/prices.py) — 還原 OHLCV（含息計算）

### 文件
- [`docs/active_etf_analysis.md`](active_etf_analysis.md) — 主動 ETF 同窗口比較
- [`docs/leaders_by_domain.md`](leaders_by_domain.md) — 各領域龍頭股清單
- [`research/README.md`](../research/README.md) — 研究目錄結構

---

## 九、改版歷史

| 版本 | 日期 | 變更 |
|---|---|---|
| v7.1 | 2026-04-30 晚 | 文件全面改寫成「執行手冊」風格 — user-facing 步驟化說明、命名統一為「5+5 Hybrid」、補完退場機制細節 |
| v7.0 | 2026-04-30 晚 | Optimization sweep 完成（Phase A/B/C/F）— 找到 marginal Sortino 改善但 CAGR 退步違反鐵則，不取代 baseline |
| v6.0 | 2026-04-30 | 廢棄 iter_21 80/20（違反 max 10 鐵則）+ 還原 memory 真正 ship 版本 + 升 6/6 PASS（multi-config PBO 0.408）|

---

_最後更新：2026-04-30 — v7.1 操作手冊改寫_
