# APEX Campaign 憲章

**使命**:研發台股(TWSE + TPEx)最強純量化交易策略,無限迭代直到頻譜推不動為止。
**血統**:全新 campaign(2026-07-09 啟動),不繼承任何歷史策略 catalog 的思路與窗口慣例;
只沿用技術底座(`var/cache/cache.duckdb`、`src/quantlib/prices.py` 正典調整價、uv 環境)與市場物理事實。

## 硬約束(使用者指定,不可違反)

1. **Long-only、零槓桿**(投入 ≤ NAV,無融資/期貨對沖)。
2. **純量化**:不抓消息面/文字資料。
3. **無 AI**:禁止 ML 預測器(GBM/NN/genetic 優化)。允許:規則、排序、門檻、
   固定權重線性組合、古典統計檢定(這些是驗證工具,不是預測器)。
4. 方法不設限:基本面、技術面、SMC、TPO/量價分佈、OFI/VPIN 日頻 proxy、型態、
   月營收事件驅動、每日門檻系統……全部納入探索空間。

## 窗口政策(第一性原理設計)

| 窗口 | 範圍 | 用途 |
|---|---|---|
| **Development** | 2012-01-02 → 2023-12-29 | 自由迭代與調參(12 年,跨 7%/10% 漲跌幅兩制度、2015 股災、2018 貿易戰、2020 COVID、2022 熊市、2023 AI 多頭)|
| **Validation** | 2024-01-02 → 2025-06-30 | 只給通過 dev 門檻的候選跑,確認無衰減 |
| **Final holdout** | 2025-07-01 → 2026-07-07 | **全 campaign 動用預算 5 次**,只給最終冠軍 |
| **Stress(只評估)** | 2008-01-02 → 2011-12-30 | GFC + 歐債存活測試,不調參(價量/籌碼策略適用;基本面資料 IFRS 前品質打折)|

理由:dev 拉長到 12 年防單一 regime 過擬合;近 30 個月完整留白作 OOS;
資料含 3,277 檔已消失代碼,survivorship bias 低,長窗可信。
訊號資料硬下限:法人買賣超 2007-04+、PER/PBR 2005-09+、月營收 2001+、
財報 2004+(現金流 2009+;IFRS 2013+ 才乾淨)、SBL 2016+(dev 窗只剩 8 年)、TDCC 2026-04+(不可回測)。

## 執行模型(era-aware 市場物理)

- **時序**:T 收盤決策(只用 ≤T 資訊)→ **T+1 成交**;`fill_at` 預設 `next_open`,
  重要結果需附 `next_close` 敏感度。
- **成本**:手續費 0.0285%/邊 + 證交稅 0.30%(賣)+ 滑價 0.10%/邊。
- **漲跌停擋單**:成交日相對參考價漲跌幅 ≥ 0.95 × era 限制(2015-06-01 前 7%、後 10%)
  → 買單擋掉不重試;賣單擋掉自動隔日重試(模擬連續跌停出不掉)。
- **停牌**:無 bar 不能成交,持倉以最後收盤 mark;**下市**:最後 bar 收盤強制清算。
- **部位**:`n_slots` 檔、TargetPercent = NAV/n_slots、現金不足自動縮小(零槓桿保證)、
  fractional shares(研究近似)、名目資本 NT$3M。
- **資格**:決策日 20 日中位成交值 ≥ NT$2,000 萬、raw_close ≥ NT$10、上市滿 60 根。
- **NAV**:一律走 `prices.fetch_adjusted_panel`(除息/減資/分割 total-return 等價)。

## Universe

TWSE + TPEx 普通股:4 碼純數字、非 0 開頭(排 ETF/ETN/受益證券)、非 91xx(排 TDR)、
字尾無字母(排特別股)。含全部已下市股。轉板股跨市場拼接(同日重複取 twse)。

## 晉級門檻與「更強」定義

**晉級 validation 資格(dev 窗)**:CAGR ≥ 15%、MDD ≥ −35%、Sharpe ≥ 1.0、
年報酬 ≥ 9/12 年為正、已平倉交易 ≥ 100 筆。

**Frontier improvement(challenger 打敗現任冠軍,同 dev 窗同執行模型)**,滿足任一:
- (a) CAGR +≥1pp 且 MDD 劣化 ≤2pp 且 Sharpe 劣化 ≤0.1
- (b) MDD 改善 ≥2pp 且 CAGR 劣化 ≤1pp
- (c) Sharpe +≥0.15 且 CAGR 劣化 ≤1pp 且 MDD 劣化 ≤1pp

且 validation 窗 Sharpe ≥ 0.6 × dev Sharpe(候選確認)。

**冠軍出廠 battery**:walk-forward(5y train / 1y test 滾動)、Monte Carlo permutation
p < 0.05、block bootstrap 95% CI 下界 CAGR > 10%、DSR > 0.95(用全 campaign trial 數)、
PBO < 0.5(CSCV,用全部已存 equity curves)、參數 ±20% 擾動 CAGR spread < 15pp、
fill 慣例雙測(next_open / next_close 皆須通過門檻)。

## 反過擬合協議

1. **每筆 trial 必進 `ledger/trials.jsonl`**(含 config + 全指標),equity curve 存
   `ledger/curves/`(PBO/DSR 原料)。只有主 loop 寫 ledger(並行 agent 回傳結果由主 loop 記錄)。
2. **Batch 預註冊**:每批 trial 開跑前先在 `ledger/batches.md` 寫下假設與判準。
3. **Holdout 動用帳**(上限 5 次):

| # | 日期 | 動用者 | 結果 |
|---|---|---|---|
| 1 | 2026-07-09 | champion-elect v3(A1-R 終局 evaluation-only;3 fill 慣例=1 次)| +49.2%(open)/+51.4%(mid)/+49.0%(close),Sharpe 1.51-1.57,MDD −13.9~−15.7% |

**憲章修正案**(全文與時間戳見 ledger/batches.md):A1/A1-R(終局條款:連 3 批無
全過閘候選 → champion-elect + evaluation-only holdout 結案)、A2(val 預算封頂)。
campaign 於 2026-07-09 依 A1-R 收斂終結,champion-elect = `apex_revcycle_v3`(6/7 閘),
詳 `REPORT.md`。

## 收斂準則(「無法再更強」的操作化定義)

冠軍存在且通過完整 battery 後,**連續 3 個預註冊 batch(每批 ≥8 trials、跨 ≥3 個方法家族)
無任何 frontier improvement** → 宣告收斂 → final holdout 確認 → 結案報告。
holdout 若重挫(Sharpe < 0.4 × validation)則冠軍作廢,記錄教訓,campaign 繼續。

## 目錄

```
src/quantlib/apex/
  CHARTER.md          本文件
  data.py             cache 連線 + panel 載入 + universe/資格過濾
  engine.py           事件驅動日頻模擬器(純函式核心)
  metrics.py          績效/交易統計
  ledger.py           trial 帳本 + equity curve 保存
  tests/              golden tests(parity/成本/漲跌停/T+1/trailing/下市/零槓桿)
  experiments/        各 trial 腳本(T0001_xxx.py)
  ledger/             trials.jsonl + batches.md + curves/
```
