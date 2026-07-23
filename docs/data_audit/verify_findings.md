# 資料 Pipeline 100% 驗證 — 發現與待修(2026-07-23 起,post-PG 新架構)

五面向:① 資料正確性 ② 有無缺口 ③ Parser 正確 ④ 存入 cache 正確 ⑤ 算法/公式正確。
方法:直接測試 + 實測 + 讀碼(不派對抗稽核 agent)。發現即修;跨 Phase 者記此。

## Phase 0(raw 封存地基)已修

- **✅ 封存缺口(鐵律違反)**:`operating_revenue`、`capital_reduction` 的 fetch 沒封存 raw
  → 補 `archive.save_raw*`(operating_revenue `{年}_{月}_c.csv`;capital_reduction 範圍檔
  `{end}_r.csv`)。守護 `test_raw_archive_coverage.py`(18 源靜態掃描全綠)。
- **✅ raw 集中統一**:兩源拆兩地已合併——`index/`(10,149 歷史)→`market_index/`;
  `stock_per_pbr_dividend_yield/`(14,762 歷史)→`stock_per_pbr/`(canonical = cache TABLE 名)。
  rebuild `_RAW_DIR` 改為 `{"index":"market_index"}`(index 模組 raw 在 market_index/);
  stock_per_pbr 不再需映射。舊目錄已除,rebuild 驗證讀到全史。
- **✅ 清 stray**:`data/tw_market.db`、`research/market_data.db`(無引用 DuckDB stray)刪除。

## Phase 2 待修(正確性/缺口,Phase 0 合併時揭露)

- **✅ capital_reduction 端點失效(已修,money-path)**:根因 = TWSE 2026-07 把參數名
  `strDate`→`startDate`(TPEx 那條早就對),舊參數回空 `\r\n`。改一字修好——2026 H1 回 2 筆
  (1414/2380 對上 cache)、raw 封存。舉一反三:全源僅此一處用舊 strDate。影響過:未來新減資
  會被 update.py 捕捉。
- **✅ stock_per_pbr TPEx「截值」= 誤報(已釐清)**:raw 兩版差異在**每股股利欄**(第 4 欄,
  parser docstring 明寫「刻意不接」),非殖利率;同列 PE/DY/PB 兩版**完全相同** → cache 值
  不受影響、無需 rebuild。raw 合併仍有效(統一目錄)。
- **🟠 覆蓋缺口(初掃)**:insider_holding 僅 156 檔/771 列(19 年應 ~47,500);
  tdcc_shareholding 僅近 3 月(歷史回補從未做);taifex 三大法人從 2023(日資料 1998 起);
  各源最新日不齊(07-17/07-20/07-23 錯位);market_index 從 2009、capital_reduction 從 2011
  ——待逐源對「真正最早可得日」驗證。
- **🟡 parallel_parity.json**:pull_kbars 的平行自證 state 檔誤放 `data/intraday/`(RAW),
  應在 var/ → Phase 1 產物遷移一併處理。
- **🟡 cash_flows 封存**:用自訂 `_archive_zip`(功能對、有原子性),未用共用 `archive.save_raw_bytes_at`
  → Phase 1/2 可收斂重用共用 helper(非缺口,小重複)。
