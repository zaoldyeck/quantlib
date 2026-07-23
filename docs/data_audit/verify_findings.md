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
- **覆蓋驗證(health harness `quantlib.verify.pipeline_health` 逐源掃描,定性)**:
  - **✅ market_index 從 2009 = 正確(誤報排除)**:2005/2007 raw 僅 2 個 header-only probe 檔
    (116 bytes 無資料列),該端點真資料 2009 起。重建確認 cache 2009+ 全對(0 errors)。
  - **🟠 insider_holding 嚴重欠收**:771 列/76 交易日(2007+ 可回溯應 ~47,500)——2026-04 才加的源,
    只收了近期幾天,2007-2026 歷史未回補。**可回補**(MOPS t56sb12 2-step ajax),但需逐日爬(~4750
    交易日 × 2 市場,rate-limited,~數小時)→ 大型網路回補任務。
  - **🔴→ 無法回補 tdcc_shareholding**:endpoint 只給當週快照 → 歷史**結構性無法回補**,只能往前累積
    (現 2026-04~07)。屬端點限制、非 bug。
  - **🟡 taifex 法人/結算停更**(institutional 2023~2026-05、settlement ~2026-05,落後 63 天):
    不在 update.py 刷新路徑;屬期貨研究用(非股票策略),低優先 → 補進刷新 + 回補待期貨戰役。
  - 近日 staleness(各源到 2026-07-20、缺 07-21~23)= cache 未跑今日 update,非 bug(daily loop 補)。
- **🟡 parallel_parity.json**:pull_kbars 的平行自證 state 檔誤放 `data/intraday/`(RAW),
  應在 var/ → Phase 1 產物遷移一併處理。
- **🟡 cash_flows 封存**:用自訂 `_archive_zip`(功能對、有原子性),未用共用 `archive.save_raw_bytes_at`
  → Phase 1/2 可收斂重用共用 helper(非缺口,小重複)。
