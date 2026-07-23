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

## 2026-07-24(下午)dimension-① 內容正確性:cache-vs-PG 抓不到的三類 raw 汙染

**關鍵洞察(使用者點破)**:當年 remediation 的驗證是「cache 對 PG 逐表比對全綠」,但 cache
與 PG **同源同錯**(共用 Scala 爬蟲餵資料),**兩邊都錯的汙染**綠燈照過。這類只能對 raw 本身
/對現實檢驗。逐一以確定性工具掃出並清除(工具落地 `src/quantlib/verify/`,可重跑防復發):

- **① 錯日**(`content_dates`:raw 檔頭日≠檔名日):掃出 49 檔(下載游標錯位把 A 日資料存到
  B 日檔名),其中 **22 個真交易日 cache 裝著別天數字**(dtd 2023-10-06 存 2017-12-18 法人、
  stock_per_pbr 2014-12-16 存 12-18 PB/PE)。`refetch_wrongday` 逐檔重爬修正(TWSE 對歷史日
  回正確資料、parser 內容日守衛驗證);27 個非交易日回無資料、cache 本就 0。複驗 0 錯日。
- **② 幽靈日**(`ghost_days`:兩日期整日內容指紋碰撞):掃出 40 個(TWSE 對非交易日請求回鄰日
  資料、標頭卻印請求日 → 檔頭對、content_dates 抓不到)。dtd 2025-11-12 曾存 2017-12-18 的
  908 列(重爬修為 1259 正確列)。修:真交易日幽靈重爬取正確值;非交易日幽靈直接移除(刪
  cache + raw 0-byte sentinel);端點無法服務的老日期(margin tpex 2008-08-29)以融資餘額
  連續性判定為幽靈後移除。複驗 0 幽靈。
- **③ 截斷**(`raw_integrity`:TPEx 缺「共N筆」結尾):全史唯一 1 檔 2017-04-17 TPEx(21 檔缺、
  Python parser 完整性守衛拒之致 cache 0 列),重爬修為 734 列。
- **總閘 `raw_integrity`**:一指令跑齊三類,現況全綠 → 可從 raw 全量 rebuild = 正確 cache。

**parser 層 tracker bug spot-check(當前 Python parser)**:int32 溢位已修(00403A 2026-05-12
dealers_diff = -24.3 億 Int64,Scala 曾溢位成 0)、自營商欄對位正常、`financial_analysis` 表
已退役(A-financial_analysis「近半錯」作廢)。

**authoritative 全量 rebuild(使用者定調:用最正確 raw 一次 parse 進 DuckDB)**:raw 三類汙染
清除後,`rebuild --all --allow-shrink` + `--quarterly` 從乾淨 raw 全量重建 → **cache = parser(raw)
by construction**,消除任何 PG 殘留、且等於把 parser 對全史 raw 跑一遍(parse 無錯 = parser 過關)。
使用者定調:舊錯誤資料乾淨移除,專案只留最新完整正確的資料。

## 2026-07-24 raw-vs-cache 全源對照定案(工具 `quantlib.verify.raw_coverage`)

**教訓先行**:先前把 `pipeline_health`(只看 cache 一層)報的「落後/稀疏」誤讀成「資料缺、
要重抓」,還啟動 insider 背景重下載。經使用者提醒盤點 raw,發現 **raw 全在專案裡**——真問
題是 rebuild 沒吃全,不是資料缺。原則:任何「疑似缺」動手前先 `find data/` 盤 raw;cache 落
後→先 rebuild(從既有 raw),不重抓(記憶 `feedback-inventory-raw-before-concluding-missing`)。

工具逐結構化源對照「raw 檔實際涵蓋 ↔ cache」,並以 daily_quote 當交易日曆真源交叉過濾假日
雜訊。定案:

- **✅ 股票資料管線關鍵路徑完整**:daily_quote / dtd / margin / foreign / sbl / stock_per_pbr /
  operating_revenue / bs / is 全部 **cache ⊇ raw 資料日**。工具初報的「缺 N 日」經抽驗**全是
  假日/補班/無資料檔**(0-byte sentinel、TPEx『共0筆』363B 頁、header-only 探針 116B、Saturday
  補班日輔助源 2~4B 空檔)——parser 正確吐 0 列,非缺。
- **✅ insider_holding 更正(推翻前一版「欠收需數小時爬」)**:raw 2030 檔早在專案裡
  (2007-2015 + 2020 + 2026 稀疏),只是 cache 沒吃全。從全 raw canonical rebuild:
  **12,593 → 15,285 列**。剩餘「105 缺日」抽驗全為「當日無內部人申報」空頁(非缺)。
- **✅ ex_right / capital_reduction / treasury = 全史 dump**:raw 檔名雖近年(2020+/2026),但
  **每檔內容涵蓋全史**(TWSE/MOPS 端點回全史,檔名只是查詢標記),parse 逐列 == cache
  (29,625 / 666 / 5,768)。→ 無歷史缺口、DROP+rebuild 安全。(一度誤讀檔名為事件日而發
  「歷史損毀」警報,已更正;`rebuild._write` 仍留一般性不准縮表防護防未來 parser 退化。)
- **🔴→ 死掉的休市日曆(高嚴重,已修)**:`data_calendar.is_trading_day` 因 research→src/quantlib
  改名後 `parents[1]` 自算根深一層 → QUOTE_DIR 指向不存在的 `src/data/...` → 讀不到任何
  sentinel → **每個假日(颱風/勞動節/端午)誤判成交易日**。改走 `paths.RAW`;守護
  `test_data_calendar.py`(路徑不變式 + sentinel/週末行為)。
- **🟡 taifex_futures_final_settlement 缺 1998**(真缺,低優先):raw `1998.html`(277 KB 真資料)
  在,但 futures 載入從 1999 起 → 期貨結算表缺 1998 一年。屬 futures 子系統(非股票策略路徑),
  待期貨戰役隨 institutional 停更一併補。
- **🔴→ 無法回補 tdcc_shareholding**:端點只給當週 → 結構性無法回補(現 2026-04~07),端點限制非 bug。

**範圍界定(使用者定調)**:只補**可量化結構化**源;需 LLM 語意分析者(MOPS 重大訊息 free-text、
法說會逐字稿)不進管線,未來每日爬蟲亦同。

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
