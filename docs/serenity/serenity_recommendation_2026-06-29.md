# Serenity 結構性瓶頸股推薦報告

> 2026-06-29 修正：本報告原本把 `memory_cycle` 放入 Serenity 推薦表。經重新審核，`memory_cycle` 屬於 tactical cycle overlay，不應列入 Serenity Core。嚴格 Serenity Core 修正版請以 `docs/strategy_research/serenity_core_recommendation_2026-06-29.md` 為準。

產出日期：2026-06-29  
本地資料截止：`daily_quote=2026-06-26`、`stock_per_pbr=2026-06-26`、`daily_trading_details=2026-06-26`、`operating_revenue=2026-05`、`industry_taxonomy_pit=2026-06-13`  
候選訊號日：`2026-06-11`，對應可執行日：`2026-06-12`  
現價基準：`2026-06-26` 收盤價。標準資料刷新已於 2026-06-29 晚間執行，但官方來源尚未提供可寫入本地 DB 的 2026-06-29 日行情。

## 結論

Serenity 目前選出的主軸非常集中：記憶體循環、AI server CCL、AI server 機構件、AI PCB 設備。若只看本地數據與 DCF+PEG overlay，前十名是 `4973 廣穎電通`、`2344 華邦電`、`5289 宜鼎`、`2408 南亞科`、`6274 台燿`、`2059 川湖`、`8299 群聯`、`3167 大量`、`8271 宇瞻`、`2451 創見`。

我的判斷是：可以優先研究買進的是 `4973`、`2344`、`5289`、`2408`、`2059`、`8299`、`3167`、`8271`、`2451`；`6274 台燿` 的結構性瓶頸很強，但目前估值與風險報酬比不夠漂亮，應列為觀察，不適合追價。

### 為什麼沒有台光電？

`2383 台光電` 沒有被排除出 Serenity universe。它在最新 industry-first ranking 裡仍是第 10 名，屬於 AI server CCL 結構性瓶頸股；但正式推薦表採用的是 `DCF+PEG blend` 估值 overlay，加上現價與風險報酬比後，台光電在最新訊號日 `2026-06-11` 排第 12 名，因此沒有進入前 10 檔。

台光電的優點很清楚：5 月營收 YoY +114.6%、3 個月 YoY +88.3%、60 日報酬 +96.0%，而且 CCL thesis 很強。但現價基準日 `2026-06-26` 收盤已到 5255 元，最新估值資料本益比約 110.5、股價淨值比約 38.1，估值壓力不低。換句話說，它不是 thesis 不成立，而是「很好，但這個價格下沒有排進前十」。如果放寬到前 12 名，台光電應列為高品質觀察名單。

## 10 檔推薦排名

目標價是「保守化 DCF+PEG 交易目標」，不是原始模型 fair value。原始 DCF/PEG 對記憶體循環股給出很高的理論上行空間，這裡已用週期股 cap 處理，避免把景氣高峰當成基本情境。停損價用 20 日 ATR、20 日低點支撐與最大損失 guardrail 估算；實際委託需再依交易所 tick size 微調。

| 排名 | 股票 | 主題 | 狀態 | 現價 | 目標價 | 停損價 | 風險報酬比 | 進場計劃 | 主要風險 |
|---:|---|---|---|---:|---:|---:|---:|---|---|
| 1 | 4973 廣穎電通 | DRAM/NAND + Edge AI storage | 推薦 | 180.0 | 297 | 147 | 3.56 | 分批；若回測 170~180 守住或再站回 190 可加碼 | NAND/DRAM 價格反轉、模組庫存回補結束、股價漲幅過快 |
| 2 | 2344 華邦電 | Specialty DRAM/NAND | 推薦 | 206.5 | 341 | 155 | 2.60 | 分批；不追急拉，回測 195~205 守住再加 | 記憶體報價反轉、產能開出快於需求、毛利率回落 |
| 3 | 5289 宜鼎 | Industrial / Edge AI storage | 推薦 | 1605.0 | 2890 | 1250 | 3.63 | 分批；高單價，回測 1550~1620 守住較佳 | 工業/Edge AI 拉貨不連續、庫存循環、單價流動性風險 |
| 4 | 2408 南亞科 | DRAM supply squeeze | 推薦 | 449.0 | 741 | 337 | 2.60 | 分批；高波動週期股，回測 420~450 守住才加碼 | DRAM 週期急轉、CAPEX 擴張造成供給壓力、價格下跌 |
| 5 | 6274 台燿 | AI server CCL / low-loss materials | 觀察 | 1580.0 | 2055 | 1215 | 1.30 | 等回檔至 1450~1500，或重新突破且估值改善 | CCL 漲價不順、玻纖布緊缺緩解、估值過高 |
| 6 | 2059 川湖 | AI server slide rail / rack mechanics | 推薦 | 6990.0 | 11180 | 5240 | 2.40 | 分批；高價股用小比例，回測 6600~7000 守住才加 | AI server 出貨延後、客戶集中、超高股價波動 |
| 7 | 8299 群聯 | NAND controller / AI storage | 推薦 | 2310.0 | 3810 | 1880 | 3.48 | 分批；回測 2200~2350 守住較佳 | NAND 控制器競爭、NAND 缺貨導致出貨受限、庫存風險 |
| 8 | 3167 大量 | AI PCB drilling / equipment bottleneck | 推薦 | 765.0 | 1110 | 610 | 2.22 | 分批；PCB 設備高波動，回測 720~770 守住才加 | PCB 設備訂單遞延、客戶 CAPEX 放緩、接單能見度下降 |
| 9 | 8271 宇瞻 | Industrial SSD / DRAM module | 推薦 | 198.0 | 327 | 167 | 4.13 | 分批；回測 185~200 守住 | 模組價格波動、通路庫存、利差收斂 |
| 10 | 2451 創見 | Industrial / embedded storage | 推薦 | 278.5 | 501 | 233 | 4.85 | 分批；回測 265~285 守住 | 模組/品牌需求轉弱、庫存評價損失、匯率與通路風險 |

## 結構性瓶頸證據

### 1. 記憶體與 Edge AI storage

推薦股：`4973`、`2344`、`5289`、`2408`、`8299`、`8271`、`2451`。

這一組不是單純「股價漲很多」，而是同時符合三個條件：AI server 把高階 DRAM/NAND 產能吸走、傳統/工業/edge AI 市場仍需要穩定供應、模組與控制器廠在價格上行期有庫存與毛利彈性。這裡要嚴格區分：上游 DRAM/NAND 製造商在供給吃緊時有週期性定價權；模組、品牌、控制器廠通常不是長期 price setter，而是受惠於庫存重估、成本轉嫁能力、產品 mix 與需求彈性。TrendForce 2026-03-31 指出，2Q26 conventional DRAM contract price 預估 QoQ +58~63%，NAND Flash contract price +70~75%，原因包含 DRAM 供應商持續把產能轉往 server-related applications、NAND 產能轉往 enterprise SSD。南亞科 2026 Q1 法說也直接寫到 AI-driven CSP capex 支撐 cloud DRAM 需求，DDR5、DDR4、LPDDR4、DDR3 供給受限。

公司層面，南亞科是 DRAM 供給受限的直接受惠者；華邦電具備 specialty DRAM/NAND 與 edge AI memory 敘事；宜鼎、宇瞻、創見、廣穎電通是 industrial / embedded / edge AI storage 的下游受惠者；群聯則是 NAND controller 與 AI storage solution 的結構性受惠者。因此，這一群的投資假設應寫成「記憶體供需吃緊與 edge AI storage 需求帶來週期性獲利彈性」，而不是「整個記憶體類股都有長期定價話語權」。

### 2. AI server CCL / low-loss materials

推薦股：`6274 台燿`。

台燿被放進前十，是因為 CCL 的瓶頸是真實存在：AI server 高速訊號、GPU UBB/OAM、HDI、高層數 PCB 對 very-low-loss / ultra-low-loss CCL 的含量提升很明確。TrendForce 2026-04-30 指出 glass fiber cloth 供給限制預期到 2027 年中才會緩解，會影響 AI server supply chain 的 lead time 與成本。台燿 2026-03 IR 投影片也把 AI server GPU OAM/UBB 對 VLL/ULL CCL 與 14~30 層板需求列為 CCL content growth driver。

但台燿目前的問題不是 thesis，而是價格。6/26 本益比約 106.6，保守目標價下風險報酬比只有 1.30，因此報告把它列為「觀察」，不是追價買進。

### 3. AI server 機構件

推薦股：`2059 川湖`。

川湖的瓶頸不是一般機構件，而是 AI server 高價設備維修與抽換需要可靠 slide rail / rack mechanics。CommonWealth 2026-05 報導指出，高價 AI server 能否安全抽出、快速維修，取決於常被忽略的滑軌，並提到川湖累積超過 3,000 件專利，已成為 NVIDIA 與主要雲端基礎設施供應商的重要夥伴。川湖官方產品頁也把 thin server slide rail 定位在 cloud computing devices。這符合 Serenity 對「小零件、但缺了會卡住整個供應鏈」的定義。

### 4. AI PCB drilling / equipment

推薦股：`3167 大量`。

AI server PCB 從普通多層板升級到高速、高層、高精度背鑽，會讓 drilling / routing / forming equipment 的瓶頸放大。KGI 2025-08 PCB sector report 指出，AI server 導入高速、多層、先進材料，鑽孔與成型設備商受惠，其中大量被列為主要受惠者。這個 thesis 的本質是「設備瓶頸」：只要高階 PCB capacity 繼續擴張，前段製程設備需求會被放大。

## 前三名理由

### 4973 廣穎電通

排名第一的原因是估值、成長與位置最均衡。它不是上游 price setter，而是在 DRAM/NAND 景氣上行時，模組與 industrial storage 會同時受惠於庫存價值、成本轉嫁與 AI edge storage 需求。5 月營收 YoY +187.2%，3 個月 YoY 約 +130.7%，6/26 本益比約 20.4，風險報酬比 3.56。缺點是 60 日漲幅已達 +162.5%，所以不適合一次買滿。

### 2344 華邦電

華邦電是 specialty DRAM/NAND 與 edge AI memory 受惠者，5 月營收 YoY +182.0%，3 個月 YoY +151.9%。它比模組廠更接近上游供給瓶頸，因此具備較直接的週期性 ASP 受惠，但仍不是全球 DRAM/NAND 的主導 price setter，也更受記憶體景氣與毛利率波動影響。現價下保守目標價 341、停損 155，風險報酬比 2.60，仍可列為推薦，但要分批。

### 5289 宜鼎

宜鼎是 industrial / edge AI storage 的代表，5 月營收 YoY +640.8%、3 個月 YoY +569.6%，數據確認度很強。它的優點是與 edge AI、工控、嵌入式儲存需求高度相關；缺點是股價單價高、波動也大。現價 1605、保守目標 2890、停損 1250，風險報酬比 3.63。

## 交易計劃與風控原則

這份報告提供的是 pre-trade plan，不是下單計劃。若要進入交易執行 workflow，必須另外做 dry-run order plan、確認資金、庫存、零股/整股、委託時間與 broker 狀態。

執行上我不建議一次買滿 10 檔，原因是這份名單高度集中在記憶體循環。較合理的做法是：

1. 先把 `4973`、`2344`、`5289`、`2408`、`2059`、`8299`、`3167` 作為第一批核心觀察/分批標的。
2. `8271`、`2451` 可作為低本益比記憶體模組補充，但不宜讓 memory module cluster 過度集中。
3. `6274` 暫時不追價，等回檔或下一次營收/法說資料證明估值可以被消化。
4. 價格跌破停損價時要降風險；若發生 thesis stop，則即使價格沒跌破也要重新評估。

## Thesis Stop

| 股票 | Thesis stop |
|---|---|
| 4973 / 8271 / 2451 | DRAM/NAND 報價轉跌、模組庫存回補結束、月營收 YoY 快速降到低於 30% |
| 2344 / 2408 | DRAM ASP 或毛利率連續轉弱、供給擴張快於需求、法說會撤回供給受限說法 |
| 5289 | Edge AI / 工控儲存拉貨中斷，月營收或訂單能見度反轉 |
| 6274 | 玻纖布/CCL 緊缺緩解、VLL/ULL CCL 漲價失敗、高速材料需求被下修 |
| 2059 | AI server 出貨遞延、主要客戶拉貨放緩、滑軌供應鏈不再是瓶頸 |
| 8299 | NAND controller 需求轉弱、NAND 缺貨導致出貨受限而非價格受惠 |
| 3167 | 高階 PCB 擴產或鑽孔設備訂單遞延 |

## 主要來源

本地數據：

- PostgreSQL / DuckDB：`daily_quote`、`stock_per_pbr`、`daily_trading_details`、`operating_revenue`、`industry_taxonomy_pit`。
- Serenity artifacts：`research/strat_lab/results/serenity_valuation_methods_replay_2025_scored_candidates.csv`、`research/strat_lab/results/serenity_industry_first_replay_2025_picks.csv`。
- 本次計算輸出：`research/strat_lab/results/serenity_recommendation_2026-06-29.csv`。

外部來源：

- TrendForce, 2026-03-31, [AI Server Demand to Drive Memory Contract Price Increases in 2Q26](https://www.trendforce.com/presscenter/news/20260331-12995.html)。
- TrendForce, 2026-05-29, [Agentic AI Drives Structural Expansion in Memory Demand](https://www.trendforce.com/presscenter/news/20260529-13068.html)。
- Nanya Technology, 2026-04-13, [Q1 2026 Investor Conference](https://www.nanya.com/en/Activity?Action=IR_investorcalendar_FileName&Id=4285&column=Presentation)。
- Nanya Technology, 2026-04-13, [Q1 2026 results](https://www.nanya.com/en/IR/16/?IRId=12105)。
- Silicon Power, 2026, [Silicon Power Presents AI Data Innovation at COMPUTEX 2026](https://www.silicon-power.com/news-detail/Silicon-Power-Presents-AI-Data-Innovation-at-COMPUTEX-2026/)。
- Innodisk, 2026-03, [Earnings presentation](https://www.innodisk.com/upload/file/innodisk_earnings_presentation_en_0318.pdf)。
- Phison, 2026-06-02, [AI Enabler: Evolving Data Storage Intelligence](https://www.phison.com/phison-unlocks-a-full-scale-ai-deployment-across-industries-building-an-ecosystem-with-pascari-aidaptiv/)。
- Apacer, 2026, [Edge AI Storage Power at COMPUTEX 2026](https://www.apacer.com/en/news/news-and-events/content/2026-computex)。
- Transcend, 2026-06-24, [218-layer 3D NAND storage solutions for AI computing and edge applications](https://us.transcend-info.com/About/press/12531)。
- TrendForce, 2026-04-30, [Glass Fiber Cloth: The Underlying Material Shortage in AI Server Supply Chains](https://insights.trendforce.com/p/glass-fiber-cloth-shortage)。
- ITEQ, 2026-03, [IR Presentation](https://s3.ap-northeast-1.wasabisys.com/cdn.iteqcorp.com/2026/04/2026-Mar_ITEQ_IR-Presentation-ENG-V2.pdf)。
- CommonWealth Magazine, 2026-05, [How Has Taiwan Metalworker King Slide Pivoted to Leading in AI Server Rails?](https://english.cw.com.tw/article/article.action?id=4767)。
- King Slide, [Thin Server Slide Rail / Cloud Computing Devices](https://www.kingslide.com/products_cloud)。
- KGI, 2025-08, [PCB sector report](https://www.kgi.com.hk/en/-/media/files/kgishk/research-reports/tw-reports/pcb-sector_27082025.pdf)。

## 重要限制

- 最新本地可交易價格是 2026-06-26 收盤，尚未包含 2026-06-29 日行情。
- Serenity 目前結果高度集中在記憶體循環，代表 alpha 很強但產業集中風險也高。
- 目標價是研究用估值目標，不是保證會到，也不是券商正式目標價。
- 停損價是 price stop；若 thesis stop 發生，應先重新評估，不應等價格跌破才反應。
