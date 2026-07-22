# Serenity 美股推薦報告（美國掛牌股票與 ADR）

產出日期：2026-07-03  
適用框架：Serenity Core 結構性瓶頸選股法  
股票宇宙：美國掛牌股票與美國 ADR。若只看美國本土公司，會漏掉 ASML、TSM 這類全球供應鏈真正瓶頸，因此本報告保留 ADR，但在表格中標示清楚。

## 結論

如果把 Serenity Core 套用到美股，我目前會選出的 10 檔是：NVDA、TSM、ASML、CDNS、AVGO、ANET、ETN、VRT、KLAC、GEV。  

其中「現在最接近可買」的是 NVDA；TSM、ASML 屬於最強結構性瓶頸，但因為股價與波動風險，較適合分批。CDNS、AVGO、ANET 是 AI 半導體供應鏈的軟體、客製化晶片與網路瓶頸；ETN、VRT、GEV 是 AI 資料中心往電力與散熱瓶頸外溢的受益者。KLAC 與 GEV 的結構性題材很強，但短線延伸度偏高，不應追價。

這不是下單指令，也不是保證獲利。這份報告是研究級交易計劃：給出現價、估值目標價、停損價、進場方式與需要追蹤的失效條件。

## 資料截止

- 美股價格：使用 Yahoo Finance / `yfinance` 取得 2026-07-02 美股 regular session close。2026-07-03 查詢時沒有更晚的 regular close。
- 技術停損：使用近 6 個月日線、20 日 ATR 與近 20 日低點估算。
- 本專案 PostgreSQL / DuckDB：本次排名沒有使用台股 PostgreSQL 或 `var/cache/cache.duckdb`，因為專案目前沒有美股基本面與價格 source-of-truth 資料表；因此不執行台股資料刷新。
- 網路來源：優先使用官方公司頁面、投資人關係資料與公司新聞稿，查詢日為 2026-07-03。

## 10 檔推薦排名

| 排名 | 股票 | 公司 | 類型 | Serenity 主題 | 推薦狀態 | 現價 | 目標價 | 停損價 | 風險報酬比 | 進場計劃 | 估值判斷 | 成長/動能確認 |
|---:|---|---|---|---|---|---:|---:|---:|---:|---|---|---|
| 1 | NVDA | NVIDIA | 美國公司 | AI 加速器平台、CUDA、生態系 | 推薦 | 194.83 | 237.69 | 175.35 | 2.20 | 可分 2-3 批，跌破停損不攤平 | 極高品質但估值仍高，只能用中等倉位 | 近 60 日 +9.4%，近 20 日 -9.3%，有回檔後再切入的條件 |
| 2 | TSM | 台積電 ADR | ADR | 先進製程、CoWoS、AI/HPC foundry | 分批 | 434.16 | 542.70 | 378.40 | 1.95 | 分批，不追漲；若跌破 378.40 先退出 | 結構性最強之一，估值比多數 AI 股合理 | 近 60 日 +25.7%，近 20 日 -0.6%，趨勢仍在 |
| 3 | ASML | ASML Holding ADR | ADR | EUV / High-NA 微影設備 | 分批 | 1769.32 | 2211.65 | 1502.26 | 1.66 | 僅適合分批或等回檔，不能重倉追價 | 壟斷性高但估值與地緣風險高 | 近 60 日 +35.4%，近 20 日 +2.5%，偏強但波動大 |
| 4 | CDNS | Cadence Design Systems | 美國公司 | EDA、AI 晶片設計自動化 | 分批 | 373.14 | 440.31 | 332.48 | 1.65 | 小倉分批；若失守 332.48 降低曝險 | 軟體型瓶頸，估值高但營收能見度佳 | 近 60 日 +33.5%，近 20 日 -8.5%，拉回中 |
| 5 | AVGO | Broadcom | 美國公司 | 客製化 AI ASIC、AI networking | 觀察 | 360.45 | 432.54 | 307.80 | 1.37 | 等止跌或重新站回短中期均線 | AI 成長非常強，但近期價格結構偏弱 | 近 60 日 +7.9%，近 20 日 -24.8%，需確認不是趨勢反轉 |
| 6 | ANET | Arista Networks | 美國公司 | AI 資料中心 Ethernet 網路 | 觀察 | 159.99 | 195.19 | 136.25 | 1.48 | 等突破或回測不破支撐後分批 | 成長佳但估值與供應限制需折價 | 近 60 日 +19.7%，近 20 日 -8.3%，題材強但短線整理 |
| 7 | ETN | Eaton | 美國公司 | 資料中心電力基礎建設 | 分批 | 398.52 | 470.25 | 352.85 | 1.57 | 防守型分批，適合作為 AI 電力瓶頸曝險 | 比純 AI 半導體估值溫和，但 upside 也較低 | 近 60 日 +8.0%，近 20 日 -5.4%，偏穩 |
| 8 | VRT | Vertiv | 美國公司 | AI 資料中心電力、UPS、液冷/散熱 | 觀察 | 300.53 | 375.66 | 244.30 | 1.34 | 不追價，等回檔到更合理風險報酬 | 題材極佳，但估值與波動偏高 | 近 60 日 +14.6%，近 20 日 -9.3%，高波動 |
| 9 | KLAC | KLA | 美國公司 | 半導體製程控制、檢測、量測 | 避免追高 | 235.55 | 287.37 | 179.64 | 0.93 | 只列觀察，等回檔重新評估 | 瓶頸品質高，但短線風險報酬不足 | 近 60 日 +52.1%，近 20 日 +10.8%，已延伸 |
| 10 | GEV | GE Vernova | 美國公司 | 電網、發電設備、資料中心電力需求 | 避免追高 | 1113.11 | 1335.73 | 834.83 | 0.80 | 不追價，等大幅回檔或基本面再上修 | 主題很強，但價格已大幅反映預期 | 近 60 日 +22.2%，近 20 日 +16.0%，短線太熱 |

## 結構性瓶頸證據

| 股票 | 為什麼是瓶頸 | 主要證據 | 失效條件 |
|---|---|---|---|
| NVDA | AI 訓練與推論的加速運算平台，優勢不只在晶片，也在 CUDA、網路、系統與軟體生態 | NVIDIA FY2026 Q4 新聞稿顯示 Data Center 單季營收 623 億美元，年增 75%，全年 Data Center 營收 1937 億美元，年增 68%：[NVIDIA FY2026 Q4 results](https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-fourth-quarter-and-fiscal-2026) | 大客戶自研 ASIC 大幅替代、毛利明顯壓縮、供應鏈瓶頸導致交付不如預期 |
| TSM | 全球先進製程與 CoWoS 高階封裝核心供應商，AI GPU/ASIC 都繞不開先進製程與 advanced packaging | TSMC CoWoS 官方資料說明 CoWoS-S 用於 AI、超級運算與 HBM 整合：[TSMC CoWoS](https://3dfabric.tsmc.com/english/dedicatedFoundry/technology/cowos.htm) | CoWoS 供需反轉、先進製程價格權力下降、地緣政治風險升高 |
| ASML | EUV / High-NA 是先進節點延續摩爾定律的核心設備，供應鏈替代性極低 | ASML 官方 EUV 頁面說明 EXE 平台支援 2nm 與 sub-2nm 量產，NXE 用於 7/5/3nm 複雜層：[ASML EUV systems](https://www.asml.com/en/products/euv-lithography-systems)；2025 年報揭露 EUV 系統出貨 48 台、總營收 327 億歐元：[ASML 2025 Annual Report](https://www.asml.com/en/investors/annual-report/2025) | 中國出口限制擴大、客戶 capex 延後、High-NA 導入速度低於預期 |
| CDNS | EDA 是晶片設計前端瓶頸，AI ASIC 越多、先進封裝越複雜，設計驗證與 IP 需求越高 | Cadence 2026 Q1 財報表示 2026 營收展望上修至年增 17%，Core EDA 年增 18%，AI-driven solutions 被列為成長來源：[Cadence Q1 2026 results](https://investor.cadence.com/news/news-details/2026/Cadence-Reports-First-Quarter-2026-Financial-Results/default.aspx) | 客戶設計案延後、EDA 價格權力下降、AI 工具未能轉化為付費增長 |
| AVGO | Hyperscaler 自研 AI ASIC 與 AI networking 的關鍵供應商，受益於「GPU + custom ASIC」雙軌化 | Broadcom Q1 FY2026 新聞稿表示 AI revenue 84 億美元、年增 106%，由 custom AI accelerators 與 AI networking 需求帶動：[Broadcom Q1 FY2026](https://investors.broadcom.com/news-releases/news-release-details/broadcom-inc-announces-first-quarter-fiscal-year-2026-financial) | ASIC 客戶集中、專案延後、AI networking 競爭加劇、非 AI 業務拖累 |
| ANET | AI datacenter 需要低延遲、高頻寬 Ethernet fabric，Arista 是 hyperscaler 網路設備核心供應商之一 | Arista 2026 Q1 財報營收 27.09 億美元、年增 35.1%，並推出 XPO MSA 以降低 networking racks 與節省 floor space：[Arista Q1 2026 results](https://investors.arista.com/Communications/Press-Releases-and-Events/Press-Release-Detail/2026/Arista-Networks-Inc--Reports-First-Quarter-2026-Financial-Results/default.aspx) | 大客戶 capex 轉弱、供應限制壓毛利、Ethernet AI fabric 滲透不如預期 |
| ETN | AI data center 從算力瓶頸外溢到電力、配電、模組化建設，Eaton 是電力設備與系統供應商 | Eaton 官方資料中心頁面指出 AI 正加速更快、更強資料中心需求，且傳統建設難以跟上：[Eaton Data Centers](https://www.eaton.com/us/en-us/markets/data-centers.html) | 資料中心建設延後、電力設備供給增加導致價格壓力、工業景氣下行 |
| VRT | 高密度 GPU 機櫃需要 UPS、電力管理、液冷與熱管理；Vertiv 是資料中心 power/cooling 專家 | Vertiv 2026 Investor Conference 主題包含 AI era converged infrastructure、AI performance services、next-gen AI end-to-end systems：[Vertiv 2026 Investor Conference](https://investors.vertiv.com/events-presentations/2026-investor-conference/default.aspx) | 大型雲端客戶自建能力提高、競爭壓縮毛利、液冷滲透不如預期 |
| KLAC | 先進製程良率提升與缺陷控制難度增加，讓 inspection / metrology / process control 變得更重要 | KLA 官方說明其 process-control 與 process-enabling solutions 用於加速電子產業創新並提升 leading-edge performance：[KLA products](https://www.kla.com/) | 半導體 capex 反轉、客戶延後先進節點、檢測強度提升不如預期 |
| GEV | AI data center 拉高電力需求，燃氣發電、電網、變壓與電氣化設備成為新瓶頸 | GE Vernova 2026 Q1 財報顯示訂單 183 億美元、年增 71%，backlog 季增 130 億美元：[GE Vernova Q1 2026 results](https://www.gevernova.com/news/press-releases/ge-vernova-reports-first-quarter-2026-financial) | 電力設備需求被過度預期、專案延遲、估值壓縮、政策與工程交付風險 |

## 交易計劃

| 股票 | 交易計劃 | 價格停損 | 論述停損 |
|---|---|---:|---|
| NVDA | 可分批建立核心 AI accelerator 曝險，單筆不重壓 | 175.35 | Data Center 成長或毛利失速，或 hyperscaler 明確轉向低毛利替代方案 |
| TSM | 只適合分批，避免一次滿倉 | 378.40 | CoWoS/advanced node 供需轉鬆、價格權力下降、地緣風險明顯升高 |
| ASML | 分批或等回檔；若高波動不能承受，寧可觀察 | 1502.26 | 客戶 capex 延後、EUV/High-NA 訂單不如預期、出口限制擴大 |
| CDNS | 小倉分批，適合作為 AI 設計軟體瓶頸曝險 | 332.48 | AI EDA adoption 未能轉收入、設計案延後、續約價格壓力 |
| AVGO | 先觀察止跌；若重新轉強才分批 | 307.80 | Custom ASIC pipeline 降溫、AI networking 成長放慢、大客戶集中風險惡化 |
| ANET | 等突破或回測支撐不破再買 | 136.25 | AI Ethernet fabric 需求不如預期、供應限制壓毛利 |
| ETN | 防守型分批，作為 AI 電力瓶頸搭配 | 352.85 | 資料中心建設週期延後、電氣設備 pricing power 下滑 |
| VRT | 等更好的價格，不追高 | 244.30 | 液冷與高密度電源需求放緩、競爭造成毛利壓縮 |
| KLAC | 只觀察，等大回檔或財報再確認 | 179.64 | 半導體 capex 週期轉弱、檢測設備訂單下滑 |
| GEV | 不追高；若回檔後 backlog 仍上修再評估 | 834.83 | 電力設備訂單高峰過後、交付或政策風險擴大 |

## 前三名理由

1. NVDA：它不是單一晶片股，而是 AI accelerator、networking、software stack 與開發者生態系的組合。Serenity 看的是瓶頸與定價權，NVDA 的平台型優勢仍然最明顯。缺點是估值高、擁擠度高，所以只能用分批與停損控制。
2. TSM：如果 AI 晶片需求持續，無論是 GPU 還是 custom ASIC，都仍需要先進製程與先進封裝。TSM 的瓶頸屬性比多數「AI 概念股」更真實。缺點是 ADR 仍承擔台灣地緣風險，且近 60 日已上漲不少。
3. ASML：EUV / High-NA 是先進節點最硬的設備瓶頸之一。ASML 的替代性極低，但股價、估值與出口限制風險都不低，因此排名高但不代表可以重倉追價。

## 主要風險

- AI capex 若從「產能不夠」轉為「投資報酬率不足」，整條鏈都會被重新估值。
- 這份報告沒有使用本專案的美股歷史基本面資料庫，因此估值不是完整 point-in-time DCF 回測結果，而是 Serenity 框架的 source-backed 研究版。
- ADR 不是美國本土公司，TSM 與 ASML 仍有地緣政治、出口管制、匯率與跨市場交易風險。
- 電力與資料中心基礎建設股已經被市場高度關注，GEV、VRT、KLAC 這類強勢股若追高，風險報酬比會惡化。
- 純記憶體週期股如 MU、部分 AI compute 租賃股如 CRWV/NBIS，可能短期漲幅很大，但不符合 Serenity Core 的「持久結構性瓶頸」預設，未列入本次核心 10 檔。

## 下一步驗證

1. 建立美股 point-in-time 資料層：日線、財報、營收/segment、forward estimates、拆股與股利調整。
2. 對這 10 檔做 2020-2026 的 Serenity replay：只允許當時可得的消息與財報資料，避免未來函數。
3. 將 DCF + PEG overlay 正式量化：用 free cash flow、revenue growth、margin、share count、WACC 與 terminal growth 建立 reverse DCF 與合理價區間。
4. 針對 ADR 與美股本土公司分開評估風險：ADR 需要額外的地緣與匯率風險折價。
5. 若要實際下單，必須切到交易執行 workflow，先產生 dry-run order plan，不應由 Serenity 推薦報告直接下單。
