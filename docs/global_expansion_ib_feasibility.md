---
title: 全球股票量化交易系統與 IB 擴展可行性評估
date: 2026-08-07
status: 研究結論,暫不實作
---

# 全球股票量化交易系統與 IB 擴展可行性評估

**結論**:本次評估決定暫不實作全球股票量化交易系統。Interactive Brokers (IB) 無法作為研究資料庫,其歷史資料缺乏已下市證券,會引入致命的生存者偏差;台股現役策略高度依賴的公開籌碼資料在國際市場並不存在,無法直接照搬。未來若重啟專案,IB 僅能作為下單通道與即時報價來源,底層資料必須外購(如 EODHD 或 Sharadar),並優先移植不依賴籌碼與財報的 Serenity 供應鏈瓶頸方法論。

## IB 財報資料

IB 的 TWS API 確實能透過 `reqFundamentalData` 取得財報資料,支援 5 種 `reportType`:ReportSnapshot (公司概況)、ReportsFinSummary (財務摘要)、ReportRatios (財務比率)、ReportsFinStatements (三大財務報表,年報 + 季報)、RESC (分析師預估)。該資料來自 Refinitiv (前身 Thomson Reuters),回傳格式為 XML,一次僅能查 1 檔。目前社群套件 `quantbelt/ib_fundamental` (https://github.com/quantbelt/ib_fundamental) 已將此 XML 解析成 pandas DataFrame,提供年/季的資產負債表、損益表、現金流量表、EPS、營收、股利、ROE/ROC/EV/BVPS 等比率,以及分析師預估。範例顯示 AAPL 有 2017 至 2024 的資料。

然而,`reqFundamentalData` 在官方文件中已明文標記為「Legacy/DEPRECATED」(https://interactivebrokers.github.io/tws-api/classIBApi_1_1EClient.html)。同時,IBKR Client Portal Web API 側的 fundamental 欄位(股利金額、殖利率、除息日、本益比、市值、EPS、Beta)同樣已標記 deprecated、不再透過 API 提供。IB 現在主推的替代品是 `reqWshMetaData` 與 `reqWshEventData`,資料源是 Wall Street Horizon,內容是「事件行事曆」(財報日、除息日、選擇權到期、分割、分拆、法人會議),不是財報數字,且需要先在帳戶管理啟用 Wall Street Horizon Enchilada Pro 研究訂閱(https://interactivebrokers.github.io/tws-api/fundamentals.html)。TWS 桌面軟體內建的 Fundamentals Explorer (Reuters 資料)對 IBKR 客戶免費,但那是 GUI 工具,不是可程式化的批次資料來源。

## IB 歷史資料的致命限制

IB 歷史資料的硬限制(https://interactivebrokers.github.io/tws-api/historical_limitations.html)是本專案無法將其作為資料庫的最主要原因:

1. **不提供下市股**:官方「歷史資料不提供」清單中明文包含 Delisted securities (已下市證券)。同一份清單還包含:小於等於 30 秒的 bar 超過 6 個月者、到期超過 2 年的期貨、已到期的選擇權/期貨選擇權/認股權證/結構型商品、選擇權的日收盤資料。
2. **限速 (pacing violations)**:
   - 15 秒內發出完全相同的歷史資料請求即違規。
   - 2 秒內對同一 Contract / Exchange / Tick Type 發出 6 個以上請求即違規。
   - 任何 10 分鐘窗口內超過 60 個請求即違規(BID_ASK 請求計為兩次)。
   - 同時開啟的歷史資料請求上限 50 筆。

**推算**:全球約 3 萬檔股票,以 60 requests / 10 分鐘計,一次全量約需 5,000 分鐘,約 83 小時;而且跑完仍然缺少所有已下市公司。

**為什麼「缺下市股」對這個 repo 特別致命**:
這是典型的生存者偏差 (survivorship bias)。券商普遍不提供下市股資料,因為券商的目的是「幫客戶下單」,而下市股不能交易。業界估計:排除下市股會讓年化報酬高估約 1 至 4 個百分點,並同時扭曲 Sharpe 與最大回撤。對照現狀:這個 repo 的台股系統是自己封存 raw 檔的,下市公司的歷史仍留在 `data/` 裡,整條資料鏈沒有這個偏差。改用 IB 當資料源等於主動放棄這個地基。

## 架構結論

台股系統架構:
TWSE 爬蟲 → `data/` raw → parse → `cache.duckdb` → 研究 + 富邦下單

全球系統架構(未來若實作):
資料商 bulk 下載 → `data/` raw → parse → `cache_global.duckdb` → 研究 + IB 下單 (`ib_async`)

IB 在全球版本裡的角色只有兩個:下單通道、即時報價。它不能當研究資料庫。原始檔封存鐵律照舊適用——資料商也會改資料、也會倒閉,raw 是唯一的保險。

## 替代資料源比較

| 資料商 | 價格方案 | 優點與資料範圍 | 缺點與限制 | 出處 |
| :--- | :--- | :--- | :--- | :--- |
| **EODHD** | Free $0 / 每日 20 次呼叫<br>EOD Historical Data $19.99 每月<br>EOD + Intraday Extended $29.99 每月<br>Fundamentals Data Feed $59.99 每月<br>All-in-One $99.99 每月 | 涵蓋 70 個以上交易所、15 萬個以上代號。付費方案每日 10 萬次呼叫。EOD 含 30 年以上歷史及下市資料,無生存者偏差。 | 財報只提供「最新重編後的數值」,不附申報日 (filing date)。這是回測前視偏差 (look-ahead bias) 的經典來源。 | https://eodhd.com/pricing |
| **Sharadar** | 未驗證確切數字 | 涵蓋 2 萬家以上美國公司,官方明文「No survivorship bias: includes active and delisted tickers」。美國公司財報 1990 年起,提供 as-reported (原始申報)與 restated (重編後)雙維度,是真正的 point-in-time 資料。 | 只涵蓋美國股票。 | https://www.quantrocket.com/sharadar/<br>https://www.quantrocket.com/pricing/data/sharadar/ |
| **Norgate Data** | 未驗證 | 美股與澳股,價格資料無生存者偏差。 | 不提供財報。 | 未驗證 |

## 台股到全球新增的 11 個坑

1. **生存者偏差**:台股靠自建 raw 封存解決;全球必須付費購買含下市股的資料集。
2. **Point-in-time 財報**:重編值 vs 原始申報值 + 申報日;台股系統已用 `report_date` 與 `industry_taxonomy_pit` 釘死 PIT 語義,全球要重做一次。
3. **識別碼**:股票代號會被回收再用,不能當主鍵;要用 IB 的 conId、FIGI 或 ISIN 當永久鍵。
4. **多幣別**:NAV 換算、匯兌損益、是否避險。
5. **多交易日曆**:每個交易所假日不同,台股系統的 sentinel 休市日曆機制要複製 N 份。
6. **時區**:事件時戳與各地收盤時間的口徑一致性。
7. **公司行動**:分割、分拆 (spinoff)、換股,跨 60 個以上交易所。
8. **交易成本**:各國不同,不能共用一份常數:台股賣出證交稅 0.3%;美股無證交稅但有 SEC 規費;英股印花稅 0.5%;港股印花稅 0.1%。
9. **借券可得性**:若要做空。
10. **流動性與日均成交值門檻**:小型市場的市場衝擊成本量級完全不同。
11. **稅務**:台灣居民持有美股,股利預扣 30%,會直接侵蝕股利型策略報酬。台美之間目前沒有生效的所得稅協定——H.R. 33(United States-Taiwan Expedited Double-Tax Relief Act)已於 2025 年 1 月以 423 比 1 通過眾議院,若立法完成,股利預扣率將降至 15%(持股達 10% 以上者 10%),利息與權利金降至 10%;但截至 2026 年 8 月該法案仍卡在參議院財政委員會,**尚未成為法律,故現行仍為 30%**(https://www.congress.gov/bill/119th-congress/house-bill/33)。

## 策略層的結論

台股系統的 alpha 有一部分來自三大法人買賣超、融資融券餘額、借券賣出餘額這類籌碼資料——這是台灣證交所免費、每日、個股層級公開的資料,在國際市場上極為罕見。美股沒有等價物:13F 是季頻、延遲 45 天申報、且只揭露多頭部位。因此現役 Serenity 引擎裡吃法人資料的成分(例如法人分佈出場 gate)到了美股直接沒有輸入,台股策略不能照搬。

反過來,**可以直接移植的是 Serenity 的供應鏈瓶頸方法論本身**——它原本就是針對美股發展出來的方法(追蹤超大規模資料中心業者的 AI 資本支出,沿著物料清單往上游找到買方繞不開的獨家供應節點),屬於質性策展加事件紀律,不依賴籌碼資料。repo 內的 `serenity-trading-system` skill 描述已寫成適用於「any IB-tradable market (US, EU, JP, KR, HK, TW)」。

## 若未來要做,建議的分階段路線

| 階段 | 行動與目標 | 說明與成本 |
| :--- | :--- | :--- |
| **階段 0** | 用 `ib_async` 接 paper account,打通下單、查詢庫存、成交回報三條路徑,並盤點帳號實際開通了哪些市場權限與各國佣金費率。成本 0 元。 | Python 用戶端現況:原 `ib_insync` 作者於 2024 年初過世,專案已停止維護。社群接手後改名為 `ib_async`,是現行建議使用的套件 (https://github.com/ib-api-reloaded/ib_async)。 |
| **階段 1** | 訂閱 EODHD All-in-One (每月 99.99 美元),照台股同一形態建立 `data/` 原始封存 → `cache_global.duckdb`,先只做價格層(含下市股)。 | IB 行情訂閱成本:美股 Bundle 每月 10 美元 (達月佣金門檻可免);香港 Level 1 對符合條件的客戶免費。行情訂閱按自然月計費 (https://www.interactivebrokers.com/en/pricing/market-data-pricing.php)。 |
| **階段 2** | 先在美股跑 Serenity 方法論。 | 因為它不依賴 PIT 財報,是最快能上線的路徑。 |
| **階段 3** | 若要做純量化截面因子,才需要補 Sharadar (美股真 PIT);非美國市場先接受重編後資料,但必須在程式碼註解明確標註「這不是 PIT」。 | 需外加 Sharadar 資料費(未驗證)。 |

## 出處清單

- IB `reqFundamentalData` DEPRECATED 標記:https://interactivebrokers.github.io/tws-api/classIBApi_1_1EClient.html
- IB `quantbelt/ib_fundamental` 社群套件:https://github.com/quantbelt/ib_fundamental
- IB Wall Street Horizon 與替代方案說明:https://interactivebrokers.github.io/tws-api/fundamentals.html
- IB 歷史資料硬限制 (含下市股與限速):https://interactivebrokers.github.io/tws-api/historical_limitations.html
- Python 用戶端 `ib_async`:https://github.com/ib-api-reloaded/ib_async
- EODHD 價格與方案:https://eodhd.com/pricing
- Sharadar 資料範圍與定價:https://www.quantrocket.com/sharadar/、https://www.quantrocket.com/pricing/data/sharadar/
- IB 行情訂閱定價:https://www.interactivebrokers.com/en/pricing/market-data-pricing.php
- H.R. 33 台美租稅法案立法進度:https://www.congress.gov/bill/119th-congress/house-bill/33

---

*本文為 2026-08-07 查證快照。IB API 的 deprecation 狀態、資料商定價、H.R. 33 立法進度都會變動;重啟評估時請重新查證,勿直接引用本文數字。*
