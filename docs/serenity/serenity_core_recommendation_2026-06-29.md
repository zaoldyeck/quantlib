# Serenity Core 結構性瓶頸股推薦報告

產出日期：2026-06-29  
本地資料截止：`daily_quote=2026-06-26`、`stock_per_pbr=2026-06-26`、`daily_trading_details=2026-06-26`、`operating_revenue=2026-05`  
候選訊號日：`2026-06-11`  
現價基準：`2026-06-26` 收盤價。

## 修正結論

`memory_cycle` 不應列入 Serenity Core。它可以是有交易價值的 tactical cycle overlay，但它的主要驅動是 DRAM/NAND 供需週期、庫存重估、成本轉嫁與景氣 beta，不是 Serenity 原始定義中的持久結構性瓶頸。

嚴格 Serenity Core 應該優先看：AI server CCL、AI server 機構件、AI PCB 設備、AI ASIC 設計服務、工業測試/電源測試、CPO/800G 光通訊等。這些比較符合「缺了會卡住供應鏈」、「資格/產能/技術門檻高」、「需求不是單一季度補庫存」的 Serenity 條件。

## Serenity Core 10 檔排名

| 排名 | 股票 | 主題 | 狀態 | 現價 | 目標價 | 停損價 | 風險報酬比 | 判斷 |
|---:|---|---|---|---:|---:|---:|---:|---|
| 1 | 6274 台燿 | AI server CCL | 觀察 | 1580.0 | 2054.0 | 1215.6 | 1.30 | Thesis 強，但估值與風險報酬比不足，不追價 |
| 2 | 2059 川湖 | AI server 機構件 / slide rail | 推薦 | 6990.0 | 11184.0 | 5242.5 | 2.40 | 高價但符合小零件卡供應鏈邏輯 |
| 3 | 3167 大量 | AI PCB drilling equipment | 推薦 | 765.0 | 1109.2 | 610.1 | 2.22 | AI PCB 設備瓶頸，風險報酬比仍可接受 |
| 4 | 2383 台光電 | AI server CCL | 觀察 | 5255.0 | 6831.5 | 4399.4 | 1.84 | 品質強，但估值壓力高，適合觀察不追價 |
| 5 | 3443 創意 | AI ASIC design service | 觀察 | 4420.0 | 5967.0 | 3493.1 | 1.67 | ASIC 設計服務稀缺，但估值偏滿 |
| 6 | 2360 致茂 | AI power / industrial testing | 推薦 | 2035.0 | 3154.2 | 1637.5 | 2.82 | 測試設備瓶頸，風險報酬比最佳之一 |
| 7 | 8021 尖點 | AI PCB drilling | 觀察 | 535.0 | 653.2 | 401.8 | 0.89 | Thesis 可看，但現價風險報酬比不足 |
| 8 | 6442 光聖 | 800G / CPO optical interconnect | 推薦 | 1680.0 | 2604.0 | 1283.8 | 2.33 | 光通訊瓶頸仍符合 Serenity |
| 9 | 2382 廣達 | AI server OEM integration | 分批 | 362.0 | 452.5 | 308.3 | 1.68 | 規模與整合能力受惠，但瓶頸純度較低 |
| 10 | 3081 聯亞 | 800G / CPO optical interconnect | 觀察 | 2035.0 | 2950.8 | 1538.8 | 1.85 | 光通訊方向正確，但估值與波動需控管 |

## 記憶體股的正確定位

`4973`、`2344`、`5289`、`2408`、`8299`、`8271`、`2451` 可以有交易價值，但它們不應被放在 Serenity Core 裡。它們比較像 tactical cycle overlay：

- 上游製造商如 `2408`、`2344`：有週期性 ASP 受惠，但這是記憶體供需週期，不是長期穩定瓶頸。
- 控制器/模組/品牌廠如 `8299`、`5289`、`8271`、`2451`、`4973`：多數靠庫存重估、成本轉嫁、產品 mix、通路需求與景氣彈性，不是供應鏈不可替代瓶頸。

如果要交易這批股票，應該另開 `Memory Cycle Overlay` 報告，使用不同的評估框架：報價週期、庫存水位、毛利率彈性、ASP、供給 discipline、庫存跌價風險，而不是 Serenity 結構性瓶頸框架。

## 主要修正

- 剔除 `memory_cycle` 作為 Serenity Core 預設推薦。
- 不再把記憶體模組/品牌/控制器股描述成 Serenity 結構性瓶頸。
- 保留它們作為 tactical overlay 的可能性，但必須獨立評估。
- Serenity Core 排名允許出現多檔 `觀察`，不為了湊滿 10 檔而強行推薦。

計算檔：`var/out/strat_lab/serenity_core_recommendation_2026-06-29.csv`。
