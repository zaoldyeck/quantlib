# QuantLib 文件索引

最後更新：2026-08-07

## 策略研究

| 文件 | 用途 |
|---|---|
| [`strategy_research/research_sop.md`](strategy_research/research_sop.md) | 台股量化策略研發 SOP。新策略、策略改版、validation、升級 stage 都依此執行。 |
| [`strategy_ranking.md`](strategy_ranking.md) | 目前策略 production 狀態、已驗證策略與交易規則說明。 |
| [`strategy_research/s_conditional_probability_campaign.md`](strategy_research/s_conditional_probability_campaign.md) | 條件機率地圖改造 S(2026-08-07):資金分配軸與 σ 標準化停損全數否決;關鍵翻轉——對右尾策略該量 P(大贏) 而非 E÷σ;S 的移動停損只決定 0.6% 出場。 |
| [`strategy_research/meanrev_post_verdict.md`](strategy_research/meanrev_post_verdict.md) | 台股短期均值回歸查證(2026-08-07):訊號真實(秩相關 −0.038)但毛邊際僅 8 bp、成本門檻 35.7 bp;留下「日頻訊號可交易門檻」這條可複用的線。 |

## 資料與方法

| 文件 | 用途 |
|---|---|
| [`data/industry_taxonomy.md`](data/industry_taxonomy.md) | 正式產業分類資料層、PIT join、normalization、驗證命令。 |
| [`active_etf_analysis.md`](active_etf_analysis.md) | 主動式 ETF 量化分析與排名依據。 |
| [`active_etf_investor_recommendation.md`](active_etf_investor_recommendation.md) | 給一般投資人的主動式 ETF 推薦報告。 |
| [`leaders_by_domain.md`](leaders_by_domain.md) | 台股各領域龍頭股 watchlist。 |
| [`global_expansion_ib_feasibility.md`](global_expansion_ib_feasibility.md) | 全球股票量化系統與 IB 擴展可行性評估(2026-08-07 查證,結論:暫不實作;IB 只能當執行層,歷史資料不含下市股)。 |

## 操作文件

| 文件 | 用途 |
|---|---|
| [`../AGENTS.md`](../AGENTS.md) | Codex / agent 在本 repo 的操作規範與不可違反原則。 |
| [`../src/quantlib/README.md`](../src/quantlib/README.md) | Python research 目錄結構與常用命令。 |
| [`../src/quantlib/trading/README.md`](../src/quantlib/trading/README.md) | 自動交易與 broker integration 說明。 |
