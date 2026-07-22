# 消息面 Alpha 研究摘要

資料截止：價格資料使用 total-return adjusted close 至 `2026-06-16`。本摘要整合目前已完成的官方事件、MOPS 重大訊息、GDELT 新聞 metadata 與 RSS 近期新聞 pilot。

## 結論

不是所有消息都有 Alpha。泛用地買進「有重大訊息」或「有新聞」反而很差；目前真正值得繼續擴大驗證的是：

1. **庫藏股支撐型事件**：尤其是股價先下跌後，公司公告買回、且買回價格上限明顯高於當時股價。
2. **半導體/AI/政策衝擊新聞**：GDELT 新聞 metadata pilot 顯示，半導體 AI、地緣政治/關稅、資本支出/擴產等類型有正向超額報酬，但目前仍是月度抽樣，不是完整新聞庫，只能視為候選 Alpha。
3. **月營收成長加速**：廣度最大、訊號較穩，但單獨強度不如庫藏股或特定新聞催化。

目前不值得直接當買進訊號的是：

- 廣義 MOPS 重大訊息。
- 例行治理、董事會、股東會、代子公司公告。
- 單純股利、配息公告。
- 一般正面語氣新聞；GDELT pilot 裡 `gdelt_positive_tone` 沒有產生正超額。
- RSS 近期新聞本身；RSS 已可抓取與分類，但目前樣本太新且 forward return 太少，不足以作為 Alpha 證據。

## 這輪完成的最小 Pipeline

- 新增 `research/records/tw_stock_news_aliases.csv` 作為台股新聞別名與消歧義表。
- 新增 `research/experiments/news_alpha_common.py`，讓 GDELT 與 RSS 共用 alias matching、分類、格式化與 summary 邏輯。
- 重構 `research/experiments/gdelt_tw_news_alpha_survey.py`，改用共享 alias 表與分類規則。
- 新增 `research/experiments/tw_news_rss_alpha_survey.py`，支援 Yahoo 個股 RSS 與 Google News RSS 的近期 metadata ingestion。
- 保留 GDELT historical sample 與 RSS recent sample 兩條不同語意的資料源，不混成同一個歷史回測結論。

## 已驗證資料源

| 資料源 | 覆蓋 | 可回測性 | 目前判斷 |
|---|---:|---|---|
| MOPS 庫藏股 `t35sc09` | 2012-2026 | 高 | 已出現明確事件 Alpha |
| MOPS 結構化事件：內部人、減資、月營收 | 2012-2026 | 高 | 月營收加速有效；減資噪音大；內部人資料需補歷史 |
| MOPS 重大訊息 `t05st02` | 2025 pilot | 高 | 廣義重大訊息不是買進 Alpha，多數類型相對 0050 落後 |
| GDELT 2.1 GKG raw metadata | 2025 月度抽樣 pilot | 中 | 半導體/AI、政策/關稅、負面 tone、market report 值得擴大 |
| Yahoo 個股 RSS + Google News RSS | 即時/近期 | 中 | 已可抓、可分類；目前不是歷史 Alpha 證據 |

## 目前 Alpha 排序

| 排名 | 類型 | 來源 | 樣本 | 60日結果 | 判斷 |
|---:|---|---|---:|---|---|
| 1 | `buyback_support_after_drop` | MOPS 庫藏股 | 60日有效 387 | 60日均值 `+13.41%`，勝率 `73.39%` | 目前最乾淨的官方事件 Alpha |
| 2 | `buyback_high_price_ceiling` | MOPS 庫藏股 | 60日有效 2,273 | 60日均值 `+8.51%`，勝率 `63.26%` | 可作為事件因子 |
| 3 | `gdelt_negative_tone` | GDELT 新聞 metadata | articles 20 | 60日相對 0050 `+11.93%`，t-stat `3.12` | 候選；可能代表「壞消息出盡」或政策壓力後反彈 |
| 4 | `gdelt_policy_geopolitics_tariff` | GDELT 新聞 metadata | articles 26 | 60日相對 0050 `+7.82%`，t-stat `4.86` | 候選；可能集中在半導體政策新聞 |
| 5 | `gdelt_market_stock_report` | GDELT 新聞 metadata | articles 60 | 60日相對 0050 `+4.32%`，t-stat `2.26` | 候選；可能捕捉市場關注度與 analyst/news momentum |
| 6 | `gdelt_customer_nvidia_apple` | GDELT 新聞 metadata | articles 39 | 60日相對 0050 `+4.03%`，t-stat `2.77` | 候選；與 AI 供應鏈客戶題材有關 |
| 7 | `gdelt_semiconductor_ai` | GDELT 新聞 metadata | articles 59 | 60日相對 0050 `+3.66%`，t-stat `2.06` | 候選；需擴大 universe 驗證 |
| 8 | `revenue_yoy30_accel` | 月營收 | 60日有效 34,126 | 60日均值 `+6.05%`，勝率 `52.64%` | 廣度高但單獨訊號弱 |

## RSS 近期新聞 Pilot

RSS 最小樣本使用 `2330, 2317, 2454`，每個 Yahoo/Google feed 最多 5 筆，產出 `19` 篇 unique articles、`60` 個 event label rows。這層驗證了 live/recent-news ingestion 可以運作，但不構成歷史 Alpha 證據：

- `rss_all_company_news`：60日有效樣本只有 `2`，60日相對 0050 `-23.53%`。
- `rss_earnings_revenue`：60日有效樣本只有 `1`，60日相對 0050 `-31.06%`。
- 多數文章事件日在 `2026-06-16` 或 `2026-06-17`，相對價格 cutoff `2026-06-16` 太新，forward return 自然不足。

因此 RSS 目前的定位是 **live feature / future monitoring input**，不是回測主證據。

## 重要反例

MOPS 重大訊息 pilot 使用 2025 年樣本，共 `17,521` 個 unique disclosure；結果顯示廣義重大訊息不是正向買進訊號：

- `material_all`：60日相對 0050 `-6.57%`，t-stat `-50.43`。
- `material_routine_governance`：60日相對 0050 `-6.74%`。
- `material_dividend_distribution`：60日相對 0050 `-7.46%`。
- `material_trading_status_risk`：60日相對 0050 `-10.75%`。

這說明消息面研究不能用「有公告就買」；必須把公告內容分類，排除例行、治理、風險與市場已知資訊。

## 後續建議

1. 把 GDELT raw GKG 從月度抽樣擴大到完整 2024-2026 bounded hourly/all-interval 樣本，確認候選新聞類型不是抽樣偏差。
2. 將 `gdelt_negative_tone`、`gdelt_policy_geopolitics_tariff`、`gdelt_market_stock_report`、`gdelt_customer_nvidia_apple` 與既有因子交互：月營收加速、價格動能、法人買超、產業輪動、庫藏股支撐。
3. 若交互後仍有 OOS Alpha，再進入策略層：事件後 N 日內只買同時通過流動性、趨勢、籌碼與風險條件的股票。
4. RSS 用於日後 live monitoring：每天收盤後抓取近期新聞，只產生候選標籤，不直接交易。
