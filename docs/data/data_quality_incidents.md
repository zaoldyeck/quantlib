# 資料品質事件紀錄

本文件只記錄已確認、已修正、可重現驗證的資料品質事件。目的不是取代
audit 腳本，而是避免同一個資料污染案例在後續研究中被重複診斷。

## 2026-05-30 週六被誤判為交易日

- 發現情境：`Main update` 看到 `2026-05-30 SBL borrowing` 回 0 筆後輸出
  `[giveup] will retry next run`。
- 根因：`data/daily_quote/twse/2026/2026_5_30.csv` 是錯誤殘留檔，檔名日期為
  `2026-05-30`，但檔案內容標示 `115年05月20日`。舊版
  `Task.loadLocalTwseDailyQuoteTradingDays()` 只用檔案大小判斷交易日，沒有檢查
  TWSE daily quote header 日期是否等於檔名日期，因此把週六錯誤加入交易日集合。
- 修正：
  - `daily_quote` TWSE / TPEx 下載驗證改為檢查 header 日期與 requested date。
  - `Detail.validate` 改成 `DownloadValidation.Valid | NoData | Invalid`，區分非交易日
    no-data 與真正 schema/內容錯誤。
  - SBL 週末 no-data 不再標成下次重試；真正 invalid 檔仍刪除並重試。
  - `Task.loadLocalTwseDailyQuoteTradingDays()` 只接受 header 日期與檔名一致的 TWSE
    daily quote 檔。
- 資料清理：
  - 刪除 PostgreSQL `daily_quote` 中 `2026-05-30` 的 TWSE 錯誤 rows。
  - 將 `data/daily_quote/twse/2026/2026_5_30.csv` 與
    `data/daily_quote/tpex/2026/2026_5_30.csv` 截斷為 0-byte no-data sentinel。
- 驗證：
  - `sbt Compile/compile` 成功。
  - `sbt "runMain Main pull sbl --since 2026-05-30"` 成功，且未再拉取或重試
    `2026-05-30`。
  - `sbt "runMain Main update"` 成功，未再出現 `2026-05-30 SBL borrowing` giveup。
  - PostgreSQL 驗證：
    - `daily_quote where date='2026-05-30'` = 0 rows。
    - `sbl_borrowing where date='2026-05-30'` = 0 rows。
