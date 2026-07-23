"""S 策略雲端 live 營運:決策膠水層 + 通知 + 盤前/開盤編排(headless)。

雲端 GCP VM 專用的精簡自動化,與本機互動式 `quantlib.tri.daily` 分家:
- `s_plan`   — 純函式,把現成 `s_advisor` 的建議萃取成今日可執行下單清單。
- `notify`   — Gmail 通知(SMTP 送信 + IMAP 取消檢查),Notifier port。
- `premarket`— 07:20:更新資料 → 建計劃 → 寄信(含取消鈕)→ 落盤。
- `execute`  — 08:55:載入計劃 → 檢查取消 → 派工現成 `execution.trade`。

一律重用既有引擎(`s_advisor` / `execution.trade` / `FubonBroker`),不重寫策略或執行。
"""
