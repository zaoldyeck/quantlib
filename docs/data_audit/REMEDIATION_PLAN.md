# 資料修復總計劃(2026-07-23 起;目標:所有資料正確,歷史有誤則重抓)

使用者定調:**全部都要修,不放過任一細節;歷史資料有誤要重新下載最正確的。**

110 個確認 BUG 不逐一亂修——先歸成**缺陷類**(同機制、多處復發),逐類根因修 +
全類掃描 + 立防復發守護(對照全域天條 §2.1)。每類獨立 commit,額度中斷可續。

## 修復順序(money-path 優先)

| 類 | 名稱 | 影響 | 手段 | 狀態 |
|---|---|---|---|---|
| **FC1** | 除權/減資還原 | 🔴 live S exit_replay + 全回測 NAV | 純 Python(prices.py/cache_tables.py)+ 重解析 ex_right_dividend | ⬜ |
| **FC8** | 產業別 PIT | 🔴 live S accel_rel | 重建 industry_taxonomy_pit(加 t21sc04、真 PIT)| ⬜ |
| **FC2** | 別天的資料/幽靈日 | 全回測 | 內容指紋偵測 → 刪汙染列(PG+cache)→ Scala 重抓 | ⬜ |
| **FC3** | 真交易日缺資料 + 假 sentinel | 全回測 fail-close | 刪假 sentinel → 重抓 → 守護 | ⬜ |
| **FC4** | F-Score/財務 view 算式 | legacy 策略 | 修 raw_quarterly.py(唯一真源)+ deprecate PG view | ⬜ |
| **FC5** | 累計制差分缺季 | 基本面因子 | raw_quarterly.py 日曆對齊 | ⬜ |
| **FC6** | 季報凍結/存活者偏誤 | PIT 正確性 | 重抓舊季不刪下市公司 | ⬜ |
| **FC7** | int32 溢位 + 欄位錯位 | dtd/margin | Slick Int→Long + Reader case 分派 + 重解析 | ⬜ |
| **FC9** | 防復發守護 | 全部 | sentinel/指紋/欄數/parity 測試 | ⬜ |

## 通用原則
- **每個修法動手前自己重現證據**(不照單全收 agent 結論;global 天條:只認可重現證據)。
- **重抓 = Scala 爬蟲**(`Main pull <target> --date ...`);抓前先刪汙染列(PG + cache 兩邊),
  否則 `dataAlreadyInDB` 會跳過。
- **money-path 修法必附 before/after 逐位 diff + 測試**(先紅後綠)。
- **負結果照記**(查了沒問題的也在 findings/ 留 OK)。

## 進度日誌
(每完成一類追加一行,附 commit)
