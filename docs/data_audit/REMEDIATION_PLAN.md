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

## 進度日誌

- **2026-07-23 FC1 除權/減資還原** ✅ 已上線(43be617, 927f0d2)
  - 配股改用交易所參考價還原(2,304 筆純配股不再幽靈崩跌);大減資護欄放寬(16 筆)。
  - cache_tables.py + db.py 同步參考價欄;prices.py 對兩種 cache 世代皆正確。
  - 待:cache 的 ex_right_dividend 表 rebuild(稽核釋放鎖後);69 筆全零列重解析。
- **2026-07-23 FC4 F-Score → Piotroski (2000) 學理精確** ✅ 已上線(fab40dd)
  - 年初資產分母 / NULL 傳播 + n_valid / 日曆對齊 / 科目優先序 / f7 去魔術數字。
  - 金鑰 2330 FY2024 = 8/9 逐項符合手算;守護 5 項。
  - 待:raw_quarterly.parquet 重生、is/bs/cf cache rebuild、S 重跑 walk-forward 對照。
- **2026-07-23 FC2/FC3/FC9 日期完整性偵測器** ✅ 已上線(工具)
  - research/audits/date_integrity.py:幽靈日(內容指紋)+ 缺交易日(≥2 證人表)。
  - 即重抓清單來源 + 永久守護。重抓執行(刪汙染列 + Scala 重抓)= 下一步。
- **進行中(背景)**:算法學理稽核(10 面,全專案每個計算式對照教科書定義)、
  資料稽核收尾(53/58)、日期偵測器精修重跑。
- **待辦大類**:FC8 產業別 report-date PIT(Scala 捕捉出表日期 + 重解析,影響 live S)、
  FC2/FC3 重抓執行、FC5 累計制缺季、FC6 季報存活者偏誤、FC7 int32 溢位、
  演算法稽核發現的修法、S 全面重驗證。
