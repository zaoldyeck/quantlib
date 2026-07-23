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

- **2026-07-23 FC1 除權/減資還原** ✅ 已上線(43be617, 927f0d2)
  - 配股改用交易所參考價還原(2,304 筆純配股不再幽靈崩跌);大減資護欄放寬(16 筆)。
  - cache_tables.py + db.py 同步參考價欄;prices.py 對兩種 cache 世代皆正確。
  - 待:cache 的 ex_right_dividend 表 rebuild(稽核釋放鎖後);69 筆全零列重解析。
- **2026-07-23 FC4 F-Score → Piotroski (2000) 學理精確** ✅ 已上線(fab40dd)
  - 年初資產分母 / NULL 傳播 + n_valid / 日曆對齊 / 科目優先序 / f7 去魔術數字。
  - 金鑰 2330 FY2024 = 8/9 逐項符合手算;守護 5 項。
  - 待:raw_quarterly.parquet 重生、is/bs/cf cache rebuild、S 重跑 walk-forward 對照。
- **2026-07-23 FC2/FC3/FC9 日期完整性偵測器** ✅ 已上線(工具)
  - src/quantlib/audits/date_integrity.py:幽靈日(內容指紋)+ 缺交易日(≥2 證人表)。
  - 即重抓清單來源 + 永久守護。重抓執行(刪汙染列 + Scala 重抓)= 下一步。
- **進行中(背景)**:算法學理稽核(10 面,全專案每個計算式對照教科書定義)、
  資料稽核收尾(53/58)、日期偵測器精修重跑。
- **待辦大類**:FC8 產業別 report-date PIT(Scala 捕捉出表日期 + 重解析,影響 live S)、
  FC2/FC3 重抓執行、FC5 累計制缺季、FC6 季報存活者偏誤、FC7 int32 溢位、
  演算法稽核發現的修法、S 全面重驗證。

## 架構決策(2026-07-23 使用者定調):全爬蟲 Python 化,Scala 退役

**背景**:line S 跑在只有 Python 的 VM,但 cfo_ni 閘門吃財報(BS/IS/CF)——那些只有
Scala 爬+解析,靠 scp 財報衍生的 raw_quarterly.parquet 到 VM。VM 因此不自足,且那份
資料是 Scala 解析的(有 bug)。

**決策**:把 11 個只有 Scala 的源全部 Python 化,Scala 爬蟲+reader 退役。這同時
(a) 讓 VM 自足(b) 用「重寫正確 Python」取代「修 Scala reader」——correct-by-construction,
照稽核 bug 一次寫對 (c) 統一技術棧。**這取代了原 FC7 + Scala reader 修復路線。**

**要 port 的源**(照 src/quantlib/crawl/sources/ 框架:source adapter + Sink upsert + SchemaDrift 守護):
- 財報(live 關鍵):balance_sheet、income_statement、cash_flows
- 籌碼:margin_transactions、foreign_holding_ratio、sbl_borrowing、tdcc、insider、treasury
- 指數:index(market_index);期貨:taifex_*;financial_analysis

**每個 port 的驗收**:(1) parser 修掉該源稽核發現的所有 bug(2) 對現存「正確」PG 資料
逐位 parity(已知壞的日期除外,那些本就要重抓)(3) 落地測試。

**順序**:稽核完成(拿完整 parser bug 清單)→ port workflow 平行重寫 → 逐個驗 parity →
切換 crawl/update.py 用新源 → slim_cache 加財報/籌碼 → Scala 退役 → S 重驗證。

**兩條軌道(使用者 2026-07-23 補充定調)**:
1. **往前 = 全 Python**:爬蟲 + 研發策略 + 實盤下單全部 Python,以後完全不依賴 Scala。
   (研發與 live S 已是 Python;唯一 Scala 依賴 = 11 源的爬蟲+解析 → port 後徹底斷開。)
2. **收尾 = Scala bug 修到結案**:不因「要退役」而擺爛。每個 Scala 稽核發現(reader
   欄位錯位、int32 溢位、name-strip、tpex fallthrough、view 算式)都要有交代——
   **能在 Scala 修的就修好 + 加測試證明**(即使之後不跑,程式碼不留破 bug);data 面
   由 Python re-crawl 產生正確版(correct-by-construction),兩者互為獨立驗證。
   findings 逐條標記 resolved(修法 + commit)才算 close,然後 Scala 正式退役封存。

## 架構決策 2(2026-07-23):PostgreSQL 退役,DuckDB cache 為唯一真源

**查證**:PG 現在的唯一角色 = Scala 資料的中繼落地區 + cache_tables.py 重建 cache 的
來源。live S(VM)早已無 PG(Python-only 讀 cache.duckdb);現有 Python 爬蟲也直寫
cache 跳過 PG。

**決策**:全爬蟲 Python 化後,全部直寫 cache.duckdb → 沒有東西再寫/讀 PG → **PG 整個
移除**。最終架構:
    Python 爬蟲 → cache.duckdb(唯一結構化真源)→ 研究 + live
    data/*.csv 仍為原始封存(不可重生的事實地基,paths.RAW)
retire:cache_tables.py(PG→cache 同步)、db.py 的 pg-attach 模式、application.conf 的 PG 設定。

**附帶消滅的 bug 類**:PG↔cache 雙寫入路徑漂移(cache 領先 PG、產業別對不上、索引
被 DROP TABLE 弄丟等 C 維 findings)——單一 store + 單一寫入路徑後結構性消失。
PG 的 buggy view(financial_index_ttm/growth_analysis_ttm)也隨之退場(已被 Python
raw_quarterly.py 取代)。

**轉換**:port 各爬蟲 → 寫正確資料進 cache → 全 24 表在 cache 內以 Python 產生 →
移除 cache_tables.py + db.py pg-attach + PG。**過渡期保留 PG 唯讀供 parity 對照**
(Python port vs 現存 PG 資料逐位比對),parity 全過 + Scala 退役後才 drop PG。
