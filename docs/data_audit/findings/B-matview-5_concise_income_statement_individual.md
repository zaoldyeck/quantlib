# B-matview-5_concise_income_statement_individual — 財務定義與算式審查

- **稽核對象**：`src/main/resources/sql/materialized_view/5_concise_income_statement_individual.sql`（28 行）
- **日期**：2026-07-22
- **結論**：🔴 **BUG**

---

## 一句話結論

**這張表在 2004-2007 年是壞的，而且壞得很難看**——它把「合併報表」和「個體報表」
兩種完全不同口徑的累計數相減，算出來的單季數字有的放大一萬倍。2013 年以後的數字
可以信（台積電 2024 逐季驗算完全正確）。**好消息是現役的 Python 交易系統根本不讀
這張表**，所以實盤沒有中彈；壞消息是所有引用 Scala 時期 2004-2012 回測結果的結論
都建立在垃圾上。

---

## 這張 view 在做什麼（逐行）

台股損益表是**當年累計數**（Q3 的數字 = 前三季合計），這張 view 的工作是把它還原成
**單季數**。

| 行 | 做的事 |
|---|---|
| 2-11 | `DISTINCT ON (market, year, quarter, company_code, title)` 去重，`ORDER BY ..., type` 決定留哪一筆 |
| 19-22 | 單季化：`Q1 → 直接取累計值`；`Q2-Q4 → 本期累計 − lag(累計)`，視窗 `PARTITION BY company_code, title ORDER BY year, quarter` |
| 24-25 | 只留 `market = 'twse' or 'tpex'` |
| 28 | `where value is not null` 丟掉算不出來的列 |

沒有任何除法、沒有比率、沒有 TTM 滾動——**這張 view 只做一件事：減法**。所以
ROE/ROIC/毛利率定義那類問題不在這一層（在下游 `financial_index_ttm`，已另案稽核）。

**單位**：新台幣千元，差分後不變。抽驗 2330 FY2024 合併營收 2,894,307,699 千元
= NT$2.894 兆，與公開數字相符。🟢

---

## 🔴 BUG 1（新發現，嚴重）— 合併報表與個體報表被拿來互相相減

### 機制

`type` 欄位只有兩個值：`consolidated`（合併）與 `individual`（個體）。
第 11 行 `order by market, year, quarter, company_code, title, type` 配 `DISTINCT ON`，
取的是**字母序最小**的 type → 恆取 `consolidated`（'c' < 'i'）。**只有合併不存在時才退回個體。**

台灣舊制（ROC GAAP，2013 IFRS 全面適用前）**合併財報只要求半年報與年報**。
所以 2006-2007 年：

```sql
SELECT year, quarter, type, count(DISTINCT company_code)
FROM concise_income_statement_progressive
WHERE year IN (2006,2007) GROUP BY 1,2,3 ORDER BY 1,2,3;
-- 2006 Q1 consolidated    21   / individual 1054   ← Q1 幾乎沒有合併
-- 2006 Q2 consolidated  1069   / individual 1205   ← Q2 有
-- 2006 Q3 consolidated    22   / individual 1079   ← Q3 幾乎沒有
-- 2006 Q4 consolidated  1101   / individual 1235   ← Q4 有
```

於是同一家公司的季度序列變成 `個體 → 合併 → 個體 → 合併`，
第 21 行的 `value - lag(value)` 就在**兩種口徑之間相減**。

### 台積電 2330 現場（最乾淨的大型股都錯）

```sql
SELECT year,quarter,type,value FROM concise_income_statement_progressive
WHERE company_code='2330' AND market='twse' AND title='營業收入' AND year IN (2006,2007);
-- 2006 Q1 individual    77,293,344
-- 2006 Q2 consolidated 159,968,160 / individual 158,520,691
-- 2006 Q3 individual   239,945,940
-- 2006 Q4 consolidated 317,407,171 / individual 313,881,635
-- 2007 Q3 individual   222,659,120
-- 2007 Q4 consolidated 322,630,596 / individual 313,647,644

SELECT year,quarter,value FROM concise_income_statement_individual
WHERE company_code='2330' AND market='twse' AND title='營業收入' AND year IN (2006,2007);
-- 2006 Q2 = 82,674,816  ← 159,968,160(合併H1) − 77,293,344(個體Q1)
-- 2006 Q3 = 79,977,780  ← 239,945,940(個體9M) − 159,968,160(合併H1)
-- 2007 Q4 = 99,971,476  ← 322,630,596(合併FY) − 222,659,120(個體9M)
```

2007Q4 應有值（純個體）= 313,647,644 − 222,659,120 = **90,988,524**，
view 給 **99,971,476**，高估 **+9.87%**。

### 全市場量級

```sql
-- 對照「純個體基準」的單季營收，2006-2007 逐季誤差分佈
WITH ind AS (SELECT market,year,quarter,company_code,value,
               lag(value) OVER (PARTITION BY market,company_code ORDER BY year,quarter) p
             FROM concise_income_statement_progressive
             WHERE title='營業收入' AND type='individual' AND market IN ('twse','tpex')),
correct AS (SELECT market,year,quarter,company_code,
              CASE WHEN quarter=1 THEN value ELSE value-p END q_correct
            FROM ind WHERE year BETWEEN 2006 AND 2007),
mv AS (SELECT market,year,quarter,company_code,value q_mv
       FROM concise_income_statement_individual WHERE title='營業收入' AND year BETWEEN 2006 AND 2007)
SELECT year,quarter,count(*) n,
  count(*) FILTER (WHERE abs(q_mv-q_correct) > 0.005*abs(nullif(q_correct,0))) n_off,
  percentile_cont(0.5)  WITHIN GROUP (ORDER BY abs(q_mv-q_correct)/nullif(abs(q_correct),0))*100 med_pct,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(q_mv-q_correct)/nullif(abs(q_correct),0))*100 p95_pct
FROM correct c JOIN mv USING (market,year,quarter,company_code)
WHERE q_correct IS NOT NULL AND q_mv IS NOT NULL AND c.quarter<>1 GROUP BY 1,2;
```

| 年 | 季 | 樣本 | 誤差 >0.5% 家數 | 中位絕對誤差 | p95 絕對誤差 |
|---|---|---|---|---|---|
| 2006 | Q2 | 1,025 | 776 (76%) | **18.48%** | 387% |
| 2006 | Q3 | 1,050 | 791 (75%) | **17.46%** | 398% |
| 2006 | Q4 | 1,171 | 892 (76%) | **30.68%** | 743% |
| 2007 | Q2 | 1,202 | 902 (75%) | **16.34%** | 455% |
| 2007 | Q3 | 1,114 | 851 (76%) | **18.49%** | 459% |
| 2007 | Q4 | 1,205 | 925 (77%) | **34.10%** | 963% |

跨基準的差分列比例：**2006 年 2,783/3,260 = 85.4%；2007 年 2,951/3,541 = 83.3%**（僅 `營業收入` 一個 title）。

### 最毒的案例：控股公司（個體營收極小、合併營收極大）

| 代號 | 名稱 | 期別 | view 單季營收（千元） | 真實個體單季 | 放大倍數 |
|---|---|---|---|---|---|
| 2514 | 龍邦 | 2006Q4 | 61,648,584 | 5,620 | **10,970×** |
| 3701 | — | 2006Q4 | 56,488,428 | 10,840 | 5,211× |
| 3702 | 大聯大 | 2006Q4 | 114,984,784 | 313,166 | 367× |
| 9904 | 寶成 | 2006Q4 | 176,553,872 | 3,283,168 | 54× |
| 2375 | — | 2006Q4 | 3,439,119 | 99,773 | 34× |

### 還有一層：同一截面上混用兩種口徑

```sql
-- 每年截面上「合併基準」公司占比
-- 2004 10.5% / 2005 26.1% / 2006 47.5% / 2007 47.0%
-- 2008 88.5% / 2012 89.4% / 2013 99.6% / 2015 100.0%
```

2006-2007 有近一半公司走合併、一半走個體。**跨股票排名等於拿合併數跟個體數比大小**
（大聯大個體營收 1.45B vs 合併 116B，差 80 倍）。任何 2004-2012 的營收 / 毛利率 /
淨利率**截面因子**都被這件事污染。2008-2012 降到每年 15-33 筆（合併覆蓋補齊），
2015 年後歸零。

### 爆炸半徑（好消息）

現役 Python 路徑**不吃這條線**：

```python
# research/cache_tables.py:84-87
("is_progressive_raw",
 "SELECT market, type, year, quarter, company_code, title, value "
 "FROM pg.public.concise_income_statement_progressive "
 "WHERE market IN ('twse','tpex') AND type='consolidated'"),   # ← 明確鎖死合併
```

Serenity / apex / Evergreen 走 `research/strat_lab/raw_quarterly.py`，
直接讀 `concise_income_statement_progressive` 且已鎖 `type='consolidated'`。
**中彈的是 Scala 凍結策略層那條鏈**（見下方「消費者」）。

---

## 🔴 BUG 2（已知類，本單位獨立復現）— 缺一季時，`lag` 跳到不相鄰的期別

第 19-22 行只判斷「是不是 Q1」，**沒有檢查前一筆是不是上一季**。

```sql
WITH d AS (SELECT DISTINCT ON (market,year,quarter,company_code,title)
             market,year,quarter,company_code,title,value,type
           FROM concise_income_statement_progressive WHERE title='營業收入'
           ORDER BY market,year,quarter,company_code,title,type),
     f AS (SELECT * FROM d WHERE market IN ('twse','tpex')),
     w AS (SELECT *, lag(value) OVER w pv, lag(year) OVER w py, lag(quarter) OVER w pq
           FROM f WINDOW w AS (PARTITION BY company_code,title ORDER BY year,quarter))
SELECT quarter, py=year AS same_year, pq, count(*) FROM w
WHERE quarter<>1 AND pv IS NOT NULL AND NOT (py=year AND pq=quarter-1)
GROUP BY 1,2,3 ORDER BY 4 DESC;
```

| 本期 | lag 落到 | 筆數 | 產出的東西 |
|---|---|---|---|
| Q2 | 去年 Q4 | **1,447** | 半年累計 − 去年全年累計 → 通常巨額負值 |
| Q4 | 同年 Q2 | **1,256** | H2 合計貼成 Q4 → 約 2 倍高估 |
| Q4 | 去年 Q4 | 231 | 全年 − 去年全年 → 垃圾 |
| Q3 | 同年 Q1 | 217 | Q2+Q3 貼成 Q3 → 2 倍高估 |
| Q3 | 去年 Q4 | 52 | 垃圾 |
| 其他 | — | 9 | 垃圾 |

**輸出端指紋**：`concise_income_statement_individual` 有 **1,975 列單季營收為負**，
歸因為：缺季 1,464 列（74%）、type 換基 484 列（25%）、無法解釋 80 列（4%）。
逐年分佈：2006 年 196、2007 年 361、2008-2017 每年 88-163、2018 後降到個位數~40。

本 bug 已在 `docs/data_audit/findings/B-view-3_financial_index_quarterly.md` BUG 3
以現金流量表記載；本單位以損益表 `營業收入` 獨立復現並補上 lag 落點分佈。

---

## 🟡 BUG 3（小，但定義上錯）— 非加總型科目也被無差別差分

第 19-22 行對**所有 122 個 title 一律差分**，包含本質上不是流量的科目。

**「換算匯率」被差分**（匯率是水準值，減法沒有意義）：

```sql
SELECT p.year,p.quarter,p.company_code,p.value raw_fx,m.value matview_fx
FROM concise_income_statement_progressive p
JOIN concise_income_statement_individual m USING (market,year,quarter,company_code,title)
WHERE p.title='換算匯率' AND p.market IN ('twse','tpex') ORDER BY 3,1,2;
-- 910069 2010Q4  22.73 → -0.11
-- 910069 2011Q2  23.38 →  0.65
-- 9102   2005Q2  18.78 → -0.33
```
影響 86 列（`換算匯率` 84 + `換算匯率參考依據` 2），下游沒有消費，爆炸半徑小。

**每股盈餘（EPS）也被差分**，而下游**確實在用**
（`6_concise_financial_statement_with_titles.sql:81-88` 的 `eps` CTE）。
累計 EPS 差分是業界通用近似，但季間加權平均股數不同（台股股票股利頻繁），
所以不是精確值。指紋——單季 EPS 與單季稅後淨利**符號矛盾**的比例：

| 期間 | 樣本 | 符號矛盾 | 比例 |
|---|---|---|---|
| 2013 年前 | 61,258 | 896 | 1.46% |
| 2013 年後 | 87,199 | 960 | 1.10% |

（部分屬正常：EPS 是母公司歸屬、淨利含非控制權益。）

---

## 🟢 更正前一份稽核：「沒有按年分區」不是 bug

`docs/data_audit/findings/B-fscore-academic.md:325` 把
`5_concise_income_statement_individual.sql:19-22` 標為 **BUG-5「沒有按年分區」**。
**這是誤判。**

`quarter` 值域實測恰為 {1,2,3,4}，而 `case when quarter = 1 then value` 在 Q1
**直接回傳累計值、完全不碰 lag** → 跨年邊界本來就正確，等價於按年分區。

```sql
-- 670,841 筆 Q1 列與來源累計值 100% 逐位相同
WITH d AS (SELECT DISTINCT ON (market,year,quarter,company_code,title)
             market,year,quarter,company_code,title,value
           FROM concise_income_statement_progressive WHERE market IN ('twse','tpex')
           ORDER BY market,year,quarter,company_code,title,type)
SELECT count(*) q1_rows, count(*) FILTER (WHERE d.value=m.value) q1_identical
FROM d JOIN concise_income_statement_individual m
  USING (market,year,quarter,company_code,title) WHERE d.quarter=1;
-- 670841 | 670841
```

真正的 bug 是**缺季**（BUG 2），不是分區。修法也不同：加年分區沒用，要加
「前一筆必須是 (同年, 本季−1)」的守衛。

---

## 🟢 缺 market 分區：實測無害，而且可能比姊妹檔更正確

第 22 行 `partition by company_code, title`（**沒有 market**），
與姊妹檔 `7_income_statement_individual.sql:11`
的 `partition by market, company_code, title` **不一致**。

實測：

```sql
-- 同一 company_code 在同一 (year,quarter) 同時出現在兩市場的筆數 = 0
-- 曾跨市場（上櫃轉上市）的公司 = 13 家
-- 跨市場相鄰差分列 = 13 筆（2021:1、2023:10、2025:2）
```

因為沒有同季碰撞，不分 market **反而讓轉板公司的單季序列保持連續**
（轉板後仍是同一年度累計，接著減是對的）；姊妹檔的 by-market 分區會在轉板季斷掉。
**結論：不是 bug，但是未文件化的隱性設計**，兩檔應統一並在註解說明理由。

---

## 🟢 其他查過沒問題的

| 項目 | 結果 |
|---|---|
| **分母保護 / ±inf** | 本 view **沒有任何除法**，只有減法 → 不可能除以零或產生 ±inf。分母保護是下游 `financial_index_*` 的責任（已另案稽核）。 |
| **鍵唯一性** | matview 對 (market, year, quarter, company_code, title) **零重複**。 |
| **`where value is not null` 丟列** | matview 2,783,022 列 vs 來源去重 2,826,092 列，丟 43,070 列。實測 43,070 列**全部**是「非 Q1 且序列無前一筆」的頭列 → 無法差分，丟棄正確。 |
| **market 值域** | 只有 `twse` / `tpex` 兩值，第 24-25 行的過濾實質是 no-op，不會誤刪。 |
| **幣別與單位** | 新台幣千元，差分後不變。2330 FY2024 合併營收 2,894,307,699 千元 = NT$2.894 兆，與公開數字相符。 |
| **現代年份正確性** | 2330 2024 逐季驗算 Q1 592,644,201 / Q2 673,510,177 / Q3 759,692,143 / Q4 868,461,178 — 與累計數逐位吻合。**2013 年後資料可信。** |

---

## 🟡 PIT（時點對齊）— 本檔無責，但有殘留前視

- 本 view **沒有任何申報日欄位**，只有 (year, quarter)。PIT 由下游
  `src/main/scala/strategy/PublicationLag.scala:33-39` 處理（法定申報期限 Q1 5/15、
  Q2 8/14、Q3 11/14、Q4 次年 3/31，各 +7 天緩衝）→ **不是本檔的 bug**。
- **殘留前視**：來源表唯一索引為
  `(market, type, year, quarter, company_code, title)`，**重編（restatement）
  直接覆蓋原值、不保留版本**。所以歷史季度存的是「最新重編版」而非「當時已公告版」。
  這是來源表的結構限制（屬 `C-is_progressive_raw` 範疇），但會原封傳導到本 view。

---

## 消費者鏈與爆炸半徑

```
concise_income_statement_progressive  (raw, 累計)
  └─ concise_income_statement_individual        ← 本單位
       └─ 6_concise_financial_statement_with_titles.sql:34,41,46,50,61,73,82
            └─ financial_index_ttm / growth_analysis_ttm
                 └─ Scala 策略層（已凍結）
                    ValueRevertStrategy.scala:66 / MultiFactorStrategy.scala:78
                    QualityFilter.scala:31 / DividendYieldStrategy.scala:49
                    ValueMomentumStrategy.scala:70
                    Signals.scala:139,142,531,549
```

- Scala 策略層依 `CLAUDE.md` 已**凍結為歷史參考**。
- 現役 Python 路徑（Serenity `ev_v3_wf` / apex / Evergreen）**完全不經此鏈**
  （`research/cache_tables.py:84-87` 直讀 progressive 且鎖 `type='consolidated'`）。
- refresh 掛在 `src/main/scala/reader/FinancialReader.scala:161`。

**白話**：實盤沒中彈。但任何引用 Scala 時期 **2004-2012** 回測數字的結論，
地基是壞的，要重跑或作廢。

---

## 建議修法（不在本單位執行）

依嚴重度排序：

1. **鎖死報表口徑**（BUG 1，根因）。`distinct on` 的 `order by ... type` 是靠
   **字母序**碰巧選到 consolidated 的隱性依賴——加第三個 type 值就會無聲改變行為。
   改成明確的口徑欄位 + 明確優先序，並且**同一條時間序列不准換基**：
   ```sql
   -- 只用合併；合併不完整的公司-年度整條不出（而不是逐季混著出）
   where type = 'consolidated'
   ```
   若要保留 2013 年前的個體資料，必須**分成兩個 basis 欄位並列**
   （`value_consolidated` / `value_individual`），由消費端明確選一種，
   **禁止在同一序列內切換**。

2. **缺季守衛**（BUG 2）：
   ```sql
   case when quarter = 1 then value
        when lag(year) over w = year and lag(quarter) over w = quarter - 1
             then value - lag(value) over w
        else null end
   ```
   缺季就給 NULL，不要生一個看起來正常的錯數字。
   **同類掃描**：`3_cash_flows_individual.sql:8-12`、
   `7_income_statement_individual.sql:8-11` 完全同型；
   `research/strat_lab/raw_quarterly.py:127-148,176-180` 雖有年分區但仍是
   「位移一列」不是「位移一季」，**缺季時同 bug**，而那條是現役研究路徑。

3. **非加總型科目白名單**（BUG 3）：`換算匯率` / `換算匯率參考依據` 這類水準值
   科目不得差分（直接沿用累計值或整個排除）。EPS 差分屬近似——**保留但在 view
   檔頂端註明「單季 EPS 為累計差分近似，季間加權股數不同」**，讓下游知道它不是精確值。

4. **統一 partition 語意**：本檔（無 market）與 `7_income_statement_individual.sql`
   （有 market）擇一，並在註解寫明理由（實測支持**不分 market**，因為轉板公司的
   累計數是連續的、且無同季碰撞）。

5. **防復發守護**（先紅後綠）：一支
   `research/tests/test_concise_income_individual.py`，鎖死三個斷言——
   (a) 台積電 2024 四季單季營收 = 592,644,201 / 673,510,177 / 759,692,143 / 868,461,178；
   (b) 任一 (company_code, title) 序列內 `type` 不得變動；
   (c) `營業收入` 單季為負的列數必須為 0（扣除 80 列已審查的真實負值白名單）。

---

## 這一輪查了什麼（避免下次重查）

- 精讀 `5_concise_income_statement_individual.sql` 全 28 行；逐行寫出算式
- 精讀姊妹檔 `1_concise_balance_sheet_individual.sql`、`3_cash_flows_individual.sql`、
  `7_income_statement_individual.sql`、下游 `6_concise_financial_statement_with_titles.sql`（149 行）
- 讀 `concise_income_statement_progressive` schema（無申報日欄位、唯一索引含 type）
- 盤點 `type` 值域（consolidated / individual）與逐年、逐季覆蓋率
- 量化：跨 type 差分列比例（逐年）、缺季 lag 落點分佈、跨市場差分列、單季負營收歸因
- 對照純個體基準，量化 2006-2007 逐季單季營收誤差分佈（中位 / p95 / 最大）
- 挑 2330（大型股）、2514 / 3701 / 3702 / 9904 / 2375（控股型）手算對帳
- 驗證跨年邊界（670,841 筆 Q1 逐位相同）→ **推翻 B-fscore-academic BUG-5**
- 驗證 `value is not null` 丟的 43,070 列全部是可解釋的頭列
- 驗證鍵唯一性（0 重複）、market 值域、幣別單位（2330 FY2024 對帳）
- 驗證 EPS 差分破裂指紋（單季 EPS vs 單季淨利符號矛盾率）
- trace 消費者鏈到 Scala 策略層；確認 `research/cache_tables.py:84-87` 已鎖
  `type='consolidated'` → 現役 Python 路徑不受 BUG 1 影響
- 讀 `strategy/PublicationLag.scala` 確認 PIT 由下游負責
- 交叉比對既有稽核 `B-view-3_financial_index_quarterly.md`、`B-fscore-academic.md`、
  `B-view-4_financial_index_ttm.md`，避免重複並更正其中一項誤判
