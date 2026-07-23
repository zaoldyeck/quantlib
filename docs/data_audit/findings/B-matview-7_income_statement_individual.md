# B-matview-7_income_statement_individual — 財務定義與算式審查

- **稽核對象**：`src/main/resources/sql/materialized_view/7_income_statement_individual.sql`（12 行）
- **日期**：2026-07-23
- **結論**：🔴 **BUG**（算式有真實缺陷，但**這張表沒有任何程式讀它** → 爆炸半徑為零）

---

## 一句話結論

**這張表算錯了，但沒有人在用它，所以實盤與回測都沒中彈。** 它把台股的累計損益表
還原成單季數，用的是跟姊妹檔 view #5 一模一樣的「本季累計 − 上季累計」減法。缺一季
時它會把兩季的錢貼成一季（營收單獨一個科目就有約 6,760 列被污染、3,217 列變成負數），
還把 14 萬列算不出來的 NULL 原封留在表裡（姊妹檔會濾掉）。**好消息有三個**：(1) 全 repo
grep 不到任何一支程式 SELECT 這張 matview——它是孤兒，每次匯入財報白刷新 399 萬列；
(2) 它的來源表**沒有 type 欄位**，所以躲過了 view #5 最毒的「合併 ÷ 個體互減」大 bug；
(3) 台積電 2011/2012/2023/2024 四段逐季驗算全部逐位吻合，現代與早期的乾淨大型股資料
本身是對的。**修法建議直接刪掉這張孤兒表**，而不是修它。

---

## 這張 view 在做什麼（逐行）

台股損益表是**當年累計數**（Q3 = 前三季合計），這張 view 把它還原成**單季數**。

```sql
create materialized view income_statement_individual as
select id, market, year, quarter, company_code, title,
       case when quarter = 1 then value
            else value - lag(value)
                 over (partition by market, company_code, title order by year, quarter)
       end as value
from income_statement_progressive;
```

| 行 | 做的事 |
|---|---|
| 8-9 | Q1 → 直接取累計值（不碰 lag）→ 跨年邊界正確 |
| 10-11 | Q2-Q4 → `本期累計 − lag(累計)`，視窗 `partition by market, company_code, title order by year, quarter` |

**沒有任何除法、沒有比率、沒有 TTM 滾動、沒有 `where value is not null`、沒有 type 去重**。
它比姊妹檔 view #5 更精簡——也因此少了 view #5 的三道保護。

**單位**：新台幣千元，差分後不變（2330 FY2024 = 2,894,307,699 千元 = NT$2.894 兆，與公開數字相符）🟢。

---

## 來源表與姊妹檔的關鍵差異（決定了哪些 bug 會發生）

| 項目 | view #7（本檔） | view #5（姊妹檔，已判 BUG） |
|---|---|---|
| 來源表 | `income_statement_progressive`（逐家完整財報 HTML，1,914 個 title） | `concise_income_statement_progressive`（簡明表 CSV，122 個 title） |
| `type` 欄（合併/個體） | **無**（唯一索引 `market,year,quarter,company_code,title`，一鍵一值） | **有** → 字母序恆取 consolidated |
| `market` 值域 | **只有 `tw`**（實測） | `twse` / `tpex` 兩值 |
| Q1 特判 | 有 | 有 |
| 缺季守衛 | **無** | 無 |
| `where value is not null` | **無**（NULL 全留） | 有（濾掉 NULL） |
| type 去重 | 不需要（一鍵一值） | `distinct on ... type` |

因為來源表**沒有 type 欄、唯一索引保證一鍵一值**，view #5 最嚴重的
「同一序列在合併 → 個體之間跳動、跨口徑相減」在 view #7 **結構上不可能發生**。
`market` 只有 `tw` 一個值，`partition by market, ...` 等於沒分 market → 姊妹檔擔心的
「轉板公司單季序列斷掉」在這裡也不會發生。**view #7 躲過了 view #5 的兩個問題，
但保留了另外兩個，還多一個。**

---

## 🔴 BUG 1（真實缺陷，與 view #5 BUG 2 同類，本檔獨立復現）— 缺一季時 `lag` 跳到不相鄰期別

第 8-11 行只判斷「是不是 Q1」，**沒有檢查前一筆是不是 (同年, 本季−1)**。缺一季時
`lag` 直接取到再前面一筆，橫跨多季的差額被貼上「單季」標籤，產出**看起來正常的錯數字**。

### 指紋（只看 `營業收入合計` 一個科目，可重跑）

```sql
WITH w AS (
  SELECT market,year,quarter,company_code,value,
         lag(value) OVER win pv, lag(year) OVER win py, lag(quarter) OVER win pq
  FROM income_statement_progressive WHERE title='營業收入合計'
  WINDOW win AS (PARTITION BY market,company_code ORDER BY year,quarter))
SELECT quarter AS this_q, (py=year) AS same_year, pq AS prev_q, count(*) n
FROM w WHERE quarter<>1 AND pv IS NOT NULL AND NOT (py=year AND pq=quarter-1)
GROUP BY 1,2,3 ORDER BY n DESC;
```

| 本期 | lag 落到 | 筆數 | 產出的東西 |
|---|---|---|---|
| Q4 | 同年 Q2 | **2,992** | 缺 Q3 → H2 合計貼成 Q4 → 約 2 倍高估 |
| Q2 | 去年 Q4 | **2,970** | 缺 Q1 → 半年累計 − 去年全年累計 → 巨額負值 |
| Q2 | 去年 Q2 | 435 | 垃圾 |
| Q3 | 同年 Q1 | 222 | 缺 Q2 → Q2+Q3 貼成 Q3 → 2 倍高估 |
| Q4 | 去年 Q4 | 153 | 全年 − 去年全年 → 垃圾 |
| 其他 | — | ~28 | 垃圾 |

**輸出端指紋**：`income_statement_individual` 的 `營業收入合計` 有 **3,217 列單季營收為負**、
**2,669 列為 NULL**（共 112,006 列）。負值主要來自「缺 Q1、Q2 減去年 Q4」那 2,970 列。
這只是 1,914 個 title 裡的一個；全表污染量級遠大於此。

---

## 🔴 BUG 2（view #5 沒有、本檔獨有）— 算不出來的 NULL 列被原封留在表裡，且與 Slick schema 相牴觸

第 8-11 行對「非 Q1 且序列無前一筆」的頭列產生 `value − NULL = NULL`。
**view #5 用 `where value is not null` 濾掉這些列；view #7 沒有這道濾網 → 全留。**

```sql
SELECT count(*) FILTER (WHERE value IS NULL) FROM income_statement_individual; -- 140,128
SELECT quarter, count(*) FROM income_statement_individual WHERE value IS NULL GROUP BY 1;
-- Q1 0 / Q2 47,350 / Q3 9,692 / Q4 83,086
```

- **140,128 列 NULL**（占全表 3.5%），全部是非-Q1 頭列（Q1 為 0，證明 Q1 特判正確、
  NULL 全來自無前一季的差分頭）。Q4 佔 83,086 列——完整財報有大量只在年報出現的科目，
  它們沒有 Q1-Q3 前身，算不出單季屬正常，但**留成 NULL 列是錯的處置**。
- **與 Slick schema 相牴觸**：`db/table/IncomeStatement.scala:29,38` 把
  `IncomeStatementIndividual.value` 宣告為 `column[Double]`（非 nullable 原生 Double）。
  這張 matview 卻含 14 萬列 NULL value。**只要有人用 `TableQuery[IncomeStatementIndividual]`
  查它，Slick 對 NULL→Double 會炸（NPE / 讀成 0.0）。** 目前沒人查（見下），所以是潛伏彈。

---

## 🟡 SUSPECT 1 — 非加總型科目（EPS 等）被無差別差分

第 8-11 行對**全部 1,914 個 title 一律差分**，包含本質上不是流量的每股盈餘。

```sql
-- 2330 基本每股盈餘合計（元），2024
SELECT p.year,p.quarter,p.value eps_cum, m.value eps_mv
FROM income_statement_progressive p
JOIN income_statement_individual m USING (market,year,quarter,company_code,title)
WHERE p.company_code='2330' AND p.title='基本每股盈餘合計' AND p.year=2024;
-- Q1 8.70→8.70 / Q2 18.25→9.55 / Q3 30.80→12.55 / Q4 45.25→14.45
```

`基本每股盈餘合計` 有 98,858 列、`稀釋每股盈餘合計` 84,016 列，全部被差分。
累計 EPS 差分是**業界通用近似**（多數資料商也這樣做），但季間加權平均股數不同
（台股股票股利頻繁），所以不是精確值。列 SUSPECT 而非 BUG，因為它是可接受的近似，
且本表無人消費。

**好消息**：view #5 有的 `換算匯率`（水準值被差分）在本表**不存在**（`title LIKE '%換算匯率%'`
= 0 列），所以那個子 bug 不適用。

---

## 🟡 SUSPECT 2 — 殘留前視（重編覆蓋，繼承自來源表）

- 本 view 與來源表都**沒有申報日欄位**，只有 (year, quarter)。
- 來源表唯一索引 `(market,year,quarter,company_code,title)`（`IncomeStatement.scala:31`），
  **重編（restatement）直接覆蓋原值、不保留版本** → 歷史季度存的是「最新重編版」而非
  「當時已公告版」。這是來源表結構限制（屬 `C-is_progressive_raw` 範疇），會原封傳導到本 view。
- 期別對齊（PIT）本身由 Scala 下游 `strategy/PublicationLag.scala` 處理，不是本檔的責任；
  但因本表無人消費，此點對現況無實害。

---

## 🟢 這張表沒有人在用（爆炸半徑為零 — 全查證）

全 repo（.scala / .sql / .py / .conf / .md）grep `income_statement_individual`（排除 `concise_`）
**只命中三處，且沒有任何一處是 SELECT / 查詢**：

| 位置 | 性質 |
|---|---|
| `7_income_statement_individual.sql:1` | 自己的 DDL |
| `db/table/IncomeStatement.scala:38` | Slick class `IncomeStatementIndividual`，**全 repo 無 `TableQuery[IncomeStatementIndividual]`、無任何查詢** |
| `reader/FinancialReader.scala:289` | 每次匯入財報 `refresh materialized view income_statement_individual`（只刷新、不讀） |

旁證：
- **Scala 策略層讀的是原始 `income_statement_progressive`，不是本 view**
  （`strategy/Signals.scala:604,649,746,754`，Greenblatt ROIC / earnings yield），
  且用 `DISTINCT ON (company_code) ... ORDER BY year DESC, quarter DESC` 取**最新累計快照**，
  **根本不做單季差分** → 不吃本 view 的任何 bug。
- **下游 `6_concise_financial_statement_with_titles.sql` 用的是 view #5（concise），不是本 view #7。**
- **Python 研究路徑**（Serenity / apex / Evergreen）完全不碰本 view
  （`research/cache_tables.py` 讀 concise progressive）。

**結論**：BUG 1/2 的產出雖然客觀錯誤，但**沒有任何回測、因子、實盤決策讀它**。
唯一代價是每次 `Main update` 白刷新一張 399 萬列的死表。

---

## 🟢 其他查過沒問題的（負結果落盤）

| 項目 | 結果 |
|---|---|
| **Q1 跨年邊界** | 2330 2024Q1 = 592,644,201（= 累計值直取），我方 naïve lag 會給 −1,569,091,640，matview 正確避開 → Q1 特判有效。 |
| **現代年份正確性** | 2330 2023 單季營收 508,632,973 / 480,841,254 / 546,732,758 / 625,528,856（ΣFY 2,161,735,841 ✓）；2024 592,644,201 / 673,510,177 / 759,692,143 / 868,461,178（ΣFY 2,894,307,699 ✓，且與 view #5 concise 管線**逐位相同**）。 |
| **早期年份（pre-2013）** | 2330 2011 105,377,495 / 110,508,367 / 106,483,616 / 104,711,167（ΣFY 427,080,645 ✓）；2012 ΣFY 506,248,580 ✓。乾淨大型股即使 pre-2013 也內部一致——因無 type 欄，view #5 的 pre-2013 雙口徑災難在此**不重現**。 |
| **合併/個體混減（view #5 BUG 1）** | **不適用**。來源表無 type 欄、唯一索引一鍵一值，結構上不可能跨口徑相減。 |
| **market 分區** | `market` 只有 `tw` 一值 → `partition by market,...` 為 no-op，無轉板斷裂。 |
| **鍵唯一性** | matview 對 (market,year,quarter,company_code,title) **零重複**。 |
| **分母保護 / ±inf** | 本 view 只有減法、無除法 → 不可能除以零或 ±inf。 |
| **幣別單位** | 新台幣千元，差分後不變。 |

---

## 🟡 SUSPECT 3 — 報表口徑（合併 vs 個體）是解析期選的、未文件化

來源表無 type 欄，那**它到底是合併還是個體？** 答案藏在爬蟲：
`reader/FinancialReader.scala:201`（2019+）用檔名的 type token
`sortBy(_._1).distinctBy(_._2)` **每家每季各留一種口徑**（字母序最小者）。
2019+ 兩種口徑每季都申報 → 穩定選同一種 → 序列內不換基；跨年換基也被 Q1 特判擋住。
2330 各年 ΣFY 逐位吻合佐證口徑穩定。**但這個選擇是隱性、未寫進任何文件**——
若哪天某公司某季只申報另一種口徑，會無聲換基而無 type 欄可偵測。列 SUSPECT
（未見實害，但屬未文件化的隱性依賴）。

---

## 建議修法（不在本單位執行）

**首選：直接刪掉這張孤兒表**——它沒有任何消費者，且與 view #5（concise 管線）功能重疊。
移除 `7_income_statement_individual.sql` 的 DDL、`IncomeStatement.scala:38` 的 Slick class、
`FinancialReader.scala:289` 的 refresh 呼叫。少刷新一張 399 萬列死表，也消滅潛伏的
NULL→Double Slick 彈。

**若要保留**（例如未來想用完整財報的逐季科目），比照 view #5 的修法補三道保護：
1. **缺季守衛**：
   ```sql
   case when quarter = 1 then value
        when lag(year) over w = year and lag(quarter) over w = quarter - 1
             then value - lag(value) over w
        else null end
   ```
   缺季給 NULL，不要生看起來正常的錯數字。
2. **加 `where value is not null`**，或把 Slick `value` 改成 nullable，二擇一消除 schema 牴觸。
3. **非加總型科目白名單**：EPS 類差分保留但於檔頂註明「單季 EPS 為累計差分近似、
   季間加權股數不同、非精確值」。

**同類掃描**（此 bug 是一整類）：`3_cash_flows_individual.sql:8-12`（現金流量表，完全同型）、
`5_concise_income_statement_individual.sql:19-22`（已判 BUG）、
`research/strat_lab/raw_quarterly.py`（現役研究路徑，雖有年分區但「位移一列 ≠ 位移一季」，
缺季時同 bug——見 `C-is_progressive_raw` BUG 2）。**本 view 是這一類的第三個樣本。**

---

## 這一輪查了什麼（避免下次重查）

- 精讀 `7_income_statement_individual.sql` 全 12 行，逐行寫出算式
- 對照姊妹檔 `5_concise_income_statement_individual.sql` 與其已完成稽核，逐項比對來源表、type 欄、
  market 值域、Q1 特判、缺季守衛、NULL 濾網、去重的有無
- 讀來源表 `income_statement_progressive` schema（`\d`）：無 type 欄、無申報日、
  唯一索引 (market,year,quarter,company_code,title)
- 讀 Slick 定義 `db/table/IncomeStatement.scala:16-38`（value 宣告為非 nullable Double）
- 讀爬蟲 `reader/FinancialReader.scala:165-291`：來源表填充路徑、口徑選擇
  （:201 `sortBy(_._1).distinctBy(_._2)`）、refresh 呼叫（:289）
- 讀 `strategy/Signals.scala:594-670`：確認 Scala 策略讀原始 progressive、用最新累計快照、不做差分
- 全 repo grep 消費者：確認本 matview 零 SELECT、Slick class 零查詢、下游 view #6 用的是 view #5
- PG 實測：全表/matview 筆數（3,989,598）、NULL value 列數（140,128，逐季分佈 Q1=0）、
  distinct title（1,914）、market 值域（僅 tw）、quarter 值域（{1,2,3,4}）、逐年覆蓋（2009-2026）
- 缺季 lag 落點分佈（`營業收入合計`：Q4←同年Q2 2,992 / Q2←去年Q4 2,970 / …）
- 單季負營收（3,217）與 NULL（2,669）列數（`營業收入合計`）
- 手算對帳 2330：2011 / 2012 / 2023 / 2024 四段逐季 ΣFY 逐位吻合；2024 與 view #5 concise 管線逐位相同
- EPS 差分示範（2330 2024 `基本每股盈餘合計` 累計 vs 單季）
- 確認 `換算匯率` 在本表不存在（0 列）→ view #5 該子 bug 不適用
- 確認 matview 鍵唯一（0 重複）
- 排除台股已知真實邊界：金融業負營收（本 view 僅減法）、營建業零營收、週六補行交易日、
  concise_* 無 market 欄（本表有 market 欄且僅 tw）
