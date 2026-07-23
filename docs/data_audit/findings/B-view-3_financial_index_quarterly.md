# B-view-3_financial_index_quarterly — 財務定義與算式審查

**結論:🔴 BUG。這張表的分數不能拿來選股;2006-2012 段連「同一支查詢跑兩次會不會給同一個答案」都做不到。**

白話講六件事:

1. **2006-2012 的答案每次都不一樣。**同一支 SQL 連跑四次,`cbs` 總分得到四個不同數字
   (2,095,836.89 / 2,095,869.73 / 2,095,853.39 / 2,095,844.95)。原因是上游把 40 家公司
   (幾乎全是金控、銀行、證券)的同一個季度複製成 2 / 4 / 8 甚至 **64 份**,那段期間
   **每四列就有一列是幽靈列(11,040 / 47,446 = 23.3%)**。統一證 2855 的 2011Q4 有 64 列,
   `cbs` 從 50.32 到 58.06 都有,你查到哪一個純看運氣。

2. **沒有財務資料的公司反而拿高分。**排名用的是 PostgreSQL 預設的「空值排最後」,
   而排名越後面分數越高,所以 **ROIC 是空的 → 獲利能力拿 96.7 分(滿分 100)**。
   邦睿生技-創 6955 的 2023Q4 完全沒有 ROIC、沒有 ROA,總分照樣 86.56,排進前 10%。

3. **公司少報一季,系統就把「兩季合計」當成「一季」。**單季數字是用「本季累計減上季累計」
   算的,但程式沒有檢查「上一筆」是不是真的上一季。中租-KY 5871 的 2023Q2 整季在資料庫裡
   不見了,結果它的 2023Q3 營收變成 493 億(其實是 Q2+Q3 兩季);同一列的收現天數
   從鄰季的 2,632 天掉到 1,282 天,正好對半——因為分母被灌了兩倍。全庫 **8.7% 的營業現金流、
   3.5% 的營業收入**單季數是這樣算出來的。最誇張的一筆:龍邦 2514 的 2016Q4 拿 2012Q4 當基準,
   「單季營收」= 負 996 億。

4. **2005 年以前的「淨利」其實是稅前淨利。**同義詞清單把「稅前純益」列進了「本期稅後淨利」,
   結果 1995-2005 有 **96.6% 的列(29,026 / 30,040)`profit` 和 `ebit` 一模一樣**。
   台積電 2003 四季相加是 510 億(稅前),不是當年的稅後淨利 473 億。所以淨利率、ROIC、ROA
   在 2005/2006 有個假的斷層——那不是體質變化,是換了口徑。

5. **資不抵債的公司拿到資本結構滿分。**權益是負的 → 權益乘數是負的 → 排序時排在最前面 →
   反而拿 100 分。科風 3043 的 2018Q1 權益乘數 -245,資本結構 100 分。

6. **產業別是拿「今天的分類」貼到 20 年前的資料上。**彩晶 6116 的 2005 年被標成 2026 年 6 月
   的「光電業」。資本結構分數是按產業分組排名的,等於整個分數建立在當時不可能知道的資訊上。
   這條同時違反本專案 CLAUDE.md:323 的鐵律(產業別一律用 `industry_taxonomy_pit`)。

**好消息:底層數字本身是對的。**台積電 2024 四季營收 5,926 / 6,735 / 7,597 / 8,685 億元、
四季營業現金流相加 1.83 兆、Q4 毛利率 59.00% / 營益率 49.02% / 淨利率 43.12%,
全部和公告一致。**壞掉的是蓋在上面那一層排名與評分,不是原始的財報數字。**

**爆炸半徑:目前這張 view 沒有任何人在用**(Python 側已明文棄用、Scala 只用
`financial_index_ttm` / `growth_analysis_ttm`、PG 也沒有相依物件)。**但第 1、3、4 點的病灶
在同一條 matview 鏈上**,`4_financial_index_ttm.sql` 讀的是同一張
`concise_financial_statement_with_titles`,而那張 view **Scala 策略層還在用**
(`Signals.scala:142, 531, 549`)。

---

## 一、受測對象

| 項目 | 內容 |
|---|---|
| 檔案 | `src/main/resources/sql/view/3_financial_index_quarterly.sql`(173 行) |
| 型態 | 普通 VIEW(非 matview,每次查詢重算 170,503 列) |
| 上游 | `concise_financial_statement_with_titles`、`balance_sheet_with_titles`、`cash_flows_with_titles`、`operating_revenue` |
| 覆蓋 | 1989-2026Q1,170,503 列,twse + tpex |
| 建立 | `Task.createViewsAndMaterializedViews()`(`Task.scala:93-101`,fresh-install only) |
| 消費者 | **零**(見第五節) |

### 欄位算式全表(分子 / 分母 / 期間 / 單位)

來源三張表的 `value` 一律 **新台幣千元**;`eps` 為 元/股;所有比率無量綱。
損益與現金流已在上游做過「本季累計 − 上季累計」的單季化,資產負債表是季末時點值。

| 欄位 | 行 | 算式 | 期間 |
|---|---|---|---|
| `roic` | 13 | `profit / (total_assets − total_current_liabilities)` | 單季流量 ÷ 期末時點 |
| `roa` | 14-16 | `profit / ((total_assets + lag(total_assets)) / 2)` | 單季流量 ÷ 兩期平均 |
| `equity_multiplier` | 17 | `total_assets / total_equity` | 期末 |
| `current_ratio` | 18 | `total_current_assets / total_current_liabilities` | 期末 |
| `quick_ratio` | 19-20 | `(流動資產 − 存貨 − 預付) / 流動負債` | 期末 |
| `cash_ratio` | 21 | `cash / total_assets` ← **分母不是流動負債** | 期末 |
| `cash_flow_ratio` | 22-24 | `SUM(ocf, 4 列) / total_current_liabilities` | TTM ÷ 期末 |
| `cash_flow_adequacy_ratio` | 25-36 | `SUM(ocf,20 列) / −(SUM(capex,20)+SUM(存貨增加,20)+SUM(現金股利,20))` | 5 年 |
| `cash_flow_reinvestment_ratio` | 37-42 | `(SUM(ocf,4) + SUM(現金股利,4)) / (total_assets − 流動負債)` | TTM ÷ 期末 |
| `days_sales_outstanding` | 43-46 | `avg(應收) × 91.25 / total_operating_revenue` | 兩期平均 ÷ 單季 |
| `gross_margin` | 51-52 | `(營收 − 營業成本) / 營收` | 單季 |
| `total_assets_turnover` | 53-55 | `營收 / avg(total_assets)` | 單季 ÷ 兩期平均 |
| `profit_margin` | 57 | `profit / 營收` | 單季 |
| `operating_margin` | 58 | `net_operating_income / 營收` | 單季 |
| `days_sales_of_inventory` | 59-62 | `avg(存貨) × 91.25 / 營業成本` | 兩期平均 ÷ 單季 |
| `inventories_ratio` | 63 | `inventories / total_assets` ← **無 nullif** | 期末 |
| `receivables_ratio` | 64 | `receivable / total_assets` ← **無 nullif** | 期末 |
| `fcf_par_share` | 66 | `(ocf + capital_expense) / total_capital_stock` | 單季 ÷ 期末股本(金額) |
| `operating_performance` | 88 | `rank(roic ASC) / count(*) × 100` | 全市場百分位 |
| `return_on_investment` | 89 | `rank(roa ASC) / count(*) × 100` | 全市場百分位 |
| `capital_structure` | 90 | `rank(equity_multiplier DESC) / count(產業內) × 100` | 產業內百分位 |
| `liquidity` | 91-101 | 流動比分級(0/10/20/40) + 速動比分級(0/10/20/40/60) | 期末 |
| `cash_flow` | 102-130 | 現金比分級(0/5/10/20/30/40/50) + 三現金流條件(0/10/20) + DSO 分級(5~30) | 混合 |
| `cbs` | 139-143 | `0.25×operating_performance + 0.25×return_on_investment + 0.1×capital_structure + 0.1×liquidity + 0.3×cash_flow` | — |

權重加總 = 1.0,各分項皆 0-100,`cbs` 實測範圍 3.63 ~ 99.34,無 NULL / inf / NaN。**尺度沒問題。**

---

## 二、🔴 BUG(確認的錯誤)

### BUG 1 — 2006-2012 段每次查詢答案不同(23.3% 是幽靈列)

上游 `concise_financial_statement_with_titles` 對同一個 `(company_code, year, quarter)`
產生多列。原因:那支 matview 裡只有 `net_operating_income` 與 `profit` 兩個 CTE 加了
`distinct on`,其餘(`total_assets` / `total_current_assets` / `total_operating_revenue` /
`total_operating_costs` / `total_equity` / `eps` / `ebit` / `operating_expenses`)都沒有,
而它們的 title 同義詞清單彼此**不互斥**——公司報表同時出現「資產合計」與「資產總計」時,
該 CTE 就回 2 列,多個 CTE 一起 join 就變成 2ⁿ 的笛卡兒積。

**重現(統一證 2855, 2011Q4 = 2⁶ = 64 列):**
```sql
SELECT 'assets-like',  count(*) FROM concise_balance_sheet_individual
  WHERE company_code='2855' AND year=2011 AND quarter=4
    AND title IN ('資產合計','資產總計','資產總額')                       -- 2
UNION ALL SELECT 'equity-like', count(*) FROM concise_balance_sheet_individual
  WHERE company_code='2855' AND year=2011 AND quarter=4
    AND title IN ('權益總額','權益總計','股東權益總計','股東權益','股東權益合計'); -- 2
-- revenue-like / profit-like / eps-like 同樣各 2 → 乘起來 64
SELECT count(*) FROM concise_financial_statement_with_titles
  WHERE company_code='2855' AND year=2011 AND quarter=4;                  -- 64
```

**在本 view 的實際傷害(四條,前兩條是本單位新查出來的):**

(a) **非決定性**。`rows between N preceding` 的視窗在同分列之間沒有 tie-break,
    重複列的排列順序由執行計畫決定。同一支查詢跑四次:
```sql
SELECT round(sum(cash_flow_ratio)::numeric,6), round(sum(cbs)::numeric,6),
       round(sum(roa)::numeric,8)
FROM financial_index_quarterly WHERE year BETWEEN 2006 AND 2012;
-- run 1: 4567.608907 | 2095836.892489 | 363.23017959
-- run 2: 4567.608907 | 2095869.730101 | 362.94538079
-- run 3: 4567.608907 | 2095853.386060 | 362.84628680
-- run 4: 4567.608907 | 2095844.947168 | 360.90005704
```

(b) **同一格有多個並存的答案**。2855 2011 每一季都有 64 列、4 個相異 `roa`:
```sql
SELECT year,quarter,count(*) n,count(DISTINCT roa) d_roa,
       min(roa),max(roa),min(cbs),max(cbs)
FROM financial_index_quarterly WHERE company_code='2855' AND year=2011
GROUP BY 1,2 ORDER BY 2;
-- 2011Q4: n=64, d_roa=4, roa 0.00713~0.00859, cbs 50.32~58.06
```

(c) **排名分母被灌水**。`count(*) over (partition by year, quarter)`(行 84)是排名分母,
    2006-2012 每年 5,200~7,708 列裡含幽靈列;那七年 **11,040 / 47,446 = 23.3%** 是幽靈。
    整段期間的 `operating_performance` / `return_on_investment` 百分位全部失真。

(d) **TTM 視窗吃到同一季的複本**。`rows between 3 preceding and current row` 是「列」不是
    「季」,64 複本的公司其 4 列視窗可能全是同一季 → `cash_flow_ratio` 變成
    「同一季 × 4」而不是 TTM。

**範圍**:858 個 `(code, year, quarter)`、40 家公司、全部落在 2006-2012。
複本數分佈:2 份 413 組、4 份 180 組、8 份 118 組、64 份 147 組。
受影響代號(絕大多數是金控/銀行/證券):
`1409,1712,1718,2514,2801,2809,2812,2832,2836,2838,2845,2849,2855,2880,2881,2882,2883,2884,2885,2886,2887,2888,2889,2890,2891,2892,2897,2905,4707,5820,5880,6005,6015,6016,6020,6021,6023,6024,6026,9902`

**修法**:根因在 `materialized_view/6_concise_financial_statement_with_titles.sql`——
每個 title CTE 都要 `distinct on (year, quarter, company_code)` 並用**明確的 title 優先序**
(不是靠 `order by title` 的字典序)決定取哪一個。修完加一條守護測試:
`SELECT count(*) FROM (SELECT company_code,year,quarter FROM … GROUP BY 1,2,3 HAVING count(*)>1)` 必須為 0。
(與 `B-view-5_growth_analysis_ttm` 是同一個根因,同修一次即可。)

---

### BUG 2 — 缺資料的公司拿到接近滿分(NULL 被排到最好那一端)

行 80-81:`rank() over (partition by year, quarter order by roic)`。PostgreSQL 的
`ORDER BY x` 預設是 **ASC NULLS LAST**,所以 `roic` 是 NULL 的列排在最後 → 名次數字最大 →
行 88 的 `roic_rank / count_by_year * 100` ≈ 100 → **獲利能力拿到最高分**。`roa` 同理。

**重現:**
```sql
SELECT CASE WHEN roic IS NULL THEN 'roic NULL' ELSE 'roic ok' END g,
       count(*), avg(operating_performance), min(operating_performance)
FROM financial_index_quarterly WHERE year BETWEEN 2015 AND 2024 GROUP BY 1;
-- roic NULL : n=1671, avg=96.72, min=90.93
-- roic ok   : n=67264, avg=48.83, min=0.05
-- roa NULL  : n=510,  avg(return_on_investment)=96.41
```

**具體受害者**(2023 年,ROIC 與 ROA 皆為 NULL 卻拿高分):

| 代號 | 名稱 | 年季 | operating_performance | return_on_investment | cbs |
|---|---|---|---|---|---|
| 6955 | 邦睿生技-創 | 2023Q4 | 90.93 | 92.41 | **86.56** |
| 6739 | 竹陞科技 | 2023Q4 | 90.93 | 92.41 | 86.40 |
| 6961 | 旅天下 | 2023Q4 | 90.93 | 92.41 | 84.74 |
| 6785 | 昱展新藥 | 2023Q3 | 98.02 | 99.72 | 84.62 |

方向還相反地不一致:`capital_structure`(行 83)用 `order by equity_multiplier desc`,
DESC 預設是 **NULLS FIRST** → 名次 1 → 分數最低。實測 NULL 的 170 列平均只有 8.21 分。
**同一支 SQL 裡,缺資料在 A 分項是滿分、在 B 分項是零分。**

**修法**:排名要把 NULL 排除在外(`count(roic) over (…)` 當分母、NULL 列的該分項直接給 NULL),
並讓 `cbs` 在任一分項為 NULL 時整格為 NULL——現在 `cbs` **一列都沒有 NULL**
(範圍 3.63~99.34),缺料被無聲換成了看起來很有信心的分數。

---

### BUG 3 — 公司少報一季時,「兩季合計」被當成「一季」

單季化在 `materialized_view/5_concise_income_statement_individual.sql:18-22` 與
`3_cash_flows_individual.sql:8-12`:`case when quarter = 1 then value else value − lag(value)
over (partition by company_code, title order by year, quarter) end`。
**它只檢查「是不是 Q1」,沒有檢查「上一筆是不是上一季」。**季度缺漏時 `lag` 會跳到更早的期別,
差出來的數字橫跨多季,卻被貼上單季的標籤。

**全庫量測(非 Q1 列中,lag 不是相鄰季且不是序列首筆 → 產出靜默錯誤的數字):**
```sql
WITH x AS (SELECT market,company_code,title,year,quarter,
                  lag(year) OVER w ly, lag(quarter) OVER w lq
           FROM cash_flows_progressive
           WHERE title IN ('營業活動之淨現金流入（流出）','營業活動之淨現金流入(流出)')
           WINDOW w AS (PARTITION BY market,company_code,title ORDER BY year,quarter))
SELECT count(*) n_nonQ1,
       count(*) FILTER (WHERE ly IS NULL) n_first_row,
       count(*) FILTER (WHERE ly IS NOT NULL AND NOT (ly=year AND lq=quarter-1)) n_silently_wrong
FROM x WHERE quarter<>1;
-- OCF     : 89,556 非-Q1 列 → 7,822 靜默錯誤 (8.7%)
-- 營業收入 : 92,675 非-Q1 列 → 3,212 靜默錯誤 (3.5%)
-- 全 title : 現金流 896,602/4,989,633 (18.0%)、損益 164,097/2,155,251 (7.6%)
```

**具體案例 A — 中租-KY 5871,2023Q2 整季在來源表缺漏:**
```sql
SELECT year,quarter,title,value FROM concise_income_statement_progressive
WHERE company_code='5871' AND year=2023 AND type='consolidated' AND title='營業收入';
-- 2023 Q1 23,140,368 / Q3 72,461,883 / Q4 97,525,510   ← Q2 不存在
SELECT year,quarter,total_operating_revenue,profit,days_sales_outstanding
FROM financial_index_quarterly WHERE company_code='5871' AND year=2023;
-- Q1 23,140,368 | 7,210,323  | DSO 2632.0
-- Q3 49,321,515 | 12,945,806 | DSO 1282.3   ← 兩季合計貼成單季,DSO 正好對半
-- Q4 25,063,627 | 5,888,575  | DSO 2611.0
```
DSO 從 2,632 → 1,282 → 2,611,對半再彈回來——因為分母(單季營收)被灌了兩倍。
這是這條 bug 在輸出端留下的指紋。

**具體案例 B — 龍邦 2514,2016Q4 的 lag 落到 2012Q4(跨 4 年):**
單季營收 = 742,271 − 100,391,654 = **−99,649,383 千元**。

**下游指紋**:2013 年以後,`concise_financial_statement_with_titles` 有 **774 列單季營收為負**、
**739 列單季營業成本為負**;本 view 有 **344 列 `gross_margin > 1`**(數學上不可能,除非成本是負的)。
例:1453 大將 2021Q3 營收 114 千元、gross_margin 44.17。

**修法**:單季化改成
`case when quarter = 1 then value
      when lag(year) over w = year and lag(quarter) over w = quarter - 1 then value − lag(value) over w
      else null end`
——缺季就給 NULL,不要生一個看起來正常的錯數字。
**同類掃描**:Python 側的替代品 `src/quantlib/strat_lab/raw_quarterly.py:127-146, 176-179` 用
`shift(1).over(["company_code","year"], order_by="quarter")`,雖然不會跨年,但一樣是「位移一列」
不是「位移一季」,**缺季時有完全相同的 bug**;而那條路徑是現役研究/回測在用的。

---

### BUG 4 — 2005 年以前的 `profit` 其實是稅前淨利

`materialized_view/6_concise_financial_statement_with_titles.sql` 的 `profit` CTE 把
`'稅前純益'` 放進了「本期稅後淨利」的同義詞清單,而同檔的 `ebit` CTE **也**用同一個 title。

**重現:**
```sql
SELECT count(*) n, count(*) FILTER (WHERE profit = ebit) n_eq
FROM concise_financial_statement_with_titles WHERE year BETWEEN 1995 AND 2005;
-- 30,040 列中 29,026 列 (96.6%) profit 與 ebit 逐位相同

WITH p AS (SELECT DISTINCT ON (year,quarter,company_code) year,title
           FROM concise_income_statement_individual
           WHERE title IN ('本期稅後淨利（淨損）','本期淨利（淨損）','合併總損益',
                           '本期損益','本期淨利(淨損)','稅前純益')
           ORDER BY year,quarter,company_code,title)
SELECT title,count(*),min(year),max(year) FROM p GROUP BY 1 ORDER BY 2 DESC;
-- 本期淨利（淨損） 87,360 (2004-2026)
-- 稅前純益         32,735 (1989-2005)   ← 稅前被當稅後
-- 合併總損益       27,158 (2005-2014)   ← 舊制含少數股權
```

**對照台積電 2330 2003 年**:view 的四季 `profit` 相加 = 4,180,270 + 12,572,175 + 16,707,765
+ 17,568,065 = **51,028,275 千元**(≈ 510 億,等於當年**稅前**淨利),而 2003 年台積電
稅後淨利約 473 億。同期 `profit_margin` 30.4% 其實是稅前淨利率。

**影響**:`profit_margin` / `roic` / `roa` 在 2005/2006 有一個純屬口徑切換的假斷層。
任何跨這個切點的長期比較(成長趨勢、5 年前對比)都會把「稅前變稅後」讀成「獲利惡化」。

**修法**:從 `profit` 同義詞清單移除 `稅前純益`;舊期若真的只有稅前數就給 NULL,
不要用另一個概念頂替。`合併總損益` 需另行確認是否含少數股權,若含則不應直接當母公司淨利。

---

### BUG 5 — 資不抵債(負權益)的公司拿到資本結構滿分

行 17 `equity_multiplier = total_assets / total_equity`,權益為負時倍數為負;
行 83 用 `order by equity_multiplier desc`,負值排在最後 → 名次最大 → 行 90 得分 100。

```sql
SELECT CASE WHEN equity_multiplier IS NULL THEN 'NULL'
            WHEN equity_multiplier < 0 THEN 'NEGATIVE' ELSE 'ok' END g,
       count(*), min(capital_structure), max(capital_structure)
FROM financial_index_quarterly WHERE year BETWEEN 2015 AND 2024 GROUP BY 1;
-- NEGATIVE : n=8,   capital_structure 全部 = 100.00
-- ok       : n=68757, 0.48 ~ 100
-- NULL     : n=170, 0.50 ~ 12.50
```
實例:科風 3043 2018Q1 `equity_multiplier = −245.35` → `capital_structure = 100`;
新零售 3085 2020Q2 `−48.97` → 100;元隆 6287 2025Q1 `−48.52` → 100。

**修法**:排名前先擋 `total_equity <= 0`(給 NULL 或直接判 0 分),不要讓負值繞過大小比較。

---

### BUG 6 — 產業別拿「最新一筆」回貼全部歷史(前視,且違反專案鐵律)

行 2-6:
```sql
with industry as (select distinct on (company_code) company_code, industry
                  from operating_revenue where market='twse' or market='tpex'
                  order by company_code, year desc, month desc)
```
`year desc, month desc` = 取**最近一個月**的產業名,再貼到該公司 1989 年至今的每一季。
`capital_structure`(行 83)是 `partition by year, quarter, industry` 的產業內排名,
等於整個分項建立在決策當下不可能知道的分類上。

```sql
SELECT DISTINCT industry FROM financial_index_quarterly WHERE company_code='6116';
-- 光電業(只有一個值,含 2005/2015/2025 全部季度;來源是 2026-06 那筆)
WITH x AS (SELECT company_code,count(DISTINCT industry) n FROM operating_revenue
           WHERE market IN ('twse','tpex') GROUP BY 1)
SELECT count(*) FILTER (WHERE n>1), count(*) FROM x;
-- 1,339 / 2,502 = 53.5% 的代號歷史上出現過一個以上的產業名
```
另有 458 列 `industry IS NULL`,它們會自成一個「NULL 產業」分組一起排名。

**這條同時違反 `CLAUDE.md:323` 的明文鐵律**:「產業別一律用 `industry_taxonomy_pit`……
**禁止**直接用 `operating_revenue.industry`(舊檔含 legacy 名稱且無 PIT 語義)」。

**修法**:改用 `industry_taxonomy_pit` 的 asof 查法(`effective_date <= 該季末` 取最新一筆)。

---

### BUG 7 — TTM 視窗算的是「列」不是「季」

行 23 / 26 / 28 / 31 / 34 / 38 / 40 的 `rows between 3 (或 19) preceding and current row`
都是列位移。三種失效:

(a) **序列開頭不足 4 季照樣輸出 TTM**:
```sql
WITH x AS (SELECT count(ocf) OVER (PARTITION BY company_code ORDER BY year,quarter
                                   ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) n,
                  cash_flow_ratio FROM financial_index_quarterly)
SELECT n, count(*), count(*) FILTER (WHERE cash_flow_ratio IS NOT NULL) FROM x GROUP BY 1 ORDER BY 1;
-- n=1: 2,063 列 → 2,036 列仍給出 cash_flow_ratio(只有 1 季 OCF 當 TTM,低估 75%)
-- n=2: 2,022 → 1,995   n=3: 2,147 → 2,112   n=4: 102,804 → 100,543
```
共 **6,143 列**的「TTM」其實只有 1-3 季,而 `cash_flow_ratio > 1` 是評分條件之一。

(b) **缺季時視窗跨越更長期間**:
```sql
WITH x AS (SELECT company_code,year,quarter,lag(year) OVER w ly,lag(quarter) OVER w lq
           FROM financial_index_quarterly
           WINDOW w AS (PARTITION BY company_code ORDER BY year,quarter))
SELECT count(*), count(*) FILTER (WHERE NOT ((ly=year AND lq=quarter-1)
                                          OR (quarter=1 AND ly=year-1 AND lq=4)))
FROM x WHERE ly IS NOT NULL;
-- 168,439 列中 17,099 列 (10.2%) 與前一列不相鄰
```
所以「20 季」的 `cash_flow_adequacy_ratio` 實際可能橫跨 25 季以上;
`roa` 的兩期平均資產、DSO 的兩期平均應收、DSI 的兩期平均存貨也全部配錯期。

(c) 搭配 BUG 1 的複本,視窗可能整個裝的是同一季。

**修法**:視窗改用季別序號(`year*4+quarter`)的 `RANGE BETWEEN 3 PRECEDING`,並加
`count(ocf) over (同視窗) = 4` 的完整性守衛,不足就給 NULL。

---

## 三、🟡 SUSPECT(定義偏離或未爆彈)

### SUSPECT 8 — 產業分桶太小,`capital_structure` 百分位退化成常數

`rank / count_by_year_industry × 100`,桶內只有 1 家時必定是 `1/1×100 = 100`。
```sql
WITH x AS (SELECT year,quarter,industry,count(*) n FROM financial_index_quarterly GROUP BY 1,2,3)
SELECT n,count(*) FROM x WHERE n<=3 GROUP BY 1 ORDER BY 1;
-- 1 家: 193 桶   2 家: 135 桶   3 家: 108 桶
```
193 個桶裡的獨家公司自動拿資本結構滿分。**修法**:桶內家數低於門檻時該分項給 NULL,
或退回全市場百分位。

### SUSPECT 9 — `roic` / `roa` 是「單季」不是年化;`roic` 分子分母不匹配

分子是單季流量、分母是期末時點,所以數字約為年度值的 1/4。
```sql
SELECT year,quarter,roic,roa FROM financial_index_quarterly WHERE company_code='2330' AND year=2024;
-- Q4 roic 0.0690 = 374,468,888 / (6,691,938,000 − 1,264,524,964) 手算一致
-- 全市場 2015+ 中位數 roic = 0.0139、P99 = 0.1252 → 明顯是季頻
```
台積電真實年化 ROIC 約 27%,view 給 6.9%。**在同一個 (year, quarter) 內做橫斷面排名沒問題**
(所有公司期間長度相同),但任何人把 `roic` 當年度數用門檻篩(例如 `roic > 0.15`)會全軍覆沒。

學理上還有第二層:ROIC 的分母 `總資產 − 流動負債` 是「債 + 權益」的投入資本,分子卻是
**已扣利息與稅**的淨利(股東報酬),應為 NOPAT(= 營業利益 ×(1−稅率))。高槓桿公司會被系統性低估。
`roa` 用淨利 ÷ 平均資產是可接受的基本定義。

**修法**:分子改 TTM(視窗已有現成寫法)或明確乘 4 並在欄名標注 `_annualized`;
ROIC 分子改 NOPAT。

### SUSPECT 10 — `fcf_par_share` 名不符實,差整整 10 倍

行 66 分母是 `total_capital_stock`,那是**股本金額**(千元)不是股數。台股面額 10 元,
股數 = 股本 ÷ 10,所以這個欄位 = 每股 FCF ÷ 10。
```sql
-- 2330 2024Q4: (620,205,283 + (−361,948,162)) / 259,327,332 = 0.9959
-- 真實單季每股自由現金流 = 258,257,121 千元 × 1000 / 25.93 億股 ≈ NT$9.96
```
而且分子是**單季**不是年度。任何把它當「元/股」設門檻的地方會差一個數量級。
(`4_financial_index_ttm.sql:95-99` 的 `fcf_per_share` 是同一個坑,已由
`B-view-5_growth_analysis_ttm` 記過。)

### SUSPECT 11 — `cash_ratio` 分母換了,分級門檻沒重新量測

教科書現金比率 = 現金及約當現金 **÷ 流動負債**;行 21 用的是 **÷ 總資產**。
換了分母,行 103-108 的 0.25 / 0.2 / 0.15 / 0.1 / 0.05 五段門檻(占 `cash_flow` 分項 50 分、
`cbs` 權重 0.3)卻沒有對應的出處。這是無出處的魔術數字——要嘛去量出來,要嘛刪掉。

### SUSPECT 12 — `operating_margin` 混進了「稅前淨利」

`net_operating_income` CTE 同時收 `營業利益（損失）`(營業利益)與
`繼續營業單位稅前損益 / 稅前淨利`(含業外的稅前利益),是兩個不同科目。
```sql
-- distinct-on 實際選中稅前類 title 的列數:
-- 繼續營業單位稅前損益 940 + 稅前淨利（淨損）738 + 稅前合併淨利 570 + 稅前淨利(淨損) 107 = 2,355 列
```
那 2,355 列的 `operating_margin` 其實是稅前利益率。而且 `distinct on … order by …, title` 是
靠**中文字典序**決定取哪個,不是靠語意優先序——換個 collation 結果會變。

### SUSPECT 13 — 2013 年以前合併/個體報表混用

`concise_balance_sheet_individual` / `concise_income_statement_individual` 用
`distinct on (…) order by …, type`,`type` 字母序 `consolidated < individual` → 有合併取合併、
只有個體才用個體。
```sql
WITH pick AS (SELECT DISTINCT ON (market,year,quarter,company_code,title) year,type
              FROM concise_balance_sheet ORDER BY market,year,quarter,company_code,title,type)
SELECT year,type,count(*) FROM pick WHERE year BETWEEN 2010 AND 2016 GROUP BY 1,2 ORDER BY 1,2;
-- 2010: consolidated 106,560 / individual 27,622 (20.6%)
-- 2011: 111,505 / 28,444    2012: 115,863 / 29,114
-- 2013: 99,181 / 722        2014: 107,565 / 474      2015+: 全部 consolidated
```
IFRS 2013 改以合併為主,所以 2013 前約 20% 的格子是個體(母公司)口徑。
跨 2013 的時序比較混了兩種報表範圍。**這是 fallback 的必然結果,不是算錯**,
但欄位沒有任何標記說「這一格是哪種報表」。

**修法**:輸出加一個 `statement_type` 欄位,讓消費者能自己決定要不要混用。

### SUSPECT 14 — 除零未爆彈:`inventories_ratio` / `receivables_ratio` 沒包 `nullif`

行 63-64 是全檔唯二沒有 `nullif(分母, 0)` 的除法(其餘十幾處都有)。
PostgreSQL 的 float8 除以 0 會直接 raise:
```sql
SELECT 1.0::float8 / 0.0::float8;   -- ERROR: division by zero
SELECT count(*) FROM concise_financial_statement_with_titles WHERE total_assets = 0;  -- 7
-- 1718(2006Q2 ×2)、910801(2008Q4/2008Q2)、3990(2011Q4)、910708(2010Q2)、1258(2012Q4)
```
目前僥倖沒炸,是因為這 7 格的 `inventories` / `receivable` 恰好都是 NULL(NULL/0 回 NULL)。
只要日後補進其中任一筆存貨或應收,**整支 view 會直接報錯**。
(`4_financial_index_ttm.sql:91-92` 是同一個未爆彈,已由 `B-view-5_growth_analysis_ttm` 記過。)

---

## 四、🟢 OK / ⚪ REAL(查過沒問題,別再查一次)

1. **⚪ 單季化(累計制差分)在資料完整時是對的,跨年邊界也對。**
   台股損益表是當年累計數,`case when quarter=1 then value else value − lag(value)` 的
   結構正確。實測 2330 2024:
   | 季 | 累計營收 | view 單季營收 | 公告單季 |
   |---|---|---|---|
   | Q1 | 592,644,201 | 592,644,201 | 5,926 億 ✓ |
   | Q2 | 1,266,154,378 | 673,510,177 | 6,735 億 ✓ |
   | Q3 | 2,025,846,521 | 759,692,143 | 7,597 億 ✓ |
   | Q4 | 2,894,307,699 | 868,461,178 | 8,685 億 ✓ |

   EPS 8.70 / 9.55 / 12.55 / 14.45(公告 8.70 / 9.56 / 12.54 / 14.45,差異僅差分捨入)。
   現金流同樣正確:四季 OCF 相加 = 436,311,108 + 377,668,210 + 391,992,467 + 620,205,283
   = **1,826,177,068 千元 = 公告全年 1.83 兆**。
   壞的只有 BUG 3 的「缺季」路徑,不是差分本身。

2. **🟢 三個利潤率算式正確,與公告完全對得上。**2330 2024Q4:
   `gross_margin` (868,461,178 − 356,083,027)/868,461,178 = **0.5900**(公告 59.0%);
   `operating_margin` 425,712,913/868,461,178 = **0.4902**(公告 49.0%);
   `profit_margin` 374,468,888/868,461,178 = **0.4312**(公告 43.1%)。

3. **🟢 DSO / DSI 的 91.25 天(= 365/4)與單季口徑一致,不是季節性偏誤。**
   一度懷疑「累計制營收 + 固定 91.25 天 → Q4 的 DSO 只有 Q1 的 1/4」,查證後上游已單季化,
   所以沒有這個問題。2330 2024Q4 DSO 27.4 天、DSI 74.4 天,皆為合理值
   (手算 ((272,087,959+250,048,429)/2)×91.25/868,461,178 = 27.43 ✓)。

4. **🟢 現金流量允當比率與現金再投資比率的定義推導正確。**
   允當比率 = 最近 20 季 OCF ÷(資本支出 + 存貨增加 + 現金股利),與教科書一致;
   `capital_expense` / `increase_in_inventories` / `cash_dividends_paid` 在來源都是負值,
   所以行 27 的 `−(a+b+c)` 是對的。再投資比率的分母 `total_assets − total_current_liabilities`
   **恰好等於**「固定資產 + 長期投資 + 其他資產 + 營運資金」
   (= (總資產 − 流動資產) + (流動資產 − 流動負債)),推導無誤。
   2330 2024Q4 允當比率 1.097、現金流量比率 1.444(手算 1,826,177,068/1,264,524,964 ✓)。

5. **🟢 `cash_dividends_paid` 只有 15,585/113,488 列(13.7%)是正常的,不是缺料。**
   一度懷疑覆蓋率太低導致允當比率分母漏項。查證:單季化後,年配息公司一年只會留下 1 列
   (Q1/Q2 = 0、Q3 = −D、Q4 = −D−(−D) = 0,再被 `value < 0` 濾掉),13.7% 正是預期值。
   實測 1301 台塑 2022-2024 的三筆年度配息都完整保留(−52,199,748 / −26,736,696 / −6,333,176)。

6. **🟢 評分尺度一致,加權正確。**權重 0.25+0.25+0.1+0.1+0.3 = 1.0;五個分項皆 0-100;
   `cbs` 實測 3.63 ~ 99.34。四則運算沒問題——問題全在 NULL 語意與上游資料。

7. **🟢 `market = 'tw'` 的 join 條件是對的,不是打錯字。**
   行 68 / 72 看起來像 bug(`cfswt.market` 是 `'twse'`/`'tpex'`),實測
   `balance_sheet` / `cash_flows_progressive` 全庫確實只有 `'tw'` 一個值
   (`balance_sheet_with_titles` 116,455 列全 `tw`、`cash_flows_with_titles` 113,488 列全 `tw`)。
   兩張表沒有市場區分,join 正常命中。

8. **🟢 沒有 ±Infinity / NaN 產出。**除了 SUSPECT 14 的兩處,所有除法都包了 `nullif`。
   極端值仍在(`roic > 1` 14 列、`|equity_multiplier| > 100` 47 列、`DSI > 10 年` 1,670 列),
   但那是分母極小的性質問題,不是溢位。

9. **🟢 matview 不是舊的。**`concise_financial_statement_with_titles` /
   `balance_sheet_with_titles` / `cash_flows_with_titles` / base tables 的
   `max(year, quarter)` 全部是 **2026Q1**,與 view 一致。

10. **🟢 PIT:view 只帶 (year, quarter) 沒有公告日,這是設計而非缺陷。**
    公告落後由消費端處理——`Signals.scala:145-158` 的 `latestQuarterField` 用
    `PublicationLag.asOfQuarter(asOf)` 換算可用季別再查。本 view 目前沒有消費者,所以
    這一條是「未來若要用必須自帶 lag」的備註,不是 bug。

---

## 五、爆炸半徑:目前零消費者,但病灶會外溢

**這張 view 沒有任何人在用:**

| 路徑 | 狀態 | 證據 |
|---|---|---|
| PostgreSQL 相依物件 | 無 | `pg_depend` × `pg_rewrite` 查詢回 0 列 |
| DuckDB cache | 已移除 | `research/cache_tables.py:78` 註解:「REMOVED: financial_index_quarterly (was a PG VIEW with margin derivation we couldn't fully verify)」 |
| Python 研究層 | 明文棄用 | `src/quantlib/strat_lab/raw_quarterly.py:10`:「We DELIBERATELY do NOT use … financial_index_quarterly (VIEW with margin computation we can't fully verify)」 |
| Scala 策略層 | 不使用本表 | `Signals.scala` 只查 `financial_index_ttm` / `growth_analysis_ttm` |

**但病灶不只活在這裡。**`4_financial_index_ttm.sql:100` 讀的是**同一張**
`concise_financial_statement_with_titles`,所以 **BUG 1(重複列 / 非決定性)、BUG 3(缺季單季化)、
BUG 4(稅前當稅後)、SUSPECT 13(合併個體混用)全部原封不動繼承過去**,而
`financial_index_ttm` **Scala 策略層還在用**(`Signals.scala:142, 531, 549`)。

另外 **BUG 3 的同類缺陷存在於現役 Python 路徑**:
`src/quantlib/strat_lab/raw_quarterly.py:127-146, 176-179` 的
`shift(1).over(["company_code","year"], order_by="quarter")` 同樣是位移一列而非一季,
缺季時會產出相同的「兩季當一季」。這條要一起修。

---

## 六、修法優先序

| 序 | 對象 | 動作 |
|---|---|---|
| 1 | `materialized_view/6_concise_financial_statement_with_titles.sql` | 每個 title CTE 加 `distinct on (year,quarter,company_code)` + **明確 title 優先序**;加「零重複」守護測試。同時修好 BUG 1 與部分 BUG 4 的取值不確定性 |
| 2 | `materialized_view/5_…_individual.sql`、`3_cash_flows_individual.sql`、`src/quantlib/strat_lab/raw_quarterly.py` | 單季化加「上一筆必須是相鄰季」守衛,缺季給 NULL |
| 3 | 同 1 | `profit` 同義詞清單移除 `稅前純益`;`net_operating_income` 移除稅前類 title |
| 4 | `view/3_…quarterly.sql` 行 80-90 | 排名排除 NULL(`count(col) over` 當分母)、負權益先擋、任一分項 NULL 時 `cbs` 給 NULL |
| 5 | 同上 行 2-6 | 產業別改 `industry_taxonomy_pit` asof |
| 6 | 同上 行 22-42 | TTM 視窗改季別 `RANGE`,加完整性守衛 |
| 7 | 同上 行 13-16, 63-66 | `roic` 改 NOPAT/TTM、補 `nullif(total_assets,0)`、`fcf_par_share` 改名或 ×10 |

**或者**——這張 view 已經沒有消費者、Python 側也已經用 `raw_quarterly.py` 從原始表重建過,
最省的做法是 **DROP 掉它**,只把上游 matview 的 BUG 1 / 3 / 4 修好(因為
`financial_index_ttm` 還在吃)。留著一張沒人用又算錯的 view,只會等著下一個人誤用。
