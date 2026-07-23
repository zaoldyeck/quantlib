# B-matview-1_concise_balance_sheet_individual：財務定義與算式審查

**審查對象**：`src/main/resources/sql/materialized_view/1_concise_balance_sheet_individual.sql`
**結論（白話）**：**這張表可以信。** 它只是把資產負債表「攤平成一格一列」再去重，算式沒有錯。
最重要的一點：資產負債表是「某一天的存量快照」，本來就不該像損益表那樣把累計數前後相減——
這張 view **正確地不相減**，所以它沒有姊妹表（損益表 view）那種「累計數亂減、單季被放大一萬倍」
的病。現代年份（2013 以後）逐格正確，台積電 2024Q4「資產＝負債＋權益」對平到 0。

要記住三個邊界（都不是算錯，是語意陷阱）：
1. 名字叫 `individual`（個體），實際去重時偏向抓「合併（consolidated）」報表。對台股分析來說
   「取合併」其實是對的預設，但名字會誤導。
2. 2013 年（IFRS 上路）以前，有 27,563 個「公司×季」是合併＋個體兩種口徑混在一起拼出來的，
   跨口徑不對帳；但現役消費端只讀「兩種口徑都有」的頂層總額，所以實際沒中彈，且只限 2013 前。
3. 這張 view 沒有「財報公告日」欄位，源表又會用重編後的數字覆蓋原值 → 有殘留前視風險。

**實盤有沒有中彈**：沒有。現役 Python／實盤根本不讀這張 view（cache 直接讀原始表
`concise_balance_sheet` 且 `type='consolidated'`，`research/cache_tables.py:88-91`），這張 view
只餵給已凍結的 Scala 舊 view 鏈（`6_concise_financial_statement_with_titles`、`1_cbs_by_year`）。

---

## view 在做什麼（全文 11 行）

```sql
create materialized view concise_balance_sheet_individual as
select distinct on (market, year, quarter, company_code, title)
       id, market, year, quarter, company_code, company_name, title, value
from concise_balance_sheet
order by market, year, quarter, company_code, title, type;
```

- 來源 `concise_balance_sheet`：長表，一列 = 某公司某季某個資產負債表科目（`title`）的金額（`value`，
  單位＝千元）。`type ∈ {consolidated（合併）, individual（個體）}`，來自原始檔名尾碼
  `c`/`i`（`FinancialReader.scala:70-73, 84-87`）。
- `distinct on (market,year,quarter,company_code,title)` ＋ `order by …, type`：每個
  (市場,年,季,代號,科目) 只留一列，字母序 `consolidated < individual` → **恆取合併**。
- **沒有任何除法、沒有任何差分（lag/相減）**。只是去重＋挑口徑。

## 逐條對照學理

### 1. TTM／累計制差分 —— 正確地「不做」（OK）
台股**損益表**是累計數（Q3＝前三季合計），要還原單季必須相減；但**資產負債表是時點存量**
（季末當天的餘額），**不能相減**。這張 view 全文沒有 `lag`／減法，正是正確做法。
對照姊妹檔 `5_concise_income_statement_individual.sql:19-22` 才有
`value - lag(value) over (…)` 的單季還原——那是損益表該做、這裡不該做的事。
- 正面佐證：台積電 2330 2024Q4 透過 view 取數，資產總計 6,691,938,000
  ＝ 負債總計 2,368,362,135 ＋ 權益總計 4,323,575,865，**residual = 0**（完全對平）。

### 2. 合併 vs 個體 —— 恆取合併，且 2013 前會混口徑（SUSPECT）
- 決定性：unique index `(market,type,year,quarter,company_code,title)` 保證每個
  (市場,年,季,代號,科目) 至多 2 列，`order by …, type` 決定性取 `consolidated`。
  抽驗 1333/1336/1565 的 2006Q2「資產合計」：合併值與個體值不同時，view 一律回**合併值**。
- 「Frankenstein 混口徑」：去重是**逐科目**做的。若某公司某季合併與個體的科目集不完全重疊，
  拼出來的資產負債表就是「大部分科目來自合併、少數只有個體的科目來自個體」——跨口徑不對帳。
  全庫掃描：有雙口徑的「公司×季」共 27,661 個，其中 **27,661 個全部**至少有一個「只有個體
  才有」的科目（即混口徑）。**但幾乎全在 2013 前**：pre-2013 雙口徑 27,563、2013+ 只有 98。
  只有個體才有的科目共 18 個（如「股東權益」「基金及長期投資」及一堆銀行業科目）。
- 為什麼實務不中彈：現役消費端（`6_…with_titles`、`1_cbs_by_year`）只讀「兩口徑都有」的頂層
  總額（資產合計／流動資產／負債總計／流動負債／權益總額／保留盈餘），這些 title 兩邊都有 →
  一律取到合併值，那 18 個「個體專有科目」根本沒被讀到。

### 3. 分母保護／±inf —— N/A（OK）
這張 view **沒有任何除法**，不可能除以零或產生 ±inf。分母保護是下游（`1_cbs_by_year` 的
`roic`、`cash_ratio`）的責任，不在本檔。

### 4. 比率定義（ROE／ROIC／毛利率分母）—— N/A（OK）
本 view **不算任何比率**，只是資產負債表的攤平去重。這些檢查對本 artifact 不適用。

### 5. 時點對齊（PIT／前視）—— 殘留風險（SUSPECT）
- view 只帶 (year, quarter)，**沒有財報公告日**。把季末數字當成「季末當天就知道」＝前視。
  這是消費端責任（Scala 凍結策略用 `PublicationLag` 補公告落後）；本 view 是原始重組，不算它的錯，
  但需標註。
- 源表唯一索引不含版本欄，**重編（restatement）直接覆蓋原值**，歷史季度存的是「最新重編版」而非
  「當時已公告版」——與姊妹檔 `B-matview-5` 同一株 SUSPECT。

### 6. 下游消費端假設一致性（SUSPECT，屬 unit 6、pre-2013）
`6_concise_financial_statement_with_titles.sql:22-28` 的 `total_equity` 標題集同時列了多個並存
同義詞（`股東權益總計`／`股東權益合計`／`股東權益`／`權益總額`／`權益總計`）。本 view 是「每個
title 一列」，若同一公司季同時存在兩個以上這種同義 title，該 CTE 會回多列 → LEFT JOIN 扇出。
- 實測：透過 view，該標題集回 >1 列的「公司×季」＝ pre-2013 **287 個**、**2013+ 為 0**。
  例：2514/2855/1409（2012Q1）同時有「股東權益總計」與「股東權益合計」兩列。
- 歸屬：這是**消費端標題集過寬**造成的，非本 view 缺陷（本 view 忠實地一 title 一列）；列出供
  unit 6 修，並限 pre-2013。

## 材料性（能不能拿來選股）
- **2013+（所有現代回測與實盤）**：可信。乾淨單一口徑（合併），逐格對平。
- **pre-2013**：頂層總額仍取合併、可用；但整張表是混口徑拼裝，若消費端讀到「個體專有科目」或
  踩到同義詞扇出，會不一致——所有引用 Scala 時期 2004-2012 回測的結論需帶此 caveat（與
  `B-matview-5` 的結論一致）。
- **現役路徑不讀本 view**：`research/cache_tables.py` 直接快取原始表 `type='consolidated'`，
  grep 全 `research/` 對 `concise_balance_sheet_individual` 零引用。

## 查了什麼（可重跑）
- 精讀 `1_concise_balance_sheet_individual.sql` 全文；對照 `5_concise_income_statement_individual.sql`
  （差分邏輯）、`6_concise_financial_statement_with_titles.sql`、`view/1_cbs_by_year.sql`（消費端）。
- `FinancialReader.scala:63-112`（`type` 來自檔名 c/i；view 於匯入後 `refresh`）。
- PG：type 分佈（consolidated 2,142,171 / individual 1,339,539）、雙口徑 key 數（532,288）、
  Frankenstein 混口徑「公司×季」數（27,661，pre-2013 27,563 / 2013+ 98）、合併恆勝 spot-check、
  個體專有 18 科目、消費端扇出（pre-2013 287 / 2013+ 0）、台積電 2024Q4 footing residual=0。
- `research/cache_tables.py:88-91`（cache 讀原始表且 `type='consolidated'`，不讀本 view）。
