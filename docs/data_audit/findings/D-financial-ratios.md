# D-financial-ratios — 財務比率 view(ROIC / ROE / margins / 週轉)

判定:**🔴 BUG**(兩個確定的定義錯誤 + 三個 SUSPECT;其餘比率學理正確)
稽核日:2026-07-23
範圍:`src/main/resources/sql/view/3_financial_index_quarterly.sql`(季頻)
+ `src/main/resources/sql/view/4_financial_index_ttm.sql`(TTM)

---

## 一句話結論

**這兩張表的比率絕大多數算對——毛利率、淨利率、營業利益率、流動比、速動比、
收現天數、存貨週轉天數、三個現金流量比率全部符合學理。但有兩個確定的定義錯誤:
(1) TTM 版的「總資產週轉率」平均資產分母錯用「5 季前」的數字(同一支 SQL 的 ROA
用「4 季前」,是打字錯);(2) 兩張表的「現金比率」分母用的是總資產,不是流動負債
——它算的其實是「現金佔資產比」,不是教科書的現金比率。另外 ROIC 分子用稅後淨利
(學理該用 NOPAT)、負權益公司排名會反轉成「最佳」、每股自由現金流除的是股本不是
股數。這些都只影響研究結論與 CBS 評分,不在實盤下單路徑上。**

能不能信:
- 毛利率 / 淨利率 / 營業利益率 / 流動比 / 速動比 / DSO / DSI / 三現金流量比率 → **可信**。
- 總資產週轉率(TTM)→ **不可信**,分母錯一季(季頻版沒錯)。
- 現金比率 → 名不副實,是「現金/總資產」不是「現金/流動負債」。
- ROIC → 是「稅後淨利 / 資本運用額」的粗略版,不是教科書 ROIC。
- 涉及負權益公司的 CBS `capital_structure` 分數 → 反向,少數危機股會拿到最佳分。

> 註:本單位的 BUG-turnover 與 cash_ratio/roic/負權益 已被姊妹單位
> `B-fscore-academic`(BUG-3、SEC-2)因 F-Score 消費這些欄位而部分記錄。本單位是
> 這兩張表**逐一比率**的完整學理對照,補齊季頻版與 TTM 版的全部算式、每股自由
> 現金流的面額問題、以及 ROA 與 `raw_quarterly.py` 的定義分歧。

---

## 先確立來源粒度(整個稽核的地基)

`concise_financial_statement_with_titles`(cfswt)的損益/現金流欄位來自
`concise_income_statement_individual` 與 `cash_flows_individual`,經實測是**單季值**
(不是當年累計 YTD):

- 2330 損益表 2024 各季營收 = 592 / 673 / 759 / 868(億千元),加總 = 全年 ~2.89 兆 ✓
- 2330 現金流 2024 各季 OCF = 436 / 377 / 392 / 620,加總 = 全年 ~1.83 兆 ✓

**所以 TTM 版(view 4)對 4 季做 `sum(... rows between 3 preceding and current row)`
是正確的**(單季 × 4 = 一年),不是把 YTD 累計數重複相加。這一點若搞錯整張 TTM
表就全毀,故先釘死。資產負債表欄位(total_assets 等)是期末時點值,亦正確。

---

## 確認的錯誤(BUG)

### BUG-1 🔴 總資產週轉率(TTM)的平均資產分母錯用「5 季前」

`4_financial_index_ttm.sql:69`:

```sql
sum(rev, 4Q) / ((total_assets + lag(total_assets, 5) over (...)) / 2)
```

**學理**:總資產週轉率 = 營收 / 平均總資產。對「截至 Qt 的 TTM」,平均資產
= (期末 TA_t + 期初)/2,而期初 = TTM 窗第一季(Q_{t-3})的期初 = Q_{t-4} 的期末
= `lag(total_assets, 4)`。

**證據(off-by-one 打字錯)**:同一支 SQL 裡 `roa`(:18)用 `lag(total_assets,4)`、
`days_sales_outstanding`(:48)用 `lag(receivable,4)`、`days_sales_of_inventory`
(:85)用 `lag(inventories,4)`——全部是「4」代表去年同期,唯獨週轉率是「5」。

數字重現(2330,2024Q4):

| 算法 | 期初資產(千元)| 週轉率 |
|---|---|---|
| view 值(lag 5 = 2023Q3)| 5,484,556,381 | **0.47540** |
| 正確(lag 4 = 2023Q4)| 5,532,371,215 | 0.47353 |

用 lag5 手算 `2,894,307,699 / ((6,691,938,000 + 5,484,556,381)/2) = 0.47540`,與
view 完全吻合,證實它用的就是 5 季前。台積電資產平滑,差 ~0.4%;但併購/現增讓
資產跳動的公司,期初差一整季可讓週轉率偏差數十 %。

**修法**:`lag(total_assets, 5)` → `lag(total_assets, 4)`。修完所有
`total_assets_turnover_*_5y_*` 旗標與引用此欄的因子掃描要重跑。

### BUG-2 🔴 現金比率的分母是「總資產」不是「流動負債」(兩張表都有)

`3_financial_index_quarterly.sql:21` 與 `4_financial_index_ttm.sql:26`:

```sql
cash / nullif(total_assets, 0) as cash_ratio
```

**學理**:現金比率 Cash Ratio =(現金 + 約當現金)/**流動負債**(Ross/Westerfield;
最嚴格的短期償債能力指標)。分母是流動負債。

**證據**:用總資產當分母,算出來的是「現金佔總資產比(現金強度)」,與現金比率
差一個量級:

| 個股(2024Q4)| impl `cash/TA` | 教科書 `cash/CL` |
|---|---|---|
| 2330 | 0.318 | 1.683 |
| 2603 | 0.277 | 1.685 |
| 2412 | 0.068 | 0.454 |
| 2317 | 0.213 | 0.431 |
| 1301 | 0.037 | 0.156 |

CBS 的 liquidity/cash 分級門檻(`cash_ratio > 0.25 → 50 分`、0.2、0.15…)是照
`cash/TA` 的尺度校準的——若換成真現金比率(TSMC 1.68),幾乎所有現金充裕股都會
頂到最高分級、失去鑑別力。這反證了門檻是把它當「現金/總資產」在用。它餵入
`liquidity` 與 `cash_flow` 分級 → 影響 `cbs`。

**修法**:分母改 `total_current_liabilities` 並重新校準分級門檻;若刻意要「現金
強度」這個構念,把欄位更名為 `cash_to_assets_ratio`,別叫 `cash_ratio`。

---

## 學理偏差但影響有限(SUSPECT)

### SEC-1 🟡 ROIC 分子用稅後淨利(該用 NOPAT),分母含閒置現金

`3_…:13` 與 `4_…:13-15`:`profit / (total_assets − total_current_liabilities)`。

**學理**:ROIC = NOPAT / 投入資本(Koller《Valuation》;Damodaran)。
NOPAT = 稅後營業利益 = EBIT×(1−t),**刻意排除利息與業外損益**,才能衡量與資本
結構無關的營運報酬;投入資本常再扣閒置現金。

**實作**:分子 = `profit`(本期淨利,稅後、已含業外與利息,經查 2330 = 「本期淨利
（淨損）」);分母 = 資本運用額(TA − 流動負債 = 權益 + 非流動負債,是標準 ROCE
分母、可接受),但**未扣閒置現金**。

**證據(分子口徑會改變排名)**:跨產業 2024Q4 的「淨利 / 營業利益」:

| 個股 | 淨利/營業利益 |
|---|---|
| 2882 國泰金 | 0.858 |
| 2330 台積電 | 0.880 |
| 2603 長榮 | 0.926 |
| 1216 統一 | 1.065 |
| 3008 大立光 | 1.071 |

用淨利取代營業利益基礎的 NOPAT,會讓橫斷面 `roic_rank` 因各公司業外/融資結構
不同而位移達 ±14%。分母含現金(TSMC 現金佔 TA 0.32)→ 現金多的公司投入資本被
灌大、ROIC 被低估。影響有限:只餵 `roic_rank → operating_performance`(佔 cbs 25%)。

**修法**:分子改 `net_operating_income × (1−有效稅率)`(NOPAT)或至少
`net_operating_income`;嚴謹版再從投入資本扣閒置現金。若維持現口徑,更名 `roce`。

### SEC-2 🟡 負權益 / 負資本運用額沒擋 → 危機股排名反轉成「最佳」

`4_…:22`(equity_multiplier)、`:116`(rank order by equity_multiplier desc)、
`:13-15`(roic 分母)只用 `nullif(x, 0)` 擋「剛好 0」,不擋負數。

**後果**:負權益公司 → 權益乘數為負 → `order by equity_multiplier desc` 把它排到
最後 → `capital_structure` 百分位 ≈ 100(**最佳**)。負資本運用額 → ROIC 符號反轉。

**證據**:全史 170,503 列中,`total_equity < 0` 有 13 列、`(TA − CL) < 0` 有 176 列。
實例 3043(2018Q1)權益乘數 = −245.35,卻拿到 `capital_structure = 100`;6287
2025Q1、6497 2019Q4 同類。`raw_quarterly.py` 一律用 `pl.when(...>0)` 護欄
(`total_assets_begin > 0`、`current_liabilities > 0`),view 只用 `nullif(≠0)`
→ 與已修版本定義不一致。罕見(~0.1%),只扭曲少數財務危機股的 cbs。

**修法**:排名前把 `total_equity <= 0` 與 `(TA − CL) <= 0` 設 NULL(比照
`raw_quarterly.py` 的 `>0` 護欄)。

### SEC-3 🟡 每股自由現金流除的是「股本」不是「股數」

`view3:66` `fcf_par_share`、`view4:99` `fcf_per_share`:
`(ocf + capital_expense) / total_capital_stock`。

**學理**:每股自由現金流 =(OCF − 資本支出)/ 流通股數;台股股數 = 股本/面額(10)。

**實作**:分母是股本(面額計價的資本額,千元)而非股數 → 結果 = 真每股 FCF ÷ 10。
2330 股本 259,327,332 千元 → 股數 ≈ 259.3 億股,`FCF/股本` 相對真 NT$/股 低估約 10×。
同表的 `eps` 是真每股(取自申報)→ 單位不一致。`view4` 命名 `fcf_per_share`(宣稱
NT$/股)卻是每「元面額」,`view3` 的 `fcf_par_share` 命名有含糊帶過。此欄不進 cbs
評分,低嚴重度。

**修法**:分母改 `total_capital_stock / 10` 得 NT$/股;或更名 `fcf_per_par_dollar`。

---

## 定義註記(非 bug,但與 raw_quarterly.py 不一致)

### ROA:view 用「平均資產」,raw_quarterly.py 用「期初資產」

`3_…:14-16`(單季淨利 / avg(TA_t, TA_{t-1}))、`4_…:16-21`(TTM 淨利 /
avg(TA_t, TA_{t-4}))用**平均**資產,是主流 ROA 定義(Damodaran)。
`raw_quarterly.py` 的 `roa_ttm = ni_ttm / total_assets_begin`(shift4,**期初**)遵
Piotroski (2000)。兩者皆合法教科書定義,但屬**不同標準**,不會逐位相同——消費端
若假設兩邊 parity 會誤判。view4 的 ROA 期別(lag4)本身正確,不像週轉率那個 lag5。

---

## 逐一比率清單(範圍內每個算式都列)

| 比率 | 位置(view3 / view4)| 學理 | 判定 |
|---|---|---|---|
| roic | :13 / :13-15 | NOPAT/投入資本 | 🟡 分子用淨利、含現金(SEC-1)|
| roa | :14-16 / :16-21 | 淨利/平均資產 | 🟢 正確(但用平均,異於 raw 的期初)|
| equity_multiplier | :17 / :22 | 總資產/權益 | 🟢 正權益正確;負權益反轉(SEC-2)|
| current_ratio | :18 / :23 | 流動資產/流動負債 | 🟢 正確 |
| quick_ratio | :19-20 / :24-25 | (流動資產−存貨−預付)/流動負債 | 🟢 正確 |
| **cash_ratio** | **:21 / :26** | **現金/流動負債** | **🔴 分母用總資產(BUG-2)** |
| cash_flow_ratio | :22-24 / :27-29 | 營業現金流/流動負債 | 🟢 正確(TTM 流量/期末 CL)|
| cash_flow_adequacy_ratio | :25-36 / :30-41 | 5 年 OCF / 5 年(capex+存貨增+股利)| 🟢 正確(官方定義、符號對)|
| cash_flow_reinvestment_ratio | :37-42 / :42-47 | (OCF−股利)/(固定+長投+其他+營運資金)| 🟢 正確(≈ TA−CL)|
| days_sales_outstanding | :43-46 / :48-53 | 平均應收/營收×天數 | 🟢 正確(91.25/365 日曆天)|
| gross_margin | :51-52 / :60-66 | (營收−營業成本)/營收 | 🟢 正確(0.5612 交叉驗證)|
| **total_assets_turnover** | :53-55 / **:67-72** | 營收/平均資產 | view3 🟢(lag1)/ **view4 🔴 lag5(BUG-1)** |
| profit_margin | :57 / :75-79 | 淨利/營收 | 🟢 正確 |
| operating_margin | :58 / :80-84 | 營業利益/營收 | 🟢 正確(少數公司退回稅前,上游)|
| days_sales_of_inventory | :59-62 / :85-90 | 平均存貨/營業成本×天數 | 🟢 正確(分母用 COGS)|
| inventories_ratio | :63 / :91 | 存貨/總資產(組成比)| 🟢(缺 nullif,微瑕)|
| receivables_ratio | :64 / :92 | 應收/總資產(組成比)| 🟢(缺 nullif,微瑕)|
| eps | :65 / :93-94 | 每股盈餘 | 🟢(view4 = 4 季加總 TTM EPS,標準)|
| **fcf_par_share/fcf_per_share** | :66 / :99 | FCF/股數 | 🟡 除股本非股數(SEC-3)|
| cbs 綜合分/百分位/分級 | :79-148 / :112-181 | 專案自訂啟發式,非教科書比率 | 🟢 非學理範圍(方向自洽)|

---

## 建議修法(不在本單位執行)

依嚴重度排序:

1. **BUG-1**:`4_financial_index_ttm.sql:69` `lag(total_assets, 5)` → `lag(total_assets, 4)`。
2. **BUG-2**:兩張表 `cash_ratio` 分母 `total_assets` → `total_current_liabilities`
   並重新校準 CBS 的 liquidity/cash 分級門檻;或更名 `cash_to_assets_ratio`。
3. **SEC-1**:ROIC 分子改 `net_operating_income × (1−有效稅率)`(NOPAT)或至少
   `net_operating_income`;可選從投入資本扣閒置現金。
4. **SEC-2**:排名前把 `total_equity <= 0` 與 `(TA − CL) <= 0` 設 NULL(比照
   `raw_quarterly.py` 的 `>0` 護欄)。
5. **SEC-3**:每股自由現金流分母改 `total_capital_stock / 10`,或更名。
6. **防復發守護**:一支 `src/quantlib/tests/test_financial_ratios_view.py`,鎖死
   「2330 2024Q4 週轉率 = 0.47353(lag4)」「cash_ratio 若改分母 = 1.683」
   「毛利率 = 0.5612」等手算錨,先紅後綠。

---

## 這一輪查了什麼(避免下次重查)

- 精讀 `3_financial_index_quarterly.sql`(173 行)、`4_financial_index_ttm.sql`
  (206 行)全部比率算式 + CBS 評分。
- 精讀底層 matview `6_concise_financial_statement_with_titles` /
  `2_balance_sheet_with_titles` / `4_cash_flows_with_titles` 的科目對映(確認
  profit = 本期稅後淨利、net_operating_income = 營業利益、現金流符號慣例)。
- 實測確立粒度:損益/現金流欄位是**單季**值(2330 2024 各季加總 = 全年),
  故 TTM 對 4 季加總正確。
- 對照 `src/quantlib/strat_lab/raw_quarterly.py` 已修版本(ROA/週轉率用期初、
  槓桿用平均、缺料傳 NULL 的定義)。
- 數字驗證:週轉率 lag5 off-by-one(0.47540 vs 0.47353)、cash_ratio 分母
  尺度差 ~5×、ROIC 分子敏感度(淨利/營業利益 0.858~1.071)、負權益全史 13 列/
  負資本運用額 176 列、view3 手查值確認季頻版 lag1 正確且 cash_ratio 同用 TA 分母。
