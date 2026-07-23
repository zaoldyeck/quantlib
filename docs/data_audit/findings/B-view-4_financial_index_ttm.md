# B-view-4_financial_index_ttm — 財務定義與算式審查

- **對象**:`src/main/resources/sql/view/4_financial_index_ttm.sql`(206 行,35 個輸出欄位)
- **維度**:B(財務定義與算式)
- **結論**:🔴 **BUG**
- **稽核日**:2026-07-22
- **可重跑證據**:`docs/data_audit/scripts/B-view-4_financial_index_ttm/checks.sql`
  (`psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/B-view-4_financial_index_ttm/checks.sql`)

---

## 一句話結論

**原始的財務數字是對的,蓋在上面那一層「比率與評分」不能拿來選股。**

台積電 2024 年的營收 2.894 兆、每股盈餘 45.25 元、毛利率 56.1%、營益率 45.7%、
淨利率 40.5%、稅後淨利 1.1724 兆——全部與官方公告對得上,四則運算逐位正確。

但只要離開「連續四季都有資料、而且沒有重複列」的理想狀況,這張表就會靜靜給出
錯的數字,而不是給 NULL:

1. **資產週轉率全表都用錯基期**——寫成「15 個月前」而不是「去年同期」,同一支
   SQL 裡的 ROA 就寫對了,顯然是複製貼上時漏改。
2. **資料缺漏的公司拿到最高分**——ROIC 算不出來的公司,排名被排到最後面,而分數
   是「排名 ÷ 家數」,所以它拿 100 分。5,370 筆這種列的平均分是 94.5,有資料的
   公司平均只有 48.5。
3. **應收帳款被算兩次**——502 家公司、6,969 個公司季(2018 年後還有 4,242 個)
   平均高估 25.7%,最高 50%。連帶把應收帳款周轉天數推過計分門檻。
4. **「最近四季」其實是「最近四列」**——富邦金 2024 年只有兩季資料,表上的
   「2024 全年稅後淨利」是 1,369.9 億,官方是 1,508.6 億;2023 年更誇張,
   表上 279.0 億、官方 650.4 億(少了 57%)。
5. **2006-2012 年同一支查詢跑四次會得到四個答案**(平均 ROIC 在 0.0616~0.0646
   之間飄)。上游的重複列讓「往前數三列」數到不同的列。

**目前的爆炸半徑**:現役實盤(Serenity `ev_v3_wf`、apex `strategy_s`)與 Python
研究線都不碰這張表——它不在 DuckDB cache 裡,`research/strat_lab/raw_quarterly.py`
開頭就明文寫「我們刻意不用 financial_index_ttm」。**還在用的是**:Scala 策略層
(`Signals.financialIndexField`、`MagicFormulaPiotStrategy` 的 `fcf_per_share > 0`、
`Main.scala:357-360` 把 `cbs`/`gross_margin`/`operating_margin` 當因子送進 IC 掃描)、
下游 `5_growth_analysis_ttm.sql:477`,以及 `.claude/agents/` 的個股基本面 agent 與
多空研究員——**最後這組是使用者會直接看到的判斷**。

---

## 每個計算欄位的公式(逐欄拆解)

單位:所有金額欄位為新台幣**千元**;`eps` 為元/股;其餘比率無量綱;
`days_sales_*` 為天。來源期間:`total_assets` 等資產負債表科目為**期末時點**,
`profit` / `total_operating_revenue` / `ocf` 等損益與現金流科目為**單季差分後
再滾四季**。

| 欄位 | 分子 | 分母 | 期間 | 行號 |
|---|---|---|---|---|
| `roic` | TTM 稅後淨利 | 期末(總資產 − 流動負債) | 流量÷時點 | 13-15 |
| `roa` | TTM 稅後淨利 | (期末總資產 + `lag(,4)`)/2 | 流量÷平均 | 16-21 |
| `equity_multiplier` | 期末總資產 | 期末權益 | 時點 | 22 |
| `current_ratio` | 期末流動資產 | 期末流動負債 | 時點 | 23 |
| `quick_ratio` | 流動資產 − `coalesce(存貨,0)` − `coalesce(預付,0)` | 流動負債 | 時點 | 24-25 |
| `cash_ratio` | 期末現金 | 期末總資產 | 時點 | 26 |
| `cash_flow_ratio` | TTM 營業現金流 | 期末流動負債 | 流量÷時點 | 27-29 |
| `cash_flow_adequacy_ratio` | 20 季營業現金流 | −(20 季 資本支出+存貨增加+現金股利) | 5 年 | 30-41 |
| `cash_flow_reinvestment_ratio` | TTM(營業現金流 + 現金股利) | 期末(總資產 − 流動負債) | 流量÷時點 | 42-47 |
| `days_sales_outstanding` | (應收 + `lag(,4)`)/2 × 365 | TTM 營業收入 | 平均÷流量 | 48-53 |
| `gross_margin` | TTM(營收 − 營業成本) | TTM 營收 | 流量 | 62-66 |
| `total_assets_turnover` | TTM 營收 | (期末總資產 + **`lag(,5)`**)/2 | 流量÷平均 | 67-72 |
| `profit_margin` | TTM 稅後淨利 | TTM 營收 | 流量 | 75-79 |
| `operating_margin` | TTM 營業利益 | TTM 營收 | 流量 | 80-84 |
| `days_sales_of_inventory` | (存貨 + `lag(,4)`)/2 × 365 | TTM 營業成本 | 平均÷流量 | 85-90 |
| `inventories_ratio` | 期末存貨 | 期末總資產(**無 nullif**) | 時點 | 91 |
| `receivables_ratio` | 期末應收 | 期末總資產(**無 nullif**) | 時點 | 92 |
| `eps` | TTM 每股盈餘 | — | 流量 | 93-94 |
| `fcf_per_share` | TTM(營業現金流 + 資本支出) | **股本**(不是股數) | 流量÷時點 | 95-99 |

評分層(`cbs` 綜合分,0-100):

```
cbs = operating_performance × 0.25   -- roic 的當季全市場百分位
    + return_on_investment  × 0.25   -- roa  的當季全市場百分位
    + capital_structure     × 0.10   -- equity_multiplier 的當季同業百分位(倒序)
    + liquidity             × 0.10   -- current_ratio(0/10/20/40)+ quick_ratio(0/10/20/40/60)
    + cash_flow             × 0.30   -- cash_ratio(0..50)+ 三現金流比合格(0/10/20)+ DSO(5..30)
```

---

## 🔴 BUG

### 1. `total_assets_turnover` 的「去年同期總資產」寫成 15 個月前

`4_financial_index_ttm.sql:69` 用 `lag(total_assets, 5)`,同一支 SQL 的
`roa`(行 18)用 `lag(total_assets, 4)`。台股一年四季,往前 4 列才是去年同期。

| 期別 | `lag(4)` 落在 | `lag(5)` 落在 | 正確周轉率 | view 值 | 誤差 |
|---|---|---|---|---|---|
| 2330 2022Q1 | 2021Q1 | 2020Q4 | 0.4966 | **0.5082** | +2.3% |
| 2330 2022Q4 | 2021Q4 | 2021Q3 | 0.5210 | **0.5457** | +4.7% |
| 2330 2023Q3 | 2022Q3 | 2022Q2 | 0.4269 | **0.4398** | +3.0% |

view 值與 `lag(5)` 版本**逐位相符**,證明就是這一處。全表 100% 的列受影響。
成長中的公司分母偏小 → 周轉率被系統性高估。

**修法**:`lag(total_assets, 5)` → `lag(total_assets, 4)`;更根本的做法是把
`(year, quarter)` 展成連續序號後用日曆對齊,別靠列位移(見 BUG 4)。

### 2. 算不出 ROIC / ROA 的公司,拿到最高的品質分

行 113-114 `rank() over (partition by year, quarter order by roic)`。
PostgreSQL 的 `ORDER BY` 預設是 `ASC NULLS LAST` → NULL 排最後 → 行 121
`roic_rank / count_by_year * 100` 給它 100 分。

| | 筆數 | 平均 `operating_performance` | 最大 |
|---|---|---|---|
| `roic` 有值 | 165,133 | 48.5 | 100.0 |
| `roic` 為 NULL | **5,370** | **94.5** | 100.0 |

`roa` 同理:8,381 筆 NULL 的 `return_on_investment` 平均 81.7,有值的 47.9。
這兩項合計佔 `cbs` 權重 **50%**。2018 年後仍有 1,226 / 1,249 筆。

**修法**:`order by roic nulls first`(讓缺料排最前=最低分),或更誠實地在
rank 前排除 NULL、讓該列的 `cbs` 直接為 NULL。

### 3. 應收帳款被加了兩次

上游 `materialized_view/2_balance_sheet_with_titles.sql:6-17` 的 `receivable`
CTE 把 28 個標題**全部 sum 起來**,而 MOPS 的完整報表同時揭露「毛額」與
「淨額」兩行:

```
1108 幸福 2019Q1(balance_sheet 原始列)
  應收帳款              305,255      應收帳款淨額          305,255   ← 同一筆,列兩次
  應收帳款－關係人       10,860      應收帳款－關係人淨額   10,860   ← 同一筆,列兩次
  應收票據淨額          379,499      應收票據－關係人淨額    3,611
  其他應收款             41,686
```

- 正確應收合計 = 740,911
- `balance_sheet_with_titles.receivable` = **1,057,026**(高估 42.7%)

全史 **6,969 個公司季 / 502 家公司**確定重複(2018 年後 4,242 個),
平均高估**佔申報值的 25.7%**、最高 50%。

連帶後果:
- `days_sales_outstanding`:1108 2019Q1 = 91.8 天,正確約 64 天。跨過行 159-160
  的 90 天門檻 → `cash_flow` 少拿 4 分(16 → 12),而 `cash_flow` 佔 `cbs` 30%。
- `receivables_ratio` = 0.1408,正確約 0.0987。

**修法**:`receivable` CTE 改成「同一科目淨額優先、無淨額才取毛額」的
COALESCE 階層(毛額與備抵一組、淨額一組、合計一組互斥),不要無腦 sum 全部標題。

### 4. 「最近四季」其實是「最近四列」,缺季時靜靜算錯而不是回 NULL

全檔 20 餘處 `rows between 3 preceding and current row` 都是**列位移**,沒有
任何「這四列是否為連續四季」的檢查;上游
`materialized_view/5_concise_income_statement_individual.sql:18-22` 的累計制
差分同樣只判 `quarter = 1`,不判前一列是否為前一季。

實例 2881 富邦金(2023 缺 Q2,2024 缺 Q1、Q3):

| 期別 | 官方累計數 | view 的 TTM 稅後淨利 | 差 |
|---|---|---|---|
| 2022Q4 | 478.6 億 | 478.6 億 | ✓ |
| 2023Q4 | **650.4 億** | **279.0 億** | −57.1% |
| 2024Q4 | **1,508.6 億** | **1,369.9 億** | −9.2% |

2024Q4 的 `eps` 顯示 9.64,富邦金 2024 年實際 EPS 為 10.77。
2024Q2 那一列更直接:H1 累計 812.6 億被差分成 162.2 億,因為 Q1 那一列不存在,
`lag` 抓到的是**去年 Q4 的累計數**,等於把去年整年扣掉。

規模:2018 年後 **2,045 / 57,496 筆(3.6%)** 的 TTM 視窗不是連續四季;
缺 Q1 的公司年 5,327 個(2018 年後 458 個,含 2880 華南金、2881 富邦金、
2882 國泰金等金控)。

**修法**:先把 `(year, quarter)` 折成 `year*4+quarter` 的連續序號,改用
`range between 3 preceding and current row`,並加守門「視窗內剛好 4 列」,
不足即 NULL。差分那一層同樣要驗前一列 = 前一季,否則回 NULL。

### 5. 2006-2012 年,同一支查詢跑四次得到四個答案

上游 `concise_financial_statement_with_titles` 對 858 個
`(market, year, quarter, company_code)` 產生 2/4/8/64 倍重複列(11,040 筆
幽靈列)。`financial_index_ttm` 在 2006-2012 有 **23.27%** 的列是幽靈列
(2013 年後 0%、2006 年前 0%)。

```
select avg(cbs), avg(roic) from financial_index_ttm where year = 2010;
  第 1 次  cbs 47.411744   roic 0.064594
  第 2 次  cbs 47.413899   roic 0.063282
  第 3 次  cbs 47.413855   roic 0.062599
  第 4 次  cbs 47.412813   roic 0.061552     ← 平均 ROIC 飄動約 5%
同一支查 2019:  51.618531 / 0.033456,連跑兩次完全一致
```

原因是重複列讓「往前數三列」依執行計畫送進來的實體順序而變。同時
行 117-118 的 `count(*) over (partition by year, quarter)` 是百分位的分母,
也被幽靈列灌水兩到四成。

根因在 `materialized_view/6_concise_financial_statement_with_titles.sql`:
`total_operating_revenue`(行 35-40)、`operating_expenses`(行 41-43)、
`eps`(行 76-82)、`total_assets`(行 2-7)、`total_equity`(行 20-25)這些
CTE 只用 `OR` 列舉同義標題卻**沒有 `distinct on`**,證券商(6005/6015/6016/
6020/6023/6026 等 55 家)同時申報「收入」與「營業收入淨額」、「營業費用」與
「費用」、「每股盈餘」與「每股稅後盈餘(元)」→ 直接笛卡兒。
(這條與姊妹單位 `B-view-5_growth_analysis_ttm` 的 BUG 1 同源;此處補上它在
`financial_index_ttm` 自身欄位 `roic` / `cbs` 上的重現。)

**修法**:每個 CTE 補 `distinct on (year, quarter, company_code)` + 明確的
標題優先序(像 `net_operating_income` 與 `profit` 已經有做的那樣)。

### 6. 存貨缺料被 `coalesce` 成 0 → 速動比率退化成流動比率,拿到最高分

行 24-25:`(total_current_assets - coalesce(inventories,0) - coalesce(prepaid_expenses,0)) / total_current_liabilities`。
`inventories` 來自 `balance_sheet_with_titles`,而 `balance_sheet` 這張表
**2009 年才開始有資料**。

| 期間 | 有 `quick_ratio` 的列 | 其中 `quick_ratio` == `current_ratio` | `liquidity` 平均 |
|---|---|---|---|
| 2009 年前 | 51,088 | **51,088(100%)** | 72.4 |
| 2009 年後 | 116,063 | 9,269(8.0%) | 68.9 |

「不知道存貨」被當成「沒有存貨」,速動比率被推高到流動比率,行 130-133 直接
給 60 分(滿分)。

**修法**:拿掉 `coalesce`,存貨為 NULL 時 `quick_ratio` 就該是 NULL。

### 7. 2009 年前 `cash_flow` 分項恆為 5 分,`cbs` 跨期完全不可比

`balance_sheet` 與 `cash_flows_progressive` 都從 2009 年起才有資料:

| 期間 | 列數 | `ocf` NULL | `cash_ratio` NULL | `DSO` NULL | `fcf_per_share` NULL | `cash_flow` 平均 |
|---|---|---|---|---|---|---|
| 2009 年前 | 51,840 | **100%** | **100%** | **100%** | **100%** | **5.0** |
| 2009-2012 | 29,544 | 29.0% | 22.5% | 43.3% | 30.3% | 34.6 |
| 2013 年後 | 89,119 | 1.2% | 0.8% | 3.8% | 3.1% | 50.7 |

三個 `case` 對 NULL 分別落到 0 / 0 / 5(行 136-162),所以 1989-2008 的
`cash_flow` **恆等於 5.0**。這一項佔 `cbs` 權重 30% → 舊年份的 `cbs` 天生
比 2013 年後低約 13.7 分。**「2010 年的 cbs 比 2020 年低」是資料補齊程度的
差別,不是體質差別。**

**修法**:資料不足的期間 `cbs` 應為 NULL,或明確把本 view 的有效起始年
標成 2009(2013 更保險)。

---

## 🟡 SUSPECT

### 8. `roic` 不是 ROIC,是「用淨利算的 ROCE」

行 13-15 = TTM **稅後淨利** ÷ 期末(總資產 − 流動負債)。

學理上 ROIC = NOPAT ÷ 投入資本:分子要用**稅後營業利益**(排除業外與一次性),
分母要**扣掉閒置現金與非營業資產**。這裡分子含業外損益、分母含全部現金,
本質是「運用資本報酬率(ROCE)」而且分子換成了淨利。

另外分子是四季**流量**、分母是單一**時點**存量;同一支 SQL 的 `roa`(行 16-21)
就有做期初期末平均。現金部位季底波動大的公司會被系統性扭曲。

`CLAUDE.md:556` 與 `.claude/agents/twstock-fundamental-analyst.md:13` 都把它
稱作 ROIC,下游 agent 會照 ROIC 的語意解讀給使用者聽。

**修法**:改名 `roce_on_net_income`,或改成正規定義
(NOPAT ÷(權益 + 付息負債 − 現金));分母改期初期末平均與 `roa` 一致。

### 9. 分母為負時 ROIC 反向,獲利公司被排到最差

行 15 只包 `nullif(..., 0)`,沒擋負值。`總資產 − 流動負債 < 0` 且 TTM 淨利 > 0
的列有 **121 筆**(2018 年後 0 筆)→ `roic` 為負 → `operating_performance`
落在最低百分位。目前不影響回測窗,但只要新增一家短期負債極重的公司就會中。

**修法**:分母 ≤ 0 時回 NULL。

### 10. `fcf_per_share` 不是「每股」,是每股的 1/10;同一張表的 `eps` 卻是真的元/股

行 99 分母是 `total_capital_stock`(**股本**,新台幣千元),不是股數。
面額 10 元時股數 = 股本 × 100(千元→股),所以本欄 = 真實每股自由現金流 ÷ 10。

台積電 2024Q4 實測:TTM 營業現金流 1,826.2 十億千元、股本推得 25,933 百萬股,
`fcf_per_share` = **3.355**,真實每股自由現金流約 **33.55 元**;
同一列的 `eps` = 45.25,是正確的元/股。**一張表裡兩個「每股」欄位單位差 10 倍。**

消費者 `src/main/scala/strategy/Signals.scala:565` 拿
`fcf_per_share / closing_price` 當自由現金流殖利率:台積電算出 0.34%,實際 3.36%。

以 EPS 反推面額(2018 年後 40,027 個樣本):36,201 個(90.4%)面額 10 元、
520 個 5 元、46 個 1 元。所以**不是單純的全體縮 10 倍**——那 1.4% 非 10 元面額的
公司,橫斷面排序也被扭曲 2~10 倍。`MagicFormulaPiotStrategy.scala:72` 只用
`fcf_per_share > 0` 判正負,不受影響。

**修法**:分母改成股數(股本 ÷ 面額 × 1000),或用 `profit / eps` 反推加權平均
股數;最低限度改名 `fcf_to_capital_stock`。

### 11. 產業別取「最新一筆」套用到全部歷史 = 前視偏誤,而且用了明令禁用的來源

行 2-6:

```sql
industry as (select distinct on (company_code) company_code, industry
             from operating_revenue where market = 'twse' or market = 'tpex'
             order by company_code, year desc, month desc)
```

拿 DB 裡**最新一個月**的產業分類,套到 1989 年以來每一季。
`operating_revenue` 裡 2,502 個代號有 **1,339 個(53.5%)**出現過一個以上的
`industry` 值。`capital_structure`(同業內的權益乘數百分位,佔 `cbs` 10%)
因此用到了決策當下不可知的資訊。另有 456 列 `industry` 為 NULL,被歸成同一個
「NULL 產業」桶一起排名。

`CLAUDE.md` 明文:「**產業別一律用 `industry_taxonomy_pit`(2026-07-10 鐵律)…
禁止直接用 `operating_revenue.industry`(舊檔含 legacy 名稱且無 PIT 語義)**」。

**修法**:改 join `industry_taxonomy_pit`,以 asof(`effective_date` ≤ 該季底)
取分類。

### 12. `cbs` 的 20 幾個門檻與 5 個權重全是沒有出處的魔術數字

行 125-162:流動比 2.5/1/0、速動比 1.5/1/0.5/0、現金比 0.25/0.2/0.15/0.1/0.05、
應收天數 15/30/60/90/150/180;配分 40/60/50/20/30;
行 172-176 的權重 0.25/0.25/0.10/0.10/0.30。repo 內查無任何量測或回測支撐這些值。

`cbs` 已被 `src/main/scala/Main.scala:357` 註冊成因子送進 `FactorResearch`:
`("cbs", strategy.Signals.financialIndexField("cbs"), true)`。

另有尺度混用問題:5 個成分裡 2 個是**相對百分位**(0-100,均值恆為 50)、
3 個是**絕對門檻分**(隨資料補齊程度變動,見 BUG 7),混在同一個加權和裡,
跨期不可比。

**修法**:要嘛把每個門檻的來源量測補上並落 repo(全域天條 §2.2),要嘛拆掉
`case` 全改成百分位,讓 5 個成分同尺度。

### 13. 五年現金流視窗有 22.8% 湊不滿 20 季,資本支出還被濾掉一部分

行 30-41 的 `cash_flow_adequacy_ratio` 用 `rows between 19 preceding and current row`。
159,463 個公司季裡 **36,385 個(22.8%)**前面不足 19 列,靜靜用較短期間算完
就當成「五年」。

另外 `materialized_view/4_cash_flows_with_titles.sql:16-19` 的 `capital_expense`
CTE 加了 `and value < 0`,單季差分後 ≥ 0 的有 **9,039 / 114,467(7.9%)**被丟掉,
再被行 32-34 的 `coalesce(..., 0)` 當成 0 → **分母偏小 → 允當比率偏高**。

**修法**:視窗不足 20 季回 NULL;資本支出用絕對值累計而不是丟掉正值。

### 14. 行 91-92 兩處裸除法是未爆彈(目前僥倖沒炸)

`inventories / total_assets` 與 `receivable / total_assets` 是全檔僅有的兩處
**沒包 `nullif`** 的除法。現存 `total_assets = 0` 的列有 7 筆,但那 7 筆的
`inventories` / `receivable` 都是 NULL,而 PostgreSQL 對 `NULL::float8 / 0`
回 NULL 不報錯(`1::float8 / 0` 才 `ERROR: division by zero`)。
只要哪天補進一家 `total_assets = 0` 且有存貨的公司,**整個 view 會直接 ERROR**。

**修法**:補 `nullif(total_assets, 0)`。
(姊妹單位 `B-view-5_growth_analysis_ttm` 也標了同一處;此處補上「為何目前沒炸」
的第一手驗證,免得下一個人再查一次。)

---

## ⚪ REAL(看起來像錯,查證後是真實現象)

### 15. 金融股沒有毛利率、營業利益率會 > 1

2024Q4 實測:

| 代號 | 公司 | 產業 | 毛利率 | 營業利益率 | 淨利率 |
|---|---|---|---|---|---|
| 2881 | 富邦金 | 金融保險業 | (NULL) | **1.116** | 0.966 |
| 2891 | 中信金 | 金融保險業 | (NULL) | **0.908** | 0.678 |
| 2882 | 國泰金 | 金融保險業 | (NULL) | 0.590 | 0.514 |
| 2330 | 台積電 | 半導體業 | 0.561 | 0.457 | 0.405 |

上游把金融業的「營業收入」對到**利息淨收益**
(`6_concise_financial_statement_with_titles.sql:35-40`),但「營業利益」含手續費、
投資、保險等**全部**收入 → 分子大於分母。這是**定義落差不是解析錯**。

**啟示**:`operating_margin` / `gross_margin` **不能跨產業排名**。
`Main.scala:359-360` 把它們當全市場因子掃 IC,對金融股是無意義的。
2018 年後另有 57 筆 `gross_margin > 1`、209 筆 `< -1`,來自營收極小或為負的分母。

---

## 🟢 OK(查過沒問題,別再查一次)

### 16. 現代(2013+)密集資料的四則運算完全正確,有外部錨

台積電 2330 2024Q4,view 值 vs 官方公告:

| 項目 | view | 官方 |
|---|---|---|
| TTM 營收 | 2,894 十億千元 = 2.894 兆 | 2.894 兆 ✓ |
| TTM 每股盈餘 | 45.25 | 45.25 ✓ |
| TTM 稅後淨利 | 1.1724 兆 | 1.1733 兆 ✓ |
| 毛利率 | 0.561 | 56.1% ✓ |
| 營業利益率 | 0.457 | 45.7% ✓ |
| 淨利率 | 0.405 | 40.5% ✓ |
| TTM 營業現金流 | 1.8262 兆 | 1.826 兆 ✓ |

`roic` / `roa` / `equity_multiplier` 手算與 view **逐位相符**
(0.2160 / 0.1918 / 1.548)。**累計制差分與跨年邊界在資料連續時是對的。**

### 17. `market = 'tw'` 的 join 條件是對的,不是筆誤

行 101、105 寫 `balance_sheet_with_titles.market = 'tw'` /
`cash_flows_with_titles.market = 'tw'`,而主表 `cfswt` 的 market 是 `twse`/`tpex`。
乍看像寫錯,實測:`balance_sheet_with_titles` 全表 116,455 列 market 只有 `tw`,
`cash_flows_with_titles` 113,488 列同樣只有 `tw`(來源 `balance_sheet` /
`cash_flows_progressive` 是完整財報,`FinancialReader.readFinancialStatements()`
一律寫死 `"tw"`)。**join 得到,沒問題。**

### 18. `net_operating_income` 在現代沒有被「繼續營業單位稅前淨利」污染

`6_concise_financial_statement_with_titles.sql:44-52` 同時列舉「營業利益」與
「繼續營業單位稅前淨利(淨損)」,靠 `distinct on ... order by title` 決勝,
看起來很危險。實測**兩者都存在且值不同的 35,321 個公司季,100% 選到營業利益、
0% 選到稅前**;只有 2,355 個「只有稅前」的季會退而求其次,且**沒有一個落在
2018 年後**。`operating_margin` 在現代是真的營業利益率。

### 19. 跨市場不會重複計算(window `partition by` 沒寫 market 也安全)

全檔 20 餘個 window 的 `partition by cfswt.company_code` 都**沒有帶 market**。
實測:同一 `company_code` 在同一 `(year, quarter)` 同時出現在 twse 與 tpex 的
情形有 **0 筆**;13 個代號跨過市場(上櫃轉上市),`partition` 不含 market 反而
讓時序連續下去,**是對的**。

### 20. 「稅後 vs 稅前淨利」的混用只到 2005 年,不進回測窗

`profit` CTE(`6_concise_financial_statement_with_titles.sql:64-73`)把
「稅前純益」也列進候選。實測:**沒有任何一個公司季同時有稅前與稅後**候選
(0 筆),32,735 個公司季**只有稅前**——而且**最晚只到 2005 年**
(1989-2005 的舊制簡明報表)。2006 年起全部是稅後。
所以 `profit` / `roic` / `roa` / `profit_margin` 在 2006 年後語意一致。

### 21. PIT 由消費者側正確處理,view 本身沒有前視(除了 SUSPECT 11 的產業別)

view 沒有公告日欄位,PIT 全靠消費者。`Signals.scala:144-158` 的
`latestQuarterField` 用 `PublicationLag.asOfQuarter` 取「法定截止日 + 7 天緩衝
已過」的最新一季,再 `DISTINCT ON (company_code) ... ORDER BY year DESC, quarter DESC`
收斂到最新一筆。`PublicationLag.scala:32-37` 的 5/15、8/14、11/14、次年 3/31
與證交法一致。**這條路徑是乾淨的。**

### 22. 爆炸半徑:不在現役實盤與 Python 研究路徑上

- **不在 DuckDB cache**:實測 `research/paths.py::CACHE_DB` 的 24 張表無此表。
- `research/strat_lab/raw_quarterly.py:8-12` 明文「We DELIBERATELY do NOT use…
  `financial_index_ttm` (VIEW)」;`research/db.py:42/107` 亦已移除同類 view。
- 現役實盤 Serenity `ev_v3_wf` 與 apex `strategy_s` 都不碰。

**仍在用的**:
- `src/main/scala/strategy/Signals.scala:142`(`financialIndexField` 泛用載入器)、
  `:531`(`ocfToNetIncome`)、`:548-565`(`fcfYield`)
- `src/main/scala/strategy/MagicFormulaPiotStrategy.scala:72`(`fcf_per_share > 0`)
- `src/main/scala/Main.scala:357-360`(`cbs` / `gross_margin` / `operating_margin`
  當因子送 IC 掃描)
- `src/main/resources/sql/view/5_growth_analysis_ttm.sql:477`
- `.claude/agents/twstock-fundamental-analyst.md`、`twstock-bull-researcher.md`、
  `twstock-bear-researcher.md`——**使用者會直接看到的個股判斷**

---

## 修復優先序建議

| 序 | 項目 | 為什麼先做 |
|---|---|---|
| 1 | BUG 2(NULL 拿滿分) | 一行 `nulls first` 就修好,影響 `cbs` 一半權重 |
| 2 | BUG 1(`lag(5)`→`lag(4)`) | 一個字元,全表 100% 受影響 |
| 3 | BUG 5(上游重複列) | 修完 2006-2012 才有可重現性,且同時修好姊妹 view |
| 4 | BUG 3(應收重複計算) | 2018 年後仍有 4,242 筆,直接影響 `cbs` 的 30% 權重項 |
| 5 | BUG 4(列位移 → 日曆對齊) | 工程量最大,但這是「靜靜給錯答案」的根源 |
| 6 | BUG 6/7(缺料 coalesce / 舊年份封頂) | 讓資料不足的期間誠實回 NULL |
| 7 | SUSPECT 10/11/12(單位、產業 PIT、魔術數字) | 定義層,不影響「能不能跑」但影響「能不能信」 |
| 8 | SUSPECT 14(裸除法) | 兩個 `nullif`,拆未爆彈 |
