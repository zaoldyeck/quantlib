# B-view-1_cbs_by_year — 財務品質評分 view 稽核

審查對象：`src/main/resources/sql/view/1_cbs_by_year.sql`（`cbs_by_year`，2022 legacy）

## 一句話結論

**這個 CBS 分數不能拿來選股。** 五個子分裡有三個算壞了：流動性子分幾乎人人滿分
（形同常數）、缺財報的公司在「營運績效」反而拿接近滿分、ROIC 的分子有 24% 的公司
用的是「稅前」獲利、其餘用「稅後」——三種病同時存在，算出來的 `cbs` 排名是失真的。
好消息是：目前沒有任何 Scala/Python 策略在吃這個 view（只有姊妹 view
`cbs_by_year_5y_over75` 引用它），所以它不在現役 Serenity/Evergreen 資金路徑上。

## 這個 view 在算什麼

年度（Q4）財務品質分 `cbs`，由 5 個子分加權：

```
cbs = operating_performance*0.25   -- ROIC 的年度橫截面百分位
    + return_on_investment*0.25    -- ROA 的年度橫截面百分位
    + capital_structure*0.10       -- 負債/資產比 的「年×產業」百分位
    + liquidity*0.10               -- 流動比 + 速動比 分桶
    + cash_flow*0.30               -- 現金比率 + 現金流量三條件 + 收現天數 分桶
```

物化後 41,226 列，其中 5,888 列 `cbs>75`，會被下游
`cbs_by_year_5y_over75.sql`（連續 5 年 CBS>75 的篩選）當作選股條件。

---

## BUG 1（高）：流動性子分因「百分比 vs 倍數」單位錯配而失效

`financial_analysis` 的 `current_ratio(%)`、`quick_ratio(%)` 是以**百分比**儲存的
（流動比 1.86 倍 → 存成 185.83）：

| 欄位 | 中位數 | 最大 |
|---|---|---|
| `current_ratio(%)` | 185.83 | 200020 |
| `quick_ratio(%)` | 131.13 | 200020 |

但 view 第 93–103 行的分桶門檻是**倍數尺度**：`current_ratio(%) > 2.5 / > 1 / > 0`、
`quick_ratio(%) > 1.5 / > 1 / > 0.5 / > 0`。因為資料是百分比，任何流動比大於 2.5%
（≈瀕臨倒閉）的公司都直接落到最高桶。實測 liquidity 子分分佈：

```
liquidity=100 → 40,162 列（99.69%）
其餘 8 種值合計 → 123 列（0.31%）
```

**流動性子分對 99.69% 的公司是常數 100**，等於固定給每家 +10（0.1 權重）進 `cbs`，
零鑑別度。門檻 2.5/1.5/1/0.5 是教科書的「倍數」切點（流動比 2.5 倍、速動比 1.5 倍），
作者顯然把 TWSE 財務分析欄位當成倍數，但它們是百分比，差 100 倍。

- 證據：`src/main/resources/sql/view/1_cbs_by_year.sql:93-103`；上表 SQL 與分佈 SQL 皆可重跑。
- 修法：門檻乘 100（2.5→250、1→100、1.5→150、0.5→50），或先把欄位 /100；並先向
  TWSE 財務分析文件確認 current/quick ratio 的單位。

## BUG 2（高）：缺財報的公司在 operating_performance 反而拿接近滿分

第 70 行 `rank() over (partition by year order by roic)` 是**升冪**。PostgreSQL 升冪排序
預設 **NULLS LAST**，所以 `roic` 為 NULL（缺資產負債表、或標題對不上）的公司被排到
**最後 = rank 數字最大 = 百分位最高**。實測：

| 群組 | 列數 | 平均 operating_performance |
|---|---|---|
| roic 為 NULL（缺財報） | 1,064 | **96.86** |
| roic 有值 | 40,162 | 48.76 |

**財報缺失的 1,064 個 firm-year 拿到接近滿分的營運績效分（25% 權重）**，方向完全相反。
第 71 行 `roa_rank`（`order by "return_on_total_assets(%)"` 升冪）結構完全相同，同類病。

- 證據：`1_cbs_by_year.sql:70-71`；上表 SQL 可重跑（重建 roic 後 rank）。
- 修法：rank 的 order by 加 `NULLS FIRST`，或先濾掉指標為 NULL 的列，或給缺值一個地板值；
  現行寫法是在獎勵資料破洞。

## BUG 3（中高）：ROIC 分子把「稅前純益」和「稅後淨利」混用

profit CTE（第 20–30 行）的標題白名單把 `稅前純益`（**稅前**）和一堆稅後/淨利標題並列，
再用 `distinct on (year,company_code) order by ... title` 取一筆。實測選中標題分佈：

```
本期淨利（淨損）      22,492
稅前純益             10,059   ← 稅前！約占全體 24%
合併總損益            8,792
本期淨利(淨損)         1,120
本期稅後淨利（淨損）     330
本期損益                18
```

進一步查證：這 10,059 個「稅前純益」全部是**標題覆蓋缺口**（該 firm-year 在白名單裡
沒有任何一個稅後標題，才 fall through 到稅前），不是排序 tie-break。台灣營所稅約 20%，
稅前系統性高於稅後，所以這 24% 的公司 ROIC（第 41–42 行）與 operating_performance 排名
（0.25 權重）被系統性高估，和其餘 76% 用稅後的公司放在同一個橫截面排名裡——定義上不一致。

- 證據：`1_cbs_by_year.sql:20-30, 41-42`；標題分佈與 GAP 分類 SQL 皆可重跑。
- 修法：把 `稅前純益` 從 profit 白名單移除，補上這 1 萬個 firm-year 實際使用的稅後標題
  （需查它們到底掛哪個標題），或 coalesce 到單一正規化的稅後淨利科目。
  **同類警示**：物化視圖 `concise_financial_statement_with_titles`（matview 6）的 profit CTE
  用的是一模一樣的白名單與寫法——同一個缺陷類，該交給 matview-6 稽核單位一併處理。

## SUSPECT（中）：現金流量三條件桶被同一個單位錯配拖累

`cash_flow_ratio(%)`（中位 3.11）、`cash_flow_adequacy_ratio(%)`（52.8）、
`cash_flow_reinvestment_ratio(%)`（10.3）都是百分比，但第 114–121 行門檻 `>1 / >1 / >0.1`
是倍數尺度（原意應是「OCF 覆蓋流動負債 100%、資本支出充足率 100%、再投資率 10%」的強門檻）。
套在百分比資料上，最高桶（20 分）退化成「三條現金流都>約0~1% = 只要為正」。實測分佈：

```
20 分 → 53.93%    10 分 → 8.55%    0 分 → 37.52%
```

還能分出正/負（37.52% 拿 0），但「強現金製造機 vs 勉強為正」的層次被抹平——
adequacy 1.5% 和 500% 的公司同拿 20 分。比 BUG 1 輕（沒完全塌成常數），但仍是同一個單位錯配。

- 證據：`1_cbs_by_year.sql:113-123`；分佈 SQL 可重跑。
- 修法：門檻改百分比尺度（1→100、0.1→10），或把三欄先 /100。

## SUSPECT（設計）：沒有時點對齊（PIT），純用日曆年 join

view 只用 `year` 串接（第 53–64 行），年度（Q4）財報要到隔年 Q1 才公告。任何消費端若
把 `cbs_by_year` 當「第 Y 年當年就知道」用，就是前視偏誤。目前沒有現役策略吃它（只有
姊妹 view + 稽核文件引用），所以是**潛在**設計缺口、不是現行外洩；一旦要上線務必先補公告落差。

## MINOR

- **產業前視 + 過時來源**：industry CTE（第 31–35 行）用 `distinct on ... order by year desc`
  取「最新」產業套用到所有年度（輕微前視）；且用 `operating_revenue.industry`——現行鐵律
  已改用 `industry_taxonomy_pit`。此為 2022 legacy view，影響小（產業近乎不變，只用在
  負債比的「年×產業」分組），但屬同類過時。
- **84 列扇出（0.2%）**：final 41,226 列 vs distinct (year,company_code) 41,142 → 多 84 列。
  total_assets / total_current_liabilities CTE 沒對標題去重，同年同時掛 `資產合計` 與 `資產總計`
  的公司會扇出。屬清潔問題，會輕微污染 rank 的 count。
- **除零防護不齊**：`cash_ratio = cash/total_assets`（第 47 行）與 roic 的 nullif 只擋分母
  「恰為 0」，未擋負值/NULL；實務上總資產不會為 0，影響極小。

---

## 查了什麼（負結果一併落盤，避免重查）

- **合併 vs 個體：沒有混用（OK）**。以台積電 2330 FY2020 Q4 實測：cash（`balance_sheet`
  market='tw'）= 660,170,647、total_assets（`concise_balance_sheet_individual`）= 2,760,711,405、
  profit = 518,158,082——三者都吻合台積電**合併**財報（現金 ~660bn、總資產 ~2.76T、
  稅後淨利 ~518bn）。matview 名稱裡的 "individual" 指的是「按 type 去重成單筆」，不是個體報表。
  兩個來源同基礎（皆合併），cash_ratio 沒有跨基礎問題。
- **TTM / 累計：算對了（OK）**。profit 取 `concise_income_statement_progressive` 的 Q4，
  progressive 是**累計數**，Q4 累計 = 全年——年度 ROIC 分子要的正是全年，無需做單季差分。
  資產負債表科目是 Q4 年底的存量快照（本來就不需跨期差分）。沒有累計/單季誤用。
- **rank 型子分不受單位錯配影響（OK）**：operating_performance、return_on_investment、
  capital_structure 都是 `rank()` 百分位，百分比 vs 倍數在排序裡自動抵銷——**唯一例外是
  BUG 2 的 NULL 排序**。
- **cash_ratio 桶單位正確（OK）**：cash_ratio = 現金/總資產 是真比率（0~1），門檻 0.25/0.2/…
  是對的比率切點——和 BUG 1 不同，這條沒錯。
- **收現天數桶單位正確（OK）**：average_collection_days 以「天」為單位，門檻 15/30/60… 對。
- **cash CTE 無扇出（OK）**：`balance_sheet` market='tw' 每個 (year,company) 剛好 1 筆現金列
  （32,165 個 company-year 全部 n=1）。
- **消費端盤點**：全庫 grep `cbs_by_year` 只命中姊妹 view `2_cbs_by_year_5y_over75.sql` 與
  一份稽核文件；無任何 Scala/Python 策略消費。`financial_analysis` 表只被 `Main.scala` 與
  `db/table/FinancialAnalysis.scala`（表定義/reader）引用。→ 此 view 是 legacy，不在現役資金路徑。
