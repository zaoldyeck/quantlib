# B-view-5_growth_analysis_ttm — 財務定義與算式審查

- 對象：`src/main/resources/sql/view/5_growth_analysis_ttm.sql`（170,503 列）
- 上游：`src/main/resources/sql/view/4_financial_index_ttm.sql`、
  `src/main/resources/sql/materialized_view/6_concise_financial_statement_with_titles.sql`
- 證據腳本（可重跑）：`docs/data_audit/scripts/B-view-5_growth_analysis_ttm/verify.sql`
- 結論：**BUG**

---

## 一句話

**這張表的分數不能拿來選股。**同一支查詢連跑四次會得到四個不同答案（不可重現），
2006-2012 全市場的排名分母被灌了兩到四成的幽靈列，`f_score` 在 2010 年以前
被資料缺口硬壓在 6 分封頂，還有五處算式本身寫錯。原始的營收/獲利數字是對的
（台積電 2024 全年營收 2.89 兆、EPS 45.25 都對得上），**壞掉的是蓋在上面的那一層
分數與旗標**。

好消息：現役實盤（Serenity `ev_v3_wf`、apex `strategy_s`）早就不碰它，
DuckDB cache 裡也沒有它。壞消息：copilot 的個股基本面 agent 與看空 agent
還在直接查它給使用者看。

---

## 1. 它到底在算什麼（欄位盤點）

view 分兩段。`index` CTE 從 `financial_index_ttm` 讀 TTM 指標，產生
`f_score` 與 56 個布林旗標；外層再加 `growth_score` / `drop_score` 與 22 個
`*_growth_rate`。

### 1.1 `f_score`（行 3-37）

九個 0/1 條件相加，宣稱是 Piotroski F-Score：

| # | 條件（原文） | Piotroski (2000) 原定義 |
|---|---|---|
| 1 | `roa > 0` | ROA > 0 ✅ |
| 2 | `ocf > 0` | CFO > 0 ✅ |
| 3 | `ocf > profit` | CFO/TA > ROA（同分母下等價）✅ |
| 4 | `total_non_current_liabilities < lag(…)` | Δ(長期負債/平均總資產) < 0 ❌ 用**金額**不是比率 |
| 5 | `current_ratio > lag(…)` | Δ流動比 > 0 ✅ |
| 6 | `total_capital_stock <= lag(…)` | 當年未增發普通股 ✅（近似）|
| 7 | `roa > lag(roa)` | ΔROA > 0 ✅ |
| 8 | `gross_margin > lag(…)` | Δ毛利率 > 0 ✅ |
| 9 | `total_assets_turnover > lag(…)` | Δ資產週轉 > 0 ✅ |

**所有 `lag()` 都是 lag(1) = 上一季**，Piotroski 全部是年度比較。

### 1.2 56 個 5 年旗標

每個指標各兩個方向、各兩種形態：

- `_overall`：`x > 1.2 * lag(x, 20)`（好的方向要贏 20%）/ `x < lag(x, 20)`（壞的方向只要跌就算）
- `_continuous`：`x > lag(x,4) > lag(x,8) > lag(x,12) > lag(x,16) > lag(x,20)`（連五年單調）

例外：`revenue_growth_rate_increase_5y_overall`（行 38-46）比的是**成長率**不是水準值。

### 1.3 `growth_score` / `drop_score`（行 492-547）

`growth_score = f_score + 28 個「變好」旗標`（觀測域 0-30，平均 7.59）
`drop_score  = 28 個「變壞」旗標`（觀測域 0-20，平均 4.58）

### 1.4 22 個 `*_growth_rate`（行 551-607）

全部是 **QoQ（lag 1）**，不是年增率。其中 15 個寫成 `x/lag(x) - 1`，
另外 7 個寫成 `1 - x/lag(x)`（符號相反）。

單位：全部新台幣千元；`eps` 為元/股；比率無單位。無單位混用。

---

## 2. 找到的問題

### BUG 1 ★最嚴重★ — 上游重複列把 TTM 打爛，而且讓整張 view 不可重現

`concise_financial_statement_with_titles` 對 55 家公司的 858 個
`(company_code, year, quarter)` 產生 2 / 4 / 8 / **64** 倍重複列，共 11,040 筆
幽靈列（佔 view 的 6.5%），全部落在 2006-2012。成因是該 matview 的 CTE 用
`where title = 'A' or title = 'B' or …` 抓科目，金融業同一季同時存在多個別名
（`利息淨收益`／`收益`／`收入`），只有 `net_operating_income` 那個 CTE 加了
`distinct on`，其餘十幾個都沒有 → 笛卡兒相乘。

受害名單全是大型金融股：金控 2880-2892、銀行 2801/2809/2812/2836/2838/2845/2849/2897、
券商 6005/6015/6016/6020/6021/6023/6024/6026，另有 1718 中纖、2514 龍邦、
1409 新纖、2905 三商、4707 磐亞、1712 興農、9902 台火。

**三重傷害：**

**(a) TTM 不是 TTM。** view 4 的 `rows between 3 preceding and current row` 走的是
「前三列」而不是「前三季」。以 2881 富邦金 2012Q4 為例，四個單季稅後淨利
6,863,610 + 4,095,146 + 11,848,315 + 6,175,728 = **28,982,799 千元**；view 卻同時
給出 **33,967,504**（+17.2%）與 **36,048,086**（+24.4%）兩個值 —— 因為它把 Q3
算了兩次、Q1 整個漏掉。

**(b) 全市場排名分母被灌水。** view 4 的
`rank() over (partition by year, quarter order by roic)` 和
`count(*) over (partition by year, quarter)` 都吃到幽靈列：

| 季別 | 排名分母 | 真實家數 | 幽靈列 |
|---|---|---|---|
| 2009Q4 | 1,938 | 1,423 | +515（+36%）|
| 2011Q4 | 2,050 | 1,535 | +515（+34%）|
| 2012Q4 | 2,145 | 1,568 | +577（+37%）|
| 2013Q1 | 1,403 | 1,403 | 0 |

所以 2006-2012 期間**每一家公司**（不只那 55 家）的 `operating_performance`、
`return_on_investment` 百分位分數與綜合分 `cbs` 都失真。`cbs` 是
`6_cbs_ttm_5y_over75` 的唯一篩選依據。

**(c) 整張 view 不可重現。** 重複列在 `order by year, quarter` 下形成 tie，
window frame 的落點隨執行計畫變動。同一支查詢連跑四次：

```
avg(f_score) 2009-2012 = 4.014859 / 4.017601 / 4.013370 / 4.019022
avg(drop_score)        = 4.058354 / 4.062212 / 4.076699 / 4.077850
```

污染還會往前漏五年（`lag(…,20)` 從 2017 回頭仍會踩進 2012 的重複列）；
2019 年以後每次跑仍有 1 列會翻面（Δ = 1/50,710 = 1.97e-5，實測 drop_score
6.70001972 vs 6.70003944）。**意思是：任何從這張表引用過的 2006-2012 數字，
今天都復現不出來。**

### BUG 2 — 權益乘數的 5 年比較拿錯欄位（複製貼上）

行 112-113 與 329-330：

```sql
equity_multiplier < lag(total_assets_turnover, 20) over (…) as equity_multiplier_decline_5y_overall
equity_multiplier > lag(total_assets_turnover, 20) over (…) as equity_multiplier_increase_5y_overall
```

應該是 `lag(equity_multiplier, 20)`。權益乘數中位數 1.753、資產週轉率中位數
0.669，量綱根本不同，所以「權益乘數上升」幾乎恆真：

| | as-written | 正確 |
|---|---|---|
| decline 為真 | 7,198 | 61,944 |
| increase 為真 | **115,996（94.2%）** | 68,603（55.7%）|

123,228 個可比格子中 **63,567（51.6%）判定相反**。方向性偏誤：`drop_score`
平均被灌 +0.278、`growth_score` 平均被扣 0.321。實測 2023 年，
`QualityFilter` 的 `drop_score < 10` 閘門排掉 1,719 檔次，修正後只該排 1,468 →
**251 檔次（14.6% 的排除）是誤殺**。

（此項 `B-fscore-academic` 已記錄；本單位以獨立量測複現，並補上對閘門的影響數。）

### BUG 3 — 2010 年以前 `f_score` 結構性封頂在 6 分

現金流量資料 2010 才開始。2005-2009 的 `ocf` **100% 為 NULL**，
`case when ocf > 0 …` 與 `case when ocf > profit …` 對 NULL 一律落到 `else 0`；
`total_capital_stock` 在 2006-2008 也 100% NULL，第 6 項同樣恆 0。

| 年 | max(f_score) | avg | `f_score>=5` 通過率 |
|---|---|---|---|
| 2006 | **6** | 3.09 | 19.6% |
| 2008 | **6** | 2.27 | 9.1% |
| 2009 | **6** | 2.54 | 11.0% |
| **2010** | **9** | 4.50 | **49.3%** |
| 2012 | 9 | 4.62 | 52.4% |
| 2013 | 9 | 5.28 | 68.2% |

2009→2010 通過率從 11% 跳到 49%，這是**資料補齊軌跡，不是台股公司在 2010 年
突然變健康**。任何跨 2009/2010 的回測，其「品質過濾」的鬆緊度會在該點無聲跳變；
橫跨該點的截面排名也不可比。

（`B-fscore-academic` 對 Python 版 `raw_quarterly.py` 記過同型缺陷；此處是 PG view
自己的量測。）

### BUG 4 — 營收成長率門檻括號錯位，無聲多加 20 個百分點

行 38-46：

```sql
(total_operating_revenue / lag(rev, 4) - 1)
  > (1.2 * lag(rev, 20) / lag(rev, 24) - 1)
```

SQL 運算優先序讓右邊 = `1.2 × (1 + g_old) − 1` = **`1.2 × g_old + 0.2`**。
其餘所有 `_increase_5y_overall` 的形態都是 `x > 1.2 * lag(x, 20)`，對成長率的
對應寫法應是 `g_now > 1.2 * g_old`。

123,621 個可比格子中 **27,256（22.0%）判定不同**（as-written 31,393 為真，
應為 58,649）。具體：5 年前成長率是 −10% 時，門檻從 −12% 被抬到 **+8%**。

### BUG 5 — `1.2 ×` 的「進步 20%」測試在基期為負時方向相反

`profit_margin` / `operating_margin` / `roa` / `eps` / `fcf_per_share` 五個
`_increase_5y_overall`。當五年前的值是負的，`x > 1.2 * x20` 會把「變得更差」
判成「進步」——例如利潤率從 −10% 惡化到 −11%，因為 −0.11 > 1.2 × (−0.10) = −0.12
而通過。

實測共 **5,826 個格子**數值比 5 年前**更低**卻拿到 `growth_score +1`：
profit_margin 1,415、operating_margin 1,501、roa 1,588、eps 767、fcf 555。

### BUG 6 — 7 個 `*_growth_rate` 欄位的正負號與另外 15 個相反，但名字看不出來

行 567-607 這七個用 `1 - x/lag(x)`（值越大代表指標下降＝越好）：
`days_sales_of_inventory` / `days_sales_outstanding` / `equity_multiplier` /
`total_non_current_liabilities` / `inventories_ratio` / `receivables_ratio` /
`total_capital_stock`。其餘 15 個用一般的 `x/lag(x) - 1`。

反例：**1294 在 2024Q2 股本從 193,669 增到 252,388（+30.3%），
`total_capital_stock_growth_rate` = −0.303**。全表 17,708 列為負、5,181 列為正——
股本通常是增加的（股票股利、現增），符號確實反了。

任何「把所有 `*_growth_rate` 一起排名」的消費者會把七個因子做反方向。
`8_valuation.sql:55-92` 正是一次吃下全部 36 個欄位。

### BUG 7 — `lag(n)` 是「往前 n 列」不是「往前 n 季」

window 只寫 `order by year, quarter`，沒有任何日曆對齊檢查。缺季 + 上述重複列
使得：

- `lag(…, 20)` 有 **14.87%**（19,674 / 132,346）不是剛好 5 年前
- `lag(…, 4)` 有 **11.62%**（18,856 / 162,331）不是剛好 1 年前

這些格子上，全部 56 個 `_5y_*` 旗標與 22 個 `*_growth_rate` 比較的是錯的期間。

---

## 3. 存疑（有證據但要看設計意圖）

### SUSPECT 1 — `growth_score` 內含 `f_score`，卻被當成獨立因子量 IC

行 492 開頭就是 `f_score + coalesce(...)`。實測
`corr(f_score, growth_score) = 0.753`，而 `avg(growth_score) = 7.591` 裡有
`4.373` 就是 `f_score` 本身（58%）。

`src/main/scala/Main.scala:348-350` 把 `fScore` / `dropScore` / `growthScore`
三個當成獨立因子丟進 `FactorResearch.individualICs` + `pairwiseCorrelations`。
量出來的 0.75 相關會被誤讀成「這兩個因子恰好相關」，實際上是**一個包含另一個**。

### SUSPECT 2 — `growth_score` 與 `drop_score` 不是鏡像，兩者相減沒有意義

門檻不對稱：growth 側有 9 個成分要求「比 5 年前好 20%」，另 5 個只要求
「有進步」；drop 側 14 個**全部**只要求「有退步」。實測 roa 側
increase 觸發率 30.3% vs decline 38.8%。

更麻煩的是名字說謊：`revenue_growth_rate_increase_5y_overall` 比的是**成長率**，
但同名的 `revenue_growth_rate_decline_5y_overall`（行 263）比的是**營收金額**；
`revenue_growth_rate_increase_5y_continuous`（行 47-65）比的也是金額。四個一組的
欄位裡只有一個真的在比成長率。

### SUSPECT 3 — `f_score` 的定義偏離 Piotroski（不是算錯，是定義換了）

(a) 五個 Δ 項全用 lag(1) = 上一季，Piotroski 全部是年度比較。實測改成 lag(4)
後 **67.8%** 的格子分數不同、**19.9%** 的格子跨過 `>=5` 門檻。而且 TTM 的
QoQ 差分中有 3/4 的期間重疊，訊號幅度只有年度版的 1/4、雜訊高得多；
第 4/5/6 項比的又是**資產負債表時點值**，QoQ 會直接吃到除息、季底調節的季節性。

(b) 第 4 項用長期負債**金額**而非 Piotroski 的長期負債/平均總資產**比率**：
資產與負債同比例成長的健康公司被扣分、縮表的衰退公司反而得分。

算術本身沒錯——手算 2330 2024Q4：`1+1+1+0+0+1+1+1+1 = 7`，與 view 輸出的
`f_score = 7` 完全一致。

（（a）與 `B-fscore-academic` 重複；此處補 PG 側的獨立量測。）

### SUSPECT 4 — 除以零的未爆彈

上游 `4_financial_index_ttm.sql:91-92`：

```sql
inventories / total_assets as inventories_ratio,
receivable  / total_assets as receivables_ratio,
```

同一支 SQL 其他二十幾處除法都包了 `nullif(..., 0)`，只有這兩處沒有。目前
`total_assets = 0` 有 7 列（910801 2008Q2/Q4、1258 2012Q4、3990 2011Q4、
910708 2010Q2、1718 2006Q2 ×2），僥倖因為這幾列的 `inventories` / `receivable`
都是 NULL（PG 的 `NULL/0` 回 NULL 不報錯）才沒炸。只要日後任何一列同時有存貨
數字與零總資產，`select * from growth_analysis_ttm` 整句會 `ERROR: division by zero`。

### SUSPECT 5 — view 本身沒有公告日欄位，PIT 完全靠消費者自律，而消費者漏了

view 只有 `(year, quarter)`，沒有任何 filing date。

- ✅ `strategy/QualityFilter.scala:26` 有走 `PublicationLag.asOfQuarter`
  （Q1→5/22、Q2→8/21、Q3→11/21、Q4→次年 4/7，含 7 天緩衝），這部分是對的。
- ❌ 同一個檔案 `QualityFilter.scala:31-37` 的 WHERE 沒有收斂到最新一季：只要
  **歷史上任何一季**符合 `drop_score < 10 and f_score >= 5` 就通過，而
  行 23-24 的註解卻寫 "Uses the latest growth_analysis_ttm quarterly snapshot
  available on D"。同檔 `MinFScore` 註解寫 "8 binary factors"，view 加總的是 9 項。
- ❌ `.claude/agents/twstock-fundamental-analyst.md:14` 與
  `.claude/agents/twstock-bear-researcher.md:13` 直接叫 agent 去查這張 view，
  沒有任何 PIT 約束說明。

### SUSPECT 6 — 下游 `8_valuation.sql` / `9_valuation_1q.sql` 的視窗排序寫壞

`8_valuation.sql:102` 與 `9_valuation_1q.sql:104`：

```sql
over (partition by company_code order by year, quarter desc rows between 39 preceding and current row)
```

`year ASC, quarter DESC` 是混向排序，同檔上下兩行的 `eps_growth_rate_3y` /
`_5y` 都用 `order by year, quarter`。這 40 列的集合不是「最近十年」。
（屬下游 view，不在本單位修補範圍，但 `growth_analysis_ttm` 是它的輸入。）

---

## 4. 查過沒問題的（負結果，別再查一次）

### REAL 1 — 累計制差分本身是**對的**

台股損益表是當年累計數。`materialized_view/5_concise_income_statement_individual.sql:18-22`
用 `case when quarter = 1 then value else value - lag(value) over (partition by company_code, title order by year, quarter) end`：
Q1 直接取累計值（＝單季）正確，**跨年邊界正確**（新年 Q1 不會去減去年 Q4）。

實測 2330：2024 四季單季營收 592,644,201 / 673,510,177 / 759,692,143 /
868,461,178 千元；TTM 2024Q4 = **2,894,307,699 千元 ≈ 台積電 2024 全年 2.89 兆**，
TTM EPS = **45.25 = 台積電 2024 官方 EPS**。在沒有重複列的公司身上，
TTM 加總邏輯是對的。

（唯一殘留風險：某公司缺 Q3 時，Q4 的 `lag` 會抓到 Q2 累計數，使該「單季」變成
Q3+Q4 兩季——與 BUG 7 同一個「位移非日曆」根因。）

### REAL 2 — 幣別與單位一致，無混用

三張來源表 `value` 一律新台幣千元；`eps` 元/股；比率無量綱；所有 5 年比較都是
同欄位對同欄位（除了 BUG 2 那一處）。

順帶一提：`fcf_per_share`（`4_financial_index_ttm.sql:95-99`）的分母是
`total_capital_stock`（股本**金額**，千元）而不是股數，所以它其實是
「每千元股本的自由現金流」，約等於真 FCF/股 的 1/10（面額 10 元）。因為只拿來
跟自己比大小，`fcf_per_share_increase/decline_*` 旗標不受影響；但它與 `eps`
（真的元/股）放在同一張表裡並列，名字會誤導。

### OK 1 — 旗標與加總的算術正確

手算複核 2330 2024Q4：
EPS TTM 序列 45.25 / 32.34 / 39.20 / 23.01 / 19.97 / 13.32（現在～5 年前）→
- `eps_increase_5y_continuous` = **f**（32.34 < 39.20，鏈斷）✅
- `eps_increase_5y_overall` = **t**（45.25 > 1.2 × 13.32 = 15.98）✅
- `eps_decline_5y_continuous` = **f** ✅

三個都與 view 輸出一致。`coalesce(bool::INT, 0)` 的 NULL→0 沒有把整個分數變成
NULL。觀測域：`f_score` 0-9、`growth_score` 0-30、`drop_score` 0-20。

### OK 2 — 沒有 ±inf / NaN

`roa_growth_rate` 的 Infinity / −Infinity / NaN 計數 = **0**。所有 `*_growth_rate`
的分母都包了 `nullif(…, 0)`。極端值仍在（`|revenue_growth_rate| > 100` 有 75 列、
標準差 29.4），但那是「基期極小」的性質問題，不是 ±inf。

### OK 3 — 不在現役實盤資金路徑上

- `research/cache_tables.py:39` 明文移除本 view；`research/db.py:107` 註明不信任；
  `research/strat_lab/raw_quarterly.py:9` 寫 "We DELIBERATELY do NOT use"。
- 實測 `var/cache/cache.duckdb` 的 24 張表裡沒有它。
- 現役 Serenity `ev_v3_wf` 與 apex `strategy_s` 都不引用。

**仍在用的**：Scala `strategy/` 套件（已凍結為歷史參考：`QualityFilter`、
`MultiFactorStrategy:78`、`ValueRevertStrategy:66`、`ValueMomentumStrategy:70`、
`DividendYieldStrategy:49`、`Signals:139`）、PG 下游 view（`valuation`、
`valuation_1q`、`cbs_ttm_5y_over75`）、`Main research` 因子 IC 掃描
（`Main.scala:347-353`），以及 **copilot agent 的個股基本面／看空分析**——
最後這項是使用者會直接看到的判斷。

---

## 5. 效能備註（非正確性）

view 內含約 100 個 window function，且 `WHERE` 推不進去：全表掃 5.5 秒，
**只查一支股票也要 3.3 秒**（每次都重算全部 170,503 列）。任何 join 它的
分析查詢會直接爆掉（本次稽核有一支 join 查詢跑超過 3,600 秒被砍）。

---

## 6. 建議修法（依序）

1. **先修 fan-out（治本，一次解決 BUG 1 的三重傷害）**：
   `6_concise_financial_statement_with_titles.sql` 每個科目 CTE 都補上
   `distinct on (market, year, quarter, company_code)` + 明確的 `order by`
   科目優先序（照 `net_operating_income` CTE 既有寫法）。修完加一條守護測試：
   `select count(*) from (select company_code,year,quarter from
   concise_financial_statement_with_titles group by 1,2,3 having count(*)>1) t` 必須為 0。
2. **BUG 2**：行 113 與 330 的 `lag(total_assets_turnover, 20)` 改成
   `lag(equity_multiplier, 20)`。
3. **BUG 6**：七個反向欄位改名為 `*_reduction_rate`（或統一成
   `x/lag(x) - 1` 並讓消費者自己決定方向）。改名時要同步掃
   `8_valuation.sql` / `9_valuation_1q.sql` 的引用。
4. **BUG 4**：改成 `(rev/lag(rev,4) - 1) > 1.2 * (lag(rev,20)/lag(rev,24) - 1)`。
5. **BUG 5**：`1.2 ×` 改成對「改善量」判斷，例如
   `x > x20 + 0.2 * abs(x20)`，或直接規定基期 ≤ 0 時該旗標為 NULL。
6. **BUG 7**：window 改成先建連續季別索引（`year*4 + quarter`）再用
   `range between` 或 self-join 對齊日曆，讓缺季直接產生 NULL 而不是錯期比較。
7. **BUG 3**：`ocf` / `total_capital_stock` 為 NULL 時，對應的 f 項應該回 NULL
   並讓整個 `f_score` 變 NULL（「不知道」≠「0 分」），或至少額外輸出一欄
   `f_score_components_available` 讓消費者能自行排除。
8. **SUSPECT 4**：`4_financial_index_ttm.sql:91-92` 補 `nullif(total_assets, 0)`。
9. **SUSPECT 5**：`QualityFilter.scala:31-37` 加 `DISTINCT ON (company_code) …
   ORDER BY company_code, year DESC, quarter DESC` 收斂到最新一季；順手修
   行 20 的 "8 binary factors" 註解。
10. **SUSPECT 3 / 定位**：既然 `research/strat_lab/raw_quarterly.py` 已經是
    first-principles 的替代品，最乾淨的解是把這張 view 標成 deprecated、
    把兩支 agent 文件改指向 `raw_quarterly`，而不是繼續維護兩套定義不同的
    「F-Score」。若要保留，至少把欄位改名為 `quality_score_qoq` 之類、
    不要再叫 Piotroski。
