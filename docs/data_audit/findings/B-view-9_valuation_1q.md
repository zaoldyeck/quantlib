# B-view-9_valuation_1q — 財務定義與算式審查

審查對象:`src/main/resources/sql/view/9_valuation_1q.sql`
(公式與 `8_valuation.sql` 完全相同;`_1q` 只多三道「近期」過濾器讓它跑快一點,
所有算式問題兩張 view 一模一樣。)

## 一句話結論

這張估值表分兩半:

- **「相對估值」半**(股價在回歸通道的位置、PER / PBR / 殖利率的 3.5 年分位)
  算得乾淨、沒偷看未來,可以拿來看一支股票相對自己歷史是貴還是便宜。
  **但有一個號誌燈壞了**:最便宜的等級 `evaluation = +2` 永遠不會亮,
  真正超便宜(跌破 −2σ)的股票被錯標成只是「便宜」(+1)。
- **「DCF 內在價值」半**(`dcf_1y/3y/5y/10y` 與對應的 `*_err`)**是壞的,不能拿來選股**。
  它用 EPS 年增率當成長率,但全市場有 **1/4** 的季度 EPS 是負的或接近零,
  一除下去成長率就爆掉,算出來的每股內在價值可以飆到 10^158 元這種天文數字
  (大魯閣股價 16 元、DCF 卻說值 1.8 兆元/股)。外加十年期成長率的視窗排序寫錯,
  **偷看了同年後面幾季的未來資料**(look-ahead)。

**殺傷範圍**:目前**沒有任何實盤或回測程式讀這張 view**——apex 直接讀
`stock_per_pbr` 資料表(`research/apex/data.py:204`),Serenity / 引擎都不碰它,
cache 也沒有它。全庫搜尋 `valuation_1q` 只有前一位稽核員的筆記引用到。
所以它是一張「手動查表 / BI 分析用」的 view,錯的數字不會直接汙染下單決策,
但只要有人拿 DCF 欄位排序選股,排最前面的一定是這些爆掉的垃圾值。

---

## 逐欄公式與判定

### DCF 半 — 壞

**根因鏈**:
`eps`(來自 `financial_index_ttm`,是 4 季加總的 **TTM EPS**,量綱正確)
→ `eps_growth_rate_1y = eps / lag(eps,4) - 1`(YoY)
→ `eps_growth_rate_3y/5y/10y` 是 1y 的移動平均
→ `x = (1 + 成長率) / (1 + 0.12)`
→ 兩段式幾何級數 DCF:`eps·x·(1−x¹⁰)/(1−x) + eps·x¹⁰·y·(1−y¹⁰)/(1−y)`。

DCF 的**數學形式本身是對的**(10 年成長 + 10 年終值,折現率 12%、終值成長 4%,
`nullif` 有擋 x=1 / y=1 的除零)。壞在**輸入的成長率**:

1. **BUG — DCF 爆炸,欄位不可用。** `eps / lag(eps,4) - 1` 這種比率型成長率,
   在分母(去年 EPS)是負的或接近零時會噴出荒謬的值。實測 `growth_analysis_ttm`
   2023 年起 **22,737 筆裡有 5,554 筆 EPS < 0(24.4%)**、13 筆 = 0。
   跨全市場當期 115,799 筆 `valuation_1q`:`dcf_10y_err` 從 **−2.9×10¹⁵³ 到 +4.9×10¹⁵²**,
   **6,191 筆宣稱「便宜超過 100 倍」**、**16,474 筆內在價值是負的**。
   具體案例:**大魯閣(1432)TTM EPS 6.03、股價 16 元,`dcf_10y = 7.86×10¹⁵⁸`、
   `dcf_1y = 1.81×10¹²`(每股 1.8 兆)**——它的 EPS 剛從近零翻正,成長率算出 −1042%,
   x¹⁰ 一乘就上天。任何拿 `dcf_*_err` 由大到小排序的選股,榜首全是這種名字。

2. **BUG — `eps_growth_rate_10y` 視窗排序寫壞(line 104):look-ahead + 集合錯誤。**
   ```sql
   sum(eps_growth_rate_1y) over (
     partition by company_code order by year, quarter desc  -- ← desc 只作用在 quarter
     rows between 39 preceding and current row) / 40
   ```
   `year ASC, quarter DESC` 是混向排序(同檔上一行 `_3y`、`_5y` 都是正的
   `order by year, quarter`)。這 40 列**不是**「最近十年」。
   實測 2330:**每一個非 Q4 的季度**,它的 10 年視窗最大期別都是**當年 Q4(未來)**——
   例如 2020Q1(期別 20201)的視窗最大期別是 20204(2020Q4),
   等於在 2020Q1 就用到 2020Q4 才知道的資料(前視偏誤)。而且 buggy 與正確排序的值
   差很多(2020Q1:0.1425 vs 0.1821;2022Q1:0.2042 vs 0.1681),
   這個差再被 x¹⁰ 放大進 `dcf_10y`。只影響 `dcf_10y` 系列(1y/3y/5y 排序是對的)。

3. **SUSPECT — 3y/5y/10y 平均除以固定 12/20/40。** `sum(...) rows between 11/19/39
   preceding / 12/20/40`,但 sum 會略過 NULL、且新股上市不足視窗長度時列數不夠,
   分母仍固定 → **上市未滿 3/5/10 年的股票平均值被系統性低估**。應除以視窗內實際列數。

### 相對估值半 — 大致可信,但一個號誌燈壞了

4. **BUG — `evaluation = +2`(超便宜)永遠不會亮(line 179-184 死分支)。**
   ```sql
   when closing_price >= highest then -2   -- 貴側:先判 highest(±2σ)再判 high,正確
   when closing_price >= high    then -1
   when closing_price <= low      then 1   -- 便宜側:先判 low(−1σ)…
   when closing_price <= lowest   then 2   -- …但跌破 lowest(−2σ)必然也 <= low,
   else 0 end                              --    所以永遠先回 1,這行是死碼
   ```
   `lowest < low`,所以 `price <= lowest` 一定先被 `<= low then 1` 攔截。
   實測 `select distinct evaluation`:出現 **−2 / −1 / 0 / 1,就是沒有 2**
   (−2 有 15,151 筆、+2 有 0 筆)。**貴的極端(−2)判得出來、便宜的極端(+2)判不出來**,
   超便宜股票被錯標成普通便宜。修法:便宜側把 `lowest` 排在 `low` 前面。

5. **OK — `per_err` / `pbr_err` / `dividend_yield_err` 乾淨、PIT 安全。**
   都是 `(H + L − 2x)/(H − L)` 形式,H/L 是**含當列**的 3.5 年滾動極值,
   所以當列的 x 一定落在 [L,H] 之間 → 三個 err 實測嚴格落在 **[−1, 1]**(無 ±inf)。
   方向一致(便宜 / 高殖利率 = 正)。TWSE 對虧損公司的 PER **給 NULL 不給負值**
   (近 4 年 0 筆負 PER),所以 PER 極值範圍不會被負值污染。`price_err` 用含當列的
   3.5 年回歸擬合值,也不偷看未來。

### PIT(時點對齊)— 這部分做對了

6. **OK — DCF 的 月→季 對應(line 218-223)是「公告落後」保守版,沒有前視。**
   價格所在月份對應到「當時已公告」的財報季:
   1-3 月→前一年 Q3、4-5 月→前一年 Q4(年報)、6-8 月→當年 Q1、9-11 月→當年 Q2、
   12 月→當年 Q3。對照台股法定申報期限(Q1 5/15、Q2 8/14、Q3 11/14、年報 3/31),
   每一格都取「保證已公布」的那季,偏保守(用月界、不用精確申報日)但**無 look-ahead**。
   注意:此 PIT 只保護 DCF join;`eps_growth_rate_10y` 的 look-ahead(BUG 2)是在更上游
   算平均時就發生了,PIT join 擋不到。

---

## 其他(非本 view 的正確性,但要知道)

- **繼承上游 bug**:passthrough 的 `*_growth_rate` 有 7 個反向欄
  (`days_sales_of_inventory_growth_rate` 等用 `1 − x/lag(x)`)以及 `growth_analysis_ttm`
  自身的多個 bug,已記在 `docs/data_audit/findings/B-view-5_growth_analysis_ttm.md`。
  好消息:**這些欄都不進 DCF**(DCF 只用 `eps`),純顯示用,問題等上游修。
- **回歸通道帶寬**:`sd = stddev(closing_price)`(價格總標準差)不是回歸殘差標準差,
  對趨勢股帶寬會偏寬(真正的 regression channel 應用殘差 σ)。自洽、影響有限,列為體例小疵。
- **join 少配 market(line 30-33)**:`left join daily_quote on (dq.market='twse' or 'tpex')
  and date and code`——**少了 `sppdy.market = dq.market`**。這條 join 是 `closing_price`
  的唯一來源(sppdy 沒有收盤價),不是死 join。實測 `daily_quote` 同 (date,code) 跨兩市場
  **0 筆**(台股代號全市場唯一),所以現在不會爆列;但屬潛在的 rank 撞號 / 重複列風險,
  且缺配時 `closing_price` 會靜默變 NULL。應補上 market 條件。
- **效能**(非正確性):view 內約 100 個 window function、WHERE 推不進去,
  單查一支股票也要數秒,任何 join 它的查詢會爆(本次一支 join 查詢跑到被 timeout 砍)。

## 可重跑證據(節選)

```sql
-- BUG 1 DCF 爆炸(全市場當期)
select count(*) filter (where dcf_10y_err>100) gt100, count(*) filter (where dcf_10y<0) neg
from valuation_1q;                                   -- 6191, 16474
-- BUG 2 look-ahead(2330;非 Q4 列 frame_max = 當年 Q4)見報告內 SQL
-- BUG 4 死分支
select distinct evaluation from valuation_1q;        -- {-2,-1,0,1},無 2
-- EPS 24% 為負
select count(*) filter(where eps<0), count(*) from growth_analysis_ttm where year>=2023; -- 5554/22737
```
