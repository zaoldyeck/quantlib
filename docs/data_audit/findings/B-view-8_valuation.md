# B-view-8_valuation — 財務定義與算式審查

**對象**：`src/main/resources/sql/view/8_valuation.sql`（`valuation` view）
**稽核日**：2026-07-23
**結論**：**BUG**——這支 view 的「估值」核心（DCF 內在價值）算出來的數字**不能拿來選股**。

---

## 一句話白話

這支 view 想做兩件事：一是畫價格的「回歸通道」（貴/便宜的技術面帶），二是用 DCF 算股票的「內在價值」。**通道那半部大致沒問題，DCF 這半部整個壞掉**：

- 在 2025-06-16 這種平常日、1799 檔股票裡，就有 **313 檔（17%）** 的 DCF 合理價喊到市價 **10 倍以上**，最誇張的到 **10 的 152 次方**（不是打錯，是真的算出這種天文數字）。
- 由虧轉盈的公司（明明是大利多）反而被算出**負的合理價**（例如 2704 的 dcf_5y = −335）。
- 還有一個明確的「偷看未來」bug：號稱「過去 10 年平均成長率」的欄位，實際把**同一年後面幾季（未來）**也算進去了。

好消息是：把股價日對映到「當時真的已經公告」財報季的那段邏輯**做得對且保守**（不會用到未來財報），DCF 用的每股盈餘也正確用了近四季（TTM）。而且**這支 view 現在沒有任何程式在用**（不在 cache、實盤也不讀它），所以壞掉的是研究/歷史回測的可信度，不是實盤下單。

---

## 能不能信這份資料？

| 欄位群 | 能不能信 | 說明 |
|---|---|---|
| `dcf_1y / dcf_3y / dcf_5y / dcf_10y` 及對應 `_err` | **不能** | 成長率沒有上限、又連乘十年，17% 的股票在任一天都爆成天文數字；虧轉盈股得到負合理價 |
| `dcf_10y / dcf_10y_err`（額外） | **不能，且含前視** | 「過去 10 年平均成長」把同年未來季灌進來 |
| `evaluation / price_err / highest…lowest`（回歸通道） | **可，但屬技術面** | 3.5 年時間趨勢 ±1/2σ，機制健全，但這是「近期價格貴不貴」，不是基本面估值 |
| `per_err / pbr_err / dividend_yield_err` | **大致可，邊角不穩** | 3.5 年高低區間的正規化分數；區間極小或虧損股 PER 為負時會歪 |
| 財報面穿透欄位（`f_score / roic / roa / *_growth_rate`） | 見 **B-fscore-academic** | 這些是從 `growth_analysis_ttm` 直接 SELECT 過來，非本 view 新算，另單已審 |
| 2006–2012 的任何列 | **不能** | 上游一季多列（最多 64 列），本 view 會 fan-out 成多筆互相矛盾的估值 |

---

## 明確的 BUG（有可重現證據）

### BUG-1｜DCF 合理價全毀：成長率無上限 × 連乘十年 → 爆炸
公式（`8_valuation.sql:116-128`）：`dcf = eps · x·(1−x¹⁰)/(1−x) + …`，其中 `x = (1+g)/(1+r)`。
成長率 `g` 沒有任何上限，`g` 一大 `x` 就遠大於 1，`x¹⁰` 變天文數字。

實測（單日 `date='2025-06-16'`，1799 檔非空）：
- `dcf_1y_err ∈ [−1.3×10¹⁵², 7.8×10¹⁴⁹]`
- **313 檔（17.4%）`dcf_1y_err > 1000%`**，`dcf_10y_err` 309 檔 > 1000%（min −3.2×10¹⁵⁶ / max 1.65×10¹⁵⁶）

```sql
WITH v AS (SELECT dcf_1y_err,dcf_10y_err FROM valuation
           WHERE date='2025-06-16' AND dcf_1y_err IS NOT NULL)
SELECT count(*), count(*) FILTER(WHERE dcf_1y_err>10), max(dcf_10y_err) FROM v;
```
**修法**：成長率須 clamp（如 [−0.5, 0.30]）；第二段改用會收斂的永續 Gordon 而非再連乘十年；或整組 DCF 欄位廢用並在檔頭標註「數值不可用」。

### BUG-2｜前視：`eps_growth_rate_10y` 用了 `order by year, quarter desc`
`8_valuation.sql:101-103` 的視窗排序寫成 `order by year, quarter desc`，而 3y/5y（`:95-100`）正確用 ASC。
`desc` 讓「同年、季別較大」的**未來季**排到 preceding，被灌進「過去 10 年平均」。

合成證明（ROWS 3 preceding）：DESC 版 (2021,Q1) 視窗 = `{204,203,202,201}`（含 2021 Q2/Q3/Q4 未來），ASC 版正確為 `{102,103,104,201}`。
此欄 → `x_10y` → `dcf_10y` → `dcf_10y_err`，任何用 `dcf_10y` 排名都含未來資訊。
**修法**：移除 `desc`。

### BUG-3｜基期 EPS 為負 → 成長率反號 → DCF 變負數/歸零
`8_valuation.sql:91-92`：`eps / nullif(lag(eps,4),0) − 1` 只擋「剛好 0」，不擋負數。
虧轉盈（強利多）被算成巨大負成長：
- 1459：EPS −0.04 → +1.55，報 **−3975%**
- 2408：EPS −1.88 → +11.17，報 **−694%**

端到端（`date='2025-06-16'` 對映 2025Q1）：2408 → `dcf_1y=−5.9 / dcf_5y=−64.0`；2704 → `dcf_5y=−334.9`（合理價為負，無意義）；1459 → dcf 全 0。
（同一「負基期成長率」缺陷類也出現在 `growth_analysis_ttm`，見 B-fscore SEC-4；此處是本 view 新算、專餵 DCF 的獨立一份。）
**修法**：`lag(eps,4) <= 0` 時輸出 NULL；與 SEC-4 同批修。

### BUG-4｜最終 join 不容忍上游一季多列 → fan-out 成多筆矛盾估值
`growth_analysis_ttm` 部分 `(code,year,quarter)` 有 2/4/8/**64** 列（413/180/118/147 組）。
`8_valuation.sql:214-220` 的 join 未去重，對單一 (date,company) 產出多列估值：

```
valuation WHERE company_code='1409' AND date='2008-07-01'  → 8 列
  eps 0.23/0.18/0.13/0.08×5，dcf_1y 0.027 ~ 0.675（全不同）
```
**時間分布：全部落在 2006–2012（2013+ 為 0）**，根因屬 B-fscore BUG-5 類（`concise_income_statement_individual` 缺 `type` 過濾、合併/個體與多版報表堆疊）。2330 等主流股與近期資料乾淨。
另外 `lag(eps,4)` 在重複列上會落錯位置（不是真正 4 季前）。
**修法**：上游修 type 過濾 + 去重為本；本 view 亦應對 dcf 來源加 `DISTINCT ON (company_code,year,quarter)` 防禦。修好前，跨 2006–2012 的回測不可用此 view。

---

## 疑慮（SUSPECT，證據較弱或影響較小）

- **SUSPECT-1｜固定除數壓低年輕股成長率**：3y/5y/10y 平均用固定 `/12 /20 /40`（`:95-103`），上市未滿期者 `sum(<N 項)/N` 系統性偏低 → DCF 低估。受影響：<12 季 202 家、12–19 季 90 家。修法：除數改 `count(*) over(同框)` 或用 `avg()`。
- **SUSPECT-2｜區間正規化分母只擋剛好 0**：`per_err/pbr_err/dividend_yield_err`（`:183-188`）在 3.5 年區間極小時爆量；虧損股 PER 為負會污染高低帶。修法：加最小區間門檻、`PER<=0` 先濾。

---

## 查過沒問題（OK，負結果落盤）

- **OK-1｜PIT join 保守正確**：`:214-220` 的 month→quarter 對映，逐段對台股申報期限（年報 3/31、Q1 5/15、Q2 8/14、Q3 11/14）核對——**從不使用未來財報**，最多在申報日前後略舊（安全方向）。
- **OK-2｜DCF 基底 eps 是 TTM**：2330 逐季 39.36/37.23/34.54/32.34，用年度盈餘能力當基底正確。
- **OK-3｜DCF 幾何級數代數本身無誤**：兩段式閉式正確、x=1/y=1 極點有 nullif 保護；問題在輸入成長率不在公式。
- **OK-4｜價格通道機制健全**：`closing_price` 來自 `daily_quote`（sppdy 無此欄）、無 (date,code) fan-out、`rank()` 無間隙；通道=3.5 年時間趨勢 ±1/2σ，屬合理技術面建構。
- **OK-5｜不在實盤/cache 路徑**：DuckDB cache 無此表、無任何程式 select from `valuation`；為凍結 Scala 層遺產，缺陷影響限研究/歷史回測。

---

## 我實際查了什麼

精讀 220 行全文（七個 CTE 每個計算欄位的分子/分母/期間/幣別/單位）；確認 closing_price 來源與 join 無 fan-out；確認 eps 為 TTM；逐段核對 PIT join；合成 window 查詢證明 10y 前視；找真實虧轉盈案例證明負基期反號並端到端在 view 看到 DCF 變負；單日量化 DCF 爆炸（17% >1000%）；量化上游重複規模與時間分布並實測 1409 fan-out；量化固定除數影響家數；確認 DCF 級數代數正確；確認 view 不在 cache／無消費者；與 B-fscore-academic 交叉比對避免重複審查穿透欄位。
