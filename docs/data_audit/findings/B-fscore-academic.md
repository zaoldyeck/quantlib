# B-fscore-academic — F-Score 全部實作 vs Piotroski (2000) 九項

判定:**🔴 BUG**(多項確認錯誤,含一個「兩套引擎算出來的不是同一個東西」)
稽核日:2026-07-22

---

## 一句話結論

**這個專案裡叫「F-Score」的東西有兩套,彼此只有 27% 的格子算出同一個分數;
其中資料庫那一套根本不是 Piotroski——它比的是「這一季 vs 上一季」,論文比的是
「今年 vs 去年」。Python 那一套定義大致對,但 2011 年以前的分數是假的(現金流
資料 2009 才開始,兩項固定給 0 分),而且金融股、剛上市的新股會被系統性壓低分數
——不是因為它們品質差,是因為算不出來就給 0 分。**

能不能用:
- **2012 年以後、非金融、上市滿三年的公司** → Python 的 `f_score_raw` 可用,但要
  知道它比正統 Piotroski 平均寬鬆 0.29 分、有 11% 的格子會在「≥5」這條線上判反。
- **2011 年以前** → 不能用,任何橫跨這段的回測結論都要重跑。
- **資料庫 view 的 `f_score`** → 不要用,它不是 Piotroski。

---

## 兩套實作在哪裡

| # | 位置 | 名稱 | 誰在用 |
|---|---|---|---|
| 1 | `src/main/resources/sql/view/5_growth_analysis_ttm.sql:3-37` | `growth_analysis_ttm.f_score` | Scala 端:`QualityFilter.scala`、`MagicFormulaPiotStrategy.scala:69`、`Main.scala:348` 因子掃描 |
| 2 | `src/quantlib/strat_lab/raw_quarterly.py:240-265` | `f_score_raw` | Python 端全部:`v4.py:92`、`iter_98`、`apex/experiments/{f02,q01,q02,b12,g04c}`、`quant_event_engine_v1.py`、`spike_factor_analysis.py`、`serenity/valuation_replay_2025.py` |

現役交易策略(Serenity `ev_v3_wf`)**沒有**用到 F-Score——查過
`src/quantlib/serenity/engine.py` 與 `src/quantlib/apex/strategy_s.py`,零命中。所以這批
問題不在實盤資金路徑上,但在**研究結論與 v4 基準線**上。

---

## 逐項對照 Piotroski (2000) 九項

原始九項(年度比較,分母一律用**年初**總資產;LEVER 用**平均**總資產):

| # | Piotroski 原定義 | PG view `f_score` | Python `f_score_raw` |
|---|---|---|---|
| 1 | ROA > 0 | `roa > 0`(TTM 淨利 / 平均資產)✅ | `ni_ttm / 期末資產 > 0` ⚠️ 分母用期末 |
| 2 | CFO > 0 | `ocf > 0` ✅ | `cfo_ttm > 0` ✅ |
| 3 | ΔROA > 0(vs 去年) | `roa > lag(roa)` = **vs 上一季** ❌ | `roa_ttm − roa_ttm[t−4] > 0` ✅ 期間對,分母不對 |
| 4 | CFO/TA > ROA(應計品質) | `ocf > profit` ✅(同分母下等價) | `cfo_ttm > ni_ttm` ✅ |
| 5 | Δ(長期負債/平均資產) < 0 | `長期負債**金額** < 上一季` ❌ 用金額不是比率、且比上一季 | `非流動負債/期末資產` vs 去年 ⚠️ 用非流動負債代長期負債、分母期末 |
| 6 | Δ流動比 > 0 | `current_ratio > lag` = vs 上一季 ❌ | vs 去年同季 ✅ |
| 7 | 當年未增發股本 | `股本 <= 上一季` ❌ 比上一季 | `Δ股本(年) <= 1` ⚠️ 台股語境問題(見下) |
| 8 | Δ毛利率 > 0 | `gross_margin > lag` = vs 上一季 ❌ | vs 去年同季 ✅ |
| 9 | Δ資產周轉率 > 0 | `total_assets_turnover > lag` = vs 上一季 ❌,且該欄位本身算錯(見 BUG-3) | vs 去年同季 ✅,分母期末 |

### 手算驗證:台積電 (2330) FY2024

用原始資料手算(千元):NI 1,172,431,759 / CFO 1,826,177,068 / 營收 2,894,307,699 /
毛利 1,624,353,564 / 年初總資產(2023Q4)5,532,371,215 / 前年底(2022Q4)4,964,778,878。

- **Piotroski 正統算法 = 8/9**(只有第 7 項失分:股本從 259,320,710 增到 259,327,332)
- **`f_score_raw` = 8** ✅ 九項旗標與手算逐項一致
- **PG view `f_score` = 7** ❌ 差在第 5、6 項比的是 2024Q3(季比季),而第 7 項因為
  Q3→Q4 股本沒變反而給了 1 分。用季比季的算式重算 view,結果精確為 7,**證實
  view 的 Δ 一律是季比季**。

---

## 確認的錯誤(BUG)

### BUG-1 🔴 PG view 的 F-Score 比的是「上一季」,不是「去年」

`5_growth_analysis_ttm.sql:8-36` 五個 Δ 項全部用 `lag(x)`(預設 offset = 1)。
底層 `financial_index_ttm` 是**季頻的 TTM**,所以 `lag(x)` = 上一季的 TTM。
Piotroski 的 Δ 是年度比較。這不是「近似」,是換了一個因子:季比季的 TTM 變動
主要反映最近一季 vs 去年同季的替換效果,雜訊高、且與論文報酬證據無關。

重現:上面台積電 FY2024 的逐項對帳。

### BUG-2 🔴 PG view 第 5 項用「長期負債金額」而不是「槓桿比率」

`5_growth_analysis_ttm.sql:6-11`:`total_non_current_liabilities < lag(...)`。
Piotroski 的 ΔLEVER 是 **長期負債 / 平均總資產** 的變化。用金額的後果:資產與
負債同比例成長的健康公司會被扣分,而縮表中的衰退公司會得分——訊號方向可能反轉。

### BUG-3 🔴 `total_assets_turnover` 的分母錯用「5 季前」的總資產

`4_financial_index_ttm.sql:69` 用 `lag(total_assets, 5)`,而同一個 CTE 裡的 `roa`
(第 18 行)用 `lag(total_assets, 4)`。**同一支 SQL 裡兩個平均資產用不同的期別**,
明顯是打字錯誤。

數字證明(2330,2024Q4):
- view 值 `total_assets_turnover = 0.4754`
- 用 lag 5(2023Q3 資產 5,484,556,381)重算:2,894,307,699 / ((6,691,938,000+5,484,556,381)/2) = **0.47540** ← 完全吻合
- 用正確的 lag 4(2023Q4 資產 5,532,371,215):**0.47353**

血統影響:PG view F-Score 第 9 項、`total_assets_turnover_*_5y_*` 全部成長/衰退旗標、
以及 `Main.scala` 因子掃描裡的 `total_assets_turnover`。

### BUG-4 🔴 `equity_multiplier` 的 5 年旗標拿「資產周轉率」當比較基準(複製貼上錯)

`5_growth_analysis_ttm.sql:112-113` 與 `329-330`:

```sql
equity_multiplier < lag(total_assets_turnover, 20) over (...) as equity_multiplier_decline_5y_overall,
equity_multiplier > lag(total_assets_turnover, 20) over (...) as equity_multiplier_increase_5y_overall,
```

權益乘數(平均 2.30)跟資產周轉率(平均 0.74)是兩種完全不同的量。實測 2015-2025:

```sql
WITH x AS (SELECT company_code, year, quarter, equity_multiplier,
  lag(equity_multiplier,20) OVER (PARTITION BY company_code ORDER BY year,quarter) em_lag20,
  lag(total_assets_turnover,20) OVER (PARTITION BY company_code ORDER BY year,quarter) at_lag20
  FROM financial_index_ttm)
SELECT count(*) n,
  count(*) FILTER (WHERE equity_multiplier < at_lag20) as_coded,
  count(*) FILTER (WHERE equity_multiplier < em_lag20) correct,
  count(*) FILTER (WHERE (equity_multiplier < at_lag20) <> (equity_multiplier < em_lag20)) disagree
FROM x WHERE year BETWEEN 2015 AND 2025 AND em_lag20 IS NOT NULL AND at_lag20 IS NOT NULL;
-- n=65589  as_coded=3397  correct=30933  disagree=29654 (45%)
```

`equity_multiplier_increase_5y_overall` 在 76,482 列裡有 62,240 列(81%)為真——
等於往 `drop_score` 灌了一個近乎常數的 +1。`drop_score < 10` 這道閘門
(`QualityFilter.scala:34`、`ValueRevertStrategy.scala:71`)因此比設計意圖嚴一格。

### BUG-5 🔴 PG matview 混用合併/個體報表,且跨年做累計差分 → 負營收

`materialized_view/5_concise_income_statement_individual.sql`:

1. **沒有 `type` 過濾**。它 `distinct on (...) order by ..., type`,`consolidated`
   字母序在前所以有合併就取合併,**但沒有合併的季別會靜靜掉回個體報表**。
   實測(`營業收入` 一欄):2006 年 4,471 個 (公司,季) 裡有 **2,346 個(52%)** 只有
   個體;2007 年 2,471/4,663(53%);2008-2012 每年約 580 個(~11%);2013-2014 殘留;
   2015 年起歸零。同一家公司的時間序列在合併與個體之間跳,ΔROA/Δ周轉/Δ毛利全部無效。
2. **累計差分的 `lag()` 沒有按年分區**(第 21-22 行 `partition by company_code, title`)。
   缺 Q1 的年度,Q2 會去減掉**前一年 Q4 的累計數**。

結果是負的營業收入。實測:

```sql
SELECT year, count(*) FILTER (WHERE value < 0) neg, count(*) n
FROM concise_income_statement_individual WHERE title='營業收入' AND market IN ('twse','tpex')
GROUP BY year ORDER BY year;
-- 2006: 196/4287   2007: 361/4624   2008: 110/4804 …  2024: 39/7279  2025: 33/7418
```

具體案例:1216 統一 2007Q3 `individual_rev = −109,953,456`(千元)。原始
`concise_income_statement_progressive` 的 2007 只有 Q2、Q4 的合併數,Q3 只有個體數
→ 個體 Q3 累計 − 合併 Q2 累計 = 大負數。

Python 端的 `raw_quarterly.py` **沒有**這個問題(`db.py:185,189` 有
`type='consolidated'` 過濾,`raw_quarterly.py:129` 的 `.over(["company_code","year"])`
有按年分區)——同樣的算式,兩邊寫法不同,這正是「同一個東西寫兩份必然漂移」的實證。

### BUG-6 🔴 「F-Score ≥ N」的閘門被寫成「歷史上曾經 ≥ N」

`src/main/scala/strategy/QualityFilter.scala:29-39` 少了 `DISTINCT ON`:

```sql
SELECT company_code FROM growth_analysis_ttm
WHERE company_code IN (...) AND (year < Y OR (year = Y AND quarter <= Q))
  AND COALESCE(drop_score,0) < 10 AND COALESCE(f_score,0) >= 5
ORDER BY company_code, year DESC, quarter DESC   -- 沒有 DISTINCT ON,ORDER BY 無作用
```

WHERE 先過濾、再 `.toSet` → 只要**歷史任一季**通過就永久通過。程式碼註解自己說的是
另一回事(第 24 行「Uses the latest ... snapshot」、第 38 行「distinct collapses to
one」)。兄弟實作 `Signals.latestQuarterField`(`Signals.scala:150`)與
`ValueRevertStrategy.dropScoreFilter`(第 65 行)都正確用了 `DISTINCT ON` ——證明
這是失誤不是設計。

**同類缺陷散在 Python 端**:`src/quantlib/strat_lab/v4.py:78-92` 的 `drop_safe` CTE
一模一樣(WHERE 在 DISTINCT ON 之前)。實測通過家數:

| 換股日 | 「曾經 ≥4」 | 「最新一季 ≥4」 | 多放行 |
|---|---|---|---|
| 2018-01-02 | 878 | 757 | +121(+16%) |
| 2021-07-01 | 951 | 841 | +110(+13%) |
| 2025-01-02 | 1024 | 883 | +141(+16%) |

不是前視偏誤(只看過去),但閘門比宣稱的鬆 16%,v4 基準線的「品質過濾」實際上
接近無效。

### BUG-7 🔴 缺資料一律給 0 分 → 2011 年以前的 F-Score 是假的

`raw_quarterly.py:243-259` 九項全部 `.otherwise(0)`,所以 NULL → 0 而不是 NULL。
分數永遠不是 NULL,呼叫端無法分辨「品質差」與「算不出來」。

**現金流資料 2009 年才開始**(`cash_flows_progressive` 最早 2009,1,269 家),
所以第 2、4 項在 2005-2009 是**全體 100% 給 0**:

| 年 | 平均 f_score_raw | `≥4` 通過率 | `cfo_ttm` NULL 比例 | f2 | f4 |
|---|---|---|---|---|---|
| 2006 | 0.00 | 0.0% | 100% | 0.000 | 0.000 |
| 2007 | 0.03 | 0.0% | 100% | 0.000 | 0.000 |
| 2008 | 1.16 | 0.2% | 100% | 0.000 | 0.000 |
| 2009 | 2.06 | 14.9% | 100% | 0.000 | 0.000 |
| 2010 | 3.43 | 53.1% | 78.3% | 0.163 | 0.119 |
| 2011 | 3.77 | 57.6% | 11.8% | 0.656 | 0.497 |
| 2016+ | 5.0-5.5 | 79-85% | 2-6% | 0.72-0.78 | 0.55-0.73 |

**「F-Score 逐年上升」是資料補齊的軌跡,不是台股品質變好。** 任何橫跨 2011 年
以前的 F-Score 因子檢定或回測都被污染。已知受影響:
`src/quantlib/apex/experiments/q02_pure_financial_books.py:27`(`SIM_START = 2007-07-02`
「全史」,品質書 Q = `f_score × gpoa × …` 幾何 rank)。
`iter_98`(`START = 2010-01-04`)前兩年也在畸變區。
apex F02/Q01/B12(2012-01-02 起)不受此項影響。

**同一個機制的另外兩個受害者**:

- **金融股**:`gross_margin_ttm` 100% NULL(銀行沒有「營業成本」)→ f8 恆 0、
  f9 僅 9.4% → 金融保險業平均 3.00、金融業 3.55,對比全市場 ~5.2。
  `f_score >= 4/5` 這種閘門等於「靜靜地把整個金融業剔除」——結果碰巧與 Piotroski
  排除金融業一致,但機制完全沒有寫在任何地方,而且做橫斷面排名時金融股不是被
  排除,是被塞在 3 分那一堆。
- **新上市公司**:資料長度不足 → Δ 項全 0。上市滿 0-3 季平均 0.46(通過 ≥5 僅 1.2%)、
  4-7 季 3.09、8-11 季 4.37、40 季以上 5.34。**任何 F-Score 閘門都內含一道
  沒人宣告過的「上市滿三年」濾網。**

### BUG-8 🟡→🔴 Python 版分母用「期末」總資產,Piotroski 用「年初」

`raw_quarterly.py:200-205`:`roa_ttm = ni_ttm / total_assets`(期末)、
`asset_turnover_ttm = rev_ttm / total_assets`(期末)、
`lt_debt_ratio = non_current_liab / total_assets`(期末,Piotroski 用平均)。

用同一份輸入依論文定義重算(分母改年初、LEVER 改平均),2013-2025 共 88,467 列:

- 逐格完全相同只有 **61.8%**
- 平均 5.02 → 4.73(現行實作系統性寬鬆 0.29 分)
- 逐項翻轉率:**第 9 項 24.9%**、第 3 項 12.6%、第 5 項 11.7%、第 1/4 項 4.3-4.4%
- **`≥5` 這道線有 11.0% 的格子判反**

第 9 項翻最兇有道理:資產成長中的公司用期末資產當分母,周轉率會被機械性壓低。

### BUG-9 🟡 TTM 與 YoY 是「數列數」不是「數日曆」

`raw_quarterly.py:162-163, 226-237` 的 `rolling_sum(4)` / `shift(4)` 是按**實體列**
移動。季別有缺口時,「TTM」會橫跨 5 個以上的日曆季,「去年同季」也不是去年同季。

實測:6,423 列(5.47%)的 TTM 視窗跨超過 4 個日曆季;6,563 列(5.58%)的
`shift(4)` 不是正好 4 季。主要集中在 2006-2007 半年報期,2018 年後仍有 479 列。

具體:1591(tpex)2023 年缺 Q2,`rev_q[2023Q3] = 33,404` 其實是 Q2+Q3 兩季合計
(原始 YTD:2023Q1 19,573 → 2023Q3 52,977),`rev_ttm[2023Q3] = 353,795` 涵蓋
2022Q3-2023Q3 共 5 個日曆季。

### BUG-10 🟡 欄位樞紐用 `MAX(value)`,同一格有兩個候選科目時挑「數字大的那個」

`raw_quarterly.py:85`:`MAX(value) FILTER (WHERE title IN (...))`。實測 2013 年起
89,006 個 (公司,季) 中:

- **87,326 格同時有**「本期淨利（淨損）」與「繼續營業單位本期淨利（淨損）」,
  其中 **1,168 格數值不同**,最大差距 **12,069,803 千元(約 NT$120 億)**
- **86,553 格同時有**「營業毛利（毛損）」與「營業毛利（毛損）淨額」,
  其中 **3,800 格不同**,差距可達營收的 ±2.1%(如 6177 2024Q4 差 0.0211)

`MAX` 挑大的 = **系統性挑對自己有利的那個數**,而且哪一個較大會隨季別變動 →
Δ毛利率(第 8 項)被灌入純雜訊。Piotroski 明確規定用**除非常項目前淨利**,
應該是固定的優先序,不是取最大值。

### BUG-11 🔴 兩套實作算出來的不是同一個因子

2012-2025、TWSE、共同的 52,581 個 (公司,年,季):

| 指標 | 值 |
|---|---|
| 逐格完全相同 | **27.3%** |
| 相關係數 | 0.646 |
| 平均(PG view / Python) | 5.47 / 5.01 |
| `≥5` 閘門一致率 | 76.6% |
| PG 過但 Python 不過 | 8,600 格 |
| Python 過但 PG 不過 | 3,692 格 |

違反 CLAUDE.md 的「引擎唯一真源鐵律」。Scala 回測與 Python 回測掛同一個名字
「F-Score」,測的是兩個不同的東西。

### BUG-12 🟡 第 7 項在台股語境不成立(兩套都有)

Piotroski 的 EQ_OFFER 問的是「有沒有**發行**普通股募資」。台股的股本會因為
**盈餘轉增資/資本公積轉增資(股票股利)**而增加,那不是募資,反而是獲利的證據。
兩套實作都拿股本增加當扣分。另外 `raw_quarterly.py:255` 的容忍值 `<= 1`(千元)
是沒有出處的魔術數字(註解只說 "small epsilon to allow rounding")。

台股正確做法應該比對「現金增資」事件(MOPS 有結構化來源)或普通股股數扣除
無償配股部分,而不是股本金額。

---

## 順帶查到的(同一批 view,非 F-Score 但同源)

| 項目 | 位置 | 問題 |
|---|---|---|
| `industry` 無 PIT | `4_financial_index_ttm.sql:2-6` | `distinct on (company_code) ... order by year desc, month desc` = 取**最新**產業別,前視。且直接用 `operating_revenue.industry`,違反 CLAUDE.md「產業別一律用 `industry_taxonomy_pit`」鐵律。影響 `capital_structure` → `cbs`(不影響 f_score) |
| `cash_ratio` 名不副實 | `4_financial_index_ttm.sql:26` | `cash / total_assets`,但現金比率的定義是 現金/流動負債。`cbs` 的 liquidity 分級門檻(0.25/0.2/0.15…)是照真的現金比率校準的 |
| `roic` 用稅後淨利 | `4_financial_index_ttm.sql:13-15` | ROIC 學理上是 NOPAT/投入資本;這裡用稅後淨利(已扣利息),且投入資本 `TA − 流動負債` 用期末、含閒置現金 |
| 負權益排名反轉 | `4_financial_index_ttm.sql:116` | `rank() ... order by equity_multiplier desc`,負權益 → 負乘數 → 排最後 → `capital_structure = 100.0`(最佳)。2013-2025 僅 9 列,影響小。實例:3043 2018Q1 em = −245.35 卻拿 100.0 |
| 成長率爆量 | `5_growth_analysis_ttm.sql:551-553` | `x/nullif(lag(x),0) − 1`,基期為負或極小時號稱「成長率」但無意義。2013-2025 有 99 列 \|roa_growth_rate\| > 100(最大 4,199)、119 列 \|eps_growth_rate\| > 100 |
| 註解過時 | `QualityFilter.scala:20` | 寫「8 binary factors」,view 實際加總 9 項 |
| PIT 慣例不一致 | `v4.py:76` vs `apex/assemble.py:118-126` | v4 用法定期限 +7 天緩衝(5/22、8/21、11/21、次年 4/7);apex 用**法定期限本身**(5/15、8/14、11/14、次年 3/31)且 `join_asof` 含等號 → 公告日當天盤後才送件的話會有最多 1 天前視。兩個數字都沒有量測出處 |

---

## 分母保護(查過,結論 🟢 OK)

- **Python 端**:`total_assets > 0` / `rev_q > 0` / `current_liabilities > 0` /
  `ni_ttm.abs() > 0` 全部有 `pl.when()` 護欄,不會產生 ±inf。副作用是負營收季
  (來自缺季差分)會靜默變 NULL → f8 拿 0(已計入 BUG-7 的機制)。
- **PG 端**:所有除法都包了 `nullif(x, 0)`,不會除以零、不會 ±inf。但只擋 0,
  不擋負數與極小值(見上表「負權益」「成長率爆量」)。

## 幣別與單位(查過,結論 🟢 OK)

三張原始表(`concise_income_statement_progressive` / `concise_balance_sheet` /
`cash_flows_progressive`)的 `value` 都是**新台幣千元**,同一量綱相除,F-Score 全部
九項都是比率或同單位比較,沒有單位混用。台積電 2024Q4 手算毛利率 0.5612 與
`gross_margin_ttm` 0.561224 吻合,交叉驗證單位一致。

## 累計制差分(查過,結論 🟢 Python OK / 🔴 PG 有 BUG-5)

台股損益表與現金流量表都是**當年累計數**。
`raw_quarterly.py:127-148, 176-180` 用 `quarter == 1 ? value : value − shift(1)`
並且 `.over(["company_code","year"])` **按年分區**——跨年邊界處理正確。
交叉驗證:台積電 2024Q4 單季營收 = 2,894,307,699 − 2,025,846,521 = 868,461,178,
與 panel 的 `rev_q` 一致。

PG 端的同一段(`5_concise_income_statement_individual.sql:19-22`)**沒有按年分區**
→ BUG-5。

---

## 建議修法(不在本單位執行)

依嚴重度排序:

1. **廢掉 `growth_analysis_ttm.f_score`**,並在 view 檔頂端註明它不是 Piotroski。
   或修成年度比較 + 槓桿比率(BUG-1/2)。既然 Scala 策略層已凍結,建議直接標為
   deprecated 並在 `QualityFilter` / `MagicFormulaPiotStrategy` 改指向 `f_score_raw`,
   讓 F-Score 只剩**一份實作**(唯一真源鐵律)。
2. **`raw_quarterly.py` 九項全部把 `.otherwise(0)` 改成 `.otherwise(None)`**,
   並新增 `f_score_n_valid` 欄位;消費端一律要求 `f_score_n_valid == 9` 才准用。
   同時在 docstring 明寫「2011 年以前不可用(CFO 缺料)」。
3. **修 BUG-3(`lag(total_assets, 5)` → `4`)、BUG-4(`total_assets_turnover` →
   `equity_multiplier`)**——兩行改完就好,但改完 `drop_score`/`growth_score`
   的分佈會變,所有引用過這兩個分數的結論要重跑。
4. **`QualityFilter.eligible` 補 `DISTINCT ON (company_code)`,`v4.py` 的
   `drop_safe` 把 `f_score_raw >= 4` 從 `WHERE` 移到 `DISTINCT ON` 之後的外層**
   (BUG-6)。修完 v4 基準線 CAGR 會變,要重新標定
   `quantlib-data-refresh` skill 的 2pp 迴歸門檻。
5. **分母改年初/平均總資產**(BUG-8)。這會讓 F-Score 平均降 0.29、11% 的
   `≥5` 判定翻轉,所以要當成一次因子改版:先跑 IC 對照(舊 vs 新)再決定採用。
6. **`_pivot_titles` 改成優先序取值**(`COALESCE(繼續營業單位本期淨利, 本期淨利)`、
   `COALESCE(營業毛利淨額, 營業毛利)`),不要 `MAX`(BUG-10)。
7. **`rolling_sum(4)` / `shift(4)` 改成以 `year*4+quarter` 為鍵的日曆對齊
   join**,或至少加一個 `窗口跨季數 == 4` 的守護欄(BUG-9)。
8. **金融業明確排除**(Piotroski 原文即排除金融業),用
   `industry_taxonomy_pit` 的 PIT 產業別做 `f_score_raw = NULL`,而不是讓它們
   靜靜落在 3 分。
9. **第 7 項改用現金增資事件**而非股本金額(BUG-12);epsilon 若保留,要附量測出處。
10. **加防復發守護**:一支 `src/quantlib/tests/test_fscore_piotroski.py`,鎖死
    「台積電 FY2024 = 8/9,逐項旗標 1,1,1,1,1,1,0,1,1」這個手算錨,再加
    「2011 年以前的 `f_score_raw` 必須為 NULL」的斷言。先紅後綠。

---

## 這一輪查了什麼(避免下次重查)

- 精讀 `5_growth_analysis_ttm.sql`(684 行)、`4_financial_index_ttm.sql`(206 行)、
  `6_concise_financial_statement_with_titles.sql`、`5_concise_income_statement_individual.sql`、
  `3_cash_flows_individual.sql`、`1_concise_balance_sheet_individual.sql`
- 精讀 `src/quantlib/strat_lab/raw_quarterly.py`(321 行)、`src/quantlib/db.py`、
  `QualityFilter.scala`、`Signals.latestQuarterField`、`ValueRevertStrategy`、
  `PublicationLag.asOfQuarter`、`v4.py` 的 `qfor`/`drop_safe`、`apex/assemble.py` 的 `q_avail`
- 盤點全部 f_score 提及點(35 個檔),確認只有兩套實作,現役 Serenity 策略未使用
- 手算台積電 FY2024 全九項並與兩套實作對帳
- 量化:兩套實作一致率、期末 vs 年初分母的翻轉率、季別缺口比例、
  科目樞紐碰撞率、逐年可用性、產業別可用性、上市年資偏誤、
  「曾經通過」閘門的放行差額
- 檢查分母保護、幣別單位、累計制差分的跨年邊界
