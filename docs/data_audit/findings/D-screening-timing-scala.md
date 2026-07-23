# D-screening-timing-scala — 篩選/擇時/PIT 學理稽核

**範圍**:`src/main/scala/strategy/Universe.scala` + `QualityFilter.scala` + `PublicationLag.scala` + `RebalanceCalendar.scala`

**一句話結論**:PIT 公告延遲(PublicationLag)、普通股界定(Universe 正則)、再平衡日曆
(RebalanceCalendar)三者學理正確;唯一 BUG 在 `QualityFilter` —— SQL 少了 `DISTINCT ON
(company_code)`,把「本季體質達標」的否決閘門實作成「歷史任一季曾達標就永久放行」,實測
在任一 PIT 日約有 40~49% 的通過名單其實是「最新一季已惡化、依學理應被否決」的公司。

---

## 1. BUG — QualityFilter 未收斂到最新一季(`QualityFilter.scala:29-39`)

### 學理定義
Quality veto 的定義是「以**決策日可得的最新一季**財務快照」判斷公司當下體質是否達標
(`f_score >= 5` 且 `drop_score < 10`)。這是 point-in-time 因子的標準做法:每個
company 取 `argmax_{(y,q) <= PIT} (y,q)` 的那一列做判斷(latest-available snapshot)。
程式自己的 docstring 也這樣寫:第 24 行「Uses the **latest** growth_analysis_ttm
quarterly snapshot available on D」、第 38 行「distinct collapses to one」。

### 程式實作
```sql
SELECT company_code
FROM growth_analysis_ttm
WHERE company_code IN (...)
  AND (year < Y OR (year = Y AND quarter <= Q))        -- PIT 上界:正確
  AND COALESCE(drop_score,0) < 10 AND COALESCE(f_score,0) >= 5   -- 濾在「每一列」上
ORDER BY company_code, year DESC, quarter DESC          -- 沒有 DISTINCT ON,ORDER BY 無作用
```
之後 Scala 端 `.as[String].toSet`(第 39 行)只做 company_code 去重。

問題:`WHERE` 的體質條件套在 **PIT 上界內的每一季**,`SELECT` 只投影 `company_code`,
`.toSet` 去重後,**membership = 該公司歷史上「任一季」曾通過**,而不是「最新一季」通過。
`ORDER BY company_code, year DESC, quarter DESC` 沒有搭配 `DISTINCT ON (company_code)`
或 window `ROW_NUMBER()=1`,對「只投影一欄再 toSet」的結果完全沒有作用 —— 是一段
寫了一半、意圖是 `DISTINCT ON` 卻沒寫出來的殘骸。等價於「once-passed ⇒ always-passed」。

### 偏差證據(可重現,PG `quantlib`)
對每個 PIT 日,比較「現行 buggy 版」與「學理正確版(先 `DISTINCT ON` 取最新季、再套體質
條件)」通過家數:

| PIT 決策日 → 季別 | buggy 通過 | 學理正確通過 | 誤放(最新季其實不合格) |
|---|---|---|---|
| 2024-06-01 → 2024Q1 | 1033 | 624 | **409(佔 buggy 池 40%)** |
| 2020-06-01 → 2020Q1 | 967 | 490 | **477(佔 buggy 池 49%)** |

具體誤放樣本(PIT=2024Q1,最新可得季即 2024Q1):
- **台塑 1301**:最新季 2024Q1 `f_score=5, drop_score=14`(drop≥10 應否決)→ 卻因
  2022Q3 曾 `drop_score=8` 被放行。
- **大飲 1213**:最新季 2024Q1 `f_score=3, drop_score=13`(明顯不合格)→ 卻因
  **2017Q3**(七年前!)曾 `f_score=7, drop_score=8` 被放行。
- 台泥 1101(最新 drop=12)、南亞 1303(最新 f=4)、台聚 1304(最新 f=4)同型。

重現 SQL(擇要):
```sql
WITH pit AS (SELECT company_code,year,quarter,f_score,drop_score FROM growth_analysis_ttm
             WHERE market='twse' AND (year<2024 OR (year=2024 AND quarter<=1))),
buggy AS (SELECT DISTINCT company_code FROM pit
          WHERE COALESCE(drop_score,0)<10 AND COALESCE(f_score,0)>=5),
latest AS (SELECT DISTINCT ON (company_code) company_code,f_score,drop_score FROM pit
           ORDER BY company_code,year DESC,quarter DESC),
correct AS (SELECT company_code FROM latest
            WHERE COALESCE(drop_score,0)<10 AND COALESCE(f_score,0)>=5)
SELECT (SELECT count(*) FROM buggy), (SELECT count(*) FROM correct),
       (SELECT count(*) FROM buggy WHERE company_code NOT IN (SELECT company_code FROM correct));
```

### 影響面
消費端為 `AlphaStackStrategy.computeComposite`(`AlphaStackStrategy.scala:67`,為第一道
硬否決 hard filter)。QualityFilter 的宗旨(自身 docstring)是「prevent picking up
obviously **deteriorating** companies」,而此 bug 恰好讓「當下正在惡化、但歷史上有過一
季好日子」的公司整批漏網 —— 完全反轉了否決閘門的設計意圖,且是無聲的(通過家數看起來
還「更多、更寬鬆」,不會報錯)。

### 修法
先 `DISTINCT ON (company_code)` 收斂到最新季,**再**套體質條件(WHERE 在 DISTINCT ON
之後),與同 repo 既有正確樣板一致(`ValueRevertStrategy.dropScoreFilter:65`、
`Signals.latestQuarterField:150` 都已正確用 `DISTINCT ON`):
```sql
SELECT company_code FROM (
  SELECT DISTINCT ON (company_code) company_code, f_score, drop_score
  FROM growth_analysis_ttm
  WHERE company_code IN (...) AND (year < Y OR (year = Y AND quarter <= Q))
  ORDER BY company_code, year DESC, quarter DESC
) latest
WHERE COALESCE(drop_score,0) < MaxDropScore AND COALESCE(f_score,0) >= MinFScore
```
(此 bug 亦見於既有稽核 `docs/data_audit/findings/B-fscore-academic.md:156-172`;本單位補上
2024Q1/2020Q1 的量化誤放證據。注意:`growth_analysis_ttm.f_score` 本身非正統 Piotroski、
`drop_score` 有近常數 +1 膨脹 —— 那是 view 層問題,屬 B 單位範疇,不在本四檔內。)

---

## 2. SUSPECT(低)— PublicationLag Q2 全產業用 8/14,金融保險證券法定為 8/31(`PublicationLag.scala:34`)

### 學理定義
證交法 §36:一般產業 Q2 財報「每季終了後 45 日內」→ 8/14;但**金融控股/保險/證券**業
第二季財報法定期限為 **8/31**(較長)。

### 程式實作
`case 2 => LocalDate.of(year, 8, 14).plusDays(7)` = 8/21,全產業一體適用。

### 偏差
對金融保險證券類股,真正保證公告日是 8/31;用 8/21 當「可安全使用日」對這些公司會**提前
約 10 天**看到 Q2 財報 = 窄幅前視(look-ahead)。

### 判定
低嚴重度、範圍窄:①緩衝 +7 已吸收一半;②`f_score >= 5` 閘門本就幾乎剔除整個金融業
(見 `B-fscore-academic.md:212`),受害樣本極少。屬「未分產業的合理簡化」但非零風險,
記錄備查。修法(若要嚴謹):Q2 對金融保險證券類股改用 8/31 期限。

---

## 3. OK — Universe 流動性門檻用「中位數」成交值(命名為 ADV 略有出入)(`Universe.scala:53`)

- 學理:ADV = Average Daily Value = 日成交值**平均**。
- 實作:`percentile_disc(0.5) WITHIN GROUP (ORDER BY trade_value)` = **中位數**;變數名
  `MinMedianTradeValue`、docstring 第 19 行都寫「median」,內部自洽。僅第 25 行 deferred
  註解口語稱「ADV-based」略不精確。
- 判定:**OK**。流動性下限用中位數是**更穩健**的做法(不被單日爆量拉高),許多指數編制亦
  採中位數日成交額;非學理錯誤,只是「ADV」一詞為 misnomer。門檻 NT$50M / 視窗 30 日曆日
  (`>` 下界、`<=` 上界)/ 最少 10 個交易日,皆為設計參數,非學理可證偽量。

---

## 4. OK — 普通股界定 `^[1-9][0-9]{3}$` + etf 表二次防護(`Universe.scala:59,65`)

- 學理:普通股應排除 ETF、特別股、TDR、受益憑證等非普通股。
- 實作:4 位純數字、首位 1-9 → 排除 0xxx/00xxxx ETF、5-6 位 TDR、字母尾特別股;再
  `NOT IN (SELECT company_code FROM etf)` 兜底任何 1-9 開頭 ETF。
- 實測(daily_quote twse, 2024+):正則命中集合中 **無** `-DR`(TDR)、無 `甲特/乙特/特別股`;
  唯一的「外國」名稱是 94 檔 `-KY`(第一上市外國企業普通股,**本應保留**)。判定:**OK**,
  普通股界定乾淨。deferred 的市值下限/財報深度門檻因 balance_sheet 稀疏而暫緩,有註解、
  合理。

---

## 5. OK — PublicationLag 季/月報期限與 PIT 選取邏輯(`PublicationLag.scala:32-74`)

- 季報期限:Q1 5/15、Q2 8/14、Q3 11/14、年報(Q4)次年 3/31 —— 與證交法 §36 一般產業
  規定逐項相符;月營收「次月 10 日」亦符公司法/證交法。
- 緩衝 +7(季)/+3(月):只會**延後**可用日、永不洩漏未來資料,方向正確且保守,有 docstring
  說明 —— 屬刻意合理保守化,非 bug。
- `asOfQuarter` / `asOfMonthlyRevenue`:在候選 (y,q) 中取「buffered 期限 ≤ d 者」的**最大
  期限**;因期限對 (year,quarter) 嚴格單調遞增,`maxBy(deadline)` 即「決策日已公告的最新
  一季/月」,PIT 正確。回溯窗(季 2 年、月 1 年)足夠;過早日期 `require` 擲例,合理。
- 判定:**OK**(唯一例外見 §2 金融業 Q2)。

---

## 6. OK — RebalanceCalendar 產生再平衡日(`RebalanceCalendar.scala:29-40`)

- 語意:每月取「日 ≥ minDay(預設 15)」中最早的交易日 = 每月 15 日(含)後第一個交易日;
  `EXTRACT(DAY FROM date) >= minDay` + `GROUP BY date_trunc('month')` + `MIN(date)` 正確。
- 以 0050 交易日為 TWSE 行事曆代理:0050 自 2003-06-30 起每個交易日成交,對 2018+ 回測窗
  無缺口,合理。
- **T+1 生效不在本檔**:此檔只產生「決策日」清單;「新選股 T+1 生效、以當日收盤成交」的
  位移是 Backtester 的職責(另一稽核單位)。消費端 `AlphaStackStrategy` 另用自己的月初
  (day 1)行事曆,未用本檔;本檔實際被 ValueRevert/DividendYield/MagicFormulaPiot/
  MultiFactor/ValueMomentum 等使用。PIT 鏈自洽:`Universe.eligible(asOf)`、
  `QualityFilter.eligible(asOf)`、`Signals.*(asOf)` 全部同一 asOf,無日期錯位。
- 判定:**OK**。

---

## 附:非本單位但相鄰之已知問題(僅引用,不重複計入)
- `growth_analysis_ttm.f_score` 非正統 Piotroski(季比季錯配)、`drop_score` 近常數 +1
  膨脹 —— view 層,屬 B 單位(`B-fscore-academic.md`),已記錄。
- `QualityFilter.scala:20` 註解寫「8 binary factors」,view 實際加總 9 項 —— 過時註解,
  同見 B 單位。
