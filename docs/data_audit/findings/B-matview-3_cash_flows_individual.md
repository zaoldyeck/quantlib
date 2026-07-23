# B-matview-3_cash_flows_individual:現金流「累計轉單季」差分公式審查

**審查對象**:`src/main/resources/sql/materialized_view/3_cash_flows_individual.sql`
**結論(白話)**:**這份現金流單季資料在一部分公司上是假的,不能直接拿來選股。**
問題出在把「累計數」換算成「單季數」的算式,對「當年度沒有 Q1 財報」的公司
(主要是興櫃這種只報半年報的公司)會算出跨年度的垃圾數字——拿今年上半年的
累計去減掉去年一整年的累計。全表約 **34.3 萬筆(占 5.4%)** 現金流單季值被這樣汙染。

好消息:**現役的 Python 選股引擎沒有用這張表**,它自己重算而且算對了(見下)。
壞消息:這張表餵給已凍結的 Scala 財務指標鏈(`financial_index_ttm`、
`growth_analysis_ttm` 的 F-score 現金流項),誰去查那些視圖或跑 Scala 策略就會中招。

判定:**BUG**(可重現、算術逐筆吻合、根因明確、正解已知)。

---

## 這個算式在做什麼

台股財報是**累計制**:Q2 財報寫的是「1~6 月合計」、Q3 是「1~9 月合計」、
Q4 是「全年」。要得到「單季」就得做差分:Q2單季 = Q2累計 − Q1累計。

檔案全文(12 行)只做這件事:

```sql
case
    when quarter = 1 then value                       -- Q1:累計就是單季,原樣保留
    else value - lag(value)                            -- Q2/3/4:減掉上一列
         over (partition by market, company_code, title order by year, quarter)
end
```

## 錯在哪(根因)

護欄只認 `quarter = 1`。它**假設每家公司每個會計年度都以 Q1 那筆開始**。
但興櫃公司依規定只報**半年報 + 年報**(只有 Q2、Q4,沒有 Q1、Q3)。這種公司
一個會計年度的第一筆是 **Q2**,`quarter = 1` 護欄對它不起作用,於是它的 Q2 累計
被拿去減「上一列」——排序上一列是**去年的 Q4(去年整年累計)**。跨會計年度相減 = 垃圾。

窗函式跨了所有年份(`order by year, quarter`),年度邊界完全靠 `quarter = 1` 這一道
護欄擋;護欄一旦失效,汙染就發生。

## 實證(company_code = 000156,營業活動淨現金流)

| 年季 | 原始累計(raw) | 意義 | 差分視圖輸出 | 該有的值 | 判定 |
|---|---|---|---|---|---|
| 2019Q4 | 1285 | 2019 全年 | (NULL) | — | 首列,安全 |
| 2020Q2 | 29041 | 2020 上半年累計 | **27756** = 29041−1285 | 29041(H1) | 減到去年整年,錯 |
| 2020Q4 | 90536 | 2020 全年 | 61495 = 90536−29041 | =2020 H2 | H2 六個月被標成 Q4 |
| 2021Q2 | 95502 | 2021 上半年累計 | **4966** = 95502−90536 | 95502(H1) | 減到去年整年,低估 95% |
| 2021Q4 | 325233 | 2021 全年 | 229731 = 325233−95502 | =2021 H2 | H2 六個月被標成 Q4 |

`value` 欄在來源表是 `NOT NULL`,所以這些汙染值是**非空、看起來合理的數字**,
會通過下游所有 `value is not null` 濾網——靜默汙染,不會報錯。已驗證 `4966` / `27756`
確實流進了下游 `cash_flows_with_titles.ocf`。

## 影響範圍

- **343,444 筆** title-列是「年度起始列拿去減前一年累計」的汙染值(全表 636 萬列的 5.4%)。
- 對應 **6,906 個 company-year**(2019 年起佔 4,728 個);半年報公司每年約 **500~660 家**
  (2019 年起大量進入此資料集)。
- **1,022** 家半年報公司中 **334** 家的代碼出現在可交易的 `daily_quote`(多為它們日後上市的
  期間;半年報那幾年多半仍在興櫃、不在選股池)。

## 為什麼現役選股大致沒被毒到

現役 canonical 引擎是 Python,讀的是**原始累計表** `cash_flows_progressive`(不是這張
matview),自己重算單季。它的差分**依年度切窗**——這就是正解:

```python
# research/strat_lab/raw_quarterly.py:176-180
pl.when(pl.col("quarter") == 1).then(pl.col("cfo"))
  .otherwise(pl.col("cfo") - pl.col("cfo").shift(1)
             .over(["company_code", "year"], order_by="quarter"))   # ← 依 (公司, 年度) 切窗
```

因為視窗鎖在同一年度內,半年報公司的 Q2(該年度第一筆)`shift(1)` 取到 NULL → 該列變 NULL
(安全),**不可能跨年度相減**。SQL 版少了 partition 裡的 `year`,才會出事。

## 建議修法

把年度併入視窗分割鍵即可根治跨年度汙染:

```sql
over (partition by market, company_code, title, year order by quarter)
```

加了 `year` 之後,跨年 lag 消失,半年報公司的年度起始列 lag=NULL → 變 NULL(安全),
語意與 Python 版一致。`case when quarter = 1` 可留可換成更穩健的
`when lag(...) over(..., year ...) is null then value`。

**同一缺陷類(必須同批修)**:`7_income_statement_individual.sql` 與
`5_concise_income_statement_individual.sql` 用完全相同的「跨年度視窗 + 只認 quarter=1」
寫法,損益表也是累計制、半年報公司同樣受害。只修現金流這一支 = 留兩個同類地雷。

**殘留(設計取捨,非此次 BUG)**:半年報公司的 Q4 單季實為 H2(6 個月)被標成 Q4,
Python 版也一樣。這是「半年報無法還原成季」的本質限制;正途是把這類 company-year
(該年度最早一季 > Q1)標記報導區間或排除,而不是硬差分。建議由主流程裁決。

---

## 查了什麼(供覆核涵蓋度)

1. 精讀 `3_cash_flows_individual.sql` 全文(12 行,單一計算欄位)。
2. 確認來源 `cash_flows_progressive` 為 table、累計制、market 全為 'tw'、value NOT NULL、
   年季覆蓋(2009Q4 起,2019 起半年報公司暴增)。
3. 追消費鏈:matview 3 → `cash_flows_with_titles`(matview 4)→ `financial_index_quarterly`
   /`financial_index_ttm`(view 3/4)→ `growth_analysis_ttm`(view 5,F-score 用 ocf)。
4. 逐筆算術驗證 000156 的 raw vs matview vs 下游 `cash_flows_with_titles`,四筆全吻合。
5. 量化 blast radius(343,444 汙染 title-列 / 6,906 company-year)。
6. 比對 Python 平行實作 `research/strat_lab/raw_quarterly.py` 與 `research/db.py`——確認
   現役路徑依年度切窗、未用此表、無此 bug。
7. 掃同類:matview 5、7 為同款差分寫法(同缺陷類)。
