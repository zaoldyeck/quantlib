# B-slick-schema — 財務定義與算式審查(Slick schema:型別 / 單位 / 唯一索引)

- **對象**:`src/main/scala/db/table/*.scala`(25 個檔、1,258 行、33 個類別宣告 = 32 個 `Table` + 1 個空殼)
- **維度**:B(財務定義與算式)
- **結論**:🔴 **BUG**
- **稽核日**:2026-07-22
- **可重跑證據**:`docs/data_audit/scripts/B-slick-schema/checks.sql`
  (`psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/B-slick-schema/checks.sql`,約 40 秒)

---

## 一句話結論

**Slick 的表定義搬到 PostgreSQL 是逐欄精準的,錯的是「欄位名稱在騙人」——最嚴重的一個叫
`cash_dividend`,但它裝的不是現金股利。**

除權除息表裡 2,245 筆「純除權」事件(公司只配股票、一毛現金都沒發)的 `cash_dividend`
平均掛著 NT$2.722;另外 5,119 筆「除權息」的數字被股票股利部分灌大(平均 6.830,對照
純除息只有 2.043)。它真正的意思是「除權息當天的參考價要往下調多少」——也就是
`除權息前收盤 − 除權息參考價`,legacy 段 27,637 筆有 99.97% 逐位符合這條公式。

**還原股價那條路剛好沒被害到**(數學上「調整總額 ÷ 前收盤」正是正確的總報酬還原因子,
`research/prices.py` 誤打誤撞用對了),但**任何拿這欄算「配息收入」「殖利率」「現金股利」的人
會拿到假數字**;而且 2024-07 換資料源之後這一欄的意思**翻轉**成真現金股利、除權列填 0,
於是換源後的股票股利事件完全不還原(C-ex_right_dividend 獨立量到 219 筆)。

其餘六件事:三張明細財報表的 `market` 只有 `'tw'`(其他表是 `'twse'/'tpex'`),跨表 join
會靜靜回 0 列;長式 `value` 欄同時裝新台幣千元與元/股卻沒有單位欄;合併報表與個體報表
擠在同一張表、靠 `type` 區分,造成 53 萬個重複鍵,而四個下游各用四種不同方式去重(其中
一個乾脆不去重);法人買賣超 26 個欄位全宣告 `Int`,已經真的溢位過;所有基本面表都沒有
公告日欄位,財報重編無處可放;還有七個 Slick 類是完全沒人用的死碼,其中一個(`NetChangeOfPrice`)
的欄位投影本身就寫錯了。

---

## 白話版:哪些數字能信、哪些不能

| 資料 | 能不能信 | 為什麼 |
|---|---|---|
| 融資 / 融券餘額 | ✅ 能 | 「前日 + 買進 − 賣出 − 現償 = 今日」在 834 萬列上零違反 |
| 外資持股比率 | ✅ 能 | 「持股 ÷ 發行股數 × 100」在 822 萬列上零違反 |
| 借券餘額 | ✅ 能 | 「前日 + 賣出 − 還券 + 調整」469 萬列只有 38 筆例外 |
| 財報金額(千元) | ✅ 能 | 最大值 1.106e11,離 double 的精確上限 9.007e15 還很遠 |
| 還原股價(2024-06 以前) | ✅ 能 | 因子剛好等於正確值(見 BUG-1 的推導) |
| **現金股利 / 殖利率 / 配息收入** | ❌ **不能** | `cash_dividend` 裝的是「除權息調整總額」,純除權事件平均掛 2.72 元假現金 |
| **還原股價(2024-07 以後的除權事件)** | ❌ **不能** | 換源後除權列 `cash_dividend = 0`,價格跳空被當成真虧損 |
| 法人買賣超(2015 年以前) | ❌ 不能 | reader 欄位錯位,A-daily_trading_details 已定案 |
| 法人買賣超(2015 年以後) | ⚠️ 兩列爛掉 | 00403A 兩天的數字超過 `Int` 上限,靜靜變成 0 |
| 2006 年以前的財報 | ⚠️ 幾乎是空的 | cache 只收合併報表,但那年代只有個體報表 → 2004 年 1,189 家只剩 4 家 |

---

## BUG-1 `ex_right_dividend.cash_dividend` 不是現金股利

### 是什麼

Slick 定義(`src/main/scala/db/table/ExRightDividend.scala:31`):

```scala
def cashDividend = column[Double]("cash_dividend")
```

實際裝的值是 `closing_price_before_ex_right_ex_dividend − ex_right_ex_dividend_reference_price`,
也就是**除權息參考價相對前收盤要往下調的總金額**(含股票股利部分)。

### 證據(不需要外部資料,用交易所自己的事件分類就能證明)

`right_or_dividend` 欄位把事件分成三類:`息`(只配現金)、`權`(只配股票)、`權息`(兩者都有)。
**「權」按定義現金股利就是 0**,但:

```
right_or_dividend | legacy 列數(pre>0) | 等於 pre−ref 的列數 | 該欄平均值
權息              |            5,119    |             5,118   |     6.830
權                |            2,245    |             2,237   |     2.722   ← 應該是 0
息                |           20,273    |            20,273   |     2.043
```

99.97% 的列逐位等於 `pre − ref`。純除權事件平均掛著 NT$2.722 的假現金股利。

個案(算術上可自證):山富 2743 於 2025-09-18「權息」,`pre = 114`、`ref = 86.15`、
`cash_dividend = 27.846154`。台股參考價公式是 `ref = (pre − 現金股利) ÷ (1 + 配股率)`;
把「配息 2.0 元、配股 30%」代入得 `(114 − 2) ÷ 1.3 = 86.153846`,而
`114 − 86.153846 = 27.846154` **逐位吻合**。真實現金股利 2 元,DB 寫 27.85,誇大 13.9 倍。

### 2024-07 語意翻轉

TWSE 的 TWT49U 端點在 2024-06 靜默停止供料,改抓 MOPS(`TradingReader.scala:311-313` 註解),
新解析器不填 `pre`/`ref`,`cash_dividend` 改成真現金股利:

```
月份     總列數  有 pre 的列數
2024-06     234        234
2024-07     454        279   ← 語意在此翻轉
2024-08     338        171
```

台積電 2330 近期各列 `pre = ref = 0`、`cash_dividend = 6.00003573 / 5.00001118 / 4.50002042`
——那是真實的季配息 6.0 / 5.0 / 4.5(帶浮點誤差)。

雲端 Python 爬蟲更是明文分岔(`research/crawl/sources/ex_right_dividend.py:8-9`):

```
每公司每期最多兩列:除息日一列(cash_dividend=現金股利合計)、除權日一列(cash_dividend=0)
```

也就是**同一個欄位有兩個生產者、兩種定義**,而且新的那個把除權寫成 0。

### 影響(分兩條路)

**還原股價 —— legacy 段誤打誤撞是對的。** `research/prices.py:290-293`:

```python
((pl.col("pre_close") - pl.col("cash_dividend")) / pl.col("pre_close")).alias("factor")
```

代入 `cash_dividend = pre − ref` 得 `factor = ref / pre`。而配現金 C、配股 s 的合併事件,
正確的總報酬還原因子是 `(pre − C)/pre × 1/(1+s) = ref/pre`——**兩者相同**。
另實測 2015 年後 17,613 筆 legacy 事件裡 17,567 筆(99.7%)的 `daily_quote` 前一日收盤
與交易所記的 `pre` 完全相同,所以這個等式在實務上成立。

**2024-07 以後就不成立了。** 換源後除權事件 `cash_dividend = 0` → `prices.py` 產不出因子 →
除權當天的價格跳空被當成真虧損。C-ex_right_dividend 獨立量到 **219 筆股票股利事件完全沒還原**。

**配息收入 / 殖利率 —— 直接錯。** `research/experiments/chase_trailing_stop.py:160` 用
`(close + cash_dividend) / prev_close − 1` 當 DRIP 報酬,legacy 的 2,245 筆純除權列會把
股票股利價值當成現金加進報酬。

### 根因(schema 層)

`ExRightDividend` 這張表**沒有配股率 / 股票股利欄位**。交易所來源有「每仟股無償配股」欄,
schema 沒接。於是「除權」只能靠濫用 `cash_dividend` 來表達,而兩個生產者濫用的方式不同。

### 修法

1. 加 `stock_dividend_ratio`(配股率,無量綱)與 `announce_note` 欄;`cash_dividend` 恢復成
   真正的現金股利(元/股)。
2. `research/prices.py` 的因子改成 `(pre − cash_dividend) / pre / (1 + stock_dividend_ratio)`;
   對 legacy 段先用 `ref / pre` 反解 `stock_dividend_ratio` 回填(`s = (pre − cash)/ref − 1`)。
3. 加匯入期不變式:`right_or_dividend = '權'` ⟹ `cash_dividend = 0`;
   `right_or_dividend = '息'` ⟹ `stock_dividend_ratio = 0`。這條 legacy 段會全紅,正是我們要的。
4. 兩個生產者(Scala reader、`research/crawl/sources/ex_right_dividend.py`)寫同一欄卻定義不同
   ——這是與 C-operating_revenue「雙寫入者互相覆蓋」同一類的缺陷,要一起收斂。

---

## BUG-2 `NetChangeOfPrice` 的欄位投影重複一欄、漏一欄(目前是死碼)

`src/main/scala/db/table/NetChangeOfPrice.scala:51`:

```scala
def * = (id, date, upOverallMarket, upStocks, limitUpOverallMarket, limitUpOverallMarket, ...)
                                              ^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^ 重複
```

第 27 行定義的 `limitUpStocks`(`limit_up_stocks`,漲停股票數)**從未出現在投影裡**,
而 `limitUpOverallMarket` 出現兩次。型別剛好都是 `Int`,所以編譯得過。

現況是死碼:`NetChangeOfPrice` 沒有任何 `TableQuery`、不在 `Task.scala:55-77` 的建表清單、
PostgreSQL 也沒有 `net_change_of_price` 表。但只要有人啟用它,Slick 產生的 INSERT 會同時指定
`limit_up_overall_market` 兩次,PostgreSQL 直接回 `column specified more than once`,
而「漲停股票數」永遠是空的。

**修法**:第 6 個位置改成 `limitUpStocks`;啟用前先寫一條「投影欄位集合 == 宣告欄位集合」的
編譯期或測試期守護(這一類錯誤 Slick 不會幫你抓)。

---

## BUG-3 26 個股數欄宣告成 `Int`,已經真的溢位過(schema 根因)

`src/main/scala/db/table/DailyTradingDetails.scala:28-70` 把 T86 的 26 個數量欄全宣告成
`column[Int]` / `column[Option[Int]]`,但來源單位是**股數**。實測:

```
00403A 主動統一升級 50,2026-05-13 原始檔三大法人買賣超 = -2,725,272,226
                                    DB total_difference = 0
```

`-2,725,272,226` 超出 int32 下限 `-2,147,483,648`,`TradingReader.scala:186/214` 的
`Try(value.toInt).getOrElse(0)` 把它靜靜變成 0。這是全庫 2015 年後**唯二**違反
「三大法人合計 = 外資 + 投信 + 自營」的兩列。

同一事件 A-daily_trading_details 已回報;此處記的是 **schema 層的根因與同類掃描**:

| 欄位 | 型別 | 實測極值 | int32 餘裕 |
|---|---|---|---|
| `daily_trading_details` 各量 | `Int` | 1,199,902,009 | **1.8×,已破過** |
| `margin_transactions` 各量(單位:張) | `Int` | 13,872,687 | 155× ✅ |
| `daily_quote.transaction` | `Int` | 1,150,086 | 1,867× ✅ |
| `daily_quote.trade_volume` | `Long` | 4,204,257,454 | 若當初宣告 `Int` 早就爆 |
| `tdcc_shareholding.num_holders` | `Int` | — | ✅ |

**修法**:`DailyTradingDetails` 全數量欄改 `Long` / `bigint`;解析失敗改成拋例外中止該檔匯入
(`getOrElse(0)` 是最壞的失敗模式);加匯入期不變式
`total_difference = foreign + trust + dealers`。

---

## SUSPECT-1 `market` 詞彙分裂:三張表只有 `'tw'`

```
balance_sheet                        | tw    | 7,205,860
income_statement_progressive         | tw    | 3,989,598
cash_flows_progressive               | tw    | 6,359,600
concise_balance_sheet                | twse  | 2,126,383 / tpex 1,355,327
concise_income_statement_progressive | twse  | 1,918,606 / tpex 1,262,648
operating_revenue                    | twse  |   270,875 / tpex   210,654
daily_quote                          | twse  | 5,257,704 / tpex 3,866,376
```

同名、同型別(`column[String]("market")`)、沒有 `CHECK` 也沒有 enum,但詞彙表不同。
後果是**任何跨這兩群表的 `JOIN ... ON market` 靜靜回 0 列**。目前每個消費者都自己寫特例:

- `research/strat_lab/raw_quarterly.py:88-91`:「cf has no market split — join by company_code only」
- `research/cache_tables.py:96`:`FROM pg.public.cash_flows_progressive WHERE market='tw'`
- `4_financial_index_ttm.sql` 的 `market = 'tw'`(B-view-4 已確認「不是筆誤」)

也就是說這條地雷目前每一處都被人工繞開了,但它是靠每個作者記得,不是靠 schema 擋。

**修法**:三張明細表補上真正的 `twse`/`tpex`(來源是逐公司查詢,市場別可從 `company_code`
對照補),或把該欄改名為 `source`;無論哪種,加 PostgreSQL `CHECK (market IN (...))` 讓
詞彙表變成 schema 的一部分。

---

## SUSPECT-2 長式 `value` 欄沒有單位欄,新台幣千元與元/股混裝

`BalanceSheet` / `IncomeStatement` / `CashFlows` / `Concise*` 五組表都是
`(title, value)` 長式,`value` 一律 `column[Double]("value")`,**沒有單位欄**。實測台積電
2024Q4 合併簡明損益表:

```
營業收入            2,894,307,699   ← 新台幣千元
營業毛利（毛損）    1,624,353,564   ← 新台幣千元
本期淨利（淨損）    1,172,431,759   ← 新台幣千元
基本每股盈餘（元）          45.25   ← 元/股,同一欄
```

單位只存在於 `title` 的中文字裡。任何跨 title 的 `SUM` 或比率都靠寫的人自己記單位——
已知受害者:`4_financial_index_ttm.sql` 的 `fcf_per_share` 分母用了「股本金額(千元)」而不是
股數,整整差 10 倍(B-view-4 / B-view-5 已記),`2_balance_sheet_with_titles.sql` 的
`receivable` CTE 一口氣 `sum` 了 28 個 title(那個 CTE 還同時加了應收帳款的毛額與淨額,
B-view-4 已記)。

**修法**:加 `unit` 欄(`TWD_THOUSAND` / `TWD_PER_SHARE` / `RATIO`),由 reader 依 title
對照表填入;所有 `SUM(value)` 的地方加 `GROUP BY unit` 或 `WHERE unit = ...`。

---

## SUSPECT-3 合併報表與個體報表擠在同一張表,四個下游四種去重法

`ConciseBalanceSheet` / `ConciseIncomeStatement` / `OperatingRevenue` 都有 `type` 欄
(值只有 `consolidated` / `individual`),而且**唯一索引把 `type` 包進去**
(`ConciseBalanceSheet.scala:60`、`IncomeStatement.scala:68`、`OperatingRevenue.scala:48`),
所以同一個 `(market, year, quarter, company_code, title)` 可以合法地有兩列:

```
concise_balance_sheet                同鍵跨 type   532,288 個鍵
concise_income_statement_progressive 同鍵跨 type   355,162 個鍵
operating_revenue                    同鍵跨 type    27,365 個鍵
```

全部落在 **2005-2012**(2013 IFRS 之後只剩合併),所以現役策略窗(2018+)目前沒被咬到。
但四個下游的處理方式四種:

| 消費者 | 做法 | 評價 |
|---|---|---|
| `research/cache_tables.py:86-91` | `WHERE type='consolidated'` | 正確但**截斷歷史**(見下) |
| `research/apex/data.py:221` | `ORDER BY CASE type WHEN 'consolidated' THEN 0 ELSE 1 END` | ✅ 正確 |
| `research/evergreen/make_tables.py:51-53` | `ORDER BY monthly_revenue DESC` | ⚠️ 挑「數字大的」而不是挑合併 |
| `research/serenity/daily.py:134-137` | 沒有任何去重 | ⚠️ 同月兩列都會進 `usable[-3:]`,3 個月均值變 1.5 個月 |

**歷史截斷的代價**(`type='consolidated'` 硬過濾):

```
year | PG 全部公司數 | 只算 consolidated
2004 |        1,189 |          4
2005 |        1,212 |          4
2006 |        1,247 |      1,106
2013 |        1,578 |      1,578
```

也就是 cache 的 `bs_concise_raw` / `is_progressive_raw` 在 2006 年以前實質是空的
(2004/2005 各只有 4 家),而價格面板從 2004(上市)/2007(上櫃)就有。
任何宣稱「1997-2026 全史回測」的基本面策略,前段其實沒有基本面。

**修法**:(a) `serenity/daily.py` 補上與 `apex/data.py` 一致的 `type` 優先序去重
(現在沒咬到是因為窗期,不是因為對);(b) `evergreen/make_tables.py` 改成同一套規則;
(c) cache 建表改成「有合併取合併、只有個體才取個體」並保留 `type` 欄讓下游知道拿到哪一種,
不要直接丟掉整段歷史。

---

## SUSPECT-4 所有基本面表都沒有公告日欄位,財報重編無處可放

`BalanceSheet` / `IncomeStatement` / `CashFlows` / `Concise*` 的鍵是
`(market, year, quarter, company_code, title)`;`OperatingRevenue` 是
`(market, type, year, month, company_code)`;`FinancialAnalysis` 是 `(market, year, company_code)`。
**沒有一張有 `announce_date` / `filing_date`。**

兩個後果:

1. **PIT 只能推估**。`src/main/scala/strategy/PublicationLag.scala:33-39` 用法定期限
   (5/15、8/14、11/14、次年 3/31)+ 7 天緩衝當代理;Python 側 `v4.py` 同款、
   `apex/assemble.py` 用期限本身(B-fscore 已記兩套不一致)。這是保守做法(不會前視),
   但也代表「這家公司其實 4/20 就公告了」的 25 天資訊優勢永遠拿不到。
2. **財報重編表達不了**。一個 `(year, quarter, code, title)` 只放得下一個版本;
   MOPS 重編後的數字若被匯入,原始公告值就消失,回測會看到「當時不知道的正確答案」。
   目前靠 reader 的「已在庫就跳過」意外擋住了(等於保留第一版 ≈ PIT 正確),
   但那是副作用不是設計。

**修法**:加 `announce_date`(來源:MOPS 各報表都有申報日)並納入唯一索引作為版本維度;
消費端一律 `WHERE announce_date <= 決策日` 取最新一版。這比現在的「法定期限 + 緩衝」精準,
也讓重編有地方放。

---

## SUSPECT-5 非 `Option` 數值欄把「沒有」寫成 0

`DailyQuote.scala:42` 的 `change` 宣告成 `column[Double]`(非 `Option`),而 OHLC 四欄
都是 `Option[Double]`。實測:

```
closing_price IS NULL(當天沒成交)的列       134,222
其中 change = 0 的列                          134,222   ← 100%
```

「今天沒有交易」在庫裡長得跟「今天平盤」一模一樣。A-index 已在 `index.change` /
`index.change(%)` 上實證同一類(1,578 列把原始檔的 `--` 寫成 0)。同一類的高風險欄位還有
`ExRightDividend` 全 8 個 `Double`、`CapitalReduction` 5 個 `Double`、
`ForeignHoldingRatio` 6 欄、`SblBorrowing` 6 欄、`MarginTransactions` 13 欄。

目前 `change` 沒有任何消費者(`research/cache_tables.py` 的 `daily_quote` 不收它),
所以是未爆彈而非已爆。

**修法**:凡是來源可能給空白 / `--` 的數值欄一律 `Option[T]`(PostgreSQL nullable);
reader 的 `Try(...).getOrElse(0)` 改成 `.toOption`。

---

## SUSPECT-6 `tdcc_shareholding` 把「差異數」與「合計」當普通級距存

`TdccShareholding.scala:11-30` 的註解說明 tier 16 = 差異數、tier 17 = 合計,但
schema 沒有任何旗標欄,17 個 tier 全部平等地存在同一張表、同一個唯一索引下。實測:

```
把 17 個 tier 的 pct_of_outstanding 相加,47,830 個 (日期, 代號) 鍵的平均 = 199.95%
```

任何人寫 `SUM(num_shares)` 或 `SUM(pct_of_outstanding)` 會直接雙倍計算。目前唯一的
消費者 `research/db.py:142` 只是註冊 view、沒有聚合,所以還沒爆。

**修法**:加 `is_summary boolean`(tier 16/17 為 true),或把合計 / 差異數搬到另一張表。

---

## SUSPECT-7 七個 Slick 類是死碼,其中三個的表名被 matview 佔用

沒有任何 `TableQuery`、不在 `Task.scala:55-77` 建表清單、PostgreSQL 也沒有對應表:

| Slick 類 | tableName | 狀態 |
|---|---|---|
| `NetChangeOfPrice` | `net_change_of_price` | 死碼,且投影寫錯(BUG-2) |
| `MarketSummary` | `market_summary` | 死碼 |
| `QuarterlyReport` | `quarterly_report` | 死碼 |
| `CashFlowsIndividual` | `cash_flows_individual` | **名稱被 matview 佔用** |
| `IncomeStatementIndividual` | `income_statement_individual` | **名稱被 matview 佔用** |
| `ConciseIncomeStatementIndividual` | `concise_income_statement_individual` | **名稱被 matview 佔用,且欄位不合** |
| `CompanyInformation` | (空 class,連 `Table` 都不是) | 死碼 |

後三個踩到 CLAUDE.md 的「所有 PG 表必須由 Slick 建立」鐵律的反面:PostgreSQL 那三個名字
已經是 `src/main/resources/sql/materialized_view/` 建的物化視圖,而且**語意完全不同**——
matview 的 `individual` 指「單季差分」(累計數相減),Slick 類的 `Individual` 指「個體報表」
(對照 `Progressive` = 合併累計)。`ConciseIncomeStatementIndividual` 比 matview 多一個
`type` 欄,一旦有人用 Slick 查它會直接在 runtime 炸。

而且 `createIfNotExists` 對這三個名字會**靜默 no-op**(PostgreSQL 的
`CREATE TABLE IF NOT EXISTS` 會把 matview 算成已存在的關聯),所以連 `Main init` 都不會報錯。

**修法**:七個類全刪(死碼是時間債,不是資產);若「個體報表」真的要做,重新命名成
`*_standalone` 以免與 matview 的「單季」語意相撞。

---

## SUSPECT-8 兩個本益比欄位,一個 TPEx 全空、一個沒人用

```
daily_quote.price_earning_ratio             twse 5,257,704 有值 / tpex 3,866,376 全 NULL
stock_per_pbr_dividend_yield.price_to_earning_ratio   兩市場都有
```

`DailyQuote.scala:52` 的 `priceEarningRatio` 只有 TWSE 有值,**不進 cache、零消費者**。
2024 起兩者每年有 1 / 709 / 709 列不一致,抽樣看全是 A-stock_per_pbr 已定案的
「整天存到別天資料」——例如 2026-02-25 台積電 `daily_quote` 32.93、`stock_per_pbr` 17.11
(後者是 2017-12-18 的舊值)。

也就是說:**手上有一個現成的交叉守門員,可以自動抓出 stock_per_pbr 的整天汙染,但沒接。**

**修法**:把 `daily_quote.price_earning_ratio` 接進匯入期驗證——兩來源在 TWSE 上偏離
> 2% 的日子直接紅燈。不需要新資料,只要把已經在庫的欄位用起來。

---

## 查過沒問題的(別再查一次)

- **OK-1 Slick ↔ PostgreSQL 逐欄 parity 完全一致**。23 張活表的欄名、型別、nullability、
  唯一索引全部與 `db/table/*.scala` 相符,包括
  `"liabilities/assets_ratio(%)"`、`"change(%)"`、`"trade_value(NT$)"` 這種帶斜線 / 括號 /
  貨幣符號的怪欄名,以及被 PostgreSQL 截斷到 63 字元的兩個索引名
  (`idx_ConciseBalanceSheet_..._ti`、`idx_ConciseIncomeStatement_...`)。CLAUDE.md 擔心的
  「raw DDL 與 Slick 漂移」目前沒有發生。
- **OK-2 恆等式全過**:融資餘額與融券餘額兩條式子在 8,341,265 列上零違反;
  外資持股比率 `持股/發行×100` 在 8,225,920 列上零違反(可投資比率 8 列例外)。
  這同時證明 `margin_transactions` 的單位(張)與欄位對應是對的。
- **OK-3 借券的符號約定實測為 `+ adjustment`**:`daily_balance = prev + sold − returned + adjustment`
  在 4,691,508 列上只有 38 筆例外,反向符號則有 6,730 筆例外。
  (schema 註解沒寫這條,建議補進 `SblBorrowing.scala` 的 doc comment。)
- **OK-4 `ex_right_dividend` 的唯一索引粒度正確**。擔心的「同一天同一檔既除權又除息會被
  唯一索引砍掉一筆」不成立——交易所本來就把同日兩事件併成一列標記為「權息」
  (息 22,729 / 權息 5,120 / 權 2,304,共 30,153 列,零同日雙事件)。
- **OK-5 `Double` 對財報金額無精度損失**。三張長式表的最大 |value| 是 1.106e11(千元),
  離 IEEE754 double 能精確表示整數的上限 9.007e15 差 5 個數量級,不會有捨入誤差。
- **OK-6 `company_code` 正規化乾淨**:五張主表都是 0 列長度 < 4、0 列含前後空白
  (A-margin 記載的「上櫃代號右補空白」問題沒有汙染到現存資料)。
- **OK-7 NOT NULL 日期欄無哨兵值**:`treasury_stock_buyback` 的
  `period_start`/`period_end` 0/2,933 異常,`insider_holding.declare_date` 0/771 異常
  (沒有 `1970-01-01` 這種「解析失敗預設值」)。
- **REAL-1 三大法人恆等式在 2015 年以前大量不成立,但那不是 schema 的錯**。
  自營商 506,397 列、合計 568,922 列違反,全部落在 2015 年以前;
  根因是 reader 對舊版 CSV 欄序的錯位(A-daily_trading_details 已定案並給了修法)。
  2015 年後兩市場皆 0 違反,唯二例外是 BUG-3 的 int32 溢位兩列。

## 順帶記兩件事

- **欄名 typo(非錯誤,但每個消費者都得照抄)**:`OperatingRevenue.scala:38/40/46` 的三個
  欄名多一個右括號,PostgreSQL 也照單全收:
  `monthly_revenue_compared_last_month(%))`、`monthly_revenue_compared_last_year(%))`、
  `cumulative_revenue_compared_last_year(%))`。
- **文件與現實脫節**:CLAUDE.md 的「Data Judgement Rules」寫
  「`concise_*` tables have no `market` column — filter via `company_code` prefix」,
  **與事實不符**——PostgreSQL 的 `concise_balance_sheet` 與
  `concise_income_statement_progressive` 都有 `market`(`twse`/`tpex`),
  DuckDB cache 的 `bs_concise_raw` / `is_progressive_raw` 也有。
  這條和「TPEx cache 覆蓋」那條一樣是過時記載,建議一併更正。
- **命名不一致(非錯誤)**:`DailyTradingDetails.scala:74` 把 `totalDifference` 欄位映到
  case class 的 `totalDifferenceInt` 欄位名。Slick 的 `mapTo` 是按位置對映所以不會錯,
  但讀碼的人會以為是兩個不同的東西。
