# C-cf_progressive_raw — 現金流量表(YTD 累計)cache 一致性與缺漏

**判定:🔴 BUG**(搬運零瑕疵,來源缺一大塊)
**稽核日:2026-07-22**
**重現腳本:`docs/data_audit/scripts/C-cf_progressive_raw/`**(parity / coverage / anomaly / gaps / downstream 五支,全部唯讀)

---

## 一句話結論

**搬運沒問題,最新一季幾乎是空的。** DuckDB cache 與 PostgreSQL 逐季逐列一模一樣
(66 季、635.96 萬列、含整列內容雜湊全對),抽樣逐欄也一字不差——cache 這一段可以
完全信任。**不能信的是「最近一季到底有沒有資料」**:2026 年第 1 季只有 544 家公司,
少了 1,719 家(其中 1,410 家在 2026 上半年天天在交易、成交值合計 NT$75 兆),
**連台積電、聯發科、鴻海都不在裡面**。原因是原始檔在 2026-05-10 被抓了一次
(申報期限 5/15 之前),而爬蟲認定「資料夾存在 = 這一季抓完了」,從此永遠不再重抓。
同一個病灶在 2025Q2、2023Q2、2024Q1 都發作過,受害的是金控、銀行、保險、KY 股
這些申報期限比較晚的公司。

**更麻煩的是缺一季不會留空,而是把下一季灌成兩季的合計。** 現金流量表是年度累計數,
下游用「本季累計 − 上季累計」還原單季;少了 Q2,Q3 就直接變成 Q2+Q3。
2025Q3 有 127 家在交易的公司(NT$6.57 兆成交值)的單季營業現金流是這樣被灌出來的,
而且數字看起來完全合理——比留空危險得多。這個值再往下走進 Piotroski F-Score 的
第 2 項(營業現金流為正)與第 4 項(營業現金流大於淨利)。

---

## 1. cache vs PostgreSQL:完全一致(🟢 OK)

| 檢查 | 結果 |
|---|---|
| 全表筆數 | cache 6,359,600 = PG 6,359,600(PG 全表也是 6,359,600,`WHERE market='tw'` 沒丟任何列) |
| 逐季 checksum(66 季) | count / distinct code / distinct title / SUM / MIN / MAX / 整列內容 `BIT_XOR(HASH(market‖code‖title‖value))` **七項全對,0 季不符** |
| 抽樣逐欄 | 亂數 seed 20260722 抽 2022Q3(2701/1905/1593/6198/8271)、2011Q4(4154/6197/2856/4120/9937)、2011Q2(3052/2313/3068/5521/8064),共 **812 列 × 6 欄 `DataFrame.equals` = True** |
| NULL / NaN / Inf | 六欄皆 0 筆 |
| 重複鍵 (year,quarter,code,title) | 0 筆 |
| schema | PG 7 欄 → cache 6 欄,只少 `id`(自增代理鍵);`character varying→VARCHAR`、`integer→INTEGER`、`double precision→DOUBLE` 逐一對應,**無型別降級** |
| `research/db.py:190` 的 pg-attach view | 與 `research/cache_tables.py:92-94` 的建表 SQL 逐字相同,parity 成立 |

`market` 欄在 PG 只有 `'tw'` 一個值(reader 硬寫,見 `FinancialReader.readFinancialStatements`),
所以 cache_tables 的 `WHERE market='tw'` 是 no-op,不是過濾造成的差異。

**跨市場代碼碰撞風險已排除**:`raw_quarterly.py:193-195` 把 cf 用 `(year,quarter,company_code)`
join(不帶 market)。實測 `is_progressive_raw` 中「同一季同一 code 出現在兩個市場」= **0 筆**
(全期間有 13 個 code 曾在兩個市場出現,但從不在同一季),所以這個 join 不會扇出。

---

## 2. 2026Q1 幾乎整季不見(🔴 BUG,最嚴重)

### 事實

```
2025Q1  1,968 家   2025Q2  2,366 家   2025Q3  2,008 家   2025Q4  2,262 家
2026Q1    544 家   ← 少 1,719 家(對照 2025Q4)
```

其中 **1,410 家在 2026-01-01~06-30 有 ≥80 個交易日**,合計成交值 **NT$74,996.5 億 ×10 = 約 75 兆**。
按成交值排序的前幾名:

| 代碼 | 2026H1 交易日 | 成交值(億) |
|---|---|---|
| 2330 台積電 | 114 | 99,879.5 |
| 2454 聯發科 | 114 | 37,191.1 |
| 2327 國巨 | 114 | 24,718.5 |
| 3481 群創 | 114 | 21,479.7 |
| 2317 鴻海 | 114 | 20,620.9 |

估計缺列數:1,410 家 × 每家每季平均 52 個科目 ≈ **7.3 萬列**。

### 根因(程式碼)

`src/main/scala/Task.scala:120-187` `pullFinancialStatements()`:

```scala
val existFiles = detail.dir.toDirectory.dirs.map { dir => ... year match {
  case y if y < 2019 => dir.files...map { ... (year, quarter, companyCode) }
  case _             => Seq((year, quarter, ""))     // ← 第 138 行
}}.reduce(_ ++ _).toSet
...
val tuples = yearToQuarterToCompany.filterNot(existFiles)  // ← 第 182 行
```

2019 年起改用**整季 bulk zip**(`application.conf:98` `bulkInstanceDocuments`,
URL 為 `.../FileDownLoad?step=9&filePath=/home/html/nas/ifrs/<YYYY>/&fileName=tifrs-<YYYY>Q<Q>.zip`)。
`existFiles` 對 2019+ 只記 `(year, quarter, "")`——**只要 `data/financial_statements/<Y>_<Q>/`
這個資料夾存在,不管裡面 1 個檔還是 2,600 個檔,都算「已抓」,永遠不再重抓**。

而 `excludeYearToQuarter`(Task.scala:150-157)的月份閘門 `case m if m < 5 => (1 to 4)`
表示 **5 月 1 日就允許抓當年 Q1**,一律早於 5/15 申報期限。

### 證據:下載日 vs 完整度(全部 66 季的檔案 mtime)

| 目錄 | 檔數 | 主要 mtime | 是否完整 |
|---|---|---|---|
| 2022_1~2022_3 | 1,828 / 2,484 / 1,837 | 2023-04-26 | ✅(遠晚於期限) |
| 2023_2 | 2,241 | **2023-08-20** | ❌ 金融業期限 8/31 前 |
| 2023_3 | 1,884 | 2024-03-12 | ✅ |
| 2024_1 | 1,886 | **2024-05-16** | ⚠️ 剛過 5/15,仍缺 13 家在交易的 |
| 2024_2 / 2024_3 | 2,580 / 1,920 | 2024-11-22 | ✅ |
| 2025_1 | 1,968 | 2025-06-15 | ✅ |
| 2025_2 | 2,376 | **2025-08-21** | ❌ 金融業期限 8/31 前 |
| 2025_3 | 2,008 | 2025-12-10 | ✅ |
| 2025_4 | 2,290 | 2026-04-01 | ✅(3/31 期限後一天) |
| **2026_1** | **544** | **2026-05-10** | ❌❌ 5/15 期限前 |

規律非常乾淨:**下載日晚於申報期限的季一律完整,落在期限前的季一律殘缺**。
今天是 2026-07-22,2026Q1 早該齊備,但因為資料夾已存在而永遠不會補。
(2026Q2 目前沒有資料是正確的——期限 8/14 還沒到。)

---

## 3. 同一病灶的歷史發作:金融業 + KY 股整批缺料(🔴 BUG)

用檔名裡的行業別模板(`tifrs-fr1-m1-<行業>-<cr|ir>-<代碼>-<YYYYQq>.html`)一眼看出來:

| 目錄 | basi 銀行 | bd 證券 | fh 金控 | ins 保險 | ci 一般 |
|---|---|---|---|---|---|
| 2019_2(正常) | 43 | 120 | 16 | 21 | 2,186 |
| 2024_2(正常) | 45 | 110 | 15 | 24 | 2,382 |
| 2025_4(正常) | 45 | 100 | 14 | 23 | 2,104 |
| **2023_2** | **4** | **21** | **0** | **2** | 2,210 |
| **2025_2** | **7** | **32** | **1** | **5** | 2,327 |
| **2026_1** | **2** | **4** | **0** | **0** | **538** |

受害家數(對照前一年同季,且該期間仍在交易):

| 季 | 缺家數 | 其中在交易 | 該期成交值 |
|---|---|---|---|
| 2023Q2 | 256 | **119** | NT$2.98 兆 |
| 2024Q1 | 24 | **13** | NT$1.70 兆 |
| 2025Q2 | 242 | **122** | NT$3.17 兆 |
| 2026Q1 | 1,719 | **1,410** | NT$75.0 兆 |

2025Q2 缺料名單(按成交值):6781 AES-KY、2887 台新金、2881 富邦金、4991 環宇-KY、
2891 中信金、2882 國泰金、4971 IET-KY、2885 元大金、4977 眾達-KY、2886 兆豐金、
6415 矽力-KY、2890 永豐金、5871 中租-KY、4763 材料-KY、1590 亞德客-KY、2892 第一金、
2883 開發金、2880 華南金、2801 彰銀、3673 TPK-KY……

**這是同一個缺陷類的第二個樣本**——`C-is_progressive_raw` 已在另一支爬蟲
(`Task.pullQuarterlyFiles`)抓到同型病灶。兩支爬蟲、兩套資料源、同一種
「檔案存在就跳過 + 閘門早於申報期限」的設計,受害季高度重疊(2023Q2 / 2025Q2 / 2026Q1)。

---

## 4. 缺一季不是留空,是把下一季灌成兩季(🔴 BUG,下游)

`research/strat_lab/raw_quarterly.py:176-182`:

```python
cf_q = cf_ytd.with_columns(
    pl.when(pl.col("quarter") == 1).then(pl.col("cfo"))
      .otherwise(pl.col("cfo") - pl.col("cfo").shift(1)
                 .over(["company_code", "year"], order_by="quarter"))
      .alias("cfo_q")
).sort(...).with_columns(
    pl.col("cfo_q").rolling_sum(window_size=4).over("company_code").alias("cfo_ttm"))
```

`shift(1)` 只在 `(company_code, year)` 內按 quarter 排序取「上一列」,**沒有檢查
quarter 是否真的差 1**。整季不見時,上一列變成再上一季。

### 逐位驗證(1590 亞德客-KY)

cache 裡的累計營業現金流(`cf_progressive_raw`):

```
2025Q1 YTD  1,313,878      2025Q3 YTD  4,977,755      2025Q4 YTD  7,666,588
2025Q2      ← 整列不存在
```

`research/raw_quarterly.parquet` 的 `cfo_q`:2025Q3 = 3,663,877 = 4,977,755 − 1,313,878
= **Q2+Q3 兩季合計,卻掛在 Q3 名下**。對照 2024 年單季 1.45M / 3.07M / 2.84M / 3.31M,
3.66M 看起來完全正常——這才是最危險的地方。

`cfo_ttm` 同步失真:2025Q4 的 cfo_ttm = 3,309,034(2024Q4)+ 1,313,878 + 3,663,877
+ 2,688,833 = **10,975,622**,實際涵蓋 2024Q4~2025Q4 **五個季度**;真正的 2025 全年
營業現金流就是 2025Q4 的 YTD = **7,666,588**,**高估 43%**。

同型受害(2025Q3,`prev_quarter_present = 1`):2881 富邦金、2882 國泰金、2891 中信金、
2887 台新金、6781 AES-KY、4991 環宇-KY、2885 元大金、2890 永豐金……

### 規模

| 年 | 被灌水的 (code,quarter) 列數 | 其中該年交易 ≥150 日 | 該年成交值 |
|---|---|---|---|
| 2023 | 664 | **120** | NT$5.65 兆 |
| 2024 | 615 | **13** | NT$2.59 兆 |
| 2025 | 339 | **127** | NT$6.57 兆 |

2019-2022 的數字(236~560 列)全部落在興櫃/未上市公司身上,交易日 ≥150 者為 0,不影響選股池。

### 影響面

`cfo_ttm` 直接進 Piotroski:`raw_quarterly.py:245` F2「CFO > 0」、`:249` F4「CFO > NI」,
以及 `:216` 的 `cfo_ni_ratio_ttm`。`research/raw_quarterly.parquet` 的消費者含
`research/serenity/engine.py`(現役策略引擎)、`research/apex/data.py`、
`research/strat_lab/v4.py` 等 20+ 支。

---

## 5. 查了沒問題的部分(🟢 OK / ⚪ REAL,負結果落盤)

### 5.1 季序列沒有洞

2009Q4 → 2026Q1 連續 66 季,**一季不缺**。起點 2009Q4 是設定值
(`FinancialStatementsSetting.scala:7,20` `firstDate = LocalDate.of(2009,4,1)`),不是漏抓。
2026Q2 未出現是正確的(期限 8/14 未到)。**本表是季頻,不涉及休市日曆 / sentinel。**

### 5.2 CFO 科目別名 100% 命中(和 is_progressive 的 op_income 不同,這裡沒有漏字)

`raw_quarterly.py:72-74` `CF_TITLES["cfo"] = ["營業活動之淨現金流入（流出）", "營業活動之淨現金流入(流出)"]`
——半形版覆蓋 2009Q4~2012Q4(18,049 筆 / 1,563 家),全形版覆蓋 2013Q1~2026Q1
(98,314 筆 / 2,859 家)。**66 季逐季公司覆蓋率全部 100.0%**,唯一例外是 2019Q2 的
`000027`(興櫃、只有 3 個科目、不在交易池)。

### 5.3 會計恆等式全過

2013 年起,以 `期初 + 本期增減 = 期末` 檢查 100,867 個公司季:**0 筆不符**;
以 `營業 + 投資 + 籌資 + 匯率影響 = 本期增減` 檢查:**0 筆不符**(每年 27~191 筆缺項
是銀行保險用不同科目名,非錯值)。

2009-2012 用 pre-IFRS 科目名檢查:恆等式 A 18,049 筆中僅 4 筆不符;恆等式 B 有
345~436 筆/年不符,**逐案查證是我的檢查式漏項,不是資料錯**——例:2492 於 2011Q4
的殘差 7,779,167 剛好等於原始 HTML 裡「合併個體變動淨影響數 −7,779,167」這一行
(`data/financial_statements/2011_4/2492.html`)。pre-IFRS 現金流量表有這類額外調節列,
四項式當然湊不齊。

### 5.4 pre-2013 科目名爆量是真的,不是解析垃圾

2010Q2~2012Q4 每季 800~1,368 個不同科目名(2013 起降為 275~311)。
逐年統計:pre-2013 共 5,509 個 (year,title) 組合,其中 **3,516 個只出現在 1 家公司**;
post-2013 為 4,150 個組合、只有 191 個 singleton。這是 IFRS 前現金流量表科目未標準化的
自然結果——核心列(期末現金、營業/投資/融資活動之淨現金流入、本期增減、期初現金、
匯率影響數)每一季都有約 1,538 家齊備。

### 5.5 期末現金為負是來源真值

全表 116,362 個公司季中只有 **8 筆**期末現金為負,涉及 3 家公司(1328、1456、5403)。
逐案查原始檔:`data/financial_statements/2013_4/1456.html` 原文即為
「期末現金及約當現金餘額 −18,343」,並在下方拆解為「資產負債表帳列之現金及約當現金
11,822」+「其他符合國際會計準則第七號現金及約當現金定義之項目 −30,165」——
**來源就是這樣寫的,reader 忠實搬運**。1328 完全沒有交易紀錄(不在任何選股池)。

### 5.6 單位沒有漂移

逐年中位數期末現金 468,831 → 920,331 仟元、中位數 |CFO| 115,711~328,868 仟元,
2013 年 IFRS 換版前後平滑,無 元/仟元 混用。極值最大 3.34e9 仟元
(2891 中信金 2011-2012 取得/處分備供出售金融資產)、2330 台積電 2025Q4 期末現金
2.768e9 仟元(NT$2.77 兆),皆為合理量級。零值佔 9.19%(現金流量表大量科目本來就是 0)。

### 5.7 半年報型態的公司不是漏抓

2019 年起 Q2/Q4 比 Q1/Q3 多約 600 家(2024Q4 2,604 vs 2024Q3 1,920)。這是 2019 年
改用 bulk zip 後把「所有公開發行公司」都收進來,其中興櫃/未上市只編半年報與年報。
逐年交叉 `daily_quote`:2010-2025 各年「只有 Q2+Q4」的公司(21~610 家)當年交易日 ≥150 者
**幾乎全部為 0**(僅 2024 年 12 家)。同理,公司內部的季空洞(2019+ 每季 553~720 家)
交叉交易日後可交易者為 **0**(2013-2018 每年 6~11 家、2024 年 13 家、2025 年 2 家)。

---

## 6. ⚠️ SUSPECT:本表沒有「合併/個體」標記,而其中一部分是個體報表

`cash_flows_progressive` 沒有 `type` 欄,但它的姊妹表 `is_progressive_raw` /
`bs_concise_raw` 在 cache 端寫死 `type='consolidated'`。實際上本表混有**個體(非合併)報表**:

| 目錄 | cr 合併 | ir 個體 | er | 只有 ir 的公司數 |
|---|---|---|---|---|
| 2024_2 | 2,126 | 425 | 29 | 425 |
| 2024_4 | 2,166 | 447 | 29 | 447 |
| 2025_2 | 1,994 | 375 | 7 | 375 |

2024Q2 那 425 家「只有個體報表」的公司中,**163 家在 2024 年有交易**,包含
5269 祥碩(2024 年成交值 NT$5,113 億)、3374 精材、8028 昇陽半導體、2455 全新、6789 采鈺。

**這多半不是錯**:沒有子公司的公司本來就只編個體財報,那就是它唯一的正式財報
——5269 從 2024Q1 到 2025Q1 都是 `ir`,2025Q2 起變成 `cr`(開始有子公司),符合現實。
**問題在於資料裡沒有留下任何「這一列是哪種基礎」的標記**,消費者無從判斷;
而 `FinancialReader.readFinancialStatements` 選檔用的是
`.sortBy(_._1).distinctBy(_._2)`(type 字典序 cr < er < ir),同時有兩種時會取 cr
——結論正確但**是靠字母順序碰巧正確,不是寫明的意圖**。

順帶:TWSE 彙總報表把 5269 這種單體公司放進 `type='consolidated'` 那一份
(`is_progressive_raw` 中 5269 的 2024 四季都在),所以兩表的公司集合對得上
(2018 年起 `is_only_traded` ≈ 0),但兩邊對「合併」的定義其實不同。

---

## 7. 建議修法(不要自己下載,由主流程統一安排)

**(1) 補抓清單(按急迫度)**

| 季 | 缺在交易的家數 | 端點 |
|---|---|---|
| **2026Q1** | 1,410 | `https://mopsov.twse.com.tw/server-java/FileDownLoad?step=9&filePath=/home/html/nas/ifrs/2026/&fileName=tifrs-2026Q1.zip` |
| 2025Q2 | 122 | 同上,`.../ifrs/2025/&fileName=tifrs-2025Q2.zip` |
| 2023Q2 | 119 | `.../ifrs/2023/&fileName=tifrs-2023Q2.zip` |
| 2024Q1 | 13 | `.../ifrs/2024/&fileName=tifrs-2024Q1.zip` |

(URL 樣板見 `application.conf:98` + `FinancialStatementsSetting.scala:32`。
另可順手補 2021Q4 / 2022Q4 / 2025Q4,家數偏低但受害者全是非交易公司。)

**必須先刪掉 `data/financial_statements/<Y>_<Q>/` 或改掉存在性判斷**,否則
`Task.scala:182` 的 `filterNot(existFiles)` 會直接跳過。

**(2) 重抓是安全的(與 is_progressive 不同,這裡不會抹掉已下市公司)**
`FinancialReader.readFinancialStatements` 的 `filterNot(dataAlreadyInDB)` 以
`(market, year, quarter, companyCode)` 為粒度,且三張表各有自己的
`xxxDataAlreadyInDB` 守門,**只 INSERT 新公司、不刪既有列**,天然是 union 語意。
補抓後記得 `refresh materialized view cash_flows_individual` 與
`uv run python research/cache_tables.py`。

**(3) 根因守護(不修這裡就會有第四次、第五次)**
把「資料夾存在 = 抓完了」換成**用資料本身判斷**:例如「該季公司家數 < 前一年同季的
90% 就重抓」,或「連續兩次下載家數相同才封存」。**不要再塞一個猜出來的天數常數**
——閘門月份(`Task.scala:150-157`)本身就是猜出來的,而且猜早了。
另加回歸測試:同一 `(year,quarter)` 的公司集合只能單調成長;且任一季的
「當季可交易公司覆蓋率」不得低於前一年同季 X%(X 由歷史齊備季實測)。

**(4) 下游防呆(即使補完也該做)**
`raw_quarterly.py:176-182` 的 lag-diff 加連續性檢查:還原單季前先確認
`quarter - shift(quarter) == 1`,不連續就輸出 `null`;`cfo_ttm` 的
`rolling_sum(4)` 同理要確認視窗真的橫跨 4 個連續季。拿到誠實的空值,
遠好過一個「看起來很合理」的兩季合計。同一支檔案的 `rev_q/ni_q/op_income_q`
用同一個 shift 寫法,同樣要修。

---

## 附:重現指令

```bash
# 1. cache vs PG 逐季 checksum + schema + NULL/重複鍵
uv run --project research python docs/data_audit/scripts/C-cf_progressive_raw/parity.py

# 2. 逐季覆蓋 + CFO 科目別名覆蓋率 + 科目字典
uv run --project research python docs/data_audit/scripts/C-cf_progressive_raw/coverage.py

# 3. 會計恆等式 + 值域 + 單位尺度 + 代碼格式
uv run --project research python docs/data_audit/scripts/C-cf_progressive_raw/anomaly.py

# 4. 季序列缺口 + 公司內部空洞 + 是否可交易 + 2026Q1 缺料名單
uv run --project research python docs/data_audit/scripts/C-cf_progressive_raw/gaps.py

# 5. 亂數抽樣逐欄比對(seed 20260722)
uv run --project research python docs/data_audit/scripts/C-cf_progressive_raw/sample.py

# 6. lag-diff 灌水規模量化
uv run --project research python docs/data_audit/scripts/C-cf_progressive_raw/downstream.py

# 下載日 vs 完整度對照表
for d in data/financial_statements/*/; do \
  echo "$(basename $d) $(ls $d | wc -l) $(ls -lT $d | awk 'NR>1{print $6,$7,$9}' | sort | uniq -c | sort -rn | head -1)"; done
```
