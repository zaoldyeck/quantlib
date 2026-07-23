# C-daily_trading_details — cache 與 PostgreSQL 的一致性與缺漏

**結論:🔴 BUG。「兩邊一不一致」完全沒問題,「資料齊不齊」有問題。**

白話講四件事:

1. **DuckDB cache 跟 PostgreSQL 的三大法人資料一模一樣**——全史 8,214 個交易日、
   586.9 萬列、4 個數值欄,逐日逐欄比對零差異。用 cache 查跟用 PG 查會拿到同樣的
   答案,這條可以放心。
2. **但有 7 個真的有開市的日子,整天的三大法人資料不見了**,而且**兩條補抓路徑都
   永遠不會自己補回來**——爬蟲把交易所回的空回應當成檔案存了下來,系統看到「檔案
   存在」就當這天做完了。7 天全部落在 2023 年之後,**現役 Serenity 策略的驗證窗與
   實盤期都在裡面**。
3. **cache 唯一領先 PG 的那一天(2026-07-20)沒有原始檔可以核對**,而寫它的那支
   Python 解析器,在**有原始檔可核的 53 個 TWSE 交易日裡少寫了 370 列**。也就是說
   cache 最新那天可能少了十幾檔股票的法人買賣超,而且無從查證。
4. **cache 忠實地把 PG 的錯也複製了過來**——A 維查出的「2015 年以前自營商欄位整組
   錯位」(111 萬列)與「23 天存的是別天的資料」,在 cache 這邊用完全獨立的方法
   (內容指紋)也全部重現。一致性 OK,但是**一致地錯**。

---

## 一、cache vs PostgreSQL:逐欄全等(🟢 OK)

不是抽樣,是**全體**。兩邊各自算每個 `(market, date)` 的三個指紋——列數、
`sum(hash(四欄)::HUGEINT)`、`bit_xor(hash(四欄))`——再 FULL JOIN 比對。

| 項目 | PostgreSQL | DuckDB cache |
|---|---|---|
| 總列數 | 5,869,005 | 5,871,268 |
| `(market,date)` 天數 | 8,214 | 8,216 |
| 共同天數上 count / 雜湊不符者 | **0 天** | |
| 只在 cache | — | `2026-07-20`(twse 1,337 + tpex 926 = 2,263 列) |
| 只在 PG | **無** | |

逐年列數(2007~2026 × twse/tpex)兩邊**每一格都相等**,只有 2026 年 cache 各多出
2026-07-20 的那一天。差的 2,263 列就是它(cache 領先 PG,原因見第四節)。

重跑:`docs/data_audit/scripts/C-daily_trading_details/01_parity.py`

抽樣佐證(3 個日期 × 5 檔,`pandas.DataFrame.equals` = **True**):

| 樣本 | 結果 |
|---|---|
| twse 2013-06-14 × 2330 / 2317 / 1101 / 3008 / 2412 | pg=5 cache=5 **equals=True** |
| tpex 2019-11-07 × 6488 / 3105 / 5483 / 8069 / 4966 | pg=5 cache=5 **equals=True** |
| twse 2026-07-17 × 2330 / 2317 / 1101 / 3008 / 6488 | pg=4 cache=4 **equals=True**(6488 是上櫃,twse 當然沒有) |

**schema**:cache 只投影 PG 26 欄中的 **7 欄**,型別**零降級**
(`varchar`→`VARCHAR`、`date`→`DATE`、`integer`→`INTEGER`),欄名做了一次改名
(`securities_investment_trust_companies_difference` → `trust_difference`)。
被丟掉的是 `id`、`company_name`、以及 18 個「買進/賣出毛額 + 自營拆自行買賣與避險 +
外資自營商」欄位(`research/cache_tables.py:45`)。全 repo 搜尋 `dealers_hedge` /
`dealers_proprietary` / `*_total_buy` / `foreign_dealers` 在 `research/` 與 `docs/` 下
**零命中**——目前沒有任何消費者需要它們,是刻意的取捨,不是漏欄。

**pg-attach 模式無漂移**:`research/db.py:126-130` 的 view 定義與
`research/cache_tables.py:45` 的 SELECT **逐字同構**(同 7 欄、同改名),兩種存取
模式不會給出不同的欄位集合。

**唯一結構性落差**:PG 有 `idx_DailyTradingDetails_market_date_companyCode` UNIQUE,
cache 只有非唯一的 `idx_dtd_code_date(company_code, date)`(`CREATE TABLE AS` 不帶
約束)。實測目前重複主鍵 **0 筆**,但保護是靠寫入端自律(`research/crawl/sink.py:35-59`
的「刪整日 + 插」),不是靠資料庫。

---

## 二、7 個真交易日整天缺漏(🔴 BUG)

**兩個互相獨立的方法,收斂到完全相同的 7 天。**

**方法 A(從資料庫側)**:在 dtd 的起訖區間內,找「別的日頻表有資料、dtd 一列都
沒有」的日子。證人 6 張表:`daily_quote`、`stock_per_pbr`、`margin_transactions`、
`market_index`、`sbl_borrowing`、`foreign_holding_ratio`。
腳本:`docs/data_audit/scripts/C-daily_trading_details/02_gaps.py`

**方法 B(從原始檔側)**:找「原始檔是空回應(TWSE 2 bytes 的 CRLF / TPEx 只有
表頭的 512 bytes)**且** `research.data_calendar.is_trading_day` 為真 **且** DB 無列」
的日子。全 4,008 個空檔裡只有 7 個中標。
腳本:`docs/data_audit/scripts/C-daily_trading_details/05_empty_on_trading_day.py`

| 市場 | 缺漏日 | 星期 | 原始檔 | 同日 daily_quote | 其他證人 |
|---|---|---|---|---|---|
| twse | **2023-08-30** | 三 | 2 B | 1,201 | fhr 1,173 / margin 1,120 / index 249 / sbl 1,125 / pbr 980 |
| twse | **2025-12-22** | 一 | 2 B | 1,329 | fhr 1,308 / margin 1,241 / index 267 / sbl 1,244 / pbr 1,062 |
| twse | **2026-02-23** | 一 | 2 B | 1,344 | fhr 1,323 / margin 1,251 / index 267 / sbl 1,256 / pbr 1,068 |
| twse | **2026-03-18** | 三 | 2 B | 1,344 | fhr 1,322 / margin 1,252 / index 267 / sbl 1,259 / pbr 1,068 |
| twse | **2026-03-31** | 二 | 2 B | 1,348 | fhr 1,326 / margin 1,255 / index 267 / sbl 1,262 / pbr 1,070 |
| tpex | **2023-06-08** | 四 | 512 B(只有表頭) | 0 ※ | fhr 815 / margin 775 / index 63 / sbl 804 / pbr 808 |
| tpex | **2023-06-09** | 五 | 512 B(只有表頭) | 907 | fhr 814 / index 63 / sbl 803 / pbr 808 |

※ tpex 2023-06-08 的 daily_quote 也整天不見,那是 C-daily_quote 單位已列的 BUG;
本表用另外 5 個證人獨立確認該日確實開市。`is_trading_day` 對 7 天**全部回 True**。

**為什麼永遠補不回來(根因,兩條路徑都中招)**:

- **Scala 路徑**:`src/main/scala/Crawler.scala:620-684` 把交易所回的空 body 原樣落檔
  (只有 `<html>` 開頭才會刪檔重試);`src/main/scala/setting/Detail.scala:132-169`
  的 `getDatesOfExistFiles` 只要「檔案存在 + 開頭不是 `<html>`」就把該日算進
  existFiles;`src/main/scala/Task.scala:576` 的
  `startDate.datesUntil(endExclusive).toScala(Seq).filterNot(existFiles)` 於是永久跳過。
- **Python 路徑**:`research/crawl/update.py:30-41` 的 `_missing_days` 從
  `max(date) + 1` 起往前走,**只前進不回頭**,歷史的洞看不到。

**這 7 天為什麼要緊**:全部落在 2023 年之後,現役 Serenity 事件引擎的驗證窗
(train 2022-07~2025-07)與實盤期都在裡面。引擎讀的正是這張表——
`research/serenity/engine.py:971-981` 取 `total_difference` 做 20 日 rolling sum
(`inst_20d`),缺一天等於那支股票的 20 日法人淨額實際涵蓋了 21 個交易日的歷史,
而 `inst_neg` 出場閘門就是看這個數字。

---

## 三、cache 忠實複製了 PG 的錯(🔴 BUG,繼承自 A 維)

cache 一致性沒問題,問題是**一致地錯**。以下全部用 **cache 端獨立的方法**複驗,
不是引用 A 維的結論。

### 3.1 2015 年以前自營商欄位錯位 → cache 的 `dealers_difference` 七年零負值

| 市場 × 世代 | 列數 | `dealers_difference < 0` 的列數 | 最小值 | 最大值 |
|---|---|---|---|---|
| twse ≤ 2014-11-28 | 509,410 | **0** | 0 | 131,642,000 |
| tpex ≤ 2014-11-28 | 602,188 | **0** | 0 | 42,737,000 |
| twse ≥ 2014-12-01 | 2,947,274 | 1,059,778 | −1,946,343,918 | 417,256,187 |
| tpex ≥ 2014-12-01 | 1,812,396 | 505,423 | −198,943,272 | 164,174,929 |

**買賣超不可能七年只有正數。** 受影響 1,111,598 列 = cache 的 **18.9%**。

同一件事的第二個指紋:恆等式 `total = 外資 + 投信 + 自營` 在 cache 破裂 **568,922 列**,
分布是 twse pre-2015 283,506 + tpex pre-2015 285,414 + **post-2015 只有 2 列**——
斷點乾淨地落在 2014-11-28/12-01 的世代交界。

post-2015 那 2 列是 int32 溢位:

| market | date | code | foreign | trust | dealers | total |
|---|---|---|---|---|---|---|
| twse | 2026-05-12 | 00403A | −327,967,957 | 0 | 0 | **0** |
| twse | 2026-05-13 | 00403A | −778,928,308 | 0 | −1,946,343,918 | **0** |

### 3.2 23 天存的是別天的資料(cache 端以內容指紋獨立複驗)

對每個 `(market, date)` 算「四欄各自加總 + 列數」做指紋,找撞號:

| 列數 | 撞號天數 | 日期 |
|---|---|---|
| 908 | **13** | 2017-12-18 / 2023-06-14 / 2023-10-06 / 2024-06-22 / 2025-01-29 / 2025-02-08 / 2025-08-16 / 2025-11-12 / 2026-02-05 / 2026-04-18 / 2026-04-30 / 2026-05-28 / 2026-05-30 |
| 1260 | 6 | 2025-02-22 / 2025-05-30 / 2025-07-19 / 2025-07-27 / 2025-12-14 / 2025-12-18 |
| 1213 | 2 | 2024-12-07 / 2024-12-18 |
| 1219 | 2 | 2025-02-16 / 2025-02-18 |
| 1262 | 2 | 2025-09-07 / 2025-09-18 |
| 1238 | 2 | 2025-08-09 / 2025-08-14 |
| 1207 | 2 | 2024-06-18 / 2024-06-30 |
| 1231 | 2 | 2025-07-18 / 2025-07-26 |

8 組、31 個日期、**23 個是複本**。四欄加總完全相同的機率實務上等於零。

---

## 四、cache 最新那天沒有原始檔可核,而寫它的解析器會漏列(🔴 BUG)

cache 的 2026-07-20 只存在於 cache(PG 全表 max(date) = 2026-07-17),由
`research/crawl/`(Python 直寫 DuckDB)寫入,**不落原始檔**。所以那一天無法用
A 維的方法(原始檔逐欄核 DB)驗證。

那就退一步問:同一支解析器在**有原始檔可核的日子**表現如何?

拿 `research.crawl.sources.daily_trading_details._parse` 重解析全部 4,132 個有原始檔
且解析器吃得下的日子(已排除第三節那 31 個內容重放日),和 cache 該日列數比:

```
可比對日數:4,132
列數不符日數:53(全部 twse),累計少寫 370 列
```

年份分布:2018 年 3 天 / 2020 年 4 / 2021 年 6 / 2022 年 1 / 2023 年 1 / 2024 年 11 /
2025 年 14 / **2026 年 13**。單日最多少寫 54 列。最近一次可核的例子:
**twse 2026-07-08,python=1,322 vs cache=1,336,少 14 列**。

**原因**:`research/crawl/sources/daily_trading_details.py:69` 的
`if len(r) < need: continue`(twse `need=19`)把 TWSE 現行檔案裡合法的 17 欄資料列
整列丟掉。Scala reader 有 `case 17` 分支正確處理,Python 沒有。

**所以 cache 的 2026-07-20(twse 1,337 列)可能少了十幾檔股票的法人買賣超,而且
沒有原始檔可以查。** 以 2026 年的命中率推估,約每 10 個交易日就有 1 天中招。

腳本:`docs/data_audit/scripts/C-daily_trading_details/04_python_writer_drop.py`

---

## 五、欄位型別 INTEGER 是活的地雷(🟡 SUSPECT)

cache 四個數值欄是 `INTEGER`(int32,繼承自 PG 的 `integer`)。兩個後果:

**(a) 讀端會炸。** 直接在 cache 上算 `foreign + trust + dealers` 會丟例外:

```
_duckdb.OutOfRangeException: Out of Range Error:
Overflow in addition of INT32 (-778928308 + -1946343918)!
```

全表**有 1 列**會觸發(twse 2026-05-13 00403A)。目前的消費者
(`research/strat_lab/iter_32_first_principles.py:216-218` 把三欄相加、
`research/futures/strategies.py:159-162` 全市場加總)只要碰到那一天就會中止。

**(b) 寫端會炸。** Python 解析器宣告的是 `pl.Int64`
(`research/crawl/sources/daily_trading_details.py:29-31`),寫進 INTEGER 欄位時
DuckDB 會嘗試轉型;實測超過 int32 就丟

```
ConversionException: Type INT64 with value -2725272226 can't be cast
because the value is out of range for the destination type INT32
```

也就是說,再出現一次 00403A 那種規模的法人單量,**每日 07:20 的 cache 更新會整支
中止**(至少是 fail-loud,不是靜默歸零,但當日 live 決策會沒有新資料)。

---

## 六、異常值掃描(🟢 OK)

對 cache 全表 5,871,268 列:

| 檢查 | 筆數 |
|---|---|
| 重複 `(market, date, company_code)` | **0** |
| `date` 在未來 | **0** |
| `market` 非 twse/tpex | **0** |
| `company_code` 非英數 | **0** |
| 任一數值欄為 NULL | **0** |
| 絕對值觸 int32 上界(> 2.1e9) | **0** |
| 恆等式 `total = 外資+投信+自營` 破裂 | 568,922 → **全部歸因**(見 3.1) |
| 三欄相加超出 int32 | 1 → **已歸因**(見第五節) |
| 四欄全為 0 | 124,496(2.12%)→ **正常**(見第七節) |

腳本:`docs/data_audit/scripts/C-daily_trading_details/03_sample_and_anomaly.py`

---

## 七、看起來像錯、其實是真的(⚪ REAL)

**(a) 四欄全為 0 共 124,496 列(2.12%)** — 集中在上櫃小型股(tpex 2021 年 12,085 列
最多),就是「那天沒有任何法人買賣這檔」。dtd 只列有法人進出的股票,所以列數天生
少於 daily_quote,這是資料本身的語義。

**(b) 單日列數只有平常一半的 14 天,全部是週六補行交易日。** 逐一查週幾:
2012-02-04、2012-03-03、2012-12-22、2013-02-23、2013-09-14、2014-12-27、2016-01-30、
2016-06-04、2016-09-10、2017-02-18、2017-06-03、2017-09-30、2018-03-31、2018-12-22
**全是 Saturday**。以 2016-06-04 為例:daily_quote twse 951 檔有 937 檔有成交,但
只有 413 檔有法人進出——補行交易日法人參與度本來就低。

**(c) 2025-04-07(關稅崩盤日)tpex 只有 618 列(中位數 843)。** 同日 daily_quote
tpex 957 檔、948 檔有成交,隔天 2025-04-08 就回到 871 列。全市場跌停鎖死那天法人
成交檔數本來就會腰斬,不是漏抓。

**(d) 145 個「只有 1 個證人」的疑似缺口,全部不是缺口。** 逐日跑
`is_trading_day` → **145 天全部回 False**(50 個週六 + 51 個週日 + 44 個平日假期)。
證人分布:tpex `foreign_holding_ratio` 111 天、twse `sbl_borrowing` 16、
tpex `margin_transactions` 7、twse `stock_per_pbr` 6、twse `market_index` 3、
twse `foreign_holding_ratio` 2。**是那些表在非交易日有幽靈列**(屬各自單位的問題),
dtd 在那些日子沒有資料才是對的。

**(e) TWSE 起點 2012-05-02 不是缺漏,是交易所的資料界線。** 第一手來源:
TWSE「三大法人買賣超日報」查詢頁明文 **「本資訊自民國101年5月2日起提供」**
(<https://www.twse.com.tw/zh/trading/foreign/t86.html>)。程式對得上:
`src/main/scala/setting/DailyTradingDetailsSetting.scala:6` 把 twse firstDate 寫死
為 `LocalDate.of(2012, 5, 2)`。daily_quote twse 從 2004-02-11 起有 2,047 個交易日
沒有對應的三大法人資料——**這 2,047 天是永遠拿不到的,任何用法人籌碼的策略不得把
2012-05-02 以前的 TWSE 樣本算進去**。TPEx 起點 2007-04-23(同檔第 22 行),比
tpex daily_quote 的 2007-07-02 還早,不構成缺口。

---

## 八、順帶記錄:表間日期錯位仍在(🟡 SUSPECT,與 C-daily_quote 同一件事)

實測 `research.data_calendar`:

```
latest_complete_trading_day() = 2026-07-21
stale_tables() = {daily_quote: 2026-07-20, market_index: 2026-07-17,
                  daily_trading_details: 2026-07-20, stock_per_pbr: 2026-07-20,
                  margin_transactions: 2026-07-17, sbl_borrowing: 2026-07-17,
                  foreign_holding_ratio: 2026-07-17}
```

Python 直寫覆蓋的 3 張表到 07-20,只有 Scala 路徑的 4 張表停在 07-17,齊備日是 07-21
——**7 張表沒有一張到齊**。這正是 CLAUDE.md 記載 2026-07-15 事故的形態
(表間日期錯位 → 策略閘門查無資料 fail-closed 靜靜砍光候選)。細節與修法見
`docs/data_audit/findings/C-daily_quote.md` 第四、五節,此處只記錄本表也在其中。

---

## 附錄:重跑

```bash
cd /Users/zaoldyeck/Documents/scala/quantlib
export PYTHONPATH=/Users/zaoldyeck/Documents/scala/quantlib
S=docs/data_audit/scripts/C-daily_trading_details
uv run --project research python $S/01_parity.py              # cache vs PG 全史逐欄
uv run --project research python $S/02_gaps.py                # 缺口(證人表投票)
uv run --project research python $S/03_sample_and_anomaly.py  # 抽樣 + 異常值
uv run --project research python $S/04_python_writer_drop.py  # Python 直寫路徑漏列(~8 分鐘)
uv run --project research python $S/05_empty_on_trading_day.py # 缺口(原始檔側獨立確認)
```

依賴:`var/cache/cache.duckdb` 為當前世代(mtime 2026-07-21 08:16)、
PostgreSQL `localhost:5432/quantlib`、`data/daily_trading_details/` 原始檔 12,218 個。
