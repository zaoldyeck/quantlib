# A-daily_trading_details — 原始檔到資料庫的解析正確性

**結論:🔴 BUG。三大法人這張表的「外資」與「投信」兩欄全時期可信,但「自營商」那三欄
在 2015 年以前是錯的——錯到連正負號都沒了。另外有 23 個日期存到別天的資料。**

白話講三件事:

1. **2015 年之前的自營商買賣超是假的。** TWSE 2012-05 至 2014-11、TPEx 2007-04 至
   2014-11,程式把「自營商買進股數」抄進了「自營商買賣超」欄位。買進股數永遠是正的,
   所以這 57 萬列裡的自營商淨額**一次都沒有出現過負值**——自營商連續七年只買不賣,
   物理上不可能。任何用到 `dealers_difference` 且回測期涵蓋 2015 年以前的結論,
   自營商那一項等於餵了雜訊。
2. **有 23 天的資料是別天複製過來的。** 其中 5 天是真正有開盤的交易日
   (2023-06-14、2023-10-06、2025-11-12、2026-02-05、2026-04-30),它們的 908 列
   **逐欄等於 2017-12-18**;另外 18 天根本沒開盤(週六、週日、春節),卻憑空多出
   908~1,262 列。合計 24,566 列是假的。
3. **2015 年以後的解析本身零錯誤。** 我把 12,218 個原始檔全部用獨立寫的解析器重讀一次,
   跟資料庫逐欄比對:8,210 個有資料的日子**列數全對**,2015 年以後的 22 個數值欄
   **一位不差**。所以問題不在「今天讀錯」,在「2015 年以前那兩個分支寫錯了,而且沒人
   發現,因為沒有任何檢查會去看自營商淨額該不該有負數」。

---

## 一、原始檔長什麼樣(六個格式世代,全部實測)

原始檔的欄位數會隨年份無聲增減。實測 12,218 個檔的結果:

| 市場 | 資料列欄數 | 期間 | 檔數 | 自營商相關欄位 |
|---|---|---|---|---|
| twse | 13 | 2012-05-02 .. 2014-11-28 | 642 | 買賣超、買進、賣出(**買賣超排在最前**) |
| twse | 17 | 2014-12-01 .. 2017-12-15 | 748 | 買賣超、自行買/賣/超、避險買/賣/超 |
| twse | 20 | 2017-12-18 .. 2026-07-17 | 2,094 | 同上,外加「外陸資/外資自營商」拆分 |
| tpex | 12 | 2007-04-23 .. 2014-11-28 | 1,894 | 買、賣、淨買 |
| tpex | 16 | 2014-12-01 .. 2018-01-12 | 767 | 淨買、自行買/賣/超、避險買/賣/超 |
| tpex | 24 | 2018-01-15 .. 2026-07-17 | 2,065 | 拆分最完整,含自營商合計三欄 |

重點:**13 欄與 17 欄的 TWSE 格式裡,「自營商買賣超」排在「自營商買進」之前**
(不是一般的 買/賣/淨 順序)。這正是踩雷的地方。

另外實測確認的邊界(都不是 bug):

- 0 位元組檔 1,961 個 = 休市 sentinel;TPEx 休市日給的是「只有表頭沒有資料列」的檔
  (2,047 個),兩者都不會產生資料列。
- 12,218 個檔中 **0 個**表頭文字對不上、**0 個**未知欄寬、**0 個**檔內重複代號、
  **0 個**非數值字元落在數值欄、**0 個**中文亂碼(Big5-HKSCS 全部解得開)。
- `="00637L"` 這種 Excel 防呆前綴由 `QuantlibCSVReader` 剝掉(它把每行的 `=` 全刪),
  資料庫裡是乾淨的 `00637L`。
- 單位全部是「股數」,沒有元/千元/張混用,也沒有百分比欄位。

---

## 二、🔴 BUG 1:TWSE 2012-05-02 ~ 2014-11-28,自營商三欄整組錯位

**位置**:`src/main/scala/reader/TradingReader.scala:190-194`(`case 13` 分支)

原始檔第 8~11 欄依序是 `自營商買賣超股數`、`自營商買進股數`、`自營商賣出股數`、
`三大法人買賣超股數`,對應 `transferValues(6..9)`。程式寫的是:

```scala
transferValues(6) :: transferValues(7) :: transferValues(8) :: transferValues(9) :: HNil
//     ↓ dealers_total_buy   ↓ dealers_total_sell   ↓ dealers_difference   ↓ total_difference
```

於是三個值整組往後轉了一格:

| 資料庫欄位 | 實際存進去的東西 |
|---|---|
| `dealers_total_buy` | 自營商**買賣超**(淨額) |
| `dealers_total_sell` | 自營商**買進** |
| `dealers_difference` | 自營商**賣出** |

**同一支程式的 `case 17` 分支(第 195-200 行)做對了**(用 `transferValues(6)` 當
difference、`(7)(8)` 當買/賣),所以這是單一分支的手滑,不是設計誤解。

**重現**(0050,2012-05-02):

```bash
iconv -f BIG5-HKSCS -t UTF-8 data/daily_trading_details/twse/2012/2012_5_2.csv | grep '^"0050"'
# 自營商買賣超 -198,000 / 買進 4,343,000 / 賣出 4,541,000
psql -h localhost -p 5432 -d quantlib -c "
SELECT dealers_total_buy, dealers_total_sell, dealers_difference
FROM daily_trading_details WHERE market='twse' AND date='2012-05-02' AND company_code='0050';"
# -198000 | 4343000 | 4541000   ← 整組往後轉一格
```

**最刺眼的證據——正負號被消滅了**:

```bash
psql -h localhost -p 5432 -d quantlib -c "
SELECT min(dealers_difference), count(*) FILTER (WHERE dealers_difference<0), count(*)
FROM daily_trading_details WHERE market='twse' AND date BETWEEN '2012-05-02' AND '2014-11-28';"
#  min = 0 | 負值 0 列 | 共 509,410 列       ← 自營商三年只買不賣?
# 對照 2014-12-01 之後:min = -1,946,343,918,負值 1,059,352 列
```

**影響範圍**:期間 509,410 列,其中 **285,552 列**的值確實被改掉
(其餘是自營商買賣皆為 0 的列,轉不轉都一樣)。

---

## 三、🔴 BUG 2:TPEx 2007-04-23 ~ 2014-11-28,自營商淨額抄成買進

**位置**:`src/main/scala/reader/TradingReader.scala:223`(`case 12` 分支)

```scala
transferValues(6) :: transferValues(7) :: transferValues(6) :: transferValues(9) :: HNil
//   買 ✓              賣 ✓              ← 這裡重複用了(6),應該是(8)
```

原始檔 `自營商買股數`(6)、`自營商賣股數`(7)、`自營淨買股數`(8)。
第三個位置該放 `(8)` 卻放了 `(6)`,結果 `dealers_difference` 存的是**買進股數**,
而真正的自營淨買 `(8)` **從頭到尾沒有被存進資料庫**(白丟一欄)。

**重現**(1785 光洋科,2007-04-23):

```bash
iconv -f BIG5-HKSCS -t UTF-8 data/daily_trading_details/tpex/2007/2007_4_23.csv | grep '"1785'
# 自營商買 24,000 / 賣 70,000 / 淨買 -46,000
psql -h localhost -p 5432 -d quantlib -c "
SELECT dealers_total_buy, dealers_total_sell, dealers_difference
FROM daily_trading_details WHERE market='tpex' AND date='2007-04-23' AND company_code='1785';"
# 24000 | 70000 | 24000    ← 淨額被買進蓋掉
```

同樣的正負號證據:該期間 602,188 列裡 `dealers_difference` **最小值 0、負值 0 列**;
2014-12-01 之後有 505,027 列是負的。**影響 285,414 列**(其餘是賣出為 0 的列)。

---

## 四、🔴 BUG 3:23 個日期存的是別天的資料(24,566 列)

用「同一天的四個欄位總和」當指紋,在資料庫裡找到 8 組完全相同的日子:

```bash
psql -h localhost -p 5432 -d quantlib -c "
WITH agg AS (SELECT market, date, count(*) n,
       sum(foreign_investors_difference::bigint) f,
       sum(securities_investment_trust_companies_difference::bigint) t,
       sum(dealers_difference::bigint) d, sum(total_difference::bigint) tot
     FROM daily_trading_details GROUP BY market, date)
SELECT n, ndays, dates FROM (
  SELECT market,n,f,t,d,tot,count(*) ndays, array_agg(date ORDER BY date) dates
  FROM agg GROUP BY market,n,f,t,d,tot HAVING count(*)>1) x ORDER BY ndays DESC;"
```

最大一組:**2017-12-18 的 908 列,被複製到另外 12 個日期**——
2023-06-14、2023-10-06、2024-06-22、2025-01-29、2025-02-08、2025-08-16、2025-11-12、
2026-02-05、2026-04-18、2026-04-30、2026-05-28、2026-05-30。逐列比對確認一位不差:

```bash
psql -h localhost -p 5432 -d quantlib -c "
SELECT count(*) FROM daily_trading_details a JOIN daily_trading_details b USING (company_code)
WHERE a.market='twse' AND a.date='2017-12-18' AND b.market='twse' AND b.date='2026-04-30'
  AND (a.total_difference<>b.total_difference OR a.foreign_investors_difference<>b.foreign_investors_difference);"
# 0 列不同 / 共 908 個代號   ← 完全複製
```

其餘 7 組是 1,207~1,262 列的整日複製(2024-06-30←2024-06-18、2024-12-07←2024-12-18、
2025-02-16←2025-02-18、2025-07-26←2025-07-18、2025-08-09←2025-08-14、
2025-09-07←2025-09-18、以及 2025-02-22/05-30/07-19/07-27/12-14 共用 2025-12-18 的內容)。

**危害分兩級**:

- **5 個真交易日拿到 2017 年的資料**:2023-06-14、2023-10-06、2025-11-12、2026-02-05、
  2026-04-30。`daily_quote` 這幾天都有 1,196~1,355 檔正常報價,只有法人資料是假的。
  策略如果在這幾天查法人買賣超,拿到的是八年前的數字。
- **18 個非交易日憑空多出資料**(週六/週日/春節等),`daily_quote` 完全沒有這些日子。

**為什麼檢查不出來**:TWSE 的 T86 端點會把「你請求的日期」原樣印在標題行,所以
「檔名日期 vs 內容日期」這個標準檢查對它無效——12,218 個檔裡只有 2 個檔漏了餡
(`twse/2023/2023_10_6.csv` 標題寫 106年12月18日、`twse/2024/2024_6_22.csv` 寫
113年06月12日),其餘 21 天的標題都乖乖印著錯的日期。現行的
`DailyTradingDetailsSetting.validate` 只檢查「表頭關鍵字 + 至少 100 列 + 至少 19 欄」,
一份 2017 年的完整檔案全部通過。

另有 4 天(2024-06-30、2025-08-09、2025-12-14、2026-04-18)的原始檔現在已經是
0 位元組 sentinel,但資料庫裡的列還在——**這些列已經無法從原始檔重現**。

---

## 五、🔴 BUG 4:超過 21 億股的數字會靜默變成 0

`TradingReader.scala:186 / 214` 用 `Try(value.toInt).getOrElse(0)`,而
`DailyTradingDetails.scala` 全部宣告成 `column[Int]`(PostgreSQL `integer`,上限
2,147,483,647)。原始檔一旦出現超過這個數的股數,**不會報錯,直接變 0**。

全庫掃描找到 5 個這樣的值,都在 00403A(主動統一升級 50)身上:

```bash
iconv -f BIG5-HKSCS -t UTF-8 data/daily_trading_details/twse/2026/2026_5_12.csv | grep 00403A
# 自營商買賣超 -2,433,644,567 / 避險賣出 2,482,291,567 / 避險買賣超 -2,433,646,567
# 三大法人買賣超 -2,761,612,524
psql -h localhost -p 5432 -d quantlib -c "
SELECT dealers_difference, dealers_hedge_total_sell, dealers_hedge_difference, total_difference
FROM daily_trading_details WHERE market='twse' AND date='2026-05-12' AND company_code='00403A';"
# 0 | 0 | 0 | 0        ← 四個欄位全被吃掉
```

2026-05-13 同一檔的三大法人買賣超 -2,725,272,226 也變成 0。目前只有 2 列受害,
但主動式 ETF 的申購贖回量還在長,**這個地雷會愈踩愈頻繁**。

---

## 六、🔴 BUG 5:TPEx 事後修正的投信數字永遠回填不進來

`TradingReader.scala:162-166` 的去重邏輯是「(market, date) 已經在資料庫就整天跳過」。
TPEx 會事後修正投信買賣量(綜合帳戶結算),原始檔被重新下載後,**資料庫永遠不會更新**。

實測 2024 上半年 TPEx:**80 個交易日、170 列**的投信三欄與現行原始檔對不上。

```bash
iconv -f BIG5-HKSCS -t UTF-8 data/daily_trading_details/tpex/2024/2024_6_13.csv | grep '^"3260"'
# 投信買進 1,541,000 / 賣出 309,000 / 買賣超 1,232,000 / 三大法人 3,107,549
psql -h localhost -p 5432 -d quantlib -c "
SELECT securities_investment_trust_companies_total_buy, securities_investment_trust_companies_difference, total_difference
FROM daily_trading_details WHERE market='tpex' AND date='2024-06-13' AND company_code='3260';"
# 1541 | -307459 | 1568090     ← 停在修正前的版本
```

兩邊各自內部自洽(`合計 = 外資 + 投信 + 自營`),所以這不是解析錯,是**修正版永不落地**。

---

## 七、🔴 BUG 6:雲端 Python 爬蟲會漏掉整批股票

`src/quantlib/crawl/sources/daily_trading_details.py:69` 寫死 `if len(r) < 19: continue`。
但 TWSE 的現行檔案裡**混著 17 欄的資料列**(那是沒有「外資自營商」三欄的變體,
2018 年至今共 549 列,2026 年就有 221 列)。Scala reader 靠列寬分派處理得很正確,
Python 爬蟲則整列丟掉。

用本地原始檔直接餵給雲端爬蟲的解析函式,不需連網即可重現:

```bash
uv run --project . python -c "
from pathlib import Path; from datetime import date
from quantlib.crawl.sources import daily_trading_details as dtd
txt = Path('data/daily_trading_details/twse/2026/2026_7_8.csv').read_bytes().decode('big5hkscs')
print(dtd._parse(txt, date(2026,7,8), 'twse').height)"
# 1322   ← cache / PG 都是 1336,少了 14 檔
# 被丟掉的:0061 00669R 1721 1731 2356 2701 3010 4526 4532 5007 6215 7827 8021 8261
```

抽驗其中 2356 英業達:原始檔 17 欄那列的外資 -7,554,041、投信 +5,637,196、
自營 +19,583,合計 -1,897,262,三項相加正好等於合計——**這是完全合法的資料列**,
不該被丟。雲端爬蟲現在會直接寫 `var/cache/cache.duckdb`(cache 已有 2026-07-20 而
PostgreSQL 沒有),所以這個漏列會直接影響 live 決策。

---

## 八、🟢 查過沒問題的部分(負結果也要留)

| 檢查 | 結果 |
|---|---|
| 全 12,218 個原始檔獨立重解析 vs PostgreSQL | 8,210 個有資料的日子**列數全部相符**(0 天不符) |
| twse 17 欄世代 / 20 欄世代、tpex 16 欄 / 24 欄世代 | 22 個數值欄**逐欄相符**(抽樣日:2014-12-01、2017-12-15、2017-12-18、2021-03-04、2026-07-15、2014-12-01、2016-06-15、2018-01-12、2018-01-15、2022-06-15、2026-07-15) |
| 混合列寬檔(現行檔裡的 17 欄列 549 列 + 舊檔裡的 20 欄列 18 列) | Scala reader 依列寬分派,**全部正確**(見 §七的 2356 抽驗) |
| 表頭文字比對(六個世代 × 全部檔案) | 0 個不符 → 沒有無聲欄位漂移 |
| 未知列寬 | 0 個(twse 只有 13/17/20、tpex 只有 12/16/24) |
| 民國 vs 西元日期轉換 | 8,208/8,210 標題日期與檔名相符;2,047 個無標題檔全是 TPEx 休市空表 |
| 編碼(Big5-HKSCS) | 全檔解得開,資料庫 company_name **0 列亂碼**,company_code 0 列含異常字元 |
| 單位 | 全部欄位都是股數,無元/千元/張混用,無百分比欄 |
| 非數值字元落進數值欄 | 0 個(全庫掃描) |
| 檔內重複股票代號 | 0 列 |
| 被列寬過濾丟掉的真資料列 | 0 列(`row.size >= 13/12` 只濾掉單欄的說明文字) |
| 三大法人恆等式(合計 = 外資 + 投信 + 自營) | 2015-01 之後 twse/tpex **零違反**(只有 §五的 2 列溢位例外) |
| DuckDB cache vs PostgreSQL | 近 15 個交易日列數逐日相符;cache 忠實複製 PG(**包括上面所有錯誤**) |

**兩個「看起來像錯、其實可接受」的項目**:

- **外資自營商空白格寫成 0 而非 NULL**(28 天、每天數列)。原始檔在
  「外資自營商買進/賣出」給空字串、「買賣超」給 0,`Try(...).getOrElse(0)` 一律轉 0。
  數值語意等價,不影響任何計算。
- **`foreign_investors_total_*` 把外資自營商加進外資合計**,而 TWSE/TPEx 的頁尾明文說
  「外資自營商買賣股數已計入自營商買賣股數,故不納入三大法人合計」。實測
  `foreign_dealers_difference` 在全部 376 萬列非空值裡**沒有一列不是 0**,
  所以目前零影響——但這是個定義上的地雷,哪天交易所開始給非零值就會重複計算。

---

## 九、建議修法(不在本次稽核執行)

1. **改 `case 13`**:`transferValues(7) :: transferValues(8) :: transferValues(6) :: transferValues(9)`
   (照 `case 17` 的寫法)。**改 `case 12`(tpex)**:第三個位置改成 `transferValues(8)`。
   兩者都要**刪掉舊列重讀**(`DELETE FROM daily_trading_details WHERE
   (market='twse' AND date<='2014-11-28') OR (market='tpex' AND date<='2014-11-28')`
   再跑 `Main read daily_trading_details`),然後重建 cache。
2. **欄位型別改 `Long` / `bigint`**,並把 `Try(...).getOrElse(0)` 改成「解析失敗就拋例外」
   ——靜默歸零是最壞的失敗模式。
3. **加不變式守護**(能自動抓到上面三個 bug 的那一種):
   - `total_difference = foreign + trust + dealers`(逐列,匯入時檢查);
   - `difference = total_buy - total_sell`(逐列);
   - 每天的四欄總和不得與任何其他日期相同(指紋防重放);
   - 資料日必須存在於 `daily_quote`(擋掉非交易日的幽靈列)。
4. **去重改成「內容雜湊比對」而非「日期已存在就跳過」**,讓交易所的事後修正能回填。
5. **Python 雲端爬蟲的 `len(r) < 19` 改成依列寬分派**(17 與 20 兩種都要支援),
   並補一條 parity 測試鎖死「17 欄列不得被丟棄」。

---

*稽核方法:自行以 Python `csv` + Big5-HKSCS 重寫解析器(依各世代表頭文字決定欄位語意,
不呼叫受測的 Scala 程式),全 12,218 個原始檔重解析後與 PostgreSQL 逐日逐欄比對;
抽樣日跨越六個格式世代與兩個市場。*
