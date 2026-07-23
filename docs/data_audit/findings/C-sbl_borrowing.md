# C-sbl_borrowing:cache 一致性與缺漏

**結論(白話):cache 抄得一字不差,但被抄的那份東西有 26 天裝錯日期、32 天整天沒抓到。**

DuckDB cache 和 PostgreSQL **零不一致**——469 萬列、9 個欄位逐位相同,5,116 個「市場×日期」
的指紋全部相等,連多一天少一天都沒有。所以「cache 有沒有走樣」可以放心。

不能信的是資料本身:

1. **26 天的檔案裝的是別天的資料**(2.6 萬列,全在上市 TWSE)。最誇張的一筆:
   `2017-06-30`(星期五、真的有開市)存的是 **2022-05-23** 的借券餘額——把五年後的答案
   寫進 2017 年。其中 16 天根本不是交易日(週末、農曆年、元旦),卻整天有一千多檔資料。
2. **32 個「市場×交易日」整天沒有資料**(估計缺 3.3 萬列)。其中 26 天被 0-byte 空檔
   蓋住,爬蟲會永遠當作「已經抓過」而不再回頭;另外 6 天是因為 `daily_quote` 留下的
   假休市檔讓爬蟲的交易日曆以為「那天沒開市」,連請求都沒送出去。
   **這兩類都已用交易所第一手回應證實資料其實存在**(現在去抓,4 個測試日 4 個都有完整資料)。
3. **沒有任何守護擋得住這一類問題**:`src/quantlib/audits/04_cross_verify.py` 這支「檔名日期
   vs 內容日期」的檢查器,程式碼第 49 行寫死只掃 `data/index`,借券表從來沒被掃過;
   `src/quantlib/audits/03_full_data_audit.py` 也完全沒有借券的檢查。

其餘 462 萬列可信:抽樣 15/15 逐欄相同、對原始 CSV 逐格核對過欄位沒有錯位(存的是「借券賣出」
區塊而不是「融券」區塊)、找不到 NULL / 負餘額 / 未來日期 / 重複主鍵 / 單日列數腰斬。
看起來離譜的東西(七成的列「餘額 > 次一營業日限額」、6.6% 的列六個數字全 0、38 列加減
對不起來)查證後全是真的。

---

## OK:cache 與 PostgreSQL 全史逐欄一致(非抽樣)

| 檢查 | 結果 |
|---|---|
| 全表列數 | PG 4,691,508 = cache 4,691,508 |
| (market, date) 指紋 | 5,116 組,`count` + `sum(hash(9 欄)::HUGEINT)` + `bit_xor(hash(9 欄))` 三者全等;mismatch=0、only_cache=0、only_pg=0 |
| 年×市場列數 | 22 格全等(twse/tpex × 2016~2026) |
| schema | PG `varchar/date/bigint` → cache `VARCHAR/DATE/BIGINT`,**零型別降級**;丟掉 `id` 與 `company_name` 兩欄,`company_name` 在 `research/` 全文搜尋零消費者 |
| 兩條投影是否漂移 | `research/cache_tables.py:53`(CREATE TABLE)與 `src/quantlib/db.py:145-148`(pg-attach view)正規化後**逐字元相同**(程式化比對回 True) |
| 抽樣逐欄 | twse 2016-04-06 × {0050,1101,2317,2330,2412}、tpex 2019-11-07 × {3105,4966,5483,6488,8069}、twse 2026-07-17 × {2317,2330,2412,2454,3008} → `DataFrame.equals` 全 True |
| 重複主鍵 | (market,date,company_code) 重複 0 筆 |
| 數值邊界 | 全部 BIGINT,最大 1,297,614,402(twse 2887 台新新光金 2025-07),無 int32 溢位風險 |

**欄位對位核對(對原始檔逐格)**:`data/sbl_borrowing/twse/2016/2016_4_6.csv` 的 0050 那一列是
`="0050","元大台灣50","2,884,000","40,000","16,000","0","2,908,000","284,625,000","36,467,000","0","0","0","36,467,000","3,249,198"," "`。
第 2~7 欄是**融券**、第 8~13 欄才是**借券賣出**。cache 該列 = prev 36,467,000 / sold 0 / ret 0 /
adj 0 / bal 36,467,000 / limit 3,249,198 —— 正是第 8~13 欄,沒有抄到融券那一段。

證據腳本:`docs/data_audit/scripts/C-sbl_borrowing/01_parity.py`、`05_sample_and_anomaly.py`。

---

## BUG 1:26 天的資料是別天的(2.6 萬列,含最多五年的前視)

**怎麼發現的**:`TradingReader.readSblBorrowing` 的日期**只從檔名解析**
(`src/main/scala/reader/TradingReader.scala:833-835`:`val fileNamePattern(y,m,d) = marketFile.file.name;
val date = LocalDate.of(...)`),完全不看檔案自己寫的日期。所以全量掃了 6,266 個原始檔的
「檔名日期 vs 內容自報日期」(TWSE CSV 第一列的民國日期、TPEx JSON 的 `date` 欄)。

**兩條互相獨立的證據收斂到同一組 26 天**:

* 原始檔側(`03_content_date_verify.py`):26 個檔案的內容日期 ≠ 檔名日期,而且**內容日期撞號
  20 組**(同一天的資料被存成 2~6 個不同檔名,例如 2017-12-18 的資料同時掛在
  2016-10-29 / 2017-07-04 / 2017-09-23 / 2017-12-14 / 2017-12-18 / 2021-05-16 六個日期底下)。
* cache 側(`04_dup_day_fingerprint.py`,完全不看原始檔):整日內容指紋
  `sum(hash(code||六個數字))` 撞號 **36 對**,收斂成 20 個群集,與原始檔側的 20 組**完全相同**。

### A. 10 個真交易日,存的是別天的資料(10,175 列)

| market | 日期 | 星期 | 實際內容日期 | 差幾天 | 列數 |
|---|---|---|---|---|---|
| twse | 2016-04-08 | Fri | 2016-10-18 | **+193** | 945 |
| twse | 2016-12-20 | Tue | 2017-01-18 | **+29** | 956 |
| twse | 2017-05-15 | Mon | 2017-11-17 | **+186** | 1000 |
| twse | 2017-06-30 | Fri | 2022-05-23 | **+1788** | 1096 |
| twse | 2017-07-04 | Tue | 2017-12-18 | **+167** | 1003 |
| twse | 2017-12-14 | Thu | 2017-12-18 | **+4** | 1003 |
| twse | 2018-01-12 | Fri | 2017-12-12 | −31(過期) | 1003 |
| twse | 2018-09-06 | Thu | 2019-08-06 | **+334** | 1056 |
| twse | 2018-10-08 | Mon | 2018-10-12 | **+4** | 1021 |
| twse | 2018-12-12 | Wed | 2022-03-31 | **+1205** | 1092 |

正號 = 把未來的資料寫進過去 = **前視汙染**。9/10 是前視。

**旁證(不依賴任何日期欄)**:全史只有兩天「借券表的檔數比同日 `daily_quote` 還多」——
twse 2017-06-30(1,096 vs 1,008,多 88 檔)與 2018-12-12(1,092 vs 1,073,多 19 檔)——
正是上表裡內容來自 2022 年的那兩天。2022 年的上市家數當然比 2017/2018 多。

### B. 16 個非交易日卻有整天資料(16,179 列)

twse 2016-08-07(日)、2016-10-29(六)、2017-01-02(元旦補假)、2017-08-06(日)、
2017-09-23(六)、2018-01-06(六)、2018-02-19(春節)、2018-04-28(六)、2018-10-07(日)、
2018-10-13(六)、2021-02-12(春節)、2021-05-16(日)、2021-11-13(六)、2022-02-01(春節)、
2022-06-05(日)、2022-06-19(日)。

已排除真正的**週六補行交易日**(2016-01-30、2016-06-04、2016-09-10、2017-02-18、2017-06-03、
2017-09-30、2018-03-31、2018-12-22 — 這 8 天 `daily_quote` 與 `margin_transactions` 都有資料,
是真的有開市,借券表有資料才是對的)。

### 根因與現況

| 環節 | 事實 |
|---|---|
| 檔案時間 | 26 個壞檔的 mtime 全部落在 **2026-04-25**(那次 2016 起的歷史回補) |
| 端點現在的行為 | 今天重抓 `TWT93U`:週日 `20160807` 回 1,616 bytes 的空表且標題正確寫「105年08月07日」;真交易日 `20170630` 回 108,636 bytes 且標題正確寫「106年06月30日」。**端點本身沒有「回最新一天」的毛病,重抓就會正確** |
| 讀檔 | `TradingReader.scala:833-835` 只信檔名,從不核對內容日期 |
| 守護 | `src/quantlib/audits/04_cross_verify.py:49` 寫死 `root = Path("data/index")`,借券表不在掃描範圍 |

**衍生**:42 組「同一天同一檔股票在兩個市場都有列」裡,**29 組落在上表 A 的 7 個錯日上**
(錯日的內容來自更晚的年份,那時該股已轉上市,而同日的上櫃檔還在原地),
另外 13 組是真的(見下方 REAL 3)。

---

## BUG 2:32 個「市場×交易日」整天沒抓到(估計缺 32,690 列)

### A. 26 天被 0-byte 空檔永久蓋住(twse 25 天 + tpex 1 天,估計 26,247 列)

twse:2016-03-23、2016-10-05、2016-11-21、2016-12-16、2016-12-19、2017-05-17、2017-05-31、
2017-08-25、2017-09-25、2017-09-26、2017-09-29、2017-11-23、2017-11-28、2017-11-29、
2018-01-16、2018-06-27、2018-07-27、2020-09-30、2020-12-03、2021-11-01、2022-04-25、
2023-07-06、2023-07-20、2023-10-06、2025-11-21;tpex:2016-05-17。

* 這 26 天在其他 5 張日頻表(`daily_quote`/`daily_trading_details`/`stock_per_pbr`/
  `margin_transactions`/`foreign_holding_ratio`)**全部有資料**——證人數 5/5。
* 本地檔案是 0-byte,mtime 全在 2026-04-25~26(同一次歷史回補)。
* `Detail.getDatesOfExistFiles`(`src/main/scala/setting/Detail.scala:132-169`)只要檔案存在
  就算「已抓過」,`Task.pullSbl`(`src/main/scala/Task.scala:463`)的
  `filterNot(coveredBoth(...))` 於是**永遠跳過**這 26 天。

**第一手證實資料存在**(現在去抓,不是推測):

| 探測 | 回應 |
|---|---|
| `TWT93U?date=20231006` | 129,081 bytes,標題「112年10月06日 信用額度總量管制餘額表」 |
| `TWT93U?date=20251121` | 144,725 bytes,標題「114年11月21日」 |
| `TWT93U?date=20160323` | 103,592 bytes,標題「105年03月23日」 |
| `tpex .../margin/sbl?date=105/05/17` | 62,162 bytes,`totalCount: 625` |

### B. 6 個是 `daily_quote` 假休市檔造成的連鎖(估計 6,443 列)

twse 與 tpex 的 **2025-08-15、2026-04-29、2026-05-28**,借券原始檔**根本不存在**(連請求都沒送出)。

* `Task.pullSbl`(`Task.scala:464`)有 `.filter(d => tradingDays.contains(d))`;
  `tradingDays` 來自 `loadTwseTradingDays()`(`Task.scala:415-439`)=
  PG `daily_quote` 的 twse 日期 ∪ 本地 twse 報價檔(>1024 bytes 且內容日期與檔名相符)。
* 這三天 `data/daily_quote/twse/<year>/*.csv` 是 0-byte sentinel、PG 也沒有列
  → 交易日曆判定「沒開市」→ 借券永遠不請求。
* 但 `margin_transactions` + `daily_trading_details` + `stock_per_pbr` 三張表都有資料,
  而且交易所現在還給得出借券資料(`TWT93U?date=20250815` → 141,953 bytes「114年08月15日」;
  `date=20260429` → 147,039 bytes「115年04月29日」)。

**這是同一顆 bug 的第二個受害者**:`C-foreign_holding_ratio` 已記錄同樣三天(加 2021-08-18)
因同一原因缺漏。**只要 `daily_quote` 的假休市檔不修,借券/外資持股/內部人三張表就補不回來。**

---

## SUSPECT 1:cache 落後齊備日兩個交易日,而且正在發生表間錯位

`sbl_borrowing` 兩市場都只到 **2026-07-17**,但同一份 cache 的 `daily_quote` /
`daily_trading_details` / `stock_per_pbr` 已經有 **2026-07-20**,而齊備日是 **2026-07-21**。

```
latest_complete_trading_day() = 2026-07-21
stale_tables() = {daily_quote: 07-20, market_index: 07-17, daily_trading_details: 07-20,
                  stock_per_pbr: 07-20, margin_transactions: 07-17,
                  sbl_borrowing: 07-17, foreign_holding_ratio: 07-17}
```

最後一次 Scala 爬蟲是 2026-07-19 21:59(`data/sbl_borrowing/*/2026/2026_7_17.csv` 的 mtime),
cache.duckdb mtime 2026-07-21 08:16。這正是 CLAUDE.md 記載 2026-07-15 事故的形態
(表間日期錯位 → 策略閘門查無資料 fail-closed 靜靜砍光候選)。與 `C-margin_transactions`
同源,不是本表獨有。

---

## SUSPECT 2:cache 沒有唯一索引

cache 只有非唯一的 `idx_sbl_code_date ON sbl_borrowing(company_code, date)`
(`research/cache_tables.py:121`),PG 側有 `idx_SblBorrowing_market_date_code` UNIQUE
`(market, date, company_code)`。目前實測重複 0 筆,唯一性完全靠「每次砍掉重建」這條自律撐著。

---

## REAL 1:38 列「當日餘額 ≠ 前日餘額 + 賣出 − 還券 + 調整」是交易所自己就這樣印

全表 4,691,508 列裡 38 列(0.0008%)加減對不起來,而且形態完全一致:
前日餘額 > 0、賣出/還券/調整都是 0、當日餘額卻變成 0。

逐格對原始檔:`data/sbl_borrowing/twse/2016/2016_4_6.csv` 的 3584 介面那一列是
`"3584","介面","296,000","0","114,000","0","182,000","35,167,042","1,000","0","0","0","0","218,454","X "`
—— 借券區塊前日餘額 1,000、賣出/還券/調整全 0、當日餘額 0。原始檔就是這樣,不是解析錯。
備註欄寫 `X`(停止融券)。

**含意**:這 38 個 (股票,日) 的「餘額變動」在時序上是斷的,做借券餘額變化率的因子
要當 NULL 而不是「一天內全部還清」。

---

## REAL 2:七成的列「當日餘額 > 次一營業日限額」是正常的

4,691,508 列裡 3,294,215 列(70.22%)`daily_balance > next_day_limit`。這不是異常——
最後一欄是**「次一營業日可借券賣出限額」**,是隔天還能再借多少的**增量額度**,
不是餘額上限。原始檔 0050 那一列自己就長這樣:當日餘額 36,467,000、次一營業日可限額 3,249,198。

另有 355 列「限額 0 但仍有餘額」——同理,額度用完/暫停,先前借的還在。

---

## REAL 3:13 組跨市場重複是「上櫃轉上市前一交易日兩邊都公告」

42 組「同一天同一檔在兩個市場都有列」裡,29 組是 BUG 1 的衍生,剩下 13 組是真的:

| 日期 | 代號 | 兩市場的值 | 該股當天的報價在哪 |
|---|---|---|---|
| 2020-12-22 | 1597 | 完全相同 | tpex(twse 首日報價 2020-12-23) |
| 2021-01-18 | 6438 | 完全相同 | tpex |
| 2021-03-23 | 6426 | 完全相同 | tpex |
| 2021-05-12 | 3092 | 完全相同 | tpex |
| 2022-01-18 | 1752 | 完全相同 | tpex |
| 2022-03-07 | 5306 | 完全相同 | tpex |
| 2022-09-20 | 3652 | 完全相同 | tpex |
| 2023-10-30 | 8476 | 完全相同 | tpex |
| 2023-12-18 | 6472 | 完全相同 | tpex |
| 2023-12-21 | 4736 | 完全相同 | tpex |
| 2024-01-24 | 6446 | 完全相同 | tpex |
| 2025-07-18 | 6589 | 完全相同 | tpex(twse 首日報價 2025-07-21) |
| 2026-07-15 | 5236 | 完全相同 | tpex(twse 首日報價 2026-07-16) |

轉上市當天上市所會先把該股列進借券報表,數字與上櫃所同步。**資料是對的,但所有
`SELECT ... FROM sbl_borrowing` 不加 market 條件的查詢會把這 13 個股票日算成兩筆。**
現有 9 支消費端腳本**一支都沒有加 market 條件**。

---

## REAL 4:6.6% 的列六個數字全 0

309,495 列(6.60%)prev/sold/ret/adj/bal/limit 全是 0 —— 在借券標的名單上、但當天沒有任何
借券活動也沒有額度。不是缺值。

---

## OK:其餘異常值掃描全清

| 項目 | 結果 |
|---|---|
| NULL(任一欄) | 0 |
| market 非 twse/tpex | 0 |
| company_code 非「數字開頭 + 英數」 | 0 |
| 日期在未來 | 0 |
| 負值(prev/sold/ret/bal/limit) | 0 |
| 負值(daily_adjustment) | 2,883 列 —— **調整數本來就可正可負**,合法 |
| 單日列數 < 前後 21 日滾動中位數 70% | 0 天(無週六補行交易日腰斬現象) |
| 借券檔數 vs 同日 `daily_quote` 檔數 | tpex 恆少 73~129 檔、twse 平均少 58.2 檔(非全部股票都是借券標的);唯二「借券比報價多」的兩天就是 BUG 1 的 2017-06-30 與 2018-12-12 |

---

## 誰會被影響(消費端盤點)

9 支腳本讀 `sbl_borrowing`,**全部只用 `daily_balance`、全部不加 market 條件**:

| 檔案 | 用法 | 受影響形態 |
|---|---|---|
| `src/quantlib/serenity/engine.py:987-997`(**現役策略**) | `shift(20)` 位置位移算 `sbl_chg_20d` | 幽靈列佔掉窗格 → 窗口位移。最後一個汙染日 2022-06-19,對 2330 而言決策日 ≤ **2022-07-15** 的窗仍跨過幽靈列;2022-07-16 之後乾淨 |
| `src/quantlib/evergreen/ev48_chip_axes.py:37-44` | `shift(5)` | 同上(位置位移) |
| `src/quantlib/evergreen/ev18_make_packs.py:87-95` | `row_number() OVER (ORDER BY date DESC) LIMIT 20` | 幽靈日進入 DISTINCT date 清單 → 20 日窗變 18~20 個真實交易日 |
| `src/quantlib/evergreen/make_tables_chips.py:56-63` | 20 日窗 first/last | 同上 |
| `src/quantlib/apex/data.py:195-198` + `experiments/f10_chip_axes.py:25-31` | `shift(5)` | 同上 |
| `src/quantlib/strat_lab/iter_52_ownership_flow_alpha.py:100-111` | 日期區間 + 位移 | 同上 |
| `src/quantlib/futures/strategies.py:174-182` | `SUM(...) GROUP BY date` 全市場加總 | 10 個真交易日的加總是別天的值;幽靈日多出 16 個日期 |
| `src/quantlib/experiments/sprint_a_signal_prototype.py:84-117` | `LAG` over date | 同上 |

**現役 Serenity 今天的決策不受影響**(汙染全在 2022-07-15 之前),但 2016~2022 的回測、
Evergreen 的月中標記、apex 的籌碼軸全部吃到。

---

## 建議修法(不要只補資料,四步走完)

1. **刪錯日資料 → 刪檔 → 重抓**(26 天,順序不能反,否則 `getDatesOfExistFiles` 仍會跳過):
   ```
   DELETE FROM sbl_borrowing WHERE market='twse' AND date IN (
     '2016-04-08','2016-08-07','2016-10-29','2016-12-20','2017-01-02','2017-05-15',
     '2017-06-30','2017-07-04','2017-08-06','2017-09-23','2017-12-14','2018-01-06',
     '2018-01-12','2018-02-19','2018-04-28','2018-09-06','2018-10-07','2018-10-08',
     '2018-10-13','2018-12-12','2021-02-12','2021-05-16','2021-11-13','2022-02-01',
     '2022-06-05','2022-06-19');
   ```
   對應的 `data/sbl_borrowing/twse/<year>/<Y>_<M>_<D>.csv` 一併刪除。
   其中 16 個非交易日刪完不要重抓(交易所會回空表 → 正確地寫成 0-byte sentinel);
   10 個真交易日用 `https://www.twse.com.tw/exchangeReport/TWT93U?response=csv&date=YYYYMMDD` 重抓。

2. **補 26 天的 0-byte 空檔**:先 `rm` 掉空檔再跑
   `sbt "runMain Main pull sbl --since 2016-03-23"`(端點同上;tpex 2016-05-17 用
   `https://www.tpex.org.tw/www/zh-tw/margin/sbl?date=105/05/17`)。

3. **6 天連鎖缺漏要先修 `daily_quote`**:2025-08-15 / 2026-04-29 / 2026-05-28 的
   `data/daily_quote/twse/<year>/*.csv` 假休市檔清掉並重抓,`loadTwseTradingDays()`
   認回這三天之後,借券才會被請求。

4. **立三道守護(先紅後綠)**:
   * `TradingReader.readSblBorrowing` 讀完檔案後**核對內容日期與檔名日期**,不符即拋錯
     (與 `daily_quote` 2026-05-30 事故的修法一致,見 `docs/data/data_quality_incidents.md`)。
   * 把 `src/quantlib/audits/04_cross_verify.py:49` 從寫死 `data/index` 改成掃**所有**
     `data/<source>/<market>/<year>/` 的日頻目錄(TWSE CSV 抓民國標題、TPEx JSON 抓 `date` 欄)。
     這一支現在跑起來應該紅 26 筆,修完應歸零。
   * cache 建好後加兩條斷言:(a)「整日 `sum(hash(code||daily_balance))` 撞號對數 = 0」
     (現在跑會紅 36 對);(b)「`sbl_borrowing` 的 (market,date) 集合 ⊇ `margin_transactions`
     同市場同區間的 (market,date) 集合」(現在跑會紅 32 個)。

5. **cache 端補唯一索引** `CREATE UNIQUE INDEX ON sbl_borrowing(market, date, company_code)`
   與 PG 對齊,並在文件註明消費端**必須加 market 條件或先 dedupe**(13 個轉市日會雙算)。

6. **修好前的暫行規則**:任何用 `daily_balance` 做時序變化率的回測,對上表 26 天
   一律當 NULL;位置位移(`shift(N)`)一律改成**依交易日曆對齊的日期位移**,不要相信列的順序。

---

## 查了什麼(供覆核涵蓋度)

* schema 對照:PG `information_schema.columns`(11 欄)vs DuckDB `DESCRIBE`(9 欄);
  `research/cache_tables.py:53` 與 `src/quantlib/db.py:145-148` 兩條投影字面比對
* cache vs PG 全史指紋(非抽樣):5,116 個 (market,date) × count + sum(hash) + bit_xor
* 逐年逐市場列數 22 格;抽樣 3 日期 × 5 檔逐欄 `DataFrame.equals`
* 原始檔全量掃描 6,266 檔:檔名日期 vs 內容自報日期(TWSE 民國標題 / TPEx JSON `date`)
* cache 端獨立複驗:整日內容指紋撞號(全欄版 + 只看餘額版,兩版皆 36 對)
* 缺口三法:證人投票(5 張日頻表)、純日曆(`quantlib.data_calendar.is_trading_day`
  讀 0-byte sentinel,颱風假才判得出來)、原始檔形態(0-byte / 不存在 / 有內容)
* 第一手覆核:`TWT93U` 探測 6 次(20231006 / 20251121 / 20160323 / 20250815 / 20260429 /
  20170630 / 20160807)+ TPEx `margin/sbl` 探測 1 次(105/05/17)
* `prev_day_balance[t]` vs `lag(daily_balance)` 連續性:全史斷裂率 twse 1.971% / tpex 0.078%;
  剔除 26 個錯日與其後一日後 twse 降到 0.478%(錯日解釋掉約 76% 的斷裂)
* 異常值:NULL、負值(六欄分開算)、未來日期、market 值域、company_code 字元集、
  重複主鍵、恆等式、限額關係、全零列、單日列數滾動中位數 70% 門檻、值域極端前 5
* 週六補行交易日辨識(8 天,已排除誤判);跨市場重複 42 組逐組查轉市時點
* 消費端盤點:9 支腳本 × 用到哪些欄 × 是否加 market 條件 × 位置位移 vs 日期位移
* 既有守護盤點:`src/quantlib/audits/03_full_data_audit.py`(無 sbl 檢查)、
  `src/quantlib/audits/04_cross_verify.py:49`(寫死 `data/index`)、`src/quantlib/tests/`(無 sbl 測試)
* 排除已知真實現象:金融業負營收 / 營建業零營收 / `concise_*` 無 market 欄皆與本表無關;
  週六補行交易日不造成列數腰斬(已量測)
