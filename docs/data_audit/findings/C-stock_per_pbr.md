# C-stock_per_pbr — cache 與 PostgreSQL 的一致性與缺漏

**判定:🔴 BUG**(2026-07-22 稽核)

## 一句話結論

**cache 和 PostgreSQL 之間沒有半點不一致——774 萬列、三個數值欄逐位相同,零差異。
不能信的是「哪幾天的數字屬於哪一天」:上市(twse)有 19 個日期存的根本是別天的資料,
其中 10 天是真的有開市的交易日;另外有 13 個真交易日整天一列都沒有。**
問題全部在 cache 上游(爬蟲 + 解析),cache 只是忠實地把 PG 的錯誤複製過來。

受影響列數 16,447 列(佔全表 0.21%),落在真交易日的 8,605 列。其餘 774 萬列可信。

## 表的規模(cache,`var/cache/cache.duckdb`)

| market | 列數 | 日期範圍 | 交易日數 | 代號數 |
|---|---:|---|---:|---:|
| twse | 4,398,439 | 2005-09-02 ~ 2026-07-20 | 5,132 | 1,208 |
| tpex | 3,342,466 | 2007-01-02 ~ 2026-07-20 | 4,798 | 1,112 |
| 合計 | **7,740,905** | | | |

PostgreSQL `stock_per_pbr_dividend_yield` = 7,738,937 列。差的 1,968 列全部是
2026-07-20 一天(見下方第 3 條)。

---

## 🔴 BUG 1:19 個上市日期存的是別天的資料(16,447 列)

TWSE 的 `BWIBBU_d` 端點在請求「它沒有資料的日期」時**不會回空**,而是回一份別天的
快照(最常見的是 2017-12-18),標題裡老實寫著真正的日期。爬蟲以「請求日」為檔名存檔,
`TradingReader.readStockPER_PBR_DividendYield` 又只從**檔名**取日期
(`src/main/scala/reader/TradingReader.scala:719` `val date = LocalDate.of(year, month, day)`,
全函式沒有讀過標題),於是這份快照就被蓋上了錯的日期戳進資料庫。

全史 7,625 個 twse 原始檔掃描結果:**19 檔的檔名日期 ≠ 內容日期**(tpex 7,137 檔全對)。

| 檔名日 | 星期 | 內容其實是 | 偏移 | 列數 | 該日有開市? |
|---|---|---|---:|---:|---|
| 2008-03-03 | 一 | 2017-12-18 | **+9.8 年(前視)** | 905 | ✅ 有(報價 732 檔) |
| 2008-03-06 | 四 | 2008-12-18 | **+287 天(前視)** | 710 | ✅ 有(732) |
| 2008-12-04 | 四 | 2008-12-18 | **+14 天(前視)** | 710 | ✅ 有(734) |
| 2012-08-07 | 二 | 2017-12-18 | **+5.4 年(前視)** | 905 | ✅ 有(863) |
| 2013-05-13 | 一 | 2017-12-18 | **+4.6 年(前視)** | 905 | ✅ 有(874) |
| 2014-12-16 | 二 | 2014-12-18 | **+2 天(前視)** | 850 | ✅ 有(909) |
| 2016-10-06 | 四 | 2017-12-18 | **+1.2 年(前視)** | 905 | ✅ 有(959) |
| 2018-12-13 | 四 | 2017-12-18 | −360 天(陳舊) | 905 | ✅ 有(1,072) |
| 2025-08-12 | 二 | 2017-12-18 | −7.7 年(陳舊) | 905 | ✅ 有(1,302) |
| 2026-02-25 | 三 | 2017-12-18 | −8.2 年(陳舊) | 905 | ✅ 有(1,344) |
| 2009-08-02 | 日 | 2017-12-18 | — | 905 | ❌ 休市 |
| 2011-07-10 | 日 | 2017-12-18 | — | 905 | ❌ 休市 |
| 2012-03-31 | 六 | 2017-12-18 | — | 905 | ❌ 休市 |
| 2012-10-21 | 日 | 2012-10-18 | — | 798 | ❌ 休市 |
| 2012-11-11 | 日 | 2012-12-18 | — | 807 | ❌ 休市 |
| 2012-11-25 | 日 | 2012-12-18 | — | 807 | ❌ 休市 |
| 2014-10-12 | 日 | 2017-12-12 | — | 905 | ❌ 休市 |
| 2017-04-03 | 一 | 2017-12-18 | — | 905 | ❌ 休市(清明連假) |
| 2026-04-12 | 日 | 2017-12-18 | — | 905 | ❌ 休市 |

**這不是推測,是逐欄對過的**:每一個錯位日在 cache 裡的列數與「內容日」完全相同,
且同代號的三個數值欄**一列不差**(`05_phantom_impact.py` 的「逐欄同」欄 = 列數)。

最直觀的一眼證據——台積電 2330 的股價淨值比:

```
2026-02-23  PB 9.86   2025-08-11  PB 6.70
2026-02-24  PB 10.20  2025-08-12  PB 4.11  ← 2017-12-18 的值
2026-02-25  PB 4.11   ← 2017-12-18 的值    2025-08-13  PB 6.82
2026-02-26  PB 9.55
```

**為什麼要緊**:Serenity 引擎(`src/quantlib/serenity/engine.py:964`)與各回測都用
「取 ≤ 決策日的最新一筆」的 as-of 取值(`row_latest_before`),所以在這 10 個交易日
當天,約 700~905 檔股票的本益比/淨值比/殖利率會是錯的。其中 7 天是**未來的數字提前出現
(前視偏誤)**,3 天是**八年前的數字冒充今天**。休市那 9 天的幽靈列因為回測只走交易日
所以不會被取到,但它們本來就不該存在。

**永遠不會自己好**:爬蟲跳過已存在的檔案、Reader 跳過已在 DB 的 (market, 檔名) 組合,
所以重跑 `Main update` 不會覆蓋它們。

## 🔴 BUG 2:13 個真交易日整天沒有資料

| market | 缺漏日(星期) |
|---|---|
| twse(11 天) | 2008-08-26(二)、2009-12-12(**六,補行交易日**)、2014-05-07(三)、2016-09-09(五)、2016-11-01(二)、2016-12-21(三)、2017-02-13(一)、2017-02-24(五)、2020-02-06(四)、2025-08-05(二)、2026-04-01(三) |
| tpex(2 天) | 2010-11-22(一)、2016-08-31(三) |

判定依據不是猜的:這些日子 `daily_quote` 都有 562~1,348 檔的報價(所以確實有開市),
`src/quantlib/data_calendar.py::is_trading_day`(讀 0-byte 休市 sentinel、能認出颱風假)
也判為交易日。

原始檔狀態:twse 那 11 天的檔案存在但**只有 2 bytes**(交易所回了空內容),
tpex 那 2 天**檔案根本不存在**。因為爬蟲「檔案已存在就跳過」,那 11 個 2-byte 空檔
把這些日子永久凍結。

## 🟡 SUSPECT 3:cache 比 PostgreSQL 多一天,重建 cache 會把它靜靜抹掉

cache 有 2026-07-20(1,968 列),PG 沒有(PG 最新 2026-07-17)。原因是這張表現在有
**兩條寫入路徑**:

- Scala 爬蟲 → PostgreSQL → `research/cache_tables.py`(**先 `os.remove` 整個
  cache.duckdb 再從 PG 全砍重建**,見 `research/cache_tables.py:22-23`)
- Python 爬蟲 `src/quantlib/crawl/sources/stock_per_pbr.py` → **直寫 cache.duckdb**
  (`src/quantlib/crawl/sink.py`),完全不經過 PG

現在誰跑後面那條,cache 就領先 PG;誰跑 `cache_tables.py`,cache 就退回 PG 的版本。
今天的狀態是 cache 領先——**現在照 CLAUDE.md 的 Step 2 重建 cache,2026-07-20 這天
會無聲消失**(daily_quote、daily_trading_details 同病)。與 C-operating_revenue 查到的
是同一個結構病。

## 🟡 SUSPECT 4:兩條管線都沒有「內容日期必須等於請求日期」的守門員

BUG 1 的根因守護在**新舊兩條管線都缺席**:

- Scala:`TradingReader.readStockPER_PBR_DividendYield` 只用檔名日期,不看標題。
- Python:`src/quantlib/crawl/sources/stock_per_pbr.py::_parse` 有欄位位移守護
  (`_guard` → `SchemaDrift`),但**沒有任何日期檢查**——同樣的錯位在雲端 VM 上會原封重演。

另外,Python 管線有一個會製造 BUG 2 的機制:`src/quantlib/crawl/update.py::_refresh_daily`
在 `fetch_day` 回 None 時只 `continue`,而下一次要抓哪幾天是用
`_missing_days`(從 `max(date)+1` 起算)決定的——**某天抓失敗、隔天成功之後,失敗那天
就永遠不會再被回頭補**。

## 🟡 SUSPECT 5:cache 落後齊備日一個交易日

`latest_complete_trading_day()` = 2026-07-21,但 cache 的 `stock_per_pbr` 最新只到
2026-07-20(`stale_tables()` 顯示七張日頻表全部落後)。這是「今天的每日 loop 還沒跑」的
營運狀態,不是這張表的資料錯誤,但在此記一筆以免誤判。

---

## 🟢 查過沒問題的部分

### cache 與 PG 逐欄完全一致(不是抽樣,是全表)

7,738,937 個共用鍵 `(market, date, company_code)`,三個數值欄
`price_book_ratio` / `dividend_yield` / `price_to_earning_ratio` 的不一致筆數
**全部是 0**;PG 獨有鍵 0 筆,cache 獨有鍵 1,968 筆(= 2026-07-20)。
逐年、逐日筆數也只有 2026-07-20 那一天有差。

指定的隨機抽樣(3 個日期 × 5 檔 = 15 組,seed 20260722)逐欄比對:**15/15 相同**。

### schema 無型別降級

| PG(8 欄) | cache(6 欄) |
|---|---|
| id `bigint` | (不同步) |
| market `varchar` | market `VARCHAR` |
| date `date` | date `DATE` |
| company_code `varchar` | company_code `VARCHAR` |
| company_name `varchar` | **(不同步)** |
| price_to_earning_ratio `double precision` | price_to_earning_ratio `DOUBLE` |
| price_book_ratio `double precision` | price_book_ratio `DOUBLE` |
| dividend_yield `double precision` | dividend_yield `DOUBLE` |

`research/cache_tables.py:38` 明確只取 6 欄,型別一一對應、無降級。
pg-attach 模式的對照 view(`src/quantlib/db.py:103-106`)欄位與順序和 cache 表完全一致,
parity 沒破。少掉的 `company_name` 在 research 端由 `industry_taxonomy_pit` 提供,
目前沒有消費端因此壞掉。

### 異常值掃描全清

774 萬列中:負的股價淨值比 0、負的本益比 0、負的殖利率 0、未來日期 0、重複鍵 0、
代號格式異常 0、三欄全 NULL 只有 713 列。
`price_to_earning_ratio` 為 NULL 的 184 萬列(23.8%)是虧損公司交易所印 `-`,正常。

### TPEx 側全清

7,137 個原始檔內容日期零錯位;逐日列數相對前一交易日的變動**沒有任何一天超過 10%**
(twse 的 17 筆全部是 BUG 1 的錯位日);兩市場都沒有「與前一交易日內容完全相同」的重複日。

### 2026-07-20(cache 獨有那天)的資料是真的

列數與 07-17 相同(twse 1,079 / tpex 889),股價淨值比中位數 1.66→1.65、
1,968 檔的 PB 變動比 5%/50%/95% 分位 = 0.933 / 0.9932 / 1.0271——會動、不是複製前一日。

## ⚪ 看起來像錯、其實是交易所原始檔就這樣

- **8444 綠河-KY 股價淨值比 115**:原始檔 `data/stock_per_pbr_dividend_yield/tpex/2026/2026_7_17.csv`
  就寫 `"8444","綠河-KY","N/A","0.00000000","114","0.00","115.00","115Q1"`。
- **6720 久昌 2024-12-04 淨值比 = 0、本益比 = 0**:原始檔就是 `"0"`(全表唯一一筆)。
- **本益比 > 1000 共 22,645 列、殖利率 > 30% 共 8,517 列**:接近損益兩平的公司與
  發特別股利者的真實數字,非解析錯位。

## 🔗 與其他稽核單位互相佐證

`stock_per_pbr` 有資料、`daily_quote` 卻整天空白的 4 個 twse 交易日
(2021-08-18、2025-08-15、2026-04-29、2026-05-28)——這幾天的 per_pbr 原始檔內容日期
正確、列數正常,所以缺的是 `daily_quote`,與 `C-daily_quote` 的 BUG 1 完全吻合。

---

## 建議修法

### (a) 清掉 19 個錯位日、重抓

```sql
-- 先看清單,再刪
DELETE FROM stock_per_pbr_dividend_yield WHERE market='twse' AND date IN
 ('2008-03-03','2008-03-06','2008-12-04','2012-08-07','2013-05-13','2014-12-16',
  '2016-10-06','2018-12-13','2025-08-12','2026-02-25',   -- 10 個真交易日:刪後重抓
  '2009-08-02','2011-07-10','2012-03-31','2012-10-21','2012-11-11','2012-11-25',
  '2014-10-12','2017-04-03','2026-04-12');                -- 9 個休市日:刪掉即可,不必重抓
```
同時刪掉對應的 19 個原始檔(否則爬蟲會跳過),再對 10 個真交易日重抓:
`https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?response=csv&selectType=ALL&date=YYYYMMDD`
**存檔前先核對標題日期**;休市那 9 天要留 0-byte sentinel 而不是留錯的內容。

### (b) 補 13 個缺漏交易日

先刪掉 11 個 2-byte 空檔(`data/stock_per_pbr_dividend_yield/twse/<year>/<y>_<m>_<d>.csv`),
再抓:
- twse:同上 BWIBBU_d,日期參數 `YYYYMMDD`
- tpex(2010-11-22、2016-08-31):
  `https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=csv&d=民國年/MM/DD`

### (c) 立守護(防復發,這才是重點)

1. **內容日期閘門**:Scala Reader 與 Python `_parse` 都要解析標題日期
   (twse `"106年12月18日 個股日本益比…"`、tpex `資料日期:113/12/04`),與請求日不符
   就 **fail-loud**,絕不入庫。這條守護要先用 `2026_4_12.csv` 驗紅再驗綠。
2. **抓失敗要留疤**:`src/quantlib/crawl/update.py::_refresh_daily` 對 `fetch_day` 回 None
   的交易日必須記錄待補(或寫 sentinel),不能靠 `max(date)+1` 往前推而永久跳過。
3. **重建不得倒退**:`cache_tables.py` 全砍重建前,先比對「PG 的 max(date) ≥ cache 的
   max(date)」,否則拒跑並提示先跑 Scala 端更新;或改成 merge 不 drop。
4. **例行體檢**:把「檔名日期 vs 內容日期」「daily_quote 有而本表無的交易日」
   兩項掃描接進每日 loop 的健康檢查——這次的洞躺了十幾年沒人發現,就是因為沒有人在掃。

## 重現用腳本(全部落在 repo)

```
docs/data_audit/scripts/C-stock_per_pbr/
  01_counts.py                  cache vs PG 整表/逐年/逐日筆數
  02_value_parity.py            全表逐鍵逐欄值比對(非抽樣)
  03_coverage_gaps.py           日期覆蓋缺口(對 daily_quote + sentinel 日曆)
  04_filename_vs_content_date.py 全史原始檔檔名日期 vs 內容日期
  05_phantom_impact.py          錯位日的汙染量化(逐欄與內容日比對)
  06_value_anomaly.py           數值欄異常掃描 + 缺漏日清單
  07_sample_and_newest.py       隨機抽樣 3 日 x 5 檔 + cache 獨有日合理性
  08_daycount_jumps.py          逐日列數跳動 / 連日重複內容偵測
```
執行:`uv run --project . python docs/data_audit/scripts/C-stock_per_pbr/<script>.py`
(需 `var/cache/cache.duckdb` 與本機 PostgreSQL)
