# C-daily_quote — cache 與 PostgreSQL 的一致性與缺漏

**結論:🔴 BUG。價格本身可以信,「哪幾天有開市」不能信。**

白話講三件事:

1. **cache 跟 PostgreSQL 的報價完全一樣**——全史 912 萬列、11 個欄位,逐列逐欄比對
   零差異。用 DuckDB 查跟用 PG 查會拿到一模一樣的答案。這條可以放心。
2. **但有 5 個真的有開市的日子,報價整天不見了**;更糟的是其中 4 天在原始檔留下
   「今天休市」的空檔案,於是系統的交易日曆把它們當假日,**永遠不會自己補回來**。
3. **還有 1 天是憑空多出來的**:2009-12-12(星期六)那 772 檔報價,是 2009-12-18
   的資料被貼上錯的日期,**逐欄一模一樣**。任何走每日淨值的回測在那一週會看到
   一次假的 +20% 再 -20%。

外加一個順手挖到、不屬於本表但會直接汙染回測的問題:**69 筆「除權(配股)」事件
在資料庫裡整列是 0**,cache 與 `research/prices.py` 都用 `cash_dividend > 0` 過濾,
所以這些事件不會被還原成調整後價格——58 筆量得到的事件裡,中位數憑空跌 **3.94%**,
最深 **-23.76%**(國巨 2327、富邦金 2881、元大金 2885、開發金 2883、信驊 5274 都在裡面)。

---

## 一、cache vs PostgreSQL:逐欄全等(🟢 OK)

不是抽樣,是**全體**。雙邊各算每個 `(market, date)` 的列數 + 兩種雜湊
(`sum(hash(全部 11 欄))` 與 `bit_xor(hash(...))`),三個數字全部相等:

| 項目 | PostgreSQL | DuckDB cache |
|---|---|---|
| 總列數 | 9,124,080 | 9,126,464 |
| 共同 `(market,date)` 數 | 10,198 | 10,198 |
| 共同日期上的列數 | 9,124,080 | 9,124,080 |
| 共同日期上 count/雜湊不符的天數 | **0** | |
| 只有一邊有的日期 | 無 | `2026-07-20`(twse 1,371 + tpex 1,013 = 2,384 列) |

差的 2,384 列就是 2026-07-20 那一天,**cache 領先 PG**(原因見第四節,不是錯)。

重跑(約 21 秒,PG 掃描 18 秒):

```python
# 見本報告附錄「重跑腳本」;核心是
# ATTACH pg + ATTACH cache,雙邊 GROUP BY (market,date) 算 count + sum(hash(...)::HUGEINT) + bit_xor(hash(...))
```

抽樣佐證(3 個日期 × 5 檔,`pandas.DataFrame.equals` = **True**):
2012-06-15 / 2019-11-07 / 2026-07-17 × 2330、2317、1101、3008、6488(共 14 列,
tpex 那兩天 6488 有資料)。

**schema**:cache 只投影 PG 18 欄中的 11 欄,**型別零降級**
(`double precision`→`DOUBLE`、`bigint`→`BIGINT`、`date`→`DATE`、`varchar`→`VARCHAR`)。
被丟掉的是 `id`、`company_name`、`transaction`(成交筆數)、`change`(漲跌價差)、
`last_best_bid_volume`、`last_best_ask_volume`、`price_earning_ratio`
(`research/cache_tables.py:32-37`,註解已寫明是刻意取捨)。研究端要用「成交筆數」
或報價量做微結構因子時會發現拿不到——這是覆蓋範圍的事實,不是錯誤。

**唯一結構性落差**:PG 有 `idx_DailyQuote_market_date_companyCode` UNIQUE,cache
沒有任何唯一鍵(`CREATE TABLE AS` 不帶約束)。實測目前重複主鍵 **0 筆**,但保護
是靠寫入端自律(`research/crawl/sink.py:35-59` 的「刪整日 + 插」),不是靠資料庫。

---

## 二、🔴 BUG 1:5 個真交易日的報價整天不見,4 天還被標成假日

| 市場 | 缺的日期 | 星期 | 佐證(同一天其他表的列數) |
|---|---|---|---|
| twse | 2021-08-18 | 三 | 三大法人 1,086 / 融資券 1,075 / 本益比 951 / 借券 1,080 / 指數 175 |
| twse | 2025-08-15 | 五 | 三大法人 1,228 / 融資券 1,219 / 本益比 1,046 / 指數 272 |
| twse | 2026-04-29 | 三 | 三大法人 1,314 / 融資券 1,264 / 本益比 1,071 / 指數 267 |
| twse | 2026-05-28 | 四 | 三大法人 908 / 融資券 1,271 / 本益比 1,077 |
| tpex | 2023-06-08 | 四 | 本益比 808 / 融資券 775 / 外資持股 815 / 借券 804 / 指數 63 |

**獨立鐵證**——加權指數在那三天有實際收盤與漲跌:

| 日期 | 發行量加權股價指數 | 漲跌點數 | 漲跌 % |
|---|---|---|---|
| 2021-08-18 | 16,826.27 | +164.91 | +0.99 |
| 2025-08-15 | 24,334.48 | +96.38 | +0.40 |
| 2026-04-29 | 39,303.50 | -218.23 | -0.55 |

(2026-05-28 連指數也一起沒了——指數與報價都出自 TWSE 同一支 `MI_INDEX` 端點,
一次抓失敗兩張表一起缺。)

**為什麼會自我延續**:這 4 天在原始檔留下 **0-byte sentinel**:

```
data/daily_quote/twse/2021/2021_8_18.csv   0 bytes  (Aug 24  2021)
data/daily_quote/twse/2025/2025_8_15.csv   0 bytes  (Aug 21  2025)
data/daily_quote/twse/2026/2026_4_29.csv   0 bytes  (Apr 30 15:40)
data/daily_quote/twse/2026/2026_5_28.csv   0 bytes  (Jun 11 13:03)
```

而 `research/data_calendar.py:44-57` 的 `is_trading_day()` **就是讀這些 sentinel
當休市日曆**。實測:

```python
is_trading_day(2021-08-18) → False    # 但加權指數 +0.99% 收在 16,826.27
is_trading_day(2025-08-15) → False
is_trading_day(2026-04-29) → False
is_trading_day(2026-05-28) → False
```

於是:(a) `latest_complete_trading_day()` 會跳過它們;(b)
`research/crawl/update.py:30-41` 的 `_missing_days()` 只補 `is_trading_day` 為真的
日子,所以**這 4 天永遠不會被自動補抓**;(c) 每一份回測都少了這 4 天,而且是靜默的。

sentinel 的 mtime 全都在 D+1 之後(合乎 `CLAUDE.md` 的「過了齊備時刻才寫 sentinel」
規則),所以**規則沒被違反,是規則不夠**:交易所回空可能是「真休市」也可能是
「這次抓失敗」,現在的程式把兩者當同一件事。TPEx 那天(2023-06-08)原始檔是 96 bytes
的「共 0 筆」回應,同樣被當成休市。

補充:反向驗證通過——TWSE 的 0-byte sentinel 共 2,676 個、TPEx 的「共 0 筆」空檔
共 2,269 天,除了上面兩天以外**全部**都對得上「那天沒有任何一張表有資料」。

---

## 三、🔴 BUG 2:2009-12-12 是幽靈交易日(772 列是 12-18 的複製品)

全表 10,198 個 `(market, date)` 做「排除日期欄後的整日內容指紋」比對,**只有一組
撞在一起**:

```
twse 2009-12-12  ≡  twse 2009-12-18     772 列,逐檔逐欄完全相同
```

原始檔就是這樣:

```
data/daily_quote/twse/2009/2009_12_12.csv 第 1 行:"098年12月12日 價格指數(臺灣證券交易所)"
data/daily_quote/twse/2009/2009_12_18.csv 第 1 行:"098年12月18日 價格指數(臺灣證券交易所)"
兩檔的發行量加權股價指數都是      "7,753.63","+","11.46","0.15"
兩檔的 9912 偉聯 那一列都是      1,029,259 股 / 9,383,333 元 / 8.99 9.18 8.80 9.18
```

也就是說 **TWSE 把「你要的日期」原樣印在標題上,但送回來的是另一天的內容**——
標題不能當作內容日期的證據。三條獨立線索一致證明 2009-12-12 沒有開市:

1. 其他 6 張日頻表在 2009-12-12 全都 0 列;TPEx 當天是「共 0 筆」。
2. 2009-12-14 的指數漲跌 +24.06 是以 7,795.07(12-11 的收盤)為基準,**跳過了 12-12**。
3. 2009-12-12 的 772 列與 12-18 逐位相同。

**後果**:偉聯 9912 的序列變成 12-11 收 7.35 → 12-12 收 9.18(+24.9%)→ 12-14 收
7.40(-19.4%);2395 研華 61.8 → 75.9 → 63.5。當年漲跌幅上限是 ±7%,這種波形在真實
市場不可能存在。任何逐日走淨值的回測在 2009 年 12 月都會吃到一次假的暴漲暴跌。

**順帶澄清(🟢 negative result,別再查一遍)**:其餘 23 個週末場次全部是**真的補行
交易日**。用「該日對前一交易日、對後一交易日的各股中位數 |log 報酬|」檢定,除
2009-12-12 外的 23 天,med|S/P| 落在 0.005~0.025、med|N/S| 落在 0.006~0.037,完全
符合正常交易日的波動;TPEx 的 19 個週末場次同樣通過。

---

## 四、🟡 SUSPECT:cache 現在領先 PG 一天,而那一天的原始檔沒有被封存

`2026-07-20` 只存在於 cache。原因不是同步壞掉,是**這張表現在有兩條寫入路徑**:

- 舊路徑:Scala `Main update` → 原始 CSV 落 `data/` → 匯入 PG → `research/cache_tables.py`
  砍掉整個 cache 重建。
- 新路徑:`research/crawl/update.py` + `research/crawl/sources/daily_quote.py`
  → 直接 upsert 進 `cache.duckdb`(**不經 PG、不寫原始 CSV**,只有休市時寫 sentinel)。

新路徑覆蓋 3 張表(daily_quote、daily_trading_details、stock_per_pbr),有 parity
守護(`research/crawl/tests/test_parity.py`,對照日 2026-07-17 逐位比對),設計上是
乾淨的。但衍生兩個後遺症:

1. **RAW 封存出現斷點**:`data/daily_quote/twse/2026/` 沒有 `2026_7_20.csv`。
   `research/paths.py` 明寫 RAW 是「不可重生的事實地基」,現在交易日的原始 CSV
   不再進封存,A 維(原始檔 → DB 逐欄核對)對這些日子**無從驗證**。
2. **重建會回退**:`research/cache_tables.py:22-23` 是 `os.remove(DB_PATH)` 後從 PG
   全砍重建。今天若照 `CLAUDE.md` 的 Step 2 跑一次,cache 的 2026-07-20 會被抹掉,
   要靠 Python 爬蟲下次再抓回來(端點還供應才行)。

## 五、🟡 SUSPECT:cache 內部表間日期錯位 3 天

```
daily_quote            2026-07-20      ← Python 爬蟲有跑
daily_trading_details  2026-07-20      ← Python 爬蟲有跑
stock_per_pbr          2026-07-20      ← Python 爬蟲有跑
market_index           2026-07-17      ← 只能靠 Scala + 全量重建
margin_transactions    2026-07-17
sbl_borrowing          2026-07-17
foreign_holding_ratio  2026-07-17
```

齊備日是 2026-07-21,**7 張表沒有一張到齊**(`stale_tables()` 全數回報)。
這正是 `CLAUDE.md` 記載的 2026-07-15 事故形態:表間日期錯位 → 策略閘門查無資料
fail-closed → 候選被靜靜砍光。目前 `research/tri/daily.py::ensure_fresh_cache` 會
擋,所以不是「已經出事」,但**新舊兩條路徑覆蓋的表不一樣**,只要 Scala 那條沒跑,
錯位就會重現。

---

## 六、🔴 BUG 3(跨單位):69 筆「除權」事件全零,調整後價格出現幽靈崩跌

這是從價格跳動掃描反推出來的。掃「連續交易日之間收盤變動 > ±15%」,扣掉除權息、
減資、停牌復牌之後,4 碼普通股全史只剩 **399 筆**(佔 912 萬列的 0.0044%);再扣掉
上市前 5 日(無漲跌幅限制)只剩 **47 筆**,2015 年後只剩 **8 筆**。逐筆追下去,
2015 年後那 5 檔普通股全部命中同一個根因:

```
2597 潤弘 2024-08-14   202.00 → 154.00   -23.76%
2364 倫飛 2025-08-28    99.30 →  77.50   -21.95%
2020 美亞 2024-07-30    36.50 →  29.70   -18.63%
2327 國巨 2024-08-15   745.00 → 622.00   -16.51%
2364 倫飛 2024-08-29   124.50 → 105.00   -15.66%
```

它們在 PG 的 `ex_right_dividend` 裡**都有**一列 `right_or_dividend='權'`,但
`closing_price_before_ex_right_ex_dividend`、`ex_right_ex_dividend_reference_price`、
`cash_dividend` **三個欄位全是 0**。全表這種列共 **69 筆**(2018 年後 **63 筆**)。

於是兩道過濾把它們一起丟掉:

- `research/cache_tables.py:42` — `... FROM pg.public.ex_right_dividend WHERE cash_dividend > 0`
- `research/prices.py:141-144` — `SELECT date AS ex_date, company_code, cash_dividend FROM ex_right_dividend ... AND cash_dividend > 0`

實測 `fetch_adjusted_panel()` 的輸出,這些日子的 `adj_factor` 就是 **1.0**,調整後
報酬原封不動吃下配股造成的參考價下修:

```
2327 2024-08-15  adj_factor=1.0  adj_ret=-16.51%
2597 2024-08-14  adj_factor=1.0  adj_ret=-23.76%
2364 2024-08-29  adj_factor=1.0  adj_ret=-15.66%
2020 2024-07-30  adj_factor=1.0  adj_ret=-18.63%
```

58 筆量得到的事件,中位數 **-3.94%**、最深 **-23.76%**,名單裡有 2881 富邦金
(-7.03%)、2885 元大金(-2.45%/-3.01%)、2883 開發金(-1.93%)、5274 信驊
(-11.40%)、6472 保瑞(-15.60%)、6446 藥華藥(-7.43%)。

**注意這只是「全零列」的部分**。`權息` 事件(5,120 筆)的 `cash_dividend` 實際上
存的是 `除權息前收盤 - 參考價` 的合計值(例:6712 前收 202、參考價 178.18、
cash_dividend 23.818182),所以 `prices.py` 的 `(pre - cash)/pre` 恰好等於交易所的
官方還原因子——**那部分是對的**。壞掉的就是這 69 筆解析成全零的。

---

## 七、🟢 異常值掃描:乾淨(負結果,別再查一遍)

912.6 萬列全掃:

| 檢查 | 筆數 |
|---|---|
| 重複主鍵 (market,date,company_code) | 0 |
| 負價格(任一 OHLC < 0) | 0 |
| high < low | 0 |
| close 不在 [low, high] | 0 |
| open 不在 [low, high] | 0 |
| trade_volume < 0 / trade_value < 0 | 0 / 0 |
| 量 = 0 但值 > 0 | 0 |
| 量 > 0 但 closing_price = 0 | 0 |
| 日期在未來 / 早於 2004-02-11 | 0 / 0 |
| 買價 > 賣價(交叉報價) | 0 |
| company_code 不符 `^[0-9A-Z]{4,6}$` | 0 |
| market 非 twse/tpex | 0 |

### ⚪ 看起來像錯、其實是真的

- **`closing_price = 0` 共 119,680 列**:100% 在 tpex、100% 落在
  **2007-07-02 ~ 2011-11-11**、100% 成交量為 0。2011-11-14 起 TPEx 改用 `---`
  (解析成 NULL),交接乾淨無重疊——這是舊 TPEx CSV 用 `0` 表示「當日無成交」的
  格式,不是壞值。**但下游要小心**:拿它算報酬會得到 -100% 再 +∞。
- **`closing_price IS NULL` 共 134,254 列(1.47%)**:四個 OHLC 一起為 NULL
  (TWSE 的 `--`)。其中 10,845 列成交量 > 0——樣本如 00681R 95 股/2,020 元、
  1213 大飲 2 股/14 元,都是只有零股或定價交易、無盤中成交的個股,成交金額 ÷
  成交股數 落在買賣價之間,內部一致。
- **`trade_value = 0` 但量 > 0 共 5 列**(2005~2009,量 1~4 股,OHLC 全 NULL):
  極零星的低價股微量成交,金額四捨五入到 0。
- **高價股不是小數點跑掉**:tpex 5274 信驊最高 19,275、twse 6515 穎崴最高 10,795。
  逐日序列連續(5274:9,520 → 9,705 → 9,250 → 9,430 → 10,055 → 10,495 …),
  沒有一日 10 倍的斷點。
- **83,555 筆「> ±15%」的跳動落在 5-6 碼代號**(權證、槓桿/反向 ETF、國外成分
  ETF)。權證沒有百分比漲跌限制、國外成分 ETF 不受漲跌幅限制
  (0061 元大寶滬深 2024-10-08 -15.6%、0080/0081 恒中國/恒香港 2015 年 4 月港股
  大時代 ±20%),槓桿 ETF 分割前後停牌(00631L 2026-03-31、00685L 2026-07-07)。
  全是真的。
- **2007 下半年 TPEx 有 33 筆普通股跳動查無對應除權息**——因為
  `ex_right_dividend` 的 tpex 資料**從 2008-01-10 才開始**,2007 年的除權息事件
  根本不在庫裡。是那張表的覆蓋問題,不是報價錯。

---

## 八、🟡 覆蓋邊界:TPEx 報價比其他 TPEx 表晚半年開始

```
tpex daily_quote          從 2007-07-02
tpex stock_per_pbr        從 2007-01-02   (119 個交易日沒有對應報價)
tpex margin_transactions  從 2007-01-02   (同上 119 天)
```

`data/daily_quote/tpex/2007/` 最早的檔就是 `2007_7_2.csv`,**這 119 天從來沒抓過**
(不是抓了失敗)。TWSE 側起點 2004-02-11,無此問題。

---

## 九、要修什麼(不在本稽核執行)

1. **補抓 5 個缺日 + 刪掉 4 個假 sentinel**(順序不能反,否則爬蟲會再跳過):
   - `rm data/daily_quote/twse/{2021/2021_8_18,2025/2025_8_15,2026/2026_4_29,2026/2026_5_28}.csv`
   - TWSE:`https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=csv&type=ALLBUT0999&date=YYYYMMDD`
   - TPEx(2023-06-08):`https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&o=csv&se=EW&d=112/06/08`
   - 兩市都要順便補 `market_index`(2026-05-28 也缺)。
2. **刪掉 2009-12-12 的 772 列幽靈資料**,並把該日改成 0-byte sentinel(它真的是休市)。
3. **根因守護(這是重點,不修就會再犯)**:
   - 寫 sentinel 前必須有**正向的休市證據**,不能只憑「交易所回空」。最便宜的
     判準:同日的 `daily_trading_details` / `margin_transactions` / `market_index`
     只要任何一張有資料,就不得寫 sentinel,改成 `[deferred]` 重抓。
   - 落檔前比對**內容指紋**:新抓到的整日內容若與庫內任何既有日期完全相同,
     一律拒收(2009-12-12 就是這樣混進來的,而且全史只此一例,守護成本極低)。
   - 加一支測試,鎖死「`daily_quote` 的日期集合 ⊇ 其他日頻表在同區間的日期集合」。
4. **除權事件**:`ex_right_dividend` 那 69 筆全零列要重新解析
   (`FinancialReader.readExRightDividend`);在修好之前,`prices.py` 應對
   「有 `權` 事件但因子拿不到」的情況 fail-loud,而不是靜靜套 `adj_factor = 1.0`。
5. **原始檔封存**:Python 直寫路徑應把抓到的 CSV 一併落 `data/`,否則 RAW 從
   2026-07-20 起就斷了。
6. **補 TPEx 2007 上半年 119 天**(需先確認 `stk_wn1430` 是否仍供應民國 96 年上半年)。

---

## 附錄:重跑腳本

四支腳本(cache vs PG 全等、缺口、重複日、異常值)完整內容見本次稽核的暫存目錄
`.../scratchpad/{cmp_dq,gaps_dq,missing_days,dupday_dq,anomaly_dq,jump_dq,saturday_dq}.py`。
核心查詢均已內嵌於上文各節,可直接以

```bash
PYTHONPATH=/Users/zaoldyeck/Documents/scala/quantlib \
  uv run --project research python -c "..."
```

重跑。最關鍵的兩條:

```sql
-- (1) cache vs PG 全等
--     雙邊 GROUP BY (market,date) 取 count / sum(hash(11欄)::HUGEINT) / bit_xor(hash(11欄))
--     再 FULL JOIN 比對 → 目前唯一差異為 2026-07-20 只在 cache

-- (2) 幽靈日偵測(排除 date 欄的整日指紋)
SELECT a.market, a.date, b.date, a.n FROM
 (SELECT market,date,count(*) n,
         sum(hash(company_code,opening_price,highest_price,lowest_price,
                  closing_price,trade_volume,trade_value)::HUGEINT) h
  FROM daily_quote GROUP BY 1,2) a
JOIN (同上) b ON a.market=b.market AND a.h=b.h AND a.n=b.n AND a.date<b.date;
-- → twse 2009-12-12 ≡ 2009-12-18(772 列)
```
