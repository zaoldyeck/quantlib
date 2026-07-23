# C-foreign_holding_ratio:cache 一致性與缺漏

**結論(白話):數字本身可以信,但「2010 年的上櫃資料」整年是假的。**

DuckDB cache 和 PostgreSQL **零不一致**——822 萬列、六個數值欄逐位相同,連一筆獨有鍵都沒有。
不能信的是兩件事:

1. **上櫃(TPEx)2010 年整年、31.9 萬列,其實是 2026-04-24 那天的快照被複製了 361 份。**
   裡面有 412 檔在 2010 年根本還沒上櫃(最晚一檔是 2026-04-22 才掛牌的),等於把 16 年後的答案
   寫進 2010 年。任何橫跨 2010 年的上櫃籌碼研究都吃到前視偏誤。
2. **4 個真的有開市的日子,兩個市場都整天沒資料**(2021-08-18、2025-08-15、2026-04-29、2026-05-28)。
   原因不在這張表——是 `daily_quote` 那 4 天留下的假休市檔讓爬蟲的交易日曆以為「那天沒開市」,
   於是連請求都沒送出去。

其餘 790 萬列可信:抽樣 30/30 逐欄相同、恆等式「持股比率 = 持有股數 ÷ 發行股數」822 萬列
零例外(證明欄位沒有錯位)、找不到未來日期、重複鍵、負值。看起來離譜的極端值(持股率 125%、
中視上限 0%)全都是交易所原始檔就這樣寫的。

---

## BUG 1:TPEx 2010 整年是 2026 年的快照(31.9 萬列,全表 3.88%)

**證據鏈**

| 環節 | 事實 |
|---|---|
| 原始檔 | `data/foreign_holding_ratio/tpex/2010/` 共 362 檔,其中 360 檔 md5 完全相同(`f276c09ce9e2b2b611a57a264774cb78`)、1 檔不同(`2010_1_4.csv`)、1 檔 0-byte(`2010_9_30.csv`) |
| 內容日期 | JSON 的 `tables[0].date`:360 檔寫 `115/04/24`(= 2026-04-24),`2010_1_4.csv` 寫 `115/05/12`(= 2026-05-12) |
| 檔案時間 | mtime 2026-04-25 ~ 2026-04-26(歷史回補那次跑的),`2010_1_4.csv` 為 2026-05-13 |
| DB | `2010-06-15` 與 `2026-04-24` 兩天的 884 檔,`outstanding_shares`/`foreign_held_shares`/`foreign_held_ratio` 逐檔相同 **884/884** |
| 相鄰日 | tpex 2010 的 360 組相鄰日對,全部「逐檔三欄完全相同」;2011 之後 0 組 |
| 直觀樣本 | 5347 世界先進:2010-01-04 / 2010-06-15 / 2010-12-31 / 2026-04-24 都是 `發行 1,867,392,355、外資持有 451,459,224、比率 24.17`;**2011-01-04 才變成真值 1,645,408,553 / 128,526,304 / 7.81** |
| 前視 | 2010 那 884 檔裡,**412 檔在 2010 年 TPEx 沒有任何報價**(首見報價年份散佈 2011~2026,最晚 2026-04-22) |
| 幽靈日 | 361 個日期裡 **111 個不是交易日**(週六日與國定假日都有資料) |

**根因(三層)**

1. TPEx `insti/qfii` 端點對「它沒有資料的日期」不回空,而是回**當下最新的快照**,JSON 標題誠實
   寫著真正的日期。
2. `Crawler.getForeignHoldingRatio` 以請求日當檔名存檔(`src/main/scala/Crawler.scala:423`)。
3. `TradingReader.readForeignHoldingRatio` **只從檔名取日期、從不讀內容日期**
   (`src/main/scala/reader/TradingReader.scala:899-901`:`val fileNamePattern(y,m,d) = marketFile.file.name`)。
   再加上第 894 行 `filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name)))`,已進 DB 的日期
   永遠不會被重讀,所以**不會自癒**——`2010_1_4.csv` 已被換成 2026-05-12 的新快照,DB 裡卻還是
   2026-04-24 的舊值,正是這條跳過邏輯的鐵證。

**設定也錯了**:`ForeignHoldingRatioSetting.scala:41` 宣告 tpex `firstDate = 2010-01-04`、
`application.conf:230` 註解寫「TPEx insti/qfii returns JSON (UTF-8, **2010+**)」,但實證**真資料起點是
2011-01-03**(該檔 `date` 欄 `100/01/03`,561 檔,與檔名相符)。

**汙染到誰**

- `src/quantlib/strat_lab/iter_52_ownership_flow_alpha.py:87` — `WHERE date BETWEEN START AND END`,
  **沒有 market 篩選**,而 `START = 2005-01-03`(`src/quantlib/strat_lab/iter_40_research_campaign.py:57`)
  → 直接吃進 31.9 萬列假資料。
- `src/quantlib/futures/strategies.py:183-193` — `SUM(foreign_held_shares)/SUM(outstanding_shares) GROUP BY date`,
  **沒有 market 也沒有日期篩選**。實測:2010-12-31 全市場比率 0.2299(只算上市 0.2473,被拉低 1.7pp);
  2010-12-25(週六)竟然有值 0.1092(只有假 tpex 那 884 列);2010-12-31 → 2011-01-03 出現 +0.30pp 的
  假跳動,而 `foreign_holding_chg5` / `chg_std63` / `foreign_holding_chg_z`(第 340-341、403-405 行,
  在第 463 行以 0.10 權重進訊號)全部繼承這個假跳動。
- Serenity(`src/quantlib/serenity/engine.py:1000`)與 Evergreen(`ev48_chip_axes.py:46`、`ev18_make_packs.py:96`)
  雖然也沒下 market 篩選,但研究期間都在 2018 年之後,沒踩到。

---

## BUG 2:4 個真交易日兩市場全缺(交易日曆的連鎖傷害)

缺的日期:**2021-08-18(三)、2025-08-15(五)、2026-04-29(三)、2026-05-28(四)**,twse 與 tpex 都是 0 列,
原始檔連下載都沒下載過(`data/foreign_holding_ratio/{twse,tpex}/…` 皆 MISSING)。

**這幾天確實有開市**(獨立佐證,不靠 daily_quote):

| 日期 | tpex 報價 | 集中市場指數 | 融資融券 | 三大法人 | 本益比表 | 台指期 |
|---|---|---|---|---|---|---|
| 2021-08-18 | 902 | 237 | 1,833 | 1,812 | 1,738 | 1,735 |
| 2025-08-15 | 967 | 346 | 2,044 | 2,073 | 1,898 | 2,341 |
| 2026-04-29 | 1,005 | 341 | 2,160 | 2,212 | 1,954 | 2,287 |
| 2026-05-28 | 1,008 | — | 2,171 | 1,842 | 1,964 | (超出台指期資料範圍) |

**根因是連鎖的**:`Task.pullForeignHoldingRatio`(`src/main/scala/Task.scala:470-483`)第 481 行
`.filter(d => tradingDays.contains(d))`,而 `tradingDays = loadTwseTradingDays()`(同檔 415-423 行)
= `SELECT DISTINCT date FROM daily_quote WHERE market='twse'` ∪ 本機 twse 報價檔(>1024 bytes)。
`daily_quote/twse` 這 4 天是 0-byte 假 sentinel(見 `docs/data_audit/_done/C-daily_quote.json` BUG 1),
於是**交易日曆判它們是假日,外資持股連請求都不會送**——而且 tpex 也被同一把 twse 尺子擋掉。
同一個機制會讓 `sbl_borrowing`、`index`、`margin` 等所有走 `tradingDays` 篩選的日頻表一起缺這幾天。

**修復順序不能反**:先修 `daily_quote` 的假 sentinel(刪檔重抓),`loadTwseTradingDays()` 才會認得
這 4 天,之後 `Main pull foreign_holding_ratio --since` 才抓得到。

---

## SUSPECT 1:2014 年起的春節休市日公告可能被系統性漏抓

2005~2013 年,twse 每年春節休市的**前 2 天**都有一份 QFIIS 公告(共 18 天,見下面的 REAL 2);
2014 年起一天都沒有。可能是 TWSE 改了政策,也可能是 BUG 2 的同一把尺子(`tradingDays` 來自
daily_quote)把它們全擋在門外——那 18 天的檔案是交易日曆篩選加進來之前抓的。**本地資料無法分辨**,
需要對 2014-2026 的春節休市首兩日各打一次 MI_QFIIS 才知道。

## SUSPECT 2:資料落後齊備日 2 個交易日

`latest_complete_trading_day() = 2026-07-21`,本表最新只到 **2026-07-17**;
同時 `daily_quote` / `daily_trading_details` / `stock_per_pbr`(Python 直寫路徑)已到 2026-07-20,
本表(Scala + 全量重建路徑)只到 07-17 → **表間日期錯位 3 天**,正是 CLAUDE.md 記載 2026-07-15 事故的形態。
這是營運狀態不是資料錯誤,但策略閘門會因此 fail-closed。

---

## OK:查過沒問題的部分

- **cache vs PG 逐位相同**:8,225,920 列全表比對(不是抽樣),六個數值欄 `IS DISTINCT FROM` 計數皆 0,
  雙向 `EXCEPT` 找獨有鍵 cache_only 0 / pg_only 0。逐年 × market 39 組筆數全同,逐日差異 0 天。
- **指定隨機抽樣**:seed 20260722,每個 market 抽 3 日 × 5 檔(共 30 組),六欄逐一比對 **30/30 相同**。
- **schema 無型別降級**:PG 11 欄 → cache 9 欄,只少了 `id`(代理鍵)與 `company_name`;
  `src/quantlib/db.py:149-153` 的 pg-attach 對照 view 欄位與順序和 cache 表逐字相同,parity 沒破。
- **檔名 vs 內容日期**:twse 5,310 檔**零錯位**;tpex 2011 年以後 3,797 檔**零錯位**(錯位全集中在 2010)。
- **欄位沒有錯位**:恆等式「持股比率 = 持有股數 ÷ 發行股數 × 100」在 8,225,919 列(排除 1 列發行股數 0)
  **零例外**,tpex(JSON 帶 `%` 字串)與 twse(Big5 CSV)都通過——證明兩條解析路徑的欄位索引都對。
- **異常值掃描**:NULL 0、負股數 0、負比率 0、上限比率越界 0、未來日期 0、重複鍵 0、代號格式異常 0。
- **TPEx 空檔**:315 個 0-byte sentinel,除了 2010-09-30(在假資料年內)全部落在非交易日。

## REAL:看起來像錯、其實是交易所原始檔就這樣

| 現象 | 筆數 | 原始檔佐證 |
|---|---|---|
| `foreign_held_ratio > 100` | 134 | 8406 F-金可 2013-11-28 = `108.56%`(TPEx JSON,備註「已達上限」);0081 恒香港 2010-10-21 發行 50,000 股卻持有 62,600 股 → 125.20(TWSE CSV)。ETF 與 KY 公司的「發行股數」登記值落後實際單位數 |
| 發行股數 = 0 全列零 | 1 | tpex 6594 展匯科 2018-10-23 = `['765','6594','展匯科','0','0','0','0%','0%','100%','已達上限']` |
| 尚可投資比率 100.19 | 1 | twse 3519 綠能 2011-05-26,CSV 原文 `"224,097,171","224,536,171","0","100.19"` |
| 法令上限 = 0 | 5,962 | 9928 中視全史 5,308 天上限都是 `0.00`(廣電三法禁外資);另 008201、0686、2891A 等少數 |
| 尚可投資股數 = 0 但上限 100% | 69,078 | twse 2408 南亞科 2014-09-03 CSV 原文 `"23,961,008,099","0","1,424,544,623","0.00","5.94","100.00"`;2475 華映同型。交易所自己就填 0 |
| 單日持股率跳動 > 20pp | 557 | 多為 ETF 單位數變動與新掛牌首日(0.00 → 87%),非解析錯誤 |
| twse 在非交易日有資料 | 18 天 | 2005~2013 每年春節休市**首 2 日**(如 2013-02-07/08)。TAIFEX 那兩天完全沒交易 → 確實休市,但 TWSE 仍公告持股統計(T+2 交割造成持股續動:2013-02-07 有 24/865 檔數字變動、02-08 有 4/865 檔)。**這是真資料** |

---

## 重跑方式

```bash
uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/01_counts.py
uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/02_value_parity.py       # ~28s
uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/03_coverage_gaps.py
uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/04_stale_repeat_scan.py
uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/05_filename_vs_content_date.py
uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/06_value_anomaly.py
uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/07_sample_parity.py
uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/08_tpex2010_impact.py
uv run --project . python docs/data_audit/scripts/C-foreign_holding_ratio/09_missing_trading_days.py
```
