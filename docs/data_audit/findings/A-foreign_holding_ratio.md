# A-foreign_holding_ratio — 外資持股比率原始檔 → DB 解析正確性

**稽核日**:2026-07-22
**受測程式**:`src/main/scala/reader/TradingReader.scala:886-954`(`readForeignHoldingRatio`)
**設定**:`src/main/scala/setting/ForeignHoldingRatioSetting.scala`
**落地表**:`foreign_holding_ratio`(PG 8,225,920 列;twse 5,091,216 列 / 5,308 天、tpex 3,134,704 列 / 4,158 天)
**原始檔**:`data/foreign_holding_ratio/{twse,tpex}/<year>/*.csv` 共 9,783 個(317 個是休市日的空檔或只有標頭)
**判定**:**BUG**

---

## 一句話結論

**解析本身是滿分:9,466 個有資料的原始檔全部重讀一遍,和資料庫逐格對照,列數零缺零多、
六個數值欄零差異、單位與百分比刻度全對、跨 22 年兩種欄位版型零錯位。**

**壞掉的不是「怎麼讀」,是「當成哪一天」——程式用檔名當日期,從頭到尾沒看過檔案裡自己寫的日期。**
於是上櫃(TPEx)2010 年整年、361 天、31.9 萬列,裝的其實是 2026-04-24 那天的快照:
裡面有 412 檔股票在 2010 年根本還沒上市櫃(最誇張的 4442 竣邦-KY 是 2023-11-29 才掛牌),
等於把 16 年後的答案寫進 2010 年。**任何橫跨 2010 年的上櫃籌碼研究都在吃前視。**

另外三件小事:2010-01-04 那個檔已經被重抓過、現在檔案有 888 列而資料庫是 884 列,
**兩邊不一致而且程式設計成永遠不會發現**;有 4 個真的開市的日子兩個市場都整天沒資料;
原始檔有 6 個欄位沒接進資料庫,其中 TPEx 的「備註」寫著「已達上限」「禁止投資」——
那正好就是這張表宣稱要做的訊號(外資接頂)。

---

## 一、查了什麼(方法)

**不呼叫受測程式**(它正是受測對象),另寫一套獨立解析器:TWSE 用 Python `csv` + `big5hkscs` 解碼、
TPEx 用 `json`;然後與 PG 逐格比對。四條互相獨立的證據線:

| # | 方法 | 覆蓋 | 腳本 |
|---|---|---|---|
| 1 | 跨時代抽樣逐欄比對(每檔股票 7 欄逐格) | twse 2005/2007/2009/2010/2015/2018/2023/2026 + tpex 2011/2013/2015/2018/2023/2026 共 17 天 | `01_sample_reparse.py` |
| 2 | **全量**重讀 + 逐 (market,date) 列數與六欄總和對帳 + 內容日期 vs 檔名日期 | 9,783 檔全掃 vs PG 8,225,920 列 | `02_full_corpus_recon.py` |
| 3 | 標頭簽章掃描(欄位有沒有無聲漂移)+ 未入庫欄位盤點 | 9,783 檔全掃 | `03_schema_and_dropped_cols.py` |
| 4 | 汙染日 / 幽靈日 / 缺日 / 單位不變式 SQL | 全表 | `04_contamination.sql` |

腳本全部在 `docs/data_audit/scripts/A-foreign_holding_ratio/`,可原地重跑。

---

## 二、確認沒問題的部分(負結果,別再查一次)

- **逐格零差異**:9,466 個有資料的檔案,獨立解析結果與 PG 的
  `(列數, Σ發行股數, Σ尚可投資股數, Σ持有股數, Σ尚可投資比率, Σ持股比率, Σ法令上限)`
  **完全相同,只有 1 個例外**(tpex 2010-01-04,見 BUG 2)。`only_raw = only_db = 0`——
  原始檔有的每一列都進了 DB,DB 沒有多出任何一列。
- **欄位沒有錯位**:TWSE 全史只有 **2 種標頭簽章**,且 index 0-8 語意完全對得上:

  | idx | 2005-10-11 ~ 2009-04-09(1,080 檔) | 2009-10-01 ~ 2026-07-09(4,230 檔) | 入庫欄位 |
  |---|---|---|---|
  | 0 | 證券代號 | 證券代號 | `company_code` |
  | 1 | 證券名稱 | 證券名稱 | `company_name` |
  | 2 | 國際證券編碼 | 國際證券編碼 | **未接** |
  | 3 | 發行股數 | 發行股數 | `outstanding_shares` |
  | 4 | 外資尚可投資股數 | 外資及陸資尚可投資股數 | `foreign_remaining_shares` |
  | 5 | 全體外資持有股數 | 全體外資及陸資持有股數 | `foreign_held_shares` |
  | 6 | 外資尚可投資比率 | 外資及陸資尚可投資比率 | `foreign_remaining_ratio` |
  | 7 | 全體外資持股比率 | 全體外資及陸資持股比率 | `foreign_held_ratio` |
  | 8 | 法令投資上限比率 | 外資及陸資共用法令投資上限比率 | `foreign_limit_ratio` |
  | 9 | 與前日異動原因 | **陸資法令投資上限比率** | **未接** |
  | 10 | 最近一次申報異動日期 | 與前日異動原因 | **未接** |
  | 11 | — | 最近一次申報異動日期 | **未接** |

  ECFA 時代(2009-04 → 2009-10 之間)新增的 `陸資法令投資上限比率` 插在 index 9,
  **沒有推移到 0-8**,所以 reader 寫死索引 0/1/3/4/5/6/7/8 在兩個時代都正確。
  TPEx JSON 全史只有 **1 種 fields 簽章**(10 欄),315.5 萬列全部剛好 10 格。
- **`row.size >= 10` / `row.size < 9` 這兩個過濾條件全史零殺傷**:TWSE 資料列只有 12 格(舊版型)
  與 13 格(新版型)兩種,沒有任何一列 < 10;TPEx 沒有任何一列 < 9。
- **單位正確**:是「股」,不是張也不是千股。實例:2026-07-17 台積電 `outstanding_shares =
  25,932,370,067`(259.3 億股,與實際發行股數相符)、`foreign_held_shares = 17,982,370,536`、
  `foreign_held_ratio = 69.34`。
- **百分比刻度一致**:兩個市場都是「5.25 代表 5.25%」,不是 0.0525。
  TPEx 原始檔寫 `"87.79%"`、TWSE 寫 `"87.79"`,`cleanCell` 把 `%` 去掉後同刻度。
  全表不變式檢定:`|持股比率 − 100×持有股數/發行股數|` 最大 **0.0100**(= 來源自己四捨五入到小數 2 位),
  超過 0.02 的 **0 列**(twse 509 萬列 + tpex 313 萬列)。這同時二次證明欄位沒有錯位。
- **編碼正確**:Big5-HKSCS 解出的中文名與 DB 全史一致,`company_name` 亂碼 0 列、
  空字串 0 列、前後空白 0 列。
- **民國/西元換算正確**:TWSE 5,310 檔的標頭日期(如 `115年07月01日`)換算後
  **全部等於檔名日期,零不符**。TPEx 4,158 檔中 361 檔不符(見 BUG 1)。
- **沒有重複列**:任何一檔裡都沒有重複的代號 → reader 的 `distinctBy(market,date,code)` 全史是空操作,
  沒有靜靜吃掉任何列。
- **沒有跨日複製**:整日內容指紋(列數 + Σ持有股數 + Σ發行股數)與前一日完全相同的日子,
  22 年全史只有 **360 天,全部落在 tpex 2010**(= BUG 1 的範圍)。2011 年以後 0 天、TWSE 全史 0 天。
- **0-byte sentinel 沒有誤殺**:315 個 0-byte TPEx 檔 + 2 個只有標頭的 TWSE 檔(2009-12-12、2026-05-30,
  都是週六),其中只有 1 個(tpex 2010-09-30)落在真交易日,而它本來就在 BUG 1 的汙染區間內。
  這 317 個空檔一列都沒進 DB(對的)。
- **看起來離譜、其實是來源真值**(逐檔比對過原始檔,不是 reader 的錯):
  - 持股比率 > 100%:134 列。如 2010-10-20 `0081 恒香港` 發行 50,000 股卻持有 62,600 股 → 125.20%,
    原始檔就是這樣寫(`="0081","恒香港","HK2833027330","50,000","0","62,600","25.20","125.20",...`)。
    集中在 ETF 與 DR(0057 富邦摩台、9105 泰金寶-DR、8406 F-金可),是受益權單位數與外資持有數
    在申購買回期間錯開造成。
  - 法令上限 = 0:5,962 列,如 `9928 中視`(特許行業禁止外資)。原始檔第 9 格就是 `"0.00"`。
  - 發行股數 = 0:1 列,tpex 2018-10-23 `6594 展匯科`,原始檔為 `['765','6594','展匯科','0','0','0','0%','0%','100%','已達上限']`。
- **2023-06-08 的「幽靈日」是 `daily_quote` 自己缺料,不是我們多出來的**:那天 tpex 的
  `foreign_holding_ratio` 有 815 檔、內容日期 `112/06/08` 完全正確,同日 TWSE 有 1,194 檔報價
  (市場確實有開),是 `daily_quote` 的 tpex 側那天 0 列。**這張表對、報價表錯。**
- **CNY 前的「非交易日有資料」不是幽靈日**:TWSE 有 18 天在 `daily_quote` 查無報價卻有外資持股資料,
  全部成對出現在農曆年前(2005-02-04/05、2006-01-26/27 … 2013-02-07/08),
  標頭日期與檔名一致(`"94年02月05日 外資投資持股統計"`),是交易所在休市窗口仍公告申報統計。
  兩天之間 993 檔有 985 檔同值、8 檔有異動——是真資料,不是複製品。

---

## BUG 1(嚴重):日期只認檔名、不看檔案內容 → TPEx 2010 整年裝的是 2026-04-24 的快照

### 現象

`data/foreign_holding_ratio/tpex/2010/` 的 361 個非空檔案,JSON 內容日期寫著 **`115/04/24`**
(= 2026-04-24;其中 `2010_1_4.csv` 寫 `115/05/12`),但檔名是 2010 年的日期。
`readForeignHoldingRatio` 的日期完全來自檔名:

```scala
// TradingReader.scala:899-901
val fileNamePattern = """(\d+)_(\d+)_(\d+).csv""".r
val fileNamePattern(y, m, d) = marketFile.file.name
val date = LocalDate.of(y.toInt, m.toInt, d.toInt)
```

JSON 裡的 `tables[0].date` 從頭到尾沒被讀過。於是 **319,124 列 2026 年的資料
被貼上 2010 年的日期**入庫,並原封傳進 `var/cache/cache.duckdb`。

### 證據

| 環節 | 事實 | 怎麼重現 |
|---|---|---|
| 內容日期 | 361 檔的 `tables[0].date` ≠ 檔名日期,全部集中在 2010 年 | `02_full_corpus_recon.py` → `DATE MISMATCH: 361 / ('tpex','2026-04-24') 360 / ('tpex','2026-05-12') 1` |
| DB 逐檔同值 | `8455 大拓-KY` 在 2010-05-14、2010-10-01、2026-04-24 三天完全同值(發行 25,219,056 / 持有 22,142,306 / 比率 87.79) | `04_contamination.sql` (3) → 884 檔 **884 檔全同** |
| 物理不可能 | `8455` 的第一筆報價是 **2016-01-08**;`4442 竣邦-KY` 是 **2023-11-29**;`2754 亞洲藏壽司` 是 2020-09-17 | `SELECT company_code, min(date) FROM daily_quote WHERE company_code IN ('8455','4442','2754') GROUP BY 1` |
| 前視規模 | 那 884 檔裡 **412 檔在 2010 年沒有任何報價** | `04_contamination.sql` (4) |
| 幽靈日 | 361 個日期裡 **111 個不是交易日**(週六日與國定假日照樣有整天資料) | `04_contamination.sql` (5) |
| 相鄰日指紋 | 全史「整日與前一日完全相同」共 360 天,**全部在 tpex 2010-01-05 ~ 2010-12-31** | `04_contamination.sql` (2) |
| 檔案時間 | mtime 2026-04-25(六)~ 2026-04-26(日),前一個交易日正是 2026-04-24(五) | `stat -f "%Sm" data/foreign_holding_ratio/tpex/2010/2010_6_15.csv` |

### 根因(三層,每層都有洞)

1. **來源端**:TPEx `insti/qfii` 對「當下取不到的日期」不回空,而是回**最近一次的快照**,
   JSON 標題誠實寫著真正的日期。(2026-07-22 實測:現在同一個端點對 `date=099/06/15` 已能回
   正確的 `99/06/15`、553 檔 —— 端點行為在這段期間變過,所以「當初抓錯」不代表「現在抓不到」,
   **重抓是可行的**。)
2. **落檔端**:`ForeignHoldingRatioSetting.tpex` 的 `validate` 直接回 `DownloadValidation.Valid`
   (`ForeignHoldingRatioSetting.scala:46`),不做任何檢查;`Crawler.getForeignHoldingRatio`
   用請求日當檔名。
3. **解析端(本單位的責任)**:reader 手上就有 `tables[0].date`,卻只用檔名。
   這是最後一道能攔下來的關卡,而它沒有攔。

### 修法

1. reader 改成**解析檔案內容的日期並與檔名比對**:TPEx 讀 `tables[0].date`(民國 `YYY/MM/DD`)、
   TWSE 讀第一行標頭(`YYY年MM月DD日`),不符即 **fail-loud + 刪檔重抓**,不得靜默入庫。
   TWSE 側目前全史零不符,加上這道守護等於零成本。
2. 清汙染:PG 與 `var/cache/cache.duckdb` 兩邊 `DELETE FROM foreign_holding_ratio
   WHERE market='tpex' AND date < '2011-01-01'`(319,124 列),刪掉 `data/foreign_holding_ratio/tpex/2010/`
   的 362 個檔,重抓(端點現在給得出真值,實測 `099/06/15` → 553 檔)。
3. 防復發守護:落檔期加「整日內容指紋與前一交易日相同 → 告警」的檢查——TPEx 休市日的標頭日期
   有時是對的,單靠「標頭 vs 檔名」抓不到,只有指紋抓得到(這是 `A-margin_transactions` 已踩過的同一株病)。

---

## BUG 2:2010-01-04 的原始檔和資料庫已經不一致,而且程式永遠不會發現

`data/foreign_holding_ratio/tpex/2010/2010_1_4.csv` 在 2026-05-13 被重抓過一次,
現在檔案裡是 **888 列(內容日期 115/05/12)**,而 DB 裡是 **884 列**(2026-04-24 那份快照)。

原因是 reader 的去重條件是「**(market, 檔名) 已經在 DB 裡就跳過**」:

```scala
// TradingReader.scala:888-894
val dataAlreadyInDB = ... map { case (market, date) => (market, date.format("yyyy_M_d") + ".csv") }
val files = ... filterNot(m => dataAlreadyInDB.contains((m.market, m.file.name)))
```

只要那個 (market, date) 有任何一列在 DB,整個檔就不會再讀。所以**原始檔被更新了,DB 也不會跟上**;
`Main read foreign_holding_ratio` 重跑一百次都不會修正。這張表因此變成「不可從原始檔重生的手工品」。

**證據**:`02_full_corpus_recon.py` → `ROW-COUNT MISMATCH: 1 / ('tpex','2010-01-04', raw=888, db=884)`
(其餘 9,465 檔的列數與六欄總和全部相同)。

**修法**:併入 BUG 1 的清洗;另把去重改成「檔案 mtime 或內容雜湊 > DB 記錄」才跳過,
或在匯入後做一次「解析列數 == DB 列數」對帳,不符即告警。

---

## SUSPECT 1:6 個欄位原始檔有、schema 沒接——其中一個正是這張表宣稱要做的訊號

`db/table/ForeignHoldingRatio.scala` 只有 9 個資料欄。原始檔還有這些沒接:

| 來源 | 欄位 | 非空 / 有差異的量 | 值得接嗎 |
|---|---|---|---|
| TPEx `[9]` | **備註** | 「禁止投資」8,054 列、「已達上限」526 列 | **值得**。Setting 的 docstring 寫這張表是為了「外資接頂訊號(逼近法令上限)」,而交易所已經**直接把答案印出來**了,我們卻沒接 |
| TWSE `[9]` | **陸資法令投資上限比率**(2009-10 起) | 與共用上限**不同的有 889,709 / 4,285,845 列(20.8%)**,多為 ETF 共用 100% 但陸資 0% | 值得。現在只存共用上限,陸資禁投的資訊整個消失 |
| TWSE `[10]` | 與前日異動原因(註) | 42,442 列非空(代碼 2 / 4 / 5,增減資之類) | 中等。可解釋發行股數跳動 |
| TWSE `[11]` | 最近一次申報持股異動日期 | 幾乎每列都有 | 中等。是 PIT 元資料 |
| TWSE `[2]` | 國際證券編碼(ISIN) | 每列都有 | 低。已有 company_code |
| TPEx `[0]` | 排行 | 每列都有 | 低。可由持股比率重算 |

**修法**:依 CLAUDE.md 的 Schema Contract——先改 Slick `ForeignHoldingRatio` 加
`note`(TPEx 備註)、`china_limit_ratio`(TWSE 陸資上限)、`change_reason`、`last_report_date`,
再 `ALTER TABLE` 對齊,然後從 `data/` 全量重讀(原始檔都在,可重生)。
若決定不接,就在本文件與 `CLAUDE.md` 明記「刻意不接」,免得下一個人重查。

---

## SUSPECT 2:4 個真交易日兩個市場都整天沒資料

2021-08-18(三)、2025-08-15(五)、2026-04-29(三)、2026-05-28(四)——
`daily_quote` 的上櫃報價各有 902 / 967 / 1,005 / 1,008 檔(市場確實有開),但
`foreign_holding_ratio` 這四天 **twse 與 tpex 都是 0 列,原始檔根本不存在**。

根因不在 reader,在上游:`Task.pullForeignHoldingRatio`(`Task.scala:480`)用
`loadTwseTradingDays()` 過濾日期,而 `daily_quote` 的 twse 側這四天也是 0 列——
交易日曆以為那天休市,連請求都沒送出去。**同一個上游缺陷會同時打掉所有以 TWSE 交易日曆
驅動的資料源**(`C-foreign_holding_ratio.md` 已記錄同一組日期)。

**修法**:先修 `daily_quote` 那四天的假休市檔,再重抓這四天的外資持股。
另加例行體檢:`foreign_holding_ratio` 的日期集合必須覆蓋 `daily_quote` 的交易日集合,缺一天就告警。

---

## OK(小事,記錄免得重查)

- `cleanCell` 會把公司名裡的空白也去掉:`元大MSCI A股` → DB 存成 `元大MSCIA股`
  (`00739`,實測 `SELECT company_name FROM foreign_holding_ratio WHERE company_code='00739'`)。
  600 檔抽樣中 104 列受影響。`company_name` 只是描述欄、鍵是 `company_code`,**不影響任何計算**,
  但用名字做 join 會踩到。
- `QuantlibCSVReader` 第 21 行「含 `""` 但無 `,""` 就跳過整列」這條特規對本資料源全史零殺傷
  (資料列一定含 `,""`,證據是全量比對 `only_raw = 0`)。
- TWSE 2009-12-12 是週六補行交易日,交易所只發了標頭沒發資料——不是我們漏抓。

---

## 附:可重跑指令

```bash
# 抽樣逐欄比對(17 天,跨 2005~2026 兩市場)
python3 docs/data_audit/scripts/A-foreign_holding_ratio/01_sample_reparse.py

# 全量重讀 + 內容日期/列數/六欄總和對帳(9,783 檔,約 3 分鐘)
python3 docs/data_audit/scripts/A-foreign_holding_ratio/02_full_corpus_recon.py

# 標頭版型 + 未入庫欄位盤點
python3 docs/data_audit/scripts/A-foreign_holding_ratio/03_schema_and_dropped_cols.py

# 汙染日 / 幽靈日 / 缺日 / 單位不變式
psql -h localhost -p 5432 -d quantlib -f docs/data_audit/scripts/A-foreign_holding_ratio/04_contamination.sql
```
