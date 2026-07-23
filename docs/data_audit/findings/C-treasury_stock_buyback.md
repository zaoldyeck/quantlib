# C-treasury_stock_buyback — 庫藏股表 cache 一致性與缺漏稽核

**結論(白話):cache 和資料庫是同一份、完全一致,複製這一步沒問題;但這張表本身的資料不能直接拿來用——有四個上游爬蟲解析 bug,cache 只是忠實把錯的東西複製過來。**

- **cache vs PostgreSQL:完全一致**（schema、總筆數、逐年逐市場、全表逐欄逐筆都零差異）。
- **但資料本身有 4 個真 bug**(全在 Scala reader，cache 忠實鏡像):
  1. **2000-2010 整整 11 年被丟光**（約 2,760 筆,幾乎等於現有整張表的量）。
  2. **`pct_of_capital` 裝錯東西**——存的是「買回總金額」(億元級),不是比例。
  3. **`executed_shares` 整欄 2,933 筆全是 0**（讀到空白欄）。
  4. **公司名近九成是亂碼**（影響低,下游用代號 join,不影響訊號）。

**能不能信?** 想知道「哪家公司哪天宣告買回、預定買幾張、價格區間、預定期間」——**可以信**(這幾欄正確,但只有 2011 年以後)。想用「實際買回股數 / 占股本比例 / 2011 年以前的歷史 / 公司中文名」——**不能信**,要先修 reader 再重讀。

---

## 一、cache 與 PG 是否一致?→ 是,零差異

| 檢查 | 結果 |
|---|---|
| Schema | 11 欄同名、同序、同型;PG 合成主鍵 `id` 正確排除;無型別降級 |
| 總筆數 | cache 2933 == PG 2933 |
| 逐年逐市場 | 32 格(2011-2026 × twse/tpex)逐格相同 |
| 全表逐欄逐筆 | DuckDB ATTACH pg 雙向 `EXCEPT` = 0/0(byte-for-byte 相同) |
| 邊界日期 | 兩邊 min/max = 2011-01-05 / 2026-07-08 |

`research/cache_tables.py:66` 是無 `WHERE` 的整表複製,設計正確。**cache 同步這一步沒有任何缺陷。** 下面四個問題全是上游 reader bug，cache 只是把 PG 裡已經錯的資料原樣搬過來。

重跑指令(可重現):
```bash
uv run --project . python -c "
import duckdb, os; from research import paths
con=duckdb.connect(str(paths.CACHE_DB), read_only=True)
con.sql('INSTALL postgres; LOAD postgres;')
con.sql(f\"ATTACH 'host=localhost port=5432 dbname=quantlib user={os.environ['USER']}' AS pg (TYPE postgres, READ_ONLY)\")
cols='market,announce_date,company_code,company_name,planned_shares,price_low,price_high,period_start,period_end,executed_shares,pct_of_capital'
print(con.sql(f'(SELECT {cols} FROM treasury_stock_buyback) EXCEPT (SELECT {cols} FROM pg.public.treasury_stock_buyback)').fetchall())
"
# → []  (兩邊完全相同)
```

---

## 二、BUG 1（最嚴重）：2000-2010 整整 11 年被靜默丟光

**現象**：原始 HTML 快照裡有 2000-2026 的資料,但資料庫只有 2011-2026。缺掉的是 twse 1,879 筆 + tpex 881 筆 = **約 2,760 筆**,幾乎等於現在整張表(2,933 筆)的量——等於這張表本來該有的歷史被砍掉一半。

**證據**：
- 原始檔 `data/treasury_stock_buyback/twse/2026/2026_7.html`(mtime 2026-07-19)解析出 2000-2026 共 3,566 筆有效列;tpex 同檔 2,157 筆。
- DB/cache `min(announce_date)=2011-01-05`。2000-2010 逐年(twse)188/188/146/143/257/155/144/125/**405**/77/51 全部不在庫。
- **根因**：`src/main/scala/reader/TradingReader.scala:993` 的日期解析器用了 pattern `"yyy/MM/dd"`。Java 的 `DateTimeFormatter` 對 `yyy`(3 個 y)要求**最少 3 位數字**;民國 89-99 年(2000-2010)只有 2 位數,解析直接拋例外 → `TradingReader.scala:1034` 的 `getOrElse(throw)` 被 `.toOption` 吞成 `None` → 整列丟棄。
- **實測**(`scratchpad/T.java`,直接跑 Java):

  | 輸入 | 結果 |
  |---|---|
  | `100/01/05`(民國100=2011) | → 2011-01-05 ✓ |
  | `99/12/31`(民國99=2010) | → **FAIL** DateTimeParseException |
  | `89/01/15`(民國89=2000) | → **FAIL** |
  | `97/11/12`(民國97=2008，台泥) | → **FAIL** |
  | `115/07/08`(民國115=2026) | → 2026-07-08 ✓ |

  cutoff 精準落在民國 100 = 2011,與資料庫起點完全吻合。

**影響**：任何橫跨 2004-2010 的回測（daily_quote 從 2004 就有）在這段期間看不到任何庫藏股事件,庫藏股訊號在該區間恆為缺值——靜默的樣本偏差 / 前視風險。

**修法**：把 `TradingReader.scala:993` 的 pattern 由 `"yyy/MM/dd"` 改成 `"y/MM/dd"`(單一 `y` 變寬,吃 1-19 位),再 `sbt "runMain Main read buyback"`。爬蟲的 insert-only 去重會自動把 2000-2010 補進來、不會重複既有列。**不用重抓**——原始快照本來就含 2000-2010。⚠️ 同一個 `parseMinguoSlashDate` 也被 `insider_holding`(line 1114)和本表的 `period_start/end` 用,是**同一缺陷類**,一次修好全覆蓋;`C-capital_reduction`(BUG_TRACKER #66)也是「從 2011 起」的症狀,建議一併查是否同源。

---

## 三、BUG 2：`pct_of_capital` 存的是「買回總金額」,不是比例

**現象**：這欄名字叫「占公司資本比例」,台股法律上限是已發行股份的 10%,不可能超過 10。但 2,743/2,933 筆(93%)都 >10,最大值 **8,509,335,774(85 億)**,而落在合理 0-10 區間的**一筆都沒有**。

**證據**：
- 根因：`TradingReader.scala:1041` 用 `cols(16)`,但 MOPS 原始資料列(20 格)的 `cols(16)` 是「**本次已買回總金額**」;真正的「占已發行股份比例」在 `cols(18)`。
- 端到端驗證:台泥(1101)2019-05-10 那筆,cache 存 `pct_of_capital = 348,959,120` —— 正好是 HTML 裡的「本次已買回總金額」;真值比例 0.15% 根本沒被存進來。
- `pct_of_capital / planned_shares` 對每一筆都落在該筆的買回價格區間內(8908:40.92∈[30,60]、6491:283.88∈[240,300]、6561:362.96∈[280,470]),證明它就是「金額 = 股數 × 價」。

**根因本質**：reader 在 `TradingReader.scala:1006-1012` 自己寫的欄位對照表以為是 18 欄,實際 HTML 是 20 欄(「是否執行完畢」後面多一欄「買回達一定標準資料」,把後面每一欄往後推)。

**修法**：`cols(16)` → `cols(18)`。因為是值錯不是鍵錯,去重不會覆蓋既有錯值,要先清表再重讀(或改 upsert)。

---

## 四、BUG 3：`executed_shares` 全表 2,933 筆一律是 0

**現象**：整欄沒有一筆非零值(`distinct = {0}`)。這欄本該記「實際已買回幾股」。

**證據**：根因同 BUG 2 的版位錯位——`TradingReader.scala:1040` 用 `cols(12)`(那是「買回達一定標準資料」,幾乎全空),真正的「本次已買回股數」在 `cols(13)`。台泥 2019-05-10 實際買回 8,000,000 股,cache 存 0;`parseLong` 對空字串回 0,於是整欄靜默歸零。

**影響**：任何依「實際買回執行率」的訊號在這張表恆為 0,不可用。

**修法**：`cols(12)` → `cols(13)`,和 BUG 2 一起修版位,清表重讀。

---

## 五、BUG 4：公司名近九成是亂碼（影響低）

**現象**：2,603/2,933 筆(89%)`company_name` 含亂碼置換字元(U+FFFD)。台泥(1101)存成 `�唳野`,台積電(2330)存成 `�啁���/td>`(還混進洩漏的 HTML 標籤 `/td>`)。近期 2026 的列是乾淨的(2388 威盛、6180 橘子)。

**證據**：codepoint 直接是 `0xfffd`,不是終端顯示問題;cache==PG,所以汙染在 PG,cache 忠實複製。汙染集中在舊列 + 近期列乾淨,與「初次大批匯入時用錯編碼、之後 insert-only 去重不覆蓋既有列」一致。

**影響：低。** 下游研究一律用 `company_code`(代號)做 join,不用中文名,所以對訊號和回測沒有影響——只是人看報告時公司名不可讀。

**修法**：修 `parseMopsHtml` 的編碼處理(強制 Big5-HKSCS 或正確讀 meta charset)+ 清表重讀;或加一個只更新名字的 upsert 讓乾淨名字覆蓋舊亂碼。低優先。

---

## 六、查了但沒問題的（負結果，免得重查）

- **異常值全過**：無負價、無反轉價帶(low>high)、無負/零 planned_shares、executed 從不超過 planned、無未來公告日、period 起訖無反轉。
- **尾端不是缺口**：原始 HTML 最新公告日 = DB 最新日 = 2026-07-08,快照 mtime 07-19,尾端忠實(事件型稀疏資料本就可長期無公告)。
- **2011-2026 逐年小差不是漏抓**:HTML 比 DB 多的 1-4 筆/年,全由去重鍵 `(market, announce_date, company_code)` 收合 twse 22 個「同公司同日多次別」的列造成,是設計去重。
- **極少數邊界**:`period_start < announce_date` 3 筆、`period 期間 > 70 天` 2 筆,疑為更正/展延公告,非系統性,不單列為 bug。

---

*稽核者驗證腳本:`scratchpad/T.java`(民國年解析實測);其餘查詢為即拋 DuckDB/psql,證據已內嵌本報告可重跑。*
