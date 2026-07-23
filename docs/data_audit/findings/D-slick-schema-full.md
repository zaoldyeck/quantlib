# D-slick-schema-full — Slick schema 全表型別/單位/溢位/索引

範圍:`src/main/scala/db/table/` 全 25 檔(~29 個 Table 定義)
判定:**BUG**(3 個真 BUG + 1 時間債 + 2 命名壞味道;其餘型別/單位/唯一索引符合學理)

白話總結:老表 `ex_right_dividend.cash_dividend` 這欄名叫「現金股利」但實際存的是「權值+息值」的股價總調整額,而且 2024-07 換資料源後同一欄語義又翻成純現金——誰拿它算股利殖利率會錯;`net_change_of_price` 的讀寫投影把同一欄寫兩次、漏掉「漲停家數」;`daily_trading_details` 的法人買賣超股數用 Int,已達上限 57%,溢位時會靜默變 0 或負數而非報錯。新表(外資持股/借券/內部人/集保/期交所)都已正確用 Long。

---

## BUG 1 — ex_right_dividend.cash_dividend 語義錯誤 + 跨期翻轉

- 學理:TWT49U 除權除息計算結果表欄序 = 前收盤 / 參考價 / **權值+息值** / 權息別...。純除權(配股)現金股利應為 0。還原因子 = 參考價/前收盤。
- 實作:legacy 解析取 `values(5)`=權值+息值 存入 cash_dividend(`TradingReader.scala:333`);MOPS 月表解析(2024-07+)改存 totalCash 純現金、pre/ref 寫 0(`:379-394`)。
- 證據(PG 可重跑):
  - 純「權」列 legacy `avg(cash_dividend)=2.669`(應為 0)。
  - 三型 `cash_dividend ≈ closing_price_before − reference_price`,`avg_abs_gap ≤ 0.003`,`max ≤ 0.54`(=交易所參考價四捨五入)。→ 存的是總調整額。
  - MOPS_era `pre_close=0` 的 2,456 筆息列語義=純現金;59 筆 MOPS 權列 cash=0 且 pre/ref=0 → prices.py 兩條還原路徑皆落空 → 該股票股利未還原。
- 修法:欄更名 `total_ex_value` + docstring 註明;需真現金另解析 TWT49U 後段「最近一次申報 現金股利」;統一兩解析器語義;同步修 `prices.py:321` 註解(其公式因這個錯命名才恰好正確 = 參考價/前收盤,屬 load-bearing 錯誤)。

## BUG 2 — NetChangeOfPrice.* 投影重複/遺漏

- 學理:Slick `*` 投影每欄恰好一次、與 Table[Tuple] 逐位對應。
- 實作(`NetChangeOfPrice.scala:51`):`limitUpOverallMarket` 出現兩次,`limitUpStocks`(line 27,漲停家數)缺席。
- 證據:逐字比對;寫入時 limit_up_stocks 永不落庫、第 6 tuple 值覆寫入 limit_up_overall_market;讀取第 6 位回傳 limit_up_overall_market。潛伏:表未列入 `Task.createTables`、pg_tables 0 rows(死表)。
- 修法:line 51 第 6 位改 `limitUpStocks`;若不用則連 reader/setting 刪除。

## BUG 3 — DailyTradingDetails 股數欄 Int 溢位(靜默)

- 學理:三大法人買賣超單位為「股」,單檔單日可達數十億股;應用 Long。
- 實作:12 個 buy/sell/difference 欄 `Int`/`Option[Int]`;parser `Try(value.toInt).getOrElse(0)`(`:186,214`)→ 超界靜默寫 0;聚合 `Int+Int`(`:200,204`)→ 和超界靜默回捲負數。
- 證據(PG):`max(total_difference)=1,199,902,009`(Int 上限 57%)、`max(foreign_investors_total_sell)=923,968,291`。對照組:ForeignHoldingRatio/SblBorrowing/InsiderHolding/TdccShareholding 股數欄皆已用 Long。
- 修法:12 欄 Int→Long(含 case class + parser `.toLong` + 聚合);`ALTER TABLE ... TYPE bigint` 維持 FRM parity。

## SUSPECT — 死 schema(時間債)

7 個定義未物化:NetChangeOfPrice、MarketSummary、QuarterlyReport、IncomeStatementIndividual、CashFlowsIndividual、ConciseIncomeStatementIndividual、CompanyInformation(空 stub)。pg_tables 皆 0 rows;`Task.scala:55-77` 僅註冊 23 表。修法:不用則刪(表/reader/setting 三處),CompanyInformation 空類直接刪。

## OK — 財務 EAV value=Double(合理近似)

`max(abs(value))` balance_sheet=1.43e10、concise_bs=1.11e11,均 ≪ 2^53(9.007e15),整數無精度損失。無需修。

## SUSPECT — 命名壞味道(cosmetic)

- `operating_revenue` 三個 `%%` 欄名尾帶多餘 `)`(`OperatingRevenue.scala:38/40/46`)。
- `ETF.scala:28 def index` 遮蔽 Slick `Table.index`(靠 arity 重載共存,可編譯)。
- `DailyTradingDetailsRow.totalDifferenceInt`(`:77`)名不符 column `totalDifference`(靠 mapTo 位置對應)。
皆不影響數值,低優先更名。

---

### 已核對且符合學理的項目(無偏差)

- 唯一索引鍵完整:DailyQuote/StockPER/Margin/ExRight/Index(market,date,code/name)、財報 EAV(market,[type,]year,quarter,code,title)、Taifex(date,contract,[investor/month])、Insider 6 欄複合鍵、Tdcc(date,code,tier)——皆正確涵蓋去重維度。
- MarginTransactions 用 Int:實測 max 融資餘額 1,095,338(單位=張),遠低於 Int 上限,合理。
- DailyQuote.tradeVolume/tradeValue、MarketSummary、Foreign/Sbl/Insider/Tdcc/Taifex 股數與金額欄皆正確用 Long。
- 日期一律 `LocalDate`;民國/西元轉換在 reader 處理,型別無誤。
