# 資料稽核 — 確認 BUG 追蹤表(2026-07-23)

共 110 個確認 BUG。標 🔴LIVE 者直接影響現役 S 策略,優先修。

| # | 單位 | 問題 | 影響 live | 狀態 |
|---|---|---|---|---|
| 1 | A-daily_quote | data/daily_quote/tpex/2017/2017_4_17.csv 是下載到一半就斷的檔案,導致 8923 時報~9962 有益共 21 檔上櫃股票在 2017-04 |  | ⬜ |
| 2 | A-daily_quote | 公司名稱的內部空白被抹掉:TradingReader.scala:112 與 :133 對整列做 .replace(" ", "")(本意是清數字欄的空白),連 company_n |  | ⬜ |
| 3 | A-daily_quote | 2009-12-12(週六)的 TWSE 原始檔內容其實是 2009-12-18 的資料——與 C-daily_quote 是同一件事,本單位用獨立解析器複現。A 維要補的是一條方 |  | ⬜ |
| 4 | A-daily_trading_details | TWSE 13 欄世代(2012-05-02..2014-11-28)自營商三欄整組往後轉一格:dealers_total_buy 存到淨額、dealers_total_sell  |  | ⬜ |
| 5 | A-daily_trading_details | TPEx 12 欄世代(2007-04-23..2014-11-28)dealers_difference 抄成「自營商買股數」,真正的「自營淨買股數」從未存進資料庫。期間 602 |  | ⬜ |
| 6 | A-daily_trading_details | 23 個日期存的是別天的資料,共 24,566 列。其中 5 個是真交易日(2023-06-14、2023-10-06、2025-11-12、2026-02-05、2026-04- |  | ⬜ |
| 7 | A-daily_trading_details | 超過 int32(21.47 億)的股數會靜默變成 0。全庫掃到 5 個這種原始值,全在 00403A 主動統一升級 50,造成 2026-05-12 的 dealers_diff |  | ⬜ |
| 8 | A-daily_trading_details | TPEx 事後修正的投信數字永遠回填不進來:2024 上半年 80 個交易日、170 列的投信買/賣/買賣超與三大法人合計停在修正前的版本。 |  | ⬜ |
| 9 | A-daily_trading_details | 雲端 Python 爬蟲 research/crawl/sources/daily_trading_details.py 硬性要求 ≥19 欄,把 TWSE 現行檔案裡合法的 17 |  | ⬜ |
| 10 | A-foreign_holding_ratio | 日期只認檔名、不看檔案內容:readForeignHoldingRatio 的 date 完全由檔名推導(TradingReader.scala:899-901 的 fileNam |  | ⬜ |
| 11 | A-foreign_holding_ratio | foreign_holding_ratio 已經不能從原始檔重生,而且不一致不會被發現:data/foreign_holding_ratio/tpex/2010/2010_1_4. |  | ⬜ |
| 12 | A-index | 8 個 twse 日期(2015-08-29、2016-05-26、2017-08-02、2018-08-04、2018-09-15、2018-10-03、2019-07-05、2 |  | ⬜ |
| 13 | A-index | 33 個 tpex 交易日、2 個 twse 交易日的指數完全查無資料,另有 2 天只落了一半的指數。tpex 2024-06-27 ~ 2024-08-12 共 32 個交易日『 |  | ⬜ |
| 14 | A-index | 『當天未公布』被寫成 0,無法與『當天真的沒動』區分。db/table/Index.scala:29-31 把 change 與 change(%) 宣告成 Double 而非 O |  | ⬜ |
| 15 | A-index | TPEx 報酬指數改名器把官方名稱弄壞。TradingReader.scala:447 對報酬區無條件做 values.head.replace("指數","") + "報酬指數" |  | ⬜ |
| 16 | A-margin_transactions | 上櫃 2007-06-01~2008-09-26(331 個交易日、135,679 列)的 short_quota 整欄裝錯東西:那 16 個月的原始檔標頭照樣印著『券限額』,但資 |  | ⬜ |
| 17 | A-margin_transactions | margin_transactions 已經無法從原始檔重建:今天的程式重跑一次會靜靜少掉 466,788 列(上櫃 2011-01-03~2014-10-30,共 948 個交易 |  | ⬜ |
| 18 | A-margin_transactions | 同一株病的第二個樣本:上櫃 2007-01-02~2007-03-30(57 天、12,944 列)的『資券相抵』重跑會被寫成 0。那五個月的原始檔尾三欄是 idx17=券限額、i |  | ⬜ |
| 19 | A-margin_transactions | 10 天的資料整天是別天的複製品,共 6,068 列。twse 2011-03-26(週六,1,000 列)裝的是 2017-12-18 的資料——六年份的前視;twse 2003 |  | ⬜ |
| 20 | A-margin_transactions | 10 個真交易日整天沒有任何融資融券資料(約 8,015 列),而且設計上永遠不會自己補。twse:2004-08-20、2007-09-06、2008-11-28、2010-08 |  | ⬜ |
| 21 | A-stock_per_pbr_dividend_yield | 19 個 TWSE 日期存的是別天的資料,共 16,447 列。TWSE bwibbu_d 端點在某些請求下會回一份固定的 2017-12-18 快照(標題就印『106年12月18 |  | ⬜ |
| 22 | A-stock_per_pbr_dividend_yield | reader 現行的 TWSE 欄數對照表與實際欄數不符,是一顆未爆彈。TradingReader.scala:734-738 的 `case 6 => (v(2), v(5),  |  | ⬜ |
| 23 | A-stock_per_pbr_dividend_yield | 13 個真交易日整天沒有估值資料。TWSE 11 天(2008-08-26、2009-12-12、2014-05-07、2016-09-09、2016-11-01、2016-12- |  | ⬜ |
| 24 | B-fscore-academic | PG view growth_analysis_ttm.f_score 的五個 Δ 項用 lag(x) = 上一季,Piotroski 定義是年度比較;定義上不是 F-Score |  | ⬜ |
| 25 | B-fscore-academic | 兩套 F-Score 實作互不一致:逐格完全相同僅 27.3%,相關 0.646,「≥5」閘門判定不一致 23.4% |  | ⬜ |
| 26 | B-fscore-academic | raw_quarterly.py 九項全部 .otherwise(0):缺資料給 0 分不是 NULL。cash_flows_progressive 最早 2009 → 第 2/4 |  | ⬜ |
| 27 | B-fscore-academic | 同一個 NULL→0 機制造成兩道隱形濾網:金融業 gross_margin_ttm 100% NULL → f8 恆 0、f9 僅 9.4% → 金融保險平均 3.00(全市場  |  | ⬜ |
| 28 | B-fscore-academic | 「F-Score ≥ N」的閘門被寫成「歷史上曾經 ≥ N」:WHERE 在 DISTINCT ON 之前,沒有收斂到最新一季。Scala 與 Python 各一處(同一缺陷類) |  | ⬜ |
| 29 | B-fscore-academic | PG matview concise_income_statement_individual 混用合併/個體報表(無 type 過濾),且累計差分的 lag() 未按年分區 → 缺 |  | ⬜ |
| 30 | B-fscore-academic | financial_index_ttm.total_assets_turnover 的平均資產分母錯用 lag(total_assets,5),同一支 SQL 的 roa 用 la |  | ⬜ |
| 31 | B-fscore-academic | growth_analysis_ttm 的 equity_multiplier_decline/increase_5y_overall 拿 lag(total_assets_tur |  | ⬜ |
| 32 | B-fscore-academic | PG view 第 5 項用長期負債『金額』而非 Piotroski 的『長期負債/平均總資產』比率;資產與負債同比例成長的健康公司被扣分,縮表的衰退公司得分 |  | ⬜ |
| 33 | B-fscore-academic | Python 版 ROA / 資產周轉率 / 槓桿比率的分母用『期末』總資產,Piotroski 用『年初』(LEVER 用平均);38.2% 的格子因此得到不同分數 |  | ⬜ |
| 34 | B-fscore-academic | rolling_sum(4) / shift(4) 是按實體列移動,不是日曆對齊;季別缺口時 TTM 跨超過 4 個日曆季、『去年同季』不是去年同季 |  | ⬜ |
| 35 | B-fscore-academic | 科目樞紐用 MAX(value) FILTER,同一格有兩個候選科目時挑數字大的那個 — 系統性挑對自己有利的數,且哪個較大會隨季別變動 → Δ毛利率被灌雜訊 |  | ⬜ |
| 36 | B-matview-5_concise_income_statement_individual | 合併報表(consolidated)與個體報表(individual)被拿來互相相減：DISTINCT ON 靠 type 字母序恆取 consolidated，但 2013 IF |  | ⬜ |
| 37 | B-matview-5_concise_income_statement_individual | 缺一季時 lag 跳到不相鄰期別：第 19-22 行只判斷 quarter=1，沒有檢查前一筆是不是 (同年, 本季−1)。橫跨多季的差額被貼上「單季」標籤，產出看起來正常的錯數字 |  | ⬜ |
| 38 | B-matview-5_concise_income_statement_individual | 非加總型科目也被無差別差分：第 19-22 行對全部 122 個 title 一律做減法，包含本質上是水準值而非流量的「換算匯率」。EPS 亦被差分，且下游確實在用。 |  | ⬜ |
| 39 | B-slick-schema | ex_right_dividend.cash_dividend 不是現金股利,是「除權息調整總額」(前收盤 − 參考價,含股票股利部分)。2,245 筆按交易所自己的分類是「純除權 |  | ⬜ |
| 40 | B-slick-schema | NetChangeOfPrice 的 * 投影重複 limitUpOverallMarket、完全遺漏 limitUpStocks(漲停股票數)。型別剛好都是 Int 所以編譯得過 |  | ⬜ |
| 41 | B-slick-schema | DailyTradingDetails 的 26 個數量欄全宣告成 Int,但來源單位是股數,已經真的溢位過:00403A 2026-05-13 的三大法人買賣超真值 −2,725 |  | ⬜ |
| 42 | B-view-3_financial_index_quarterly | 2006-2012 段的查詢結果不具決定性,而且該期間 23.3% 是幽靈列。上游 materialized_view/6_concise_financial_statement_ |  | ⬜ |
| 43 | B-view-3_financial_index_quarterly | 缺財務資料的公司反而拿到接近滿分。行 80-81 的 rank() over (partition by year, quarter order by roic) 用 Postgr |  | ⬜ |
| 44 | B-view-3_financial_index_quarterly | 單季化(累計制差分)在公司少報一季時,把『兩季合計』當成『一季』輸出。materialized_view/5_concise_income_statement_individual |  | ⬜ |
| 45 | B-view-3_financial_index_quarterly | 2005 年以前 profit 欄位裝的其實是稅前淨利。materialized_view/6_concise_financial_statement_with_titles.sq |  | ⬜ |
| 46 | B-view-3_financial_index_quarterly | 資不抵債(股東權益為負)的公司拿到資本結構滿分。行 17 equity_multiplier = total_assets / total_equity,權益為負時倍數為負;行 8 |  | ⬜ |
| 47 | B-view-3_financial_index_quarterly | 產業別取『最新一筆』回貼全部歷史,構成前視偏誤,且違反本專案明文鐵律。行 2-6 的 industry CTE 用 select distinct on (company_code |  | ⬜ |
| 48 | B-view-3_financial_index_quarterly | TTM 與滾動視窗算的是『列』不是『季』。行 23/26/28/31/34/38/40 的 rows between 3(或 19) preceding and current r |  | ⬜ |
| 49 | B-view-4_financial_index_ttm | total_assets_turnover 的「去年同期總資產」寫成 lag(total_assets, 5) = 15 個月前;同一支 SQL 的 roa 用的是正確的 lag( |  | ⬜ |
| 50 | B-view-4_financial_index_ttm | 算不出 ROIC / ROA 的公司拿到最高的品質分。PostgreSQL 的 ORDER BY 預設 ASC NULLS LAST,rank() 把 NULL 排到最後,而分數  |  | ⬜ |
| 51 | B-view-4_financial_index_ttm | 應收帳款被加兩次。上游 receivable CTE 把 28 個標題全部 sum,而 MOPS 完整報表同時揭露同一筆的「毛額」與「淨額」兩行,兩行都被加進去。連帶把 days_ |  | ⬜ |
| 52 | B-view-4_financial_index_ttm | 「最近四季」其實是「最近四列」。全檔 20 餘處 rows between 3 preceding and current row 是列位移,沒有任何「這四列是否為連續四季」的檢查 |  | ⬜ |
| 53 | B-view-4_financial_index_ttm | 2006-2012 年同一支查詢跑四次得到四個答案。上游 concise_financial_statement_with_titles 的 858 個鍵有 2/4/8/64 倍重 |  | ⬜ |
| 54 | B-view-4_financial_index_ttm | 存貨與預付款缺料被 coalesce 成 0,速動比率退化成流動比率並拿到滿分。「不知道存貨」被當成「沒有存貨」。 |  | ⬜ |
| 55 | B-view-4_financial_index_ttm | 2009 年以前 cash_flow 分項恆為 5 分,cbs 跨期完全不可比——「2010 年的 cbs 比 2020 年低」是資料補齊程度的差別,不是體質差別。 |  | ⬜ |
| 56 | B-view-5_growth_analysis_ttm | 上游 concise_financial_statement_with_titles 對 55 家公司的 858 個 (company_code, year, quarter) 產 |  | ⬜ |
| 57 | B-view-5_growth_analysis_ttm | 權益乘數的 5 年比較拿錯欄位(複製貼上錯):5_growth_analysis_ttm.sql 行 112-113 的 equity_multiplier_decline_5y_ |  | ⬜ |
| 58 | B-view-5_growth_analysis_ttm | f_score 在 2010 年以前結構性封頂在 6 分。現金流量資料 2010 才有:2005-2009 的 ocf 100% NULL,行 4 的 case when ocf  |  | ⬜ |
| 59 | B-view-5_growth_analysis_ttm | revenue_growth_rate_increase_5y_overall(行 38-46)的括號位置錯,門檻無聲多加 20 個百分點。寫法是 (rev/lag(rev,4)  |  | ⬜ |
| 60 | B-view-5_growth_analysis_ttm | 1.2 倍的「比五年前進步 20%」測試在基期為負時方向相反,把惡化判成進步。受影響的是 profit_margin(行 66)、operating_margin(行 79)、ro |  | ⬜ |
| 61 | B-view-5_growth_analysis_ttm | 7 個 *_growth_rate 欄位的正負號與另外 15 個相反,但名字看不出來。行 567-607 的 days_sales_of_inventory / days_sale |  | ⬜ |
| 62 | B-view-5_growth_analysis_ttm | lag(n) 是「往前 n 列」不是「往前 n 季」。所有 window 只寫 order by company_code, year, quarter,沒有任何日曆對齊檢查。缺季 |  | ⬜ |
| 63 | C-bs_concise_raw | 2026Q1 資產負債表只有 539 家公司(twse 311 / tpex 228),對照 2025Q4 的 1,950 家(twse 1069 / tpex 881),缺 1, |  | ⬜ |
| 64 | C-bs_concise_raw | 同一根因造成的歷史殘留:至少 13 個季度因為『下載時間離截止日太近』而永久少了一批晚申報的公司,且永遠不會補。twse 2023Q2 少 111 家、twse 2025Q2 少  |  | ⬜ |
| 65 | C-capital_reduction | research/prices.py 的減資因子護欄 0.05<f<5.0 把 15 筆真實的大比例減資(台股「彌補虧損」型可減到只剩 2.5%,因子最高 40.04)當髒資料丟掉 |  | ⬜ |
| 66 | C-capital_reduction | 減資表的覆蓋起點比報價晚 7 年:twse 從 2011-01-25、tpex 從 2013-01-16,但 daily_quote 從 2004-02-11 / 2007-07- |  | ⬜ |
| 67 | C-capital_reduction | TWSE TWTAUU 端點會對『明明有事件』的區間回空(2 bytes),而爬蟲把空回應當成『那段沒事件』照樣存檔並推進游標,所以漏抓永遠不會自癒。2024-05~2025-12 |  | ⬜ |
| 68 | C-cf_progressive_raw | 2026Q1 幾乎整季不見:只有 544 家公司,比 2025Q4 少 1,719 家,其中 1,410 家在 2026-01-01~06-30 有 ≥80 個交易日、成交值合計約 |  | ⬜ |
| 69 | C-cf_progressive_raw | 同一病灶反覆發作,歷史上已污染 2023Q2、2024Q1、2025Q2:整批金融業(金控/銀行/證券/保險)與 KY 股缺料,因為它們的申報期限比一般公司晚,而爬蟲在期限前就抓了 |  | ⬜ |
| 70 | C-cf_progressive_raw | 缺一季不是留下空值,而是把下一季算成兩季合計。現金流量表是年度累計數,research/strat_lab/raw_quarterly.py:176-182 用「本季累計 − 上季 |  | ⬜ |
| 71 | C-daily_quote | 5 個真的有開市的交易日,daily_quote 一列都沒有:twse 2021-08-18、2025-08-15、2026-04-29、2026-05-28,tpex 2023- |  | ⬜ |
| 72 | C-daily_quote | twse 2009-12-12(星期六)是幽靈交易日:772 列與 2009-12-18 逐欄完全相同(TWSE 對非交易日的請求把要求的日期原樣印在標題上,卻送回另一天的內容)。 |  | ⬜ |
| 73 | C-daily_quote | 69 筆 right_or_dividend='權'(純配股)的除權事件在 PG 的 ex_right_dividend 裡三個數值欄(closing_price_before / |  | ⬜ |
| 74 | C-daily_trading_details | 7 個真的有開市的交易日,daily_trading_details 在 PG 與 cache 都一列都沒有:twse 2023-08-30、2025-12-22、2026-02- |  | ⬜ |
| 75 | C-daily_trading_details | cache 唯一領先 PG 的那一天(2026-07-20,twse 1,337 + tpex 926 列)由 Python 直寫路徑寫入、不落原始檔,無法用 A 維方法核對;而同 |  | ⬜ |
| 76 | C-daily_trading_details | cache 忠實複製了 PG 的兩類錯,用 cache 端獨立方法全部重現:(a) 2015 年以前自營商欄位整組錯位——twse ≤2014-11-28 的 509,410 列與 |  | ⬜ |
| 77 | C-ex_right_dividend | TWSE 2024 年除息紀錄大缺口:旺季窗口 6/22~7/14 只有 2 筆(2022 年 308、2023 年 274、2025 年 297、2026 年 305);全年公司 |  | ⬜ |
| 78 | C-ex_right_dividend | ETF 配息自 2024-06-20(twse)/ 2026-04-17(tpex)之後全數缺漏。twse ETF 列數 2023 年 143、2024 年 86(全在上半年)、2 |  | ⬜ |
| 79 | C-ex_right_dividend | 換 MOPS 之後股票股利(除權)完全沒有被還原:219 筆事件(twse 187 = 2024:72 / 2025:85 / 2026:30;tpex 32 = 2024:3 / |  | ⬜ |
| 80 | C-ex_right_dividend | PG 對『事後更正的公告』永遠不更新,停在第一次匯入的舊值。目前抓到 1 筆:tpex 6185 幃翔 2026-07-02,PG 記 0.39,原始檔寫 0.39261977(c |  | ⬜ |
| 81 | C-foreign_holding_ratio | TPEx 2010 年整年 319,124 列(全表 3.88%、tpex 2010 覆蓋率 100%)不是 2010 年的資料,而是 2026-04-24 的快照被複製到 361 |  | ⬜ |
| 82 | C-foreign_holding_ratio | 4 個真的有開市的交易日,twse 與 tpex 兩市場都整天 0 列,而且原始檔連下載都沒下載過:2021-08-18(三)、2025-08-15(五)、2026-04-29(三 |  | ⬜ |
| 83 | C-industry_taxonomy_pit | 產業標籤沒有 point-in-time 語義:MOPS 月營收彙總檔是下載當下即時渲染的,產業別用的是「渲染當下」的分類。全表 452,891 列中只有 79,766 列(17. |  | ⬜ |
| 84 | C-industry_taxonomy_pit | 具體失真樣本:電子類分拆(半導體業/光電業/電子零組件業…)在我們的表出現在 source_ym=200606(effective_date 2006-07-13),但真正的分類變 |  | ⬜ |
| 85 | C-industry_taxonomy_pit | money path 直接吃到:S 策略唯一真源 research/apex/strategy_s.py 用這張表算 accel_rel(營收加速度減同業中位數),特徵起點 DS= | 🔴LIVE | ⬜ |
| 86 | C-industry_taxonomy_pit | cache 與 PG 對不上 70 列,而且產業標籤本身會被來回改寫:cache 獨有 63 列、PG 獨有 7 列(全在 2026-04..06),共同鍵上 14 列的 raw_ |  | ⬜ |
| 87 | C-industry_taxonomy_pit | 來源只抓 MOPS t21sc03(合併營收)、沒抓 t21sc04(個別營收),導致 2013 年 IFRS 上路後只申報個別營收的公司整片消失,分類表因此出現公司層的洞:201 |  | ⬜ |
| 88 | C-industry_taxonomy_pit | 每日增量重算會把索引弄丟:cache_tables.py 建的兩個索引在 crawl 的 DROP TABLE + CREATE TABLE 之後永久消失,且不會自己回來。純效能問 |  | ⬜ |
| 89 | C-is_progressive_raw | 季報原始檔在申報期限前被抓一次就永久凍結,造成整批公司缺料。受害季與規模(缺料且當年可交易的家數/當年成交值):2023Q2 缺 149 家(121 家可交易,NT$6.28 兆) |  | ⬜ |
| 90 | C-is_progressive_raw | 缺一季不是留下空值,而是把下一季算成兩季合併值。財報是年度累計數,下游 research/strat_lab/raw_quarterly.py 用「本季累計 − 上季累計」還原單季 |  | ⬜ |
| 91 | C-is_progressive_raw | 重抓舊季會抹掉期間已下市的公司,把存活者偏誤寫進歷史資料。2023Q1 與 2023Q4 的原始檔於 2026-04-23 被重新下載,TWSE 彙總報表只回傳「當下仍在市」的公司 |  | ⬜ |
| 92 | C-is_progressive_raw | research/strat_lab/raw_quarterly.py 的 op_income 別名清單漏收 2013 年以前的舊科目名「營業淨利(淨損)」,導致 2005-201 |  | ⬜ |
| 93 | C-margin_transactions | 11 個真的有開市的交易日,margin_transactions 在 PG 與 cache 都一列都沒有,估計缺 8,618 列:twse 2002-10-24、2004-08- |  | ⬜ |
| 94 | C-margin_transactions | cache 忠實複製了 PG 的兩類日期汙染,共 6,068 列:(a) 8 個非交易日有整天的資料——tpex 2012-08-02、2014-07-23、2015-07-10、 |  | ⬜ |
| 95 | C-margin_transactions | A 維查出的 short_quota 欄位錯位(tpex 2007-06-01~2008-09-29,135,679 列)原封傳進 cache,用 cache 自己的資料就能重現: |  | ⬜ |
| 96 | C-market_index | 8 個 twse 日期共 947 列整片是別的日期的資料(每一檔指數的 close 與 change 都與來源日一字不差),cache 忠實複製:2015-08-29(週六,93  |  | ⬜ |
| 97 | C-market_index | 髒掉的 TAIEX 會在每次重建 cache 時被二次加工,汙染 cache 自己生的衍生表 taifex_futures_daily_factors 的期貨基差:5 個平日幽靈日 |  | ⬜ |
| 98 | C-market_index | 36 個真的有開市的交易日整天沒有指數資料,PG 與 cache 都沒有,估計缺約 3,055 列:tpex 2024-06-27~2024-08-12 連續 31 個交易日(≈2 |  | ⬜ |
| 99 | C-market_index | 加權指數(TAIEX)這條線在 2019-04-29 有一天洞,因為那天交易所把它印成別的名字。2019-04-29 是 TWSE 換指數名冊的那一天,當天檔案裡大盤指數叫『加權股 |  | ⬜ |
| 100 | C-market_index | 有 4 天交易所回的是 2019-04-29 改名前的『舊名冊』,害 160 檔現行指數在 2026 的兩天各多一個洞。TWSE 在 2019-04-29 改了一批指數名(電子類指 |  | ⬜ |
| 101 | C-market_index | 從 A 維原封傳進 cache 的兩件事(cache 忠實搬運,但值本身有問題):(a) 1,578 列『當天未公布』被寫成漲跌 0——close 是 NULL(正確)但 chan |  | ⬜ |
| 102 | C-operating_revenue | PostgreSQL 永久漏掉「15 號之後才申報」的公司:twse/consolidated 金融保險業從 2026-01..03 的 32 家掉到 2026-04/05/06  |  | ⬜ |
| 103 | C-operating_revenue | 同一張表有兩條會互相覆蓋的寫入路徑,內容取決於今天先跑了哪一條,回測不可重現。Python 爬蟲 research/crawl/sources/operating_revenue. |  | ⬜ |
| 104 | C-operating_revenue | 重抓舊月份會刪掉已下市公司的歷史列,製造生存者偏誤。3426 台興(最後交易 2026-06-01)、4987 科誠(2026-05-20)、6806 森崴能源(2026-06-2 |  | ⬜ |
| 105 | C-operating_revenue | 6 碼 TDR(存託憑證)被 Python 爬蟲的代號正則吃掉,912000 晨訊科-DR / 910069 / 912398 的月營收在被 Python 重寫過的月份整列消失。 |  | ⬜ |
| 106 | C-operating_revenue | 這張表唯一的稽核腳本 research/audits/05_revenue_audit.py 一跑就爆,所以沒人在稽核它——這是上面的洞能安靜躺三個月的直接原因。CLAUDE.md |  | ⬜ |
| 107 | C-sbl_borrowing | 26 個 TWSE 原始檔裝的是別天的資料,共 26,354 列,而且已經進了 PG 和 cache。(a) 10 個真交易日內容是別天的(10,175 列),9 個是把未來寫進過 |  | ⬜ |
| 108 | C-sbl_borrowing | 32 個 (market, date) 在真交易日整天沒有資料,估計缺 32,690 列,而且都不會自己補回來。(a) 26 天被 0-byte 空檔永久蓋住(twse 25 天: |  | ⬜ |
| 109 | C-stock_per_pbr | 19 個 twse 日期存的是別天的資料,共 16,447 列;其中 10 天是真的有開市的交易日(8,605 列),7 天存的是未來的數字(前視偏誤,最遠 +9.8 年),3 天 |  | ⬜ |
| 110 | C-stock_per_pbr | 13 個真的有開市的交易日,stock_per_pbr 整天一列都沒有:twse 11 天(2008-08-26、2009-12-12〔週六補行交易日〕、2014-05-07、20 |  | ⬜ |
