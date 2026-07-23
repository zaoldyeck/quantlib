# C-taifex_futures_contract_rank — cache 一致性與缺漏

**結論(白話):** 這張表**沒有 PostgreSQL 對應表**——它是 `research/cache_tables.py`
在 cache 裡用 `taifex_futures_daily` + `taifex_futures_final_settlement`(兩者都與 PG
逐筆一致)**現算出來的衍生表**。算出來的**每一個「值」都對**(OHLC/結算/量/未平倉和來源
逐格零差異、筆數對得起來),但**「排序」有一個真 bug**:把不是月份合約的兩種東西——
**月間價差(calendar spread,如 `202606/202609`)**與**週結算合約(weekly,如 `202605W4`)**
——混進來一起排「近月/次近月」名次(`month_rank`)。後果是**次近月名次有 7% 的交易日
指到價差合約**,下游 `taifex_futures_daily_factors` 的「次月價差」欄因此變成 **≈ -99% 的
垃圾值**,而這個值**正被 `research/futures/strategies.py` 當交易訊號在用**。

另外,整張表**缺 2026 年 1-2 月整整兩個月、以及 2026-05-22 至今**的資料——這一段和
`taifex_futures_daily`(base)是**同一個洞**(爬蟲根本沒抓進來,不是 cache 的錯),已在
姊妹單位 `C-taifex_futures_daily` 記錄過,此處只是忠實繼承。

**能不能信?** 值可以信、覆蓋到的日期可以信;但**「近月排序」在 rank≥2 不能信**
(價差合約污染),**用到 `tx_next_term_spread` / `tx_next_term_spread_pct` 的任何研究都拿到
髒訊號**;而且 2026 年有兩段大洞。

---

## BUG 1(主,本單位獨有的新發現):價差 / 週結算合約污染 `month_rank`,並把下游「次月價差」訊號搞成垃圾

### 病灶
`research/futures/taifex.py:56` 的過濾條件
`regexp_matches(d.contract_month, '^\d{6}')` **沒有收尾錨點 `$`**。它的原意是「只留
YYYYMM 的月份合約」,但因為只要求「開頭是 6 位數字」,於是:

- 價差合約 `202606/202609`(開頭 `202606`)→ **通過**
- 週結算 `202605W4`(開頭 `202605`)→ **通過**

而 `month_key = TRY_CAST(regexp_extract(d.contract_month, '^(\d{6})',1) AS INTEGER)`
(line 39)只抓前 6 碼,所以價差 / 週合約會拿到**和月份合約相同的 month_key**,一起參加
`row_number() OVER (PARTITION BY date, product ORDER BY month_key, contract_month)` 的排名。

### 規模(cache 實測)
148,031 列裡:**月份合約 141,175、週合約 3,488、價差合約 3,368**(三者相加 = 148,031,對得起來)。

| month_rank | 該名次總列數 | 其中價差污染 | 佔比 |
|---|---|---|---|
| 1(近月) | 26,645 | **0** | 0.0% |
| 2(次近月) | 26,645 | **1,833** | 6.9% |
| 3 | 26,645 | 678 | 2.5% |

- **rank=1（近月）永遠不會是價差**:因為月份合約 `202606` 字典序排在同月價差
  `202606/202609` 之前(前綴規則),所以近月/連續序列(`taifex_futures_continuous`,
  它只吃 `month_rank=1`)**對 TX 是乾淨的**。
- **但 rank=1 有 985 列是「週合約」(全是 MTX,2013-08-23~2026-05-21)**:因為週合約到期
  日可能比當時的近月月份合約還近(例:2026-05-21 已過 5 月結算,近月月份是 `202606`,
  而週合約 `202605W4` 的 month_key=202605 < 202606,於是週合約排 rank=1)。TX 在 rank=1
  沒有週合約(0 列),所以 **TX 連續序列不受影響,MTX 連續序列會多出每週換月的假 roll**。
- **rank=2（次近月)是重災區**:1,833 列指到價差合約。

### 具體案例(TX,2026-05-21,依 month_rank 排序)
```
rank=1 cm='202606'        close=41473  vol=54092   <- 真近月月份合約 ✓
rank=2 cm='202606/202609' close=236    vol=37      <- 價差合約!污染
rank=3 cm='202606/202612' close=733    vol=1       <- 價差合約!污染
rank=4 cm='202607'        close=41474  vol=687     <- 真「次近月」被擠到第 4 名
```

### 下游污染(這才是會咬人的地方)
`taifex_futures_daily_factors`(同檔 line 141-241)的 `second_month`
= `WHERE month_rank = 2`,於是把**價差合約當成「次月合約」**:

- `tx_next_contract_month` 有 **515 / 6,875 列(7.5%)** 是價差合約(如 `202606/202609`)。
- `tx_next_term_spread = 次月價 − 近月價` = `236 − 41503 = -41267`(應該是幾十點的基差)。
- `tx_next_term_spread_pct`:**乾淨列範圍 [-0.037, +0.056]**(正常基差),
  **污染的 515 列範圍 [-1.0, -0.9906]**(≈ -99%,因為把價差報價 236 拿去除以近月價 41503)。
- 全表 |值| ≥ 0.5 的「不可能基差」正好 = **515 列**。

### 消費端(確認會咬人,不是死碼)
`research/futures/strategies.py`:
- line 209 `SELECT ... f.tx_next_term_spread_pct`
- line 243 `WHERE r.month_rank = 1`(近月;MTX 會取到週合約)
- line 323-324 `tx_next_term_spread_pct` 的 **63 日滾動均值 / 標準差**
- line 371 拿它做 **z-score 訊號**:`(tx_next_term_spread_pct − term_mean63) / term_std63`

也就是說,那 515 個 ≈ -99% 的離群值會把 63 日均值/標準差整段拉爆,z-score 訊號在價差
污染日前後全部失真。

### 建議修法(fix)
把 `taifex.py:56` 的過濾錨死成**只收純月份合約**:
`regexp_matches(d.contract_month, '^\d{6}$')`(或等價地加
`AND d.contract_month NOT LIKE '%/%' AND d.contract_month NOT LIKE '%W%'`)。這會移除 3,368
價差 + 3,488 週合約,留下 141,175 純月份合約,`month_rank` 才是真正的月份階梯。改完
**重建 cache** 並回跑 `research/futures/tests/test_taifex_futures.py`;另加一條守護:
`taifex_futures_contract_rank` 不得出現 `contract_month LIKE '%/%'`,且
`taifex_futures_daily_factors` 的 `ABS(tx_next_term_spread_pct) < 0.5`(先拿現況資料跑必須
紅、修完轉綠)。

> 週合約是否要保留可討論(它確實是更近到期的真合約,價格和月份合約幾乎一樣),
> 但**價差合約一定要移除**——它的「close」是價差報價、不是指數價位,混進來在數學上
> 就是錯的。此表 docstring 明寫用途是「front contract by nearest listed **contract month**」,
> 依此語意兩者都應排除。

---

## BUG 2(覆蓋缺口,繼承自 base;與 `C-taifex_futures_daily` 同源)

期貨資料本身有兩段大洞,**衍生表忠實地也沒有**(不是 cache 同步問題):

1. **2026-01-02 ~ 2026-02-26**:整整 1、2 月缺(33 個交易日)。TX 日期由 2025-12 直接跳到
   2026-03,同窗 `daily_quote(twse)` 有 33 個交易日、`is_trading_day` 皆 True。
2. **2026-05-22 至今**:TX 最大日期 = 2026-05-21,但 `daily_quote(twse)` 已到 2026-07-20,
   中間 ~40 個交易日期貨全無(且持續擴大)。

另有 3 個週六(2015-08-29、2018-08-04、2018-09-15)base 也是 0 列——TWSE 補行交易日但
TAIFEX 一般盤未開,屬合理(非缺陷)。

**根因**:base 爬蟲未下載這些原始檔(見姊妹單位 `C-taifex_futures_daily`,raw 目錄缺
`2026_1.csv / 2026_2.csv / 2026_6.csv / 2026_7.csv`,`2026_5.csv` 為半月檔)。
**fix**:用官方端點 `https://www.taifex.com.tw/cht/3/futDataDown`
(`application.conf` taifex.futuresDaily)補抓 `2026-01-02~2026-02-26` 與 `2026-05-22` 起至最新,
匯入 PG 後重建 cache。**補抓由主流程統一安排,本稽核不下載**。此缺口跨所有 taifex_* 單位
共用,建議統一在 base 端修一次。

---

## OK(負結果,已逐項查證乾淨)

- **沒有 PG 對應表 = 正常**:此為 cache-only 衍生表(`cache_tables.py:145` 呼叫
  `build_taifex_futures_tables`)。它依賴的兩張 base 表 `taifex_futures_daily`
  (5,780,185 列)、`taifex_futures_final_settlement`(3,152 列)**cache 與 PG 筆數/日期範圍
  完全相同**(逐筆一致見姊妹單位 `C-taifex_futures_daily`)。
- **衍生筆數對得起來**:照 builder 的過濾條件從 base 重算 = **148,031**,與實表 148,031
  **完全相等**。
- **值忠實複製**:衍生表 join 回 base,OHLC/settlement/volume/open_interest **0 筆不符**、
  **0 筆孤兒(找不到 base 來源)**;`final_settlement_price` join **0 筆不符**。
- **異常值掃描(限純月份合約)全 0**:負/零價、high<low、close 超出 [low,high]、負量、
  負未平倉、未來日期、null month_key、量>0 但三價全 null——**皆 0**。
  (價差合約的 close 可以是小值/負值,那是價差報價的正常現象,故異常掃描要排除價差列,
  否則會誤判——這也正是 BUG 1 把兩種價格尺度混進同欄的副作用。)
- **`month_rank` 結構完整**:每個 (date, product) 的名次都是連續 1..N,無跳號/重號;
  (date, product, contract_month) **無重複鍵**。
- **2 個 base 日期(2005-02-04/05)不在衍生表 = 正確排除**:那兩天 base 對 5 檔指數期貨
  (TX/MTX/TMF/TE/TF)**任何盤別都 0 列**,故衍生表正確地沒有它們(非漏算)。
- **反向日期缺口是假警報**:TX 有、而 `market_index` 沒有的 2,645 個日期,其中 2,644 個是
  `market_index` 自己 2009 年才開始(TX 從 1998);對 `daily_quote`(2004 起)同理。兩個
  日曆的歷史都比 TX 短,不是期貨問題。

---

## 查了什麼(供覆核涵蓋度)
1. Schema:`taifex_futures_contract_rank` 不在 PG(`\d` 查無此表)→ 確認為 cache 衍生表;
   讀 `cache_tables.py:145-148` 與 `futures/taifex.py:23-67` builder 全文。
2. base 一致性:cache vs PG 對 `taifex_futures_daily`(5,780,185)、
   `taifex_futures_final_settlement`(3,152)筆數與日期範圍逐項比對,相等。
3. 衍生筆數重算:照 builder 五個過濾條件從 base 重建 count = 148,031 = 實表。
4. contract_month 形狀分佈:月份 141,175 / 週 3,488 / 價差 3,368(LIKE 分桶,三者相加對齊)。
5. 值忠實性:衍生 join base 對 8 欄逐格 diff(0 不符)、孤兒(0)、final_settlement(0)。
6. 具體抽樣:3 日(2020-03-19 / 2023-06-15 / 2026-05-21)× TX/MTX rank=1 顯式 dump,值全 match。
7. month_rank 污染量化:rank 1/2/3 的價差污染列數(0 / 1,833 / 678);週合約在 rank=1
   共 985(全 MTX)。
8. 下游傳播:`taifex_futures_daily_factors` 的 `tx_next_contract_month` 含 `/` = 515 列;
   `tx_next_term_spread_pct` 乾淨 vs 污染分佈([-0.037,0.056] vs [-1.0,-0.9906])。
9. 消費端盤點:grep 出 `strategies.py:209/323/324/371` 確實用 `tx_next_term_spread_pct`
   做 63 日 z-score 訊號;`ev48/ev51/f10` 只用 `foreign_*_net_oi`(不受此 bug 影響)。
10. 覆蓋缺口:以 `daily_quote` + `market_index` 兩套交易日曆交叉比對,定出 Jan-Feb 2026
    與 post-2026-05-21 兩段真缺口 + 3 個週六(合理)+ 2 個 base 日期正確排除。
11. 異常掃描 + month_rank 結構(連續性/重複鍵)+ 反向缺口成因(日曆歷史較短)。
