# D-crawler-scala — 爬蟲正確性稽核(Crawler / Http / Task / Job)

**判定:BUG**（1 個已證實、仍在發生的無聲資料遺失 bug；2 個 SUSPECT;3 項 OK）

**白話總結:** 爬蟲的「休市判定」分不出「交易所真的休市」和「伺服器回了個空包」——
只要回應小於 50 bytes 就無條件當成休市、寫 0-byte sentinel,全程沒有任何「這天到底
有沒有開盤」的正向查核。結果在 **27 個真實交易日**(近至 2026-03-31、2026-04-01)把
TWSE 那半邊的融資融券 / 本益比 / 三大法人 / 指數整段標成「沒交易」並永久丟棄,而且
因為 `getDatesOfExistFiles` 把 sentinel 當「已完成」,永遠不會自己重抓。DB 已證實這些
日子 twse rows = 0。民國換算、發佈時刻閘、有界重試、價格源的檔名-內容日期驗證都正確。

範圍:`src/main/scala/Crawler.scala` + `Http.scala` + `Task.scala` + `Job.scala`
(判定所需的 `setting/Detail.scala`、各 `*Setting.scala`、`util/Helpers.scala`、
`docs/data/twse_publish_times.md` 一併對照)。

---

## Finding 1 — BUG:0-byte sentinel 無正向休市證據,把交易日誤判休市 → 永久遺失 TWSE 半邊

**位置:** `Crawler.scala:721-735`(`isMarketHolidayResponse`)+ `Crawler.scala:659-669`
(`downloadFile` 休市分支)+ `Task.scala:569-579`(`pullDailyFiles` 未套交易日閘)。

**學理/政策定義(出處):** 依專案自訂政策 `docs/data/twse_publish_times.md` 的
「sentinel(無資料日)規則」與 `CLAUDE.md`「sentinel 規則」:0-byte sentinel = 「交易所
親口說該日無交易」的**正向證據**,並**同時是我們的休市日曆**(颱風假無法從星期幾推得)。
因此判休市的正確依據是「交易所回應無資料 **且** 該日非交易日」;判斷交易日的 ground truth
就是 `daily_quote`(程式已有 `loadTwseTradingDays()` 讀它)。不確定一律 `deferred` 刪檔重抓
(寧晚勿錯)。**位元組數不是休市的證據。**

**程式實作:** `isMarketHolidayResponse` 對任何 `size < 50` 的回應**無條件 `return true`**
(`Crawler.scala:726`),且這個檢查排在 per-source `validate()` **之前**
(`downloadFile` 659→662→671),所以 <50B 的回應直接進休市分支、`validate()` 永遠不會跑
—— `details` 的 `minDataRows=100`、`index` 的表頭日期驗證都被短路掉。休市分支唯一的閘是
`isSentinelUnsafe` 的 D+1 00:30 時間閘(只答「是不是太早、還不知道」),對「過了完備時刻、
伺服器卻回了空包」毫無防護。`pullDailyFiles`(daily_quote / margin / stock_per)也沒套用
`loadTwseTradingDays()` 的交易日過濾(`pullSbl` / `pullForeign` / `pullInsider` 有套)。
`dailyEndExclusive` 註解宣稱「over-optimistic time costs one wasted request — never data」,
對這個失效模式**不成立**。

**可重現證據:** 27 個「daily_quote TWSE 檔 >1KB(真實交易日)」卻被寫成 <50B 休市 sentinel:

| 來源 | 誤判筆數 | 近期樣本 | DB 佐證(TWSE 側) |
|---|---|---|---|
| stock_per | 11 | 2026-04-01(檔 2 bytes = `\r\n`) | stock_per twse rows = **0**(同日 TPEx 檔 66599 bytes) |
| margin | 9 | 2018-11-01(檔 4 bytes) | — |
| details | 5 | 2026-03-31(檔 2 bytes,mtime 2026-04-01) | daily_trading_details 該日 **twse=0** / tpex=890(鄰日 03-30 twse=1306) |
| index | 2 | 2026-03-12(檔 0 bytes) | index 該日 **twse=0**(鄰日 135 / 267) |

`getDatesOfExistFiles` 把這些 sentinel 當「已完成」(非 `<html>`、可讀 → `Some(date)`)→
**永不重抓、無法自癒**。對照組:`daily_quote`(端點較穩)0 例;但**有日期驗證的 `index`
仍有 2 例**,直接證明 `size < 50` 規則會短路掉 `validate()`,連被驗證的來源都會漏。

**修法:**
1. sentinel 需**正向休市證據**:在 `downloadFile` 休市分支(與 `isMarketHolidayResponse`
   的結果之外)加入「檔案日期是否 ∈ `loadTwseTradingDays()`(daily_quote ground truth)」
   比對——若該日為已知交易日,**禁止寫 sentinel**,一律 `deferSameDayNoData`(刪檔重抓)。
2. 把 per-source `validate()` 移到「`size<50` 判休市」**之前**:`details` / `index` 的
   `validateCSV*` 已能正確把近空檔判 `Invalid` → delete + retry;`margin` / `stock_per`
   這類無 `validate` 者以交易日比對兜底。
3. `pullDailyFiles` 補上與 `pullSbl` 相同的 `tradingDays.contains(d)` 過濾。
4. 刪除這 27 個誤判 sentinel(讓 `getDatesOfExistFiles` 重新視為缺、下次重抓)。

---

## Finding 2 — SUSPECT:SBL 22:30 閘落在「第二次更新起點」而非「完成後」,可能抓到 partial

**位置:** `Task.scala:463`(`pullSbl` → `dailyEndExclusive(LocalTime.of(22, 30))`)。

**學理/政策定義(出處):** `docs/data/twse_publish_times.md`(第 60-63 行,TWT93U 官方原文):
借券餘額「每日晚間執行**二次**更新,更新時間分別約為 20 時 30 分及 22 時 30 分,**實際視日結
作業完成時間可能有所異動**」。即 22:30 是第二次更新的**起點**、完成可能更晚。發佈閘應在
第二次更新**完成後**才抓(政策定調:D+1 開市前唯一完整更新)。

**程式實作:** `now ≥ 22:30` 即把 `endExclusive` 設為 D+1、納入當日 D。`SBL` twse `validate`
只檢查 schema(關鍵字「借券賣出」「代號」、≥20 rows),**不驗證數值是否為最終值**;20:30
partial 版 schema 完全合法。

**證據:** 同日落在 `[22:30, 實際完成)` 之間的 run 會抓到 20:30 partial → schema 通過 →
存為正常檔 → 記為完成 → 永不重抓 → **永久 partial**。partial 外觀與 final 相同、無法事後從
磁碟偵測,故列 SUSPECT。doc 自身已把舊 `21:30` 標為 bug,改 `22:30` 只縮小、未關閉此窗;
實務靠 D+1 07:20 loop 迴避,但**程式允許**同日 22:30+ 捕捉。

**修法:** 要同日抓 SBL 就把閘門移到已知完成上界(如 23:30 並註明證據等級),或維持政策
「只在 D+1 完整更新」(讓 SBL 的 `publishAfter` 效果強制 D+1);更穩健者對 SBL 加「第二次
更新完成」的內容指紋(notes 內更新時間戳)比對,partial 則 defer。

---

## Finding 3 — SUSPECT(latent):daily_quote/margin/stock_per 去重只看 TWSE,TPEx 缺口會被遮蔽

**位置:** `Task.scala:220-229`(`pullDailyQuote`)/`318-323`(`pullMarginTransactions`)/
`336-341`(`pullStockPER_PBR_DividendYield`)→ `pullDailyFiles` `569-577`。

**學理定義:** 去重(skip-already-downloaded)的正確判準是「**所有會抓的市場都就緒**才算
完成」。`crawler.getDailyQuote`/`getMarginTransactions` 每次同時抓 twse+tpex,故完成判定應
跨兩市場(intersection / `coveredBoth`),與 `pullDailyTradingDetails` 的修正一致
(`Task.scala:326-330` 註解「Intersection (not union)」)。

**程式實作:** 上述三者經 `pullDailyFiles` 傳入單一 `Setting().twse`,`existFiles` 只看 TWSE。
若某日 TWSE 成功、TPEx retry 全敗(`.recover` 刪 partial),TWSE 檔存在 → 下次 skip →
TPEx 缺口永不回補。

**證據:** 此為 `pullDailyTradingDetails` 已修正之同類 bug **未擴散**到 daily_quote/margin/
stock_per。實測 daily_quote TPEx 於 firstDate(2007-07-02)後真實缺口 = **0**
(no-file gap 0、sentinel-but-traded 0),目前未發作,列 SUSPECT(latent)。

**修法:** `pullDailyFiles` 改接受完整 `Setting`,以 `twse.getDatesOfExistFiles &
tpex.getDatesOfExistFiles`(或 `coveredBoth` per-date,處理不同 firstDate)判完成,與
`pullDailyTradingDetails` / `pullSbl` 一致。

---

## Finding 4 — OK:民國/西元轉換全部正確

`TwseDetail`(`yyyyMMdd`,西元)、`TpexDetail`(`MinguoChronology` + `y/MM/dd`,民國)、
`Task.twseDailyQuoteFileDate`(ROC 3-digit `+1911`)、`Crawler.postMopsDirect`
(`minguoYear+1911`)、`DailyQuote`/`Index` validate marker(`date.getYear-1911`)全部符合
「ROC N = 西元 1911+N」基準,無 off-by-one。抽樣 620 個 margin 檔「內容日期 vs 檔名日期」
0 mismatch 佐證西元請求與檔名一致。

## Finding 5 — OK:價格關鍵源的檔名-內容日期驗證正確且有效

`daily_quote`、`index` 以 `validateCSVHeaderDate` 檢查表頭民國日期 marker + `minDataRows`,
能擋掉 TWSE 的 silent-fallback(無資料時回傳如 2018-02-18 的錯日表頭)。margin(無 date
validate)抽樣 620 檔 0 content-date mismatch,顯示 MI_MARGN 等**不做** silent-fallback(無
資料回空而非回錯日),故 margin/stock_per 缺 date-validation 未造成「錯日存檔」;它們真正的
傷害是空回應被誤判休市(見 Finding 1),而非錯日。

## Finding 6 — OK:發佈時刻閘邏輯 + 有界重試正確

`dailyEndExclusive`:`now < publishAfter` → 排除今日;`now ≥ publishAfter` → 納入今日,
語義正確。`Helpers.retry`:預設 3 次、遞減至 0 後 `Future.failed`(前身缺 `retries` 參數 →
落 0 → else 無窮迴圈,已修為 bounded)。兩者皆刻意且正確之工程實作。

---

### 稽核方法
讀四支主檔 + `Detail`/各 `Setting`/`Helpers`/policy doc → 對每個計算式寫學理/政策定義 →
比對實作 → 以本地檔案(`data/**`)size×日期 交叉 `daily_quote` 交易日、以 `psql` 查
`quantlib` DB 逐日 rows 取可重現證據(margin/stock_per/details/index、TWSE/TPEx 分市場)。
