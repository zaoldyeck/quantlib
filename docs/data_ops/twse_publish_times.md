# TWSE 日頻資料公布時刻(第一手來源蒐證)

調查日:2026-07-15。用途:決定每日一次完整更新的排程時點(必須排在「全表齊備」之後)。
方法:官方明文優先;查無明文者標 low 並說明推論依據。

## 結論

| 表 | 端點 | 官方公布時刻 | confidence |
|---|---|---|---|
| sbl_borrowing | TWT93U | **每日晚間二次更新,約 20:30 及 22:30**(視日結作業可能異動) | high |
| margin_transactions | MI_MARGN | **次一營業日開市前公告**(法規保證;實務為 D 日晚間) | high(規則面) |
| daily_quote / index | MI_INDEX | 無明文;**下界 15:30**(外幣成交值用當日 15:30 公告匯率) | low |
| daily_trading_details | T86 | 無明文 | low |
| stock_per_pbr | BWIBBU_d | 無明文 | low |
| foreign_holding_ratio | MI_QFIIS | 無明文 | low |

**最晚(同日)= TWT93U 約 22:30。**

## 兩個會改排程的發現

1. **TWT93U 一天更新兩次(20:30 / 22:30)**,第二次才納入「原為上櫃、次日轉上市」個股。
   現行 CLAUDE.md 假設借券 21:30 → 只會抓到**第一次**更新,系統性漏掉第二次的補正。
2. **MI_MARGN 的官方保證只到「次一營業日開市前」**,不是 D 日晚間。D 日晚間抓得到是
   實務結果,非官方承諾。要「官方保證齊備」只能等 D+1 09:00 前。

`次一營業日` 語義陷阱:MI_MARGN / TWT93U 的 notes 與欄位大量出現「次一營業日限額」
「次一營業日進行額度分配」——那是**下一日的額度/狀態欄位**,不是公布延遲,勿誤讀。

## 第一手原文

### TWT93U(信用額度總量管制餘額表 / 借券賣出餘額)
來源:`https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U?response=json&date=20260714` notes
> 配合標的證券維護作業系統完成之時間點,本項資訊將於每日晚間執行二次更新作業,更新時間
> 分別約為20時30分及22時30分,實際視日結作業完成時間可能有所異動。另本項資訊執行第二次
> 更新時將納入原為上櫃次日將轉為上市交易之個股。

### MI_MARGN(融資融券餘額)
來源:證券商辦理有價證券買賣融資融券業務操作辦法(115.01.09)第 69 條
`https://twse-regulation.twse.com.tw/m/LawContent.aspx?FID=FL007121`
> 證券商每日應將委託人融資融券額度、交易明細與餘額等資料傳送證券交易所及櫃檯買賣中心,
> 有關資券相抵交割部分應分開列示。證券交易所及櫃檯買賣中心彙計後,於次一營業日開市前
> 公告融資融券餘額。

(此條同時規範櫃買中心 → TPEx 融資融券亦同。)

### MI_INDEX(每日收盤行情)——15:30 下界
來源:`https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&type=ALLBUT0999&date=20260714` notes
> 外幣成交值係以本公司當日下午3時30分公告匯率換算後加入成交金額。

### T86(三大法人買賣超日報)——無公布時刻,但不等錯帳更正
來源:`https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALLBUT0999&date=20260714` notes
> 本資訊以當日原始成交情形統計,不以證券商申報錯帳、更正帳號等調整後資料統計。

## 負結果(省下重查)

- **openapi.twse.com.tw swagger 完全沒有更新時間**:`v1/swagger.json` 143 個 path,
  全文 grep `更新|時間|每日|營業日|發布|公布` = **0 hits**;每個 endpoint 的 description
  只是 summary 重複。「TWSE OpenAPI 會註明更新頻率/時間」的前提不成立。
- **T86 不在 OpenAPI**:`/v1/fund/T86` 回 HTML 錯誤頁,swagger 無此 path。只能走 RWD。
- **報表頁面無更新時間**:MI_MARGN/T86/TWT93U/BWIBBU/MI_QFIIS 頁面僅有
  「※ 本資訊自民國XX年XX月XX日起提供」。且新站為 SPA,notes 需由 RWD JSON 取得。
- **OpenAPI 無 Last-Modified header**(只有 `Cache-Control: no-cache`),無法用 header 推公布時刻。
- **data.gov.tw metadata 只給頻率不給時刻**:`updateFrequency = {unittime:'日'}`。
- 資訊服務/盤後資訊/問答集/報表索引頁面皆無逐表時刻表。

## 未解

- T86 / BWIBBU / MI_QFIIS 的公布時刻無任何官方明文。要釘死只能實測:
  D 日傍晚起每 5 分鐘輪詢 RWD endpoint 記錄首次 `stat=OK` 的時刻,連續數個交易日取穩定值。
