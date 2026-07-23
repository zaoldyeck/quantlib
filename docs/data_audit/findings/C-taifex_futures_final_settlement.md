# 稽核報告:cache 表 `taifex_futures_final_settlement`(期貨最後結算價)

**稽核單位**:C-taifex_futures_final_settlement(維度 C:cache 一致性與缺漏)
**日期**:2026-07-23
**結論(白話)**:**這份資料可以信。** Cache 跟 PostgreSQL 逐筆一模一樣(3152 筆全對、
零差異),裡面的每一個結算價、日期、契約都對得起來,沒有負值、沒有未來日期、沒有破洞。
**唯一要知道的一件事**:這張表是「手動更新」設計的,現在停在 **2026-05-20**、比今天落後
約 2 個月;要用近期的期貨結算價,得先手動補抓再重建 cache(指令見下)。

---

## 一句話總結每個面向

| 面向 | 結果 | 白話 |
|---|---|---|
| Cache vs PG 一致性 | ✅ 位元相同 | 3152 筆雙向比對零差異,cache 是 PG 的忠實鏡像 |
| Schema | ✅ 一致 | 只少了 PG 的 `id` 流水號(不是資料),其餘四欄名稱型別全對 |
| 內部涵蓋度 | ✅ 完整 | 329 個月每月都有結算日,中間沒有破洞 |
| 異常值 | ✅ 乾淨 | 沒有負/零/NULL 結算價、沒有未來日期、價格區間合理 |
| 尾端新鮮度 | ⚠️ 落後約 2 個月 | 停在 2026-05-20,**設計如此**(手動更新),可補 |
| TMF/MTX 上市前也有資料 | ℹ️ 正確非錯 | 看似怪,其實是同標的台指結算價的合理複製 |

---

## 1. Cache 和 PostgreSQL 一致嗎?——完全一致(位元級)

不是抽 3 個日期 × 5 檔比對而已,而是**整張表 3152 筆全部逐欄雙向比對**:

- `cache` 有、`PG` 沒有的列:**0**
- `PG` 有、`cache` 沒有的列:**0**
- 3152 筆對得上 key 的列,結算價數值不一樣的:**0**
- 總筆數 3152 = 3152;日期範圍兩邊都是 1999-01-21 ~ 2026-05-20
- 逐契約筆數兩邊相同:TX / MTX / TMF 各 836、TE / TF 各 322

**同步程式**(`research/cache_tables.py:83-85`)只是把 PG 整表複製過來、刻意丟掉
`id` 自增流水號(那是資料庫代理鍵、不是資料),四個資料欄
`date / contract_code / contract_month / final_settlement_price` 型別忠實對應
(`date→DATE`、`varchar→VARCHAR`、`double precision→DOUBLE`)。**沒有漏欄、沒有漏表、
沒有型別降級。**

## 2. 時間序列有沒有洞?——內部沒有,尾巴落後 2 個月(設計如此)

**內部零破洞**:1999-01 到 2026-05 共 329 個月,**每一個月都至少有一個結算日**
(psql 用 `generate_series` 逐月比對:應有 329、實有 329、缺 0)。中間最大的幾個間隔
(41 天、40 天、其餘 35 天)全部落在 2013 年以前的「月結算」年代、而且都圍繞農曆年
——那是正常的月頻節奏,不是漏抓。2013 年以後進入「週結算」年代(每週三結算),也沒有
異常的大洞。

結算節奏本身也對:**週結算日 = 3 個契約**(TX/MTX/TMF,台指系列),
**月結算日(每月第 3 個週三)= 5 個契約**(再加 TE 電子期、TF 金融期)。
這符合 TAIFEX 的規則:電子、金融類指數期貨只有月結算。

**尾巴落後約 2 個月**:最後一筆是 **2026-05-20**,今天是 2026-07-23。中間該有的
每週結算(5/27 起)和月結算(6/17、7/15)都還沒進來。**但這不是 cache 或讀檔的問題**
——cache 和 PG 一樣停在 2026-05-20(兩邊一致),原始檔
`data/taifex/futures_final_settlement/2026/2026.html` 最後下載時間是 **2026-05-21**、
檔內最新日期就是 2026/05/20。根本原因是:**這張表被刻意排除在每日 `Main update` 之外**
(`Job.scala:49-50` 註解白紙黑字:TAIFEX 期貨是 target-driven、要靠 `pull/read taifex`
手動觸發,免得每次首跑回補 1998 年起的整年檔案「驚動」股票日更),而手動更新自
2026-05-21 之後就沒再跑過。

## 3. 異常值——乾淨

- 負或零結算價:**0**;NULL 結算價:**0**;未來日期:**0**;空的 code/month:**0**
- 結算價區間 **151.2 ~ 41485**:合理。低端是 TE/TF 這種分類指數(本來就低點數),
  高端是 2026 年 TAIEX 約 4 萬點(2026-05-20 台指結算 40080)。
- `contract_month` 全數格式正確:月契約 1631 筆是 `YYYYMM`(如 `202605`)、
  週契約 1521 筆是 `YYYYMMWn`(如 `202605W2` = 2026 年 5 月第 2 週),**0 筆畸形**。
  (`W` 尾綴是 TAIFEX 週契約的正式命名,不是髒資料。)

## 4. 一個「看起來像錯、其實是對」的現象(記下來免得下一個人再查一次)

**TX / MTX / TMF 三個代碼每個結算日的結算價完全一樣,而且三者都從 1999 年就有資料**
——即使小型台指(MTX)2001 年才上市、微型台指(TMF)2022 年才上市。

這**不是** bug。TAIFEX 的最後結算價網頁每個結算日只公布**一個**台股加權指數結算價,
reader(`TradingReader.scala:618-624`)刻意把它同時寫進 TX/MTX/TMF 三個「同標的(台指)」
產品代碼。結算價是**指數的屬性**、對每一天都有定義,不是某一張契約的成交紀錄。
836 個結算日裡 TX≠MTX 或 TX≠TMF 的筆數 = 0,正是這個複製邏輯的直接證據。

**研究時的注意事項**:用 `WHERE contract_code='TMF'`(或 `'MTX'`)撈到的 2022 年前
(或 2001 年前)資料,不是「該微型/小型契約當年真的有在交易」,而是同標的台指結算價的
複製值——**數值正確**,但別誤解成掛牌歷史。

---

## 補資料指令(需要近期期貨結算價時才要跑;不是修 bug)

```bash
# 1. 手動補抓(endpoint 每年一頁 HTML,重抓 2026 當年即涵蓋新結算日)
sbt "runMain Main pull taifex_futures_final_settlement"
# 2. 讀進 PostgreSQL
sbt "runMain Main read taifex_futures_final_settlement"
# 3. 重建 DuckDB cache
uv run --project . python research/cache_tables.py
```

官方來源:`https://www.taifex.com.tw/cht/5/futIndxFSP`(見 `db/table/TaifexFuturesFinalSettlement.scala` 註解)。

## 我實際查了什麼

1. **Schema 比對**:`psql \d` vs cache `PRAGMA table_info`,對照 `cache_tables.py:83-85` 同步 SQL。
2. **筆數比對**:總數、逐契約、逐年分布(cache vs PG)。
3. **逐筆全表 parity**:DuckDB attach PG 對四欄雙向 `EXCEPT` + 3152 筆 key-matched 價格逐筆比對(強於指定的 3 日期 × 5 檔抽樣)。
4. **涵蓋缺口**:329 月逐月檢查、consecutive 結算日最大間隔分析、尾端 staleness 及其歸因。
5. **staleness 根因**:raw 檔 mtime + 檔內最新日期 + `Main.scala` pull/read 路由 + `Job.scala:49-50` 註解(taifex 刻意排除每日 update)。
6. **異常值掃描**:負/零/NULL 結算價、未來日期、空 code/month、價格 min/max、`contract_month` 格式(月 vs 週契約)。
7. **資料模型查證**:TX/MTX/TMF 同日結算價是否相同 + `TradingReader.scala:618-624` 契約家族複製邏輯 + 官方來源註解。
