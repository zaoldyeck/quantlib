# C-taifex_futures_institutional — cache 一致性與缺漏稽核

**一句話結論:這份資料可以信。** DuckDB cache 是 PostgreSQL 的「逐格完全複本」——
全 47,796 列、16 個資料欄，兩邊雙向比對零差異;涵蓋範圍內沒有漏掉任何一個交易日;
表內數字自洽、無不可能值。唯一要提醒的兩件事都不是資料錯：(1) 尾端落後 38 個交易日
（爬蟲自 2026-05-21 起就沒再跑，PG 和 cache 一起停在那天）;(2) 最早只到 2023-05-22
（TAIFEX 免費端點只給滾動三年，更早要付費申請），這是端點天生的界線，不是漏抓。

這張表是**期貨三大法人資料**（三大法人-區分各期貨契約-依日期），不是個股資料。
key = (日期, 契約代碼, 身份別)，每個 (日期,契約) 固定 3 列（外資及陸資 / 投信 / 自營商）。

---

## 1. Schema 比對:cache 忠實,只丟掉一個代理主鍵（正確）

| | PG `taifex_futures_institutional` | cache | 判定 |
|---|---|---|---|
| 欄數 | 17 | 16 | cache 少 1 欄 |
| 少的那欄 | `id`（bigint，自增 PK，`nextval(...)`） | — | **正確**:純代理鍵，非資料欄 |
| 16 個資料欄 | date, contract_code, product_name, investor_type, long/short/net_volume, long/short/net_value_thousands, long/short/net_open_interest, long/short/net_oi_value_thousands | 全部在，型別全等（date=DATE，其餘 12 個數值欄=BIGINT） | **零型別降級** |

同步程式 `research/cache_tables.py:69-74` 明列這 16 欄 `SELECT ... FROM pg.public.taifex_futures_institutional`，
沒有漏欄、沒有把 BIGINT 降成 INT、沒有漏表。丟 `id` 是對的——它是 Slick 自增代理鍵，
真正的唯一鍵是 `(date, contract_code, investor_type)`（PG unique index `idx_TaifexFuturesInstitutional_date_contract_investor`），
這個組合在資料欄裡完整保留。

## 2. 筆數比對:整表 + 逐年，兩邊完全相等

```
                PG        cache
total       47,796      47,796
2023         9,270       9,270
2024        15,618      15,618
2025        16,767      16,767
2026         6,141       6,141
distinct dates  728        728
distinct contracts 23       23
investor_type  {外資及陸資, 投信, 自營商}（兩邊相同）
```

## 3. 逐格比對:不是抽樣，是**全表全欄**

不做 3×5 抽樣（太弱），直接在 DuckDB 裡 `ATTACH` PG，對全部 16 欄跑雙向 `EXCEPT`：

```
rows in CACHE not in PG (all 16 cols): 0
rows in PG not in CACHE (all 16 cols): 0
=> CELL-EXACT MIRROR
```

外加全欄聚合交叉驗證（COUNT + 12 個數值欄 SUM + net_volume/net_open_interest 的 MIN/MAX）
兩邊字元級相同：`47796,334817392,436752909742,...,-435884,150451`（PG）==（cache）。

## 4. 覆蓋缺口:窗內零漏，且比 twse 日曆還完整

以 `daily_quote market='twse'` 的交易日當日曆基準，在 institutional 涵蓋窗
[2023-05-22, 2026-05-21] 內：

- **有報價卻無法人資料的交易日 = 0**（沒有任何一個交易日被漏掉）
- **institutional 有、twse 報價卻沒有的日子 = 2**:`2025-08-15`、`2026-04-29`

這 2 天不是 institutional 的錯，反而是它更完整。這兩天是**真的交易日**被 twse 的
0-byte sentinel 誤標成休市（已在 C-daily_quote 立案）：
- 原始 Big5 CSV 實體存在該日 69 列（23 契約 × 3 身份別），日期欄是西元格式
  `2026/04/29` / `2025/08/15`（不是民國），`iconv -f BIG5` 解碼後可見。
- `taifex_futures_daily` 同日各有 2,341 / 2,287 列（完整期貨交易日）。
- TAIEX 加權指數當天有公告真實收盤:24,334.48 (+96.38) / 39,303.50 (-218.23) → 市場確實開盤。
- 但 `research/data_calendar.py::is_trading_day` 對這兩天回 False（讀 twse 0-byte sentinel）。

→ institutional 這張表**獨立佐證了 C-daily_quote 的 sentinel 誤標 bug**。它在窗內唯二的
「額外日」正是那兩個被誤殺的真交易日，兩天它都正確收錄。窗內另外兩個誤標日
（2021-08-18 在窗外、2026-05-28 在尾端 2026-05-21 之後）與本表無關。

**前緣 2023-05-22 不是漏抓**:端點 `futContractsDateDown`（`application.conf`
`data.taifex.futuresInstitutional`）是靜態 URL（`Setting` 裡 `override def url = file`，無日期參數），
官方註解明寫「免費頁只給滾動三年，更早要付費/申請」。爬蟲 2026-05-21 那次抓回的就是
~2023-05 至 2026-05 的三年窗。這是端點界線，不是缺口。

## 5. 尾端落後 38 個交易日（**唯一要處理的事，但不是資料錯**）

`taifex_futures_institutional` 與 `taifex_futures_daily` 都停在 **2026-05-21**
（`data/taifex/` 目錄 mtime = May 21 07:42，爬蟲最後一次跑的時間）。以 twse 指數當日曆，
2026-05-22 至今（截至 2026-07-17 最新可得）有 **38 個交易日**沒有 TAIFEX 資料。

- 這是**爬蟲排程沒跑**，不是 cache 不同步（PG 也一樣停在 5/21，cache 忠實鏡射了 PG 的舊）。
- 落後 ~2 個月 < 滾動三年窗，所以**還沒有永久遺失**:現在重跑爬蟲，端點回的三年窗
  仍涵蓋 5/22 起的全部，reader 是 insert（不刪舊），補得回來。但若拖到超過三年才補，
  最舊那段會滾出免費窗、只能付費取得——**建議儘快補**。

## 6. 異常值掃描:全清

| 檢查 | 結果 |
|---|---|
| net_volume ≠ long−short（口數） | 0 |
| net_open_interest ≠ long−short（未平倉口數） | 0 |
| net_oi_value ≠ long−short（未平倉金額） | 0 |
| **net_value ≠ long−short（交易金額千元）** | **6,867 列，但全部 ∈ [−2, +2] 千元** |
| 負的 long/short volume、long/short OI | 0 |
| 成交量=0 但金額≠0（或反之） | 0 |
| 未來日期（> 2026-07-23） | 0 |
| 每個 (date,contract) 不等於 3 列 | 0 |
| 極值 | long_volume ∈ [0, 588,861]、long_OI ∈ [0, 258,435]、net_OI ∈ [−435,884, 150,451]（合理） |

**net_value 的 6,867 個 ±2 千元差異不是 bug**:原始 CSV 本來就把「多方交易契約金額」
「空方交易契約金額」「多空交易契約金額淨額」印成**三個各自獨立四捨五入到千元**的欄，
reader 忠實照收官方的「淨額」欄（`readTaifexFuturesInstitutional`,TradingReader.scala:537），
沒有自己重算。所以 long−short 與官方 net 差在四捨五入的 ±2 千元（≈ ±NT$2,000，對上億名目
可忽略）。口數（整數，不需捨入）的 net 則完全精確。**忠實保留來源欄 > 重算**，此為正確設計。

---

## 判定

**verdict = OK。** cache 對 institutional 而言是 PG 的逐格完全複本，涵蓋窗內無漏日、
表內自洽、無異常值。兩個 caveat 都不是資料正確性問題：尾端落後 38 交易日（跑一次爬蟲即補）、
前緣 2023-05-22（端點滾動三年天生界線）。

## 補抓指令（由主流程統一安排，稽核不自己下載）

```bash
sbt "runMain Main pull taifex"    # 抓 futContractsDateDown 滾動三年窗（同時含 daily/institutional/settlement）
sbt "runMain Main read taifex"    # reader insert（不刪舊），補上 2026-05-22 起的尾端
uv run --project research python research/cache_tables.py   # 重建 cache 帶入新列
```
端點:`https://www.taifex.com.tw/cht/3/futContractsDateDown`（三大法人-區分各期貨契約-依日期，免費滾動三年）。
