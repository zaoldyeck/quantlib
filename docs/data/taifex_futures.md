# 期交所期貨日線資料模組

本專案新增一條官方免費的 TAIFEX 期貨日線資料管線，先以研究需要最核心的指數期貨為主：

- 臺股期貨 `TX`
- 小型臺指期貨 `MTX`
- 微型臺指期貨 `TMF`
- 電子期貨 `TE`
- 金融期貨 `TF`

## 官方來源

期貨每日交易行情使用期交所「期貨每日交易行情下載」：

- 網頁：https://www.taifex.com.tw/cht/3/dlFutDailyMarketView
- Scala 下載端點：`https://www.taifex.com.tw/cht/3/futDataDown`
- 年度歷史：`down_type=2&his_year=YYYY`，官方提供 1998 年起的年度 zip。
- 當年度資料：`down_type=1&queryStartDate=YYYY/MM/DD&queryEndDate=YYYY/MM/DD&commodity_id=all`，官方限制單次區間不超過一個月。

三大法人使用期交所「三大法人-區分各期貨契約-依日期」：

- 網頁：https://www.taifex.com.tw/cht/3/futContractsDateView?menuid1=03
- Scala 下載端點：`https://www.taifex.com.tw/cht/3/futContractsDateDown`
- 免費限制：官方下載頁只開放最近三年。更早歷史需要走期交所歷史資料申請或付費資料，不應在研究層誤標成完整歷史。

最後結算價使用期交所「結算業務 / 最後結算價 / 指數期貨」：

- 網頁：https://www.taifex.com.tw/cht/5/futIndxFSP
- 這是到期契約的最後結算價，和期貨每日行情中的 `settlement_price` 欄位不是同一個資料集。
- 日行情檔在部分到期近月列會永久保持 `settlement_price` 空白；這時應使用本資料集的 `final_settlement_price`，不能一直等待日行情回補。

## Scala 指令

建立資料表：

```bash
sbt "runMain Main init"
```

下載 TAIFEX 原始檔：

```bash
sbt "runMain Main pull taifex"
```

只下載期貨每日行情：

```bash
sbt "runMain Main pull taifex_futures_daily"
```

只下載三大法人期貨契約資料：

```bash
sbt "runMain Main pull taifex_futures_institutional"
```

只下載指數期貨最後結算價：

```bash
sbt "runMain Main pull taifex_futures_final_settlement"
```

下載期交所免費前 30 個交易日高頻原始檔：

```bash
sbt "runMain Main pull taifex_intraday_raw"
```

下載全部官方免費期交所資料（日線、法人、最後結算價、30 交易日高頻原始檔）：

```bash
sbt "runMain Main pull taifex_all_free"
```

讀入 PostgreSQL：

```bash
sbt "runMain Main read taifex"
```

## 免費高頻 / 逐筆資料

期交所免費高頻資料不是完整長歷史，只提供目前頁面上的前 30 個交易日。專案現在會保存官方原始 zip，不立即解壓，避免逐筆資料量污染一般日線 reader。後續要研究日內策略時，應另外建立 Parquet/DuckDB parser。

目前保存的官方免費來源：

- `futures_sales`：前30個交易日期貨每筆成交資料，含 RPT 與 CSV zip。
- `futures_spread_sales`：前30個交易日期貨價差每筆成交資料，含 RPT 與 CSV zip。
- `futures_spread_orders`：前30個交易日期貨價差委託成交概況表，含 RPT 與 CSV zip。
- `options_sales`：前30個交易日選擇權每筆成交資料，含 RPT 與 CSV zip。這不是期貨交易標的，但可作為臺指期波動率/選擇權流量研究訊號。
- `flex_futures_sales`：前30個交易日客製化期貨每筆成交資料，官方目前只提供 CSV zip。

原始檔位置：

```text
data/taifex/intraday_raw/
```

每個來源目錄都有 `manifest.csv`，欄位包含抓取時間、來源、日期、是否本次重新下載、bytes、local path 與官方 URL。

### 滾動保存規則

1. 官方只保留前 30 個交易日；若要累積自己的長歷史，必須每天固定執行 `pull taifex_intraday_raw`。
2. 下載器預設會重抓最近 2 個日曆日的檔案，避免剛發布時檔案還在更新。可用 `QL_TAIFEX_INTRADAY_REFRESH_DAYS` 調整。
3. 下載器預設在台北時間 `16:00:00` 前跳過今日檔案，避免把盤中 partial tick 保存成研究資料。可用 `QL_TAIFEX_INTRADAY_SAFE_AFTER` 把安全時間往後延。
4. `QL_TAIFEX_INTRADAY_ALLOW_TODAY=true` 只能用於 live capture 或人工診斷，不可用於歷史回測。
5. 若要強制全數重抓，可設定 `QL_TAIFEX_INTRADAY_FORCE=true`。
6. 不要把這批資料稱為官方完整歷史；完整長歷史逐筆/間隔資料仍屬期交所歷史資料申請或 E-Data Shop 付費/申購範圍。

## 長歷史 Daily RPT Tick 資料湖

本專案另外支援使用外部公開 Google Drive 鏡像保存的 TAIFEX `Daily_YYYY_MM_DD.rpt` 長歷史逐筆成交資料。這條管線**不經過 PostgreSQL**，避免把高頻 tick 資料放進 row-store：

```text
Google Drive public folder
-> data/taifex/rpt/raw/                 # immutable zip/rpt archive
-> data/taifex/rpt/manifest.csv         # file id/date/local path manifest
-> data/taifex/rpt/lake/ticks/          # selected products as Parquet
-> data/taifex/rpt/lake/bars/           # 1m/5m/15m/30m/60m OHLCV bars
```

資料來源：

- Drive folder: https://drive.google.com/drive/folders/1mLvxQdqEQUty9EOeUQ33BoQcqxToM-SE
- 已驗證根目錄包含 `2011` 到 `2026` 年度資料夾，檔名型態為 `Daily_YYYY_MM_DD.zip`，少數歷史例外可為 `.rpt`。
- 部分 zip 內部 payload 是 `.csv` 而不是 `.rpt`，但欄位語意相同，parser 會視為合法 tick payload。
- 這是外部鏡像，不是專案直接向 TAIFEX 付費申購的官方歷史資料。使用前必須保留 manifest、checksum、zip/RPT validation 與缺檔報告。

下載與驗證：

```bash
# 只重新建立 manifest，不下載
uv run --project . python -m quantlib.futures.taifex_rpt --refresh-manifest discover

# 下載完整 raw zip/rpt archive；預設跳過已存在且可驗證的檔案
uv run --project . python -m quantlib.futures.taifex_rpt download --workers 8

# 驗證 raw archive 是否存在且 zip/RPT 可讀
uv run --project . python -m quantlib.futures.taifex_rpt verify-raw

# 看目前下載量、日期區間與缺檔數
uv run --project . python -m quantlib.futures.taifex_rpt summary
```

解析 tick Parquet：

```bash
# 只解析研究核心商品，避免把所有商品無差別膨脹到本機硬碟
uv run --project . python -m quantlib.futures.taifex_rpt parse-ticks --products TX,MTX,TMF,TE,TF

# 建立研究用分 K
uv run --project . python -m quantlib.futures.taifex_rpt build-bars --timeframes 1m,5m,15m,30m,60m
```

完整同步入口：

```bash
uv run --project . python -m quantlib.futures.taifex_rpt sync --workers 8 --parse-workers 6 --products TX,MTX,TMF,TE,TF --timeframes 1m,5m,15m,30m,60m --force-bars
```

同步流程會依序執行 manifest、raw download、raw validation、tick Parquet parse、bar aggregation。bar cache 已存在時必須明確使用 `--force-bars` 重建，避免新舊 partition 混在一起。解析狀態會寫入：

```text
data/taifex/rpt/parse_status/
```

`parse_status` 會記錄每個 raw 檔的檔案大小、mtime、請求商品、實際輸出商品與 parquet path。這可以避免舊年份因為 `TMF` 尚未上市而在每次重跑時被誤判為未完成。若已經手動完成一次全量解析，需要補建狀態索引，可執行：

```bash
uv run --project . python -m quantlib.futures.taifex_rpt index-existing --products TX,MTX,TMF,TE,TF
```

官方近 30 交易日 tick overlay：

```bash
uv run --project . python -m quantlib.futures.taifex_rpt parse-official-intraday --products TX,MTX,TMF,TE,TF
```

`parse-official-intraday` 會使用 `data/taifex/intraday_raw/futures_sales/`，並以官方檔案覆蓋同日期 mirror parquet。預設 latest safe date 與 Scala 下載器一致：台北時間 `16:00:00` 前最多只解析到昨日，避免今日盤中 partial tick 進入回測。只有 live capture 或人工診斷才可加 `--allow-today`。

設計原則：

1. Raw archive 是 source of truth；不全量解壓保存。
2. Parser streaming 讀 zip 內 RPT，只輸出選定商品到 Parquet。
3. Parquet 依 `product/year/month/source_date` 分區；同一個 `Daily_YYYY_MM_DD` 檔可能包含前一日夜盤成交，所以資料欄位同時保留 `source_date`、`trade_date`、`trade_ts`。
4. Bar aggregation 依 `product + contract_month + bar_start` 分組，不把不同契約月份混在同一根 K 棒。
5. 研究查詢使用 DuckDB/Polars 直接讀 Parquet；不要把 tick 資料匯入 PostgreSQL。
6. Drive 或 TAIFEX 回 HTML / 404 / 維護頁時，下載器會標成 `source_unavailable`，不和網路 transient error 混在一起。
7. 已判定為 `source_unavailable` 的舊日期會被快取成 `source_unavailable_cached`，避免每次同步都對週末或休市 placeholder 重複打網路；最近 14 天仍會重試以允許資料延遲發布。
8. 若未來要做實盤日內策略，必須再加上點位排序、同秒成交序、換月規則、交易時段切分與商品規格驗證。

## 富邦 Neo 期貨日內行情

本專案也可以用已打通的富邦 Neo API 做只讀日內行情捕捉：

```bash
uv run --project . python -m quantlib.futures.fubon_intraday_capture
```

預設會登入後初始化 market-data client，抓取 `TXF/MXF/TMF/EXF/FXF` 的近月與次近月：

- 1 / 5 / 10 / 15 / 30 / 60 分 K。
- quote。
- volumes。
- trades 分頁。

原始 JSON 位置：

```text
data/fubon/futures_intraday/YYYY-MM-DD/<regular|afterhours>/
```

富邦 API 的定位是「目前 session 可用日內資料捕捉」，不是一次補完整長歷史。若要把富邦資料變成長歷史，做法也是每天日盤/夜盤固定捕捉並累積到本地資料湖。

## 自動回補規則

期交所當日資料可能在盤後不同階段陸續完成，尤其是結算價、未平倉量與法人部位。為避免先抓到暫定資料後永久污染資料庫，TAIFEX 模組採用以下規則：

1. 每次 `pull taifex_futures_daily` 都會強制重抓最近 3 個月資料。可用環境變數 `QL_TAIFEX_DAILY_BACKFILL_MONTHS` 調整。
2. 每次 `pull taifex_futures_institutional` 都會強制重抓最近 2 個月資料。可用 `QL_TAIFEX_INSTITUTIONAL_BACKFILL_MONTHS` 調整。
3. 期貨日線會自動掃描最近 14 天的 `TX/MTX/TMF/TE/TF` 近月日盤資料。如果近月 `settlement_price` 或 `open_interest` 缺失，該日期所在月份會被列入強制重抓清單。可用 `QL_TAIFEX_STALE_SCAN_DAYS` 調整掃描窗口。
4. 已過期契約最後交易日的 `settlement_price` 空白不視為 stale。實測官方年度歷史檔中，多個已過期很久的交割月份最後交易日仍只有收盤價與未平倉量、沒有日行情 `settlement_price`；這不是下載太早，而是日行情資料語意。真正的最後結算價屬於期交所結算業務的「最後結算價」資料集，不應靠日行情檔等待回補。
5. Reader 不是 append-only；它會先刪除檔案涵蓋日期，再重新寫入。官方日後回補的結算價、未平倉量或修正值會覆蓋 PostgreSQL 舊值。

建議每日排程時間放在台北時間 18:30 之後。如果需要更保守，可以放在 20:00 後；但即使偶爾提早執行，rolling backfill 與 stale detector 也會在後續日常更新中自動修復。

## PostgreSQL 原始表

`taifex_futures_daily`

- `date`
- `contract_code`
- `contract_month`
- `open`, `high`, `low`, `close`
- `volume`
- `settlement_price`
- `open_interest`
- `best_bid`, `best_ask`
- `historical_high`, `historical_low`
- `trading_session`
- `spread_single_volume`

`taifex_futures_institutional`

- `date`
- `contract_code`
- `product_name`
- `investor_type`
- 多方、空方、淨額交易口數
- 多方、空方、淨額交易契約金額
- 多方、空方、淨額未平倉口數
- 多方、空方、淨額未平倉契約金額

`taifex_futures_final_settlement`

- `date`
- `contract_code`
- `contract_month`
- `final_settlement_price`

## DuckDB 研究表

重建 cache 後會產生：

```bash
uv run --project . python research/cache_tables.py
```

`taifex_futures_contract_rank`

- 每日每商品依到期月份排序。
- `month_rank=1` 是近月，`month_rank=2` 是次近月。

`taifex_futures_continuous`

- 以近月為主，建立 return-spliced 連續期貨。
- 換月日用新契約自己的前一日收盤計算報酬，避免把新舊契約價差誤算成策略報酬。
- 提供 `continuous_open/high/low/close/settlement`。
- `settlement` 會依序使用日行情 `settlement_price`、到期 `final_settlement_price`，再退到 `close`。

`taifex_futures_daily_factors`

- 大台/小台/微台價格差：`tx_mtx_close_spread`, `tx_tmf_close_spread`
- 期現價差：`tx_spot_basis`, `tx_spot_basis_pct`
- 近月/次近月期限價差：`tx_next_term_spread`, `tx_next_term_spread_pct`
- 三大法人淨未平倉與淨交易口數：例如 `foreign_tx_net_oi`, `trust_tx_net_oi`, `dealer_tx_net_oi`
- 到期最後結算價：例如 `tx_final_settlement_price`

## 重要限制

1. 期貨每日行情的 OHLC、成交量、結算價、未平倉量可以用官方免費年度 zip 回補完整歷史。
2. 三大法人期貨契約資料的官方免費下載只有最近三年；更早資料不能假裝存在。
3. 連續期貨是研究用價格序列，不是實際可交易商品。實盤交易仍必須落到實際契約月份。
4. 夜盤資料存在於原始表的 `trading_session='盤後'`；目前研究表預設使用 `trading_session='一般'` 日盤資料。
