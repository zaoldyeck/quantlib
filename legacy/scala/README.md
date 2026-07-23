# Scala 爬蟲 + 策略引擎(已封存,2026-07-23 退役)

這是 quantlib 專案早期的 **Scala 實作**,2026-07-23 正式退役、封存於此當歷史藍圖。
**不再維護、不再執行**;所有功能已由 Python(`research/`)取代。

## 為什麼退役

專案定調為**全 Python、零 Scala/JVM/PostgreSQL 依賴**的架構:

```
舊:Scala 爬蟲 → PostgreSQL → cache_tables.py → cache.duckdb → 研究/實盤
新:Python 爬蟲(research/crawl)→ cache.duckdb(唯一結構化真源)→ 研究/實盤
```

- **PostgreSQL 已拔除**(2026-07-23):cache.duckdb 由 `research/crawl/rebuild.py`
  從 `data/` raw 封存重建、`research.crawl.update` 每日增量更新,不再需要 PG 中繼。
- **爬蟲全 Python 化**:`research/crawl/sources/` 15+ 源(日頻/月頻/季報/籌碼/期貨),
  逐位 parity 驗證過 Scala 產物後接手。
- **策略引擎在 Python**:`research/strat_lab`、`research/apex`(S)、`research/evergreen`、
  `research/serenity`;回測 5 秒級(DuckDB cache)vs Scala 10-15 分。

## 內容(封存快照,對應退役時 HEAD)

- `src/main/scala/` — 90 個 .scala:Setting/Task/Crawler/Reader/db(Slick)/strategy 分層。
- `src/main/resources/` — application.conf(DB/URL 設定)+ sql/(PG view/matview 定義)。
- `build.sbt` / `project_build.properties` — sbt 建置設定(SBT 1.10.5、Scala 2.13.15)。

## 若要重跑(不建議)

需把 `src/`、`build.sbt` 移回 repo 根、還原 `project/build.properties`、重建
PostgreSQL `quantlib` DB(已 drop)。Python 側已是唯一真源,重跑 Scala 只有考古價值。
