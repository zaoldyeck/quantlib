"""Python 直寫 DuckDB 的台股日/月頻爬蟲(無 PostgreSQL、無 JVM)。

雲端 VM 每日自主更新 S 策略所需的 cache 表,取代「Scala 爬蟲 → PostgreSQL →
cache_tables.py 全砍重建」那條重管線。設計原則:

- **只抓當前格式**:live 只前進「今日起」的新交易日,歷史舊格式已在 scp 種入的
  cache 裡;故每個 source 只實作現行欄位、對非預期欄數 **fail-loud**(絕不靜默錯位
  —— TWSE 悄悄加欄是已知地雷)。
- **增量 upsert**:對 `cache.duckdb`(read_write)做「刪該日 + 插入」,非全砍重建。
- **齊備 + 休市日曆**:沿用 `quantlib.data_calendar`(D+1 00:30 齊備、0-byte sentinel
  休市日曆),只抓已齊備日;交易所回無資料就寫 sentinel。
- **parity 守護**:每個 source 的輸出必須逐位重現舊 Scala 管線寫進 cache 的值
  (`src/quantlib/crawl/tests/test_parity.py`),未過不得驅動 live 決策。
"""
