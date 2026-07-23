# PostgreSQL 退役快照(2026-07-23)

拔 PG 前 cache 最終狀態記錄。cache 由 research/crawl/rebuild.py 從 data/ raw 封存重建、
research.crawl.update 增量更新;PG 為 buggy 舊源(cache 校正後更正確、raw 為真源可重建),
拔除不損失任何可信資料。verify_vs_pg 全綠 + FC1 修復完成後執行。

| cache 表 | 列數 |
|---|---:|
| bs_concise_raw | 3,481,668 |
| capital_reduction | 666 |
| cf_progressive_raw | 6,359,600 |
| daily_quote | 9,125,751 |
| daily_trading_details | 5,864,519 |
| etf | 228 |
| ex_right_dividend | 29,625 |
| foreign_holding_ratio | 8,125,783 |
| industry_taxonomy_pit | 452,870 |
| insider_holding | 771 |
| is_progressive_raw | 3,181,254 |
| margin_transactions | 8,341,830 |
| market_index | 769,751 |
| operating_revenue | 481,564 |
| sbl_borrowing | 4,673,814 |
| stock_per_pbr | 7,722,490 |
| taifex_futures_continuous | 26,645 |
| taifex_futures_contract_rank | 148,031 |
| taifex_futures_daily | 5,880,146 |
| taifex_futures_daily_factors | 6,875 |
| taifex_futures_final_settlement | 3,152 |
| taifex_futures_institutional | 47,796 |
| tdcc_shareholding | 813,110 |
| treasury_stock_buyback | 5,768 |
| **合計** | **65,543,707** (24 表) |
