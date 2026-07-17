# 台股量化策略排行與目前 Champion

最後更新：2026-05-18

資料刷新：2026-05-18 16:18 CST 已執行 `sbt "runMain Main update"` 與 `uv run --project research python research/cache_tables.py`

資料截止日：2026-05-15

最近一年績效窗口：2025-05-15 至 2026-05-15

## 結論

目前最強的 backtest champion 是 **Iter86 Dual Max 42.44 / 287.01**。

它是本輪為了同時提高 OOS CAGR 與最近一年 CAGR 而研發的多策略 PM allocator。它不是固定持有 2330，也不是寫死某個產業，而是在兩個已驗證 sleeve 之間切換：

- 長期 OOS sleeve：`iter83_inner0_blend_weekly_lb21_m0_hold40_c1_w0_100_d75`
- 最近強勢 sleeve：`iter83_inner5_blend_weekly_lb10_m-1_hold40_c1_w0_100_d75`

Allocator 使用 lagged NAV 相對動能：

- 每週檢查一次。
- 使用 5 個交易日的落後相對動能。
- 門檻 `margin = 0`。
- 確認天數 1 天。
- 最短持有 20 個交易日。
- 在兩個 sleeve 之間做 0% / 100% 切換，預設 recent sleeve 權重 75%。
- Long-only、不開槓桿、下一交易日開盤執行、最多同時持有 6 檔。

這個版本同時超越上一輪 Iter84 的兩個關鍵高點：

- 舊最高 OOS CAGR：Iter84 Aggressive +40.21%。
- 舊最高最近一年 CAGR：Iter84 Recent-Fit +286.27%。

Iter86 Dual Max 的結果是：

- OOS CAGR：+42.44%
- 最近一年 CAGR：+287.01%
- DSR：0.997
- PBO：0.138
- OOS MDD：-26.05%
- 主動式 ETF 比較：17 / 17 全勝

## 重要區分

本輪不是只有一個「最高」：

| 類型 | 策略 | OOS CAGR | 最近一年 CAGR | 結論 |
|---|---|---:|---:|---|
| 絕對最高 OOS | Iter86 raw OOS max | +42.98% | +223.94% | 輸 2 檔主動 ETF，不能當 champion |
| 絕對最高最近一年 | Iter86 raw recent max | +36.06% | +287.30% | 最近一年最高，但長期 OOS 較弱 |
| 雙目標 champion | Iter86 Dual Max 42.44 / 287.01 | +42.44% | +287.01% | 同時高 OOS、高最近一年，且完整驗證通過 |

所以若只問「單一欄位最高」，會得到不同答案；若問「最適合列為目前最強策略」，答案是 Iter86 Dual Max。

## 最終策略排行

只把資料 cutoff 等於 2026-05-15 的候選列為正式排行候選。Iter67 / Iter72 lineage 目前仍停在 2026-05-08，只能作為舊 lineage 參考，不列入 current-cutoff champion。

| 排名 | 策略 | 狀態 | Full CAGR | OOS CAGR | 最近一年 CAGR | OOS Sortino | OOS MDD | DSR | PBO | 主動式 ETF 比較 | 最大持股 |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | Iter86 Dual Max 42.44 / 287.01 | Champion | +35.09% | +42.44% | +287.01% | 2.737 | -26.05% | 0.997 | 0.138 | 17 / 17 | 6 |
| 2 | Iter86 Strict OOS 42.49 | 長期 OOS 嚴格候選 | +35.38% | +42.49% | +262.24% | 2.718 | -25.78% | 0.998 | 0.244 | 17 / 17 | 6 |
| 3 | Iter86 Recent Max 287.30 | 近期最高候選 | +30.60% | +36.06% | +287.30% | 2.322 | -25.83% | 0.961 | 0.270 | 17 / 17 | 10 |
| 4 | Iter86 Raw OOS 42.98 | 高 OOS 但未通過嚴格 champion 標準 | +34.88% | +42.98% | +223.94% | 2.748 | -26.03% | 0.998 | 0.242 | 15 / 17 | 6 |
| 5 | Iter84 Aggressive 40.21 | 舊高 OOS 候選，已被 Iter86 超越 | +32.41% | +40.21% | +223.81% | 2.570 | -25.83% | 0.995 | 0.094 | 15 / 17 | 6 |
| 6 | Iter84 Conservative 37.20 | 舊 champion，已被 Iter86 超越 | +31.73% | +37.20% | +268.25% | 2.379 | -26.69% | 0.978 | 0.074 | 17 / 17 | 6 |
| 參考 | Iter67 / Iter72 cap6 source-reconciled | 舊 lineage 參考，cutoff 2026-05-08 | +29.68% | +33.40% | +259.58% | 2.103 | -25.79% | 0.954 | 0.216 | 17 / 17 | 6 |

OOS 使用 2010-01-01 至 2025-12-31。最近一年使用目前最新可用資料日往前一年。

## Benchmark

| Benchmark | OOS CAGR | OOS Sortino | OOS MDD | 最近一年 CAGR |
|---|---:|---:|---:|---:|
| 0050 total return | +13.29% | 0.933 | -33.96% | +107.41% |
| 2330 total return | +25.76% | 1.523 | -44.80% | +131.63% |

Iter86 Dual Max 明顯打贏 0050 與 2330 的 OOS CAGR，也打贏 2330 最近一年 CAGR。這代表目前已經通過「不能比長持 2330 差」這個門檻。

## Champion 策略內容

Iter86 Dual Max 是一個 long-only 台股多策略 PM allocator，不做空、不開槓桿，最多同時持有 6 檔股票。

它的交易內容可以理解為：

1. 先準備兩個已驗證的子策略 sleeve。
2. 每週用過去 5 個交易日的 NAV 相對動能判斷哪個 sleeve 佔優。
3. 若 recent sleeve 近期相對強，就切到 recent sleeve。
4. 若 OOS sleeve 近期相對強，就切到 OOS sleeve。
5. 切換後至少持有 20 個交易日，避免太頻繁來回切。
6. 最終每日 target book 仍然是股票持倉，而不是只在 NAV 上做紙上配置。

它的賺錢來源不是單一股票或單一產業，而是：

- 長期強勢 alpha sleeve 提供 OOS 穩定性。
- 最近強勢 sleeve 在近一年台股結構中捕捉更強的主升段。
- PM allocator 避免永遠押單一 sleeve，讓策略能在長期穩健與近期強勢之間切換。

## 為什麼不是選 +42.98% OOS 的版本

Iter86 raw OOS max 的 OOS CAGR 是 +42.98%，比 champion 的 +42.44% 更高。但它只贏 15 / 17 檔主動式 ETF，輸給 00400A 與 00401A，而且最近一年只有 +223.94%。

本輪目標不是只把 OOS CAGR 榨到最高，而是同時追求：

- 高 OOS CAGR。
- 高最近一年 CAGR。
- DSR / PBO 不顯示明顯過度最佳化。
- 主動式 ETF 比較不能露出短窗弱點。
- 最大持股與 long-only 限制符合實盤約束。

依這個標準，Iter86 Dual Max 是更合理的 champion。

## Deployment Stage

| Stage | 目前狀態 |
|---|---|
| research_candidate | 已通過 |
| backtest_validated | Iter86 Dual Max 42.44 / 287.01 已達成 |
| execution_ready | 尚未達成 |
| live_pilot | 尚未達成 |
| production_scaled | 尚未達成 |

因此：

- 可以把 Iter86 Dual Max 視為目前最強的研究 / 回測 champion。
- 不能直接讓券商 SDK 用它下單，除非先完成 execution-ready target book。
- 自動交易系統仍應 fail-closed：沒有 execution-ready 策略時，不可送出真實委託。

## 驗證與輸出檔

本次排行由 `research/strat_lab/iter_86_oos_recent_maximizer.py` 產生。

主要輸出：

- `research/strat_lab/results/iter_86_oos_recent_maximizer_base_sleeves.csv`
- `research/strat_lab/results/iter_86_oos_recent_maximizer_fast_screen.csv`
- `research/strat_lab/results/iter_86_oos_recent_maximizer_summary.csv`
- `research/strat_lab/results/iter_86_oos_recent_maximizer_active_etf_comparison.csv`
- `research/strat_lab/results/iter_86_oos_recent_maximizer_iter86_b15_b08_weekly_lb5_m0_hold20_c1_rw0_100_d75_daily.csv`

本輪也依照資料新鮮度規則，先確認 PostgreSQL 與 DuckDB 來源表截止日都是 2026-05-15，再進行策略研發。

## 下一個必要工程

若要把 Iter86 Dual Max 從 `backtest_validated` 推進到 `execution_ready`，下一步不是再調參，而是工程化：

1. 建立可重現每日 target book。
2. 驗證 target book NAV 與 Iter86 source NAV reconciliation。
3. 加入完整交易成本、稅、滑價、next-open 或可執行成交假設。
4. 產生 broker order plan，但先維持 dry-run。
5. 通過後再更新 `research/trading/strategy_registry.py` stage。
