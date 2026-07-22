# 台股量化策略排行與目前 Champion

最後更新：2026-05-19

資料刷新：2026-05-18 20:48 CST 已執行 `sbt "runMain Main update"` 與 `uv run --project research python research/cache_tables.py`。

資料截止日：價格表 `daily_quote` 到 2026-05-18；PER / 三大法人 / 融資券來源表到 2026-05-15。

## 目前結論

正式策略排行只採用 **富邦 realistic execution** 後的結果。Paper / raw NAV 只保留為研發診斷資料，不再作為正式排行依據。

目前 champion 是：

| 類型 | 策略 | 狀態 | 判斷 |
|---|---|---|---|
| Realistic execution champion | Iter95 Global Exit-Aware Time50 r-1 | `execution_ready` | 目前最強且已可產生 dry-run 訂單計畫的策略 |
| Paper backtest champion | Iter86 Dual Max 42.44 / 287.01 | 歷史研究參考 | 無成本 NAV 最高，但不作正式排行 |

Iter95 不是只把停利 / 停損硬貼到 Iter92，而是在加入 execution-layer exit engine 後，重新從 Iter86 / Iter89 / Iter92 的 target-book 候選池做全局搜尋。流程先用 91 個 target-book 候選做 realistic no-exit 篩選，再用 16 個代表性 exit rules 做 coarse search，最後只對勝出的 3 個 target books 做 76 組 focused exit grid。

Iter95 的 base target book 仍是 Iter92，但新增一個策略層 time exit：**持倉滿 50 個交易日且該部位仍低於進場均價 -1% 時退出**。這個 exit rule 在回測引擎內執行，所以同樣扣除富邦手續費、賣出交易稅、滑價、成交量上限、漲跌停阻擋與部分成交。

Iter95 不是所有欄位都完美第一。`tp100_time50_r-1` 的 OOS CAGR 較高，但綜合 objective、最近一年 CAGR、近 6 / 3 / 1 個月表現與交易分布穩定性由 `time50_r-1` 勝出。因此正式 champion 採用 `time50_r-1`，而不是單純最高 OOS CAGR 的版本。

## 正式策略排行

排序原則：先看 realistic execution 後的 OOS CAGR、最近一年 CAGR、近 6 / 3 / 1 個月表現，再用 DSR、PBO、OOS Sortino、MDD 判斷是否可靠。MDD 不是單獨最高權重，但若明顯惡化會降級。

| 排名 | 策略 | 資料截止 | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 12 月報酬 | 近 6 月報酬 | 近 3 月報酬 | 近 1 月報酬 | OOS Sortino | OOS MDD | DSR | PBO | Fill Ratio | 觀測最大持股 | 判斷 |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | Iter95 Global Exit-Aware Time50 r-1 | 2026-05-18 | +34.56% | +39.14% | +295.53% | +295.53% | +168.75% | +94.61% | +11.47% | 2.470 | -22.09% | 0.993 | 0.032 | 81.49% | 10 | 新 champion |
| 2 | Iter95 TP100 + Time50 r-1 | 2026-05-18 | +34.41% | +39.66% | +275.39% | +275.39% | +146.86% | +78.95% | +4.39% | 2.543 | -22.12% | 0.995 | 0.020 | 80.81% | 10 | OOS CAGR 最高，但近期較弱 |
| 3 | Iter95 TP150 + Time50 r-1 | 2026-05-18 | +34.46% | +39.14% | +289.41% | +289.41% | +164.57% | +91.59% | +5.72% | 2.470 | -22.09% | 0.993 | 0.032 | 81.37% | 10 | 接近 champion，近 1 月較弱 |
| 4 | Iter92 Unconstrained Execution Meta Switch | 2026-05-18 | +33.67% | +38.11% | +260.55% | +262.77% | +167.26% | +94.08% | +11.00% | 2.397 | -26.55% | 0.986 | 0.032 | 81.53% | 11 | 前任 champion |
| 5 | Iter89 Robust Execution Champion | 2026-05-18 | +31.54% | +35.80% | +253.84% | +255.99% | +156.85% | +88.84% | +7.43% | 2.234 | -26.22% | 0.956 | 0.034 | 84.75% | 10 | 穩健備選，但已被 Iter95 超越 |

### 為什麼 Iter95 排第一

Iter95 對前任 Iter92 的比較：

- OOS CAGR：+39.14%，高於 Iter92 的 +38.11%。
- 最近一年 CAGR：+295.53%，高於 Iter92 的 +260.55%。窗口為 2025-05-16 至 2026-05-18。
- 近 6 / 3 / 1 個月：+168.75% / +94.61% / +11.47%，三個窗口都略高於 Iter92。
- OOS MDD：-22.09%，明顯優於 Iter92 的 -26.55%。
- OOS Sortino：2.470，高於 Iter92 的 2.397。
- DSR：0.993，高於 Iter92 的 0.986。
- PBO：0.032，與 Iter92 相同，仍在低 overfit 風險區間。

但需要保留的風險判斷：

- Fill ratio 81.49%，仍低於 Iter89 的 84.75%，代表成交摩擦還是真實風險。
- champion exit rule 是 time stop，不是 broker-side 停損停利單；目前已在 order planner 中以目標部位調整處理。
- 它目前是 `execution_ready`，不是 `live_pilot` 或 `production_scaled`。真實送單仍需要使用者明確設定 `QL_STRATEGY_CAPITAL_TWD`，完成最新 broker accounting smoke test，並在送單那次明確關閉 dry-run。

### 10 檔限制下的反事實比較

如果恢復原本「最多 10 檔」限制，最佳候選會變成 `cap10_best_lb21_h5`。它仍然能取代舊 Iter89 champion，但整體不如解除限制後的 Iter92。

| 條件 | 策略 | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 12 月報酬 | 近 6 月報酬 | 近 3 月報酬 | 近 1 月報酬 | OOS Sortino | OOS MDD | DSR | PBO | Fill Ratio | 觀測最大持股 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 最多 10 檔 | cap10_best_lb21_h5 | +32.10% | +36.33% | +266.30% | +268.58% | +164.64% | +94.38% | +10.61% | 2.309 | -26.53% | 0.970 | 0.024 | 84.92% | 10 |
| 不設持股上限 | Iter92 Unconstrained Execution Meta Switch | +33.67% | +38.11% | +260.55% | +262.77% | +167.26% | +94.08% | +11.00% | 2.397 | -26.55% | 0.986 | 0.032 | 81.53% | 11 |

判斷：

- 若基準是舊 Iter89，10 檔內可以找到更好的策略：`cap10_best_lb21_h5` 的 OOS CAGR、最近一年 CAGR、近 6 / 3 / 1 月報酬、DSR、PBO 與 fill ratio 都優於 Iter89；MDD 則略差。
- 若基準是「目前整體最強」，10 檔限制下沒有超越 Iter92。cap10 candidate 的最近一年 CAGR 與 PBO 較好，但 OOS CAGR、Full CAGR、OOS Sortino、DSR、近 6 月與近 1 月都輸給 Iter92。
- 因此持股限制確實有 opportunity cost：多允許 1 檔持股，換來約 +1.78pp OOS CAGR、較高 Sortino 與較高 DSR。

### Iter93 解除限制後重新搜尋

2026-05-19 追加 Iter93：解除持股檔數限制後，從既有可轉成 target book 且已通過 realistic execution 的 sleeve universe 重新搜尋，而不是只沿用 Iter92 的三個 sleeve。搜尋架構包含：

- single-sleeve momentum switch。
- top-2 / top-3 momentum blend。
- weekly / monthly schedule。
- 3 / 5 / 10 / 21 / 42 / 63 日 lookback。
- 3 / 5 / 10 / 20 / 40 日 minimum hold。
- 3% / 5% / 8% target-change compression threshold。

Iter93 的結論是：**沒有找到全勝現有全部策略的候選**。`ALL_WIN_COUNT = 0`。

| 策略 | 全勝指標數 | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 12 月報酬 | 近 6 月報酬 | 近 3 月報酬 | 近 1 月報酬 | OOS Sortino | OOS MDD | DSR | PBO | Fill Ratio | 觀測最大持股 | 判斷 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Iter92 Unconstrained Execution Meta Switch | 現任 champion | +33.67% | +38.11% | +260.55% | +262.77% | +167.26% | +94.08% | +11.00% | 2.397 | -26.55% | 0.986 | 0.032 | 81.53% | 11 | 保留 |
| iter93_single_monthly_lb10_h40_thr3 | 5 / 10 | +32.57% | +36.50% | +272.01% | +274.35% | +171.18% | +98.84% | +13.65% | 2.307 | -26.37% | 0.969 | 0.140 | 82.14% | 11 | 近期更強，但 OOS / DSR / PBO 輸 |
| iter93_single_monthly_lb5_h40_thr5 | 4 / 10 | +34.58% | +39.10% | +269.03% | +271.35% | +160.70% | +94.37% | +14.49% | 2.283 | -28.89% | 0.973 | 0.064 | 76.92% | 10 | OOS CAGR 更高，但風險品質下降 |

判斷：

- `iter93_single_monthly_lb10_h40_thr3` 是近期窗口最強 challenger，贏最近一年、近 12 / 6 / 3 / 1 月，但 OOS CAGR、OOS Sortino、DSR、PBO 都輸 Iter92，因此不升級。
- `iter93_single_monthly_lb5_h40_thr5` 的 OOS CAGR 達 +39.10%，高於 Iter92 的 +38.11%，但 MDD 深到 -28.89%、Sortino 下降到 2.283、PBO 0.064、fill ratio 只有 76.92%，不能視為更穩健。
- 目前沒有任何 Iter93 候選能同時超越所有現有策略的高水位：OOS CAGR、最近一年 CAGR、近 12 / 6 / 3 / 1 月報酬、OOS Sortino、MDD、DSR、PBO。

Iter93 artifacts：

- `research/strat_lab/iter_93_unconstrained_all_win_search.py`
- `var/out/strat_lab/iter_93_unconstrained_all_win_search/sleeve_universe.csv`
- `var/out/strat_lab/iter_93_unconstrained_all_win_search/selected_sleeves.csv`
- `var/out/strat_lab/iter_93_unconstrained_all_win_search/quick_summary.csv`
- `var/out/strat_lab/iter_93_unconstrained_all_win_search/final_summary.csv`

## Iter95 策略內容

Iter95 是 Iter92 target book 加上策略層 exit engine 後重新全局搜尋出的 champion。它不是固定持有任何股票，也不是寫死產業；選股仍由 Iter92 的多策略 PM allocator 決定，Iter95 只在持倉層加上一條經過驗證的退出規則。

核心交易邏輯：

1. 先由 Iter92 產生每日 target book。
2. 每個部位記錄進場均價與持有天數。
3. 若部位持有滿 50 個交易日，且當日開盤相對進場均價仍低於 -1%，則退出該部位。
4. 退出後資金留現金，直到下一次 target book 重新要求買入。
5. 所有退出交易都在富邦 realistic execution simulator 中成交，納入手續費、交易稅、滑價、成交量限制與漲跌停阻擋。

這條 exit rule 的含義是：策略不砍短期震盪，也不急著賣掉剛進入主升段的股票；它只清掉「持有一段時間後仍沒有證明自己」的拖累部位。MFE / MAE 診斷顯示，原始 Iter92 交易的平均 MFE 約 +11.50%、平均 MAE 約 -10.85%，虧損交易也常曾經有約 +5.54% 的有利波動，因此緊停損或緊 trailing stop 會過早砍掉主升段。最佳化結果也支持這點：寬鬆 time exit 比固定 stop-loss / trailing stop 更有效。

### Iter95 搜尋方式

Iter95 的搜尋不是 Iter92-only：

- Stage 1：91 個 target-book 候選做 realistic no-exit 篩選。
- Stage 2 coarse：16 個勝出 target books 乘以 16 個代表性 exit rules。
- Stage 3 focused：3 個勝出 target books 乘以 76 個完整 exit rules。
- 共同驗證：同一套 Fubon realistic execution、同一套 validator、同一資料截止日 2026-05-18。

結論：

- 全局最高綜合 objective：`Iter95 Global Exit-Aware Time50 r-1`。
- 最高 OOS CAGR：`Iter95 TP100 + Time50 r-1`，OOS CAGR +39.66%，但近 6 / 3 / 1 月都弱於 champion，因此不排第一。
- 非 Iter92 target-book 家族在 focused search 中沒有超車；多數不是 MDD 太深，就是 DSR / PBO / 近期表現不夠好。

## Iter92 策略內容

Iter92 是一個多策略 PM allocator。它不是固定持有 2330，也不是寫死半導體或科技股，而是在三個已經 realistic-execution 驗證過的 sleeve 之間切換：

| Sleeve | 角色 |
|---|---|
| Iter89 Robust Execution Champion | 前一代穩健 execution champion |
| Iter87 baseline realistic | OOS CAGR 較高但回撤較深的 baseline |
| Iter67 / Iter72 realistic recheck | 近期強度高、回撤較低的 sleeve |

交易規則：

1. 每月第一個交易日評估一次。
2. 使用前一交易日以前的 5 個交易日 realistic NAV 報酬做相對動能比較。
3. 選擇最近 5 個交易日表現最強的 sleeve。
4. 選定後至少持有 5 個交易日，避免過度來回切換。
5. 每天拿被選中 sleeve 的股票 target book 作為目標持倉。
6. 目標權重變動若小於 5% L1 threshold，會被壓縮掉，避免無意義小換倉。
7. 最後再通過富邦 realistic execution simulator，扣除手續費、交易稅、滑價、成交量限制與漲跌停阻擋。

執行假設：

- Long-only。
- 不放空、不開槓桿。
- 使用 total-return-equivalent adjusted prices。
- 富邦 odd-lot 交易。
- 單日成交量參與率上限 5%。
- 固定滑價 5 bps。
- 每月成交額 100 萬內手續費 1.8 折，100 萬以上 4 折。
- 賣出交易稅 0.3%。
- 漲跌停阻擋與部分成交都納入 simulation。

## Benchmark

| Benchmark | OOS CAGR | OOS Sortino | OOS MDD | 最近一年 CAGR |
|---|---:|---:|---:|---:|
| 0050 total return | +13.29% | 0.933 | -33.96% | +107.41% |
| 2330 total return | +25.76% | 1.523 | -44.80% | +131.63% |

Iter95 的 OOS CAGR、OOS Sortino、OOS MDD 與最近一年 CAGR 都明顯優於 0050 與 2330 total return。這代表它通過「不能比長持 2330 差」這個門檻。

## Deployment Stage

| Stage | 目前狀態 |
|---|---|
| research_candidate | 已通過 |
| backtest_validated | Iter95 Global Exit-Aware Time50 r-1 已達成 |
| execution_ready | Iter95 Global Exit-Aware Time50 r-1 已達成 |
| live_pilot | 尚未達成 |
| production_scaled | 尚未達成 |

因此：

- 可以把 Iter95 視為目前最強且可 dry-run 產生訂單計畫的 execution-ready champion。
- 小額實盤前仍必須設定策略可動用資金上限，並跑 `smoke-test --accounting` 確認富邦登入、餘額與庫存查詢正常。
- 自動交易系統仍應 fail-closed：真實送單必須同時有 `--live` 與 `FUBON_DRY_RUN=false`。

## Artifacts

Iter95 可重跑腳本：

- `research/strat_lab/iter_95_global_exit_aware_search.py`

Iter95 主要結果：

- `var/out/strat_lab/iter_95_global_exit_aware_search_summary.csv`
- `var/out/strat_lab/iter_95_global_exit_aware_search_stage1_summary.csv`
- `var/out/strat_lab/iter_95_global_exit_aware_search_coarse_summary.csv`
- `var/out/strat_lab/iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_daily.csv`
- `var/out/strat_lab/iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_fills.csv`
- `var/out/strat_lab/iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_trades.csv`
- `var/out/strat_lab/iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_target_weights.csv`

Iter92 可重跑腳本：

- `research/strat_lab/iter_92_execution_meta_switch.py`

Iter92 主要結果：

- `var/out/strat_lab/iter_92_execution_meta_switch_summary.csv`
- `var/out/strat_lab/iter_92_execution_meta_switch_daily.csv`
- `var/out/strat_lab/iter_92_execution_meta_switch_fills.csv`
- `var/out/strat_lab/iter_92_execution_meta_switch_target_weights.csv`
- `var/out/strat_lab/iter_92_execution_meta_switch_state.csv`

研發搜尋摘要：

- `var/out/strat_lab/codex_meta_execution_search_fast/final_summary.csv`
