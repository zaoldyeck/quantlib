# 臺指期進階多策略組合研究排行

日線資料截止：`2026-05-20`；tick-derived / intraday 資料截止：`2026-05-20`。本輪從已成本化的 daily / intraday sleeves 出發，加入 lagged equity-curve risk filter、vol targeting 與多策略 PM allocator。執行時間約 `283.0` 秒。

## 結論

本輪仍沒有候選同時通過 DSR、PBO、bootstrap、成本壓力、MDD 與保證金 gate。這代表它們仍只可作研究診斷，不能列為可上線臺指期 champion。

| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | PF | SQN |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | ECF_TX_cross_market_z0.8_tv0.45_nostop_tr4_lb42_min-0.04_dd0.2_tv0.5_cap1.5 | reject_dsr | +5.27% | +12.66% | -0.40% | -7.56% | +0.00% | +0.00% | -42.94% | 0.419 | 0.131 | 0.860 | 0.000 | 0.000 |
| 2 | ECF_MTX_technical_chip_vote_t3_tv0.3_nostop_tr4_lb42_min-0.04_dd0.2_tv0.5_cap1.5 | reject_dsr | +4.19% | +10.06% | +32.89% | +23.84% | +26.36% | +0.00% | -32.40% | 0.496 | 0.197 | 0.860 | 0.000 | 0.000 |
| 3 | ECF_MTX_technical_chip_vote_t2_tv0.3_nostop_tr4_lb42_min-0.04_dd0.2_tv0.5_cap1.5 | reject_mdd | +3.55% | +9.58% | +49.30% | +31.78% | +44.98% | -1.48% | -46.50% | 0.399 | 0.102 | 0.860 | 0.000 | 0.000 |
| 4 | ECF_TX_h_model_z1.25_v-1.0_tv0.45_nostop_tr4_lb42_min-0.04_dd0.2_tv0.5_cap1.5 | reject_dsr | +6.77% | +9.54% | -6.69% | -2.01% | +0.00% | +0.00% | -30.86% | 0.335 | 0.246 | 0.860 | 0.000 | 0.000 |
| 5 | ECF_TX_h_model_z0.5_v-1.0_tv0.45_sl2.5_tr4_lb42_min0_dd0.2_tv0.35_cap1.25 | reject_dsr | +4.56% | +8.31% | +0.94% | -6.22% | +0.00% | +0.00% | -41.27% | 0.340 | 0.137 | 0.860 | 0.000 | 0.000 |
| 6 | ECF_TX_h_model_z1.25_v-1.0_tv0.45_sl2.5_tr4_lb42_min-0.04_dd0.2_tv0.5_cap1.5 | reject_dsr | +6.00% | +8.21% | -4.28% | -2.97% | +0.00% | +0.00% | -30.51% | 0.276 | 0.175 | 0.860 | 0.000 | 0.000 |
| 7 | ECF_TX_h_model_flow_z0.75_v-0.5_tv0.45_nostop_tr4_lb126_min-0.04_dd0.25_tv0.5_cap1.5 | reject_dsr | +5.22% | +7.93% | -0.12% | -4.28% | +0.00% | +0.00% | -39.37% | 0.331 | 0.135 | 0.860 | 0.000 | 0.000 |
| 8 | ECF_TX_h_model_z0.5_v-0.5_tv0.45_nostop_tr4_lb126_min-0.04_dd0.25_tv0.5_cap1.5 | reject_mdd | +4.91% | +7.64% | -3.16% | +0.00% | +0.00% | +0.00% | -58.03% | 0.268 | 0.075 | 0.860 | 0.000 | 0.000 |
| 9 | ECF_TX_h_model_z0.5_v-1.0_tv0.45_sl2.5_tr4_lb126_min0_dd0.25_tv0.35_cap1.25 | reject_dsr | +5.52% | +7.62% | -6.55% | +0.00% | +0.00% | +0.00% | -37.41% | 0.306 | 0.093 | 0.860 | 0.000 | 0.000 |
| 10 | ECF_TX_h_model_z1.25_v-1.0_tv0.45_nostop_tr4_lb42_min0_dd0.2_tv0.35_cap1.25 | reject_dsr | +4.96% | +7.25% | -3.19% | -2.14% | +0.00% | +0.00% | -24.02% | 0.325 | 0.276 | 0.860 | 0.000 | 0.000 |
| 11 | ECF_TX_h_model_z1.25_v-1.0_tv0.45_nostop_tr4_lb126_min0_dd0.25_tv0.35_cap1.25 | reject_dsr | +5.02% | +7.14% | -2.63% | -0.02% | +0.00% | +0.00% | -22.72% | 0.331 | 0.242 | 0.860 | 0.000 | 0.000 |
| 12 | ECF_TX_basis_z1.25_tv0.45_nostop_tr4_lb126_min-0.04_dd0.25_tv0.5_cap1.5 | reject_dsr | +5.78% | +7.09% | -1.62% | -0.90% | +0.00% | +0.00% | -44.07% | 0.225 | 0.108 | 0.860 | 0.000 | 0.000 |
| 13 | ECF_TX_basis_z1.25_tv0.45_nostop_tr4_lb126_min0_dd0.25_tv0.35_cap1.25 | reject_dsr | +5.33% | +7.04% | -5.02% | -0.54% | +0.00% | +0.00% | -22.81% | 0.305 | 0.207 | 0.860 | 0.000 | 0.000 |
| 14 | ECF_MTX_technical_vote_t3_tv0.2_nostop_tr4_lb42_min-0.04_dd0.2_tv0.5_cap1.5 | reject_dsr | +4.27% | +6.88% | +36.21% | +18.56% | +24.61% | +0.00% | -33.93% | 0.412 | 0.108 | 0.860 | 0.000 | 0.000 |
| 15 | ECF_TX_basis_trend_z1.25_tv0.45_nostop_tr4_lb126_min-0.04_dd0.25_tv0.5_cap1.5 | reject_dsr | +4.77% | +6.69% | +6.77% | +3.67% | +0.00% | +0.00% | -24.39% | 0.176 | 0.170 | 0.860 | 0.000 | 0.000 |
| 16 | ECF_TX_basis_trend_z0.75_tv0.45_nostop_tr4_lb126_min-0.04_dd0.25_tv0.5_cap1.5 | reject_mdd | +5.13% | +6.67% | +3.23% | +3.23% | +0.00% | +0.00% | -46.08% | 0.201 | 0.102 | 0.860 | 0.000 | 0.000 |
| 17 | ECF_TX_basis_trend_z1.25_tv0.45_nostop_tr4_lb42_min-0.04_dd0.2_tv0.5_cap1.5 | reject_dsr | +4.70% | +6.64% | +10.69% | +3.30% | +0.00% | +0.00% | -30.17% | 0.179 | 0.164 | 0.860 | 0.000 | 0.000 |
| 18 | ECF_TX_h_model_z0.5_v-0.5_tv0.45_nostop_tr4_lb126_min0_dd0.25_tv0.35_cap1.25 | reject_dsr | +4.76% | +6.51% | -5.13% | +0.00% | +0.00% | +0.00% | -36.72% | 0.260 | 0.089 | 0.860 | 0.000 | 0.000 |

## 方法

- 所有風控 overlay 都只使用前一日以前的 sleeve NAV，沒有用當日或未來績效。
- `ECF_*` 是 equity-curve filter：rolling return 不足或 rolling drawdown 太深時停用該 sleeve，否則按已知波動調整曝險。
- `PM_ADV_*` 是多策略 allocator：根據 lagged return、vol、當前 drawdown、worst drawdown 分數，選 top sleeves 並做 inverse-vol allocation。
- Daily sleeves 已含期貨手續費、交易稅、滑價、roll cost、停損、追蹤停損與 time stop；intraday sleeves 已含手續費、交易稅、slippage、停損與停利。
- PM 組合目前是 daily-return 層的研究 simulator；若有策略通過 gate，仍需再升級成 position-level portfolio simulator 才能進 execution-ready。

## Artifacts

- `research/strat_lab/results/futures_tx_advanced/futures_advanced_summary.csv`
- `research/strat_lab/results/futures_tx_advanced/futures_advanced_base_summary.csv`
- `research/strat_lab/results/futures_tx_advanced/top_daily.csv`
- `research/strat_lab/results/futures_tx_advanced/top_weights.csv`
