# 臺指期 Session-Level 策略研究排行

RPT session features 截止：`2026-05-20`。本輪測試夜盤 -> 日盤、日盤 -> 夜盤與尾盤 -> 夜盤的 momentum / reversal session 策略，執行時間約 `532.0` 秒。

## 結論

本輪沒有候選通過嚴格 gate；結果仍只能作為研究診斷。

| 排名 | 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | 近 6 月 | 近 3 月 | 近 1 月 | OOS MDD | OOS Sortino | DSR | PBO | PF | SQN | Trades |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | TX_regular_to_night_follow_z1_r0.04_sl1.5_tpnone | reject_dsr | +0.31% | +0.60% | -5.13% | +0.00% | +0.00% | +0.00% | -15.32% | -0.047 | 0.000 | 0.548 | 1.087 | 0.677 | 363 |
| 2 | TX_regular_to_night_follow_z1_r0.04_sl1.5_tp2.0 | reject_dsr | +0.31% | +0.60% | -5.13% | +0.00% | +0.00% | +0.00% | -15.32% | -0.047 | 0.000 | 0.548 | 1.087 | 0.677 | 363 |
| 3 | TX_regular_to_night_follow_z1.25_r0.04_sl1.5_tp2.0 | reject_dsr | +0.10% | +0.20% | +1.11% | +0.00% | +0.00% | +0.00% | -17.65% | -0.089 | 0.000 | 0.548 | 1.064 | 0.391 | 221 |
| 4 | TX_regular_to_night_follow_z1.25_r0.04_sl1.5_tpnone | reject_dsr | +0.10% | +0.20% | +1.11% | +0.00% | +0.00% | +0.00% | -17.65% | -0.089 | 0.000 | 0.548 | 1.064 | 0.391 | 221 |
| 5 | MTX_night_to_regular_follow_z1.25_r0.01_sl1_tp2.0 | reject_dsr | +0.07% | +0.13% | -0.61% | -0.48% | +0.00% | +0.00% | -5.01% | -0.256 | 0.000 | 0.548 | 1.093 | 0.518 | 191 |
| 6 | MTX_night_to_regular_follow_z1.25_r0.01_sl1_tpnone | reject_dsr | +0.07% | +0.13% | -0.61% | -0.48% | +0.00% | +0.00% | -5.01% | -0.256 | 0.000 | 0.548 | 1.093 | 0.518 | 191 |
| 7 | TX_regular_to_night_follow_z1.25_r0.02_sl1.5_tp2.0 | reject_dsr | +0.05% | +0.10% | +0.00% | +0.00% | +0.00% | +0.00% | -6.46% | -0.177 | 0.000 | 0.548 | 1.127 | 0.478 | 89 |
| 8 | TX_regular_to_night_follow_z1.25_r0.02_sl1.5_tpnone | reject_dsr | +0.05% | +0.10% | +0.00% | +0.00% | +0.00% | +0.00% | -6.46% | -0.177 | 0.000 | 0.548 | 1.127 | 0.478 | 89 |
| 9 | TX_regular_to_night_follow_z1_r0.02_sl1_tpnone | reject_dsr | +0.04% | +0.08% | +0.46% | +0.00% | +0.00% | +0.00% | -15.49% | -0.127 | 0.000 | 0.548 | 1.058 | 0.365 | 235 |
| 10 | TX_regular_to_night_follow_z1_r0.02_sl1_tp2.0 | reject_dsr | +0.04% | +0.08% | +0.46% | +0.00% | +0.00% | +0.00% | -15.49% | -0.127 | 0.000 | 0.548 | 1.058 | 0.365 | 235 |
| 11 | TX_regular_to_night_follow_z1_r0.02_sl1.5_tp2.0 | reject_dsr | +0.03% | +0.07% | +0.00% | +0.00% | +0.00% | +0.00% | -8.43% | -0.194 | 0.000 | 0.548 | 1.091 | 0.422 | 130 |
| 12 | TX_regular_to_night_follow_z1_r0.02_sl1.5_tpnone | reject_dsr | +0.03% | +0.07% | +0.00% | +0.00% | +0.00% | +0.00% | -8.43% | -0.194 | 0.000 | 0.548 | 1.091 | 0.422 | 130 |
| 13 | MTX_night_to_regular_reversal_z1_r0.02_sl1.5_tpnone | reject_dsr | +0.04% | +0.07% | -2.20% | +0.11% | +0.00% | +0.00% | -8.15% | -0.212 | 0.000 | 0.548 | 1.043 | 0.352 | 379 |
| 14 | MTX_night_to_regular_reversal_z1_r0.02_sl1.5_tp2.0 | reject_dsr | +0.04% | +0.07% | -2.20% | +0.11% | +0.00% | +0.00% | -8.15% | -0.212 | 0.000 | 0.548 | 1.043 | 0.352 | 379 |
| 15 | TX_late_regular_to_night_reversal_z1.25_r0.005_sl1_tp2.0 | reject_dsr | +0.03% | +0.05% | +0.00% | +0.00% | +0.00% | +0.00% | -0.34% | -1.008 | 0.000 | 0.548 | 3.765 | 1.605 | 10 |
| 16 | TX_late_regular_to_night_reversal_z1.25_r0.005_sl1_tpnone | reject_dsr | +0.03% | +0.05% | +0.00% | +0.00% | +0.00% | +0.00% | -0.34% | -1.008 | 0.000 | 0.548 | 3.765 | 1.605 | 10 |
| 17 | TX_regular_to_night_follow_z1.25_r0.02_sl1_tpnone | reject_dsr | +0.02% | +0.05% | +0.47% | +0.00% | +0.00% | +0.00% | -13.64% | -0.134 | 0.000 | 0.548 | 1.059 | 0.297 | 154 |
| 18 | TX_regular_to_night_follow_z1.25_r0.02_sl1_tp2.0 | reject_dsr | +0.02% | +0.05% | +0.47% | +0.00% | +0.00% | +0.00% | -13.64% | -0.134 | 0.000 | 0.548 | 1.059 | 0.297 | 154 |

## 方法

- 日盤策略只使用已完成夜盤訊號；夜盤策略只使用已完成日盤或尾盤訊號。
- 每筆交易均扣手續費、交易稅與 slippage；同一 session 內停損與停利同時觸發時採 stop-first。
- 部位大小由每筆風險與保證金 survival constraint 限制，且重跑 2x/5x 成本壓力。

## Artifacts

- `var/out/strat_lab/futures_tx_session/session_strategy_summary.csv`
- `var/out/strat_lab/futures_tx_session/top_daily.csv`
- `var/out/strat_lab/futures_tx_session/top_trades.csv`
