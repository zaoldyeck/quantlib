# 臺指期 Aggressive Leverage Probe

資料截止：`2026-05-20`。本文件只記錄研究診斷，不代表可上線策略。

## 結論

本輪測試 525 組 H-model、basis trend、term、technical-chip 類候選，在保證金 survival constraint 下把 target volatility 提高到 `0.45 / 0.70 / 1.00 / 1.30 / 1.60`，並重跑 2x / 5x 成本壓力。

結果沒有任何候選通過嚴格 gate：

- `reject_mdd`：301 組
- `reject_dsr`：224 組
- 通過數：0 組
- 群組 PBO：`0.596`

重點判斷：提高槓桿確實能把部分候選 OOS CAGR 拉高，但主要是放大既有曝險，不是創造更穩定的 alpha。高報酬版本的 OOS MDD 與最近一年虧損過大；低回撤版本的 DSR 仍明顯不足，距離 `DSR >= 0.95` 很遠。

## 代表性結果

| 策略 | Verdict | Full CAGR | OOS CAGR | 最近一年 CAGR | OOS MDD | OOS Sortino | DSR | Boot CAGR LB | 2x Cost OOS | 5x Cost OOS |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `TX_h_model_z0.5_v-1.0_probe_tv1.3_slnone_tr6.0` | reject_mdd | +15.52% | +21.48% | -47.31% | -83.66% | 0.424 | 0.033 | -14.92% | +26.11% | +11.01% |
| `TX_basis_trend_z1.25_probe_tv1.3_slnone_tr6.0` | reject_dsr | +11.00% | +13.55% | -0.28% | -35.38% | 0.294 | 0.145 | +0.10% | +21.53% | +18.37% |
| `TX_h_model_z1.25_v-1.0_probe_tv0.7_slnone_tr6.0` | reject_dsr | +8.59% | +13.05% | -12.71% | -36.39% | 0.432 | 0.147 | +3.12% | +14.79% | +13.15% |
| `TX_basis_trend_z1.25_probe_tv0.45_slnone_tr6.0` | reject_dsr | +3.74% | +5.54% | +6.58% | -13.43% | 0.336 | 0.213 | +0.90% | +7.16% | +6.54% |

## 研究判斷

這輪回答的是「槓桿是不是限制策略變強的主因」。答案是否定的。

如果只是放大 target volatility，OOS CAGR 可以上升，但 DSR、PBO、MDD 與最近一年表現無法同步改善。這代表目前瓶頸不是槓桿不足，而是訊號本身的穩定性不足。

後續如果要繼續推進，應優先尋找新的資訊來源或更正確的可交易微結構訊號，而不是繼續加槓桿或微調停損參數。

## Artifacts

- `var/out/strat_lab/futures_tx_aggressive_probe/aggressive_probe_summary.csv`
