# serenity_event_engine_v1 validation — ev_v2_thesis_inst

- campaign trial count for DSR: 40

| name                              | window                |   cagr |   sharpe |   sortino |     mdd |   lo_t |   lo_p |    dsr |    pbo |   boot_cagr_lb95 |   boot_sortino_lb95 |   folds |   fold_cagr_min |   fold_pos_share |
|:----------------------------------|:----------------------|-------:|---------:|----------:|--------:|-------:|-------:|-------:|-------:|-----------------:|--------------------:|--------:|----------------:|-----------------:|
| registry_lag0/ev_v2_thesis_inst   | 2025-01-03~2026-07-06 | 2.5333 |   6.8382 |    8.9680 | -0.1804 | 4.2883 | 0.0000 | 1.0000 | 0.4760 |           1.0250 |              3.6583 |       6 |         -0.3667 |           0.8333 |
| registry_lag90/ev_v2_thesis_inst  | 2025-01-03~2026-07-06 | 1.9767 |   5.7489 |    7.2854 | -0.1180 | 4.1370 | 0.0000 | 1.0000 | 0.6440 |           1.0380 |              3.8680 |       6 |          0.0000 |           0.8333 |
| registry_lag180/ev_v2_thesis_inst | 2025-01-03~2026-07-06 | 1.7997 |   5.2354 |    6.2913 | -0.1334 | 3.8960 | 0.0000 | 0.9999 | 0.7780 |           0.9284 |              3.1534 |       6 |          0.0000 |           0.6667 |
| mech_2018/ev_v2_thesis_inst       | 2018-01-03~2026-07-06 | 0.2110 |   0.7343 |    0.9413 | -0.3618 | 2.4464 | 0.0072 | 0.4586 | 0.4140 |         nan      |            nan      |       9 |         -0.2124 |           0.7778 |

- selection permutation: {'registry_lag0': {'n_perm': 200, 'perm_cagr_med': 1.2785240253315224, 'perm_cagr_p95': 1.718521673165579, 'p_value': 0.0}}
- champion blend: {'overlap': '2025-01-02~2026-05-22', 'corr_daily': 0.6276925363298378, 'engine_cagr': np.float64(2.5780817316216456), 'champion_cagr': np.float64(1.8926309376004151), 'blend_cagr': np.float64(2.258917720698908), 'engine_mdd': np.float64(-0.18040363180005942), 'champion_mdd': np.float64(-0.2126717200027068), 'blend_mdd': np.float64(-0.15949252801555777), 'blend_sortino': np.float64(9.556288239948485)}