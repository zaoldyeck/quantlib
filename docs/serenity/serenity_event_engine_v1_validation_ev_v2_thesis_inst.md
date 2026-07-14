# serenity_event_engine_v1 validation — ev_v2_thesis_inst

- campaign trial count for DSR: 40

| name                              | window                |   cagr |   sharpe |   sortino |     mdd |   lo_t |   lo_p |    dsr |    pbo |   boot_cagr_lb95 |   boot_sortino_lb95 |   folds |   fold_cagr_min |   fold_pos_share |
|:----------------------------------|:----------------------|-------:|---------:|----------:|--------:|-------:|-------:|-------:|-------:|-----------------:|--------------------:|--------:|----------------:|-----------------:|
| registry_lag0/ev_v2_thesis_inst   | 2025-01-03~2026-07-14 | 2.8971 |   7.7188 |    9.9690 | -0.1693 | 4.5890 | 0.0000 | 1.0000 | 0.5260 |           1.1138 |              4.0837 |       6 |         -0.2823 |           0.8333 |
| registry_lag90/ev_v2_thesis_inst  | 2025-01-03~2026-07-14 | 2.1018 |   5.8753 |    7.0548 | -0.1684 | 4.0602 | 0.0000 | 1.0000 | 0.5940 |           0.8803 |              2.6240 |       6 |          0.0000 |           0.8333 |
| registry_lag180/ev_v2_thesis_inst | 2025-01-03~2026-07-14 | 1.6764 |   4.7925 |    5.8994 | -0.1415 | 3.7387 | 0.0001 | 0.9996 | 0.7780 |           0.6866 |              2.2927 |       6 |          0.0000 |           0.6667 |
| mech_2018/ev_v2_thesis_inst       | 2018-01-03~2026-07-06 | 0.2110 |   0.7343 |    0.9413 | -0.3618 | 2.4464 | 0.0072 | 0.4586 | 0.4140 |         nan      |            nan      |       9 |         -0.2124 |           0.7778 |

- selection permutation: {'registry_lag0': {'n_perm': 200, 'perm_cagr_med': 1.4665067907735374, 'perm_cagr_p95': 1.9559932270809595, 'p_value': 0.0}}
- champion blend: {'overlap': '2025-01-02~2026-05-22', 'corr_daily': 0.6197646484195097, 'engine_cagr': np.float64(3.5912008641565745), 'champion_cagr': np.float64(1.8926309376004151), 'blend_cagr': np.float64(2.694064376672363), 'engine_mdd': np.float64(-0.16933710422819037), 'champion_mdd': np.float64(-0.2126717200027068), 'blend_mdd': np.float64(-0.15272770172531291), 'blend_sortino': np.float64(11.486196637710103)}