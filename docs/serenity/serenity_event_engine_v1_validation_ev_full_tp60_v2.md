# serenity_event_engine_v1 validation — ev_full_tp60_v2

- campaign trial count for DSR: 40

| name                            | window                |   cagr |   sharpe |   sortino |     mdd |   lo_t |   lo_p |    dsr |    pbo |   boot_cagr_lb95 |   boot_sortino_lb95 |   folds |   fold_cagr_min |   fold_pos_share |
|:--------------------------------|:----------------------|-------:|---------:|----------:|--------:|-------:|-------:|-------:|-------:|-----------------:|--------------------:|--------:|----------------:|-----------------:|
| registry_lag0/ev_full_tp60_v2   | 2025-01-03~2026-07-03 | 2.3142 |   6.2980 |    8.2651 | -0.2180 | 4.1171 | 0.0000 | 1.0000 | 0.4760 |           0.9463 |              3.3149 |       6 |         -0.3615 |           0.6667 |
| registry_lag90/ev_full_tp60_v2  | 2025-01-03~2026-07-03 | 2.1075 |   6.2892 |    8.2434 | -0.1042 | 4.3828 | 0.0000 | 1.0000 | 0.6440 |           1.0931 |              4.4189 |       6 |          0.0000 |           0.8333 |
| registry_lag180/ev_full_tp60_v2 | 2025-01-03~2026-07-03 | 1.7946 |   5.2295 |    6.3206 | -0.1174 | 3.9425 | 0.0000 | 0.9999 | 0.7780 |           0.8909 |              3.1620 |       6 |          0.0000 |           0.6667 |

- selection permutation: {'registry_lag0': {'n_perm': 200, 'perm_cagr_med': 1.321767893675176, 'perm_cagr_p95': 1.7959463258556183, 'p_value': 0.0}}
- champion blend: {'overlap': '2025-01-02~2026-05-22', 'corr_daily': 0.6195524474571136, 'engine_cagr': np.float64(2.2558130129719656), 'champion_cagr': np.float64(1.8926309376004151), 'blend_cagr': np.float64(2.109874655192607), 'engine_mdd': np.float64(-0.21803861424897222), 'champion_mdd': np.float64(-0.2126717200027068), 'blend_mdd': np.float64(-0.17699488457852344), 'blend_sortino': np.float64(8.650534668209284)}