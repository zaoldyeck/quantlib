# Realistic execution road test — g_tp0.4_tr0.2_abnone_td50

- book: `b18cand_g_tp0.4_tr0.2_abnone_td50_book.csv`, 201 trade days, 45 codes
- fills at next-day open after paper close-decision (conservative shift)
- config: fubon_odd_lot (tiered fees, 5% participation cap, limit blocks, 5bps + impact slippage)

| series              |   cagr |   sharpe |   sortino |     mdd |   calmar |
|:--------------------|-------:|---------:|----------:|--------:|---------:|
| paper (5bps model)  | 0.9272 |   3.5318 |    4.6090 | -0.1807 |   5.1317 |
| realistic execution | 1.0189 |   3.6632 |    5.0605 | -0.2057 |   4.9535 |

- execution stats: `{'max_active': 11.0, 'trade_days': 201.0, 'avg_turnover_trade_day': 0.2178, 'requested_notional': 195033349.5525, 'filled_notional': 194502085.2796, 'fill_ratio': 0.9973, 'total_commission': 120125.0281, 'total_tax': 290869.915, 'total_slippage_cost': 88897.3095, 'blocked_orders': 172.0, 'partial_orders': 14.0, 'exit_orders': 0.0, 'exit_notional': 0.0, 'exit_notional_ratio': 0.0}`
