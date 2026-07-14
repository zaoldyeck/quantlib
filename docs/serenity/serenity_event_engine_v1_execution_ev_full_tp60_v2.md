# Realistic execution road test — ev_full_tp60_v2

- book: `serenity_event_engine_v1_ev_full_tp60_v2_book.csv`, 73 trade days, 30 codes
- fills at next-day open after paper close-decision (conservative shift)
- config: fubon_odd_lot (tiered fees, 5% participation cap, limit blocks, 5bps + impact slippage)

| series              |   cagr |   sharpe |   sortino |     mdd |   calmar |
|:--------------------|-------:|---------:|----------:|--------:|---------:|
| paper (5bps model)  | 2.3070 |   6.2864 |    8.2391 | -0.2180 |  10.5805 |
| realistic execution | 2.5026 |   6.7262 |    9.0508 | -0.2049 |  12.2109 |

- execution stats: `{'max_active': 11.0, 'trade_days': 73.0, 'avg_turnover_trade_day': 0.2431, 'requested_notional': 48842097.4622, 'filled_notional': 48819682.0086, 'fill_ratio': 0.9995, 'total_commission': 29866.2033, 'total_tax': 71882.7032, 'total_slippage_cost': 22327.5901, 'blocked_orders': 178.0, 'partial_orders': 4.0, 'exit_orders': 0.0, 'exit_notional': 0.0, 'exit_notional_ratio': 0.0}`
