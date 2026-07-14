# Realistic execution road test — ev_v2_thesis_inst

- book: `serenity_event_engine_v1_ev_v2_thesis_inst_book.csv`, 81 trade days, 31 codes
- fills at next-day open after paper close-decision (conservative shift)
- config: fubon_odd_lot (tiered fees, 5% participation cap, limit blocks, 5bps + impact slippage)

| series              |   cagr |   sharpe |   sortino |     mdd |   calmar |
|:--------------------|-------:|---------:|----------:|--------:|---------:|
| paper (5bps model)  | 2.5252 |   6.8252 |    8.9392 | -0.1804 |  13.9976 |
| realistic execution | 2.7156 |   7.2784 |    9.8242 | -0.1724 |  15.7535 |

- execution stats: `{'max_active': 10.0, 'trade_days': 81.0, 'avg_turnover_trade_day': 0.2478, 'requested_notional': 59921273.197, 'filled_notional': 58065458.011, 'fill_ratio': 0.969, 'total_commission': 35718.3195, 'total_tax': 85780.6194, 'total_slippage_cost': 26525.6207, 'blocked_orders': 197.0, 'partial_orders': 8.0, 'exit_orders': 0.0, 'exit_notional': 0.0, 'exit_notional_ratio': 0.0}`
