# Realistic execution road test — blend_rot

- book: `quant_event_engine_v1_blend_rot_book.csv`, 102 trade days, 445 codes
- fills at next-day open after paper close-decision (conservative shift)
- config: fubon_odd_lot (tiered fees, 5% participation cap, limit blocks, 5bps + impact slippage)

| series              |   cagr |   sharpe |   sortino |     mdd |   calmar |
|:--------------------|-------:|---------:|----------:|--------:|---------:|
| paper (5bps model)  | 0.6834 |   1.6929 |    2.4649 | -0.4156 |   1.6445 |
| realistic execution | 0.6117 |   1.5445 |    2.2537 | -0.4092 |   1.4947 |

- execution stats: `{'max_active': 14.0, 'trade_days': 102.0, 'avg_turnover_trade_day': 1.2869, 'requested_notional': 1625262457.7567, 'filled_notional': 1528786288.25, 'fill_ratio': 0.9406, 'total_commission': 842733.4482, 'total_tax': 2296388.3707, 'total_slippage_cost': 781028.0446, 'blocked_orders': 177.0, 'partial_orders': 146.0, 'exit_orders': 0.0, 'exit_notional': 0.0, 'exit_notional_ratio': 0.0}`
