# Realistic execution road test — newhigh_rot

- book: `quant_event_engine_v1_newhigh_rot_book.csv`, 103 trade days, 516 codes
- fills at next-day open after paper close-decision (conservative shift)
- config: fubon_odd_lot (tiered fees, 5% participation cap, limit blocks, 5bps + impact slippage)

| series              |   cagr |   sharpe |   sortino |     mdd |   calmar |
|:--------------------|-------:|---------:|----------:|--------:|---------:|
| paper (5bps model)  | 0.7019 |   1.7151 |    2.4926 | -0.4266 |   1.6454 |
| realistic execution | 0.6800 |   1.6889 |    2.4606 | -0.4227 |   1.6087 |

- execution stats: `{'max_active': 14.0, 'trade_days': 103.0, 'avg_turnover_trade_day': 1.4293, 'requested_notional': 1953241232.1367, 'filled_notional': 1848507193.7069, 'fill_ratio': 0.9464, 'total_commission': 1024250.3169, 'total_tax': 2776962.8991, 'total_slippage_cost': 935093.51, 'blocked_orders': 155.0, 'partial_orders': 127.0, 'exit_orders': 0.0, 'exit_notional': 0.0, 'exit_notional_ratio': 0.0}`
