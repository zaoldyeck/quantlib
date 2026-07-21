# Realistic execution road test — revmom_v2

- book: `quant_event_engine_v1_revmom_v2_book.csv`, 395 trade days, 329 codes
- fills at next-day open after paper close-decision (conservative shift)
- config: fubon_odd_lot (tiered fees, 5% participation cap, limit blocks, 5bps + impact slippage)

| series              |   cagr |   sharpe |   sortino |     mdd |   calmar |
|:--------------------|-------:|---------:|----------:|--------:|---------:|
| paper (5bps model)  | 0.3905 |   1.2266 |    1.5893 | -0.4182 |   0.9338 |
| realistic execution | 0.3558 |   1.1337 |    1.4993 | -0.4393 |   0.8099 |

- execution stats: `{'max_active': 12.0, 'trade_days': 395.0, 'avg_turnover_trade_day': 0.2287, 'requested_notional': 315870854.8769, 'filled_notional': 270846540.8284, 'fill_ratio': 0.8575, 'total_commission': 167741.7359, 'total_tax': 405630.4636, 'total_slippage_cost': 127267.5822, 'blocked_orders': 1245.0, 'partial_orders': 269.0, 'exit_orders': 0.0, 'exit_notional': 0.0, 'exit_notional_ratio': 0.0}`
