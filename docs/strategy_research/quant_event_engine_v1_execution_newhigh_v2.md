# Realistic execution road test — newhigh_v2

- book: `quant_event_engine_v1_newhigh_v2_book.csv`, 423 trade days, 375 codes
- fills at next-day open after paper close-decision (conservative shift)
- config: fubon_odd_lot (tiered fees, 5% participation cap, limit blocks, 5bps + impact slippage)

| series              |   cagr |   sharpe |   sortino |     mdd |   calmar |
|:--------------------|-------:|---------:|----------:|--------:|---------:|
| paper (5bps model)  | 0.4513 |   1.3749 |    1.7599 | -0.4292 |   1.0515 |
| realistic execution | 0.4529 |   1.3841 |    1.8133 | -0.4342 |   1.0432 |

- execution stats: `{'max_active': 13.0, 'trade_days': 423.0, 'avg_turnover_trade_day': 0.2499, 'requested_notional': 656779590.0024, 'filled_notional': 586478715.2502, 'fill_ratio': 0.893, 'total_commission': 351408.0572, 'total_tax': 880065.5936, 'total_slippage_cost': 272562.9263, 'blocked_orders': 947.0, 'partial_orders': 211.0, 'exit_orders': 0.0, 'exit_notional': 0.0, 'exit_notional_ratio': 0.0}`
