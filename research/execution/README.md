# Execution Validation Architecture

This package owns broker-aware target-book execution simulation. It is the
boundary between research NAV strategies and broker order planning.

## Module Boundaries

| Layer | Owner | Responsibility |
|---|---|---|
| Strategy research | `research/strat_lab/iter_*.py` | Generate validated daily NAV and target weights |
| Execution simulation | `research/execution/` | Convert target weights into broker-like fills and executable NAV |
| Strategy validation | `research/strat_lab/validator.py` | Compute canonical KPI, OOS diagnostics, DSR, PBO, and recent 1Y CAGR |
| Trading registry | `research/trading/strategy_registry.py` | Decide which strategy stage is allowed for automation |
| Broker runner | `research/trading/auto_trader.py` | Create dry-run or live broker plans only for eligible strategies |

## Design Rules

- Execution validation starts from target weights, not from already-computed NAV.
- Price simulation uses total-return-adjusted OHLC so dividend-adjusted strategy
  performance remains consistent.
- Liquidity checks use raw volume and trade value, not adjusted prices.
- Broker fees, sell tax, slippage, volume caps, lot rounding, and limit-up/down
  blocking are modeled before any strategy can be considered execution-ready.
- KPI calculations stay in the shared strategy validator. Execution modules do
  not define their own CAGR, Sortino, DSR, PBO, or recent-1Y logic.
- `execution_ready` requires a strategy to remain attractive after this
  execution layer, not only in the source NAV backtest.

## Current Finding

Iter87 validates Iter86 Dual Max through this execution layer. The source NAV
champion remains `backtest_validated`. After loading both TWSE and TPEx bars,
the best realistic Fubon execution case remains above the 2330 total-return OOS
CAGR benchmark, but the strategy must not be promoted to `execution_ready`
until broker order-plan generation and dry-run reconciliation are implemented.
