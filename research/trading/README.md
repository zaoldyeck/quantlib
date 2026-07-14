# Automated Trading Runner

This package is stage-gated.

Strategy stages are defined in `strategy_registry.py`:

| Stage | Meaning | Automation permission |
|---|---|---|
| `research_candidate` | idea / exploratory result | no trading |
| `backtest_validated` | strict professional backtest passed | research, reports, shadow work |
| `execution_ready` | target-book/order implementation reconciles | dry-run and order planning |
| `live_pilot` | approved for small-capital live test | small-capital live trading |
| `production_scaled` | approved for unattended large capital | scaled live trading |

`auto_trader.py` refuses to trade unless a strategy is at least
`execution_ready`. A strong backtest alone is not enough for broker orders.
When multiple strategies eventually qualify, the runner selects only the single
strongest eligible strategy from the registry.

The current execution-ready strategy is **Serenity `ev_v2_thesis_inst`**
(`serenity_ev_v2_thesis_inst` in the registry; Iter95 remains the pure-quant
reference). Live operation runs through the Serenity daily loop
(`research/serenity/daily.py`).

**盤中執行(2026-07-09 起)**:實際下單由 `research/trading/execution/`
盤中執行器負責(階梯/結構錨定/接管冪等/TCA)——**完整使用手冊見
`execution/README.md`**(參數全表、plan 格式、模式矩陣、安全機制、故障排除)。
`submit-plan` 的一次性 LimitUp 送單保留為備援路徑。

## Commands

```bash
uv run --project research python -m research.trading.auto_trader status
uv run --project research python -m research.trading.auto_trader smoke-test --accounting
uv run --project research python -m research.trading.auto_trader smoke-test --login-method password
uv run --project research python -m research.trading.auto_trader capital-check
uv run --project research python -m research.trading.auto_trader plan
uv run --project research python -m research.trading.auto_trader run-after-close
uv run --project research python -m research.trading.auto_trader submit-plan research/out/trading/plans/<plan>.json
uv run --project research python -m research.trading.auto_trader reconcile-plan research/out/trading/plans/<plan>.json --write
uv run --project research python -m research.trading.auto_trader run-daily
```

`run-after-close` runs the data refresh sequence, rebuilds the DuckDB cache, and
writes an immutable order plan for the next session. If the refresh has already
been completed and verified on the same Taiwan calendar day, use
`--skip-refresh` for local testing.

Fubon's first API login can return an activation-pending message that includes
`連線測試成功` and says access will open the next day. The smoke test classifies
that as `connection_test_success_pending_activation`, not as a broker
connectivity failure, and still records `placed_order=false`.

Use `--login-method password` to match Fubon's document step
`sdk.login(person_id, login_password, cert_path, cert_password)`. Use
`--login-method apikey` after API-key mode is active.

`submit-plan` is a separate next-session step. This is intentional: the strategy
decides after the close, but order submission should happen on the next trading
morning unless the broker explicitly supports the exact reservation-order
semantics needed by the plan.

`reconcile-plan` must run after live orders have had a chance to fill. It reads
broker filled history by the plan `user_def` tag and updates the managed ledger
only when `--write` is passed. `--assume-filled` exists only for manual recovery
after you have independently confirmed fills; do not use it as normal live
reconciliation.

`run-daily` is kept as a compatibility alias for generating the current plan. It
does not refresh data.

Live trading requires both:

```bash
uv run --project research python -m research.trading.auto_trader submit-plan research/out/trading/plans/<plan>.json --live
```

and `FUBON_DRY_RUN=false` in `research/.env`.

Before any live submit, the runner also queries Fubon inventory and compares it
with the local managed ledger for all planned symbols. If broker inventory and
managed ledger disagree, submission fails closed. This prevents duplicate buys
or selling manually held shares by mistake.

## Capital Controls

Set `QL_STRATEGY_CAPITAL_TWD` in `research/.env`. This is a mandatory capital
ceiling. Even though Fubon Neo API can query `accounting.bank_remain`, the bot
must not assume all brokerage cash belongs to this strategy.

Default live-pilot settings:

```bash
QL_STRATEGY_CAPITAL_TWD=50000
QL_CASH_BUFFER_PCT=0.03
QL_ORDER_PRICE_POLICY=limit_up_down
QL_BUY_PRICE_BUFFER_PCT=0.10
```

`NT$50,000` is the practical minimum strategy-verification size for the current
Iter95 target basket. `NT$1,000,000` remains the backtest-comparable size, not
the minimum required to test live automation.

The local managed-position ledger lives at
`research/state/trading/managed_positions.json`. It exists because brokerage
inventory cannot distinguish bot-managed shares from manual holdings in the same
account. Start a pilot from an empty ledger or import positions explicitly.
