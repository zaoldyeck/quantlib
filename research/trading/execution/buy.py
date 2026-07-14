"""盤中買入執行器——被動撈價 → 限時升級 → 護欄內必成交。

用法:
    uv run --project research python -m research.trading.execution.buy \
        --code 2408 --qty 20                    # dry-run 模擬(預設)
    uv run --project research python -m research.trading.execution.buy \
        --plan research/out/trading/plans/serenity_daily_YYYYMMDD.json   # 吃 plan 的 Buy 腿
    ... --live    # 真實下單:需使用者自行設 FUBON_DRY_RUN=false + QL_STRATEGY_CAPITAL_TWD
"""

from __future__ import annotations

from ._cli import run

if __name__ == "__main__":
    run("Buy")
