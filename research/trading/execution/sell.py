"""盤中賣出執行器——普通賣被動撈價;停損賣速度優先(--urgency stop)。

用法:
    uv run --project research python -m research.trading.execution.sell \
        --code 2059 --qty 2                     # dry-run 模擬(預設)
    uv run --project research python -m research.trading.execution.sell \
        --code 2059 --qty 2 --urgency stop      # 六道門觸發的停損賣:首輪即跨價
    ... --live    # 真實下單:需使用者自行設 FUBON_DRY_RUN=false + QL_STRATEGY_CAPITAL_TWD
"""

from __future__ import annotations

from ._cli import run

if __name__ == "__main__":
    run("Sell")
