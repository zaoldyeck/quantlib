"""撤銷帳上未完成委託(工具;升格自 scratch/cancel_pre_orders.py)。

用法:
    uv run --project . python -m quantlib.trading.execution.cancel_all           # 列出+dry-run
    uv run --project . python -m quantlib.trading.execution.cancel_all --live    # 真實撤單(使用者自行執行)
    ... --code 2408    # 只撤單一代碼
"""

from __future__ import annotations

import argparse

from quantlib.brokers.fubon import FubonBroker

# 官方終態:30 未成交刪單、40 部分成交剩餘取消、50 完全成交、90 失敗
_TERMINAL = {30, 40, 50, 90}


def main() -> None:
    ap = argparse.ArgumentParser(description="撤銷帳上所有(或指定代碼)未完成委託")
    ap.add_argument("--code", default=None, help="只撤這個代碼")
    ap.add_argument("--live", action="store_true", help="真實撤單(預設只列出)")
    args = ap.parse_args()

    broker = FubonBroker.from_env()
    broker.login()
    orders = broker.get_order_results()
    active = [
        o for o in (getattr(orders, "data", []) or [])
        if getattr(o, "status", None) not in _TERMINAL
        and (args.code is None or str(getattr(o, "stock_no", "")) == str(args.code))
    ]
    if not active:
        print("沒有未完成委託。")
        return
    for o in active:
        print(f"- {getattr(o, 'stock_no', '?')} {getattr(o, 'buy_sell', '?')} "
              f"qty {getattr(o, 'quantity', '?')} @ {getattr(o, 'price', '?')} "
              f"(seq {getattr(o, 'seq_no', '?')}, status {getattr(o, 'status', '?')})")
    if not args.live:
        print(f"\n[dry-run] 共 {len(active)} 筆;加 --live 執行真實撤單(使用者自行執行)。")
        return
    for o in active:
        try:
            res = broker.sdk.stock.cancel_order(broker.account, o)
            ok = bool(getattr(res, "is_success", False))
            print(f"cancel seq {getattr(o, 'seq_no', '?')} → {'OK' if ok else getattr(res, 'message', 'FAIL')}")
        except Exception as exc:  # noqa: BLE001
            print(f"cancel seq {getattr(o, 'seq_no', '?')} → 例外 {exc}")


if __name__ == "__main__":
    main()
