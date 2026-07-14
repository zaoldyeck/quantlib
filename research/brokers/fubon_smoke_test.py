"""Read-only Fubon Neo API smoke test.

The test logs in, selects the configured account, and reads today's order result
list. It never places, modifies, or cancels orders.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

from research.brokers.fubon import FubonBroker, classify_login_exception, redacted_account


OUT_PATH = Path("research/out/fubon_smoke_test.json")


def result_ok(result: object) -> bool:
    return bool(getattr(result, "is_success", False))


def result_count(result: object) -> int | None:
    data = getattr(result, "data", None)
    if data is None:
        return None
    try:
        return len(data)
    except TypeError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Run read-only Fubon SDK smoke test.")
    parser.add_argument("--out", type=Path, default=OUT_PATH)
    parser.add_argument("--login-method", choices=["apikey", "password"], default="apikey")
    args = parser.parse_args()

    broker = FubonBroker.from_env()
    try:
        account = broker.login(method=args.login_method)
        order_results = broker.get_order_results()
    except Exception as exc:  # noqa: BLE001 - smoke test must preserve broker error text.
        summary = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "dry_run": broker.dry_run,
            "login_method": args.login_method,
            **classify_login_exception(exc),
        }
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    summary = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "dry_run": broker.dry_run,
        "login_method": args.login_method,
        "login_success": True,
        "account": redacted_account(account),
        "order_results_success": result_ok(order_results),
        "order_results_count": result_count(order_results),
        "order_results_message": getattr(order_results, "message", None),
        "placed_order": False,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
