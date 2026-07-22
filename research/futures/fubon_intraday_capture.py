"""Capture Fubon Neo futures intraday market data.

This is a read-only data collector. It logs in only to unlock market-data
permission, initializes the market-data client, then saves the current session's
available futures data. It never submits, modifies, or cancels orders.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import time
from typing import Any

from research.brokers.fubon import FubonBroker, classify_login_exception, redacted_account
from research import paths


DEFAULT_PRODUCTS = ("TXF", "MXF", "TMF", "EXF", "FXF")
DEFAULT_TIMEFRAMES = ("1", "5", "10", "15", "30", "60")


def _json_default(value: Any) -> str:
    return str(value)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")


def response_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
    data = getattr(payload, "data", None)
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    return []


def sort_contracts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def key(row: dict[str, Any]) -> tuple[str, str]:
        return (str(row.get("settlementDate") or row.get("endDate") or "9999-99-99"), str(row.get("symbol") or ""))

    return sorted(rows, key=key)


class RateLimiter:
    def __init__(self, min_interval_seconds: float) -> None:
        self.min_interval_seconds = min_interval_seconds
        self.last_call = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self.last_call
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)
        self.last_call = time.monotonic()


def call_api(limiter: RateLimiter, fn: Any, **kwargs: Any) -> Any:
    limiter.wait()
    return fn(**kwargs)


def endpoint_session_params(session: str) -> dict[str, str]:
    return {"session": "afterhours"} if session == "AFTERHOURS" else {}


def capture(args: argparse.Namespace) -> dict[str, Any]:
    broker = FubonBroker.from_env()
    account = broker.login(method=args.login_method)
    broker.sdk.init_realtime()
    intraday = broker.sdk.marketdata.rest_client.futopt.intraday
    limiter = RateLimiter(args.min_interval_seconds)

    requested_sessions = ["AFTERHOURS", "REGULAR"] if args.session == "auto" else [args.session.upper()]
    selected_session = ""
    product_contracts: dict[str, list[dict[str, Any]]] = {}
    products_payload: dict[str, Any] = {}
    tickers_payload: dict[str, Any] = {}

    for session in requested_sessions:
        products_payload[session] = call_api(
            limiter,
            intraday.products,
            type="FUTURE",
            exchange="TAIFEX",
            session=session,
            contractType="I",
        )
        for product in args.products:
            payload = call_api(
                limiter,
                intraday.tickers,
                type="FUTURE",
                exchange="TAIFEX",
                session=session,
                product=product,
            )
            tickers_payload[f"{session}_{product}"] = payload
            rows = sort_contracts(response_rows(payload))
            if rows:
                product_contracts[product] = rows[: args.max_contracts]
        if product_contracts:
            selected_session = session
            break

    if not selected_session:
        raise RuntimeError("Fubon futures intraday returned no active contracts for requested products")

    now = datetime.now().isoformat(timespec="seconds")
    root = args.out_dir / datetime.now().strftime("%Y-%m-%d") / selected_session.lower()
    write_json(root / "_products.json", products_payload)
    write_json(root / "_tickers.json", tickers_payload)

    endpoint_session = endpoint_session_params(selected_session)
    captured_symbols: list[str] = []
    files_written: list[str] = []
    errors: list[dict[str, str]] = []

    for product, contracts in product_contracts.items():
        for contract in contracts:
            symbol = str(contract.get("symbol"))
            if not symbol:
                continue
            captured_symbols.append(symbol)
            symbol_dir = root / product / symbol
            write_json(symbol_dir / "contract.json", contract)

            for timeframe in args.timeframes:
                try:
                    payload = call_api(limiter, intraday.candles, symbol=symbol, timeframe=timeframe, **endpoint_session)
                    path = symbol_dir / "candles" / f"{timeframe}.json"
                    write_json(path, payload)
                    files_written.append(str(path))
                except Exception as exc:  # noqa: BLE001 - collector records endpoint-level failures.
                    errors.append({"symbol": symbol, "endpoint": f"candles/{timeframe}", "error": f"{type(exc).__name__}: {exc}"})

            for endpoint_name, fn in (("quote", intraday.quote), ("volumes", intraday.volumes)):
                try:
                    payload = call_api(limiter, fn, symbol=symbol, **endpoint_session)
                    path = symbol_dir / f"{endpoint_name}.json"
                    write_json(path, payload)
                    files_written.append(str(path))
                except Exception as exc:  # noqa: BLE001
                    errors.append({"symbol": symbol, "endpoint": endpoint_name, "error": f"{type(exc).__name__}: {exc}"})

            all_trades: list[dict[str, Any]] = []
            for page in range(args.max_trade_pages):
                try:
                    payload = call_api(
                        limiter,
                        intraday.trades,
                        symbol=symbol,
                        offset=page * args.trade_limit,
                        limit=args.trade_limit,
                        **endpoint_session,
                    )
                    rows = response_rows(payload)
                    if not rows:
                        break
                    all_trades.extend(rows)
                    if len(rows) < args.trade_limit:
                        break
                except Exception as exc:  # noqa: BLE001
                    errors.append({"symbol": symbol, "endpoint": f"trades/page/{page}", "error": f"{type(exc).__name__}: {exc}"})
                    break
            path = symbol_dir / "trades.json"
            write_json(path, {"symbol": symbol, "session": selected_session, "data": all_trades})
            files_written.append(str(path))

    summary = {
        "timestamp": now,
        "placed_order": False,
        "login_success": True,
        "account": redacted_account(account),
        "selected_session": selected_session,
        "products": list(args.products),
        "timeframes": list(args.timeframes),
        "symbols": captured_symbols,
        "files_written": files_written,
        "errors": errors,
        "out_dir": str(root),
        "source": "Fubon Neo futures market-data intraday API",
    }
    try:
        broker.sdk.logout()
        summary["logout_success"] = True
    except Exception as exc:  # noqa: BLE001
        summary["logout_success"] = False
        summary["logout_error"] = f"{type(exc).__name__}: {exc}"
    write_json(root / "_manifest.json", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only Fubon futures intraday market-data capture.")
    parser.add_argument("--out-dir", type=Path, default=Path("data/fubon/futures_intraday"))
    parser.add_argument("--login-method", choices=["apikey", "password"], default="apikey")
    parser.add_argument("--session", choices=["auto", "REGULAR", "AFTERHOURS"], default="auto")
    parser.add_argument("--products", nargs="+", default=list(DEFAULT_PRODUCTS))
    parser.add_argument("--timeframes", nargs="+", default=list(DEFAULT_TIMEFRAMES))
    parser.add_argument("--max-contracts", type=int, default=2)
    parser.add_argument("--trade-limit", type=int, default=1000)
    parser.add_argument("--max-trade-pages", type=int, default=20)
    parser.add_argument("--min-interval-seconds", type=float, default=0.25)
    args = parser.parse_args()

    try:
        summary = capture(args)
    except Exception as exc:  # noqa: BLE001
        summary = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "placed_order": False,
            **classify_login_exception(exc),
        }
        write_json(Path(f"{paths.OUT}/fubon_futures_marketdata_capture_error.json"), summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    main()
