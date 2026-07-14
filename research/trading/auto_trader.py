"""Stage-gated automated trading runner.

The runner is intentionally conservative:

1. It refuses to trade when no execution-ready strategy is registered.
2. It selects only the single strongest eligible strategy.
3. It defaults to dry-run.
4. Live execution requires both `--live` and `FUBON_DRY_RUN=false`.
5. The after-close research step writes an order plan; submission is a separate
   next-session step.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime
import json
import os
from pathlib import Path
import subprocess
from typing import Any

import duckdb

from research.brokers.fubon import FubonBroker, StockOrderRequest, classify_login_exception
from research.trading.live_config import LiveTradingConfig
from research.trading.order_planner import OrderPlan, build_order_plan
from research.trading.portfolio import (
    apply_trade_fills,
    available_balance_from_fubon_bank_remain,
    fills_from_fubon_result,
    fills_from_order_plan,
    inventory_mismatches,
    load_managed_positions,
    positions_from_fubon_inventories,
    save_managed_positions,
)
from research.trading.strategy_registry import (
    StrategyStage,
    best_backtest_validated_strategy,
    best_execution_ready_strategy,
)


CACHE_DB = Path("research/cache.duckdb")
OUT_DIR = Path("research/out/trading")


@dataclass(frozen=True)
class TradingStatus:
    timestamp: str
    latest_market_data_date: str | None
    selection_policy: str
    backtest_validated_strategy_available: bool
    backtest_validated_strategy_id: str | None
    backtest_validated_stage: str | None
    execution_ready_strategy_available: bool
    execution_ready_strategy_id: str | None
    execution_blocker: str | None
    broker_env_ready: bool
    cert_exists: bool
    dry_run: bool
    strategy_capital_twd: float | None
    managed_positions_path: str
    plans_dir: str


def latest_market_data_date(cache_db: Path = CACHE_DB) -> date | None:
    if not cache_db.exists():
        return None
    with duckdb.connect(str(cache_db), read_only=True) as con:
        value = con.execute("select max(date) from daily_quote").fetchone()[0]
    return value


def broker_env_status() -> tuple[bool, bool, bool]:
    from research.brokers.fubon import DEFAULT_ENV_PATH, load_env_file

    load_env_file(DEFAULT_ENV_PATH)
    required = ["FUBON_PERSON_ID", "FUBON_API_KEY", "FUBON_CERT_PATH", "FUBON_CERT_PASSWORD"]
    env_ready = all(bool(os.environ.get(key)) for key in required)
    cert_path = os.environ.get("FUBON_CERT_PATH")
    cert_exists = bool(cert_path and Path(cert_path).expanduser().exists())
    dry_run = os.environ.get("FUBON_DRY_RUN", "true").lower() not in {"0", "false", "no"}
    return env_ready, cert_exists, dry_run


def live_config_status() -> LiveTradingConfig:
    return LiveTradingConfig.from_env(require_capital=False)


def status() -> TradingStatus:
    try:
        backtest_strategy = best_backtest_validated_strategy()
        backtest_available = True
        backtest_id = backtest_strategy.strategy_id
        backtest_stage = backtest_strategy.stage.value
        blocker = backtest_strategy.blocker
    except RuntimeError:
        backtest_available = False
        backtest_id = None
        backtest_stage = None
        blocker = None

    try:
        execution_strategy = best_execution_ready_strategy()
        execution_available = True
        execution_id = execution_strategy.strategy_id
        blocker = None
    except RuntimeError:
        execution_available = False
        execution_id = None

    env_ready, cert_exists, dry_run = broker_env_status()
    live_config = live_config_status()
    latest_date = latest_market_data_date()
    return TradingStatus(
        timestamp=datetime.now().isoformat(timespec="seconds"),
        latest_market_data_date=latest_date.isoformat() if latest_date else None,
        selection_policy="single_strongest_strategy_only",
        backtest_validated_strategy_available=backtest_available,
        backtest_validated_strategy_id=backtest_id,
        backtest_validated_stage=backtest_stage,
        execution_ready_strategy_available=execution_available,
        execution_ready_strategy_id=execution_id,
        execution_blocker=blocker,
        broker_env_ready=env_ready,
        cert_exists=cert_exists,
        dry_run=dry_run,
        strategy_capital_twd=(
            live_config.strategy_capital_twd if live_config.strategy_capital_twd > 0 else None
        ),
        managed_positions_path=str(live_config.managed_positions_path),
        plans_dir=str(live_config.plans_dir),
    )


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_status(args: argparse.Namespace) -> None:
    payload = asdict(status())
    write_json(args.out, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def run_smoke_test(args: argparse.Namespace) -> None:
    broker = FubonBroker.from_env()
    try:
        account = broker.login(method=args.login_method)
        orders = broker.get_order_results()
    except Exception as exc:  # noqa: BLE001 - preserve broker-side read-only failure.
        payload = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "login_method": args.login_method,
            **classify_login_exception(exc),
        }
        write_json(args.out, payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "login_method": args.login_method,
        "login_success": True,
        "order_results_success": bool(getattr(orders, "is_success", False)),
        "order_results_count": len(getattr(orders, "data", []) or []),
        "placed_order": False,
        "account_type": str(getattr(account, "account_type", None)),
    }
    if args.accounting:
        bank = broker.get_bank_remain()
        inv = broker.get_inventories()
        payload["bank_remain_success"] = bool(getattr(bank, "is_success", False))
        payload["inventories_success"] = bool(getattr(inv, "is_success", False))
        if getattr(bank, "is_success", False):
            payload["available_balance_twd"] = available_balance_from_fubon_bank_remain(bank)
        if getattr(inv, "is_success", False):
            payload["inventory_symbols_count"] = len(positions_from_fubon_inventories(inv))
    write_json(args.out, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _broker_available_balance(args: argparse.Namespace) -> float | None:
    if not getattr(args, "query_broker_balance", False):
        return None
    broker = FubonBroker.from_env()
    result = broker.get_bank_remain()
    return available_balance_from_fubon_bank_remain(result)


def _build_plan(args: argparse.Namespace) -> OrderPlan:
    strategy = best_execution_ready_strategy()
    config = LiveTradingConfig.from_env(require_capital=True)
    current = load_managed_positions(config.managed_positions_path)
    return build_order_plan(
        strategy=strategy,
        cache_db=CACHE_DB,
        config=config,
        current=current,
        broker_available_balance=_broker_available_balance(args),
    )


def run_plan(args: argparse.Namespace) -> None:
    plan = _build_plan(args)
    write_json(args.out, plan.to_dict())
    print(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2))


def run_capital_check(args: argparse.Namespace) -> None:
    config = LiveTradingConfig.from_env(require_capital=False)
    if config.strategy_capital_twd <= 0:
        config = LiveTradingConfig(strategy_capital_twd=50_000.0)
    strategy = best_execution_ready_strategy()
    current = load_managed_positions(config.managed_positions_path)
    plan = build_order_plan(
        strategy=strategy,
        cache_db=CACHE_DB,
        config=config,
        current=current,
        broker_available_balance=None,
    )
    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "strategy": strategy.strategy_id,
        "data_cutoff": plan.data_cutoff,
        "minimum_capital_for_one_share_per_target": plan.diagnostics[
            "minimum_capital_for_one_share_per_target"
        ],
        "minimum_verification_capital_twd": 50_000,
        "backtest_comparable_capital_twd": 1_000_000,
        "reason": (
            "NT$50,000 is the practical minimum strategy-verification size for "
            "the current Iter95 basket. It produces several odd-lot orders while "
            "keeping capital small. NT$1,000,000 is only the backtest-comparable "
            "size because Iter95 validation used that capital, odd-lot shares, "
            "and NT$20 minimum commission."
        ),
        "sample_plan": {
            "target_count": plan.diagnostics["target_count"],
            "order_count": plan.diagnostics["order_count"],
            "deployable_capital_twd": plan.deployable_capital_twd,
            "estimated_buy_notional": plan.diagnostics["estimated_buy_notional"],
            "estimated_fees": plan.diagnostics["estimated_fees"],
        },
    }
    write_json(args.out, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def refresh_data() -> None:
    subprocess.run(["sbt", "runMain Main update"], check=True)
    subprocess.run(
        ["uv", "run", "--project", "research", "python", "research/cache_tables.py"],
        check=True,
    )


def run_after_close(args: argparse.Namespace) -> None:
    if not args.skip_refresh:
        refresh_data()
    plan = _build_plan(args)
    out = args.out
    if out is None:
        out = LiveTradingConfig.from_env(require_capital=True).plans_dir / f"{plan.plan_id}.json"
    write_json(out, plan.to_dict())
    print(json.dumps({"plan_path": str(out), **plan.to_dict()}, ensure_ascii=False, indent=2))


def load_plan(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def run_submit_plan(args: argparse.Namespace) -> None:
    current = status()
    if args.live and current.dry_run:
        raise RuntimeError("Live mode requested but FUBON_DRY_RUN is still true.")
    if args.live and not current.execution_ready_strategy_available:
        raise RuntimeError("Live mode requested but no execution-ready strategy is registered.")

    payload = load_plan(args.plan)
    broker = FubonBroker.from_env() if args.live else FubonBroker(dry_run=True)
    if args.live:
        config = LiveTradingConfig.from_env(require_capital=True)
        managed = load_managed_positions(config.managed_positions_path).normalized_positions()
        inventory = positions_from_fubon_inventories(broker.get_inventories())
        plan_symbols = {
            str(raw["symbol"]).zfill(4)
            for raw in payload.get("orders", []) or []
        } | set(managed)
        mismatches = inventory_mismatches(
            managed_positions=managed,
            broker_positions=inventory,
            symbols=plan_symbols,
        )
        if mismatches:
            raise RuntimeError(
                "Broker inventory does not match managed-position ledger for "
                f"planned symbols: {mismatches}. Reconcile or import positions "
                "before live submission."
            )
    results = []
    placed = False
    for raw in payload.get("orders", []):
        order_request = {
            "symbol": raw["symbol"],
            "side": raw["side"],
            "quantity": int(raw["quantity"]),
            "price_type": raw["price_type"],
            "market_type": raw["market_type"],
            "time_in_force": raw["time_in_force"],
            "order_type": raw["order_type"],
            "user_def": raw.get("user_def"),
        }
        result = broker.place_stock_order(StockOrderRequest(**order_request))
        placed = placed or not isinstance(result, dict) or not result.get("dry_run", False)
        results.append(result if isinstance(result, dict) else str(result))

    submit_payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "plan": str(args.plan),
        "live_requested": bool(args.live),
        "dry_run": current.dry_run,
        "order_count": len(payload.get("orders", [])),
        "placed_order": placed,
        "results": results,
    }
    write_json(args.out, submit_payload)
    print(json.dumps(submit_payload, ensure_ascii=False, indent=2))


def run_reconcile_plan(args: argparse.Namespace) -> None:
    plan = load_plan(args.plan)
    config = LiveTradingConfig.from_env(require_capital=False)
    before = load_managed_positions(config.managed_positions_path)
    if args.assume_filled:
        fills = fills_from_order_plan(plan)
        source = "assume_filled_plan"
    else:
        user_defs = sorted(
            {
                raw.get("user_def")
                for raw in plan.get("orders", []) or []
                if raw.get("user_def")
            }
        )
        if len(user_defs) != 1:
            raise RuntimeError("Cannot reconcile from broker unless the plan has one user_def.")
        broker = FubonBroker.from_env()
        start = str(plan["expected_submit_after"])[:10].replace("-", "")
        result = broker.get_filled_history(start, start)
        fills = fills_from_fubon_result(result, user_def=user_defs[0])
        source = "fubon_filled_history"

    after = apply_trade_fills(before, fills)
    if args.write:
        save_managed_positions(config.managed_positions_path, after)
    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "plan": str(args.plan),
        "source": source,
        "write": bool(args.write),
        "fill_count": len(fills),
        "before_positions": before.normalized_positions(),
        "after_positions": after.normalized_positions(),
        "managed_positions_path": str(config.managed_positions_path),
        "placed_order": False,
    }
    write_json(args.out, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def run_daily(args: argparse.Namespace) -> None:
    current = status()
    if not current.execution_ready_strategy_available:
        payload = {
            **asdict(current),
            "blocked": True,
            "minimum_required_stage": StrategyStage.EXECUTION_READY.value,
            "reason": "No execution-ready strategy is registered.",
            "placed_order": False,
        }
        write_json(args.out, payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    if args.live and current.dry_run:
        raise RuntimeError("Live mode requested but FUBON_DRY_RUN is still true.")
    run_plan(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage-gated automated trading runner.")
    sub = parser.add_subparsers(dest="command", required=True)

    status_cmd = sub.add_parser("status")
    status_cmd.add_argument("--out", type=Path, default=OUT_DIR / "status.json")
    status_cmd.set_defaults(func=run_status)

    smoke_cmd = sub.add_parser("smoke-test")
    smoke_cmd.add_argument("--out", type=Path, default=OUT_DIR / "fubon_smoke_test.json")
    smoke_cmd.add_argument("--accounting", action="store_true")
    smoke_cmd.add_argument("--login-method", choices=["apikey", "password"], default="apikey")
    smoke_cmd.set_defaults(func=run_smoke_test)

    plan_cmd = sub.add_parser("plan")
    plan_cmd.add_argument("--out", type=Path, default=OUT_DIR / "plan.json")
    plan_cmd.add_argument("--query-broker-balance", action="store_true")
    plan_cmd.set_defaults(func=run_plan)

    capital_cmd = sub.add_parser("capital-check")
    capital_cmd.add_argument("--out", type=Path, default=OUT_DIR / "capital_check.json")
    capital_cmd.set_defaults(func=run_capital_check)

    after_close_cmd = sub.add_parser("run-after-close")
    after_close_cmd.add_argument("--skip-refresh", action="store_true")
    after_close_cmd.add_argument("--query-broker-balance", action="store_true")
    after_close_cmd.add_argument("--out", type=Path, default=None)
    after_close_cmd.set_defaults(func=run_after_close)

    submit_cmd = sub.add_parser("submit-plan")
    submit_cmd.add_argument("plan", type=Path)
    submit_cmd.add_argument("--live", action="store_true")
    submit_cmd.add_argument("--out", type=Path, default=OUT_DIR / "submit_plan.json")
    submit_cmd.set_defaults(func=run_submit_plan)

    reconcile_cmd = sub.add_parser("reconcile-plan")
    reconcile_cmd.add_argument("plan", type=Path)
    reconcile_cmd.add_argument("--assume-filled", action="store_true")
    reconcile_cmd.add_argument("--write", action="store_true")
    reconcile_cmd.add_argument("--out", type=Path, default=OUT_DIR / "reconcile_plan.json")
    reconcile_cmd.set_defaults(func=run_reconcile_plan)

    daily_cmd = sub.add_parser("run-daily")
    daily_cmd.add_argument("--live", action="store_true")
    daily_cmd.add_argument("--query-broker-balance", action="store_true")
    daily_cmd.add_argument("--out", type=Path, default=OUT_DIR / "daily_run.json")
    daily_cmd.set_defaults(func=run_daily)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
