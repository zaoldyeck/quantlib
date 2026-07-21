from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from research.trading.live_config import LiveTradingConfig
from research.trading.order_planner import (
    build_order_plan,
    load_latest_target_weights,
    minimum_capital_for_full_basket,
    split_order_quantity,
)
from research.trading.portfolio import PortfolioSnapshot
from research.trading.portfolio import apply_trade_fills, fills_from_order_plan, inventory_mismatches
from research.trading.strategy_registry import StrategyRegistration, StrategyStage


def make_cache(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute(
        """
        create table daily_quote(
            market varchar,
            date date,
            company_code varchar,
            closing_price double
        )
        """
    )
    con.executemany(
        "insert into daily_quote values (?, ?, ?, ?)",
        [
            ("twse", date(2026, 5, 18), "2330", 100.0),
            ("twse", date(2026, 5, 18), "2454", 200.0),
            ("twse", date(2026, 5, 18), "0050", 50.0),
            ("twse", date(2026, 5, 19), "2330", 100.0),
            ("twse", date(2026, 5, 19), "2454", 200.0),
            ("twse", date(2026, 5, 19), "0050", 50.0),
        ],
    )
    con.close()


def make_strategy(targets: Path) -> StrategyRegistration:
    return StrategyRegistration(
        strategy_id="test_strategy",
        name="Test Strategy",
        stage=StrategyStage.EXECUTION_READY,
        validation_doc=Path("docs/test.md"),
        target_weights_path=targets,
    )


def test_load_latest_target_weights_preserves_leading_zero(tmp_path: Path) -> None:
    targets = tmp_path / "targets.csv"
    targets.write_text(
        "date,company_code,target_weight\n"
        "2026-05-17,0050,1.0\n"
        "2026-05-18,0050,0.5\n"
        "2026-05-18,2330,0.5\n",
        encoding="utf-8",
    )

    target_date, weights = load_latest_target_weights(targets)

    assert target_date == date(2026, 5, 18)
    assert weights == {"0050": 0.5, "2330": 0.5}


def test_split_order_quantity_uses_board_and_intraday_odd_lots() -> None:
    orders = split_order_quantity(
        1250,
        side="Buy",
        reference_price=100.0,
        user_def="TEST",
    )

    assert [(order.market_type, order.quantity, order.price_type) for order in orders] == [
        ("Common", 1000, "LimitUp"),
        ("IntradayOdd", 250, "LimitUp"),
    ]


def test_build_order_plan_uses_managed_positions_and_sells_first(tmp_path: Path) -> None:
    cache = tmp_path / "cache.duckdb"
    make_cache(cache)
    targets = tmp_path / "targets.csv"
    targets.write_text(
        "date,company_code,target_weight\n"
        "2026-05-18,2330,0.6\n"
        "2026-05-18,2454,0.4\n",
        encoding="utf-8",
    )
    current = PortfolioSnapshot(
        as_of="2026-05-18T15:00:00",
        positions={"0050": 100, "2330": 10},
    )
    config = LiveTradingConfig(
        strategy_capital_twd=10_000,
        cash_buffer_pct=0.0,
        buy_price_buffer_pct=0.0,
    )

    plan = build_order_plan(
        strategy=make_strategy(targets),
        cache_db=cache,
        config=config,
        current=current,
    )

    assert plan.desired_positions["2330"] == 60
    assert plan.desired_positions["2454"] == 20
    assert plan.desired_positions["0050"] == 0
    assert plan.target_date == "2026-05-18"
    assert plan.data_cutoff == "2026-05-19"
    assert plan.expected_submit_after.startswith("2026-05-20T08:30:00")
    assert [order.side for order in plan.orders[:1]] == ["Sell"]
    assert any(order.symbol == "0050" and order.side == "Sell" for order in plan.orders)
    assert any(order.symbol == "2330" and order.side == "Buy" for order in plan.orders)


def test_minimum_capital_for_full_basket_includes_min_commission() -> None:
    required = minimum_capital_for_full_basket(
        {"2330": 0.5, "2454": 0.5},
        {"2330": 100.0, "2454": 200.0},
    )

    assert required == 440.0


def test_apply_filled_order_plan_updates_managed_positions() -> None:
    plan = {
        "orders": [
            {"symbol": "2330", "side": "Buy", "quantity": 10, "reference_price": 100.0},
            {"symbol": "0050", "side": "Sell", "quantity": 5, "reference_price": 50.0},
        ]
    }
    before = PortfolioSnapshot(as_of="2026-05-18T15:00:00", positions={"0050": 5})

    fills = fills_from_order_plan(plan)
    after = apply_trade_fills(before, fills)

    assert after.normalized_positions() == {"2330": 10}


def test_inventory_mismatch_checks_only_planned_symbols() -> None:
    mismatches = inventory_mismatches(
        managed_positions={"2330": 10},
        broker_positions={"2330": 12, "0050": 100},
        symbols={"2330"},
    )

    assert mismatches == {"2330": {"managed": 10, "broker": 12}}
