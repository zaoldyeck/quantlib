"""Current and one-month-ago holdings snapshot for the Iter67 research leader."""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.dirname(__file__))

from iter_69_production_audit_and_ablation import ITER67_NAV_PATH  # noqa: E402
from iter_70_hierarchical_position_audit import build_hierarchical_books  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_73_current_holdings_snapshot"
CAPITAL = 1_000_000.0


def previous_month_same_day(d: date) -> date:
    year = d.year
    month = d.month - 1
    if month == 0:
        year -= 1
        month = 12
    month_lengths = {
        1: 31,
        2: 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
        3: 31,
        4: 30,
        5: 31,
        6: 30,
        7: 31,
        8: 31,
        9: 30,
        10: 31,
        11: 30,
        12: 31,
    }
    return date(year, month, min(d.day, month_lengths[month]))


def trading_day_on_or_before(days: list[date], target: date) -> date:
    candidates = [d for d in days if d <= target]
    if not candidates:
        raise ValueError(f"No trading day on or before {target}")
    return candidates[-1]


def price_lookup(panel: pl.DataFrame, snapshot_dates: list[date]) -> dict[tuple[date, str], float]:
    px = (
        panel.filter(pl.col("date").is_in(snapshot_dates))
        .select(["date", "company_code", "close"])
        .drop_nulls(["close"])
    )
    return {(row["date"], row["company_code"]): float(row["close"]) for row in px.iter_rows(named=True)}


def holdings_frame(book: dict[str, float], asof: date, prices: dict[tuple[date, str], float]) -> pl.DataFrame:
    rows = []
    for code, weight in sorted(book.items(), key=lambda kv: (-kv[1], kv[0])):
        close = prices.get((asof, code))
        target_value = CAPITAL * weight
        round_lot_shares = int(target_value // ((close or float("inf")) * 1000) * 1000) if close else 0
        odd_lot_shares = int(target_value // close) if close else 0
        rows.append(
            {
                "date": asof,
                "company_code": code,
                "target_weight": weight,
                "target_value_per_1m_twd": target_value,
                "close": close,
                "round_lot_shares_per_1m_twd": round_lot_shares,
                "odd_lot_shares_per_1m_twd": odd_lot_shares,
            }
        )
    return pl.DataFrame(rows)


def add_forward_returns(holdings: pl.DataFrame, end_date: date, prices: dict[tuple[date, str], float]) -> pl.DataFrame:
    if holdings.is_empty():
        return holdings
    return (
        holdings.with_columns(
            pl.col("company_code")
            .map_elements(lambda code: prices.get((end_date, code)), return_dtype=pl.Float64)
            .alias("end_close")
        )
        .with_columns(((pl.col("end_close") / pl.col("close")) - 1.0).alias("one_month_price_return"))
    )


def nav_return(start: date, end: date) -> float:
    daily = pl.read_csv(ITER67_NAV_PATH, try_parse_dates=True).sort("date")
    navs = dict(zip(daily["date"].to_list(), daily["nav"].to_list(), strict=True))
    return float(navs[end] / navs[start] - 1.0)


def main() -> None:
    days, panel, books_by_name, iter67_state = build_hierarchical_books()
    latest = days[-1]
    one_month_target = previous_month_same_day(latest)
    month_start = trading_day_on_or_before(days, one_month_target)
    prices = price_lookup(panel, [month_start, latest])

    books = books_by_name["iter67_hierarchical"]
    current = holdings_frame(books.get(latest, {}), latest, prices)
    previous = add_forward_returns(holdings_frame(books.get(month_start, {}), month_start, prices), latest, prices)

    strategy_return = nav_return(month_start, latest)
    static_basket_return = 0.0
    if not previous.is_empty() and "one_month_price_return" in previous.columns:
        static_basket_return = float(
            previous.select((pl.col("target_weight") * pl.col("one_month_price_return")).sum()).item()
        )
    summary = pl.DataFrame(
        [
            {
                "latest_data_date": latest,
                "one_month_snapshot_date": month_start,
                "selected_latest": "attack64" if iter67_state.get(latest) == "attack" else "core63",
                "selected_one_month_ago": "attack64" if iter67_state.get(month_start) == "attack" else "core63",
                "current_positions": current.height,
                "one_month_ago_positions": previous.height,
                "current_total_weight": float(current["target_weight"].sum()) if not current.is_empty() else 0.0,
                "one_month_ago_total_weight": float(previous["target_weight"].sum()) if not previous.is_empty() else 0.0,
                "iter67_nav_return": strategy_return,
                "static_one_month_ago_basket_return": static_basket_return,
            }
        ]
    )

    RESULTS.mkdir(parents=True, exist_ok=True)
    current.write_csv(RESULTS / f"{OUT_PREFIX}_current.csv")
    previous.write_csv(RESULTS / f"{OUT_PREFIX}_one_month_ago.csv")
    summary.write_csv(RESULTS / f"{OUT_PREFIX}_summary.csv")

    print("iter_73 current holdings snapshot")
    print(summary.to_pandas().to_string(index=False))
    print("\nCurrent holdings")
    print(current.to_pandas().to_string(index=False))
    print("\nOne-month-ago holdings")
    print(previous.to_pandas().to_string(index=False))
    print(f"\nSaved: {RESULTS / f'{OUT_PREFIX}_summary.csv'}")
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_current.csv'}")
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_one_month_ago.csv'}")


if __name__ == "__main__":
    main()
