"""iter_92 - unconstrained execution-aware meta switch.

This strategy is intentionally evaluated only through the realistic execution
simulator.  It does not rank a paper NAV blend.  The allocator chooses one of
three already execution-validated sleeves using lagged realistic NAV momentum:

- Iter89 Robust Execution Champion
- Iter87 baseline realistic
- Iter67 / Iter72 cap6 realistic recheck

Rules:
- monthly selection schedule
- 5 trading-day lagged NAV momentum
- minimum hold 5 trading days
- no hard holding-count cap; order-level max_active is reported, not rejected
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, str(RESEARCH_ROOT))

from constants import CAPITAL  # noqa: E402
from db import connect  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from iter_82_oos_recent_pm_allocator import expand_book_targets, load_execution_targets  # noqa: E402
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path("research/strat_lab/results")
OUT_PREFIX = "iter_92_execution_meta_switch"
N_TRIALS = 41_116


@dataclass(frozen=True)
class Sleeve:
    key: str
    label: str
    target_path: Path
    daily_path: Path


SLEEVES = (
    Sleeve(
        "iter89",
        "Iter89 Robust Execution Champion",
        RESULTS
        / "iter_89_execution_champion_search_iter86_b20_b08_weekly_lb5_m2_hold40_c1_rw0_100_d75_target_weights.csv",
        RESULTS
        / "iter_89_execution_champion_search_iter86_b20_b08_weekly_lb5_m2_hold40_c1_rw0_100_d75_daily.csv",
    ),
    Sleeve(
        "iter87",
        "Iter87 baseline realistic",
        RESULTS / "iter_87_iter86_execution_validation_iter86_dual_target_weights.csv",
        RESULTS / "iter_87_iter86_execution_validation_fubon_odd_lot_5pct_vol_slip5bp_daily.csv",
    ),
    Sleeve(
        "iter67",
        "Iter67 / Iter72 cap6 realistic recheck",
        RESULTS / "codex_iter67_realistic_check" / "targets.csv",
        RESULTS / "codex_iter67_realistic_check" / "daily.csv",
    ),
)


def load_nav_lookup(path: Path) -> dict[date, float]:
    frame = pl.read_csv(path, try_parse_dates=True).sort("date")
    return {row["date"]: float(row["nav"]) for row in frame.iter_rows(named=True)}


def scheduled_monthly(days: list[date], idx: int) -> bool:
    return idx == 0 or days[idx].month != days[idx - 1].month


def build_targets(
    days: list[date],
    daily_books: dict[str, dict[date, dict[str, float]]],
    navs: dict[str, dict[date, float]],
    *,
    lookback: int = 5,
    min_hold_days: int = 5,
    weight_change_threshold: float = 0.05,
) -> tuple[dict[date, dict[str, float]], pl.DataFrame]:
    active_key = SLEEVES[0].key
    held = 10_000
    daily_targets: dict[date, dict[str, float]] = {}
    state_rows: list[dict[str, object]] = []

    for i, day in enumerate(days):
        if scheduled_monthly(days, i) and held >= min_hold_days and i > lookback + 1:
            prev_day = days[i - 1]
            past_day = days[i - 1 - lookback]
            scores: list[tuple[float, str]] = []
            for sleeve in SLEEVES:
                nav = navs[sleeve.key]
                if prev_day in nav and past_day in nav and nav[past_day] > 0:
                    scores.append((nav[prev_day] / nav[past_day] - 1.0, sleeve.key))
            if scores:
                active_key = sorted(scores, reverse=True)[0][1]
                held = 0
        else:
            held += 1

        book = {
            code: weight
            for code, weight in daily_books[active_key].get(day, {}).items()
            if weight > 1e-12 and code != "CASH"
        }
        state_rows.append({"date": day, "selected": active_key, "target_names": len(book), "target_gross": sum(book.values())})
        daily_targets[day] = book

    targets: dict[date, dict[str, float]] = {}
    last_target: dict[str, float] | None = None
    for day, book in daily_targets.items():
        if last_target is None:
            changed = True
        else:
            keys = set(book) | set(last_target)
            l1_change = sum(abs(book.get(code, 0.0) - last_target.get(code, 0.0)) for code in keys)
            changed = set(book) != set(last_target) or l1_change > weight_change_threshold
        if changed:
            targets[day] = book
            last_target = book

    return targets, pl.DataFrame(state_rows)


def trailing_return(daily: pl.DataFrame, trading_days: int) -> float:
    ordered = daily.sort("date")
    if ordered.height <= trading_days:
        return float(ordered["nav"][-1] / CAPITAL - 1.0)
    return float(ordered["nav"][-1] / ordered["nav"][-1 - trading_days] - 1.0)


def trailing_calendar_return(daily: pl.DataFrame, months: int) -> float:
    ordered = daily.sort("date")
    end = ordered["date"][-1]
    anchor = end - relativedelta(months=months)
    start = ordered.filter(pl.col("date") <= anchor).tail(1)
    if start.is_empty():
        return float(ordered["nav"][-1] / CAPITAL - 1.0)
    return float(ordered["nav"][-1] / start["nav"][0] - 1.0)


def main() -> None:
    base_daily = pl.read_csv(SLEEVES[0].daily_path, try_parse_dates=True).sort("date")
    days = base_daily["date"].to_list()

    raw_targets = {s.key: load_execution_targets(s.target_path) for s in SLEEVES}
    daily_books = {key: expand_book_targets(days, targets) for key, targets in raw_targets.items()}
    navs = {s.key: load_nav_lookup(s.daily_path) for s in SLEEVES}

    targets, state = build_targets(days, daily_books, navs)
    codes = sorted({code for book in targets.values() for code in book})

    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, codes, days[0], days[-1])
    finally:
        con.close()

    config = ExecutionConfig(
        name="fubon_odd_lot_5pct_vol_slip5bp",
        capital=CAPITAL,
        lot_size=1,
        max_participation_rate=0.05,
        fixed_slippage_bps=5.0,
        impact_bps_per_1pct_volume=1.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
    )
    result = RealisticExecutionSimulator(bars, config).simulate(days, targets)
    row = validate_daily_nav(
        "iter92_unconstrained_execution_meta_switch",
        result.daily.select(["date", "nav"]),
        n_trials=N_TRIALS,
        extra={
            **result.stats,
            "target_rebalance_days": len(targets),
            "selector_switches": int((state["selected"] != state["selected"].shift(1)).sum()),
            "max_target_names": int(state["target_names"].max()),
            "avg_target_names": float(state["target_names"].mean()),
        },
    )
    row.update(
        {
            "ret_1y": trailing_calendar_return(result.daily, 12),
            "ret_6m": trailing_calendar_return(result.daily, 6),
            "ret_3m": trailing_calendar_return(result.daily, 3),
            "ret_1m": trailing_calendar_return(result.daily, 1),
            "ret_6m_approx": trailing_return(result.daily, 126),
            "ret_3m_approx": trailing_return(result.daily, 63),
            "ret_1m_approx": trailing_return(result.daily, 21),
            "daily_path": str(RESULTS / f"{OUT_PREFIX}_daily.csv"),
            "fills_path": str(RESULTS / f"{OUT_PREFIX}_fills.csv"),
            "target_weights_path": str(RESULTS / f"{OUT_PREFIX}_target_weights.csv"),
            "state_path": str(RESULTS / f"{OUT_PREFIX}_state.csv"),
        }
    )

    result.daily.write_csv(RESULTS / f"{OUT_PREFIX}_daily.csv")
    result.fills.write_csv(RESULTS / f"{OUT_PREFIX}_fills.csv")
    state.write_csv(RESULTS / f"{OUT_PREFIX}_state.csv")
    pl.DataFrame(
        [
            {"date": day, "company_code": code, "target_weight": weight}
            for day, book in sorted(targets.items())
            for code, weight in sorted(book.items())
        ]
    ).write_csv(RESULTS / f"{OUT_PREFIX}_target_weights.csv")
    pl.DataFrame([row]).write_csv(RESULTS / f"{OUT_PREFIX}_summary.csv")

    print(
        pl.DataFrame([row])
        .select(
            [
                "name",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("ret_1y").mul(100).round(2),
                pl.col("ret_6m").mul(100).round(2),
                pl.col("ret_3m").mul(100).round(2),
                pl.col("ret_1m").mul(100).round(2),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("dsr").round(3),
                pl.col("pbo").round(3),
                pl.col("fill_ratio").mul(100).round(2).alias("fill_ratio_pct"),
                "max_active",
            ]
        )
        .to_pandas()
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
