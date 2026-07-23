"""iter_93 - unconstrained all-win realistic execution search.

This pass deliberately removes the old max-position gate and searches for a
strategy that can beat all currently registered realistic-execution strategies
on the same execution model.  It does not rank paper NAV.  The search treats
existing realistic-execution target books as sleeves, then builds new PM
allocator target books from scratch:

- single-sleeve momentum switch
- top-2 / top-3 momentum blends
- weekly / monthly schedules
- multiple lagged lookbacks and minimum holds

Only final candidates are validated with the full validator.  All finalists are
rerun through the Fubon realistic execution simulator.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from dateutil.relativedelta import relativedelta
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, str(RESEARCH_ROOT))

from quantlib.constants import CAPITAL  # noqa: E402
from quantlib.db import connect  # noqa: E402
from evaluation import nav_metrics  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from iter_82_oos_recent_pm_allocator import expand_book_targets, load_execution_targets  # noqa: E402
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_DIR = RESULTS / "iter_93_unconstrained_all_win_search"
N_TRIALS = 41_116 + 320


@dataclass(frozen=True)
class Sleeve:
    key: str
    target_path: Path
    daily_path: Path
    score: float


def trailing_calendar_return(daily: pl.DataFrame, months: int) -> float:
    ordered = daily.sort("date")
    end = ordered["date"][-1]
    anchor = end - relativedelta(months=months)
    start = ordered.filter(pl.col("date") <= anchor).tail(1)
    base = CAPITAL if start.is_empty() else start["nav"][0]
    return float(ordered["nav"][-1] / base - 1.0)


def scheduled(days: list[date], idx: int, mode: str) -> bool:
    if idx == 0:
        return True
    if mode == "weekly":
        return days[idx].isocalendar().week != days[idx - 1].isocalendar().week
    if mode == "monthly":
        return days[idx].month != days[idx - 1].month
    raise ValueError(f"unknown schedule {mode}")


def load_nav_lookup(path: Path) -> dict[date, float]:
    frame = pl.read_csv(path, try_parse_dates=True).sort("date")
    return {row["date"]: float(row["nav"]) for row in frame.iter_rows(named=True)}


def target_daily_for(target_path: Path) -> Path | None:
    stem = target_path.name.removesuffix("_target_weights.csv")
    candidate = target_path.with_name(f"{stem}_daily.csv")
    if candidate.exists():
        return candidate
    return None


def discover_realistic_sleeves() -> list[tuple[str, Path, Path]]:
    sleeves: list[tuple[str, Path, Path]] = []
    allowed_prefixes = (
        "iter_89_execution_champion_search_",
        "iter_90_active_etf_aware_search_",
        "iter_91_active_etf_challenger_refinement_",
        "iter_92_execution_meta_switch",
    )
    for target_path in sorted(RESULTS.glob("*_target_weights.csv")):
        if not target_path.name.startswith(allowed_prefixes):
            continue
        daily_path = target_daily_for(target_path)
        if daily_path and daily_path.exists():
            key = target_path.name.removesuffix("_target_weights.csv")
            sleeves.append((key, target_path, daily_path))

    iter87_target = RESULTS / "iter_87_iter86_execution_validation_iter86_dual_target_weights.csv"
    iter87_daily = RESULTS / "iter_87_iter86_execution_validation_fubon_odd_lot_5pct_vol_slip5bp_daily.csv"
    if iter87_target.exists() and iter87_daily.exists():
        sleeves.append(("iter87_baseline_realistic", iter87_target, iter87_daily))

    iter67_target = RESULTS / "codex_iter67_realistic_check" / "targets.csv"
    iter67_daily = RESULTS / "codex_iter67_realistic_check" / "daily.csv"
    if iter67_target.exists() and iter67_daily.exists():
        sleeves.append(("iter67_72_realistic_recheck", iter67_target, iter67_daily))

    return sleeves


def score_sleeve(key: str, target_path: Path, daily_path: Path) -> dict[str, object]:
    daily = pl.read_csv(daily_path, try_parse_dates=True).select(["date", "nav"]).sort("date")
    metrics = nav_metrics(daily)
    end = daily["date"][-1]
    oos = daily.filter((pl.col("date").dt.year() >= 2010) & (pl.col("date").dt.year() <= 2025))
    oos_metrics = nav_metrics(oos, prefix="oos_") if not oos.is_empty() else {}
    row = {
        "key": key,
        "target_path": str(target_path),
        "daily_path": str(daily_path),
        "end": end,
        **metrics,
        **oos_metrics,
        "ret_12m": trailing_calendar_return(daily, 12),
        "ret_6m": trailing_calendar_return(daily, 6),
        "ret_3m": trailing_calendar_return(daily, 3),
        "ret_1m": trailing_calendar_return(daily, 1),
    }
    # Fast sleeve-ranking score only; final candidates use full validator.
    row["score"] = (
        float(row.get("oos_cagr", 0.0)) * 3.0
        + float(row.get("ret_12m", 0.0)) * 1.0
        + float(row.get("ret_6m", 0.0)) * 1.0
        + float(row.get("oos_sortino", 0.0)) * 0.25
        + max(float(row.get("oos_mdd", -1.0)), -1.0) * 0.5
    )
    return row


def compress_targets(
    daily_targets: dict[date, dict[str, float]],
    *,
    threshold: float,
) -> dict[date, dict[str, float]]:
    targets: dict[date, dict[str, float]] = {}
    last: dict[str, float] | None = None
    for day, book in daily_targets.items():
        if last is None:
            changed = True
        else:
            keys = set(book) | set(last)
            l1 = sum(abs(book.get(code, 0.0) - last.get(code, 0.0)) for code in keys)
            changed = set(book) != set(last) or l1 > threshold
        if changed:
            targets[day] = book
            last = book
    return targets


def combine_books(items: list[tuple[float, dict[str, float]]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for sleeve_weight, book in items:
        for code, weight in book.items():
            if code == "CASH" or weight <= 0:
                continue
            out[code] = out.get(code, 0.0) + sleeve_weight * weight
    gross = sum(out.values())
    if gross > 1.0:
        out = {code: weight / gross for code, weight in out.items()}
    return {code: weight for code, weight in out.items() if weight > 1e-12}


def build_allocator_targets(
    days: list[date],
    sleeves: list[Sleeve],
    daily_books: dict[str, dict[date, dict[str, float]]],
    navs: dict[str, dict[date, float]],
    *,
    schedule_mode: str,
    lookback: int,
    min_hold_days: int,
    top_n: int,
    blend_mode: str,
    change_threshold: float,
) -> tuple[dict[date, dict[str, float]], pl.DataFrame]:
    sleeve_keys = [s.key for s in sleeves]
    active_keys = [sleeve_keys[0]]
    held = 10_000
    daily_targets: dict[date, dict[str, float]] = {}
    states: list[dict[str, object]] = []

    for idx, day in enumerate(days):
        if scheduled(days, idx, schedule_mode) and held >= min_hold_days and idx > lookback + 1:
            prev_day = days[idx - 1]
            past_day = days[idx - 1 - lookback]
            scores: list[tuple[float, str]] = []
            for sleeve in sleeves:
                nav = navs[sleeve.key]
                if prev_day in nav and past_day in nav and nav[past_day] > 0:
                    scores.append((nav[prev_day] / nav[past_day] - 1.0, sleeve.key))
            if scores:
                ordered = sorted(scores, reverse=True)
                active_keys = [key for _, key in ordered[:top_n]]
                held = 0
        else:
            held += 1

        if blend_mode == "single":
            weights = [1.0]
        elif blend_mode == "top2_7030":
            weights = [0.7, 0.3]
        elif blend_mode == "top2_5050":
            weights = [0.5, 0.5]
        elif blend_mode == "top3_602020":
            weights = [0.6, 0.2, 0.2]
        else:
            raise ValueError(f"unknown blend_mode {blend_mode}")
        active = active_keys[: len(weights)]
        if len(active) < len(weights):
            weights = weights[: len(active)]
            total = sum(weights)
            weights = [w / total for w in weights]
        book = combine_books(
            [(weight, daily_books[key].get(day, {})) for weight, key in zip(weights, active, strict=True)]
        )
        daily_targets[day] = book
        states.append(
            {
                "date": day,
                "active_keys": ",".join(active),
                "target_names": len(book),
                "target_gross": sum(book.values()),
            }
        )

    return compress_targets(daily_targets, threshold=change_threshold), pl.DataFrame(states)


def blend_weights(blend_mode: str, active_count: int) -> list[float]:
    if blend_mode == "single":
        weights = [1.0]
    elif blend_mode == "top2_7030":
        weights = [0.7, 0.3]
    elif blend_mode == "top2_5050":
        weights = [0.5, 0.5]
    elif blend_mode == "top3_602020":
        weights = [0.6, 0.2, 0.2]
    else:
        raise ValueError(f"unknown blend_mode {blend_mode}")
    weights = weights[:active_count]
    total = sum(weights)
    return [w / total for w in weights] if total > 0 else []


def estimate_source_nav(
    days: list[date],
    state: pl.DataFrame,
    navs: dict[str, dict[date, float]],
    *,
    blend_mode: str,
) -> pl.DataFrame:
    """Fast source-realistic NAV proxy used only for finalist prefiltering."""
    state_rows = state.sort("date").iter_rows(named=True)
    active_by_day = {row["date"]: str(row["active_keys"]).split(",") for row in state_rows}
    out: list[dict[str, object]] = [{"date": days[0], "nav": CAPITAL}]
    nav = CAPITAL
    for idx in range(1, len(days)):
        prev_day = days[idx - 1]
        day = days[idx]
        active = [key for key in active_by_day.get(prev_day, []) if key]
        weights = blend_weights(blend_mode, len(active))
        ret = 0.0
        observed = 0.0
        for weight, key in zip(weights, active, strict=True):
            lookup = navs[key]
            if prev_day in lookup and day in lookup and lookup[prev_day] > 0:
                ret += weight * (lookup[day] / lookup[prev_day] - 1.0)
                observed += weight
        if observed > 0:
            ret /= observed
        else:
            ret = 0.0
        nav *= 1.0 + ret
        out.append({"date": day, "nav": nav})
    return pl.DataFrame(out)


def quick_result_row(name: str, daily: pl.DataFrame, stats: dict[str, float]) -> dict[str, object]:
    metrics = nav_metrics(daily.select(["date", "nav"]))
    oos = daily.filter((pl.col("date").dt.year() >= 2010) & (pl.col("date").dt.year() <= 2025))
    oos_metrics = nav_metrics(oos.select(["date", "nav"]), prefix="oos_")
    ret_12m = trailing_calendar_return(daily, 12)
    row: dict[str, object] = {
        "name": name,
        **metrics,
        **oos_metrics,
        **stats,
        "end": daily["date"][-1],
        "recent_1y_cagr": ret_12m,
        "ret_12m": ret_12m,
        "ret_6m": trailing_calendar_return(daily, 6),
        "ret_3m": trailing_calendar_return(daily, 3),
        "ret_1m": trailing_calendar_return(daily, 1),
    }
    row["quick_score"] = (
        float(row["oos_cagr"]) * 4.0
        + float(row["ret_12m"]) * 1.0
        + float(row["ret_6m"]) * 1.0
        + float(row["ret_3m"]) * 0.5
        + float(row["ret_1m"]) * 0.5
        + float(row["oos_sortino"]) * 0.3
        + max(float(row["oos_mdd"]), -1.0) * 0.6
    )
    return row


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    raw_sleeves = discover_realistic_sleeves()
    sleeve_rows = [score_sleeve(*item) for item in raw_sleeves]
    sleeve_frame = pl.DataFrame(sleeve_rows).sort("score", descending=True)
    sleeve_frame.write_csv(OUT_DIR / "sleeve_universe.csv")

    selected_keys: list[str] = []
    for frame in [
        sleeve_frame.sort("score", descending=True).head(14),
        sleeve_frame.sort("oos_cagr", descending=True).head(8),
        sleeve_frame.sort("ret_12m", descending=True).head(8),
        sleeve_frame.sort("ret_1m", descending=True).head(8),
        sleeve_frame.filter(pl.col("key").str.contains("iter_92_execution_meta_switch")),
    ]:
        for key in frame["key"].to_list():
            if key not in selected_keys:
                selected_keys.append(str(key))

    selected_rows = sleeve_frame.filter(pl.col("key").is_in(selected_keys)).sort("score", descending=True)
    sleeves = [
        Sleeve(
            key=str(row["key"]),
            target_path=Path(str(row["target_path"])),
            daily_path=Path(str(row["daily_path"])),
            score=float(row["score"]),
        )
        for row in selected_rows.iter_rows(named=True)
    ]
    selected_rows.write_csv(OUT_DIR / "selected_sleeves.csv")

    base_daily = pl.read_csv(RESULTS / "iter_92_execution_meta_switch_daily.csv", try_parse_dates=True).sort("date")
    days = base_daily["date"].to_list()
    raw_targets = {s.key: load_execution_targets(s.target_path) for s in sleeves}
    daily_books = {key: expand_book_targets(days, targets) for key, targets in raw_targets.items()}
    navs = {s.key: load_nav_lookup(s.daily_path) for s in sleeves}

    candidate_specs: list[dict[str, object]] = []
    for schedule_mode in ["weekly", "monthly"]:
        for lookback in [3, 5, 10, 21, 42, 63]:
            for min_hold_days in [3, 5, 10, 20, 40]:
                for change_threshold in [0.03, 0.05, 0.08]:
                    candidate_specs.append(
                        {
                            "schedule_mode": schedule_mode,
                            "lookback": lookback,
                            "min_hold_days": min_hold_days,
                            "top_n": 1,
                            "blend_mode": "single",
                            "change_threshold": change_threshold,
                        }
                    )
                    for top_n, blend_mode in [
                        (2, "top2_7030"),
                        (2, "top2_5050"),
                        (3, "top3_602020"),
                    ]:
                        if min_hold_days <= 20 and lookback <= 42:
                            candidate_specs.append(
                                {
                                    "schedule_mode": schedule_mode,
                                    "lookback": lookback,
                                    "min_hold_days": min_hold_days,
                                    "top_n": top_n,
                                    "blend_mode": blend_mode,
                                    "change_threshold": change_threshold,
                                }
                            )

    target_cache: dict[str, tuple[dict[date, dict[str, float]], pl.DataFrame, dict[str, object]]] = {}
    quick_rows: list[dict[str, object]] = []
    for idx, spec in enumerate(candidate_specs):
        name = (
            f"iter93_{spec['blend_mode']}_{spec['schedule_mode']}"
            f"_lb{spec['lookback']}_h{spec['min_hold_days']}_thr{int(float(spec['change_threshold']) * 100)}"
        )
        targets, state = build_allocator_targets(days, sleeves, daily_books, navs, **spec)
        target_cache[name] = (targets, state, spec)
        estimate_daily = estimate_source_nav(days, state, navs, blend_mode=str(spec["blend_mode"]))
        row = quick_result_row(
            name,
            estimate_daily,
            {
                "max_active": float(state["target_names"].max()),
                "trade_days": float(len(targets)),
                "avg_turnover_trade_day": np.nan,
                "requested_notional": np.nan,
                "filled_notional": np.nan,
                "fill_ratio": np.nan,
                "total_commission": np.nan,
                "total_tax": np.nan,
                "total_slippage_cost": np.nan,
                "blocked_orders": np.nan,
                "partial_orders": np.nan,
            },
        )
        row.update(spec)
        row["target_rebalance_days"] = len(targets)
        row["max_target_names"] = int(state["target_names"].max())
        row["avg_target_names"] = float(state["target_names"].mean())
        quick_rows.append(row)

    quick_frame = pl.DataFrame(quick_rows).sort("quick_score", descending=True)
    quick_frame.write_csv(OUT_DIR / "quick_summary.csv")

    thresholds = {
        "oos_cagr": 0.38105992468983674,
        "recent_1y_cagr": 2.662997,  # cap10 rounded high-water from rerun.
        "ret_12m": 2.6858346234442394,
        "ret_6m": 1.6726110124645097,
        "ret_3m": 0.9438309153296215,
        "ret_1m": 0.11003831107307915,
        "oos_sortino": 2.3972228364238624,
        "oos_mdd": -0.2465,
    }
    finalist_names: list[str] = []
    for frame in [
        quick_frame.sort("quick_score", descending=True).head(16),
        quick_frame.sort("oos_cagr", descending=True).head(10),
        quick_frame.sort("recent_1y_cagr", descending=True).head(10),
        quick_frame.sort("ret_1m", descending=True).head(10),
        quick_frame.filter(
            (pl.col("oos_cagr") > thresholds["oos_cagr"])
            | (pl.col("ret_12m") > thresholds["ret_12m"])
            | (pl.col("ret_6m") > thresholds["ret_6m"])
            | (pl.col("ret_1m") > thresholds["ret_1m"])
        ).head(20),
    ]:
        for name in frame["name"].to_list():
            if name not in finalist_names:
                finalist_names.append(str(name))

    finalist_codes: set[str] = set()
    for name in finalist_names:
        targets, _, _ = target_cache[name]
        for book in targets.values():
            finalist_codes.update(book)

    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, sorted(finalist_codes), days[0], days[-1])
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
    simulator = RealisticExecutionSimulator(bars, config)

    final_rows: list[dict[str, object]] = []
    daily_cache: dict[str, pl.DataFrame] = {}
    fills_cache: dict[str, pl.DataFrame] = {}
    states_cache: dict[str, pl.DataFrame] = {}
    targets_cache_final: dict[str, dict[date, dict[str, float]]] = {}
    for name in finalist_names:
        targets, state, spec = target_cache[name]
        result = simulator.simulate(days, targets)
        daily_cache[name] = result.daily
        fills_cache[name] = result.fills
        states_cache[name] = state
        targets_cache_final[name] = targets
        row = validate_daily_nav(
            name,
            result.daily.select(["date", "nav"]),
            n_trials=N_TRIALS,
            extra={
                **result.stats,
                "target_rebalance_days": len(targets),
                "max_target_names": int(state["target_names"].max()),
                "avg_target_names": float(state["target_names"].mean()),
                "schedule_mode": spec["schedule_mode"],
                "lookback": spec["lookback"],
                "min_hold_days": spec["min_hold_days"],
                "blend_mode": spec["blend_mode"],
                "top_n": spec["top_n"],
                "change_threshold": spec["change_threshold"],
            },
        )
        row.update(
            {
                "end": daily_cache[name]["date"][-1],
                "ret_12m": trailing_calendar_return(daily_cache[name], 12),
                "ret_6m": trailing_calendar_return(daily_cache[name], 6),
                "ret_3m": trailing_calendar_return(daily_cache[name], 3),
                "ret_1m": trailing_calendar_return(daily_cache[name], 1),
            }
        )
        row["beat_oos_cagr"] = row["oos_cagr"] > thresholds["oos_cagr"]
        row["beat_recent_1y_cagr"] = row["recent_1y_cagr"] > thresholds["recent_1y_cagr"]
        row["beat_ret_12m"] = row["ret_12m"] > thresholds["ret_12m"]
        row["beat_ret_6m"] = row["ret_6m"] > thresholds["ret_6m"]
        row["beat_ret_3m"] = row["ret_3m"] > thresholds["ret_3m"]
        row["beat_ret_1m"] = row["ret_1m"] > thresholds["ret_1m"]
        row["beat_sortino"] = row["oos_sortino"] > thresholds["oos_sortino"]
        row["beat_mdd"] = row["oos_mdd"] > thresholds["oos_mdd"]
        row["beat_dsr"] = row["dsr"] > 0.9857647226360408
        row["beat_pbo"] = row["pbo"] < 0.024
        row["beat_count"] = int(
            sum(
                bool(row[key])
                for key in [
                    "beat_oos_cagr",
                    "beat_recent_1y_cagr",
                    "beat_ret_12m",
                    "beat_ret_6m",
                    "beat_ret_3m",
                    "beat_ret_1m",
                    "beat_sortino",
                    "beat_mdd",
                    "beat_dsr",
                    "beat_pbo",
                ]
            )
        )
        final_rows.append(row)

    final_frame = pl.DataFrame(final_rows).sort(["beat_count", "oos_cagr"], descending=[True, True])
    final_frame.write_csv(OUT_DIR / "final_summary.csv")

    if final_rows:
        best_name = str(final_frame["name"][0])
        daily_cache[best_name].write_csv(OUT_DIR / f"{best_name}_daily.csv")
        fills_cache[best_name].write_csv(OUT_DIR / f"{best_name}_fills.csv")
        states_cache[best_name].write_csv(OUT_DIR / f"{best_name}_state.csv")
        pl.DataFrame(
            [
                {"date": day, "company_code": code, "target_weight": weight}
                for day, book in sorted(targets_cache_final[best_name].items())
                for code, weight in sorted(book.items())
            ]
        ).write_csv(OUT_DIR / f"{best_name}_target_weights.csv")

    display_cols = [
        "name",
        "end",
        "beat_count",
        "cagr",
        "oos_cagr",
        "recent_1y_cagr",
        "ret_12m",
        "ret_6m",
        "ret_3m",
        "ret_1m",
        "oos_sortino",
        "oos_mdd",
        "dsr",
        "pbo",
        "boot_cagr_lb",
        "fill_ratio",
        "max_active",
        "blend_mode",
        "schedule_mode",
        "lookback",
        "min_hold_days",
    ]
    printable = final_frame.select(display_cols).with_columns(
        [pl.col(c).mul(100).round(2) for c in ["cagr", "oos_cagr", "recent_1y_cagr", "ret_12m", "ret_6m", "ret_3m", "ret_1m", "oos_mdd", "boot_cagr_lb", "fill_ratio"]]
        + [pl.col(c).round(3) for c in ["oos_sortino", "dsr", "pbo"]]
    )
    print(printable.head(20).to_pandas().to_string(index=False))
    all_win = final_frame.filter(pl.col("beat_count") == 10)
    print(f"ALL_WIN_COUNT {all_win.height}")


if __name__ == "__main__":
    main()
