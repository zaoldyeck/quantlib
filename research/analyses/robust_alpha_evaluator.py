"""Robust-alpha diagnostics for strategy candidates.

This evaluator is deliberately benchmark-relative.  A strategy that only wins
at the final endpoint, or wins only after a very late catch-up, should not look
strong here even if its full-window CAGR is high.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import sys

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from constants import CAPITAL  # noqa: E402
from db import connect  # noqa: E402
from prices import fetch_adjusted_panel  # noqa: E402
from validator import validate_daily_nav  # noqa: E402


RESULTS = REPO_ROOT / "research/strat_lab/results"
OUT_DIR = REPO_ROOT / "docs/strategy_research/robust_alpha"
OUT_DIR.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class SeriesSpec:
    name: str
    path: Path | None = None
    code: str | None = None
    market: str = "twse"


DEFAULT_STRATEGIES = [
    SeriesSpec(
        "Iter95 realistic",
        RESULTS / "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_daily.csv",
    ),
    SeriesSpec("Iter92 realistic", RESULTS / "iter_92_execution_meta_switch_daily.csv"),
    SeriesSpec(
        "Iter89 robust",
        RESULTS / "iter_89_execution_champion_search_iter86_b20_b08_weekly_lb5_m2_hold40_c1_rw0_100_d75_daily.csv",
    ),
    SeriesSpec(
        "Iter86/87 realistic",
        RESULTS / "iter_87_iter86_execution_validation_fubon_odd_lot_5pct_vol_slip5bp_daily.csv",
    ),
]

DEFAULT_BENCHMARKS = [
    SeriesSpec("0050 total return", code="0050", market="twse"),
    SeriesSpec("2330 total return", code="2330", market="twse"),
]


def pct(value: float | None) -> str:
    if value is None or not np.isfinite(value):
        return ""
    return f"{value * 100:.2f}%"


def load_strategy(path: Path, name: str) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pl.read_csv(path, try_parse_dates=True).select(["date", "nav"]).sort("date")
    return frame.with_columns(pl.lit(name).alias("name"))


def load_benchmark(code: str, market: str, name: str, start: date, end: date) -> pl.DataFrame:
    con = connect(read_only=True)
    try:
        panel = fetch_adjusted_panel(
            con,
            start.isoformat(),
            end.isoformat(),
            codes=[code],
            market=market,
            include_extra_history_days=30,
        )
    finally:
        con.close()
    frame = panel.filter(pl.col("company_code") == code).sort("date")
    if frame.is_empty():
        raise RuntimeError(f"Benchmark {code} has no adjusted panel rows.")
    base = float(frame["close"][0])
    return frame.select(["date", (pl.col("close") / base * CAPITAL).alias("nav")]).with_columns(
        pl.lit(name).alias("name")
    )


def nav_arrays(left: pl.DataFrame, right: pl.DataFrame) -> tuple[list[date], np.ndarray, np.ndarray]:
    joined = (
        left.select(["date", pl.col("nav").alias("left_nav")])
        .join(right.select(["date", pl.col("nav").alias("right_nav")]), on="date", how="inner")
        .sort("date")
    )
    if joined.height < 2:
        return [], np.array([]), np.array([])
    dates = joined["date"].to_list()
    left_nav = joined["left_nav"].to_numpy().astype(float)
    right_nav = joined["right_nav"].to_numpy().astype(float)
    left_nav = left_nav / left_nav[0]
    right_nav = right_nav / right_nav[0]
    return dates, left_nav, right_nav


def cagr_ratio(start_value: float, end_value: float, start: date, end: date) -> float:
    years = max((end - start).days / 365.25, 1e-9)
    return (end_value / start_value) ** (1.0 / years) - 1.0 if start_value > 0 and end_value > 0 else -1.0


def add_years(day: date, years: int) -> date:
    try:
        return date(day.year + years, day.month, day.day)
    except ValueError:
        # February 29 -> February 28 in non-leap years.
        return date(day.year + years, day.month, 28)


def rolling_excess(dates: list[date], strat: np.ndarray, bench: np.ndarray, years: int) -> dict[str, float | int | None]:
    min_days = int(years * 252 * 0.85)
    rows: list[float] = []
    for i, end in enumerate(dates):
        # Use calendar-year windows, picking the nearest prior date.
        anchor = add_years(end, -years)
        start_idx = np.searchsorted(dates, anchor, side="right") - 1
        if start_idx < 0 or i - start_idx < min_days:
            continue
        s_cagr = cagr_ratio(strat[start_idx], strat[i], dates[start_idx], end)
        b_cagr = cagr_ratio(bench[start_idx], bench[i], dates[start_idx], end)
        rows.append(s_cagr - b_cagr)
    if not rows:
        return {
            f"rolling_{years}y_count": 0,
            f"rolling_{years}y_win_rate": None,
            f"rolling_{years}y_median_excess": None,
            f"rolling_{years}y_min_excess": None,
        }
    arr = np.array(rows, dtype=float)
    return {
        f"rolling_{years}y_count": int(len(arr)),
        f"rolling_{years}y_win_rate": float((arr > 0).mean()),
        f"rolling_{years}y_median_excess": float(np.median(arr)),
        f"rolling_{years}y_min_excess": float(np.min(arr)),
    }


def start_date_win_rate(dates: list[date], strat: np.ndarray, bench: np.ndarray, years: int) -> dict[str, float | int | None]:
    min_days = int(years * 252 * 0.85)
    wins = 0
    count = 0
    for i, start in enumerate(dates):
        target = add_years(start, years)
        end_idx = np.searchsorted(dates, target, side="left")
        if end_idx >= len(dates) or end_idx - i < min_days:
            continue
        s_ret = strat[end_idx] / strat[i] - 1.0
        b_ret = bench[end_idx] / bench[i] - 1.0
        wins += int(s_ret > b_ret)
        count += 1
    return {
        f"start_{years}y_count": count,
        f"start_{years}y_win_rate": float(wins / count) if count else None,
    }


def max_consecutive(condition: np.ndarray) -> int:
    best = cur = 0
    for item in condition:
        if bool(item):
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def first_permanent_outperform_date(dates: list[date], relative: np.ndarray) -> date | None:
    suffix_min = np.minimum.accumulate(relative[::-1])[::-1]
    idxs = np.flatnonzero(suffix_min >= 1.0)
    if len(idxs) == 0:
        return None
    return dates[int(idxs[0])]


def relative_metrics(strategy_name: str, benchmark_name: str, strategy: pl.DataFrame, benchmark: pl.DataFrame) -> dict[str, object]:
    dates, strat, bench = nav_arrays(strategy, benchmark)
    if not dates:
        return {"strategy": strategy_name, "benchmark": benchmark_name, "error": "no_overlap"}
    relative = strat / np.maximum(bench, 1e-12)
    rel_peak = np.maximum.accumulate(relative)
    rel_dd = relative / np.maximum(rel_peak, 1e-12) - 1.0
    first_above_idx = np.flatnonzero(relative > 1.0)
    row: dict[str, object] = {
        "strategy": strategy_name,
        "benchmark": benchmark_name,
        "start": dates[0],
        "end": dates[-1],
        "days": len(dates),
        "final_relative_nav": float(relative[-1]),
        "first_outperform_date": dates[int(first_above_idx[0])] if len(first_above_idx) else None,
        "first_permanent_outperform_date": first_permanent_outperform_date(dates, relative),
        "pct_days_ahead": float((relative > 1.0).mean()),
        "longest_below_start_relative_days": max_consecutive(relative < 1.0),
        "max_relative_drawdown": float(np.min(rel_dd)),
        "longest_relative_drawdown_days": max_consecutive(rel_dd < -1e-9),
    }
    for years in (1, 3, 5):
        row.update(rolling_excess(dates, strat, bench, years))
        row.update(start_date_win_rate(dates, strat, bench, years))
    return row


def strategy_metrics(name: str, daily: pl.DataFrame) -> dict[str, object]:
    row = validate_daily_nav(name, daily.select(["date", "nav"]), n_trials=1)
    return {
        "strategy": name,
        "start": row.get("start"),
        "end": daily["date"].max(),
        "full_cagr": row["cagr"],
        "full_mdd": row["mdd"],
        "full_sortino": row["sortino"],
        "oos_cagr": row["oos_cagr"],
        "oos_mdd": row["oos_mdd"],
        "oos_sortino": row["oos_sortino"],
        "recent_1y_start": row["recent_1y_start"],
        "recent_1y_end": row["recent_1y_end"],
        "recent_1y_cagr": row["recent_1y_cagr"],
        "dsr": row["dsr"],
        "pbo": row["pbo"],
    }


def load_all_series() -> tuple[dict[str, pl.DataFrame], dict[str, pl.DataFrame]]:
    strategies: dict[str, pl.DataFrame] = {}
    for spec in DEFAULT_STRATEGIES:
        if spec.path and spec.path.exists():
            strategies[spec.name] = load_strategy(spec.path, spec.name)
    if not strategies:
        raise RuntimeError("No strategy daily artifacts found.")
    start = min(frame["date"].min() for frame in strategies.values())
    end = max(frame["date"].max() for frame in strategies.values())
    benchmarks = {
        spec.name: load_benchmark(spec.code or "", spec.market, spec.name, start, end)
        for spec in DEFAULT_BENCHMARKS
    }
    return strategies, benchmarks


def main() -> None:
    strategies, benchmarks = load_all_series()
    cutoff = max(frame["date"].max() for frame in strategies.values())
    metric_rows = [strategy_metrics(name, frame) for name, frame in strategies.items()]
    rel_rows = [
        relative_metrics(s_name, b_name, s_frame, b_frame)
        for s_name, s_frame in strategies.items()
        for b_name, b_frame in benchmarks.items()
    ]
    metrics = pl.DataFrame(metric_rows)
    relative = pl.DataFrame(rel_rows)
    metrics_path = OUT_DIR / "strategy_metrics.csv"
    relative_path = OUT_DIR / "relative_alpha_metrics.csv"
    metrics.write_csv(metrics_path)
    relative.write_csv(relative_path)
    print(f"data_cutoff={cutoff}")
    print(f"metrics={metrics_path}")
    print(f"relative={relative_path}")
    print(
        relative.select(
            [
                "strategy",
                "benchmark",
                "final_relative_nav",
                "first_outperform_date",
                "first_permanent_outperform_date",
                "pct_days_ahead",
                "longest_below_start_relative_days",
                "max_relative_drawdown",
                "rolling_3y_win_rate",
                "rolling_5y_win_rate",
                "start_3y_win_rate",
                "start_5y_win_rate",
            ]
        )
        .sort(["benchmark", "final_relative_nav"], descending=[False, True])
        .to_pandas()
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
