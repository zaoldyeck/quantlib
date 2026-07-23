"""iter_96 - robust benchmark-relative alpha research.

This pass is a first-principles restart focused on the user's current concern:
endpoint CAGR is not enough if the strategy only beats 2330 after the 2021 AI
cycle.  Candidates are therefore ranked by realistic execution plus
benchmark-relative robustness against both 0050 and 2330.

The design deliberately avoids hard-coded stocks or industries:

* universe: TWSE/TPEx common stocks from the PIT adjusted feature panel;
* signal: close-known leader score, executed at the next trading day's open;
* portfolio: long-only target book, optionally concentrated or broader;
* validation: Fubon realistic execution, canonical validator, and relative
  alpha diagnostics.
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from quantlib.constants import CAPITAL  # noqa: E402
from quantlib.db import connect  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    ExitConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from evaluation import nav_metrics, nav_returns  # noqa: E402
from iter_33_pm_first_principles import load_or_build_panel  # noqa: E402
from quantlib.prices import fetch_adjusted_panel  # noqa: E402
from validator import recent_one_year_metrics, validate_daily_nav  # noqa: E402


START = date(2005, 1, 3)
RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_96_robust_alpha_research"
N_TRIALS_PRIOR = 41_116 + 229 + 960


@dataclass(frozen=True)
class LeaderConfig:
    name: str
    score_kind: str
    schedule: str
    topn: int
    min_adv: float
    min_listed_days: int = 252
    trend_mode: str = "ma200"
    risk_mode: str = "none"
    quality_floor: str = "loose"
    weight_mode: str = "equal"
    max_weight: float = 1.0


def log(message: str) -> None:
    print(message, flush=True)


def latest_0050_day() -> date:
    con = connect(read_only=True)
    try:
        return con.sql(
            """
            SELECT MAX(date)
            FROM daily_quote
            WHERE market='twse' AND company_code='0050'
            """
        ).fetchone()[0]
    finally:
        con.close()


def add_years(day: date, years: int) -> date:
    try:
        return date(day.year + years, day.month, day.day)
    except ValueError:
        return date(day.year + years, day.month, 28)


def cagr_ratio(start_value: float, end_value: float, start: date, end: date) -> float:
    years = max((end - start).days / 365.25, 1e-9)
    if start_value <= 0 or end_value <= 0:
        return -1.0
    return (end_value / start_value) ** (1.0 / years) - 1.0


def z(col: str) -> pl.Expr:
    mean = pl.col(col).mean().over("date")
    std = pl.col(col).std().over("date")
    return ((pl.col(col) - mean) / std.clip(1e-9, None)).clip(-3.0, 3.0).fill_null(0.0)


def score_expr(kind: str) -> pl.Expr:
    if kind == "structural_leader":
        return (
            0.26 * pl.col("z_ret126")
            + 0.24 * pl.col("z_ret252")
            + 0.14 * pl.col("z_ret63")
            + 0.12 * pl.col("z_quality")
            + 0.10 * pl.col("z_rev")
            + 0.08 * pl.col("z_inst")
            + 0.06 * pl.col("z_ma200_gap")
            - 0.10 * pl.col("z_vol63")
        )
    if kind == "acceleration_leader":
        return (
            0.28 * pl.col("z_ret63")
            + 0.22 * pl.col("z_ret126")
            + 0.18 * pl.col("z_rev_accel")
            + 0.12 * pl.col("z_vol_ratio")
            + 0.10 * pl.col("z_inst")
            + 0.06 * pl.col("z_quality")
            + 0.04 * pl.col("z_ma200_gap")
            - 0.06 * pl.col("z_vol63")
        )
    if kind == "smooth_compounder":
        return (
            0.28 * pl.col("z_ret252")
            + 0.18 * pl.col("z_ret126")
            + 0.18 * pl.col("z_quality")
            + 0.14 * pl.col("z_rev")
            + 0.10 * pl.col("z_inst")
            + 0.08 * pl.col("z_ma200_gap")
            + 0.04 * pl.col("z_near_high")
            - 0.18 * pl.col("z_vol63")
        )
    if kind == "balanced_alpha":
        return (
            0.22 * pl.col("z_ret126")
            + 0.18 * pl.col("z_ret252")
            + 0.16 * pl.col("z_ret63")
            + 0.14 * pl.col("z_quality")
            + 0.12 * pl.col("z_rev")
            + 0.10 * pl.col("z_inst")
            + 0.08 * pl.col("z_near_high")
            - 0.10 * pl.col("z_vol63")
        )
    raise ValueError(f"unknown score_kind: {kind}")


def load_research_panel(end: date) -> tuple[pl.DataFrame, list[date], dict[date, float], dict[date, float]]:
    t0 = time.time()
    con = connect(read_only=True)
    try:
        panel, days = load_or_build_panel(con, START, end, use_cache=True)
        benchmark = (
            fetch_adjusted_panel(
                con,
                START.isoformat(),
                end.isoformat(),
                codes=["0050"],
                market="twse",
                include_extra_history_days=260,
            )
            .sort("date")
            .with_columns(pl.col("close").rolling_mean(200).alias("market_ma200"))
            .filter((pl.col("date") >= START) & (pl.col("date") <= end))
            .select(["date", "close", "market_ma200"])
        )
    finally:
        con.close()

    if panel.is_empty() or not days:
        raise RuntimeError("empty research panel")

    panel = panel.sort(["company_code", "date"]).with_columns(
        [
            pl.col("close").pct_change(21).over("company_code").alias("ret21"),
            pl.col("close").pct_change(63).over("company_code").alias("ret63"),
            pl.col("close").pct_change(126).over("company_code").alias("ret126"),
            pl.col("close").pct_change(252).over("company_code").alias("ret252"),
            pl.col("close").pct_change().over("company_code").alias("daily_ret"),
            pl.col("close").rolling_max(252).over("company_code").alias("hi252"),
        ]
    )
    panel = panel.with_columns(
        [
            pl.col("daily_ret").rolling_std(63).over("company_code").alias("vol63"),
            (pl.col("close") / pl.col("hi252") - 1.0).alias("dd252"),
            (pl.col("close") / pl.col("ma200") - 1.0).alias("ma200_gap"),
            (pl.col("close") / pl.col("hi252")).alias("near_high"),
            (pl.col("vol") / pl.col("vol_avg60")).alias("vol_ratio"),
            (
                0.40 * (pl.col("roa_ttm") / 0.12).clip(-1.0, 2.0)
                + 0.30 * (pl.col("gross_margin_ttm") / 0.35).clip(-1.0, 2.0)
                + 0.30 * ((pl.col("f_score_raw") - 4.0) / 3.0).clip(-1.0, 1.0)
            ).alias("quality_score"),
        ]
    )
    panel = panel.with_columns(
        [
            z("ret21").alias("z_ret21"),
            z("ret63").alias("z_ret63"),
            z("ret126").alias("z_ret126"),
            z("ret252").alias("z_ret252"),
            z("vol63").alias("z_vol63"),
            z("ma200_gap").alias("z_ma200_gap"),
            z("near_high").alias("z_near_high"),
            z("vol_ratio").alias("z_vol_ratio"),
            z("quality_score").alias("z_quality"),
            z("latest_yoy").alias("z_rev"),
            z("yoy_delta").alias("z_rev_accel"),
            z("inst_flow20").alias("z_inst"),
        ]
    )
    market_close = dict(zip(benchmark["date"].to_list(), benchmark["close"].to_list(), strict=True))
    market_ma200 = dict(zip(benchmark["date"].to_list(), benchmark["market_ma200"].to_list(), strict=True))
    log(
        f"[iter96] panel rows={panel.height:,} codes={panel['company_code'].n_unique():,} "
        f"days={len(days):,} end={end} elapsed={time.time() - t0:.1f}s"
    )
    return panel, days, market_ma200, market_close


def signal_dates(days: list[date], schedule: str) -> set[date]:
    if schedule == "weekly":
        return {
            days[i]
            for i in range(len(days) - 1)
            if days[i].isocalendar()[:2] != days[i + 1].isocalendar()[:2]
        }
    if schedule == "monthly":
        return {
            days[i]
            for i in range(len(days) - 1)
            if (days[i].year, days[i].month) != (days[i + 1].year, days[i + 1].month)
        }
    if schedule == "quarterly":
        return {
            days[i]
            for i in range(len(days) - 1)
            if (days[i].year, (days[i].month - 1) // 3)
            != (days[i + 1].year, (days[i + 1].month - 1) // 3)
        }
    raise ValueError(f"unknown schedule: {schedule}")


def base_filter(cfg: LeaderConfig) -> pl.Expr:
    expr = (
        (~pl.col("is_etf"))
        & (~pl.col("is_finance"))
        & (pl.col("listed_days") >= cfg.min_listed_days)
        & (pl.col("adv60") >= cfg.min_adv)
        & (pl.col("close") > 0)
        & (pl.col("open") > 0)
        & pl.col("ret63").is_not_null()
        & pl.col("ret126").is_not_null()
        & pl.col("ret252").is_not_null()
        & pl.col("vol63").is_not_null()
        & (pl.col("vol63") > 0)
    )
    if cfg.trend_mode == "ma200":
        expr &= pl.col("close") > pl.col("ma200")
    elif cfg.trend_mode == "ma100":
        expr &= pl.col("close") > pl.col("ma100")
    elif cfg.trend_mode == "positive_126":
        expr &= pl.col("ret126") > 0
    elif cfg.trend_mode == "none":
        pass
    else:
        raise ValueError(f"unknown trend_mode: {cfg.trend_mode}")

    if cfg.quality_floor == "loose":
        expr &= (
            (pl.col("f_score_raw").fill_null(0) >= 3)
            & (pl.col("gross_margin_ttm").fill_null(-999) >= 0.08)
            & (pl.col("roa_ttm").fill_null(-999) >= 0.00)
        )
    elif cfg.quality_floor == "quality":
        expr &= (
            (pl.col("f_score_raw").fill_null(0) >= 4)
            & (pl.col("gross_margin_ttm").fill_null(-999) >= 0.15)
            & (pl.col("roa_ttm").fill_null(-999) >= 0.04)
        )
    elif cfg.quality_floor == "none":
        pass
    else:
        raise ValueError(f"unknown quality_floor: {cfg.quality_floor}")
    return expr


def weight_book(codes: list[str], scores: np.ndarray, cfg: LeaderConfig, gross: float) -> dict[str, float]:
    if not codes or gross <= 0:
        return {}
    if cfg.weight_mode == "score":
        x = scores - float(np.nanmin(scores))
        weights = x / x.sum() if np.isfinite(x).all() and x.sum() > 0 else np.full(len(codes), 1.0 / len(codes))
    elif cfg.weight_mode == "rank_tilt":
        x = np.arange(len(codes), 0, -1, dtype=float)
        weights = x / x.sum()
    elif cfg.weight_mode == "equal":
        weights = np.full(len(codes), 1.0 / len(codes))
    else:
        raise ValueError(f"unknown weight_mode: {cfg.weight_mode}")
    weights = weights * gross
    if cfg.max_weight < 1.0:
        weights = np.minimum(weights, cfg.max_weight)
        scale = gross / weights.sum() if weights.sum() > 0 else 1.0
        weights = weights * min(scale, 1.0)
    return {code: float(weight) for code, weight in zip(codes, weights, strict=True) if weight > 1e-12}


def risk_gross(signal_day: date, cfg: LeaderConfig, market_ma200: dict[date, float], market_close: dict[date, float]) -> float:
    if cfg.risk_mode == "none":
        return 1.0
    close = market_close.get(signal_day)
    ma200 = market_ma200.get(signal_day)
    if close is None or ma200 is None or ma200 <= 0:
        return 1.0
    risk_off = close < ma200
    if not risk_off:
        return 1.0
    if cfg.risk_mode == "half":
        return 0.5
    if cfg.risk_mode == "cash":
        return 0.0
    raise ValueError(f"unknown risk_mode: {cfg.risk_mode}")


def market_close_lookup(panel: pl.DataFrame) -> dict[date, float]:
    frame = panel.filter(pl.col("company_code") == "0050").select(["date", "close"]).sort("date")
    return dict(zip(frame["date"].to_list(), frame["close"].to_list(), strict=True))


def build_targets(
    panel: pl.DataFrame,
    days: list[date],
    market_ma200: dict[date, float],
    market_close: dict[date, float],
    cfg: LeaderConfig,
) -> dict[date, dict[str, float]]:
    sig_dates = signal_dates(days, cfg.schedule)
    day_to_next = {days[i]: days[i + 1] for i in range(len(days) - 1)}
    score_col = "__score"
    candidates = (
        panel.filter(pl.col("date").is_in(sig_dates))
        .filter(base_filter(cfg))
        .with_columns(score_expr(cfg.score_kind).alias(score_col))
        .filter(pl.col(score_col).is_finite())
        .sort(["date", score_col, "company_code"], descending=[False, True, False])
        .select(["date", "company_code", score_col])
    )
    targets: dict[date, dict[str, float]] = {}
    for key, sub in candidates.group_by("date", maintain_order=True):
        sig = key[0] if isinstance(key, tuple) else key
        exec_day = day_to_next.get(sig)
        if exec_day is None:
            continue
        top = sub.head(cfg.topn)
        gross = risk_gross(sig, cfg, market_ma200, market_close)
        targets[exec_day] = weight_book(
            [str(code) for code in top["company_code"].to_list()],
            top[score_col].to_numpy().astype(float),
            cfg,
            gross,
        )
    return {day: book for day, book in targets.items() if book or cfg.risk_mode == "cash"}


def target_rows(targets: dict[date, dict[str, float]]) -> pl.DataFrame:
    rows = [
        {"date": day, "company_code": code, "target_weight": weight}
        for day, book in sorted(targets.items())
        for code, weight in sorted(book.items())
    ]
    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={"date": pl.Date, "company_code": pl.Utf8, "target_weight": pl.Float64}
    )


def build_price_lookup(panel: pl.DataFrame, codes: set[str]) -> dict[tuple[date, str], tuple[float, float]]:
    px = (
        panel.filter(pl.col("company_code").is_in(sorted(codes)))
        .select(["date", "company_code", "open", "close"])
        .sort(["company_code", "date"])
    )
    return {
        (row["date"], str(row["company_code"])): (float(row["open"]), float(row["close"]))
        for row in px.iter_rows(named=True)
    }


def paper_simulate(
    days: list[date],
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    targets: dict[date, dict[str, float]],
) -> tuple[pl.DataFrame, dict[str, float]]:
    """Fast first-pass target-book simulator.

    This is intentionally only a funnel: it includes open execution, commission
    and sell tax, but not volume caps, limit blocks, or slippage. Final numbers
    must always come from RealisticExecutionSimulator.
    """
    commission = 0.000285
    sell_tax = 0.003
    cash = CAPITAL
    shares: dict[str, float] = {}
    last_close: dict[str, float] = {}
    active_target: dict[str, float] = {}
    rows: list[dict[str, object]] = []
    turnover_sum = 0.0
    trade_days = 0
    max_active = 0

    for day in days:
        open_values: dict[str, float] = {}
        for code, qty in sorted(shares.items()):
            op, cl = price_lookup.get((day, code), (last_close.get(code, 0.0), last_close.get(code, 0.0)))
            if op <= 0 or cl <= 0:
                op = cl = last_close.get(code, 0.0)
            open_values[code] = qty * op
            if cl > 0:
                last_close[code] = cl
        nav_open = cash + sum(open_values.values())

        turnover = 0.0
        if day in targets:
            active_target = targets[day]
            all_codes = sorted(set(shares) | set(active_target))
            target_values = {code: nav_open * active_target.get(code, 0.0) for code in all_codes}
            deltas = {code: target_values[code] - open_values.get(code, 0.0) for code in all_codes}
            turnover = sum(abs(value) for value in deltas.values()) / max(nav_open, 1e-9)

            for code, delta in sorted(deltas.items()):
                if delta >= 0 or code not in shares:
                    continue
                op, _cl = price_lookup.get((day, code), (last_close.get(code, 0.0), last_close.get(code, 0.0)))
                if op <= 0:
                    continue
                sell_value = min(-delta, shares[code] * op)
                sell_shares = sell_value / op
                shares[code] -= sell_shares
                cash += sell_value * (1.0 - commission - sell_tax)
                if shares.get(code, 0.0) <= 1e-9:
                    shares.pop(code, None)

            buy_demand = [(code, delta) for code, delta in sorted(deltas.items()) if delta > 0]
            needed = sum(value * (1.0 + commission) for _code, value in buy_demand)
            scale = min(1.0, cash / needed) if needed > 0 else 1.0
            for code, delta in buy_demand:
                op, cl = price_lookup.get((day, code), (0.0, 0.0))
                if op <= 0 or cl <= 0:
                    continue
                buy_value = delta * scale
                cost = buy_value * (1.0 + commission)
                if cost > cash + 1e-6:
                    continue
                shares[code] = shares.get(code, 0.0) + buy_value / op
                cash -= cost
                last_close[code] = cl

        nav_close = cash
        for code, qty in sorted(shares.items()):
            _op, cl = price_lookup.get((day, code), (last_close.get(code, 0.0), last_close.get(code, 0.0)))
            if cl <= 0:
                cl = last_close.get(code, 0.0)
            nav_close += qty * cl
            if cl > 0:
                last_close[code] = cl
        if turnover > 1e-8:
            trade_days += 1
            turnover_sum += turnover
        max_active = max(max_active, len(shares))
        rows.append({"date": day, "nav": nav_close, "active": len(shares), "turnover": turnover})

    daily = pl.DataFrame(rows)
    stats = {
        "paper_trade_days": float(trade_days),
        "paper_avg_turnover_trade_day": turnover_sum / trade_days if trade_days else 0.0,
        "paper_max_active": float(max_active),
    }
    return daily, stats


def quick_nav_row(name: str, daily: pl.DataFrame, extra: dict[str, object]) -> dict[str, object]:
    """Fast screen metrics without bootstrap / DSR / PBO.

    The output schema intentionally mirrors enough of validate_daily_nav for
    ranking, but rows from this helper must never be reported as final
    validated strategy results.
    """
    ordered = daily.select(["date", "nav"]).sort("date")
    full = nav_metrics(ordered, capital=CAPITAL)
    dates = ordered["date"].to_list()
    nav = ordered["nav"].to_numpy().astype(float)
    rets = nav_returns(nav, CAPITAL)
    ret_frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos_ret = ret_frame.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_dates = oos_ret["date"].to_list()
    oos_nav = CAPITAL * np.cumprod(1.0 + oos_ret["ret"].to_numpy())
    oos_daily = pl.DataFrame({"date": oos_dates, "nav": oos_nav}) if oos_dates else pl.DataFrame({"date": [], "nav": []})
    oos = nav_metrics(oos_daily, capital=CAPITAL, prefix="oos_")
    row: dict[str, object] = {
        "name": name,
        "full_days": ordered.height,
        "oos_days": oos_daily.height,
        **full,
        **oos,
        **recent_one_year_metrics(ordered, capital=CAPITAL),
        "dsr": 1.0,
        "pbo": 0.0,
        "lo_p": 0.0,
        "boot_cagr_lb": 0.0,
        "boot_cagr_ub": 0.0,
        "boot_sortino_lb": 0.0,
        "boot_sortino_ub": 0.0,
    }
    row.update(extra)
    return row


def load_benchmark_nav(code: str, start: date, end: date, name: str) -> pl.DataFrame:
    con = connect(read_only=True)
    try:
        panel = fetch_adjusted_panel(
            con,
            start.isoformat(),
            end.isoformat(),
            codes=[code],
            market="twse",
            include_extra_history_days=20,
        ).sort("date")
    finally:
        con.close()
    base = float(panel["close"][0])
    return panel.select(["date", (pl.col("close") / base * CAPITAL).alias("nav")]).with_columns(
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
    return dates, left_nav / left_nav[0], right_nav / right_nav[0]


def max_consecutive(mask: np.ndarray) -> int:
    best = cur = 0
    for value in mask:
        if bool(value):
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def first_permanent_outperform(dates: list[date], relative: np.ndarray) -> date | None:
    suffix_min = np.minimum.accumulate(relative[::-1])[::-1]
    idxs = np.flatnonzero(suffix_min >= 1.0)
    return dates[int(idxs[0])] if len(idxs) else None


def rolling_excess_stats(
    dates: list[date],
    ordinals: np.ndarray,
    strategy: np.ndarray,
    benchmark: np.ndarray,
    years: int,
) -> dict[str, float]:
    rows: list[float] = []
    min_days = int(years * 252 * 0.85)
    for i, end in enumerate(dates):
        anchor = add_years(end, -years)
        j = int(np.searchsorted(ordinals, anchor.toordinal(), side="right") - 1)
        if j < 0 or i - j < min_days:
            continue
        s_cagr = cagr_ratio(strategy[j], strategy[i], dates[j], end)
        b_cagr = cagr_ratio(benchmark[j], benchmark[i], dates[j], end)
        rows.append(s_cagr - b_cagr)
    if not rows:
        return {
            f"rolling_{years}y_win_rate": 0.0,
            f"rolling_{years}y_median_excess": 0.0,
            f"rolling_{years}y_min_excess": 0.0,
        }
    arr = np.asarray(rows, dtype=float)
    return {
        f"rolling_{years}y_win_rate": float((arr > 0).mean()),
        f"rolling_{years}y_median_excess": float(np.median(arr)),
        f"rolling_{years}y_min_excess": float(np.min(arr)),
    }


def start_win_rate(
    dates: list[date],
    ordinals: np.ndarray,
    strategy: np.ndarray,
    benchmark: np.ndarray,
    years: int,
) -> float:
    min_days = int(years * 252 * 0.85)
    wins = 0
    count = 0
    for i, start in enumerate(dates):
        target = add_years(start, years)
        j = int(np.searchsorted(ordinals, target.toordinal(), side="left"))
        if j >= len(dates) or j - i < min_days:
            continue
        wins += int(strategy[j] / strategy[i] > benchmark[j] / benchmark[i])
        count += 1
    return float(wins / count) if count else 0.0


def relative_metrics(strategy: pl.DataFrame, benchmark: pl.DataFrame, prefix: str) -> dict[str, object]:
    dates, s, b = nav_arrays(strategy, benchmark)
    if not dates:
        return {}
    ordinals = np.asarray([day.toordinal() for day in dates], dtype=np.int32)
    relative = s / np.maximum(b, 1e-12)
    peak = np.maximum.accumulate(relative)
    rel_dd = relative / np.maximum(peak, 1e-12) - 1.0
    first = np.flatnonzero(relative > 1.0)
    row: dict[str, object] = {
        f"{prefix}_final_relative_nav": float(relative[-1]),
        f"{prefix}_first_outperform_date": dates[int(first[0])] if len(first) else None,
        f"{prefix}_first_permanent_outperform_date": first_permanent_outperform(dates, relative),
        f"{prefix}_pct_days_ahead": float((relative > 1.0).mean()),
        f"{prefix}_longest_below_start_days": max_consecutive(relative < 1.0),
        f"{prefix}_max_relative_drawdown": float(np.min(rel_dd)),
        f"{prefix}_start_3y_win_rate": start_win_rate(dates, ordinals, s, b, 3),
        f"{prefix}_start_5y_win_rate": start_win_rate(dates, ordinals, s, b, 5),
    }
    for years in (1, 3, 5):
        stats = rolling_excess_stats(dates, ordinals, s, b, years)
        row.update({f"{prefix}_{key}": value for key, value in stats.items()})
    return row


def robust_alpha_objective(row: dict[str, object]) -> float:
    oos = max(float(row.get("oos_cagr") or 0.0), 0.0)
    recent = min(max(float(row.get("recent_1y_cagr") or 0.0), 0.0), 3.5)
    mdd_abs = abs(float(row.get("oos_mdd") or 0.0))
    sortino = max(float(row.get("oos_sortino") or 0.0), 0.0)
    dsr = max(float(row.get("dsr") or 0.0), 0.0)
    pbo = max(float(row.get("pbo") or 0.0), 0.0)
    fill = max(float(row.get("fill_ratio") or 0.0), 0.0)
    rel0050 = max(float(row.get("b0050_final_relative_nav") or 0.0), 0.0)
    rel2330 = max(float(row.get("b2330_final_relative_nav") or 0.0), 0.0)
    win0050 = float(row.get("b0050_rolling_3y_win_rate") or 0.0)
    win2330 = float(row.get("b2330_rolling_3y_win_rate") or 0.0)
    start2330 = float(row.get("b2330_start_5y_win_rate") or 0.0)
    rel_dd2330 = abs(float(row.get("b2330_max_relative_drawdown") or 0.0))
    below2330 = float(row.get("b2330_longest_below_start_days") or 0.0)

    mdd_factor = 1.0 if mdd_abs <= 0.32 else max(0.35, 0.32 / max(mdd_abs, 1e-9))
    rel_dd_factor = 1.0 if rel_dd2330 <= 0.45 else max(0.35, 0.45 / max(rel_dd2330, 1e-9))
    below_factor = 1.0 if below2330 <= 1_800 else max(0.35, 1_800 / max(below2330, 1.0))
    return (
        oos
        * (1.0 + recent)
        * min(1.25, max(0.50, sortino / 2.0))
        * mdd_factor
        * min(1.0, max(0.40, dsr / 0.95))
        * min(1.0, max(0.35, (0.50 - pbo) / 0.50))
        * min(1.0, max(0.60, fill / 0.85))
        * min(1.20, max(0.50, np.log1p(rel0050) / np.log(10.0)))
        * min(1.20, max(0.45, np.log1p(rel2330) / np.log(3.0)))
        * min(1.15, max(0.45, win0050 / 0.75))
        * min(1.15, max(0.35, win2330 / 0.60))
        * min(1.10, max(0.35, start2330 / 0.55))
        * rel_dd_factor
        * below_factor
    )


def build_configs() -> list[LeaderConfig]:
    configs: list[LeaderConfig] = []
    seen: set[str] = set()
    for score_kind in ("structural_leader", "acceleration_leader", "smooth_compounder", "balanced_alpha"):
        for schedule in ("weekly", "monthly"):
            for topn in (3, 5, 10, 20):
                for trend_mode in ("ma200", "positive_126"):
                    quality_floors = ("loose", "quality") if score_kind in {"structural_leader", "smooth_compounder"} else ("loose",)
                    for quality_floor in quality_floors:
                        # Keep this as a first robust-alpha sweep, not an
                        # unlimited curve-fit.  Exit rules are tested only on
                        # the best coarse candidates below.
                        weight_modes = ("equal", "rank_tilt") if topn <= 10 else ("equal",)
                        for weight_mode in weight_modes:
                            name = (
                                f"iter96_{score_kind}_{schedule}_top{topn}"
                                f"_adv50_{trend_mode}_{quality_floor}_{weight_mode}"
                            )
                            if name in seen:
                                continue
                            seen.add(name)
                            configs.append(
                                LeaderConfig(
                                    name=name,
                                    score_kind=score_kind,
                                    schedule=schedule,
                                    topn=topn,
                                    min_adv=50_000_000.0,
                                    trend_mode=trend_mode,
                                    quality_floor=quality_floor,
                                    weight_mode=weight_mode,
                                    max_weight=0.35 if weight_mode != "equal" else 1.0,
                                )
                            )
    return configs


def main() -> None:
    t0 = time.time()
    RESULTS.mkdir(parents=True, exist_ok=True)
    end = latest_0050_day()
    panel, days, market_ma200, market_close = load_research_panel(end)
    configs = build_configs()
    n_trials = N_TRIALS_PRIOR + len(configs)
    log(f"[iter96] configs={len(configs)} n_trials={n_trials}")

    target_sets: dict[str, dict[date, dict[str, float]]] = {}
    build_rows: list[dict[str, object]] = []
    all_codes: set[str] = set()
    for i, cfg in enumerate(configs, 1):
        targets = build_targets(panel, days, market_ma200, market_close, cfg)
        codes = {code for book in targets.values() for code in book}
        all_codes |= codes
        target_sets[cfg.name] = targets
        build_rows.append(
            {
                **asdict(cfg),
                "target_rebalance_days": len(targets),
                "candidate_codes": len(codes),
            }
        )
        if i % 32 == 0 or i == len(configs):
            log(f"[iter96] target build {i:03d}/{len(configs)} codes={len(all_codes):,}")

    if not all_codes:
        raise RuntimeError("no target codes generated")

    benchmarks = {
        "b0050": load_benchmark_nav("0050", days[0], days[-1], "0050 total return"),
        "b2330": load_benchmark_nav("2330", days[0], days[-1], "2330 total return"),
    }
    con = connect(read_only=True)
    try:
        data_cutoff = con.sql("SELECT MAX(date) FROM daily_quote").fetchone()[0]
    finally:
        con.close()

    build_by_name = {str(row["name"]): row for row in build_rows}
    price_lookup = build_price_lookup(panel, all_codes)
    paper_rows: list[dict[str, object]] = []
    log(f"[iter96] paper screen price rows={len(price_lookup):,}")
    for i, cfg in enumerate([cfg for cfg in configs if target_sets[cfg.name]], 1):
        daily, stats = paper_simulate(days, price_lookup, target_sets[cfg.name])
        row = quick_nav_row(
            f"{cfg.name}__paper",
            daily.select(["date", "nav"]),
            extra={
                **stats,
                **build_by_name[cfg.name],
                "exit_config": "paper",
                "search_stage": "paper_screen",
                "fill_ratio": 1.0,
                "data_cutoff": data_cutoff,
            },
        )
        for prefix, bench in benchmarks.items():
            row.update(relative_metrics(daily.select(["date", "nav"]), bench, prefix))
        row["robust_alpha_objective"] = robust_alpha_objective(row)
        paper_rows.append(row)
        if i % 24 == 0 or i == len(configs):
            log(
                f"[iter96 paper] {i:03d}/{len(configs)} best_obj="
                f"{max(float(r['robust_alpha_objective']) for r in paper_rows):.4f}"
            )

    paper = pl.DataFrame(paper_rows).sort("robust_alpha_objective", descending=True)
    paper_path = RESULTS / f"{OUT_PREFIX}_paper_screen.csv"
    paper.write_csv(paper_path)
    selected: list[str] = []
    for cols, descending, n in [
        (["robust_alpha_objective"], [True], 18),
        (["oos_cagr", "recent_1y_cagr"], [True, True], 10),
        (["recent_1y_cagr", "oos_cagr"], [True, True], 10),
        (["b2330_rolling_3y_win_rate", "b2330_final_relative_nav"], [True, True], 10),
        (["b0050_rolling_3y_win_rate", "b0050_final_relative_nav"], [True, True], 8),
    ]:
        for name in paper.sort(cols, descending=descending).head(n)["name"].to_list():
            base_name = str(name).removesuffix("__paper")
            if base_name not in selected:
                selected.append(base_name)
    selected = selected[:24]
    selected_codes = {
        code
        for cfg_name in selected
        for book in target_sets[cfg_name].values()
        for code in book
    }
    log(f"[iter96] realistic_candidates={len(selected)} selected_codes={len(selected_codes):,}")

    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, sorted(selected_codes), days[0], days[-1])
    finally:
        con.close()
    log(f"[iter96] execution bars rows={bars.height:,} codes={bars['company_code'].n_unique():,} cutoff={data_cutoff}")

    no_exit = ExitConfig(name="no_exit")
    focused_exit_configs = [
        no_exit,
        ExitConfig(name="time50_r-1", time_stop_days=50, time_stop_min_return_pct=-0.01),
        ExitConfig(name="time70_r-1", time_stop_days=70, time_stop_min_return_pct=-0.01),
        ExitConfig(name="tr30", trailing_stop_pct=0.30),
        ExitConfig(name="tp150_time50_r-1", take_profit_pct=1.50, time_stop_days=50, time_stop_min_return_pct=-0.01),
    ]
    base_config = ExecutionConfig(
        name="fubon_odd_lot_5pct_vol_slip5bp_iter96",
        capital=CAPITAL,
        lot_size=1,
        max_participation_rate=0.05,
        fixed_slippage_bps=5.0,
        impact_bps_per_1pct_volume=1.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
    )
    simulator = RealisticExecutionSimulator(bars, base_config)

    rows: list[dict[str, object]] = []
    artifacts: list[tuple[float, str, ExitConfig, object]] = []

    def run_one(cfg: LeaderConfig, exit_config: ExitConfig, stage: str) -> tuple[dict[str, object], object]:
        targets = target_sets[cfg.name]
        simulator.config = ExecutionConfig(
            **{
                **asdict(base_config),
                "fee_schedule": base_config.fee_schedule,
                "exit_config": exit_config,
            }
        )
        result = simulator.simulate(days, targets)
        row = validate_daily_nav(
            f"{cfg.name}__{exit_config.name}",
            result.daily.select(["date", "nav"]),
            n_trials=n_trials,
            extra={
                **result.stats,
                **build_by_name[cfg.name],
                "exit_config": exit_config.name,
                "search_stage": stage,
                "data_cutoff": data_cutoff,
            },
        )
        for prefix, bench in benchmarks.items():
            row.update(relative_metrics(result.daily.select(["date", "nav"]), bench, prefix))
        row["robust_alpha_objective"] = robust_alpha_objective(row)
        return row, result

    cfg_by_name = {cfg.name: cfg for cfg in configs}
    log(f"[iter96] focused_exit_candidates={len(selected)} exit_configs={len(focused_exit_configs)}")

    total_focused = len(selected) * len(focused_exit_configs)
    focused_idx = 0
    for cfg_name in selected:
        cfg = cfg_by_name[cfg_name]
        for exit_config in focused_exit_configs:
            focused_idx += 1
            row, result = run_one(cfg, exit_config, "focused_exit")
            rows.append(row)
            artifacts.append((float(row["robust_alpha_objective"]), cfg.name, exit_config, result))
            artifacts = sorted(artifacts, key=lambda item: item[0], reverse=True)[:12]
            if focused_idx % 16 == 0 or focused_idx == total_focused:
                log(
                    f"[iter96 focused] {focused_idx:03d}/{total_focused} best_obj="
                    f"{max(float(r['robust_alpha_objective']) for r in rows):.4f}"
                )

    summary = pl.DataFrame(rows).sort("robust_alpha_objective", descending=True)
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.write_csv(summary_path)
    pl.DataFrame(build_rows).write_csv(RESULTS / f"{OUT_PREFIX}_target_build_summary.csv")

    keep = {
        f"{row['name']}__{row['exit_config']}"
        for row in summary.head(8).iter_rows(named=True)
    }
    for _score, cfg_name, exit_config, result in sorted(artifacts, key=lambda item: item[0], reverse=True):
        key = f"{cfg_name}__{exit_config.name}"
        if key not in keep:
            continue
        safe = key.replace("/", "_")
        result.daily.write_csv(RESULTS / f"{OUT_PREFIX}_{safe}_daily.csv")
        result.fills.write_csv(RESULTS / f"{OUT_PREFIX}_{safe}_fills.csv")
        result.trades.write_csv(RESULTS / f"{OUT_PREFIX}_{safe}_trades.csv")
        target_rows(target_sets[cfg_name]).write_csv(RESULTS / f"{OUT_PREFIX}_{safe}_target_weights.csv")

    view = summary.select(
        [
            "name",
            "score_kind",
            "schedule",
            "topn",
            "min_adv",
            "trend_mode",
            "quality_floor",
            "weight_mode",
            "exit_config",
            pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
            pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
            pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
            pl.col("oos_sortino").round(3),
            pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
            pl.col("dsr").round(3),
            pl.col("pbo").round(3),
            pl.col("fill_ratio").mul(100).round(2).alias("fill_ratio_pct"),
            pl.col("b0050_final_relative_nav").round(3),
            pl.col("b0050_first_permanent_outperform_date"),
            pl.col("b0050_rolling_3y_win_rate").mul(100).round(1).alias("b0050_roll3y_win_pct"),
            pl.col("b2330_final_relative_nav").round(3),
            pl.col("b2330_first_permanent_outperform_date"),
            pl.col("b2330_rolling_3y_win_rate").mul(100).round(1).alias("b2330_roll3y_win_pct"),
            pl.col("b2330_start_5y_win_rate").mul(100).round(1).alias("b2330_start5y_win_pct"),
            pl.col("b2330_longest_below_start_days"),
            pl.col("robust_alpha_objective").round(4),
        ]
    ).head(20)
    print("=" * 180)
    print("iter_96 robust benchmark-relative alpha research")
    print("=" * 180)
    print(view.to_pandas().to_string(index=False))
    print(f"Saved: {summary_path}")
    print(f"[iter96] elapsed={time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
