"""Common validation harness for strategy daily NAV series.

All strategy iter scripts should converge here instead of each carrying a
slightly different validation stack.  This module evaluates a daily NAV with
the same OOS window, overfit diagnostics, path-quality metrics, and robust
growth objective.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping

import numpy as np
import polars as pl

from evaluation import CAPITAL_DEFAULT, RF_DEFAULT, nav_metrics, nav_returns, robust_growth_score
from validate_hybrid import (
    N_TRIALS_DSR,
    bootstrap_ci,
    deflated_sharpe,
    lo_2002_sharpe_test,
    pbo_cscv,
    walk_forward_folds,
)


@dataclass(frozen=True)
class ValidationConfig:
    capital: float = CAPITAL_DEFAULT
    rf: float = RF_DEFAULT
    oos_start_year: int = 2010
    oos_end_year: int = 2025
    min_trials_for_dsr: int = N_TRIALS_DSR


def recent_one_year_metrics(daily: pl.DataFrame, capital: float = CAPITAL_DEFAULT) -> dict[str, object]:
    ordered = daily.sort("date")
    dates = ordered["date"].to_list()
    navs = ordered["nav"].to_numpy().astype(float)
    if not dates:
        return {"recent_1y_start": None, "recent_1y_end": None, "recent_1y_cagr": 0.0}
    end = dates[-1]
    anchor = date(end.year - 1, end.month, end.day)
    candidates = [d for d in dates if d <= anchor]
    start = candidates[-1] if candidates else dates[0]
    nav_lookup = dict(zip(dates, navs, strict=True))
    base = nav_lookup[start] if start in nav_lookup else capital
    years = max((end - start).days / 365.25, 1e-9)
    ending_ratio = nav_lookup[end] / base if base > 0 else 0.0
    return {
        "recent_1y_start": start,
        "recent_1y_end": end,
        "recent_1y_cagr": ending_ratio ** (1.0 / years) - 1.0 if ending_ratio > 0 else -1.0,
    }


def validate_daily_nav(
    name: str,
    daily: pl.DataFrame,
    *,
    n_trials: int = 1,
    extra: Mapping[str, object] | None = None,
    config: ValidationConfig = ValidationConfig(),
) -> dict[str, object]:
    """Validate a strategy daily NAV series with the canonical research gates."""
    ordered = daily.select(["date", "nav"]).sort("date")
    dates = ordered["date"].to_list()
    nav = ordered["nav"].to_numpy().astype(float)
    rets = nav_returns(nav, config.capital)

    full = nav_metrics(ordered, capital=config.capital, rf=config.rf)
    ret_frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos_ret_frame = ret_frame.filter(
        (pl.col("year") >= config.oos_start_year) & (pl.col("year") <= config.oos_end_year)
    )
    oos_dates = oos_ret_frame["date"].to_list()
    oos_nav = config.capital * np.cumprod(1.0 + oos_ret_frame["ret"].to_numpy())
    oos_daily = pl.DataFrame({"date": oos_dates, "nav": oos_nav}) if oos_dates else pl.DataFrame({"date": [], "nav": []})
    oos = nav_metrics(oos_daily, capital=config.capital, rf=config.rf, prefix="oos_")
    oos_rets = oos_ret_frame["ret"].to_numpy()

    lo = lo_2002_sharpe_test(oos_rets)
    boot = bootstrap_ci(oos_rets, oos_dates) if len(oos_rets) > 0 else {
        "cagr_lb": 0.0,
        "cagr_ub": 0.0,
        "sortino_lb": 0.0,
        "sortino_ub": 0.0,
    }
    dsr = deflated_sharpe(
        float(oos.get("oos_sharpe", 0.0)),
        max(config.min_trials_for_dsr, n_trials),
        oos_rets,
    )
    pbo = pbo_cscv(walk_forward_folds(rets, dates))

    row: dict[str, object] = {
        "name": name,
        "full_days": len(dates),
        "oos_days": len(oos_dates),
        **full,
        **oos,
        "lo_p": lo["p_value"],
        "boot_cagr_lb": boot["cagr_lb"],
        "boot_cagr_ub": boot["cagr_ub"],
        "boot_sortino_lb": boot["sortino_lb"],
        "boot_sortino_ub": boot["sortino_ub"],
        "dsr": dsr,
        "pbo": pbo,
        **recent_one_year_metrics(ordered, capital=config.capital),
    }
    if extra:
        row.update(dict(extra))
    row["robust_growth_score"] = robust_growth_score(row)
    return row
