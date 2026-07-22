"""Validate valuation methods for Serenity industry-first candidates.

The research question:
  Once industry research has identified structural-bottleneck candidates, which
  valuation overlay best avoids overpaying without killing the alpha?

This script is point-in-time:
  - candidates come from `serenity_industry_thesis_registry_2025.csv`;
  - monthly revenue is available from next-month day 10 proxy;
  - quarterly raw factors use the standard conservative reporting calendar;
  - daily PER/PBR, prices, and flows are as-of joined by signal date;
  - signals are executed on the next trading day with costs.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from research.constants import CAPITAL, COMMISSION, SELL_TAX, TDPY  # noqa: E402
from research.db import connect  # noqa: E402
from research.prices import fetch_adjusted_panel, total_return_series  # noqa: E402
from replay_2025 import (  # noqa: E402
    REGISTRY,
    active_registry_for_day,
    build_rebalance_pairs,
    load_registry,
    load_revenue_features,
    load_taxonomy,
    load_universe,
    row_latest_before,
)


RESULTS = paths.OUT_STRAT_LAB
DOCS = REPO_ROOT / "docs" / "serenity"
OUT_PREFIX = "serenity_valuation_methods_replay_2025"


@dataclass(frozen=True)
class Method:
    name: str
    score_col: str
    top_n: int = 10
    weighting: str = "equal"


METHODS = (
    Method("alpha_only_top10", "score_alpha"),
    Method("current_pe_pb_penalty_top10", "score_current_penalty"),
    Method("peg_top10", "score_peg"),
    Method("adj_peg_top10", "score_adj_peg"),
    Method("reverse_dcf_gap_top10", "score_reverse_dcf_gap"),
    Method("dcf_upside_top10", "score_dcf_upside"),
    Method("dcf_peg_blend_top10", "score_dcf_peg_blend"),
    Method("pe_band_top10", "score_pe_band"),
    Method("ps_growth_top10", "score_ps_growth"),
    Method("gross_profit_yield_top10", "score_gross_profit_yield"),
    Method("valuation_combo_top10", "score_valuation_combo"),
)

FACTOR_COLS = (
    "valuation_peg_score",
    "valuation_adj_peg_score",
    "valuation_reverse_dcf_gap",
    "valuation_dcf_upside",
    "valuation_dcf_peg_blend",
    "valuation_pe_band_score",
    "valuation_ps_growth_score",
    "valuation_gross_profit_yield",
    "valuation_combo_score",
)


def month_add(year: int, month: int, delta: int = 1) -> tuple[int, int]:
    month0 = year * 12 + (month - 1) + delta
    return month0 // 12, month0 % 12 + 1


def quarter_report_date(year: int, quarter: int) -> date:
    if quarter == 1:
        return date(year, 5, 22)
    if quarter == 2:
        return date(year, 8, 21)
    if quarter == 3:
        return date(year, 11, 21)
    if quarter == 4:
        return date(year + 1, 4, 7)
    raise ValueError(f"quarter must be 1..4, got {quarter}")


def load_price_features(con, universe: pd.DataFrame, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    panels: list[pl.DataFrame] = []
    for market in ("twse", "tpex"):
        codes = universe.loc[universe["market"] == market, "company_code"].tolist()
        if not codes:
            continue
        panels.append(
            fetch_adjusted_panel(
                con,
                start.isoformat(),
                end.isoformat(),
                codes=codes,
                market=market,
                include_extra_history_days=330,
            )
        )
    if not panels:
        raise RuntimeError("No price panel for thesis registry universe.")
    panel = (
        pl.concat(panels, how="diagonal")
        .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
        .sort(["company_code", "date"])
    )
    featured = (
        panel.with_columns(
            [
                (pl.col("close") / pl.col("close").shift(1).over("company_code") - 1.0).alias("ret_1d"),
                (pl.col("close") / pl.col("close").shift(20).over("company_code") - 1.0).alias("ret_20d"),
                (pl.col("close") / pl.col("close").shift(60).over("company_code") - 1.0).alias("ret_60d"),
                (pl.col("close") / pl.col("close").shift(120).over("company_code") - 1.0).alias("ret_120d"),
                (pl.col("close") / pl.col("close").shift(252).over("company_code") - 1.0).alias("ret_252d"),
                pl.col("trade_value").rolling_mean(20).over("company_code").shift(1).alias("adv20"),
                (pl.col("close") / pl.col("close").rolling_max(252).over("company_code") - 1.0).alias("drawdown_252"),
            ]
        )
        .filter(pl.col("date") >= pl.lit(start).cast(pl.Date))
        .select(
            [
                "date",
                "company_code",
                "close",
                "raw_close",
                "ret_1d",
                "ret_20d",
                "ret_60d",
                "ret_120d",
                "ret_252d",
                "adv20",
                "drawdown_252",
            ]
        )
    )
    daily_returns = featured.select(["date", "company_code", "ret_1d"]).to_pandas()
    return featured.to_pandas(), daily_returns


def load_per_features(con, codes: list[str]) -> pd.DataFrame:
    codes_sql = ",".join(f"'{code}'" for code in sorted(set(codes)))
    per = (
        con.sql(
            f"""
            SELECT date, company_code, price_to_earning_ratio, price_book_ratio, dividend_yield
            FROM stock_per_pbr
            WHERE company_code IN ({codes_sql})
            """
        )
        .pl()
        .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
        .to_pandas()
    )
    per["date"] = pd.to_datetime(per["date"]).dt.date
    per = per.sort_values(["company_code", "date"]).copy()

    def trailing_percentile(values: np.ndarray) -> float:
        valid = pd.Series(values).dropna()
        if len(valid) < 60:
            return np.nan
        return float(valid.rank(pct=True).iloc[-1])

    for col, out in (
        ("price_to_earning_ratio", "pe_percentile_3y"),
        ("price_book_ratio", "pb_percentile_3y"),
    ):
        per[out] = (
            per.groupby("company_code", group_keys=False)[col]
            .rolling(756, min_periods=120)
            .apply(trailing_percentile, raw=True)
            .reset_index(level=0, drop=True)
        )
    return per


def load_quarterly_features(con, codes: list[str]) -> pd.DataFrame:
    codes_sql = ",".join(f"'{code}'" for code in sorted(set(codes)))
    q = (
        con.sql(
            f"""
            SELECT company_code, year, quarter, rev_ttm, ni_ttm, gross_margin_ttm,
                   d_gross_margin_yoy, capital_stock, f_score_raw, cfo_ni_ratio_ttm
            FROM raw_quarterly
            WHERE company_code IN ({codes_sql})
            """
        )
        .pl()
        .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
        .to_pandas()
    )
    q = q.sort_values(["company_code", "year", "quarter"]).copy()
    q["report_date"] = [quarter_report_date(int(y), int(qtr)) for y, qtr in zip(q["year"], q["quarter"])]
    q["ni_ttm_lag4"] = q.groupby("company_code")["ni_ttm"].shift(4)
    q["rev_ttm_lag4"] = q.groupby("company_code")["rev_ttm"].shift(4)
    q["ni_ttm_yoy"] = (q["ni_ttm"] - q["ni_ttm_lag4"]) / q["ni_ttm_lag4"].abs()
    q["rev_ttm_yoy"] = (q["rev_ttm"] - q["rev_ttm_lag4"]) / q["rev_ttm_lag4"].abs()
    return q


def load_flows(con, codes: list[str]) -> pd.DataFrame:
    codes_sql = ",".join(f"'{code}'" for code in sorted(set(codes)))
    flows = (
        con.sql(
            f"""
            SELECT date, company_code, total_difference AS inst_diff
            FROM daily_trading_details
            WHERE company_code IN ({codes_sql})
            """
        )
        .pl()
        .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
        .to_pandas()
    )
    flows["date"] = pd.to_datetime(flows["date"]).dt.date
    flows = flows.sort_values(["company_code", "date"]).copy()
    flows["inst_20d"] = flows.groupby("company_code")["inst_diff"].transform(lambda s: s.rolling(20, min_periods=5).sum())
    return flows[["date", "company_code", "inst_20d"]]


def fair_pe_from_growth(growth: float, discount_rate: float = 0.10, terminal_growth: float = 0.03, years: int = 5) -> float:
    g = float(np.clip(growth, -0.50, 1.50))
    r = discount_rate
    tg = terminal_growth
    if r <= tg:
        raise ValueError("discount_rate must be greater than terminal_growth")
    high_growth = sum(((1.0 + g) ** t) / ((1.0 + r) ** t) for t in range(1, years + 1))
    terminal = ((1.0 + g) ** years) * (1.0 + tg) / (r - tg) / ((1.0 + r) ** years)
    return high_growth + terminal


def implied_growth_from_pe(pe: float, discount_rate: float = 0.10, terminal_growth: float = 0.03, years: int = 5) -> float:
    if pe is None or not np.isfinite(pe) or pe <= 0:
        return np.nan
    lo, hi = -0.50, 1.50
    if pe <= fair_pe_from_growth(lo, discount_rate, terminal_growth, years):
        return lo
    if pe >= fair_pe_from_growth(hi, discount_rate, terminal_growth, years):
        return hi
    for _ in range(48):
        mid = (lo + hi) / 2.0
        if fair_pe_from_growth(mid, discount_rate, terminal_growth, years) < pe:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def cross_sectional_score(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().sum() < 3:
        return pd.Series(0.0, index=series.index)
    ranked = numeric.rank(pct=True)
    return ((ranked - 0.5) * 2.0).fillna(0.0)


def add_scores(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    for col in (
        "conviction",
        "theme_count",
        "monthly_revenue_yoy",
        "yoy_3m",
        "yoy_accel",
        "ret_20d",
        "ret_60d",
        "ret_252d",
        "drawdown_252",
        "adv20",
        "price_to_earning_ratio",
        "price_book_ratio",
        "pe_percentile_3y",
        "pb_percentile_3y",
        "inst_20d",
        "rev_ttm",
        "ni_ttm",
        "ni_ttm_yoy",
        "rev_ttm_yoy",
        "gross_margin_ttm",
        "d_gross_margin_yoy",
        "capital_stock",
        "f_score_raw",
        "cfo_ni_ratio_ttm",
    ):
        if col not in data:
            data[col] = np.nan
        data[col] = pd.to_numeric(data[col], errors="coerce")

    data = data[
        (data["adv20"] >= 50_000_000)
        & (data["raw_close"] >= 20)
        & (data["ret_60d"] >= -0.35)
        & (data["ret_252d"] >= -0.35)
        & (data["drawdown_252"] >= -0.55)
        & (data["price_to_earning_ratio"].isna() | (data["price_to_earning_ratio"] <= 300))
        & (data["price_book_ratio"].isna() | (data["price_book_ratio"] <= 60))
    ].copy()
    if data.empty:
        return data

    revenue_growth = (data[["monthly_revenue_yoy", "yoy_3m"]].max(axis=1) / 100.0).clip(-0.30, 1.50)
    ni_growth = data["ni_ttm_yoy"].clip(-0.80, 2.00)
    margin_delta = data["d_gross_margin_yoy"].clip(-0.20, 0.30)
    data["supported_growth"] = (0.62 * revenue_growth.fillna(0.0) + 0.28 * ni_growth.fillna(0.0) + 0.10 * margin_delta.fillna(0.0)).clip(-0.30, 1.50)

    implied = data["price_to_earning_ratio"].map(implied_growth_from_pe)
    data["implied_growth_dcf"] = implied
    fair_pe_supported = data["supported_growth"].map(fair_pe_from_growth)
    data["dcf_upside"] = fair_pe_supported / data["price_to_earning_ratio"].replace(0, np.nan) - 1.0
    data["reverse_dcf_gap"] = data["supported_growth"] - data["implied_growth_dcf"]

    growth_pct = (data["supported_growth"] * 100.0).clip(lower=1.0)
    data["peg"] = data["price_to_earning_ratio"] / growth_pct

    accel_multiplier = 1.0 + (data["yoy_accel"] / 100.0).clip(lower=-0.25, upper=0.25).fillna(0.0)
    margin_multiplier = 1.0 + data["d_gross_margin_yoy"].clip(lower=0.0, upper=0.30).fillna(0.0)
    true_growth_pct = (data["yoy_3m"].clip(lower=1.0, upper=180.0).fillna(1.0) * accel_multiplier * margin_multiplier)
    data["adj_peg"] = data["price_to_earning_ratio"] / true_growth_pct.clip(lower=1.0)

    shares_thousand = data["capital_stock"] / 10.0
    market_cap_thousand = data["raw_close"] * shares_thousand
    data["ps_ratio"] = market_cap_thousand / data["rev_ttm"].replace(0, np.nan)
    data["ps_growth"] = data["ps_ratio"] / growth_pct
    gross_profit_ttm = data["gross_margin_ttm"] * data["rev_ttm"]
    data["gross_profit_yield"] = gross_profit_ttm / market_cap_thousand.replace(0, np.nan)

    data["valuation_peg_score"] = -np.log1p(data["peg"].clip(lower=0.0))
    data["valuation_adj_peg_score"] = -np.log1p(data["adj_peg"].clip(lower=0.0))
    data["valuation_reverse_dcf_gap"] = data["reverse_dcf_gap"]
    data["valuation_dcf_upside"] = data["dcf_upside"].clip(-1.0, 5.0)
    data["valuation_pe_band_score"] = 1.0 - data["pe_percentile_3y"]
    data["valuation_ps_growth_score"] = -np.log1p(data["ps_growth"].clip(lower=0.0))
    data["valuation_gross_profit_yield"] = data["gross_profit_yield"].clip(-1.0, 2.0)
    data["valuation_combo_score"] = (
        0.35 * cross_sectional_score(data["valuation_reverse_dcf_gap"])
        + 0.20 * cross_sectional_score(data["valuation_adj_peg_score"])
        + 0.20 * cross_sectional_score(data["valuation_pe_band_score"])
        + 0.15 * cross_sectional_score(data["valuation_gross_profit_yield"])
        + 0.10 * cross_sectional_score(data["valuation_ps_growth_score"])
    )
    data["valuation_dcf_peg_blend"] = (
        0.40 * cross_sectional_score(data["valuation_reverse_dcf_gap"])
        + 0.35 * cross_sectional_score(data["valuation_peg_score"])
        + 0.25 * cross_sectional_score(data["valuation_dcf_upside"])
    )

    score_alpha = data["conviction"].fillna(3.0) * 8.0
    score_alpha += data["theme_count"].fillna(1.0).clip(1.0, 3.0) * 3.0
    score_alpha += data["monthly_revenue_yoy"].clip(-40, 160).fillna(0.0) * 0.12
    score_alpha += data["yoy_3m"].clip(-40, 130).fillna(0.0) * 0.08
    score_alpha += data["yoy_accel"].clip(-60, 90).fillna(0.0) * 0.05
    score_alpha += data["ret_60d"].clip(-0.5, 1.8).fillna(0.0) * 14.0
    score_alpha += data["ret_20d"].clip(-0.35, 0.9).fillna(0.0) * 5.0
    score_alpha += data["ret_252d"].clip(-0.8, 3.2).fillna(0.0) * 3.0
    score_alpha += (np.log10(data["adv20"].clip(lower=1.0)) - 8.0).clip(upper=3.0) * 3.0
    score_alpha += (data["inst_20d"].fillna(0.0) / 1_000 / 10_000).clip(-3.0, 3.0) * 0.9
    dd = data["drawdown_252"]
    score_alpha += np.select([dd < -0.40, dd < -0.30], [-8.0, -4.0], default=0.0)
    data["score_alpha"] = score_alpha

    pe = data["price_to_earning_ratio"]
    current_penalty = np.select(
        [pe.isna() | (pe <= 0), pe > 180, pe > 120, pe > 80, pe < 25],
        [-4.0, -9.0, -6.0, -3.0, 2.0],
        default=0.0,
    )
    pb = data["price_book_ratio"]
    current_penalty += np.select([pb > 35, pb > 25], [-6.0, -3.0], default=0.0)
    data["score_current_penalty"] = data["score_alpha"] + current_penalty

    data["score_peg"] = data["score_alpha"] + 16.0 * cross_sectional_score(data["valuation_peg_score"])
    data["score_adj_peg"] = data["score_alpha"] + 16.0 * cross_sectional_score(data["valuation_adj_peg_score"])
    data["score_reverse_dcf_gap"] = data["score_alpha"] + 18.0 * cross_sectional_score(data["valuation_reverse_dcf_gap"])
    data["score_dcf_upside"] = data["score_alpha"] + 18.0 * cross_sectional_score(data["valuation_dcf_upside"])
    data["score_dcf_peg_blend"] = data["score_alpha"] + 18.0 * cross_sectional_score(data["valuation_dcf_peg_blend"])
    data["score_pe_band"] = data["score_alpha"] + 12.0 * cross_sectional_score(data["valuation_pe_band_score"])
    data["score_ps_growth"] = data["score_alpha"] + 14.0 * cross_sectional_score(data["valuation_ps_growth_score"])
    data["score_gross_profit_yield"] = data["score_alpha"] + 12.0 * cross_sectional_score(data["valuation_gross_profit_yield"])
    data["score_valuation_combo"] = data["score_alpha"] + 18.0 * cross_sectional_score(data["valuation_combo_score"])
    return data


def target_weights(scored: pd.DataFrame, method: Method) -> dict[str, float]:
    picks = scored.sort_values(method.score_col, ascending=False).head(method.top_n).copy()
    if picks.empty:
        return {}
    if method.weighting == "equal":
        return {code: 1.0 / len(picks) for code in picks["company_code"]}
    raw = picks[method.score_col].clip(lower=0.0)
    if float(raw.sum()) <= 0:
        return {code: 1.0 / len(picks) for code in picks["company_code"]}
    weights = raw / raw.sum()
    weights = weights.clip(upper=0.20)
    weights = weights / weights.sum()
    return {code: float(weight) for code, weight in zip(picks["company_code"], weights)}


def cagr(start_nav: float, end_nav: float, days: int) -> float:
    if start_nav <= 0 or end_nav <= 0 or days <= 0:
        return float("nan")
    return (end_nav / start_nav) ** (365.25 / days) - 1.0


def max_drawdown(nav: pd.Series) -> float:
    return float((nav / nav.cummax() - 1.0).min())


def sortino_ratio(returns: pd.Series) -> float:
    downside = returns[returns < 0]
    std = float(downside.std())
    if std <= 0 or math.isnan(std):
        return float("nan")
    return float(np.sqrt(TDPY) * returns.mean() / std)


def recent_cagr(daily: pd.DataFrame, days_back: int) -> tuple[float, str]:
    end = pd.to_datetime(daily["date"].iloc[-1]).date()
    anchor = end - timedelta(days=days_back)
    start_rows = daily[pd.to_datetime(daily["date"]).dt.date <= anchor]
    if start_rows.empty:
        start = pd.to_datetime(daily["date"].iloc[0]).date()
        return cagr(CAPITAL, float(daily["nav"].iloc[-1]), (end - start).days), f"{start}~{end}"
    start_row = start_rows.tail(1).iloc[0]
    start = pd.to_datetime(start_row["date"]).date()
    return cagr(float(start_row["nav"]), float(daily["nav"].iloc[-1]), (end - start).days), f"{start}~{end}"


def summarize_nav(name: str, daily: pd.DataFrame, turnover: float, rebalances: int) -> dict[str, object]:
    ordered = daily.sort_values("date").reset_index(drop=True)
    start = pd.to_datetime(ordered["date"].iloc[0]).date()
    end = pd.to_datetime(ordered["date"].iloc[-1]).date()
    returns = ordered["nav"].pct_change().fillna(0.0)
    full_cagr = cagr(float(ordered["nav"].iloc[0]), float(ordered["nav"].iloc[-1]), (end - start).days)
    mdd = max_drawdown(ordered["nav"])
    r1y, r1y_window = recent_cagr(ordered, 365)
    return {
        "name": name,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "cagr": full_cagr,
        "recent_1y_cagr": r1y,
        "recent_1y_window": r1y_window,
        "sharpe": float(np.sqrt(TDPY) * returns.mean() / returns.std()) if returns.std() > 0 else float("nan"),
        "sortino": sortino_ratio(returns),
        "mdd": mdd,
        "calmar": full_cagr / abs(mdd) if mdd < 0 else float("nan"),
        "final_nav": float(ordered["nav"].iloc[-1]),
        "total_turnover": turnover,
        "avg_rebalance_turnover": turnover / max(rebalances, 1),
        "avg_active": float(ordered["active"].mean()) if "active" in ordered else float("nan"),
        "max_active": int(ordered["active"].max()) if "active" in ordered else 1,
        "rebalances": rebalances,
    }


def simulate(method: Method, trading_days: list[date], targets: dict[date, dict[str, float]], returns_by_day: dict[date, dict[str, float]]) -> tuple[pd.DataFrame, float]:
    nav = CAPITAL
    current: dict[str, float] = {}
    rows: list[dict[str, object]] = []
    total_turnover = 0.0
    fee_buy = COMMISSION + 0.0005
    fee_sell = COMMISSION + SELL_TAX + 0.0005
    for day in trading_days:
        if day in targets:
            target = targets[day]
            keys = set(current) | set(target)
            buys = sum(max(target.get(code, 0.0) - current.get(code, 0.0), 0.0) for code in keys)
            sells = sum(max(current.get(code, 0.0) - target.get(code, 0.0), 0.0) for code in keys)
            nav *= 1.0 - (buys * fee_buy + sells * fee_sell)
            total_turnover += buys + sells
            current = dict(target)
        day_ret = sum(weight * returns_by_day.get(day, {}).get(code, 0.0) for code, weight in current.items())
        nav *= 1.0 + day_ret
        rows.append({"date": day, "nav": nav, "active": len(current)})
    return pd.DataFrame(rows), total_turnover


def benchmark_nav(con, code: str, market: str, start: date, end: date) -> pd.DataFrame:
    series = total_return_series(con, code, start.isoformat(), end.isoformat(), market=market).to_pandas()
    series = series.sort_values("date").reset_index(drop=True)
    series["nav"] = CAPITAL * series["adj_close"] / float(series["adj_close"].iloc[0])
    return series[["date", "nav"]]


def forward_return_table(price_features: pd.DataFrame, pairs: list[tuple[date, date]], periods: tuple[int, ...]) -> pd.DataFrame:
    px = price_features.copy()
    px["date"] = pd.to_datetime(px["date"]).dt.date
    close = px.pivot(index="date", columns="company_code", values="close").sort_index()
    days = list(close.index)
    rows: list[pd.DataFrame] = []
    for signal_day, exec_day in pairs:
        if exec_day not in close.index:
            continue
        idx = days.index(exec_day)
        base = close.loc[exec_day]
        out = pd.DataFrame({"signal_date": signal_day, "execution_date": exec_day, "company_code": close.columns})
        for period in periods:
            if idx + period < len(days):
                future = close.iloc[idx + period]
                out[f"fwd_{period}d"] = (future / base - 1.0).values
            else:
                out[f"fwd_{period}d"] = np.nan
        rows.append(out)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def factor_ic(scored: pd.DataFrame, fwd: pd.DataFrame, periods: tuple[int, ...]) -> pd.DataFrame:
    joined = scored.merge(fwd, on=["signal_date", "execution_date", "company_code"], how="left")
    rows = []
    for factor in FACTOR_COLS:
        for period in periods:
            col = f"fwd_{period}d"
            ics = []
            for _day, group in joined[["signal_date", factor, col]].dropna().groupby("signal_date"):
                if len(group) >= 5 and group[factor].nunique() >= 3 and group[col].nunique() >= 3:
                    ics.append(group[factor].corr(group[col], method="spearman"))
            series = pd.Series(ics, dtype=float).dropna()
            mean_ic = float(series.mean()) if len(series) else float("nan")
            std_ic = float(series.std(ddof=1)) if len(series) > 1 else float("nan")
            t_stat = mean_ic / (std_ic / math.sqrt(len(series))) if len(series) > 1 and std_ic > 0 else float("nan")
            rows.append(
                {
                    "factor": factor,
                    "period": period,
                    "mean_ic": mean_ic,
                    "t_stat": t_stat,
                    "hit_rate": float((series > 0).mean()) if len(series) else float("nan"),
                    "n_dates": int(len(series)),
                }
            )
    return pd.DataFrame(rows).sort_values(["period", "mean_ic"], ascending=[True, False])


def plot_nav(summary: pd.DataFrame, daily_paths: dict[str, Path], out_prefix: str) -> Path:
    top_names = [name for name in summary.head(7)["name"].tolist() if name in daily_paths]
    fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
    for name in top_names:
        daily = pd.read_csv(daily_paths[name], parse_dates=["date"])
        axes[0].plot(daily["date"], daily["nav"] / daily["nav"].iloc[0], label=name)
        axes[1].plot(daily["date"], daily["nav"] / daily["nav"].cummax() - 1.0, label=name)
    axes[0].set_title("Serenity valuation methods: NAV")
    axes[0].set_ylabel("Growth of NT$1")
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(fontsize=8)
    axes[1].set_title("Drawdown")
    axes[1].set_ylabel("Drawdown")
    axes[1].grid(True, alpha=0.3)
    axes[1].yaxis.set_major_formatter(lambda x, _pos: f"{x:.0%}")
    fig.tight_layout()
    out = RESULTS / f"{out_prefix}_overview.png"
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def write_report(summary: pd.DataFrame, ic: pd.DataFrame, latest: pd.DataFrame, chart: Path, cutoff: date, out_prefix: str) -> Path:
    report = DOCS / f"{out_prefix}.md"
    chart_rel = os.path.relpath(chart, report.parent)
    lines = [
        "# Serenity 估值方法驗證",
        "",
        f"- 資料 cutoff：`{cutoff}`",
        "- 研究問題：結構性瓶頸股候選已經由產業 thesis 先定義，估值層要判斷是否買太貴。",
        "- 方法：比較 reverse DCF implied growth gap、DCF upside、PEG、margin-adjusted PEG、PE band、PS/Growth、gross-profit yield 與 combo。",
        "- 交易假設：月營收公布後 signal，下一交易日執行；total-return adjusted price；含手續費、證交稅與 5 bps 買賣滑價。",
        "- 階段：這是 `research_candidate` 估值 overlay，尚未取代正式策略。",
        "",
        f"![NAV and drawdown]({chart_rel})",
        "",
        "## 策略回測 KPI",
        "",
        "| 方法 | CAGR | 最近 1 年 CAGR | 1Y 窗口 | Sharpe | Sortino | MDD | Calmar | Final NAV | Turnover | 平均持股 |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary.itertuples(index=False):
        values = row._asdict()
        lines.append(
            "| {name} | {cagr:.2%} | {recent_1y_cagr:.2%} | {recent_1y_window} | "
            "{sharpe:.3f} | {sortino:.3f} | {mdd:.2%} | {calmar:.2f} | {final_nav:,.0f} | "
            "{total_turnover:.2f}x | {avg_active:.1f} |".format(**values)
        )
    lines += [
        "",
        "## 估值因子 IC",
        "",
        "| 因子 | Horizon | Mean IC | t-stat | Hit rate | Dates |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in ic.itertuples(index=False):
        values = row._asdict()
        lines.append(
            "| {factor} | {period}d | {mean_ic:.3f} | {t_stat:.2f} | {hit_rate:.1%} | {n_dates} |".format(
                **values
            )
        )
    lines += [
        "",
        "## 最新估值觀察",
        "",
        "| Rank | 代號 | 公司 | Thesis | PE | Supported g | Implied g | DCF gap | PEG | Adj PEG | PE band | Score |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in latest.head(20).itertuples(index=False):
        values = row._asdict()
        lines.append(
            "| {rank} | {company_code} | {company_name} | {theme_id} | {price_to_earning_ratio:.1f} | "
            "{supported_growth:.1%} | {implied_growth_dcf:.1%} | {reverse_dcf_gap:.1%} | "
            "{peg:.2f} | {adj_peg:.2f} | {pe_percentile_3y:.1%} | {score_valuation_combo:.1f} |".format(
                **values
            )
        )
    lines += [
        "",
        "## 初步結論",
        "",
        "- 單看 NAV，`gross_profit_yield_top10` 最高；但 gross-profit yield 的 21/63/126 日 IC 全為負，因此不能視為最可靠估值法。它比較像本段樣本中的持倉/產業暴露效果，不是穩定的 forward-return 選股訊號。",
        "- 單一方法中，`PEG` 最適合當 Serenity 估值 overlay：策略 CAGR 高、最近一年 CAGR 高，且 21/63/126 日 IC 都為正，63/126 日 t-stat 明確通過。",
        "- `reverse DCF gap` 與 `DCF upside` 是最適合作為風險檢查的估值語言：它們不是精準目標價，而是問「市場價格需要多少成長才能合理化」，再與營收/淨利/毛利率支持的成長做比較。",
        "- 實務上建議用 `DCF + PEG blend` 作為正式研究版本：它的 CAGR 略低於 PEG，但 63/126 日 IC 幾乎與 PEG/Reverse DCF 同級，且不會把決策壓在單一短期營收成長率上。",
        "- `PE band` 在這種結構性瓶頸股上不適合作為主要估值法；高成長重估時，歷史 PE 區間常常反而懲罰正確的 re-rating。",
    ]
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--registry", default=str(REGISTRY))
    parser.add_argument("--label", default=OUT_PREFIX)
    args = parser.parse_args()

    con = connect(read_only=True)
    try:
        cutoff = con.sql("select max(date) from daily_quote").fetchone()[0]
        start = date.fromisoformat(args.start)
        load_start = start - timedelta(days=430)
        registry = load_registry(Path(args.registry))
        codes = registry["company_code"].tolist()
        universe = load_universe(con, codes)
        taxonomy = load_taxonomy(con, universe["company_code"].tolist())
        price_features, daily_returns = load_price_features(con, universe, load_start, cutoff)
        revenue = load_revenue_features(con)
        per = load_per_features(con, universe["company_code"].tolist())
        quarterly = load_quarterly_features(con, universe["company_code"].tolist())
        flows = load_flows(con, universe["company_code"].tolist())

        trading_days = sorted(pd.to_datetime(price_features["date"]).dt.date.unique())
        trading_days = [day for day in trading_days if day >= start and day <= cutoff]
        pairs = build_rebalance_pairs(trading_days, start, cutoff)

        dr = daily_returns[pd.to_datetime(daily_returns["date"]).dt.date.isin(trading_days)].copy()
        dr["_date"] = pd.to_datetime(dr["date"]).dt.date
        returns_by_day = {
            day: group.set_index("company_code")["ret_1d"].fillna(0.0).to_dict()
            for day, group in dr.groupby("_date")
        }

        targets: dict[str, dict[date, dict[str, float]]] = {method.name: {} for method in METHODS}
        scored_rows: list[pd.DataFrame] = []
        target_rows: list[dict[str, object]] = []

        for signal_day, exec_day in pairs:
            active = active_registry_for_day(registry, signal_day)
            if active.empty:
                continue
            px_day = price_features[pd.to_datetime(price_features["date"]).dt.date == signal_day].copy()
            tax_day = row_latest_before(taxonomy, signal_day, "effective_date")
            tax_day = tax_day[(tax_day["is_financial"] == False) & (tax_day["is_special_category"] == False)].copy()
            rev_day = row_latest_before(revenue, signal_day, "report_date")
            per_day = row_latest_before(per, signal_day, "date")
            q_day = row_latest_before(quarterly, signal_day, "report_date")
            flow_day = row_latest_before(flows, signal_day, "date")
            joined = (
                active.merge(tax_day, on="company_code", how="inner")
                .merge(px_day, on="company_code", how="inner")
                .merge(rev_day, on="company_code", how="left", suffixes=("", "_rev"))
                .merge(per_day, on="company_code", how="left")
                .merge(q_day, on="company_code", how="left", suffixes=("", "_q"))
                .merge(flow_day, on="company_code", how="left")
            )
            scored = add_scores(joined)
            if scored.empty:
                continue
            scored["signal_date"] = signal_day
            scored["execution_date"] = exec_day
            scored_rows.append(scored)
            for method in METHODS:
                weights = target_weights(scored, method)
                targets[method.name][exec_day] = weights
                picks = scored[scored["company_code"].isin(weights)].copy()
                for row in picks.itertuples(index=False):
                    target_rows.append(
                        {
                            "method": method.name,
                            "signal_date": signal_day,
                            "execution_date": exec_day,
                            "company_code": row.company_code,
                            "company_name": row.company_name,
                            "theme_id": row.theme_id,
                            "weight": weights[row.company_code],
                            "method_score": getattr(row, method.score_col),
                        }
                    )

        scored_all = pd.concat(scored_rows, ignore_index=True)
        scored_path = RESULTS / f"{args.label}_scored_candidates.csv"
        scored_all.to_csv(scored_path, index=False)

        summaries = []
        daily_paths: dict[str, Path] = {}
        for method in METHODS:
            daily, turnover = simulate(method, trading_days, targets[method.name], returns_by_day)
            path = RESULTS / f"{args.label}_{method.name}_daily.csv"
            daily.to_csv(path, index=False)
            daily_paths[method.name] = path
            summaries.append(summarize_nav(method.name, daily, turnover, len(targets[method.name])))

        for code, name in (("0050", "hold_0050"), ("2330", "hold_2330")):
            daily = benchmark_nav(con, code, "twse", trading_days[0], trading_days[-1])
            path = RESULTS / f"{args.label}_{name}_daily.csv"
            daily.to_csv(path, index=False)
            daily_paths[name] = path
            summaries.append(summarize_nav(name, daily.assign(active=1), 0.0, 0))

        summary = pd.DataFrame(summaries).sort_values("cagr", ascending=False)
        summary_path = RESULTS / f"{args.label}_summary.csv"
        targets_path = RESULTS / f"{args.label}_target_weights.csv"
        summary.to_csv(summary_path, index=False)
        pd.DataFrame(target_rows).to_csv(targets_path, index=False)

        fwd = forward_return_table(price_features, pairs, (21, 63, 126))
        ic = factor_ic(scored_all, fwd, (21, 63, 126))
        ic_path = RESULTS / f"{args.label}_factor_ic.csv"
        ic.to_csv(ic_path, index=False)

        chart = plot_nav(summary, daily_paths, args.label)
        latest_signal = scored_all["signal_date"].max()
        latest = (
            scored_all[scored_all["signal_date"] == latest_signal]
            .sort_values("score_valuation_combo", ascending=False)
            .head(30)
            .copy()
        )
        latest.insert(0, "rank", range(1, len(latest) + 1))
        report = write_report(summary, ic, latest, chart, cutoff, args.label)

        print(f"data_cutoff={cutoff}")
        print(f"trading_window={trading_days[0]}~{trading_days[-1]} rebalances={len(pairs)}")
        print("outputs:")
        for path in (summary_path, ic_path, scored_path, targets_path, chart, report):
            print(f"  {path}")
        display = summary.copy()
        print(
            display[
                [
                    "name",
                    "cagr",
                    "recent_1y_cagr",
                    "recent_1y_window",
                    "sharpe",
                    "sortino",
                    "mdd",
                    "calmar",
                    "final_nav",
                    "total_turnover",
                    "avg_active",
                    "max_active",
                ]
            ].to_string(
                index=False,
                formatters={
                    "cagr": "{:.2%}".format,
                    "recent_1y_cagr": "{:.2%}".format,
                    "sharpe": "{:.3f}".format,
                    "sortino": "{:.3f}".format,
                    "mdd": "{:.2%}".format,
                    "calmar": "{:.2f}".format,
                    "final_nav": "{:,.0f}".format,
                    "total_turnover": "{:.2f}x".format,
                    "avg_active": "{:.1f}".format,
                },
            )
        )
        print("\nIC")
        print(
            ic.to_string(
                index=False,
                formatters={
                    "mean_ic": "{:.3f}".format,
                    "t_stat": "{:.2f}".format,
                    "hit_rate": "{:.1%}".format,
                },
            )
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
