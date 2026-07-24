"""Iter101 - revenue-nowcast DCF value spread backtest.

This is a first pass at turning the TSMC reverse-DCF idea into a cross-sectional
Taiwan-stock signal:

* use only point-in-time observable data already in the research cache;
* infer TTM EPS from exchange PE and raw close;
* nowcast forward EPS growth from monthly revenue YoY / acceleration;
* convert forward EPS into a simple five-year owner-earnings DCF value;
* buy the largest positive value spreads at monthly rebalance points.

The goal is not to fit a final production strategy.  It is to answer whether
"DCF-implied undervaluation + monthly revenue nowcast" has enough alpha to
justify a stricter second pass.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

REPO_ROOT = paths.REPO
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from quantlib.constants import CAPITAL  # noqa: E402
from quantlib.db import connect  # noqa: E402
from evaluation import nav_metrics  # noqa: E402
from iter_32_first_principles import COMMISSION, SELL_TAX  # noqa: E402
from iter_33_pm_first_principles import load_or_build_panel  # noqa: E402
from quantlib.prices import total_return_series  # noqa: E402
from validator import recent_one_year_metrics  # noqa: E402


START = date(2012, 1, 3)
RESULTS = REPO_ROOT / f"{paths.OUT_STRAT_LAB}"
OUT_PREFIX = "iter_101_dcf_revenue_value"
TDPY = 252


@dataclass(frozen=True)
class DcfConfig:
    name: str
    topn: int = 10
    discount_rate: float = 0.09
    terminal_growth: float = 0.03
    owner_earnings_conversion: float = 0.75
    max_growth_5y: float = 0.25
    min_growth_1y: float = 0.00
    min_margin: float = 0.20
    min_adv60: float = 50_000_000.0
    min_roa: float = 0.03
    min_gm: float = 0.08
    min_fscore: int = 3
    require_trend: bool = False
    require_positive_yoy_delta: bool = False
    max_pe: float = 45.0


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


def first_trading_day_by_month(days: list[date], start: date) -> list[date]:
    out: list[date] = []
    seen: set[tuple[int, int]] = set()
    for day in days:
        if day < start:
            continue
        key = (day.year, day.month)
        if key not in seen:
            out.append(day)
            seen.add(key)
    return out


def previous_day_lookup(days: list[date]) -> dict[date, date]:
    return {days[i]: days[i - 1] for i in range(1, len(days))}


def dcf_value_per_share(
    eps_ttm: pl.Expr,
    growth_1y: pl.Expr,
    growth_5y: pl.Expr,
    *,
    discount_rate: float,
    terminal_growth: float,
    owner_earnings_conversion: float,
) -> pl.Expr:
    """Vectorized five-year owner-earnings DCF."""
    eps = eps_ttm * (1.0 + growth_1y) * owner_earnings_conversion
    pv = pl.lit(0.0)
    current = eps
    for year in range(1, 6):
        if year > 1:
            current = current * (1.0 + growth_5y)
        pv = pv + current / ((1.0 + discount_rate) ** year)
    terminal = current * (1.0 + terminal_growth) / (discount_rate - terminal_growth)
    return pv + terminal / ((1.0 + discount_rate) ** 5)


def load_valuation_panel(start: date, end: date) -> tuple[pl.DataFrame, list[date]]:
    con = connect(read_only=True)
    try:
        panel, days = load_or_build_panel(con, start, end, use_cache=True)
        value_data = con.sql(
            f"""
            SELECT q.date,
                   q.company_code,
                   q.closing_price AS raw_close,
                   p.price_to_earning_ratio AS pe,
                   p.price_book_ratio AS pb,
                   p.dividend_yield
            FROM daily_quote q
            JOIN stock_per_pbr p
              ON p.market = q.market
             AND p.date = q.date
             AND p.company_code = q.company_code
            WHERE q.date BETWEEN DATE '{start}' AND DATE '{end}'
              AND regexp_matches(q.company_code, '^[1-9][0-9]{{3}}$')
              AND q.closing_price > 0
              AND p.price_to_earning_ratio > 0
            """
        ).pl()
    finally:
        con.close()

    value_data = value_data.unique(["date", "company_code"], keep="last")
    out = (
        panel.join(value_data, on=["date", "company_code"], how="left")
        .with_columns(
            [
                (pl.col("raw_close") / pl.col("pe")).alias("eps_ttm"),
                (pl.col("latest_yoy") / 100.0).alias("rev_yoy"),
                (pl.col("yoy_delta").fill_null(0.0) / 100.0).alias("rev_accel"),
            ]
        )
        .with_columns(
            [
                (0.85 * pl.col("rev_yoy") + 0.15 * pl.col("rev_accel")).clip(-0.30, 0.80).alias("growth_1y"),
                (0.45 * pl.col("rev_yoy") + 0.10 * pl.col("rev_accel")).clip(-0.05, 0.35).alias("growth_5y_raw"),
                (pl.col("close") / pl.col("close").shift(1).over("company_code") - 1.0).fill_null(0.0).alias("ret"),
            ]
        )
    )
    return out, days


def target_book(panel: pl.DataFrame, days: list[date], cfg: DcfConfig) -> dict[date, list[str]]:
    prev = previous_day_lookup(days)
    rebalances = first_trading_day_by_month(days, START)
    signal_dates = [prev[d] for d in rebalances if d in prev]
    if not signal_dates:
        return {}

    growth_5y = pl.min_horizontal(pl.col("growth_5y_raw"), pl.lit(cfg.max_growth_5y))
    valued = (
        panel.filter(pl.col("date").is_in(signal_dates))
        .with_columns(
            [
                growth_5y.alias("growth_5y"),
                dcf_value_per_share(
                    pl.col("eps_ttm"),
                    pl.col("growth_1y"),
                    growth_5y,
                    discount_rate=cfg.discount_rate,
                    terminal_growth=cfg.terminal_growth,
                    owner_earnings_conversion=cfg.owner_earnings_conversion,
                ).alias("fair_value"),
            ]
        )
        .with_columns(
            [
                (pl.col("fair_value") / pl.col("raw_close") - 1.0).alias("value_spread"),
                (
                    pl.col("fair_value") / pl.col("raw_close") - 1.0
                    + 0.20 * pl.col("growth_1y")
                    + 0.08 * (pl.col("roa_ttm").fill_null(0.0) / 0.10).clip(-1.0, 2.0)
                    + 0.04 * ((pl.col("f_score_raw").fill_null(0.0) - 4.0) / 3.0).clip(-1.0, 1.0)
                ).alias("dcf_score"),
            ]
        )
    )

    filt = (
        (~pl.col("is_etf"))
        & (~pl.col("is_finance"))
        & (pl.col("listed_days") >= 252)
        & (pl.col("adv60") >= cfg.min_adv60)
        & (pl.col("raw_close") >= 10.0)
        & (pl.col("eps_ttm") > 0)
        & (pl.col("pe").is_between(3.0, cfg.max_pe))
        & (pl.col("growth_1y") >= cfg.min_growth_1y)
        & (pl.col("value_spread") >= cfg.min_margin)
        & (pl.col("roa_ttm").fill_null(-999.0) >= cfg.min_roa)
        & (pl.col("gross_margin_ttm").fill_null(-999.0) >= cfg.min_gm)
        & (pl.col("f_score_raw").fill_null(0.0) >= cfg.min_fscore)
        & pl.col("fair_value").is_finite()
        & pl.col("dcf_score").is_finite()
    )
    if cfg.require_trend:
        filt = filt & (pl.col("close") > pl.col("ma200"))
    if cfg.require_positive_yoy_delta:
        filt = filt & (pl.col("rev_accel") > 0)

    picks = (
        valued.filter(filt)
        .sort(["date", "dcf_score", "value_spread"], descending=[False, True, True])
        .group_by("date", maintain_order=True)
        .head(cfg.topn)
        .select(
            [
                "date",
                "company_code",
                "raw_close",
                "pe",
                "eps_ttm",
                "growth_1y",
                "growth_5y",
                "fair_value",
                "value_spread",
                "dcf_score",
                "adv60",
                "latest_yoy",
                "yoy_delta",
            ]
        )
    )
    pick_by_signal = {
        key[0] if isinstance(key, tuple) else key: g["company_code"].to_list()
        for key, g in picks.group_by("date", maintain_order=True)
    }
    out = {rebalance: pick_by_signal.get(prev[rebalance], []) for rebalance in rebalances if rebalance in prev}
    return out


def simulate(
    panel: pl.DataFrame,
    days: list[date],
    targets: dict[date, list[str]],
    cfg: DcfConfig,
) -> tuple[dict[str, float | int | str], pl.DataFrame, pl.DataFrame]:
    ret_lookup = {
        (row["date"], row["company_code"]): float(row["ret"] or 0.0)
        for row in panel.select(["date", "company_code", "ret"]).iter_rows(named=True)
    }

    nav = CAPITAL
    weights: dict[str, float] = {}
    daily_rows: list[dict[str, object]] = []
    rebalance_rows: list[dict[str, object]] = []
    turnover_sum = 0.0
    active_counts: list[int] = []

    for day in days:
        day_ret = sum(weight * ret_lookup.get((day, code), 0.0) for code, weight in weights.items())
        nav *= 1.0 + day_ret

        cost = 0.0
        turnover = 0.0
        if day in targets:
            codes = targets[day]
            if codes:
                next_weight = {code: 1.0 / len(codes) for code in codes}
            else:
                next_weight = {}
            all_codes = set(weights) | set(next_weight)
            buy_turnover = sum(max(next_weight.get(code, 0.0) - weights.get(code, 0.0), 0.0) for code in all_codes)
            sell_turnover = sum(max(weights.get(code, 0.0) - next_weight.get(code, 0.0), 0.0) for code in all_codes)
            turnover = buy_turnover + sell_turnover
            cost_rate = buy_turnover * COMMISSION + sell_turnover * (COMMISSION + SELL_TAX)
            cost = nav * cost_rate
            nav -= cost
            weights = next_weight
            turnover_sum += turnover
            rebalance_rows.append(
                {
                    "date": day,
                    "codes": ",".join(codes),
                    "position_count": len(codes),
                    "buy_turnover": buy_turnover,
                    "sell_turnover": sell_turnover,
                    "turnover": turnover,
                    "cost": cost,
                    "nav_after_cost": nav,
                }
            )

        active_counts.append(len(weights))
        daily_rows.append(
            {
                "date": day,
                "nav": nav,
                "daily_return": day_ret,
                "position_count": len(weights),
                "rebalance_cost": cost,
                "turnover": turnover,
            }
        )

    daily = pl.DataFrame(daily_rows)
    rebalances = pl.DataFrame(rebalance_rows)
    metrics = nav_metrics(daily.select(["date", "nav"]))
    metrics.update(recent_one_year_metrics(daily.select(["date", "nav"]), capital=CAPITAL))
    metrics.update(
        {
            "name": cfg.name,
            "start": str(days[0]),
            "end": str(days[-1]),
            "rebalance_count": len(rebalance_rows),
            "avg_positions": float(np.mean(active_counts)) if active_counts else 0.0,
            "max_positions": int(max(active_counts)) if active_counts else 0,
            "avg_monthly_turnover": turnover_sum / max(len(rebalance_rows), 1),
            "final_nav": float(nav),
        }
    )
    return metrics, daily, rebalances


def benchmark_daily(con, code: str, start: date, end: date, label: str) -> tuple[dict[str, float | str], pl.DataFrame]:
    s = total_return_series(con, code, start.isoformat(), end.isoformat(), market="twse").sort("date")
    nav = CAPITAL * (s["adj_close"] / s["adj_close"][0])
    daily = s.select(["date"]).with_columns(nav.alias("nav"))
    row = nav_metrics(daily)
    row.update(recent_one_year_metrics(daily, capital=CAPITAL))
    row.update({"name": label, "start": str(daily["date"][0]), "end": str(daily["date"][-1]), "final_nav": float(daily["nav"][-1])})
    return row, daily


def default_configs() -> list[DcfConfig]:
    return [
        DcfConfig("dcf_top10_r09_m20_adv50", topn=10, discount_rate=0.09, min_margin=0.20, min_adv60=50_000_000),
        DcfConfig("dcf_top20_r09_m20_adv50", topn=20, discount_rate=0.09, min_margin=0.20, min_adv60=50_000_000),
        DcfConfig("dcf_top10_r085_m15_adv100", topn=10, discount_rate=0.085, min_margin=0.15, min_adv60=100_000_000),
        DcfConfig("dcf_trend_top10_r09_m15_adv50", topn=10, discount_rate=0.09, min_margin=0.15, min_adv60=50_000_000, require_trend=True),
        DcfConfig("dcf_accel_top10_r09_m10_adv50", topn=10, discount_rate=0.09, min_margin=0.10, min_adv60=50_000_000, require_positive_yoy_delta=True),
        DcfConfig("dcf_quality_top15_r09_m10_adv50", topn=15, discount_rate=0.09, min_margin=0.10, min_adv60=50_000_000, min_roa=0.08, min_gm=0.15, min_fscore=4),
        DcfConfig("dcf_strict_top10_r10_m25_adv100", topn=10, discount_rate=0.10, min_margin=0.25, min_adv60=100_000_000, min_growth_1y=0.05),
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=START.isoformat())
    ap.add_argument("--end", default=None)
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else latest_0050_day()
    RESULTS.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    log(f"[iter101] load valuation panel {start} -> {end}")
    panel, days = load_valuation_panel(start, end)
    days = [d for d in days if start <= d <= end]
    log(f"[iter101] panel rows={panel.height:,} codes={panel['company_code'].n_unique():,} days={len(days):,}")

    rows: list[dict[str, object]] = []
    best_daily: pl.DataFrame | None = None
    best_rebalances: pl.DataFrame | None = None
    best_name = ""
    best_cagr = -math.inf

    for cfg in default_configs():
        log(f"\n[iter101] run {cfg.name}")
        t = time.time()
        targets = target_book(panel, days, cfg)
        row, daily, rebalances = simulate(panel, days, targets, cfg)
        row.update(asdict(cfg))
        row["runtime_sec"] = time.time() - t
        daily_path = RESULTS / f"{OUT_PREFIX}_{cfg.name}_daily.csv"
        rebal_path = RESULTS / f"{OUT_PREFIX}_{cfg.name}_rebalances.csv"
        daily.write_csv(daily_path)
        rebalances.write_csv(rebal_path)
        row["daily_path"] = str(daily_path)
        row["rebalances_path"] = str(rebal_path)
        rows.append(row)
        if float(row["cagr"]) > best_cagr:
            best_daily = daily
            best_rebalances = rebalances
            best_name = cfg.name
            best_cagr = float(row["cagr"])
        log(
            f"  CAGR={float(row['cagr']):+.2%} recent1Y={float(row['recent_1y_cagr']):+.2%} "
            f"Sortino={float(row['sortino']):.3f} MDD={float(row['mdd']):+.2%} "
            f"avg_pos={float(row['avg_positions']):.1f} turnover={float(row['avg_monthly_turnover']):.2f} "
            f"({row['runtime_sec']:.1f}s)"
        )

    con = connect(read_only=True)
    try:
        for code, label in [("0050", "0050 TR"), ("2330", "2330 TR")]:
            row, daily = benchmark_daily(con, code, start, end, label)
            row.update(
                {
                    "daily_path": str(RESULTS / f"{OUT_PREFIX}_{code}_benchmark_daily.csv"),
                    "rebalances_path": "",
                    "runtime_sec": 0.0,
                }
            )
            daily.write_csv(row["daily_path"])
            rows.append(row)
    finally:
        con.close()

    summary = pl.DataFrame(rows).sort("cagr", descending=True)
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.write_csv(summary_path)

    if best_daily is not None:
        best_daily.write_csv(RESULTS / f"{OUT_PREFIX}_best_daily.csv")
    if best_rebalances is not None:
        best_rebalances.write_csv(RESULTS / f"{OUT_PREFIX}_best_rebalances.csv")

    log("\n" + "=" * 110)
    log(f"Iter101 DCF revenue-value summary ({start} -> {end})")
    log("=" * 110)
    print(
        summary.select(
            [
                "name",
                pl.col("cagr").mul(100).round(2).alias("cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("sortino").round(3),
                pl.col("sharpe").round(3),
                pl.col("mdd").mul(100).round(2).alias("mdd_pct"),
                pl.col("avg_positions").round(2),
                pl.col("avg_monthly_turnover").round(2),
                pl.col("final_nav").round(0),
            ]
        )
    )
    log(f"\n[iter101] best={best_name}")
    log(f"[iter101] wrote {summary_path}")
    log(f"[iter101] total runtime {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
