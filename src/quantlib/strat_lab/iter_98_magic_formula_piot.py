"""Iter98 - Magic Formula + Piotroski strategy port and validation.

This implements the user's old ``MFPiot.ipynb`` strategy family in the current
research stack:

* point-in-time financial statement availability;
* total-return-equivalent adjusted execution prices;
* realistic Fubon-style target-book execution;
* canonical KPI/overfit validator;
* comparison against Iter95, 0050 TR, and 2330 TR on a common window.

The original notebook variants are kept as separate named strategies so we can
distinguish the value of each added filter instead of only testing the final
optimized version.
"""

from __future__ import annotations

import html
import math
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

import numpy as np
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots
from quantlib import paths

REPO_ROOT = paths.REPO
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from quantlib.constants import CAPITAL  # noqa: E402
from quantlib.db import connect  # noqa: E402
from evaluation import drawdown_series, nav_metrics  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    ExitConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from iter_96_robust_alpha_research import load_benchmark_nav, relative_metrics  # noqa: E402
from quantlib.prices import fetch_adjusted_panel  # noqa: E402
from validator import validate_daily_nav  # noqa: E402


START = date(2010, 1, 4)
RESULTS = REPO_ROOT / f"{paths.OUT_STRAT_LAB}"
OUT_DIR = REPO_ROOT / "docs/strategy_research/mf_piot"
OUT_PREFIX = "iter_98_magic_formula_piot"
N_TRIALS_PRIOR = 41_116 + 229 + 960

TimingMode = Literal["article_deadline", "pit_buffer"]


@dataclass(frozen=True)
class RebalanceEvent:
    target_date: date
    feature_date: date
    report_year: int
    report_quarter: int
    revenue_year: int
    revenue_month: int
    timing: TimingMode


@dataclass(frozen=True)
class Variant:
    name: str
    timing: TimingMode
    topn: int
    min_fscore: int | None = None
    require_revenue_yoy: bool = False
    require_op_growth: bool = False
    require_fcf: bool = False
    min_rsv: float | None = None
    min_acc: float | None = None
    min_slope: float | None = None
    stop_loss_pct: float | None = None
    min_adv30: float = 50_000_000.0
    markets: tuple[str, ...] = ("twse", "tpex")


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


def next_trading_day(days: list[date], day: date) -> date | None:
    idx = int(np.searchsorted(np.asarray([d.toordinal() for d in days], dtype=np.int32), day.toordinal(), side="left"))
    return days[idx] if idx < len(days) else None


def prev_trading_day(days: list[date], day: date) -> date | None:
    idx = int(np.searchsorted(np.asarray([d.toordinal() for d in days], dtype=np.int32), day.toordinal(), side="left")) - 1
    return days[idx] if idx >= 0 else None


def q_available_date(year: int, quarter: int, timing: TimingMode) -> date:
    if timing == "article_deadline":
        if quarter == 1:
            return date(year, 5, 16)
        if quarter == 2:
            return date(year, 8, 15)
        if quarter == 3:
            return date(year, 11, 15)
        if quarter == 4:
            return date(year + 1, 4, 1)
    else:
        if quarter == 1:
            return date(year, 5, 15) + timedelta(days=7)
        if quarter == 2:
            return date(year, 8, 14) + timedelta(days=7)
        if quarter == 3:
            return date(year, 11, 14) + timedelta(days=7)
        if quarter == 4:
            return date(year + 1, 3, 31) + timedelta(days=7)
    raise ValueError(f"quarter must be 1..4, got {quarter}")


def monthly_revenue_deadline(year: int, month: int) -> date:
    if month == 12:
        base = date(year + 1, 1, 10)
    else:
        base = date(year, month + 1, 10)
    return base + timedelta(days=3)


def asof_monthly_revenue(as_of: date) -> tuple[int, int]:
    candidates: list[tuple[date, int, int]] = []
    for year in range(as_of.year - 2, as_of.year + 1):
        for month in range(1, 13):
            dl = monthly_revenue_deadline(year, month)
            if dl <= as_of:
                candidates.append((dl, year, month))
    if not candidates:
        raise RuntimeError(f"no monthly revenue is safely available on {as_of}")
    _, year, month = max(candidates, key=lambda item: item[0])
    return year, month


def trading_days(start: date, end: date) -> list[date]:
    con = connect(read_only=True)
    try:
        rows = con.sql(
            f"""
            SELECT date
            FROM daily_quote
            WHERE market='twse'
              AND company_code='0050'
              AND date BETWEEN DATE '{start}' AND DATE '{end}'
            ORDER BY date
            """
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        con.close()


def rebalance_events(days: list[date], start: date, end: date, timing: TimingMode) -> list[RebalanceEvent]:
    events: list[RebalanceEvent] = []
    for year in range(start.year - 1, end.year + 1):
        for quarter in (1, 2, 3, 4):
            available = q_available_date(year, quarter, timing)
            if available < start or available > end:
                continue
            target = next_trading_day(days, available)
            if target is None or target > end:
                continue
            feature = prev_trading_day(days, target)
            if feature is None:
                continue
            rev_year, rev_month = asof_monthly_revenue(target)
            events.append(
                RebalanceEvent(
                    target_date=target,
                    feature_date=feature,
                    report_year=year,
                    report_quarter=quarter,
                    revenue_year=rev_year,
                    revenue_month=rev_month,
                    timing=timing,
                )
            )
    return sorted({event.target_date: event for event in events}.values(), key=lambda event: event.target_date)


def clean_code_expr() -> pl.Expr:
    return pl.col("company_code").str.contains(r"^[1-9][0-9]{3}$")


def load_price_features(start: date, end: date) -> pl.DataFrame:
    con = connect(read_only=True)
    try:
        panels = [
            fetch_adjusted_panel(
                con,
                start.isoformat(),
                end.isoformat(),
                market=market,
                include_extra_history_days=380,
            )
            for market in ("twse", "tpex")
        ]
        etf = con.sql("SELECT DISTINCT company_code FROM etf").pl().with_columns(pl.lit(True).alias("is_etf"))
    finally:
        con.close()

    panel = (
        pl.concat([p for p in panels if not p.is_empty()], how="diagonal")
        .sort(["company_code", "date", "trade_value"], descending=[False, False, True])
        .unique(subset=["company_code", "date"], keep="first", maintain_order=True)
    )
    if panel.is_empty():
        raise RuntimeError("empty adjusted price panel")

    return (
        panel.sort(["company_code", "date"])
        .with_columns(
            [
                pl.col("close").rolling_min(120).over("company_code").alias("low120"),
                pl.col("close").rolling_max(120).over("company_code").alias("high120"),
                pl.col("close").shift(120).over("company_code").alias("close_lag120"),
                pl.col("close").shift(240).over("company_code").alias("close_lag240"),
                pl.col("close").rolling_mean(20).over("company_code").alias("ma20"),
                pl.col("trade_value").rolling_median(30).over("company_code").alias("adv30_median"),
                pl.col("trade_value").is_not_null().cast(pl.Int32).rolling_sum(30).over("company_code").alias("trading_days30"),
            ]
        )
        .with_columns(
            [
                ((pl.col("close") - pl.col("low120")) / (pl.col("high120") - pl.col("low120")).clip(1e-9, None)).alias("rsv120"),
                (((pl.col("close") + pl.col("close_lag240")) / 2.0) / pl.col("close_lag120") - 1.0).alias("acc120"),
                (pl.col("ma20") - pl.col("ma20").shift(1).over("company_code")).alias("sma_slope20"),
            ]
        )
        .join(etf, on="company_code", how="left")
        .with_columns(pl.col("is_etf").fill_null(False))
        .filter((pl.col("date") >= start) & (pl.col("date") <= end))
        .select(
            [
                "date",
                "market",
                "company_code",
                "raw_close",
                "close",
                "trade_value",
                "adv30_median",
                "trading_days30",
                "rsv120",
                "acc120",
                "sma_slope20",
                "is_etf",
            ]
        )
    )


def load_fundamentals() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    con = connect(read_only=True)
    try:
        quarterly = con.sql(
            """
            SELECT market, year, quarter, company_code,
                   f_score_raw, op_income_q, total_assets, current_liabilities,
                   total_equity, capital_stock
            FROM raw_quarterly
            """
        ).pl()
        ebit = con.sql(
            """
            SELECT market, year, quarter, company_code,
                   MAX(value) FILTER (
                       WHERE title IN (
                           '繼續營業單位稅前淨利（淨損）',
                           '繼續營業單位稅前淨利(淨損)',
                           '繼續營業單位稅前純益（純損）',
                           '繼續營業單位稅前純益(純損)',
                           '繼續營業單位稅前合併淨利(淨損)',
                           '繼續營業單位稅前損益',
                           '繼續營業單位稅前淨利',
                           '稅前淨利（淨損）',
                           '本期稅前淨利（淨損）'
                       )
                   ) AS ebit_ytd
            FROM is_progressive_raw
            GROUP BY market, year, quarter, company_code
            """
        ).pl()
        cash = con.sql(
            """
            SELECT market, year, quarter, company_code,
                   MAX(value) FILTER (
                       WHERE title IN (
                           '現金及約當現金',
                           '現金及約當現金合計',
                           '現金及約當現金總額'
                       )
                   ) AS cash_and_equiv
            FROM bs_concise_raw
            GROUP BY market, year, quarter, company_code
            """
        ).pl()
        cf = con.sql(
            """
            SELECT year, quarter, company_code,
                   MAX(value) FILTER (
                       WHERE title IN (
                           '營業活動之淨現金流入（流出）',
                           '營業活動之淨現金流入(流出)'
                       )
                   ) AS cfo_ytd,
                   MAX(value) FILTER (
                       WHERE title IN (
                           '投資活動之淨現金流入（流出）',
                           '投資活動之淨現金流入(流出)'
                       )
                   ) AS invest_ytd
            FROM cf_progressive_raw
            GROUP BY year, quarter, company_code
            """
        ).pl()
        revenue = con.sql(
            """
            WITH ranked AS (
              SELECT market, year, month, company_code, monthly_revenue_yoy,
                     CASE WHEN type='consolidated' THEN 0 ELSE 1 END AS type_rank
              FROM operating_revenue
              WHERE monthly_revenue > 0
            )
            SELECT market, year, month, company_code, monthly_revenue_yoy
            FROM ranked
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY year, month, company_code
              ORDER BY type_rank, market
            ) = 1
            """
        ).pl()
    finally:
        con.close()

    op_last = quarterly.select(
        [
            "market",
            (pl.col("year") + 1).alias("year"),
            "quarter",
            "company_code",
            pl.col("op_income_q").alias("op_income_q_last_year"),
        ]
    )
    cf_q = (
        cf.sort(["company_code", "year", "quarter"])
        .with_columns(
            [
                pl.when(pl.col("quarter") == 1)
                .then(pl.col("cfo_ytd"))
                .otherwise(pl.col("cfo_ytd") - pl.col("cfo_ytd").shift(1).over(["company_code", "year"], order_by="quarter"))
                .alias("cfo_q"),
                pl.when(pl.col("quarter") == 1)
                .then(pl.col("invest_ytd"))
                .otherwise(
                    pl.col("invest_ytd") - pl.col("invest_ytd").shift(1).over(["company_code", "year"], order_by="quarter")
                )
                .alias("invest_q"),
            ]
        )
        .with_columns((pl.col("cfo_q") + pl.col("invest_q")).alias("fcf_q"))
        .select(["year", "quarter", "company_code", "fcf_q"])
    )

    fundamentals = (
        quarterly.join(ebit, on=["market", "year", "quarter", "company_code"], how="left")
        .join(cash, on=["market", "year", "quarter", "company_code"], how="left")
        .join(cf_q, on=["year", "quarter", "company_code"], how="left")
        .join(op_last, on=["market", "year", "quarter", "company_code"], how="left")
        .with_columns(
            [
                (pl.col("total_assets") - pl.col("total_equity")).alias("total_debt"),
                (
                    pl.col("op_income_q") / pl.col("op_income_q_last_year").abs().clip(1e-9, None) - 1.0
                ).alias("op_income_growth_yoy"),
            ]
        )
    )
    return fundamentals, revenue, load_cap_reductions(), load_etf_codes()


def load_cap_reductions() -> pl.DataFrame:
    con = connect(read_only=True)
    try:
        return con.sql("SELECT date, company_code FROM capital_reduction").pl()
    finally:
        con.close()


def load_etf_codes() -> pl.DataFrame:
    con = connect(read_only=True)
    try:
        return con.sql("SELECT DISTINCT company_code FROM etf").pl()
    finally:
        con.close()


def build_candidates(timing: TimingMode, days: list[date], start: date, end: date) -> tuple[pl.DataFrame, list[RebalanceEvent]]:
    t0 = time.time()
    events = rebalance_events(days, start, end, timing)
    log(f"[iter98] {timing}: {len(events)} rebalance events")
    px = load_price_features(start, end)
    fundamentals, revenue, cap_reductions, _ = load_fundamentals()

    pieces: list[pl.DataFrame] = []
    for event in events:
        px_day = px.filter(pl.col("date") == event.feature_date)
        if px_day.is_empty():
            continue
        q = fundamentals.filter(
            (pl.col("year") == event.report_year) & (pl.col("quarter") == event.report_quarter)
        )
        rev = revenue.filter((pl.col("year") == event.revenue_year) & (pl.col("month") == event.revenue_month)).select(
            ["company_code", pl.col("monthly_revenue_yoy").alias("revenue_yoy_pct")]
        )
        blacklist = set(
            cap_reductions.filter(
                (pl.col("date") <= event.feature_date) & (pl.col("date") >= event.feature_date - timedelta(days=365 * 3))
            )["company_code"].to_list()
        )
        joined = (
            px_day.join(q, on=["market", "company_code"], how="inner")
            .join(rev, on="company_code", how="left")
            .filter(clean_code_expr())
            .filter(~pl.col("is_etf"))
            .filter(~pl.col("company_code").is_in(sorted(blacklist)))
            .with_columns(
                [
                    pl.lit(event.target_date).alias("signal_date"),
                    pl.lit(event.feature_date).alias("feature_date"),
                    pl.lit(event.report_year).alias("report_year"),
                    pl.lit(event.report_quarter).alias("report_quarter"),
                    pl.lit(event.revenue_year).alias("revenue_year"),
                    pl.lit(event.revenue_month).alias("revenue_month"),
                ]
            )
            .with_columns(
                [
                    (pl.col("ebit_ytd") / (pl.col("total_assets") - pl.col("current_liabilities")).clip(1e-9, None)).alias(
                        "roic"
                    ),
                    (
                        pl.col("ebit_ytd")
                        / (
                            pl.col("raw_close") * pl.col("capital_stock") / 10.0
                            + pl.col("total_debt")
                            - pl.col("cash_and_equiv").fill_null(0.0)
                        ).clip(1e-9, None)
                    ).alias("earnings_yield"),
                ]
            )
        )
        pieces.append(joined)

    if not pieces:
        raise RuntimeError(f"no candidate rows for {timing}")
    candidates = (
        pl.concat(pieces, how="diagonal")
        .filter((pl.col("roic") > 0) & (pl.col("earnings_yield") > 0))
        .with_columns(
            [
                (pl.col("roic").rank("ordinal").over("signal_date") - 1).alias("roic_rank"),
                (pl.col("earnings_yield").rank("ordinal").over("signal_date") - 1).alias("ey_rank"),
            ]
        )
        .with_columns((pl.col("roic_rank") + pl.col("ey_rank")).alias("mf_score"))
        .sort(["signal_date", "mf_score"], descending=[False, True])
    )
    log(
        f"[iter98] {timing}: candidate rows={candidates.height:,} "
        f"codes={candidates['company_code'].n_unique():,} elapsed={time.time() - t0:.1f}s"
    )
    return candidates, events


def variant_filter(frame: pl.DataFrame, variant: Variant) -> pl.DataFrame:
    expr = pl.col("market").is_in(list(variant.markets))
    expr &= pl.col("adv30_median").fill_null(0.0) >= variant.min_adv30
    expr &= pl.col("trading_days30").fill_null(0) >= 10
    if variant.min_fscore is not None:
        expr &= pl.col("f_score_raw").fill_null(0) >= variant.min_fscore
    if variant.require_revenue_yoy:
        expr &= pl.col("revenue_yoy_pct").fill_null(-math.inf) > 0
    if variant.require_op_growth:
        expr &= pl.col("op_income_growth_yoy").fill_null(-math.inf) > 0
    if variant.require_fcf:
        expr &= pl.col("fcf_q").fill_null(-math.inf) > 0
    if variant.min_rsv is not None:
        expr &= pl.col("rsv120").fill_null(-math.inf) > variant.min_rsv
    if variant.min_acc is not None:
        expr &= pl.col("acc120").fill_null(-math.inf) > variant.min_acc
    if variant.min_slope is not None:
        expr &= pl.col("sma_slope20").fill_null(-math.inf) > variant.min_slope
    return frame.filter(expr)


def targets_for_variant(candidates: pl.DataFrame, variant: Variant) -> tuple[dict[date, dict[str, float]], pl.DataFrame]:
    filtered = variant_filter(candidates, variant)
    selected_rows: list[dict[str, object]] = []
    targets: dict[date, dict[str, float]] = {}
    for signal_date in sorted(candidates["signal_date"].unique().to_list()):
        picked = filtered.filter(pl.col("signal_date") == signal_date).sort("mf_score", descending=True).head(variant.topn)
        if picked.is_empty():
            targets[signal_date] = {}
            continue
        codes = picked["company_code"].to_list()
        weight = 1.0 / len(codes)
        targets[signal_date] = {str(code): weight for code in codes}
        for rank, row in enumerate(picked.iter_rows(named=True), start=1):
            selected_rows.append(
                {
                    "date": signal_date,
                    "rank": rank,
                    "company_code": row["company_code"],
                    "market": row["market"],
                    "target_weight": weight,
                    "mf_score": row["mf_score"],
                    "roic": row["roic"],
                    "earnings_yield": row["earnings_yield"],
                    "f_score_raw": row["f_score_raw"],
                    "revenue_yoy_pct": row["revenue_yoy_pct"],
                    "op_income_growth_yoy": row["op_income_growth_yoy"],
                    "fcf_q": row["fcf_q"],
                    "rsv120": row["rsv120"],
                    "acc120": row["acc120"],
                    "adv30_median": row["adv30_median"],
                    "report_year": row["report_year"],
                    "report_quarter": row["report_quarter"],
                    "revenue_year": row["revenue_year"],
                    "revenue_month": row["revenue_month"],
                }
            )
    selected = pl.DataFrame(selected_rows, infer_schema_length=10_000) if selected_rows else pl.DataFrame()
    return targets, selected


def make_variants() -> list[Variant]:
    base = dict(timing="pit_buffer", min_adv30=50_000_000.0)
    article = dict(timing="article_deadline", min_adv30=50_000_000.0)
    return [
        Variant("mf_top10_pit_adv50", topn=10, **base),
        Variant("mf_piot_f8_top10_pit_adv50", topn=10, min_fscore=8, **base),
        Variant(
            "mf_piot_opt_top10_pit_adv50",
            topn=10,
            min_fscore=8,
            require_revenue_yoy=True,
            require_op_growth=True,
            require_fcf=True,
            **base,
        ),
        Variant(
            "mf_piot_opt_top5_pit_adv50",
            topn=5,
            min_fscore=8,
            require_revenue_yoy=True,
            require_op_growth=True,
            require_fcf=True,
            **base,
        ),
        Variant(
            "mf_piot_opt_top5_rsv90_pit_adv50",
            topn=5,
            min_fscore=8,
            require_revenue_yoy=True,
            require_op_growth=True,
            require_fcf=True,
            min_rsv=0.9,
            **base,
        ),
        Variant(
            "mf_piot_opt_top5_rsv90_acc_pit_adv50",
            topn=5,
            min_fscore=8,
            require_revenue_yoy=True,
            require_op_growth=True,
            require_fcf=True,
            min_rsv=0.9,
            min_acc=0.0,
            **base,
        ),
        Variant(
            "mf_piot_opt_top5_rsv90_sl20_pit_adv50",
            topn=5,
            min_fscore=8,
            require_revenue_yoy=True,
            require_op_growth=True,
            require_fcf=True,
            min_rsv=0.9,
            stop_loss_pct=0.20,
            **base,
        ),
        Variant(
            "mf_piot_opt_top5_rsv90_pit_adv1",
            topn=5,
            min_fscore=8,
            require_revenue_yoy=True,
            require_op_growth=True,
            require_fcf=True,
            min_rsv=0.9,
            timing="pit_buffer",
            min_adv30=1_000_000.0,
        ),
        Variant(
            "mf_piot_opt_top5_rsv90_article_adv50",
            topn=5,
            min_fscore=8,
            require_revenue_yoy=True,
            require_op_growth=True,
            require_fcf=True,
            min_rsv=0.9,
            **article,
        ),
    ]


def write_target_weights(path: Path, selected: pl.DataFrame) -> None:
    if selected.is_empty():
        pl.DataFrame(schema={"date": pl.Date, "company_code": pl.Utf8, "target_weight": pl.Float64}).write_csv(path)
        return
    selected.select(["date", "company_code", "target_weight"]).write_csv(path)


def summarize_variant(
    variant: Variant,
    daily: pl.DataFrame,
    selected: pl.DataFrame,
    result_stats: dict[str, float],
    benchmark_0050: pl.DataFrame,
    benchmark_2330: pl.DataFrame,
    n_trials: int,
) -> dict[str, object]:
    extra = {
        "timing": variant.timing,
        "topn": variant.topn,
        "min_fscore": variant.min_fscore,
        "min_adv30": variant.min_adv30,
        "stop_loss_pct": variant.stop_loss_pct,
        "avg_positions": float(daily["active"].mean()) if daily.height and "active" in daily.columns else 0.0,
        "max_positions": int(result_stats.get("max_active", 0.0)),
        "rebalance_count": int(selected["date"].n_unique()) if not selected.is_empty() else 0,
        **result_stats,
    }
    row = validate_daily_nav(variant.name, daily.select(["date", "nav"]), n_trials=n_trials, extra=extra)
    row.update(relative_metrics(daily.select(["date", "nav"]), benchmark_0050, "b0050"))
    row.update(relative_metrics(daily.select(["date", "nav"]), benchmark_2330, "b2330"))
    return row


def load_iter95_common(start: date, end: date) -> pl.DataFrame:
    path = (
        RESULTS
        / "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_daily.csv"
    )
    daily = pl.read_csv(path, try_parse_dates=True).select(["date", "nav"]).sort("date")
    return daily.filter((pl.col("date") >= start) & (pl.col("date") <= end))


def normalize_nav(daily: pl.DataFrame, start_nav: float = CAPITAL) -> pl.DataFrame:
    first = float(daily.sort("date")["nav"][0])
    return daily.with_columns((pl.col("nav") / first * start_nav).alias("nav"))


def benchmark_common(code: str, start: date, end: date, label: str) -> pl.DataFrame:
    return load_benchmark_nav(code, start, end, label).select(["date", "nav"]).sort("date")


def add_benchmark_rows(summary: list[dict[str, object]], start: date, end: date) -> None:
    for code, name in [("0050", "0050 TR"), ("2330", "2330 TR")]:
        daily = benchmark_common(code, start, end, name)
        row: dict[str, object] = {"name": name, "timing": "benchmark", "topn": None}
        row.update(nav_metrics(daily, capital=CAPITAL))
        row.update({f"oos_{k}": v for k, v in nav_metrics(daily.filter(pl.col("date").dt.year() >= 2010), capital=CAPITAL).items()})
        row.update(validate_daily_nav(name, daily, n_trials=1))
        summary.append(row)


def drawdown_frame(daily: pl.DataFrame, label: str) -> pl.DataFrame:
    ordered = daily.select(["date", "nav"]).sort("date")
    dd = drawdown_series(ordered["nav"].to_numpy().astype(float), CAPITAL)
    return pl.DataFrame({"date": ordered["date"].to_list(), "drawdown": dd, "series": label})


def generate_report(summary: pl.DataFrame, navs: pl.DataFrame, out_html: Path, cutoff: date) -> None:
    top = summary.filter(~pl.col("name").str.contains("TR$")).sort("oos_cagr", descending=True).head(4)
    labels = ["Iter95 champion", *top["name"].to_list(), "0050 TR", "2330 TR"]
    plot_nav = navs.filter(pl.col("series").is_in(labels)).sort(["series", "date"])
    dd = pl.concat(
        [
            drawdown_frame(plot_nav.filter(pl.col("series") == label).select(["date", "nav"]), label)
            for label in labels
            if not plot_nav.filter(pl.col("series") == label).is_empty()
        ],
        how="diagonal",
    )

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=("NAV / P&L", "Drawdown"),
    )
    for label in labels:
        s = plot_nav.filter(pl.col("series") == label).sort("date")
        if s.is_empty():
            continue
        fig.add_trace(go.Scatter(x=s["date"].to_list(), y=s["nav"].to_list(), name=label, mode="lines"), row=1, col=1)
        d = dd.filter(pl.col("series") == label).sort("date")
        fig.add_trace(
            go.Scatter(x=d["date"].to_list(), y=d["drawdown"].to_list(), name=f"{label} DD", mode="lines", showlegend=False),
            row=2,
            col=1,
        )
    fig.update_layout(template="plotly_white", height=850, hovermode="x unified")
    fig.update_yaxes(type="log", row=1, col=1)
    fig.update_yaxes(tickformat=".0%", row=2, col=1)

    display_cols = [
        "name",
        "timing",
        "cagr",
        "oos_cagr",
        "recent_1y_cagr",
        "sortino",
        "calmar",
        "mdd",
        "dsr",
        "pbo",
        "fill_ratio",
        "avg_positions",
        "max_positions",
        "b0050_final_relative_nav",
        "b2330_final_relative_nav",
    ]
    table = summary.select([c for c in display_cols if c in summary.columns]).sort("oos_cagr", descending=True)
    rows = []
    for row in table.iter_rows(named=True):
        cells = []
        for col in table.columns:
            value = row[col]
            if isinstance(value, float):
                if col.endswith("cagr") or col in {"sortino", "calmar", "mdd", "dsr", "pbo", "fill_ratio"}:
                    if col in {"sortino", "calmar", "dsr", "pbo", "fill_ratio"}:
                        cells.append(f"<td>{value:.3f}</td>")
                    else:
                        cells.append(f"<td>{value:.2%}</td>")
                else:
                    cells.append(f"<td>{value:.2f}</td>")
            else:
                cells.append(f"<td>{html.escape(str(value))}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")

    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(
        f"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <title>Magic Formula + Piotroski Strategy Validation</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Noto Sans TC", sans-serif; margin: 32px; color: #172026; }}
    h1, h2 {{ margin: 0 0 16px; }}
    p {{ line-height: 1.7; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #d9e1e8; padding: 7px 8px; text-align: right; white-space: nowrap; }}
    th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) {{ text-align: left; }}
    th {{ background: #eef4f7; position: sticky; top: 0; }}
    .note {{ background: #f6f8fa; border-left: 4px solid #577590; padding: 12px 14px; margin: 16px 0 24px; }}
  </style>
</head>
<body>
  <h1>Magic Formula + Piotroski 台股策略驗證</h1>
  <p>資料 cutoff：{cutoff}。本報告把 MFPiot notebook/article 的策略規則移植到目前專案的 adjusted total-return 與 realistic execution 框架。</p>
  <div class="note">
    正式判斷以 <code>pit_buffer</code> 為準：財報採申報期限後 7 日 buffer，價格訊號只使用送單日前一個交易日資料，並以隔日 open 執行。<code>article_deadline</code> 僅用來觀察原文較寬鬆時點口徑。
  </div>
  {fig.to_html(full_html=False, include_plotlyjs="cdn")}
  <h2>KPI Ranking</h2>
  <table>
    <thead><tr>{''.join(f'<th>{html.escape(c)}</th>' for c in table.columns)}</tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</body>
</html>
""",
        encoding="utf-8",
    )


def run() -> None:
    t0 = time.time()
    cutoff = latest_0050_day()
    days = trading_days(START, cutoff)
    if not days:
        raise RuntimeError("no trading days")

    variants = make_variants()
    timings = sorted({variant.timing for variant in variants})
    candidate_by_timing: dict[TimingMode, pl.DataFrame] = {}
    for timing in timings:
        candidate_by_timing[timing], _ = build_candidates(timing, days, START, cutoff)

    first_target = min(
        date_value
        for variant in variants
        for date_value in candidate_by_timing[variant.timing]["signal_date"].unique().to_list()
    )
    sim_days = [d for d in days if d >= first_target]
    common_start = sim_days[0]
    log(f"[iter98] simulation window {common_start} -> {cutoff} days={len(sim_days):,}")

    all_codes = sorted(
        {
            str(code)
            for candidates in candidate_by_timing.values()
            for code in candidates["company_code"].unique().to_list()
        }
    )
    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, all_codes, common_start, cutoff, markets=("twse", "tpex"))
    finally:
        con.close()
    log(f"[iter98] execution bars rows={bars.height:,} codes={bars['company_code'].n_unique():,}")

    benchmark_0050 = benchmark_common("0050", common_start, cutoff, "0050 TR")
    benchmark_2330 = benchmark_common("2330", common_start, cutoff, "2330 TR")

    summary_rows: list[dict[str, object]] = []
    nav_frames: list[pl.DataFrame] = []
    n_trials = N_TRIALS_PRIOR + len(variants)

    for variant in variants:
        log(f"[iter98] simulate {variant.name}")
        targets, selected = targets_for_variant(candidate_by_timing[variant.timing], variant)
        config = ExecutionConfig(
            name="fubon_odd_lot_iter98",
            lot_size=1,
            max_participation_rate=0.05,
            fixed_slippage_bps=5.0,
            impact_bps_per_1pct_volume=1.0,
            fee_schedule=FubonFeeSchedule(),
            exit_config=ExitConfig(name="stop_loss_20", stop_loss_pct=variant.stop_loss_pct)
            if variant.stop_loss_pct
            else ExitConfig(),
        )
        result = RealisticExecutionSimulator(bars, config).simulate(sim_days, targets)
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", variant.name)
        result.daily.write_csv(RESULTS / f"{OUT_PREFIX}_{safe_name}_daily.csv")
        result.fills.write_csv(RESULTS / f"{OUT_PREFIX}_{safe_name}_fills.csv")
        result.trades.write_csv(RESULTS / f"{OUT_PREFIX}_{safe_name}_trades.csv")
        selected.write_csv(RESULTS / f"{OUT_PREFIX}_{safe_name}_selection.csv")
        write_target_weights(RESULTS / f"{OUT_PREFIX}_{safe_name}_target_weights.csv", selected)

        daily = result.daily.select(["date", "nav", "active", "turnover"]).sort("date")
        summary_rows.append(
            summarize_variant(
                variant,
                daily,
                selected,
                result.stats,
                benchmark_0050,
                benchmark_2330,
                n_trials,
            )
        )
        nav_frames.append(daily.select(["date", "nav"]).with_columns(pl.lit(variant.name).alias("series")))

    iter95 = normalize_nav(load_iter95_common(common_start, cutoff))
    iter95_row = validate_daily_nav("Iter95 champion", iter95, n_trials=1)
    iter95_row.update(relative_metrics(iter95, benchmark_0050, "b0050"))
    iter95_row.update(relative_metrics(iter95, benchmark_2330, "b2330"))
    iter95_row.update({"timing": "champion", "topn": None, "avg_positions": None, "max_positions": None})
    summary_rows.append(iter95_row)
    nav_frames.append(iter95.with_columns(pl.lit("Iter95 champion").alias("series")))

    for code, label in [("0050", "0050 TR"), ("2330", "2330 TR")]:
        bench = benchmark_common(code, common_start, cutoff, label)
        row = validate_daily_nav(label, bench, n_trials=1)
        row.update({"timing": "benchmark", "topn": None, "avg_positions": None, "max_positions": None})
        summary_rows.append(row)
        nav_frames.append(bench.with_columns(pl.lit(label).alias("series")))

    summary = pl.DataFrame(summary_rows, infer_schema_length=10_000).sort("oos_cagr", descending=True)
    navs = pl.concat(nav_frames, how="diagonal")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    nav_path = RESULTS / f"{OUT_PREFIX}_daily_nav.csv"
    doc_summary_path = OUT_DIR / "mf_piot_kpi_summary.csv"
    doc_nav_path = OUT_DIR / "mf_piot_daily_nav.csv"
    html_path = OUT_DIR / "mf_piot_strategy_report.html"
    summary.write_csv(summary_path)
    navs.write_csv(nav_path)
    summary.write_csv(doc_summary_path)
    navs.write_csv(doc_nav_path)
    generate_report(summary, navs, html_path, cutoff)

    log("[iter98] top KPI rows:")
    print(
        summary.select(
            [
                "name",
                "cagr",
                "oos_cagr",
                "recent_1y_cagr",
                "sortino",
                "calmar",
                "mdd",
                "dsr",
                "pbo",
                "fill_ratio",
                "b0050_final_relative_nav",
                "b2330_final_relative_nav",
            ]
        )
        .head(12)
        .to_pandas()
        .to_string(index=False),
        flush=True,
    )
    log(f"[iter98] wrote {summary_path}")
    log(f"[iter98] wrote {html_path}")
    log(f"[iter98] elapsed={time.time() - t0:.1f}s")


if __name__ == "__main__":
    run()
