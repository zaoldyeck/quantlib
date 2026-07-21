"""Generate an investor/quant-review HTML spec for the current Iter95 champion.

The report intentionally refreshes the fixed registered champion rather than
running another search.  It extends:

    Iter92 target book + Iter95 time50_r-1 exit layer

to the latest cached market date, under the same realistic Fubon execution
assumptions used in research validation.
"""

from __future__ import annotations

import html
import math
import os
import sys
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

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
from evaluation import nav_metrics  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    ExitConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from iter_82_oos_recent_pm_allocator import load_execution_targets  # noqa: E402
from prices import fetch_adjusted_panel  # noqa: E402
from validator import validate_daily_nav  # noqa: E402


RESULTS = REPO_ROOT / "research/strat_lab/results"
OUT_DIR = REPO_ROOT / "docs/strategy_research"
ASSET_DIR = OUT_DIR / "iter95_strategy_spec_assets"

ITER92_TARGETS = RESULTS / "iter_92_execution_meta_switch_target_weights.csv"
ITER92_STATE = RESULTS / "iter_92_execution_meta_switch_state.csv"

CHAMPION_PREFIX = "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1"
CHAMPION_DAILY = RESULTS / f"{CHAMPION_PREFIX}_daily.csv"
CHAMPION_FILLS = RESULTS / f"{CHAMPION_PREFIX}_fills.csv"
CHAMPION_TRADES = RESULTS / f"{CHAMPION_PREFIX}_trades.csv"
CHAMPION_TARGETS = RESULTS / f"{CHAMPION_PREFIX}_target_weights.csv"
CHAMPION_SUMMARY = RESULTS / f"{CHAMPION_PREFIX}_summary.csv"

REPORT_HTML = OUT_DIR / "iter95_strategy_spec.html"
ROTATION_SUMMARY_CSV = ASSET_DIR / "iter95_rotation_summary.csv"
ROTATION_LEGS_CSV = ASSET_DIR / "iter95_rotation_legs.csv"
DAILY_NAV_CSV = ASSET_DIR / "iter95_daily_nav.csv"

N_TRIALS = 41_116 + 229 + 960


def pct(value: float | None, digits: int = 2) -> str:
    if value is None or not math.isfinite(float(value)):
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


def num(value: float | int | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    if isinstance(value, int):
        return f"{value:,}"
    if not math.isfinite(float(value)):
        return "-"
    return f"{float(value):,.{digits}f}"


def html_table(rows: list[dict[str, object]], columns: list[tuple[str, str]], *, max_rows: int | None = None) -> str:
    shown = rows if max_rows is None else rows[:max_rows]
    head = "".join(f"<th>{html.escape(label)}</th>" for key, label in columns)
    body_parts: list[str] = []
    for row in shown:
        cells = []
        for key, _label in columns:
            value = row.get(key, "")
            cells.append(f"<td>{html.escape(str(value))}</td>")
        body_parts.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_parts)}</tbody></table>"


def drawdown_series(navs: np.ndarray) -> np.ndarray:
    peak = np.maximum.accumulate(navs)
    return navs / np.maximum(peak, 1e-12) - 1.0


def rolling_cagr(dates: list[date], navs: np.ndarray, window: int = 252) -> list[tuple[date, float]]:
    out: list[tuple[date, float]] = []
    for i in range(window, len(navs)):
        years = max((dates[i] - dates[i - window]).days / 365.25, 1e-9)
        value = (navs[i] / navs[i - window]) ** (1.0 / years) - 1.0 if navs[i - window] > 0 else 0.0
        out.append((dates[i], float(value)))
    return out


def line_svg(
    series: dict[str, list[tuple[date, float]]],
    *,
    width: int = 1040,
    height: int = 340,
    log_scale: bool = False,
    percent_axis: bool = False,
) -> str:
    colors = ["#0969da", "#d1242f", "#1a7f37", "#8250df", "#bf8700"]
    all_points = [(d, v) for values in series.values() for d, v in values if v is not None and math.isfinite(v)]
    if not all_points:
        return "<svg></svg>"
    min_date = min(d for d, _v in all_points)
    max_date = max(d for d, _v in all_points)
    span_days = max((max_date - min_date).days, 1)

    def y_value(v: float) -> float:
        return math.log(max(v, 1e-9)) if log_scale else v

    values = [y_value(v) for _d, v in all_points]
    lo, hi = min(values), max(values)
    if abs(hi - lo) < 1e-12:
        hi += 1.0
        lo -= 1.0
    pad = (hi - lo) * 0.08
    lo -= pad
    hi += pad
    left, right, top, bottom = 64, 18, 18, 42
    plot_w = width - left - right
    plot_h = height - top - bottom

    def xy(d: date, v: float) -> tuple[float, float]:
        x = left + ((d - min_date).days / span_days) * plot_w
        y = top + (hi - y_value(v)) / (hi - lo) * plot_h
        return x, y

    grid = []
    for i in range(5):
        y = top + i * plot_h / 4
        raw = hi - i * (hi - lo) / 4
        label_value = math.exp(raw) if log_scale else raw
        label = pct(label_value, 0) if percent_axis else num(label_value, 1)
        grid.append(f"<line x1='{left}' x2='{width-right}' y1='{y:.1f}' y2='{y:.1f}' class='grid'/>")
        grid.append(f"<text x='8' y='{y+4:.1f}' class='axis'>{html.escape(label)}</text>")
    for i in range(5):
        x = left + i * plot_w / 4
        dt = min_date + (max_date - min_date) * i / 4
        grid.append(f"<line x1='{x:.1f}' x2='{x:.1f}' y1='{top}' y2='{height-bottom}' class='grid'/>")
        grid.append(f"<text x='{x-30:.1f}' y='{height-14}' class='axis'>{dt:%Y-%m}</text>")

    polylines = []
    legend = []
    for idx, (name, values_) in enumerate(series.items()):
        color = colors[idx % len(colors)]
        # Downsample large daily series for compact SVGs while preserving shape.
        step = max(1, len(values_) // 1200)
        pts = [xy(d, v) for d, v in values_[::step] if v is not None and math.isfinite(v)]
        points = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        polylines.append(f"<polyline points='{points}' fill='none' stroke='{color}' stroke-width='2.1'/>")
        legend.append(
            f"<span class='legend-item'><span style='background:{color}'></span>{html.escape(name)}</span>"
        )
    return (
        f"<div class='legend'>{''.join(legend)}</div>"
        f"<svg viewBox='0 0 {width} {height}' role='img'>"
        f"{''.join(grid)}{''.join(polylines)}</svg>"
    )


def dates_and_navs(daily: pl.DataFrame) -> tuple[list[date], np.ndarray]:
    ordered = daily.select(["date", "nav"]).sort("date")
    return ordered["date"].to_list(), ordered["nav"].to_numpy().astype(float)


def simulate_champion() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, dict[str, object]]:
    targets = load_execution_targets(ITER92_TARGETS)
    if not targets:
        raise RuntimeError(f"No targets found in {ITER92_TARGETS}")
    codes = sorted({code for book in targets.values() for code in book})
    start = min(targets)

    con = connect(read_only=True)
    try:
        end = con.sql("select max(date) from daily_quote").fetchone()[0]
        # Use TWSE's broad market calendar as the portfolio clock.  The cache can
        # contain isolated TPEx-only rows on dates where TWSE has no source rows;
        # feeding those union dates into the simulator would mark TWSE holdings at
        # zero because no bars exist for that day.
        days = con.execute(
            "select distinct date from daily_quote where market='twse' and date between ? and ? order by date",
            [start, end],
        ).pl()["date"].to_list()
        bars = load_adjusted_execution_bars(con, codes, days[0], days[-1])
    finally:
        con.close()

    config = ExecutionConfig(
        name="fubon_odd_lot_5pct_vol_slip5bp_time50_r-1",
        capital=CAPITAL,
        lot_size=1,
        max_participation_rate=0.05,
        fixed_slippage_bps=5.0,
        impact_bps_per_1pct_volume=1.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
        exit_config=ExitConfig(
            name="time50_r-1",
            time_stop_days=50,
            time_stop_min_return_pct=-0.01,
        ),
    )
    result = RealisticExecutionSimulator(bars, config).simulate(days, targets)
    row = validate_daily_nav(
        "iter95_global_exit_time50_r_minus1",
        result.daily.select(["date", "nav"]),
        n_trials=N_TRIALS,
        extra={**result.stats, "exit_config_detail": str(asdict(config.exit_config))},
    )
    row["data_cutoff"] = days[-1]
    row["target_event_cutoff"] = max(targets)

    CHAMPION_DAILY.parent.mkdir(parents=True, exist_ok=True)
    result.daily.write_csv(CHAMPION_DAILY)
    result.fills.write_csv(CHAMPION_FILLS)
    result.trades.write_csv(CHAMPION_TRADES)
    pl.DataFrame(
        [
            {"date": day, "company_code": code, "target_weight": weight}
            for day, book in sorted(targets.items())
            for code, weight in sorted(book.items())
        ]
    ).write_csv(CHAMPION_TARGETS)
    pl.DataFrame([row]).write_csv(CHAMPION_SUMMARY)
    return result.daily, result.fills, result.trades, row


def benchmark_daily(code: str, market: str, start: date, end: date) -> pl.DataFrame:
    con = connect(read_only=True)
    try:
        panel = fetch_adjusted_panel(con, start.isoformat(), end.isoformat(), codes=[code], market=market, include_extra_history_days=30)
    finally:
        con.close()
    frame = panel.filter(pl.col("company_code") == code).sort("date")
    if frame.is_empty():
        return pl.DataFrame({"date": [], "nav": []})
    first = float(frame["close"][0])
    return frame.select(["date", (pl.col("close") / first * CAPITAL).alias("nav")])


def formatted_metric_row(label: str, row: dict[str, object]) -> dict[str, object]:
    return {
        "Strategy": label,
        "Full CAGR": pct(float(row["cagr"])),
        "Full Sortino": num(float(row["sortino"]), 3),
        "Full MDD": pct(float(row["mdd"])),
        "OOS CAGR": pct(float(row["oos_cagr"])),
        "OOS Sortino": num(float(row["oos_sortino"]), 3),
        "OOS MDD": pct(float(row["oos_mdd"])),
        "Recent 1Y CAGR": pct(float(row["recent_1y_cagr"])),
        "DSR": num(float(row["dsr"]), 3),
        "PBO": num(float(row["pbo"]), 3),
    }


def metric_row(label: str, daily: pl.DataFrame) -> dict[str, object]:
    row = validate_daily_nav(label, daily.select(["date", "nav"]), n_trials=1)
    return formatted_metric_row(label, row)


def current_book() -> tuple[date, dict[str, float]]:
    targets = load_execution_targets(CHAMPION_TARGETS)
    latest = max(targets)
    return latest, targets[latest]


def current_holding_technicals(codes: list[str], latest_target_weights: dict[str, float], end: date) -> list[dict[str, object]]:
    con = connect(read_only=True)
    try:
        raw = con.execute(
            f"""
            with names as (
              with latest as (
                select company_code, max(year*100+month) ym
                from operating_revenue
                where company_code in ({','.join(repr(c) for c in codes)})
                group by company_code
              )
              select o.company_code, any_value(o.company_name) company_name,
                     any_value(o.industry) industry,
                     max(o.year*100+o.month) revenue_ym,
                     any_value(o.monthly_revenue_yoy) revenue_yoy
              from operating_revenue o
              join latest l on o.company_code=l.company_code and o.year*100+o.month=l.ym
              group by o.company_code
            )
            select q.company_code, q.market, n.company_name, n.industry,
                   q.closing_price, q.trade_value,
                   p.price_to_earning_ratio pe,
                   p.price_book_ratio pb,
                   p.dividend_yield dy,
                   n.revenue_ym, n.revenue_yoy
            from daily_quote q
            left join stock_per_pbr p using(market,date,company_code)
            left join names n using(company_code)
            where q.date = ? and q.company_code in ({','.join(repr(c) for c in codes)})
            order by q.company_code
            """,
            [end],
        ).pl()
        flow = con.execute(
            f"""
            with x as (
             select company_code, date,
                    foreign_investors_difference::double foreign_diff,
                    trust_difference::double trust_diff,
                    dealers_difference::double dealer_diff,
                    total_difference::double total_diff
             from daily_trading_details
             where company_code in ({','.join(repr(c) for c in codes)}) and date <= ?
            ), latest as (select max(date) d from x)
            select company_code,
                   sum(foreign_diff) filter(where date > (select d from latest)-interval '5 days') as foreign_5d,
                   sum(foreign_diff) filter(where date > (select d from latest)-interval '20 days') as foreign_20d,
                   sum(trust_diff) filter(where date > (select d from latest)-interval '5 days') as trust_5d,
                   sum(trust_diff) filter(where date > (select d from latest)-interval '20 days') as trust_20d,
                   sum(total_diff) filter(where date > (select d from latest)-interval '5 days') as total_5d,
                   sum(total_diff) filter(where date > (select d from latest)-interval '20 days') as total_20d
            from x group by company_code
            """,
            [end],
        ).pl()
        panels = []
        for market in ("twse", "tpex"):
            frame = fetch_adjusted_panel(con, "2023-01-01", end.isoformat(), codes=codes, market=market, include_extra_history_days=350)
            if not frame.is_empty():
                panels.append(frame)
    finally:
        con.close()
    panel = pl.concat(panels, how="diagonal").sort(["company_code", "date"]) if panels else pl.DataFrame()
    raw_lookup = {r["company_code"]: r for r in raw.iter_rows(named=True)}
    flow_lookup = {r["company_code"]: r for r in flow.iter_rows(named=True)}
    rows: list[dict[str, object]] = []
    for code in sorted(codes, key=lambda item: latest_target_weights.get(item, 0.0), reverse=True):
        df = panel.filter(pl.col("company_code") == code).sort("date")
        closes = df["close"].to_list()
        if not closes:
            continue

        def ret(n: int) -> float | None:
            return closes[-1] / closes[-1 - n] - 1.0 if len(closes) > n and closes[-1 - n] > 0 else None

        def ma_gap(n: int) -> float | None:
            if len(closes) < n:
                return None
            avg = sum(closes[-n:]) / n
            return closes[-1] / avg - 1.0 if avg > 0 else None

        rr = raw_lookup.get(code, {})
        fr = flow_lookup.get(code, {})
        rows.append(
            {
                "Code": code,
                "Name": rr.get("company_name") or "",
                "Weight": pct(latest_target_weights.get(code, 0.0)),
                "Close": num(rr.get("closing_price"), 1),
                "20D": pct(ret(20)),
                "60D": pct(ret(60)),
                "1Y": pct(ret(252)),
                "MA20 Gap": pct(ma_gap(20)),
                "MA60 Gap": pct(ma_gap(60)),
                "PE": num(rr.get("pe"), 2),
                "PB": num(rr.get("pb"), 2),
                "Revenue YoY": pct((rr.get("revenue_yoy") or 0.0) / 100.0),
                "Foreign 20D": num(fr.get("foreign_20d"), 0),
                "Trust 20D": num(fr.get("trust_20d"), 0),
                "Total 20D": num(fr.get("total_20d"), 0),
            }
        )
    return rows


def price_lookup_for_codes(codes: Iterable[str], start: date, end: date) -> dict[tuple[date, str], float]:
    codes = sorted(set(codes))
    con = connect(read_only=True)
    try:
        panels = []
        for market in ("twse", "tpex"):
            frame = fetch_adjusted_panel(con, start.isoformat(), end.isoformat(), codes=codes, market=market, include_extra_history_days=5)
            if not frame.is_empty():
                panels.append(frame)
    finally:
        con.close()
    if not panels:
        return {}
    panel = pl.concat(panels, how="diagonal").sort(["date", "company_code"])
    return {(r["date"], str(r["company_code"])): float(r["close"]) for r in panel.iter_rows(named=True)}


def target_book_by_date(path: Path) -> dict[date, dict[str, float]]:
    frame = pl.read_csv(path, try_parse_dates=True, schema_overrides={"company_code": pl.Utf8, "target_weight": pl.Float64})
    out: dict[date, dict[str, float]] = {}
    for row in frame.iter_rows(named=True):
        out.setdefault(row["date"], {})[str(row["company_code"]).zfill(4)] = float(row["target_weight"])
    return {d: {c: w for c, w in book.items() if w > 1e-12} for d, book in out.items()}


def previous_trading_day(days: list[date], day: date) -> date:
    idx = days.index(day)
    return days[max(0, idx - 1)]


def build_rotation_tables(
    daily: pl.DataFrame,
    fills: pl.DataFrame,
    target_path: Path,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    events = target_book_by_date(target_path)
    event_dates = sorted(events)
    days, navs = dates_and_navs(daily)
    nav_lookup = dict(zip(days, navs, strict=True))
    code_set = sorted({code for book in events.values() for code in book})
    price_lookup = price_lookup_for_codes(code_set, days[0], days[-1])
    state = pl.read_csv(ITER92_STATE, try_parse_dates=True) if ITER92_STATE.exists() else pl.DataFrame()
    selected_lookup = {r["date"]: r.get("selected", "") for r in state.iter_rows(named=True)} if not state.is_empty() else {}
    fill_rows = fills.to_dicts() if fills.height else []

    summary_rows: list[dict[str, object]] = []
    leg_rows: list[dict[str, object]] = []
    prev_book: dict[str, float] = {}
    for i, start in enumerate(event_dates):
        if start not in nav_lookup:
            continue
        if i + 1 < len(event_dates):
            end = previous_trading_day(days, event_dates[i + 1])
        else:
            end = days[-1]
        if end < start or end not in nav_lookup:
            end = start
        seg_days = [d for d in days if start <= d <= end]
        seg_navs = np.array([nav_lookup[d] for d in seg_days], dtype=float)
        seg_return = seg_navs[-1] / seg_navs[0] - 1.0 if len(seg_navs) else 0.0
        seg_mdd = float(drawdown_series(seg_navs).min()) if len(seg_navs) else 0.0
        book = events[start]
        added = sorted(set(book) - set(prev_book))
        removed = sorted(set(prev_book) - set(book))
        kept = sorted(set(book) & set(prev_book))
        event_fills = [r for r in fill_rows if start <= r["date"] <= end]
        buy_notional = sum(float(r["notional"] or 0.0) for r in event_fills if r.get("side") == "buy")
        sell_notional = sum(float(r["notional"] or 0.0) for r in event_fills if r.get("side") == "sell")
        summary_rows.append(
            {
                "start_date": start,
                "end_date": end,
                "trading_days": len(seg_days),
                "selected_sleeve": selected_lookup.get(start, ""),
                "strategy_return": seg_return,
                "segment_mdd": seg_mdd,
                "target_count": len(book),
                "gross_weight": sum(book.values()),
                "added": " ".join(added),
                "removed": " ".join(removed),
                "kept": " ".join(kept),
                "top_weights": " ".join(f"{code}:{book[code]:.1%}" for code in sorted(book, key=book.get, reverse=True)[:8]),
                "fill_count": len(event_fills),
                "buy_notional": buy_notional,
                "sell_notional": sell_notional,
            }
        )
        for code, weight in sorted(book.items(), key=lambda item: item[1], reverse=True):
            start_px = price_lookup.get((start, code))
            end_px = price_lookup.get((end, code))
            stock_ret = end_px / start_px - 1.0 if start_px and end_px else None
            leg_rows.append(
                {
                    "start_date": start,
                    "end_date": end,
                    "company_code": code,
                    "target_weight": weight,
                    "stock_return": stock_ret,
                    "strategy_segment_return": seg_return,
                    "relative_to_strategy": stock_ret - seg_return if stock_ret is not None else None,
                    "start_adj_close": start_px,
                    "end_adj_close": end_px,
                }
            )
        prev_book = book
    return pl.DataFrame(summary_rows), pl.DataFrame(leg_rows)


def rotation_rows_for_html(frame: pl.DataFrame, limit: int = 80) -> list[dict[str, object]]:
    rows = []
    for row in frame.tail(limit).iter_rows(named=True):
        rows.append(
            {
                "Start": row["start_date"],
                "End": row["end_date"],
                "Days": row["trading_days"],
                "Sleeve": row["selected_sleeve"],
                "Return": pct(row["strategy_return"]),
                "MDD": pct(row["segment_mdd"]),
                "Targets": row["target_count"],
                "Gross W": pct(row["gross_weight"]),
                "Added": row["added"],
                "Removed": row["removed"],
                "Top Weights": row["top_weights"],
            }
        )
    return rows


def leg_rows_for_html(frame: pl.DataFrame, limit: int = 180) -> list[dict[str, object]]:
    rows = []
    for row in frame.tail(limit).iter_rows(named=True):
        rows.append(
            {
                "Start": row["start_date"],
                "End": row["end_date"],
                "Code": row["company_code"],
                "Weight": pct(row["target_weight"]),
                "Stock Return": pct(row["stock_return"]),
                "Segment Return": pct(row["strategy_segment_return"]),
                "Relative": pct(row["relative_to_strategy"]),
                "Start Px": num(row["start_adj_close"], 2),
                "End Px": num(row["end_adj_close"], 2),
            }
        )
    return rows


def html_doc(
    daily: pl.DataFrame,
    fills: pl.DataFrame,
    trades: pl.DataFrame,
    summary_row: dict[str, object],
    metrics_table: list[dict[str, object]],
    holdings: list[dict[str, object]],
    rotation_summary: pl.DataFrame,
    rotation_legs: pl.DataFrame,
    benchmark_0050: pl.DataFrame,
    benchmark_2330: pl.DataFrame,
) -> str:
    dates, navs = dates_and_navs(daily)
    dd = list(zip(dates, drawdown_series(navs).tolist(), strict=True))
    roll = rolling_cagr(dates, navs)
    nav_series = {
        "Iter95": list(zip(dates, navs / CAPITAL, strict=True)),
    }
    b_dates, b_navs = dates_and_navs(benchmark_0050)
    nav_series["0050 TR"] = list(zip(b_dates, b_navs / CAPITAL, strict=True))
    t_dates, t_navs = dates_and_navs(benchmark_2330)
    nav_series["2330 TR"] = list(zip(t_dates, t_navs / CAPITAL, strict=True))
    dd_series = {"Iter95 drawdown": dd}
    roll_series = {"Iter95 rolling 1Y CAGR": roll}

    trade_rows = []
    if trades.height:
        closed = trades
        trade_rows = [
            {
                "Metric": "Closed trades",
                "Value": str(closed.height),
            },
            {
                "Metric": "Win rate",
                "Value": pct(float((closed["gross_return"] > 0).mean())),
            },
            {
                "Metric": "Avg gross return",
                "Value": pct(float(closed["gross_return"].mean())),
            },
            {
                "Metric": "Median gross return",
                "Value": pct(float(closed["gross_return"].median())),
            },
            {
                "Metric": "Avg MFE",
                "Value": pct(float(closed["mfe_pct"].mean())),
            },
            {
                "Metric": "Avg MAE",
                "Value": pct(float(closed["mae_pct"].mean())),
            },
        ]
    cost_rows = [
        {"Metric": "Fill ratio", "Value": pct(float(summary_row.get("fill_ratio", 0.0)))},
        {"Metric": "Max active positions", "Value": num(float(summary_row.get("max_active", 0.0)), 0)},
        {"Metric": "Trade days", "Value": num(float(summary_row.get("trade_days", 0.0)), 0)},
        {"Metric": "Avg turnover on trade day", "Value": pct(float(summary_row.get("avg_turnover_trade_day", 0.0)))},
        {"Metric": "Total commission", "Value": num(float(summary_row.get("total_commission", 0.0)), 0)},
        {"Metric": "Total tax", "Value": num(float(summary_row.get("total_tax", 0.0)), 0)},
        {"Metric": "Total slippage cost", "Value": num(float(summary_row.get("total_slippage_cost", 0.0)), 0)},
        {"Metric": "Exit orders", "Value": num(float(summary_row.get("exit_orders", 0.0)), 0)},
    ]
    status_cards = [
        ("Data cutoff", str(summary_row["data_cutoff"])),
        ("Latest target event", str(summary_row["target_event_cutoff"])),
        ("Full CAGR", pct(float(summary_row["cagr"]))),
        ("OOS CAGR", pct(float(summary_row["oos_cagr"]))),
        ("Recent 1Y CAGR", pct(float(summary_row["recent_1y_cagr"]))),
        ("Recent 1Y Window", f"{summary_row['recent_1y_start']} ~ {summary_row['recent_1y_end']}"),
        ("OOS MDD", pct(float(summary_row["oos_mdd"]))),
        ("DSR / PBO", f"{num(float(summary_row['dsr']), 3)} / {num(float(summary_row['pbo']), 3)}"),
    ]
    cards_html = "".join(
        f"<div class='card'><div class='k'>{html.escape(k)}</div><div class='v'>{html.escape(v)}</div></div>"
        for k, v in status_cards
    )
    css = """
    :root { --fg:#1f2328; --muted:#57606a; --line:#d0d7de; --bg:#f6f8fa; --accent:#0969da; }
    body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,"Noto Sans TC",sans-serif; color:var(--fg); margin:0; background:#fff; }
    header { padding:36px 46px 22px; background:#0b1f3a; color:#fff; }
    header h1 { margin:0 0 10px; font-size:30px; letter-spacing:0; }
    header p { margin:4px 0; color:#c9d7ef; }
    main { padding:28px 46px 60px; max-width:1280px; margin:auto; }
    h2 { margin:34px 0 12px; font-size:22px; border-bottom:1px solid var(--line); padding-bottom:8px; }
    h3 { margin:24px 0 10px; font-size:17px; }
    p, li { line-height:1.65; }
    .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:12px; margin:18px 0 24px; }
    .card { border:1px solid var(--line); border-radius:8px; padding:12px 14px; background:var(--bg); }
    .card .k { color:var(--muted); font-size:12px; margin-bottom:8px; }
    .card .v { font-size:20px; font-weight:650; }
    .chart { border:1px solid var(--line); border-radius:8px; padding:16px; margin:14px 0 18px; overflow-x:auto; }
    svg { width:100%; height:auto; display:block; }
    .grid { stroke:#d8dee4; stroke-width:1; }
    .axis { fill:#57606a; font-size:11px; }
    .legend { display:flex; gap:18px; flex-wrap:wrap; margin-bottom:10px; color:#57606a; font-size:13px; }
    .legend-item span { display:inline-block; width:12px; height:12px; border-radius:2px; margin-right:6px; vertical-align:-2px; }
    table { border-collapse:collapse; width:100%; font-size:13px; margin:10px 0 18px; }
    th, td { border:1px solid var(--line); padding:7px 8px; vertical-align:top; }
    th { background:var(--bg); text-align:left; position:sticky; top:0; }
    .note { color:var(--muted); font-size:13px; }
    .callout { border-left:4px solid var(--accent); background:#f6f8fa; padding:12px 14px; margin:16px 0; }
    code { background:#f6f8fa; padding:1px 4px; border-radius:4px; }
    a { color:#0969da; }
    """
    return f"""<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Iter95 Global Exit-Aware Time50 r-1 策略規格報告</title>
<style>{css}</style>
</head>
<body>
<header>
  <h1>Iter95 Global Exit-Aware Time50 r-1 策略規格報告</h1>
  <p>給投資人與量化交易研究員的策略 review 文件。產生時間：{datetime.now():%Y-%m-%d %H:%M:%S}</p>
  <p>資料截止：{summary_row['data_cutoff']}；最新 target event：{summary_row['target_event_cutoff']}；模擬資本：NT$1,000,000。</p>
</header>
<main>
<section>
  <h2>Executive Summary</h2>
  <div class="cards">{cards_html}</div>
  <div class="callout">
  Iter95 是目前登錄在 <code>research/trading/strategy_registry.py</code> 的最高階段策略：
  <code>execution_ready</code>。它已能產生 broker order plan，但尚不是 <code>live_pilot</code> 或
  <code>production_scaled</code>；正式送單仍需要最新 broker accounting smoke test、資金上限設定與 dry-run 明確關閉。
  </div>
</section>

<section>
  <h2>1. 選股與輪動邏輯</h2>
  <h3>策略架構</h3>
  <p>Iter95 不是單一因子選股，也不是固定持有半導體或台積電。它使用 Iter92 多策略 PM allocator 產生 target book，再加上策略層 exit engine。核心思想是：讓多個已經通過 realistic execution 驗證的 sleeve 競爭，依最近 realized NAV 強度切換，而非直接人工指定產業。</p>
  <ol>
    <li>Iter92 每月第一個交易日評估一次 sleeve。</li>
    <li>使用前一交易日前的 5 個交易日 realistic NAV 報酬做相對動能評分。</li>
    <li>候選 sleeve：Iter89 robust execution champion、Iter87 baseline realistic、Iter67/Iter72 realistic recheck。</li>
    <li>選定 sleeve 後至少持有 5 個交易日，避免月內來回切換。</li>
    <li>每日套用被選中 sleeve 的 target book；若 target book 的 L1 權重變化不超過 5%，不觸發無意義換倉。</li>
    <li>Iter95 在此之上加入 time exit：持倉滿 50 個交易日且開盤相對進場均價仍低於 -1%，即退出該部位。</li>
  </ol>
  <h3>執行假設</h3>
  <p>Long-only、不放空、不槓桿；使用 total-return-equivalent adjusted prices；富邦 odd-lot realistic execution；單日成交量參與率上限 5%；固定滑價 5 bps；富邦手續費級距與賣出交易稅 0.3%；漲跌停阻擋與部分成交都納入。</p>
</section>

<section>
  <h2>2. 回測績效</h2>
  {html_table(metrics_table, [(k, k) for k in metrics_table[0].keys()])}
  <div class="chart"><h3>NAV / P&L：Iter95 vs 0050 vs 2330</h3>{line_svg(nav_series, log_scale=True)}</div>
  <div class="chart"><h3>Drawdown</h3>{line_svg(dd_series, percent_axis=True)}</div>
  <div class="chart"><h3>Rolling 1Y CAGR</h3>{line_svg(roll_series, percent_axis=True)}</div>
</section>

<section>
  <h2>3. 目前持倉與技術/籌碼指標</h2>
  <p class="note">技術指標使用 total-return-equivalent adjusted price；估值、交易金額與籌碼使用交易所原始資料。籌碼單位依原資料表，主要用於方向比較。</p>
  {html_table(holdings, [(k, k) for k in holdings[0].keys()] if holdings else [])}
</section>

<section>
  <h2>4. 交易品質與成本</h2>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
    <div>{html_table(cost_rows, [("Metric","Metric"),("Value","Value")])}</div>
    <div>{html_table(trade_rows, [("Metric","Metric"),("Value","Value")])}</div>
  </div>
</section>

<section>
  <h2>5. 換股時間點與每輪報酬</h2>
  <p>完整換股 summary 與每檔股票 leg 報酬已輸出成 CSV，供研究員重算或抽查：</p>
  <ul>
    <li><a href="iter95_strategy_spec_assets/iter95_rotation_summary.csv">iter95_rotation_summary.csv</a></li>
    <li><a href="iter95_strategy_spec_assets/iter95_rotation_legs.csv">iter95_rotation_legs.csv</a></li>
    <li><a href="iter95_strategy_spec_assets/iter95_daily_nav.csv">iter95_daily_nav.csv</a></li>
  </ul>
  <h3>最近 80 次 target-book 換股/調倉事件</h3>
  {html_table(rotation_rows_for_html(rotation_summary, 80), [("Start","Start"),("End","End"),("Days","Days"),("Sleeve","Sleeve"),("Return","Return"),("MDD","MDD"),("Targets","Targets"),("Gross W","Gross W"),("Added","Added"),("Removed","Removed"),("Top Weights","Top Weights")])}
  <h3>最近 180 筆換股 leg 報酬</h3>
  {html_table(leg_rows_for_html(rotation_legs, 180), [("Start","Start"),("End","End"),("Code","Code"),("Weight","Weight"),("Stock Return","Stock Return"),("Segment Return","Segment Return"),("Relative","Relative"),("Start Px","Start Px"),("End Px","End Px")])}
</section>

<section>
  <h2>6. 研究員 Review 重點</h2>
  <ul>
    <li>本報告使用固定 champion 設定，不重新搜尋參數；因此可用於檢查策略本身，而不是最新資料 overfit。</li>
    <li>最重要的風險不是 paper CAGR，而是 realistic execution 下的 fill ratio、slippage、partial fill、漲跌停阻擋與高集中度。</li>
    <li>目前策略仍偏向強勢股/主升段捕捉；在市場快速轉空或題材退潮時，time exit 不會像緊 stop-loss 那樣立即降風險。</li>
    <li>若進入 live pilot，應先以小資金驗證 broker reconciliation、成交回報、ledger 與隔日重複下單防護。</li>
  </ul>
</section>
</main>
</body>
</html>
"""


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    daily, fills, trades, row = simulate_champion()
    start, end = daily["date"].min(), daily["date"].max()
    b0050 = benchmark_daily("0050", "twse", start, end)
    b2330 = benchmark_daily("2330", "twse", start, end)
    metrics = [
        formatted_metric_row("Iter95 realistic", row),
        metric_row("0050 total return", b0050),
        metric_row("2330 total return", b2330),
    ]
    latest_target_event, latest_book = current_book()
    holdings = current_holding_technicals(list(latest_book), latest_book, end)
    rotation_summary, rotation_legs = build_rotation_tables(daily, fills, CHAMPION_TARGETS)
    daily.write_csv(DAILY_NAV_CSV)
    rotation_summary.write_csv(ROTATION_SUMMARY_CSV)
    rotation_legs.write_csv(ROTATION_LEGS_CSV)
    REPORT_HTML.write_text(
        html_doc(
            daily,
            fills,
            trades,
            row,
            metrics,
            holdings,
            rotation_summary,
            rotation_legs,
            b0050,
            b2330,
        ),
        encoding="utf-8",
    )
    print(
        "\n".join(
            [
                f"report={REPORT_HTML}",
                f"daily_nav={DAILY_NAV_CSV}",
                f"rotation_summary={ROTATION_SUMMARY_CSV}",
                f"rotation_legs={ROTATION_LEGS_CSV}",
                f"data_cutoff={row['data_cutoff']}",
                f"target_event_cutoff={row['target_event_cutoff']}",
                f"full_cagr={row['cagr']:.6f}",
                f"oos_cagr={row['oos_cagr']:.6f}",
                f"recent_1y_cagr={row['recent_1y_cagr']:.6f}",
            ]
        )
    )


if __name__ == "__main__":
    main()
