"""iter_75 - dynamic industry leadership research.

Goal: replace the fixed 0052/0050 technology gate with a market-wide,
condition-driven industry leadership framework. The strategy may select
technology or semiconductor stocks only when their observable conditions rank
well against all other industries; no industry name is hard-coded into the
selection rule.

This is still a research pass, not an execution-ready broker target book.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from quantlib import paths

os.environ.setdefault("POLARS_MAX_THREADS", str(os.cpu_count() or 1))

import numpy as np
import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from iter_32_first_principles import build_feature_panel, _clip_expr  # noqa: E402
from iter_40_research_campaign import CAPITAL, COMMISSION, RF, SELL_TAX, metrics_from_rets  # noqa: E402
from validator import validate_daily_nav  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from quantlib.db import connect  # noqa: E402
from quantlib.industry_taxonomy import TAXONOMY_VERSION  # noqa: E402
from quantlib.prices import fetch_adjusted_panel  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
CACHE_DIR = RESULTS / "cache"
START = date(2005, 1, 3)
FEATURE_VERSION = f"iter75-dynamic-industry-v2-{TAXONOMY_VERSION}"
ELECTRONIC_INDUSTRIES = {
    "半導體業",
    "電子零組件業",
    "電腦及週邊設備業",
    "光電業",
    "通信網路業",
    "電子通路業",
    "資訊服務業",
    "其他電子業",
}


@dataclass(frozen=True)
class DynamicIndustryConfig:
    name: str
    score_kind: str
    max_positions: int
    industry_topn: int
    industry_cap: int
    rebalance: str
    min_stock_score: float
    min_industry_score: float
    min_adv: float = 30_000_000.0
    min_hold_days: int = 15
    exit_score: float = -0.10
    exit_industry_rank: int = 10
    trail_mult: float = 3.5
    trail_min: float = 0.10
    trail_max: float = 0.28
    risk_mode: str = "none"  # none | market_ma200_cash


@dataclass
class Position:
    shares: float
    entry_date: date
    high_water: float
    trail_pct: float
    industry: str | None


def log(msg: str) -> None:
    print(msg, flush=True)


def latest_trading_day(con) -> date:
    return con.sql(
        """
        SELECT MAX(date)
        FROM daily_quote
        WHERE market='twse' AND company_code='0050'
        """
    ).fetchone()[0]


def data_signature(con, start: date, end: date) -> dict[str, object]:
    daily_max, daily_n = con.sql(
        f"""
        SELECT MAX(date)::VARCHAR, COUNT(*)
        FROM daily_quote
        WHERE date BETWEEN DATE '{start}' - INTERVAL '320 days' AND DATE '{end}'
        """
    ).fetchone()
    rev_max, rev_n = con.sql("SELECT MAX(year * 100 + month), COUNT(*) FROM operating_revenue").fetchone()
    flow_max, flow_n = con.sql(
        f"""
        SELECT MAX(date)::VARCHAR, COUNT(*)
        FROM daily_trading_details
        WHERE date BETWEEN DATE '{start}' - INTERVAL '320 days' AND DATE '{end}'
        """
    ).fetchone()
    q_max, q_n = con.sql("SELECT MAX(year * 10 + quarter), COUNT(*) FROM raw_quarterly").fetchone()
    tax_max, tax_n = con.sql("SELECT MAX(effective_date)::VARCHAR, COUNT(*) FROM industry_taxonomy_pit").fetchone()
    return {
        "feature_version": FEATURE_VERSION,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "daily_quote": [daily_max, daily_n],
        "operating_revenue": [rev_max, rev_n],
        "industry_taxonomy_pit": [tax_max, tax_n],
        "daily_trading_details": [flow_max, flow_n],
        "raw_quarterly": [q_max, q_n],
    }


def cache_path(start: date, end: date) -> Path:
    digest = hashlib.sha1(f"{FEATURE_VERSION}|{start}|{end}".encode()).hexdigest()[:10]
    return CACHE_DIR / f"iter75_features_{start}_{end}_{digest}.parquet"


def read_meta(path: Path) -> dict[str, object] | None:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def trading_days(con, start: date, end: date) -> list[date]:
    return [
        row[0]
        for row in con.sql(
            f"""
            SELECT date FROM daily_quote
            WHERE market='twse' AND company_code='0050'
              AND date BETWEEN DATE '{start}' AND DATE '{end}'
            ORDER BY date
            """
        ).fetchall()
    ]


def load_or_build_panel(con, start: date, end: date, use_cache: bool = True) -> tuple[pl.DataFrame, list[date]]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cpath = cache_path(start, end)
    mpath = cpath.with_suffix(".json")
    sig = data_signature(con, start, end)
    days = trading_days(con, start, end)
    if use_cache and cpath.exists() and read_meta(mpath) == sig:
        log(f"[iter75] load feature cache: {cpath}")
        return pl.scan_parquet(cpath).collect(), days

    panel, _ = build_feature_panel(con, start, end)
    panel = add_dynamic_industry_features(panel).rechunk()
    if use_cache:
        tmp = cpath.with_suffix(".tmp.parquet")
        panel.write_parquet(tmp, compression="zstd", statistics=True)
        tmp.replace(cpath)
        mpath.write_text(json.dumps(sig, indent=2, sort_keys=True))
        log(f"[iter75] saved feature cache: {cpath}")
    return panel, days


def add_dynamic_industry_features(panel: pl.DataFrame) -> pl.DataFrame:
    base = panel.with_columns(
        [
            (
                0.40 * _clip_expr(pl.col("roa_ttm") / 0.12, -1.0, 2.0).fill_null(0.0)
                + 0.30 * _clip_expr(pl.col("gross_margin_ttm") / 0.35, -1.0, 2.0).fill_null(0.0)
                + 0.30 * _clip_expr((pl.col("f_score_raw") - 4.0) / 3.0, -1.0, 1.0).fill_null(0.0)
            ).alias("quality_score"),
            _clip_expr(pl.col("adv60").log10() - 7.5, -1.0, 1.0).fill_null(-1.0).alias("liquidity_score"),
            _clip_expr(pl.col("latest_yoy") / 60.0, -1.0, 2.0).fill_null(0.0).alias("s_rev75"),
            _clip_expr(pl.col("yoy_delta") / 60.0, -1.0, 2.0).fill_null(0.0).alias("s_accel75"),
            _clip_expr(pl.col("ret120") / 0.60, -1.0, 2.0).fill_null(0.0).alias("s_mom120_75"),
            _clip_expr(pl.col("ret60") / 0.35, -1.0, 2.0).fill_null(0.0).alias("s_mom60_75"),
            _clip_expr(pl.col("ret20") / 0.18, -1.0, 2.0).fill_null(0.0).alias("s_mom20_75"),
            _clip_expr(pl.col("inst_flow20") / 0.10, -1.0, 2.0).fill_null(0.0).alias("s_inst75"),
            _clip_expr((0.08 - pl.col("atr_pct")) / 0.05, -1.0, 1.0).fill_null(0.0).alias("s_risk75"),
        ]
    )
    tradable = (
        (~pl.col("is_etf").fill_null(False))
        & pl.col("industry").is_not_null()
        & (pl.col("listed_days") >= 252)
        & (pl.col("adv60") >= 20_000_000)
        & pl.col("ret120").is_finite()
    )
    ind = (
        base.filter(tradable)
        .group_by(["date", "industry"])
        .agg(
            [
                pl.len().alias("industry_n"),
                pl.col("ret120").median().alias("industry_ret120"),
                pl.col("ret60").median().alias("industry_ret60"),
                pl.col("latest_yoy").median().alias("industry_yoy"),
                pl.col("yoy_delta").median().alias("industry_accel"),
                pl.col("inst_flow20").median().alias("industry_flow20"),
                (pl.col("close") > pl.col("ma200")).mean().alias("industry_breadth"),
            ]
        )
        .filter(pl.col("industry_n") >= 5)
        .with_columns(
            [
                _clip_expr(pl.col("industry_ret120") / 0.50, -1.0, 2.0).fill_null(0.0).alias("s_ind_mom120"),
                _clip_expr(pl.col("industry_ret60") / 0.28, -1.0, 2.0).fill_null(0.0).alias("s_ind_mom60"),
                _clip_expr(pl.col("industry_yoy") / 40.0, -1.0, 2.0).fill_null(0.0).alias("s_ind_rev"),
                _clip_expr(pl.col("industry_accel") / 40.0, -1.0, 2.0).fill_null(0.0).alias("s_ind_accel"),
                _clip_expr(pl.col("industry_flow20") / 0.08, -1.0, 2.0).fill_null(0.0).alias("s_ind_flow"),
                ((pl.col("industry_breadth") - 0.50) * 2.0).clip(-1.0, 1.0).fill_null(0.0).alias("s_ind_breadth"),
            ]
        )
        .with_columns(
            (
                0.28 * pl.col("s_ind_mom120")
                + 0.18 * pl.col("s_ind_mom60")
                + 0.18 * pl.col("s_ind_breadth")
                + 0.16 * pl.col("s_ind_rev")
                + 0.10 * pl.col("s_ind_accel")
                + 0.10 * pl.col("s_ind_flow")
            ).alias("industry_score")
        )
        .with_columns(
            [
                pl.col("industry_score").rank("dense", descending=True).over("date").alias("industry_rank"),
                pl.col("industry_ret120").rank("dense", descending=True).over("date").alias("industry_mom_rank"),
                pl.col("industry_yoy").rank("dense", descending=True).over("date").alias("industry_rev_rank"),
            ]
        )
    )
    out = base.join(ind, on=["date", "industry"], how="left")
    return out.with_columns(
        [
            (
                0.22 * pl.col("s_mom120_75")
                + 0.14 * pl.col("s_mom60_75")
                + 0.14 * pl.col("s_mom20_75")
                + 0.16 * pl.col("s_rev75")
                + 0.10 * pl.col("s_accel75")
                + 0.10 * pl.col("s_inst75")
                + 0.08 * pl.col("quality_score")
                + 0.06 * pl.col("industry_score").fill_null(0.0)
            ).alias("score_balanced"),
            (
                0.30 * pl.col("s_mom120_75")
                + 0.22 * pl.col("s_mom60_75")
                + 0.18 * pl.col("s_mom20_75")
                + 0.12 * pl.col("s_inst75")
                + 0.10 * pl.col("industry_score").fill_null(0.0)
                + 0.08 * pl.col("s_risk75")
            ).alias("score_price"),
            (
                0.26 * pl.col("s_rev75")
                + 0.20 * pl.col("s_accel75")
                + 0.18 * pl.col("s_mom120_75")
                + 0.12 * pl.col("s_mom20_75")
                + 0.10 * pl.col("quality_score")
                + 0.08 * pl.col("s_inst75")
                + 0.06 * pl.col("industry_score").fill_null(0.0)
            ).alias("score_revenue"),
            (
                0.28 * pl.col("quality_score")
                + 0.20 * pl.col("s_mom120_75")
                + 0.16 * pl.col("s_rev75")
                + 0.12 * pl.col("s_inst75")
                + 0.12 * pl.col("s_risk75")
                + 0.08 * pl.col("liquidity_score")
                + 0.04 * pl.col("industry_score").fill_null(0.0)
            ).alias("score_quality"),
        ]
    )


def score_col(kind: str) -> str:
    mapping = {
        "balanced": "score_balanced",
        "price": "score_price",
        "revenue": "score_revenue",
        "quality": "score_quality",
    }
    return mapping[kind]


def signal_expr(rebalance: str) -> pl.Expr:
    if rebalance == "weekly":
        return pl.col("date").dt.weekday() == 5
    if rebalance == "monthly":
        month_key = pl.col("date").dt.year() * 100 + pl.col("date").dt.month()
        return pl.col("date") == pl.col("date").min().over(month_key)
    raise ValueError(rebalance)


def build_configs() -> list[DynamicIndustryConfig]:
    configs: list[DynamicIndustryConfig] = []
    for score in ("balanced", "price", "revenue", "quality"):
        for max_pos in (6, 8, 10):
            for ind_top in (3, 5):
                for ind_cap in (2, 3, 4):
                    if ind_cap > max_pos:
                        continue
                    for rebalance in ("weekly", "monthly"):
                        for risk in ("none", "market_ma200_cash"):
                            name = (
                                f"iter75_dynind_{score}_pos{max_pos}_ind{ind_top}"
                                f"_cap{ind_cap}_{rebalance}_{risk}"
                            )
                            configs.append(
                                DynamicIndustryConfig(
                                    name=name,
                                    score_kind=score,
                                    max_positions=max_pos,
                                    industry_topn=ind_top,
                                    industry_cap=ind_cap,
                                    rebalance=rebalance,
                                    min_stock_score=0.28 if score != "quality" else 0.22,
                                    min_industry_score=0.05,
                                    risk_mode=risk,
                                )
                            )
    return configs


def market_risk_flags(con, end: date) -> dict[date, bool]:
    px = (
        fetch_adjusted_panel(
            con,
            START.isoformat(),
            end.isoformat(),
            codes=["0050"],
            market="twse",
            include_extra_history_days=320,
        )
        .sort("date")
        .with_columns(pl.col("close").rolling_mean(200).alias("ma200"))
        .filter((pl.col("date") >= START) & (pl.col("date") <= end))
    )
    return {
        row["date"]: bool(row["close"] < row["ma200"]) if row["ma200"] is not None else False
        for row in px.iter_rows(named=True)
    }


def build_candidates(
    panel: pl.DataFrame,
    cfg: DynamicIndustryConfig,
    banned_industries: set[str] | None = None,
) -> dict[date, list[dict[str, object]]]:
    banned_industries = banned_industries or set()
    s = score_col(cfg.score_kind)
    base_filter = (
        (~pl.col("is_etf").fill_null(False))
        & pl.col("industry").is_not_null()
        & (~pl.col("industry").is_in(list(banned_industries)))
        & (pl.col("listed_days") >= 252)
        & (pl.col("adv60") >= cfg.min_adv)
        & (pl.col("open") > 0)
        & (pl.col("close") > 0)
        & pl.col("next_open").is_not_null()
        & pl.col("atr_pct").is_between(0.008, 0.12)
        & (pl.col("close") > pl.col("ma200"))
        & (pl.col("industry_rank") <= cfg.industry_topn)
        & (pl.col("industry_score") >= cfg.min_industry_score)
        & (pl.col(s) >= cfg.min_stock_score)
        & signal_expr(cfg.rebalance)
    )
    candidates = (
        panel.filter(base_filter)
        .with_columns(pl.col(s).alias("stock_score"))
        .sort(["date", "industry", "stock_score"], descending=[False, False, True])
        .with_columns(pl.col("stock_score").rank("ordinal", descending=True).over(["date", "industry"]).alias("rank_in_industry"))
        .filter(pl.col("rank_in_industry") <= cfg.industry_cap)
        .sort(["date", "stock_score"], descending=[False, True])
        .with_columns(pl.col("stock_score").rank("ordinal", descending=True).over("date").alias("rank_global"))
        .filter(pl.col("rank_global") <= max(cfg.max_positions * 2, cfg.max_positions + 4))
        .select(
            [
                "date",
                "company_code",
                "industry",
                "stock_score",
                "industry_rank",
                "industry_score",
                "latest_yoy",
                "ret120",
                "atr_pct",
            ]
        )
    )
    return {
        key[0] if isinstance(key, tuple) else key: g.to_dicts()
        for key, g in candidates.group_by("date", maintain_order=True)
    }


def build_row_store(panel: pl.DataFrame) -> dict[str, dict[date, dict[str, object]]]:
    cols = [
        "date",
        "company_code",
        "open",
        "close",
        "ma100",
        "ma200",
        "latest_yoy",
        "industry",
        "industry_rank",
        "industry_score",
        "score_balanced",
        "score_price",
        "score_revenue",
        "score_quality",
        "atr_pct",
    ]
    store: dict[str, dict[date, dict[str, object]]] = {}
    for key, frame in panel.select(cols).partition_by("company_code", as_dict=True, maintain_order=True).items():
        code = key[0] if isinstance(key, tuple) else key
        store[str(code)] = {row["date"]: row for row in frame.iter_rows(named=True)}
    return store


def safe_float(value: object, default: float = math.nan) -> float:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def valid_price(value: float) -> bool:
    return math.isfinite(value) and value > 0


def run_strategy(
    days: list[date],
    store: dict[str, dict[date, dict[str, object]]],
    candidates: dict[date, list[dict[str, object]]],
    risk_off: dict[date, bool],
    cfg: DynamicIndustryConfig,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, float]]:
    cash = CAPITAL
    positions: dict[str, Position] = {}
    pending_entries: list[dict[str, object]] = []
    pending_exits: set[str] = set()
    nav_rows = []
    hold_rows = []
    trades = 0
    max_active = 0
    score_name = score_col(cfg.score_kind)

    def row_at(d: date, code: str) -> dict[str, object] | None:
        return store.get(code, {}).get(d)

    def px_at(d: date, code: str, field: str) -> float:
        row = row_at(d, code)
        if row is None:
            return math.nan
        return safe_float(row.get(field))

    def mark_open_value(d: date, code: str, pos: Position) -> float:
        op = px_at(d, code, "open")
        if not valid_price(op):
            op = px_at(d, code, "close")
        return pos.shares * op if valid_price(op) else 0.0

    for d in days:
        risk_cash = cfg.risk_mode == "market_ma200_cash" and risk_off.get(d, False)
        if risk_cash:
            pending_exits.update(positions.keys())

        open_values: dict[str, float] = {}
        for code, pos in positions.items():
            value = mark_open_value(d, code, pos)
            if value > 0:
                open_values[code] = value
        nav_open = cash + sum(open_values.values())

        for code in list(pending_exits):
            pos = positions.pop(code, None)
            if pos is None:
                continue
            op = px_at(d, code, "open")
            if not valid_price(op):
                op = px_at(d, code, "close")
            if valid_price(op):
                cash += pos.shares * op * (1 - SELL_TAX - COMMISSION)
                trades += 1
        pending_exits.clear()

        if not risk_cash and pending_entries:
            current_by_ind: dict[str, int] = {}
            for pos in positions.values():
                if pos.industry:
                    current_by_ind[pos.industry] = current_by_ind.get(pos.industry, 0) + 1
            for cand in pending_entries:
                code = str(cand["company_code"])
                industry = cand.get("industry")
                if code in positions or len(positions) >= cfg.max_positions:
                    continue
                if industry and current_by_ind.get(str(industry), 0) >= cfg.industry_cap:
                    continue
                op = px_at(d, code, "open")
                if not valid_price(op):
                    continue
                nav_open = cash + sum(mark_open_value(d, c, p) for c, p in positions.items())
                slot_value = nav_open / cfg.max_positions
                buy_value = min(slot_value, cash / (1 + COMMISSION))
                if buy_value <= nav_open * 0.01:
                    continue
                atr = safe_float(cand.get("atr_pct"))
                trail = min(max((atr if math.isfinite(atr) else 0.04) * cfg.trail_mult, cfg.trail_min), cfg.trail_max)
                cash -= buy_value * (1 + COMMISSION)
                positions[code] = Position(
                    shares=buy_value / op,
                    entry_date=d,
                    high_water=op,
                    trail_pct=trail,
                    industry=str(industry) if industry is not None else None,
                )
                if industry:
                    current_by_ind[str(industry)] = current_by_ind.get(str(industry), 0) + 1
                trades += 1
        pending_entries = []

        nav_close = cash
        close_values: dict[str, float] = {}
        for code, pos in list(positions.items()):
            row = row_at(d, code)
            if row is None:
                pending_exits.add(code)
                continue
            close = safe_float(row.get("close"))
            if not valid_price(close):
                pending_exits.add(code)
                continue
            pos.high_water = max(pos.high_water, close)
            value = pos.shares * close
            close_values[code] = value
            nav_close += value

            held_days = (d - pos.entry_date).days
            ma100 = safe_float(row.get("ma100"))
            ma200 = safe_float(row.get("ma200"))
            yoy = safe_float(row.get("latest_yoy"))
            ind_rank = safe_float(row.get("industry_rank"))
            stock_score = safe_float(row.get(score_name))
            trail_fail = close / pos.high_water - 1.0 <= -pos.trail_pct
            trend_fail = close < ma100 if ma100 > 0 else False
            deep_trend_fail = close < ma200 if ma200 > 0 else False
            revenue_fail = math.isfinite(yoy) and yoy < -20.0
            score_fail = held_days >= cfg.min_hold_days and math.isfinite(stock_score) and stock_score < cfg.exit_score
            industry_fail = held_days >= cfg.min_hold_days and math.isfinite(ind_rank) and ind_rank > cfg.exit_industry_rank
            if trail_fail or deep_trend_fail or trend_fail or revenue_fail or score_fail or industry_fail:
                pending_exits.add(code)

        if nav_close > 0:
            ind_weights: dict[str, float] = {}
            for code, value in close_values.items():
                industry = positions[code].industry or "UNKNOWN"
                weight = value / nav_close
                ind_weights[industry] = ind_weights.get(industry, 0.0) + weight
                hold_rows.append(
                    {
                        "date": d,
                        "company_code": code,
                        "industry": industry,
                        "weight": weight,
                    }
                )
            max_ind_weight = max(ind_weights.values()) if ind_weights else 0.0
        else:
            max_ind_weight = 0.0

        max_active = max(max_active, len(positions))
        nav_rows.append({"date": d, "nav": nav_close, "active": len(positions), "max_industry_weight": max_ind_weight})

        if d in candidates and not risk_cash:
            held = set(positions) | {str(c["company_code"]) for c in pending_entries}
            current_by_ind: dict[str, int] = {}
            for pos in positions.values():
                if pos.industry:
                    current_by_ind[pos.industry] = current_by_ind.get(pos.industry, 0) + 1
            for cand in candidates[d]:
                code = str(cand["company_code"])
                industry = cand.get("industry")
                if code in held:
                    continue
                if industry and current_by_ind.get(str(industry), 0) >= cfg.industry_cap:
                    continue
                pending_entries.append(cand)
                held.add(code)
                if industry:
                    current_by_ind[str(industry)] = current_by_ind.get(str(industry), 0) + 1
                if len(held) >= cfg.max_positions:
                    break

    daily = pl.DataFrame(nav_rows)
    holdings = pl.DataFrame(hold_rows)
    stats = {
        "trade_days": float(trades),
        "max_active": float(max_active),
        "avg_turnover_trade_day": float("nan"),
        "avg_max_industry_weight": float(daily["max_industry_weight"].mean()) if daily.height else 0.0,
        "max_industry_weight": float(daily["max_industry_weight"].max()) if daily.height else 0.0,
    }
    return daily, holdings, stats


def recent_one_year(daily: pl.DataFrame) -> dict[str, object]:
    daily = daily.sort("date")
    dates = daily["date"].to_list()
    navs = daily["nav"].to_numpy().astype(float)
    end = dates[-1]
    candidates = [d for d in dates if d <= date(end.year - 1, end.month, end.day)]
    start = candidates[-1] if candidates else dates[0]
    nav_lookup = dict(zip(dates, navs, strict=True))
    return {
        "recent_1y_start": start,
        "recent_1y_end": end,
        "recent_1y_cagr": nav_lookup[end] / nav_lookup[start] - 1.0,
    }


def return_metrics(daily: pl.DataFrame, prefix: str = "") -> dict[str, float]:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy().astype(float)
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    full = metrics_from_rets(rets, dates)
    frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = frame.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_m = metrics_from_rets(oos["ret"].to_numpy(), oos["date"].to_list())
    recent = recent_one_year(daily)
    return {
        f"{prefix}cagr": full["cagr"],
        f"{prefix}sortino": full["sortino"],
        f"{prefix}mdd": full["mdd"],
        f"{prefix}oos_cagr": oos_m["cagr"],
        f"{prefix}oos_sortino": oos_m["sortino"],
        f"{prefix}oos_mdd": oos_m["mdd"],
        f"{prefix}recent_1y_cagr": recent["recent_1y_cagr"],
    }


def run_research(max_configs: int | None = None, validate_top_n: int = 12, use_cache: bool = True) -> None:
    t0 = time.time()
    con = connect(read_only=True)
    try:
        end = latest_trading_day(con)
        panel, days = load_or_build_panel(con, START, end, use_cache=use_cache)
        risk_off = market_risk_flags(con, end)
    finally:
        con.close()

    log(f"[iter75] panel rows={panel.height:,} codes={panel['company_code'].n_unique():,} end={end}")
    configs = build_configs()
    if max_configs is not None:
        configs = configs[:max_configs]
    config_path = RESULTS / "iter_75_dynamic_industry_configs.jsonl"
    with config_path.open("w") as f:
        for cfg in configs:
            f.write(json.dumps(asdict(cfg), ensure_ascii=False) + "\n")

    store = build_row_store(panel)
    log(f"[iter75] market risk-off days={sum(risk_off.values())}/{len(risk_off)}")
    rows = []
    dailies: list[tuple[DynamicIndustryConfig, pl.DataFrame, pl.DataFrame, dict[str, float | str]]] = []
    for i, cfg in enumerate(configs, 1):
        cfg_t0 = time.time()
        candidates = build_candidates(panel, cfg)
        if not candidates:
            continue
        daily, holdings, stats = run_strategy(days, store, candidates, risk_off, cfg)
        row = validate_daily_nav(cfg.name, daily.select(["date", "nav"]), n_trials=len(configs), extra=stats)
        row.update(recent_one_year(daily))
        row.update(
            {
                "score_kind": cfg.score_kind,
                "max_positions_cfg": float(cfg.max_positions),
                "industry_topn": float(cfg.industry_topn),
                "industry_cap": float(cfg.industry_cap),
                "rebalance": cfg.rebalance,
                "risk_mode": cfg.risk_mode,
                "candidate_signal_days": float(len(candidates)),
            }
        )
        rows.append(row)
        dailies.append((cfg, daily, holdings, row))
        log(
            f"[iter75] {i:03d}/{len(configs)} {cfg.name}: "
            f"OOS={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
            f"MDD={row['oos_mdd']:.2%} 1Y={row['recent_1y_cagr']:+.2%} "
            f"max_ind={stats['max_industry_weight']:.1%} ({time.time()-cfg_t0:.1f}s)"
        )

    if not rows:
        raise RuntimeError("no iter75 candidates produced trades")

    summary = pl.DataFrame(rows).sort(["robust_growth_score", "oos_log_cagr", "oos_cagr"], descending=[True, True, True])
    selected = set(summary.head(validate_top_n)["name"].to_list())
    stress_rows = []
    for cfg, daily, holdings, row in dailies:
        if cfg.name not in selected:
            continue
        for label, banned in [
            ("no_semiconductor", {"半導體業"}),
            ("no_electronics", ELECTRONIC_INDUSTRIES),
        ]:
            candidates = build_candidates(panel, cfg, banned_industries=banned)
            if candidates:
                stress_daily, _, stress_stats = run_strategy(days, store, candidates, risk_off, cfg)
                stress = return_metrics(stress_daily, prefix=f"{label}_")
                stress_rows.append({"name": cfg.name, "stress": label, **stress, **stress_stats})
            else:
                stress_rows.append({"name": cfg.name, "stress": label})
        daily.write_csv(RESULTS / f"{cfg.name}_daily.csv")
        holdings.write_csv(RESULTS / f"{cfg.name}_holdings.csv")

    stress_df = pl.DataFrame(stress_rows) if stress_rows else pl.DataFrame()
    if stress_df.height:
        wide = (
            stress_df.select(
                [
                    "name",
                    "stress",
                    "no_semiconductor_oos_cagr",
                    "no_semiconductor_oos_sortino",
                    "no_semiconductor_oos_mdd",
                    "no_semiconductor_recent_1y_cagr",
                    "no_electronics_oos_cagr",
                    "no_electronics_oos_sortino",
                    "no_electronics_oos_mdd",
                    "no_electronics_recent_1y_cagr",
                ]
            )
            .group_by("name")
            .agg(pl.all().drop_nulls().first())
        )
        summary = summary.join(wide, on="name", how="left")

    out = RESULTS / "iter_75_dynamic_industry_summary.csv"
    stress_out = RESULTS / "iter_75_dynamic_industry_stress.csv"
    summary.write_csv(out)
    if stress_df.height:
        stress_df.write_csv(stress_out)

    top_cols = [
        "name",
        "score_kind",
        "cagr",
        "oos_cagr",
        "recent_1y_cagr",
        "oos_sortino",
        "oos_mdd",
        "oos_cdar_95",
        "oos_ulcer_index",
        "oos_k_ratio",
        "robust_growth_score",
        "boot_cagr_lb",
        "dsr",
        "pbo",
        "max_active",
        "avg_max_industry_weight",
        "max_industry_weight",
        "no_semiconductor_oos_cagr",
        "no_electronics_oos_cagr",
    ]
    existing = [c for c in top_cols if c in summary.columns]
    print("\niter_75 dynamic industry leadership")
    print(summary.select(existing).head(20).to_pandas().to_string(index=False))
    log(f"\nSaved: {out}")
    if stress_df.height:
        log(f"Saved: {stress_out}")
    log(f"Saved selected daily/holdings: {len(selected)}")
    log(f"[iter75] elapsed={time.time()-t0:.1f}s")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-configs", type=int, default=None)
    ap.add_argument("--validate-top-n", type=int, default=12)
    ap.add_argument("--no-cache", action="store_true")
    args = ap.parse_args()
    run_research(max_configs=args.max_configs, validate_top_n=args.validate_top_n, use_cache=not args.no_cache)


if __name__ == "__main__":
    main()
