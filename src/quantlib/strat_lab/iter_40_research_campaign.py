"""iter_40 - broad professional research campaign.

This script is the campaign harness for trying many new alpha families under
one production-sane simulator:
  - adjusted total-return OHLCV from the cached iter_33 panel;
  - signals known after close, executed next open;
  - long-only, no leverage, <=10 simultaneous stock positions;
  - transaction costs included;
  - OOS/DSR/PBO validation for every candidate.

Generation 1 intentionally focuses on deterministic, monotonic score families:
quality compounders, revenue/volume breakouts, spike preconditions, industry
regimes, and market-risk overlays. Heavy ML should enter only after these
families show where the edge actually lives.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from quantlib.db import connect  # noqa: E402
from quantlib.prices import fetch_adjusted_panel  # noqa: E402

from validate_hybrid import (  # noqa: E402
    N_TRIALS_DSR,
    TDPY,
    bootstrap_ci,
    deflated_sharpe,
    lo_2002_sharpe_test,
    pbo_cscv,
    walk_forward_folds,
)
from iter_33_pm_first_principles import (  # noqa: E402
    load_or_build_panel as load_or_build_iter33_panel,
    panel_cache_path as iter33_panel_cache_path,
)


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
CACHE_DIR = RESULTS / "cache"
CAPITAL = 1_000_000.0
COMMISSION = 0.000285
SELL_TAX = 0.003
RF = 0.01
START = date(2005, 1, 3)
EVENT_FAMILIES = {"breakout", "breakout_risk", "spike_precondition"}


def latest_trading_day() -> date:
    env_end = os.environ.get("QL_STRAT_END")
    if env_end:
        return date.fromisoformat(env_end)
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


END = latest_trading_day()


@dataclass(frozen=True)
class CampaignConfig:
    name: str
    family: str
    score_kind: str
    topn: int
    rebalance: str = "daily"       # daily | weekly | monthly
    persist: bool = False
    gross: float = 1.0
    risk_mode: str = "none"        # none | ma200_half | ma200_cash | dd_half
    min_adv: float = 50_000_000.0
    min_yoy: float | None = None
    min_yoy_delta: float | None = None
    min_roa: float | None = None
    min_gm: float | None = None
    min_fscore: int | None = None
    min_inst_flow: float | None = None
    breakout_lkb: int | None = None
    breakout_ratio: float = 1.0
    vol_mult: float | None = None
    min_ret120: float | None = None
    max_ret120: float | None = None
    max_atr: float | None = None
    require_trend: bool = False
    industry_topn: int | None = None


def log(msg: str) -> None:
    print(msg, flush=True)


def latest_feature_cache(end: date = END) -> Path:
    con = connect(read_only=True)
    try:
        load_or_build_iter33_panel(con, START, end, use_cache=True)
    finally:
        con.close()
    expected = iter33_panel_cache_path(START, end)
    if expected.exists():
        return expected
    files = sorted(CACHE_DIR.glob(f"iter33_features_{START}_{end}_*.parquet"))
    if not files:
        raise FileNotFoundError(f"missing iter33 feature cache for {START} -> {end}")
    return max(files, key=lambda p: p.stat().st_mtime)


def z_expr(col: str, lo: float, hi: float) -> pl.Expr:
    return ((pl.col(col) - lo) / (hi - lo) * 2 - 1).clip(-1.0, 1.0)


def fetch_industry_map() -> pl.DataFrame:
    con = connect()
    try:
        return con.sql(
            """
            SELECT DISTINCT ON (company_code) company_code, industry
            FROM operating_revenue
            WHERE industry IS NOT NULL
            ORDER BY company_code, year DESC, month DESC
            """
        ).pl()
    finally:
        con.close()


def fetch_market_calendar(end: date = END) -> tuple[list[date], dict[str, dict[date, float]]]:
    con = connect()
    try:
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
    finally:
        con.close()
    days = px["date"].to_list()
    return days, {
        "close": dict(zip(days, px["close"].to_list(), strict=True)),
        "ma200": dict(zip(days, px["ma200"].to_list(), strict=True)),
    }


def load_panel(end: date = END) -> tuple[pl.DataFrame, list[date], dict[str, dict[date, float]]]:
    t0 = time.time()
    path = latest_feature_cache(end)
    log(f"[iter40] load feature cache: {path}")
    cols = [
        "date",
        "company_code",
        "open",
        "close",
        "vol",
        "adv60",
        "vol_avg60",
        "hi60",
        "hi90",
        "hi120",
        "ma50",
        "ma100",
        "ma200",
        "atr_pct",
        "ret120",
        "latest_yoy",
        "yoy_delta",
        "inst_flow20",
        "roa_ttm",
        "gross_margin_ttm",
        "f_score_raw",
        "is_etf",
        "is_finance",
        "listed_days",
        "core_score",
        "sat_score",
        "rank_score",
    ]
    industry = fetch_industry_map()
    panel = (
        pl.scan_parquet(path)
        .select(cols)
        .filter((pl.col("date") >= START) & (pl.col("date") <= end))
        .collect()
        .join(industry, on="company_code", how="left")
        .sort(["company_code", "date"])
    )
    panel = panel.with_columns(
        [
            (pl.col("vol") / pl.col("vol_avg60")).alias("vol_ratio"),
            (pl.col("close") / pl.col("hi60")).alias("br60"),
            (pl.col("close") / pl.col("hi90")).alias("br90"),
            (pl.col("close") / pl.col("hi120")).alias("br120"),
            (pl.col("close") / pl.col("ma50") - 1).alias("trend50"),
            (pl.col("close") / pl.col("ma200") - 1).alias("trend200"),
            (pl.col("adv60").log10() - 7.5).clip(-1.0, 1.0).alias("liq_score"),
            (
                0.42 * (pl.col("roa_ttm") / 0.12).clip(-1.0, 2.0)
                + 0.32 * (pl.col("gross_margin_ttm") / 0.35).clip(-1.0, 2.0)
                + 0.26 * ((pl.col("f_score_raw") - 4.0) / 3.0).clip(-1.0, 1.0)
            ).alias("quality_score"),
        ]
    )
    panel = panel.with_columns(
        [
            z_expr("latest_yoy", -20.0, 80.0).alias("z_yoy"),
            z_expr("yoy_delta", -40.0, 80.0).alias("z_yoy_delta"),
            z_expr("ret120", -0.30, 0.80).alias("z_ret120"),
            z_expr("vol_ratio", 0.5, 3.0).alias("z_vol_ratio"),
            z_expr("inst_flow20", -0.10, 0.20).alias("z_inst"),
            (-pl.col("atr_pct") / 0.08).clip(-1.0, 0.0).alias("z_low_atr"),
        ]
    )
    panel = panel.with_columns(
        [
            (
                0.30 * pl.col("sat_score")
                + 0.22 * pl.col("z_yoy_delta")
                + 0.18 * pl.col("z_vol_ratio")
                + 0.18 * pl.col("z_ret120")
                + 0.12 * pl.col("z_inst")
            ).alias("spike_score"),
            (
                0.34 * pl.col("quality_score")
                + 0.22 * pl.col("core_score")
                + 0.18 * pl.col("z_ret120")
                + 0.14 * pl.col("z_inst")
                + 0.12 * pl.col("z_low_atr")
            ).alias("defensive_quality_score"),
            (
                0.36 * pl.col("z_yoy")
                + 0.26 * pl.col("z_yoy_delta")
                + 0.18 * pl.col("z_vol_ratio")
                + 0.12 * pl.col("z_ret120")
                + 0.08 * pl.col("quality_score")
            ).alias("rev_accel_score"),
        ]
    )
    industry_stats = (
        panel.filter(pl.col("industry").is_not_null())
        .group_by(["date", "industry"])
        .agg(
            [
                pl.col("ret120").median().alias("industry_ret120"),
                pl.col("latest_yoy").median().alias("industry_yoy"),
                pl.len().alias("industry_n"),
            ]
        )
        .with_columns(
            [
                pl.col("industry_ret120").rank("dense", descending=True).over("date").alias("industry_mom_rank"),
                pl.col("industry_yoy").rank("dense", descending=True).over("date").alias("industry_rev_rank"),
            ]
        )
    )
    panel = panel.join(industry_stats, on=["date", "industry"], how="left")
    panel = panel.with_columns(
        [
            z_expr("industry_ret120", -0.20, 0.60).alias("z_ind_mom"),
            z_expr("industry_yoy", -10.0, 50.0).alias("z_ind_rev"),
        ]
    ).with_columns(
        [
            (
                0.34 * pl.col("sat_score")
                + 0.22 * pl.col("z_ind_mom").fill_null(0.0)
                + 0.18 * pl.col("z_ind_rev").fill_null(0.0)
                + 0.14 * pl.col("z_yoy")
                + 0.12 * pl.col("z_ret120")
            ).alias("industry_catalyst_score"),
            (
                0.32 * pl.col("quality_score")
                + 0.24 * pl.col("core_score")
                + 0.20 * pl.col("z_ind_mom").fill_null(0.0)
                + 0.12 * pl.col("z_ind_rev").fill_null(0.0)
                + 0.12 * pl.col("z_low_atr")
            ).alias("industry_quality_score"),
        ]
    )

    days, market = fetch_market_calendar(end)
    log(f"[iter40] panel rows={panel.height:,}, codes={panel['company_code'].n_unique():,}, elapsed={time.time()-t0:.1f}s")
    return panel, days, market


def score_expr(kind: str) -> pl.Expr:
    if kind == "core":
        return pl.col("core_score")
    if kind == "sat":
        return pl.col("sat_score")
    if kind == "quality":
        return pl.col("quality_score")
    if kind == "defensive_quality":
        return pl.col("defensive_quality_score")
    if kind == "spike":
        return pl.col("spike_score")
    if kind == "rev_accel":
        return pl.col("rev_accel_score")
    if kind == "industry_catalyst":
        return pl.col("industry_catalyst_score")
    if kind == "industry_quality":
        return pl.col("industry_quality_score")
    raise ValueError(f"unknown score kind: {kind}")


def build_configs() -> list[CampaignConfig]:
    configs: list[CampaignConfig] = []

    for topn in (3, 5, 7, 10):
        for score in ("quality", "defensive_quality", "core"):
            configs.append(
                CampaignConfig(
                    name=f"quality_{score}_top{topn}_monthly",
                    family="quality_compounder",
                    score_kind=score,
                    topn=topn,
                    rebalance="monthly",
                    persist=True,
                    min_roa=0.08,
                    min_gm=0.20,
                    min_fscore=4,
                    require_trend=False,
                )
            )
            configs.append(
                CampaignConfig(
                    name=f"quality_{score}_top{topn}_monthly_riskhalf",
                    family="quality_risk",
                    score_kind=score,
                    topn=topn,
                    rebalance="monthly",
                    persist=True,
                    min_roa=0.08,
                    min_gm=0.20,
                    min_fscore=4,
                    risk_mode="ma200_half",
                )
            )

    for topn in (3, 5, 7, 10):
        for lkb in (60, 90, 120):
            for yoy, vol in ((20, 1.2), (30, 1.5), (50, 1.5), (30, 2.0)):
                configs.append(
                    CampaignConfig(
                        name=f"breakout_top{topn}_l{lkb}_y{yoy}_v{vol:g}",
                        family="breakout",
                        score_kind="rev_accel",
                        topn=topn,
                        min_yoy=float(yoy),
                        breakout_lkb=lkb,
                        breakout_ratio=0.98,
                        vol_mult=float(vol),
                        min_roa=0.02,
                        min_gm=0.10,
                        min_fscore=3,
                        max_atr=0.10,
                    )
                )

    for topn in (3, 5, 7):
        for dy in (10, 30, 50):
            for vol in (1.2, 1.8):
                configs.append(
                    CampaignConfig(
                        name=f"spike_top{topn}_dy{dy}_v{vol:g}",
                        family="spike_precondition",
                        score_kind="spike",
                        topn=topn,
                        min_yoy=10.0,
                        min_yoy_delta=float(dy),
                        vol_mult=float(vol),
                        min_ret120=0.0,
                        max_ret120=1.5,
                        max_atr=0.14,
                        require_trend=True,
                    )
                )

    for topn in (3, 5, 7, 10):
        for score in ("industry_catalyst", "industry_quality"):
            for ind_top in (3, 5):
                configs.append(
                    CampaignConfig(
                        name=f"industry_{score}_top{topn}_ind{ind_top}_weekly",
                        family="industry_regime",
                        score_kind=score,
                        topn=topn,
                        rebalance="weekly",
                        persist=True,
                        min_yoy=0.0 if score == "industry_catalyst" else None,
                        min_roa=0.04,
                        min_gm=0.12,
                        min_fscore=3,
                        require_trend=True,
                        industry_topn=ind_top,
                    )
                )

    for topn in (3, 5, 7):
        for risk_mode in ("ma200_half", "ma200_cash", "dd_half"):
            configs.append(
                CampaignConfig(
                    name=f"breakout_risk_{risk_mode}_top{topn}",
                    family="breakout_risk",
                    score_kind="rev_accel",
                    topn=topn,
                    risk_mode=risk_mode,
                    min_yoy=30.0,
                    breakout_lkb=90,
                    breakout_ratio=0.98,
                    vol_mult=1.5,
                    min_roa=0.02,
                    min_gm=0.10,
                    min_fscore=3,
                    max_atr=0.10,
                )
            )

    return configs


def signal_dates_expr(rebalance: str) -> pl.Expr:
    if rebalance == "daily":
        return pl.lit(True)
    if rebalance == "weekly":
        return pl.col("date").dt.weekday() == 5  # Friday close, next open.
    if rebalance == "monthly":
        month_key = pl.col("date").dt.year() * 100 + pl.col("date").dt.month()
        return pl.col("date") == pl.col("date").min().over(month_key)
    raise ValueError(f"unknown rebalance: {rebalance}")


def candidate_filter(cfg: CampaignConfig) -> pl.Expr:
    expr = (
        (~pl.col("is_etf"))
        & (~pl.col("is_finance"))
        & (pl.col("listed_days") >= 90)
        & (pl.col("adv60") >= cfg.min_adv)
        & (pl.col("open") > 0)
        & (pl.col("close") > 0)
    )
    if cfg.min_yoy is not None:
        expr &= pl.col("latest_yoy") >= cfg.min_yoy
    if cfg.min_yoy_delta is not None:
        expr &= pl.col("yoy_delta") >= cfg.min_yoy_delta
    if cfg.min_roa is not None:
        expr &= pl.col("roa_ttm") >= cfg.min_roa
    if cfg.min_gm is not None:
        expr &= pl.col("gross_margin_ttm") >= cfg.min_gm
    if cfg.min_fscore is not None:
        expr &= pl.col("f_score_raw") >= cfg.min_fscore
    if cfg.min_inst_flow is not None:
        expr &= pl.col("inst_flow20") >= cfg.min_inst_flow
    if cfg.breakout_lkb is not None:
        expr &= pl.col(f"br{cfg.breakout_lkb}") >= cfg.breakout_ratio
    if cfg.vol_mult is not None:
        expr &= pl.col("vol_ratio") >= cfg.vol_mult
    if cfg.min_ret120 is not None:
        expr &= pl.col("ret120") >= cfg.min_ret120
    if cfg.max_ret120 is not None:
        expr &= pl.col("ret120") <= cfg.max_ret120
    if cfg.max_atr is not None:
        expr &= pl.col("atr_pct") <= cfg.max_atr
    if cfg.require_trend:
        expr &= pl.col("trend200") > 0
    if cfg.industry_topn is not None:
        expr &= (pl.col("industry_mom_rank") <= cfg.industry_topn) | (pl.col("industry_rev_rank") <= cfg.industry_topn)
    return expr & signal_dates_expr(cfg.rebalance)


def build_targets(panel: pl.DataFrame, days: list[date], cfg: CampaignConfig) -> dict[date, dict[str, float]]:
    score = "__score"
    candidates = (
        panel.filter(candidate_filter(cfg))
        .with_columns(score_expr(cfg.score_kind).fill_null(-999.0).alias(score))
        .filter(pl.col(score).is_finite())
        .sort(["date", score, "company_code"], descending=[False, True, False])
        .select(["date", "company_code", score])
    )
    day_to_next = {days[i]: days[i + 1] for i in range(len(days) - 1)}
    targets: dict[date, dict[str, float]] = {}
    for d, sub in candidates.group_by("date", maintain_order=True):
        signal_d = d[0] if isinstance(d, tuple) else d
        exec_d = day_to_next.get(signal_d)
        if exec_d is None or sub.is_empty():
            continue
        sub = sub.head(cfg.topn)
        codes = sub["company_code"].to_list()
        scores = np.asarray(sub[score].to_numpy(), dtype=float)
        scores = scores - np.nanmin(scores)
        if np.isfinite(scores).all() and scores.sum() > 0 and cfg.family not in {"quality_compounder", "quality_risk"}:
            weights = scores / scores.sum() * cfg.gross
        else:
            weights = np.full(len(codes), cfg.gross / len(codes))
        targets[exec_d] = {c: float(w) for c, w in zip(codes, weights, strict=True)}
    return targets


def build_event_candidates(panel: pl.DataFrame, cfg: CampaignConfig) -> dict[date, list[str]]:
    score = "__score"
    topn = max(cfg.topn * 3, cfg.topn + 5)
    candidates = (
        panel.filter(candidate_filter(cfg))
        .with_columns(score_expr(cfg.score_kind).fill_null(-999.0).alias(score))
        .filter(pl.col(score).is_finite())
        .sort(["date", score, "company_code"], descending=[False, True, False])
        .select(["date", "company_code"])
    )
    return {
        (d[0] if isinstance(d, tuple) else d): sub.head(topn)["company_code"].to_list()
        for d, sub in candidates.group_by("date", maintain_order=True)
    }


def risk_multipliers(days: list[date], market: dict[str, dict[date, float]], mode: str) -> dict[date, float]:
    if mode == "none":
        return {d: 1.0 for d in days}
    out = {}
    peak = 0.0
    prev_risk_off = False
    for d in days:
        if not prev_risk_off:
            out[d] = 1.0
        elif mode == "ma200_cash":
            out[d] = 0.0
        else:
            out[d] = 0.5

        close = market["close"].get(d)
        ma200 = market["ma200"].get(d)
        peak = max(peak, close or peak)
        risk_off = False
        if mode in {"ma200_half", "ma200_cash"} and close and ma200:
            risk_off = close < ma200
        elif mode == "dd_half" and close and peak:
            risk_off = close / peak - 1 < -0.20
        prev_risk_off = risk_off
    return out


def build_price_lookup(panel: pl.DataFrame, codes: set[str]) -> dict[tuple[date, str], tuple[float, float]]:
    px = panel.filter(pl.col("company_code").is_in(list(codes))).select(["date", "company_code", "open", "close"])
    return {
        (r["date"], r["company_code"]): (float(r["open"]), float(r["close"]))
        for r in px.iter_rows(named=True)
    }


def build_event_lookup(panel: pl.DataFrame, codes: set[str]) -> dict[tuple[date, str], dict[str, float]]:
    px = panel.filter(pl.col("company_code").is_in(list(codes))).select(
        ["date", "company_code", "open", "close", "ma200", "latest_yoy", "atr_pct"]
    )
    return {
        (r["date"], r["company_code"]): {
            "open": float(r["open"]),
            "close": float(r["close"]),
            "ma200": float(r["ma200"]) if r["ma200"] is not None else math.nan,
            "latest_yoy": float(r["latest_yoy"]) if r["latest_yoy"] is not None else math.nan,
            "atr_pct": float(r["atr_pct"]) if r["atr_pct"] is not None else math.nan,
        }
        for r in px.iter_rows(named=True)
    }


class EventStore:
    """Lazy per-code row cache for event lifecycle simulations."""

    def __init__(self, panel: pl.DataFrame) -> None:
        cols = ["date", "company_code", "open", "close", "ma200", "latest_yoy", "atr_pct"]
        self.frames = panel.select(cols).partition_by("company_code", as_dict=True, maintain_order=True)
        self.rows: dict[str, dict[date, dict[str, float]]] = {}

    def row(self, d: date, code: str) -> dict[str, float] | None:
        if code not in self.rows:
            frame = self.frames.get(code)
            if frame is None:
                frame = self.frames.get((code,))
            if frame is None:
                self.rows[code] = {}
            else:
                self.rows[code] = {
                    r["date"]: {
                        "open": float(r["open"]) if r["open"] is not None else math.nan,
                        "close": float(r["close"]) if r["close"] is not None else math.nan,
                        "ma200": float(r["ma200"]) if r["ma200"] is not None else math.nan,
                        "latest_yoy": float(r["latest_yoy"]) if r["latest_yoy"] is not None else math.nan,
                        "atr_pct": float(r["atr_pct"]) if r["atr_pct"] is not None else math.nan,
                    }
                    for r in frame.iter_rows(named=True)
                }
        return self.rows[code].get(d)


def simulate(
    days: list[date],
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    targets: dict[date, dict[str, float]],
    risk_mult: dict[date, float],
    persist: bool,
) -> tuple[pl.DataFrame, dict[str, float]]:
    cash = CAPITAL
    shares: dict[str, float] = {}
    last_close: dict[str, float] = {}
    active_target: dict[str, float] = {}
    nav_rows = []
    turnover_sum = 0.0
    trade_days = 0
    max_active = 0
    last_gross_mult = 1.0

    for d in days:
        open_values: dict[str, float] = {}
        for code, qty in sorted(shares.items()):
            op, cl = price_lookup.get((d, code), (last_close.get(code, 0.0), last_close.get(code, 0.0)))
            if op <= 0 or cl <= 0:
                op = cl = last_close.get(code, 0.0)
            open_values[code] = qty * op
            last_close[code] = cl
        nav_open = cash + sum(open_values.values())

        gross_mult = risk_mult.get(d, 1.0)
        rebalance_today = False
        if d in targets:
            active_target = targets[d]
            rebalance_today = True
        elif not persist:
            active_target = {}
            rebalance_today = True
        elif abs(gross_mult - last_gross_mult) > 1e-9:
            rebalance_today = True
        last_gross_mult = gross_mult
        target = {c: w * gross_mult for c, w in active_target.items()}

        turnover = 0.0
        if rebalance_today:
            all_codes = sorted(set(shares) | set(target))
            target_values = {c: nav_open * target.get(c, 0.0) for c in all_codes}
            deltas = {c: target_values[c] - open_values.get(c, 0.0) for c in all_codes}
            turnover = sum(abs(v) for v in deltas.values()) / max(nav_open, 1e-9)

            for code, delta in list(deltas.items()):
                if delta >= 0 or code not in shares:
                    continue
                op, _ = price_lookup.get((d, code), (last_close.get(code, 0.0), last_close.get(code, 0.0)))
                if op <= 0:
                    continue
                sell_value = min(-delta, shares[code] * op)
                sell_shares = sell_value / op
                shares[code] -= sell_shares
                cash += sell_value * (1 - SELL_TAX - COMMISSION)
                if shares.get(code, 0.0) <= 1e-9:
                    shares.pop(code, None)

            buy_demand = [(c, v) for c, v in deltas.items() if v > 0]
            needed = sum(v * (1 + COMMISSION) for _, v in buy_demand)
            scale = min(1.0, cash / needed) if needed > 0 else 1.0
            for code, delta in buy_demand:
                op, cl = price_lookup.get((d, code), (0.0, 0.0))
                if op <= 0 or cl <= 0:
                    continue
                buy_value = delta * scale
                cost = buy_value * (1 + COMMISSION)
                if cost > cash + 1e-6:
                    continue
                shares[code] = shares.get(code, 0.0) + buy_value / op
                cash -= cost
                last_close[code] = cl

        close_nav = cash
        for code, qty in sorted(shares.items()):
            _, cl = price_lookup.get((d, code), (last_close.get(code, 0.0), last_close.get(code, 0.0)))
            if cl <= 0:
                cl = last_close.get(code, 0.0)
            close_nav += qty * cl
            last_close[code] = cl

        if turnover > 1e-8:
            turnover_sum += turnover
            trade_days += 1
        max_active = max(max_active, len(shares))
        nav_rows.append({"date": d, "nav": close_nav, "active": len(shares), "turnover": turnover})

    daily = pl.DataFrame(nav_rows)
    stats = {
        "avg_turnover_trade_day": turnover_sum / trade_days if trade_days else 0.0,
        "trade_days": float(trade_days),
        "max_active": float(max_active),
    }
    return daily, stats


def simulate_event_lifecycle(
    days: list[date],
    store: EventStore,
    candidates: dict[date, list[str]],
    risk_mult: dict[date, float],
    cfg: CampaignConfig,
) -> tuple[pl.DataFrame, dict[str, float]]:
    cash = CAPITAL
    positions: dict[str, dict[str, float | date]] = {}
    pending_entries: list[str] = []
    pending_exits: set[str] = set()
    nav_rows = []
    trades = 0
    max_active = 0

    def value_at(d: date, code: str, px_col: str) -> float:
        pos = positions[code]
        row = store.row(d, code)
        px = row.get(px_col) if row else None
        if px is None or px <= 0 or not math.isfinite(px):
            px = float(pos.get("last_close", 0.0))
        return float(pos["shares"]) * px

    for d in days:
        gross_mult = risk_mult.get(d, 1.0)

        if gross_mult <= 0:
            pending_exits.update(positions.keys())

        for code in sorted(pending_exits):
            pos = positions.pop(code, None)
            if pos is None:
                continue
            row = store.row(d, code)
            px = row.get("open") if row else None
            if px is None or px <= 0 or not math.isfinite(px):
                px = float(pos.get("last_close", 0.0))
            if px <= 0:
                continue
            cash += float(pos["shares"]) * px * (1 - SELL_TAX - COMMISSION)
            trades += 1
        pending_exits.clear()

        nav_open = cash + sum(value_at(d, code, "open") for code in sorted(positions))
        if gross_mult > 0:
            slot_value = nav_open * cfg.gross * gross_mult / cfg.topn
            for code in pending_entries:
                if len(positions) >= cfg.topn or code in positions:
                    continue
                row = store.row(d, code)
                if not row:
                    continue
                px = row["open"]
                if px <= 0 or not math.isfinite(px):
                    continue
                buy_value = min(slot_value, cash / (1 + COMMISSION))
                if buy_value <= nav_open * 0.01:
                    continue
                cash -= buy_value * (1 + COMMISSION)
                shares = buy_value / px
                atr_pct = row.get("atr_pct", math.nan)
                trail_pct = min(max((atr_pct if math.isfinite(atr_pct) else 0.04) * 3.0, 0.10), 0.25)
                positions[code] = {
                    "shares": shares,
                    "entry_px": px,
                    "high_water": px,
                    "trail_pct": trail_pct,
                    "last_close": px,
                    "entry_date": d,
                }
                trades += 1
        pending_entries = []

        nav_close = cash
        for code, pos in sorted(positions.items()):
            row = store.row(d, code)
            close = row.get("close") if row else None
            if close is None or close <= 0 or not math.isfinite(close):
                close = float(pos.get("last_close", 0.0))
            if close <= 0:
                pending_exits.add(code)
                continue
            pos["last_close"] = close
            pos["high_water"] = max(float(pos["high_water"]), close)
            nav_close += float(pos["shares"]) * close
            ma200 = row.get("ma200", math.nan) if row else math.nan
            yoy = row.get("latest_yoy", math.nan) if row else math.nan
            trail = close / float(pos["high_water"]) - 1 <= -float(pos["trail_pct"])
            trend_fail = math.isfinite(ma200) and close < ma200
            rev_fail = math.isfinite(yoy) and yoy < (0.0 if cfg.family != "spike_precondition" else -20.0)
            if trail or trend_fail or rev_fail:
                pending_exits.add(code)

        if gross_mult > 0 and len(positions) < cfg.topn:
            for code in candidates.get(d, []):
                if code not in positions and code not in pending_entries:
                    pending_entries.append(code)
                if len(positions) + len(pending_entries) >= cfg.topn:
                    break

        max_active = max(max_active, len(positions))
        nav_rows.append({"date": d, "nav": nav_close, "active": len(positions), "turnover": 0.0})

    daily = pl.DataFrame(nav_rows)
    stats = {
        "avg_turnover_trade_day": 0.0,
        "trade_days": float(trades),
        "max_active": float(max_active),
    }
    return daily, stats


def metrics_from_rets(rets: np.ndarray, dates: list[date]) -> dict[str, float]:
    if len(rets) < 2:
        return {"cagr": 0.0, "sortino": 0.0, "sharpe": 0.0, "mdd": 0.0, "final_nav": CAPITAL}
    nav = CAPITAL * np.cumprod(1 + rets)
    years = max((dates[-1] - dates[0]).days / 365.25, len(rets) / TDPY, 1e-9)
    cagr = (nav[-1] / CAPITAL) ** (1 / years) - 1
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY))
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    peak = CAPITAL
    mdd = 0.0
    for v in nav:
        peak = max(peak, float(v))
        mdd = min(mdd, (float(v) - peak) / peak)
    return {
        "cagr": float(cagr),
        "sortino": float((cagr - RF) / downvol) if downvol > 0 else 0.0,
        "sharpe": float((cagr - RF) / vol) if vol > 0 else 0.0,
        "mdd": float(mdd),
        "final_nav": float(nav[-1]),
    }


def validate_daily(name: str, daily: pl.DataFrame, n_trials: int, extra: dict[str, float]) -> dict[str, float | str]:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    full = metrics_from_rets(rets, dates)
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = df.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_rets = oos["ret"].to_numpy()
    oos_dates = oos["date"].to_list()
    oos_metrics = metrics_from_rets(oos_rets, oos_dates)
    lo = lo_2002_sharpe_test(oos_rets)
    boot = bootstrap_ci(oos_rets, oos_dates)
    dsr = deflated_sharpe(oos_metrics["sharpe"], max(N_TRIALS_DSR, n_trials), oos_rets)
    pbo = pbo_cscv(walk_forward_folds(rets, dates))
    return {
        "name": name,
        **full,
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_sharpe": oos_metrics["sharpe"],
        "oos_mdd": oos_metrics["mdd"],
        "lo_p": lo["p_value"],
        "boot_cagr_lb": boot["cagr_lb"],
        "boot_sortino_lb": boot["sortino_lb"],
        "dsr": dsr,
        "pbo": pbo,
        **extra,
    }


def run_campaign(max_configs: int | None = None, write_top_n: int = 12) -> None:
    t0 = time.time()
    panel, days, market = load_panel()
    configs = build_configs()
    if max_configs is not None:
        configs = configs[:max_configs]
    log(f"[iter40] generation configs={len(configs)}")

    config_path = RESULTS / "iter_40_campaign_configs.jsonl"
    with config_path.open("w") as f:
        for cfg in configs:
            f.write(json.dumps(asdict(cfg), ensure_ascii=False) + "\n")

    rows = []
    dailies: list[tuple[CampaignConfig, pl.DataFrame, dict[str, float]]] = []
    event_store: EventStore | None = None
    for i, cfg in enumerate(configs, 1):
        cfg_t0 = time.time()
        risk = risk_multipliers(days, market, cfg.risk_mode)
        if cfg.family in EVENT_FAMILIES:
            if event_store is None:
                log("[iter40] build event row store ...")
                event_store = EventStore(panel)
            event_candidates = build_event_candidates(panel, cfg)
            codes = {c for target in event_candidates.values() for c in target}
            if not codes:
                log(f"[iter40] {i:03d}/{len(configs)} {cfg.name}: no candidates")
                continue
            daily, stats = simulate_event_lifecycle(days, event_store, event_candidates, risk, cfg)
        else:
            targets = build_targets(panel, days, cfg)
            codes = {c for target in targets.values() for c in target}
            if not codes:
                log(f"[iter40] {i:03d}/{len(configs)} {cfg.name}: no candidates")
                continue
            px_lookup = build_price_lookup(panel, codes)
            daily, stats = simulate(days, px_lookup, targets, risk, cfg.persist)
        row = validate_daily(cfg.name, daily, len(configs), stats)
        row["family"] = cfg.family
        row["score_kind"] = cfg.score_kind
        rows.append(row)
        dailies.append((cfg, daily, row))
        log(
            f"[iter40] {i:03d}/{len(configs)} {cfg.name}: "
            f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
            f"MDD={row['oos_mdd']:.2%} max_active={stats['max_active']:.0f} "
            f"({time.time()-cfg_t0:.1f}s)"
        )

    if not rows:
        raise RuntimeError("no campaign results")

    summary = pl.DataFrame(rows).sort(["oos_sortino", "oos_cagr"], descending=[True, True])
    out = RESULTS / "iter_40_research_campaign_summary.csv"
    summary.write_csv(out)

    for cfg, daily, _ in dailies:
        daily.write_csv(RESULTS / f"iter_40_{cfg.name}_daily.csv")
    log(f"Saved daily files: {len(dailies)}")

    log("=" * 120)
    log("iter_40 research campaign - generation 1")
    log("=" * 120)
    print(
        summary.select(
            [
                "name",
                "family",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("sortino").round(3).alias("full_sortino"),
                pl.col("mdd").mul(100).round(2).alias("full_mdd_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
                pl.col("dsr").round(3),
                pl.col("pbo").round(3),
                pl.col("max_active").cast(pl.Int64),
                pl.col("trade_days").cast(pl.Int64),
            ]
        )
        .head(20)
        .to_pandas()
        .to_string(index=False)
    )
    log(f"\nSaved: {out}")
    log(f"Saved configs: {config_path}")
    log(f"Saved daily files: {len(dailies)}")
    log(f"[iter40] elapsed={time.time()-t0:.1f}s")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-configs", type=int, default=None)
    ap.add_argument("--write-top-n", type=int, default=12)
    args = ap.parse_args()
    run_campaign(max_configs=args.max_configs, write_top_n=args.write_top_n)


if __name__ == "__main__":
    main()
