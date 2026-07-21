"""iter_35 - dynamic core/satellite strategy without hard-coded 2330.

This iteration fixes the key flaw in iter_34: the core sleeve must be selected
by indicators, not by permanently holding 2330. 2330 remains only a benchmark.

Design
======

* Long-only Taiwan common stocks.
* At most 10 simultaneous holdings.
* Adjusted total-return OHLCV from the canonical price module.
* Next-open execution.
* Dynamic core sleeve:
    - liquid, non-financial common stocks
    - quality floor
    - positive long trend
    - ranked by quality + revenue + momentum + institutional flow
* Dynamic satellite sleeve:
    - revenue/price/volume acceleration
    - ranked by acceleration score
* Event-driven exits/replacements:
    - ATR trailing stop
    - trend break
    - revenue/score fade
    - materially stronger replacement candidate
* Unused capital stays cash.

The full feature panel is cached by iter_33. This file should mostly spend time
on vectorized candidate construction and small portfolio-state loops.
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from iter_32_first_principles import CAPITAL, COMMISSION, SELL_TAX, benchmark_rows  # noqa: E402
from iter_33_pm_first_principles import (  # noqa: E402
    RESULTS,
    build_pm_lookup,
    load_or_build_panel,
    log,
    market_regime,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect  # noqa: E402

RF = 0.01
TDPY = 252


@dataclass(frozen=True)
class DynConfig:
    name: str
    core_slots: int = 2
    sat_slots: int = 3
    core_weight: float = 0.60
    min_adv: float = 50_000_000.0
    core_min_score: float = 0.45
    core_exit_score: float = 0.05
    core_min_yoy: float = -10.0
    core_min_roa: float = 0.06
    core_min_gm: float = 0.16
    core_min_fscore: int = 4
    sat_min_score: float = 0.70
    sat_min_yoy: float = 20.0
    sat_min_yoy_delta: float = -15.0
    sat_breakout_lkb: int = 90
    sat_breakout_ratio: float = 0.98
    sat_vol_mult: float = 1.1
    sat_min_fscore: int = 3
    min_inst_flow: float = -0.05
    replace_gap: float = 0.25
    min_hold_days: int = 20
    exit_yoy: float = -25.0
    core_atr_mult: float = 5.0
    sat_atr_mult: float = 4.0
    trail_min: float = 0.12
    trail_max: float = 0.35
    market_risk_off: bool = True
    risk_off_exit_all: bool = False
    risk_off_block_entries: bool = False
    portfolio_stop: float | None = None

    @property
    def max_positions(self) -> int:
        return self.core_slots + self.sat_slots

    @property
    def sat_weight(self) -> float:
        return 1.0 - self.core_weight


def nav_metrics(nav: np.ndarray, days: list[date]) -> dict[str, float]:
    nav = np.asarray(nav, dtype=float)
    if len(nav) < 2:
        raise ValueError("need at least two NAV rows")
    rets = np.diff(nav) / nav[:-1]
    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (nav[-1] / nav[0]) ** (1 / years) - 1
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY)) if len(rets) > 1 else 0.0
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    peak = float(nav[0])
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


def _hi_col(lookback: int) -> str:
    if lookback == 60:
        return "hi60"
    if lookback == 90:
        return "hi90"
    if lookback == 120:
        return "hi120"
    raise ValueError(f"unsupported lookback: {lookback}")


def _common_expr(cfg: DynConfig) -> pl.Expr:
    return (
        (~pl.col("is_etf"))
        & (~pl.col("is_finance"))
        & (pl.col("listed_days") >= 252)
        & (pl.col("adv60") >= cfg.min_adv)
        & (pl.col("next_open").is_not_null())
        & (pl.col("atr_pct").is_between(0.008, 0.11))
        & (pl.col("inst_flow20").fill_null(0) >= cfg.min_inst_flow)
    )


def build_lane_groups(panel: pl.DataFrame, cfg: DynConfig) -> dict[str, dict[date, list[dict]]]:
    t0 = time.time()
    base = panel.filter(_common_expr(cfg))
    core = (
        base.filter(
            (pl.col("roa_ttm").fill_null(-999) >= cfg.core_min_roa)
            & (pl.col("gross_margin_ttm").fill_null(-999) >= cfg.core_min_gm)
            & (pl.col("f_score_raw").fill_null(0) >= cfg.core_min_fscore)
            & (pl.col("latest_yoy").fill_null(-999) >= cfg.core_min_yoy)
            & (pl.col("close") > pl.col("ma200"))
            & (pl.col("ma50") > pl.col("ma200") * 0.98)
            & (pl.col("core_score") >= cfg.core_min_score)
        )
        .select(
            [
                "date",
                "company_code",
                pl.lit("core").alias("lane"),
                pl.col("core_score").alias("lane_score"),
                "rank_score",
                "latest_yoy",
                "ret120",
                "inst_flow20",
            ]
        )
        .sort(["date", "lane_score"], descending=[False, True])
        .group_by("date", maintain_order=True)
        .head(max(cfg.core_slots * 8, 20))
    )

    hi_col = _hi_col(cfg.sat_breakout_lkb)
    sat = (
        base.filter(
            (pl.col("latest_yoy") >= cfg.sat_min_yoy)
            & (pl.col("yoy_delta").fill_null(0) >= cfg.sat_min_yoy_delta)
            & (pl.col("close") >= pl.col(hi_col) * cfg.sat_breakout_ratio)
            & (pl.col("vol") >= pl.col("vol_avg60") * cfg.sat_vol_mult)
            & (pl.col("close") > pl.col("ma100"))
            & (pl.col("sat_score") >= cfg.sat_min_score)
            & (pl.col("f_score_raw").fill_null(0) >= cfg.sat_min_fscore)
        )
        .select(
            [
                "date",
                "company_code",
                pl.lit("sat").alias("lane"),
                pl.col("sat_score").alias("lane_score"),
                "rank_score",
                "latest_yoy",
                "ret120",
                "inst_flow20",
            ]
        )
        .sort(["date", "lane_score"], descending=[False, True])
        .group_by("date", maintain_order=True)
        .head(max(cfg.sat_slots * 10, 30))
    )

    out = {}
    for name, frame in {"core": core, "sat": sat}.items():
        out[name] = {
            key[0] if isinstance(key, tuple) else key: g.to_dicts()
            for key, g in frame.group_by("date", maintain_order=True)
        }
    log(
        f"  groups {cfg.name}: core_rows={core.height:,} sat_rows={sat.height:,} "
        f"days={len(set(out['core']) | set(out['sat'])):,} ({time.time() - t0:.1f}s)"
    )
    return out


def _position_value(pos: dict, row: dict | None, px_col: str) -> float:
    px = row.get(px_col) if row else None
    if px is None or px <= 0 or math.isnan(float(px)):
        px = pos["last_close"]
    return pos["shares"] * px


def run_dynamic_config(
    cfg: DynConfig,
    groups: dict[str, dict[date, list[dict]]],
    days: list[date],
    row_lookup: dict[tuple[date, str], dict],
    risk_on: dict[date, bool],
) -> tuple[dict, pl.DataFrame, pl.DataFrame]:
    cash = CAPITAL
    positions: dict[str, dict] = {}
    pending_exits: set[str] = set()
    pending_entries: list[dict] = []
    nav_hist = []
    trades = []
    max_active = 0
    nav_peak = CAPITAL
    risk_lock = False

    lane_slots = {"core": cfg.core_slots, "sat": cfg.sat_slots}
    lane_weights = {"core": cfg.core_weight, "sat": cfg.sat_weight}

    def current_nav(d: date, use_open: bool = False) -> float:
        px_col = "open" if use_open else "close"
        return cash + sum(
            _position_value(pos, row_lookup.get((d, code)), px_col)
            for code, pos in positions.items()
        )

    def lane_exposure(d: date, lane: str, use_open: bool = False) -> float:
        px_col = "open" if use_open else "close"
        return sum(
            _position_value(pos, row_lookup.get((d, code)), px_col)
            for code, pos in positions.items()
            if pos["lane"] == lane
        )

    def holding_days(d: date, pos: dict) -> int:
        return max((d - pos["entry_date"]).days, 0)

    for d in days:
        for code in list(pending_exits):
            pos = positions.pop(code, None)
            if pos is None:
                continue
            row = row_lookup.get((d, code))
            sell_px = row.get("open") if row else None
            if sell_px is None or sell_px <= 0:
                sell_px = pos["last_close"]
            cash += pos["shares"] * sell_px * (1 - SELL_TAX - COMMISSION)
            trades.append(
                {
                    "date": d,
                    "code": code,
                    "action": "exit",
                    "lane": pos["lane"],
                    "price": sell_px,
                    "ret": sell_px / pos["entry_px"] - 1,
                    "reason": pos.get("pending_reason", ""),
                }
            )
        pending_exits.clear()

        if pending_entries:
            nav_open = current_nav(d, use_open=True)
            for sig in pending_entries:
                lane = sig["lane"]
                if lane_slots[lane] <= 0:
                    continue
                if sum(1 for p in positions.values() if p["lane"] == lane) >= lane_slots[lane]:
                    continue
                code = sig["company_code"]
                if code in positions:
                    continue
                row = row_lookup.get((d, code))
                buy_px = row.get("open") if row else None
                if buy_px is None or buy_px <= 0:
                    continue
                lane_budget = nav_open * lane_weights[lane]
                lane_value = lane_exposure(d, lane, use_open=True)
                target_dollar = lane_budget / lane_slots[lane]
                remaining_lane_budget = max(0.0, lane_budget - lane_value)
                spend = min(cash, target_dollar, remaining_lane_budget)
                if spend <= 0:
                    continue
                shares = spend / buy_px / (1 + COMMISSION)
                cost = shares * buy_px * (1 + COMMISSION)
                if shares <= 0 or cost > cash + 1e-6:
                    continue
                cash -= cost
                atr_pct = row.get("atr_pct") or 0.05
                atr_mult = cfg.core_atr_mult if lane == "core" else cfg.sat_atr_mult
                positions[code] = {
                    "shares": shares,
                    "entry_px": buy_px,
                    "entry_date": d,
                    "lane": lane,
                    "trail_pct": max(cfg.trail_min, min(cfg.trail_max, atr_pct * atr_mult)),
                    "peak_close": buy_px,
                    "last_close": buy_px,
                    "entry_score": sig.get("lane_score", 0.0),
                }
                trades.append(
                    {
                        "date": d,
                        "code": code,
                        "action": "entry",
                        "lane": lane,
                        "price": buy_px,
                        "ret": None,
                        "reason": f"lane_score={sig.get('lane_score', 0):.3f}",
                    }
                )
            pending_entries = []

        for code, pos in list(positions.items()):
            row = row_lookup.get((d, code))
            if row is None:
                continue
            close = row.get("close")
            if close is None or close <= 0:
                continue
            pos["last_close"] = close
            pos["peak_close"] = max(pos["peak_close"], close)

        nav = current_nav(d)
        max_active = max(max_active, len(positions))
        nav_hist.append(
            (
                d,
                nav,
                len(positions),
                cash,
                lane_exposure(d, "core"),
                lane_exposure(d, "sat"),
            )
        )

        pending_exits = set()
        for code, pos in positions.items():
            row = row_lookup.get((d, code))
            if row is None:
                continue
            close = row.get("close")
            ma100 = row.get("ma100")
            ma200 = row.get("ma200")
            latest_yoy = row.get("latest_yoy")
            lane_score = row.get("core_score") if pos["lane"] == "core" else row.get("sat_score")
            reason = None
            if pos["peak_close"] > 0 and close / pos["peak_close"] - 1 <= -pos["trail_pct"]:
                reason = "atr_trailing"
            elif cfg.risk_off_exit_all and not risk_on.get(d, True):
                reason = "market_risk_off"
            elif ma200 is not None and close < ma200:
                reason = "ma200_break"
            elif pos["lane"] == "sat" and ma100 is not None and close < ma100:
                reason = "sat_ma100_break"
            elif latest_yoy is not None and latest_yoy < cfg.exit_yoy:
                reason = "yoy_fade"
            elif pos["lane"] == "core" and cfg.market_risk_off and not risk_on.get(d, True) and ma100 is not None and close < ma100:
                reason = "risk_off_core_weak"
            elif (
                lane_score is not None
                and lane_score < cfg.core_exit_score
                and holding_days(d, pos) >= cfg.min_hold_days
            ):
                reason = "score_fade"
            if reason:
                pos["pending_reason"] = reason
                pending_exits.add(code)

        if cfg.portfolio_stop is not None:
            nav_peak = max(nav_peak, nav)
            if nav_peak > 0 and nav / nav_peak - 1 <= -cfg.portfolio_stop:
                risk_lock = True
                for code, pos in positions.items():
                    pos["pending_reason"] = "portfolio_stop"
                    pending_exits.add(code)
            elif risk_lock and risk_on.get(d, True):
                risk_lock = False
                nav_peak = nav

        held_or_exiting = set(positions) | pending_exits
        pending_entries = []
        if (cfg.risk_off_block_entries and not risk_on.get(d, True)) or risk_lock:
            continue
        for lane in ("core", "sat"):
            if lane_slots[lane] <= 0:
                continue
            active_after_exits = sum(
                1 for code, pos in positions.items()
                if pos["lane"] == lane and code not in pending_exits
            )
            available = lane_slots[lane] - active_after_exits
            desired = groups[lane].get(d, [])
            for sig in desired:
                if available <= 0:
                    break
                code = sig["company_code"]
                if code in held_or_exiting:
                    continue
                pending_entries.append(sig)
                held_or_exiting.add(code)
                available -= 1

            if available <= 0:
                for sig in desired:
                    code = sig["company_code"]
                    if code in positions or code in pending_exits:
                        continue
                    replaceable = []
                    for held_code, pos in positions.items():
                        if pos["lane"] != lane or held_code in pending_exits or holding_days(d, pos) < cfg.min_hold_days:
                            continue
                        row = row_lookup.get((d, held_code))
                        cur_score = row.get("core_score") if lane == "core" and row else row.get("sat_score") if row else pos.get("entry_score", 0.0)
                        replaceable.append((cur_score or -999.0, held_code, pos))
                    if not replaceable:
                        break
                    worst_score, worst_code, worst_pos = min(replaceable, key=lambda x: x[0])
                    if sig["lane_score"] >= worst_score + cfg.replace_gap:
                        worst_pos["pending_reason"] = "better_candidate"
                        pending_exits.add(worst_code)
                        pending_entries.append(sig)
                        held_or_exiting.add(code)
                    break

    nav_df = pl.DataFrame(
        {
            "date": pl.Series([x[0] for x in nav_hist], dtype=pl.Date),
            "nav": pl.Series([x[1] for x in nav_hist], dtype=pl.Float64),
            "n_active": pl.Series([x[2] for x in nav_hist], dtype=pl.Int64),
            "cash": pl.Series([x[3] for x in nav_hist], dtype=pl.Float64),
            "core_value": pl.Series([x[4] for x in nav_hist], dtype=pl.Float64),
            "sat_value": pl.Series([x[5] for x in nav_hist], dtype=pl.Float64),
        }
    )
    trades_df = pl.DataFrame(trades) if trades else pl.DataFrame(
        schema={"date": pl.Date, "code": pl.Utf8, "action": pl.Utf8, "lane": pl.Utf8, "price": pl.Float64}
    )
    m = nav_metrics(nav_df["nav"].to_numpy(), nav_df["date"].to_list())
    m.update(
        {
            "name": cfg.name,
            "max_active": max_active,
            "n_entries": int((trades_df["action"] == "entry").sum()) if trades_df.height else 0,
            "n_exits": int((trades_df["action"] == "exit").sum()) if trades_df.height else 0,
            "avg_active": float(nav_df["n_active"].mean()),
            "cash_avg": float((nav_df["cash"] / nav_df["nav"]).mean()),
            "core_avg": float((nav_df["core_value"] / nav_df["nav"]).mean()),
            "sat_avg": float((nav_df["sat_value"] / nav_df["nav"]).mean()),
        }
    )
    return m, nav_df, trades_df


def configs() -> list[DynConfig]:
    return [
        DynConfig("dyn_1c_3s_w60", core_slots=1, sat_slots=3, core_weight=0.60),
        DynConfig("dyn_1c_3s_w70", core_slots=1, sat_slots=3, core_weight=0.70),
        DynConfig("dyn_2c_3s_w60", core_slots=2, sat_slots=3, core_weight=0.60),
        DynConfig("dyn_2c_3s_w70", core_slots=2, sat_slots=3, core_weight=0.70),
        DynConfig("dyn_2c_5s_w60", core_slots=2, sat_slots=5, core_weight=0.60, sat_min_score=0.65, sat_vol_mult=1.0),
        DynConfig("dyn_3c_4s_w60", core_slots=3, sat_slots=4, core_weight=0.60, core_min_score=0.40, sat_min_score=0.70),
        DynConfig("dyn_quality_2c_3s_w65", core_slots=2, sat_slots=3, core_weight=0.65, core_min_roa=0.10, core_min_gm=0.22, core_min_score=0.50),
        DynConfig("dyn_quality_2c_3s_w65_riskoff", core_slots=2, sat_slots=3, core_weight=0.65, core_min_roa=0.10, core_min_gm=0.22, core_min_score=0.50, risk_off_exit_all=True, risk_off_block_entries=True),
        DynConfig("dyn_quality_2c_3s_w65_stop25", core_slots=2, sat_slots=3, core_weight=0.65, core_min_roa=0.10, core_min_gm=0.22, core_min_score=0.50, portfolio_stop=0.25),
        DynConfig("dyn_quality_2c_3s_w65_risk_stop", core_slots=2, sat_slots=3, core_weight=0.65, core_min_roa=0.10, core_min_gm=0.22, core_min_score=0.50, risk_off_exit_all=True, risk_off_block_entries=True, portfolio_stop=0.25),
        DynConfig("dyn_quality_1c_2s_w75_risk_stop", core_slots=1, sat_slots=2, core_weight=0.75, core_min_roa=0.10, core_min_gm=0.22, core_min_score=0.55, sat_min_score=0.75, risk_off_exit_all=True, risk_off_block_entries=True, portfolio_stop=0.22),
        DynConfig("dyn_leader_1c_w100", core_slots=1, sat_slots=0, core_weight=1.00, min_adv=1_000_000_000, core_min_roa=0.08, core_min_gm=0.18, core_min_score=0.45, replace_gap=0.35, min_hold_days=35),
        DynConfig("dyn_leader_1c_w100_riskoff", core_slots=1, sat_slots=0, core_weight=1.00, min_adv=1_000_000_000, core_min_roa=0.08, core_min_gm=0.18, core_min_score=0.45, replace_gap=0.35, min_hold_days=35, risk_off_exit_all=True, risk_off_block_entries=True),
        DynConfig("dyn_leader_2c_w100", core_slots=2, sat_slots=0, core_weight=1.00, min_adv=600_000_000, core_min_roa=0.08, core_min_gm=0.18, core_min_score=0.42, replace_gap=0.30, min_hold_days=30),
        DynConfig("dyn_leader_1c_2s_w75", core_slots=1, sat_slots=2, core_weight=0.75, min_adv=600_000_000, core_min_roa=0.08, core_min_gm=0.18, core_min_score=0.45, sat_min_score=0.72, replace_gap=0.25, min_hold_days=25),
        DynConfig("dyn_leader_1c_2s_w75_riskoff", core_slots=1, sat_slots=2, core_weight=0.75, min_adv=600_000_000, core_min_roa=0.08, core_min_gm=0.18, core_min_score=0.45, sat_min_score=0.72, replace_gap=0.25, min_hold_days=25, risk_off_exit_all=True, risk_off_block_entries=True),
        DynConfig("dyn_fast_1c_4s_w55", core_slots=1, sat_slots=4, core_weight=0.55, sat_min_score=0.65, sat_vol_mult=1.0, replace_gap=0.15, min_hold_days=10),
        DynConfig("dyn_defensive_2c_2s_w75", core_slots=2, sat_slots=2, core_weight=0.75, core_min_score=0.50, sat_min_score=0.80, replace_gap=0.35, min_hold_days=35),
    ]


def add_benchmark_rows(con, start: date, end: date, rows: list[dict]) -> None:
    rows.extend(benchmark_rows(con, start, end))
    hybrid_path = RESULTS / "latest_true_3q_7c_best_w60_daily.csv"
    if hybrid_path.exists():
        h = pl.read_csv(hybrid_path, try_parse_dates=True)
        m = nav_metrics(h["nav"].to_numpy(), h["date"].to_list())
        m.update({"name": "hybrid_3q7_w60", "max_active": 10, "n_entries": None, "n_exits": None, "avg_active": None, "cash_avg": None, "core_avg": None, "sat_avg": None})
        rows.append(m)


def period_validation(summary_names: list[str]) -> pl.DataFrame:
    periods = [
        ("Full", "2005-01-03", "2026-05-08"),
        ("OOS_2013", "2013-01-01", "2026-05-08"),
        ("OOS_2017", "2017-01-01", "2026-05-08"),
        ("COVID_plus", "2020-01-01", "2026-05-08"),
        ("Recent_2022", "2022-01-01", "2026-05-08"),
    ]
    paths = {name: RESULTS / f"iter_35_{name}_daily.csv" for name in summary_names}
    paths["hybrid_3q7_w60"] = RESULTS / "latest_true_3q_7c_best_w60_daily.csv"
    paths["hold_2330"] = RESULTS / "latest_hold_2330_daily.csv"
    rows = []
    for name, path in paths.items():
        if not path.exists():
            continue
        df = pl.read_csv(path, try_parse_dates=True)
        for period, start, end in periods:
            sub = df.filter((pl.col("date") >= pl.lit(start).str.to_date()) & (pl.col("date") <= pl.lit(end).str.to_date()))
            if sub.height < 3:
                continue
            m = nav_metrics(sub["nav"].to_numpy(), sub["date"].to_list())
            rows.append({"period": period, "strategy": name, **m})
    return pl.DataFrame(rows)


def holding_summary(trades_df: pl.DataFrame) -> pl.DataFrame:
    if trades_df.is_empty() or "action" not in trades_df.columns:
        return pl.DataFrame()
    return (
        trades_df.filter(pl.col("action") == "entry")
        .group_by(["lane", "code"])
        .agg(pl.len().alias("entries"))
        .sort(["lane", "entries"], descending=[False, True])
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2005-01-03")
    ap.add_argument("--end", default="2026-05-08")
    ap.add_argument("--no-cache", action="store_true")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    RESULTS.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    con = connect()
    panel, days = load_or_build_panel(con, start, end, use_cache=not args.no_cache)
    risk_on = market_regime(con, start, end, days)

    cfgs = configs()
    group_map = {}
    candidate_codes: set[str] = set()
    log("[iter35] build dynamic core/satellite groups ...")
    for cfg in cfgs:
        groups = build_lane_groups(panel, cfg)
        group_map[cfg.name] = groups
        for lane in ("core", "sat"):
            candidate_codes.update(row["company_code"] for rows in groups[lane].values() for row in rows)

    row_lookup = build_pm_lookup(panel, candidate_codes)
    log(f"[iter35] row lookup: {len(row_lookup):,} rows ({len(candidate_codes):,} candidate codes)")

    rows = []
    best_trades = None
    best_name = None
    for cfg in cfgs:
        log(f"\n[iter35] run {cfg.name}")
        t = time.time()
        result, nav_df, trades_df = run_dynamic_config(cfg, group_map[cfg.name], days, row_lookup, risk_on)
        nav_path = RESULTS / f"iter_35_{cfg.name}_daily.csv"
        trades_path = RESULTS / f"iter_35_{cfg.name}_trades.csv"
        nav_df.write_csv(nav_path)
        trades_df.write_csv(trades_path)
        result.update(asdict(cfg))
        result["nav_path"] = str(nav_path)
        rows.append(result)
        if best_trades is None or result["sortino"] > max(r["sortino"] for r in rows[:-1]):
            best_trades = trades_df
            best_name = cfg.name
        log(
            f"  CAGR {result['cagr'] * 100:+.2f}% Sortino {result['sortino']:.3f} "
            f"Sharpe {result['sharpe']:.3f} MDD {result['mdd'] * 100:.2f}% "
            f"entries {result['n_entries']} max_active {result['max_active']} "
            f"cash_avg {result['cash_avg']:.1%} ({time.time() - t:.1f}s)"
        )

    add_benchmark_rows(con, start, end, rows)
    out = pl.DataFrame(rows).sort(["sortino", "cagr"], descending=[True, True])
    out_path = RESULTS / "iter_35_dynamic_core_satellite_summary.csv"
    out.write_csv(out_path)

    validation = period_validation([cfg.name for cfg in cfgs])
    val_path = RESULTS / "iter_35_dynamic_core_satellite_validation.csv"
    validation.write_csv(val_path)

    if best_trades is not None:
        holds = holding_summary(best_trades)
        if not holds.is_empty():
            holds.write_csv(RESULTS / f"iter_35_{best_name}_holding_summary.csv")

    log("\n" + "=" * 96)
    log(f"iter_35 dynamic core/satellite summary ({start} -> {end})")
    log("=" * 96)
    log(
        out.select(
            [
                "name",
                pl.col("cagr").mul(100).round(2).alias("cagr_pct"),
                pl.col("sortino").round(3),
                pl.col("sharpe").round(3),
                pl.col("mdd").mul(100).round(2).alias("mdd_pct"),
                "max_active",
                pl.col("avg_active").round(2),
                pl.col("cash_avg").mul(100).round(1).alias("cash_avg_pct"),
                "n_entries",
            ]
        )
        .head(16)
        .to_pandas()
        .to_string(index=False)
    )
    log(f"\nSaved: {out_path}")
    log(f"Saved: {val_path}")
    log(f"Total runtime: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
