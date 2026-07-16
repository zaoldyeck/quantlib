"""Event-driven Serenity engine v1.

Extends `serenity_industry_first_replay_2025.py` from monthly rotation into a
daily-monitored event engine, per the Serenity-system backtest design:

- Entries are event-triggered (monthly-revenue publication refresh, optional
  treasury-buyback support events), executed T+1.
- Exits are rule-based and monitored daily: trailing stop, absolute stop,
  optional take-profit, time stop (champion-style: still below entry after K
  days), and thesis stop (3M revenue YoY turns negative at refresh).
- Freed slots are refilled from the freshest scored candidate list (age-capped),
  with a re-entry cooldown.

Point-in-time discipline is inherited from the replay: revenue usable from the
10th of the following month, taxonomy/PER/flows as-of joins, thesis registry
activation dates with optional lag stress. Unstructured news is deliberately
NOT part of the backtest (no PIT news archive) — it stays a live-monitoring
layer only.

Data freshness: requires `research/cache.duckdb` current (see research SOP).

Run:
  uv run --project research python research/serenity/engine.py \
      --start 2025-01-01 --activation-lag-days 0
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, replace
from datetime import date, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT / "serenity"))

from constants import CAPITAL, COMMISSION, SELL_TAX  # noqa: E402
from db import connect  # noqa: E402
from prices import total_return_series  # noqa: E402

from replay_2025 import set_ablate, set_conv_weight, set_fresh, set_pe_pen_mode, set_role_bonus  # noqa: E402
from replay_2025 import (  # noqa: E402
    REGISTRY,
    active_registry_for_day,
    benchmark_nav,
    load_price_features,
    load_registry,
    load_point_in_time_table,
    load_revenue_features,
    load_taxonomy,
    load_universe,
    row_latest_before,
    score_candidates,
    summarize_nav,
)

sys.path.insert(0, str(RESEARCH_ROOT / "strat_lab"))
from evaluation import nav_metrics, trade_distribution_metrics  # noqa: E402

RESULTS = REPO_ROOT / "research" / "strat_lab" / "results"
DOCS = REPO_ROOT / "docs" / "serenity"
OUT_PREFIX = "serenity_event_engine_v1"

FEE_BUY = COMMISSION + 0.0005
FEE_SELL = COMMISSION + SELL_TAX + 0.0005
CANDIDATE_MAX_AGE = 25  # trading days a scored list stays usable for refills
COOLDOWN_DAYS = 20  # trading days before a stopped-out name can re-enter


@dataclass(frozen=True)
class ExitRules:
    trail: float | None = None  # exit if close < peak * (1 - trail)
    abs_stop: float | None = None  # exit if close < entry * (1 - abs_stop)
    take_profit: float | None = None  # exit if close > entry * (1 + tp)
    time_days: int | None = None  # holding days threshold ...
    time_ret: float = -0.01  # ... exit if still below entry * (1 + time_ret)
    thesis_stop: bool = False  # exit at refresh when yoy_3m < 0


@dataclass(frozen=True)
class EngineVariant:
    name: str
    max_positions: int = 10
    rules: ExitRules = ExitRules()
    refresh_rotation: bool = False  # sell names that dropped out of top list at refresh
    use_buyback: bool = False
    theme_cap: int | None = None  # max concurrent positions per theme_id
    # v2 defensive layer (pre-committed params, see serenity_engine_trials_ledger.md)
    regime_guard: bool = False  # pool-dd risk-off + 0050<MA120 entry halt
    max_new_per_day: int | None = None  # entry throttle
    adv_cap: float | None = None  # position notional <= adv_cap * ADV20
    chips_score: bool = False  # battle 4: score + SBL 20d change + foreign 60d trend
    tp_mode: str = "fixed"  # battle 6: "fixed" | "peg_target" | "peg_exit"
    weight_mode: str = "equal"  # battle 7: "equal" | "score" | "inv_atr"
    thesis_mode: str = "yoy3m_neg"  # battle 8: yoy3m_neg | yoy3m_lt10 | yoy1m_neg | inst_neg | decel40
    gap_w: float = 0.0  # battle 10: score += gap_w * gap_unit (expectations gap, iter_87)
    # battle 14: entry-consistency veto — refuse a seat when the exit gate is
    # already half-armed at entry ("none" | "inst" | "inst_yoy")
    entry_veto: str = "none"
    # battle 18: regime-adaptive exits — bull (0050>=MA120): no TP + wide trail
    # (backcast-optimal); bear: TP60 + tight trail. Overrides rules by day.
    regime_exit: bool = False
    # battle 18: force-exit a holding once ALL its themes hit active_until
    # (theme-invalidation dates from the backfill campaign).
    theme_dead_exit: bool = False


FULL_RULES = ExitRules(trail=0.20, abs_stop=0.15, time_days=50, thesis_stop=True)

WIDE_RULES = ExitRules(trail=0.30, abs_stop=0.25, time_days=50, thesis_stop=True)

VARIANTS: tuple[EngineVariant, ...] = (
    EngineVariant("ev_rotation_only", rules=ExitRules(), refresh_rotation=True),
    EngineVariant("ev_rot_tp100", rules=ExitRules(take_profit=1.00), refresh_rotation=True),
    EngineVariant("ev_rot_tp100_thesis", rules=ExitRules(take_profit=1.00, thesis_stop=True), refresh_rotation=True),
    EngineVariant("ev_rot_wide", rules=WIDE_RULES, refresh_rotation=True),
    EngineVariant("ev_rot_wide_tp100", rules=replace(WIDE_RULES, take_profit=1.00), refresh_rotation=True),
    EngineVariant("ev_full", rules=FULL_RULES),
    EngineVariant("ev_full_tp60", rules=replace(FULL_RULES, take_profit=0.60)),
    EngineVariant("ev_full_tp60_tc3", rules=replace(FULL_RULES, take_profit=0.60), theme_cap=3),
    EngineVariant(
        "ev_full_tp60_throttle",
        rules=replace(FULL_RULES, take_profit=0.60),
        max_new_per_day=3,
        adv_cap=0.20,
    ),
    EngineVariant(
        "ev_full_tp60_v2",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
    ),
    EngineVariant(
        "ev_full_tp60_v2_bb",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        use_buyback=True,
    ),
    EngineVariant(
        "ev_v2_thesis_lt10",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        thesis_mode="yoy3m_lt10",
    ),
    EngineVariant(
        "ev_v2_thesis_m1",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        thesis_mode="yoy1m_neg",
    ),
    EngineVariant(
        "ev_v2_thesis_inst",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        thesis_mode="inst_neg",
    ),
    EngineVariant(
        "ev_v2_ti_veto_inst",  # battle 14 V1: champion + entry veto on inst_20d<0
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        thesis_mode="inst_neg",
        entry_veto="inst",
    ),
    EngineVariant(
        "ev_v2_ti_veto_instyoy",  # battle 14 V2: V1 + yoy_3m<0 veto
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        thesis_mode="inst_neg",
        entry_veto="inst_yoy",
    ),
    EngineVariant(
        "ev_v2_gap6",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        thesis_mode="inst_neg",
        gap_w=6.0,
    ),
    EngineVariant(
        "ev_v2_gap12",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        thesis_mode="inst_neg",
        gap_w=12.0,
    ),
    EngineVariant(
        "ev_v2_thesis_decel",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        thesis_mode="decel40",
    ),
    EngineVariant(
        "ev_v2_wscore",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        weight_mode="score",
    ),
    EngineVariant(
        "ev_v2_watr",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        weight_mode="inv_atr",
    ),
    EngineVariant(
        "ev_v2_tpdyn",
        rules=replace(FULL_RULES, take_profit=None),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        tp_mode="peg_target",
    ),
    EngineVariant(
        "ev_v2_pegexit",
        rules=replace(FULL_RULES, take_profit=None),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        tp_mode="peg_exit",
    ),
    EngineVariant(
        "ev_full_tp60_v2_chips",
        rules=replace(FULL_RULES, take_profit=0.60),
        regime_guard=True,
        max_new_per_day=3,
        adv_cap=0.20,
        chips_score=True,
    ),
    EngineVariant("ev_full_tp100", rules=replace(FULL_RULES, take_profit=1.00)),
    EngineVariant("ev_full_tp150", rules=replace(FULL_RULES, take_profit=1.50)),
    EngineVariant("ev_wide", rules=WIDE_RULES),
    EngineVariant("ev_wide_tp100", rules=replace(WIDE_RULES, take_profit=1.00)),
    EngineVariant("ev_wide_tp100_bb", rules=replace(WIDE_RULES, take_profit=1.00), use_buyback=True),
    EngineVariant("ev_time50", rules=ExitRules(time_days=50)),
)


def build_refresh_days(days: list[date], start: date, end: date) -> list[date]:
    """First trading day on/after the 11th of each month (revenue published by the 10th)."""
    out: list[date] = []
    year, month = start.year, start.month
    while date(year, month, 1) <= end:
        target = date(year, month, 11)
        pick = next((d for d in days if d >= target), None)
        if pick is not None and start <= pick <= end and pick not in out:
            out.append(pick)
        year, month = (year + (month // 12), month % 12 + 1)
    return out


def load_universe_history(con, min_avg_trade_value: float = 30_000_000) -> pd.DataFrame:
    """Survivorship-free universe for mechanical mode.

    Uses every code that ever appears in industry_taxonomy_pit (latest attributes
    per code, delisted included) with a coarse historical liquidity screen that a
    delisted stock can also pass.
    """
    frame = con.sql(
        f"""
        WITH tax AS (
            SELECT market, company_code, company_name, industry, broad_sector,
                   is_financial, is_special_category
            FROM industry_taxonomy_pit
            WHERE regexp_matches(company_code, '^[0-9]{{4}}$')
            QUALIFY row_number() OVER (
                PARTITION BY company_code
                ORDER BY effective_date DESC NULLS LAST, source_ym DESC NULLS LAST
            ) = 1
        ),
        liquid AS (
            SELECT company_code
            FROM daily_quote
            GROUP BY company_code
            HAVING avg(trade_value) >= {min_avg_trade_value}
        )
        SELECT t.* FROM tax t JOIN liquid l USING (company_code)
        WHERE t.is_financial = false AND t.is_special_category = false
        """
    ).pl()
    return frame.with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4)).to_pandas()


def load_buyback_events(con) -> pd.DataFrame:
    frame = con.sql(
        """
        SELECT market, company_code, announce_date, price_high, pct_of_capital
        FROM treasury_stock_buyback
        WHERE announce_date IS NOT NULL
        """
    ).pl()
    return (
        frame.with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
        .to_pandas()
        .assign(announce_date=lambda f: pd.to_datetime(f["announce_date"]).dt.date)
    )


def mechanical_registry_for_day(
    day: date,
    tax_day: pd.DataFrame,
    rev_day: pd.DataFrame,
    min_median_yoy3m: float = 10.0,
    min_breadth: float = 0.55,
    min_members: int = 5,
) -> pd.DataFrame:
    """Registry-free theme detection: industries whose PIT revenue trend qualifies.

    Replaces the human-curated thesis registry so the engine can run pre-2025
    windows without hindsight. Conviction scales with industry revenue strength.
    """
    frame = tax_day.merge(rev_day[["company_code", "yoy_3m"]], on="company_code", how="left")
    stats = (
        frame.groupby("industry")
        .agg(
            members=("company_code", "nunique"),
            median_yoy3m=("yoy_3m", "median"),
            breadth=("yoy_3m", lambda s: float((s > 0).mean()) if len(s) else 0.0),
        )
        .reset_index()
    )
    good = stats[
        (stats["members"] >= min_members)
        & (stats["median_yoy3m"] >= min_median_yoy3m)
        & (stats["breadth"] >= min_breadth)
    ]
    if good.empty:
        return pd.DataFrame(columns=["company_code", "theme_id", "theme_name", "conviction", "theme_count"])
    picks = frame[frame["industry"].isin(good["industry"])].copy()
    strength = good.set_index("industry")["median_yoy3m"]
    picks["theme_id"] = picks["industry"]
    picks["theme_name"] = picks["industry"]
    picks["conviction"] = picks["industry"].map(lambda i: float(np.clip(3.0 + strength[i] / 40.0, 3.0, 5.0)))
    picks["theme_count"] = 1
    return picks[["company_code", "theme_id", "theme_name", "conviction", "theme_count"]]


@dataclass
class Position:
    value: float
    entry_close: float
    peak_close: float
    entry_idx: int
    source: str
    missing_days: int = 0
    target_mult: float = 1.6  # battle 6 dynamic take-profit multiple
    thesis_anchor: float | None = None  # battle 8: yoy_3m at entry (decel40 mode)


def peg_target_mult(pe: float | None, growth: float | None) -> float:
    """PEG=1 implied upside, clipped to [1.2, 3.0]; fallback 1.6."""
    if pe is None or growth is None:
        return 1.6
    if not (np.isfinite(pe) and np.isfinite(growth)) or pe <= 0 or growth <= 0:
        return 1.6
    return float(np.clip(growth / pe, 1.2, 3.0))


def simulate_event_variant(
    variant: EngineVariant,
    days: list[date],
    close_by_day: dict[date, dict[str, float]],
    ret_by_day: dict[date, dict[str, float]],
    scored_by_refresh: dict[date, pd.DataFrame],
    thesis_ok_by_refresh: dict[date, dict[str, bool]],
    buyback_by_day: dict[date, list[str]],
    adv_by_day: dict[date, dict[str, float]] | None = None,
    market_risk_off: set[date] | None = None,
    book_sink: dict[date, dict[str, float]] | None = None,
    state_sink: dict | None = None,
    valuation_by_refresh: dict[date, dict[str, tuple[float, float]]] | None = None,
    thesis_metrics_by_refresh: dict[date, dict[str, dict]] | None = None,
    theme_dead_by_code: dict[str, date] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    cash = CAPITAL
    positions: dict[str, Position] = {}
    pending_exits: dict[str, str] = {}
    pending_entries: list[tuple[str, str]] = []
    cooldown: dict[str, int] = {}
    latest_scored: pd.DataFrame | None = None
    latest_scored_idx = -(10**9)
    total_traded = 0.0
    nav_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    theme_risk_off = False  # pool-drawdown state, refreshed monthly (guard A)
    adv_by_day = adv_by_day or {}
    market_risk_off = market_risk_off or set()
    valuation_by_refresh = valuation_by_refresh or {}
    latest_val: dict[str, tuple[float, float]] = {}
    thesis_metrics_by_refresh = thesis_metrics_by_refresh or {}
    latest_thesis: dict[str, dict] = {}

    for idx, day in enumerate(days):
        closes = close_by_day.get(day, {})
        rets = ret_by_day.get(day, {})
        traded_today = False

        # 1) mark-to-market (+ delisting guard: >10 straight sessions without a
        #    price -> force-exit at last value with a 10% haircut)
        for code, pos in positions.items():
            pos.value *= 1.0 + rets.get(code, 0.0)
            px = closes.get(code)
            if px is not None:
                pos.missing_days = 0
                if px > pos.peak_close:
                    pos.peak_close = px
            else:
                pos.missing_days += 1
                if pos.missing_days > 10 and code not in pending_exits:
                    pos.value *= 0.90
                    pending_exits[code] = "delist"

        # 2) execute yesterday's scheduled exits at today's close
        for code, reason in list(pending_exits.items()):
            pos = positions.pop(code, None)
            if pos is None:
                continue
            proceeds = pos.value * (1.0 - FEE_SELL)
            cash += proceeds
            total_traded += pos.value
            cooldown[code] = idx
            traded_today = True
            exit_close = closes.get(code, pos.peak_close)
            trade_rows.append(
                {
                    "code": code,
                    "entry_date": days[pos.entry_idx],
                    "exit_date": day,
                    "days_held": idx - pos.entry_idx,
                    "ret": exit_close / pos.entry_close - 1.0,
                    "reason": reason,
                    "source": pos.source,
                }
            )
        pending_exits.clear()

        # 3) execute yesterday's scheduled entries at today's close
        nav_now = cash + sum(p.value for p in positions.values())
        for code, source, tilt in pending_entries:
            if code in positions or len(positions) >= variant.max_positions:
                continue
            px = closes.get(code)
            if px is None or cash <= 1.0:
                continue
            alloc = min(cash, nav_now / variant.max_positions * tilt)
            if variant.adv_cap is not None:
                adv = adv_by_day.get(day, {}).get(code)
                if adv is not None and np.isfinite(adv):
                    alloc = min(alloc, variant.adv_cap * adv)
            if alloc <= 1.0:
                continue
            cash -= alloc
            total_traded += alloc
            traded_today = True
            tm = 1.6
            if variant.tp_mode == "peg_target":
                pe_g = latest_val.get(code)
                tm = peg_target_mult(*pe_g) if pe_g is not None else 1.6
            met = latest_thesis.get(code) or {}
            positions[code] = Position(
                value=alloc * (1.0 - FEE_BUY),
                entry_close=px,
                peak_close=px,
                entry_idx=idx,
                source=source,
                target_mult=tm,
                thesis_anchor=met.get("yoy_3m"),
            )
        pending_entries.clear()

        if book_sink is not None:
            nav_book = cash + sum(p.value for p in positions.values())
            if nav_book > 0:
                book_sink[day] = (
                    {c: p.value / nav_book for c, p in positions.items()},
                    traded_today,
                )

        # 4) refresh day: update candidate list, thesis stops, optional rotation
        is_refresh = day in scored_by_refresh
        if is_refresh:
            latest_scored = scored_by_refresh[day]
            if variant.gap_w and "gap_unit" in latest_scored.columns:
                # battle 10: expectations-gap tilt (rank(yoy_3m) - rank(PE), in [-1, 1])
                latest_scored = (
                    latest_scored.assign(
                        score=latest_scored["score"]
                        + variant.gap_w * latest_scored["gap_unit"].fillna(0.0)
                    )
                    .sort_values("score", ascending=False)
                    .reset_index(drop=True)
                )
            latest_scored_idx = idx
            if day in valuation_by_refresh:
                latest_val = valuation_by_refresh[day]
                if variant.tp_mode == "peg_target":
                    for code, pos in positions.items():
                        pe_g = latest_val.get(code)
                        if pe_g is not None:
                            pos.target_mult = peg_target_mult(*pe_g)
                if variant.tp_mode == "peg_exit":
                    for code, pos in positions.items():
                        if code in pending_exits:
                            continue
                        pe_g = latest_val.get(code)
                        px_now = closes.get(code)
                        if pe_g is None or px_now is None:
                            continue
                        pe, growth = pe_g
                        if (
                            pe is not None
                            and growth is not None
                            and np.isfinite(pe)
                            and np.isfinite(growth)
                            and growth > 0
                            and pe / growth >= 1.5
                            and px_now >= pos.entry_close * 1.20
                        ):
                            pending_exits[code] = "val_exit"
            if variant.regime_guard and "drawdown_252" in latest_scored:
                pool_dd = pd.to_numeric(latest_scored["drawdown_252"], errors="coerce").median()
                theme_risk_off = bool(np.isfinite(pool_dd) and pool_dd <= -0.30)
            rules = variant.rules
            if day in thesis_metrics_by_refresh:
                latest_thesis = thesis_metrics_by_refresh[day]
            if rules.thesis_stop:
                if variant.thesis_mode == "yoy3m_neg" or not latest_thesis:
                    ok_map = thesis_ok_by_refresh.get(day, {})
                    for code in positions:
                        if code not in pending_exits and not ok_map.get(code, True):
                            pending_exits[code] = "thesis"
                else:
                    for code, pos in positions.items():
                        if code in pending_exits:
                            continue
                        met = latest_thesis.get(code)
                        if not met:
                            continue
                        yoy3 = met.get("yoy_3m")
                        yoy1 = met.get("yoy_1m")
                        inst = met.get("inst_20d")
                        px_now = closes.get(code)
                        fire = False
                        if variant.thesis_mode == "yoy3m_lt10":
                            fire = yoy3 is not None and yoy3 < 10.0
                        elif variant.thesis_mode == "yoy1m_neg":
                            fire = yoy1 is not None and yoy1 < 0.0
                        elif variant.thesis_mode == "inst_neg":
                            fire = (
                                inst is not None
                                and inst < 0
                                and px_now is not None
                                and px_now < pos.entry_close
                            )
                        elif variant.thesis_mode == "decel40":
                            fire = (
                                yoy3 is not None
                                and pos.thesis_anchor is not None
                                and (pos.thesis_anchor - yoy3) > 40.0
                            )
                        if fire:
                            pending_exits[code] = "thesis"
            if variant.refresh_rotation and latest_scored is not None:
                keep = set(latest_scored.head(variant.max_positions)["company_code"])
                for code in positions:
                    if code not in keep and code not in pending_exits:
                        pending_exits[code] = "rotation"

        # 5) evaluate daily exit rules on today's close
        rules = variant.rules
        if variant.regime_exit:
            # battle 18: bull regime → ride winners (no TP, wide trail);
            # bear regime (0050 < MA120) → tighten (TP60, trail 20).
            if day in market_risk_off:
                rules = replace(rules, take_profit=0.60, trail=0.20)
            else:
                rules = replace(rules, take_profit=None, trail=0.30)
        effective_trail = rules.trail
        if variant.regime_guard and theme_risk_off:
            effective_trail = 0.15 if rules.trail is None else min(rules.trail, 0.15)
        theme_dead = theme_dead_by_code or {}
        for code, pos in positions.items():
            if code in pending_exits:
                continue
            if variant.theme_dead_exit and code in theme_dead and day >= theme_dead[code]:
                pending_exits[code] = "theme_dead"
                continue
            px = closes.get(code)
            if px is None:
                continue
            if rules.abs_stop is not None and px <= pos.entry_close * (1.0 - rules.abs_stop):
                pending_exits[code] = "abs_stop"
            elif effective_trail is not None and px <= pos.peak_close * (1.0 - effective_trail):
                pending_exits[code] = "trail"
            elif (
                variant.tp_mode == "fixed"
                and rules.take_profit is not None
                and px >= pos.entry_close * (1.0 + rules.take_profit)
            ):
                pending_exits[code] = "take_profit"
            elif variant.tp_mode == "peg_target" and px >= pos.entry_close * pos.target_mult:
                pending_exits[code] = "take_profit_dyn"
            elif (
                rules.time_days is not None
                and idx - pos.entry_idx >= rules.time_days
                and px <= pos.entry_close * (1.0 + rules.time_ret)
            ):
                pending_exits[code] = "time_stop"

        # 6) schedule refills for tomorrow
        held_net = len(positions) - len(pending_exits)
        queue: list[tuple[str, str, float]] = []
        if variant.use_buyback:
            # Counter-cyclical channel: exempt from regime guards, capped at 2
            # concurrent buyback-sourced positions (see trials ledger battle 3).
            bb_active = sum(
                1 for c, p in positions.items() if p.source == "buyback" and c not in pending_exits
            )
            bb_budget = min(2 - bb_active, variant.max_positions - held_net - len(pending_entries))
            for code in buyback_by_day.get(day, []):
                if bb_budget <= 0:
                    break
                if code in positions or idx - cooldown.get(code, -(10**9)) <= COOLDOWN_DAYS:
                    continue
                queue.append((code, "buyback", 1.0))
                bb_budget -= 1
        n_bb = len(queue)

        slot_budget = variant.max_positions
        if variant.regime_guard:
            if day in market_risk_off:
                slot_budget = 0  # guard B: 0050 below MA120 -> no new entries
            elif theme_risk_off:
                slot_budget = variant.max_positions // 2  # guard A: halve exposure to new names
        free = slot_budget - held_net - len(pending_entries) - len(queue)
        if variant.max_new_per_day is not None:
            free = min(free, variant.max_new_per_day)
        if free > 0:
            if latest_scored is not None and idx - latest_scored_idx <= CANDIDATE_MAX_AGE:
                theme_of = (
                    dict(zip(latest_scored["company_code"], latest_scored["theme_id"]))
                    if "theme_id" in latest_scored
                    else {}
                )
                tilt_of: dict[str, float] = {}
                if variant.weight_mode == "score" and "score" in latest_scored:
                    med = float(pd.to_numeric(latest_scored["score"], errors="coerce").median())
                    if med > 0:
                        tilt_of = {
                            c: float(np.clip(s / med, 0.6, 1.6))
                            for c, s in zip(latest_scored["company_code"], latest_scored["score"])
                            if pd.notna(s)
                        }
                elif variant.weight_mode == "inv_atr" and "atr20_pct" in latest_scored:
                    atr = pd.to_numeric(latest_scored["atr20_pct"], errors="coerce")
                    med = float(atr.median())
                    if np.isfinite(med) and med > 0:
                        tilt_of = {
                            c: float(np.clip(med / a, 0.6, 1.6))
                            for c, a in zip(latest_scored["company_code"], atr)
                            if pd.notna(a) and a > 0
                        }
                theme_counts: dict[str, int] = {}
                if variant.theme_cap is not None:
                    for held, pos in positions.items():
                        if held in pending_exits:
                            continue
                        theme = theme_of.get(held)
                        if theme is not None:
                            theme_counts[theme] = theme_counts.get(theme, 0) + 1
                for code in latest_scored["company_code"]:
                    if code in positions or code in pending_exits:
                        continue
                    if idx - cooldown.get(code, -(10**9)) <= COOLDOWN_DAYS:
                        continue
                    if any(code == q for q, _src, _t in queue):
                        continue
                    if variant.entry_veto != "none":
                        # battle 14: exit gate half-armed at entry -> no seat.
                        # Same data source as the thesis exit (latest_thesis),
                        # so entry and exit judge by identical evidence.
                        met_v = latest_thesis.get(code) or {}
                        inst_v = met_v.get("inst_20d")
                        if inst_v is not None and inst_v < 0:
                            continue
                        if variant.entry_veto == "inst_yoy":
                            yoy3_v = met_v.get("yoy_3m")
                            if yoy3_v is not None and yoy3_v < 0.0:
                                continue
                    if variant.theme_cap is not None:
                        theme = theme_of.get(code)
                        if theme is not None and theme_counts.get(theme, 0) >= variant.theme_cap:
                            continue
                        if theme is not None:
                            theme_counts[theme] = theme_counts.get(theme, 0) + 1
                    if len(queue) - n_bb >= free:
                        break
                    queue.append((code, "refresh", tilt_of.get(code, 1.0)))
        pending_entries.extend(queue[: n_bb + max(free, 0)])

        nav_rows.append(
            {"date": day, "nav": cash + sum(p.value for p in positions.values()), "active": len(positions)}
        )

    daily = pd.DataFrame(nav_rows)
    trades = pd.DataFrame(trade_rows)
    if state_sink is not None:
        last_day = days[-1]
        closes_last = close_by_day.get(last_day, {})
        nav_final = cash + sum(p.value for p in positions.values())
        state_sink.update(
            {
                "as_of": last_day,
                "nav": nav_final,
                "cash_weight": cash / nav_final if nav_final > 0 else 1.0,
                "theme_risk_off": theme_risk_off,
                "market_risk_off_today": last_day in market_risk_off,
                "positions": [
                    {
                        "code": c,
                        "entry_date": days[p.entry_idx],
                        "days_held": len(days) - 1 - p.entry_idx,
                        "entry_close": p.entry_close,
                        "peak_close": p.peak_close,
                        "last_close": closes_last.get(c),
                        "weight": p.value / nav_final if nav_final > 0 else 0.0,
                        "unrealized": (closes_last.get(c, p.entry_close) / p.entry_close - 1.0),
                        "source": p.source,
                    }
                    for c, p in positions.items()
                ],
            }
        )
    return daily, trades, total_traded / CAPITAL


def _run_variant(variant, args, out_prefix, trading_days, close_by_day, ret_by_day,
                 scored_by_refresh, scored_by_refresh_chips, thesis_ok_by_refresh,
                 buyback_by_day, adv_by_day, market_risk_off,
                 valuation_by_refresh, thesis_metrics_by_refresh, theme_dead_by_code):
    """單 variant 模擬 + emit-book/state 副作用(battle 18 round 2 重構抽出)。"""
    book_sink: dict[date, dict[str, float]] | None = (
        {} if args.emit_book == variant.name else None
    )
    state_sink: dict | None = {} if args.emit_book == variant.name else None
    daily, trades, turnover = simulate_event_variant(
        variant, trading_days, close_by_day, ret_by_day,
        scored_by_refresh_chips if variant.chips_score else scored_by_refresh,
        thesis_ok_by_refresh, buyback_by_day, adv_by_day=adv_by_day,
        market_risk_off=market_risk_off, book_sink=book_sink, state_sink=state_sink,
        valuation_by_refresh=valuation_by_refresh,
        thesis_metrics_by_refresh=thesis_metrics_by_refresh,
        theme_dead_by_code=theme_dead_by_code,
    )
    if state_sink:
        import json

        state_path = RESULTS / f"{out_prefix}_{variant.name}_state.json"
        state_path.write_text(
            json.dumps(state_sink, default=str, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        print(f"state -> {state_path}")
    if book_sink is not None:
        book_rows = [
            {"date": d, "company_code": c, "weight": w}
            for d, (book, traded) in sorted(book_sink.items())
            if traded
            for c, w in sorted(book.items())
        ]
        book_path = RESULTS / f"{out_prefix}_{variant.name}_book.csv"
        pd.DataFrame(book_rows).to_csv(book_path, index=False)
        tw_rows = [
            {"date": d, "company_code": c, "target_weight": w}
            for d, (book, _traded) in sorted(book_sink.items())
            for c, w in sorted(book.items())
        ]
        tw_path = RESULTS / f"{out_prefix}_{variant.name}_target_weights.csv"
        pd.DataFrame(tw_rows).to_csv(tw_path, index=False)
        print(f"book -> {book_path} ({len(book_rows)} rows, trade days only)")
        print(f"target_weights -> {tw_path} (daily, planner format)")
    return daily, trades, turnover


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default=None, help="optional backtest end date (default: data cutoff)")
    parser.add_argument("--registry", default=None, help="alternative thesis registry CSV path")
    parser.add_argument("--mode", choices=("registry", "mechanical"), default="registry")
    parser.add_argument("--activation-lag-days", type=int, default=0)
    parser.add_argument("--label", default=None)
    parser.add_argument("--max-positions", type=int, default=10)
    parser.add_argument(
        "--emit-book",
        default=None,
        help="variant name whose daily target book (trade days only) is written for the realistic-execution runner",
    )
    parser.add_argument(
        "--live-revenue",
        action="store_true",
        help=(
            "LIVE semantics: rows present in operating_revenue are treated as "
            "published now (in-window month usable before the 10th) and the "
            "data cutoff becomes an extra rolling refresh day"
        ),
    )
    parser.add_argument(
        "--ablate",
        default=None,
        help="battle 11: comma-separated score components to disable "
        "(conviction|theme_count|revenue|momentum|adv|inst|pe_pen|pb_pen|dd_pen|filters)",
    )
    parser.add_argument(
        "--pe-pen-mode",
        default="full",
        choices=("full", "extreme", "off"),
        help="battle 13: PE-penalty schedule (full=legacy, extreme=only PE>180+low-PE bonus)",
    )
    parser.add_argument(
        "--variants",
        default=None,
        help="comma-separated variant names to run (default: all); e.g. battle subsets",
    )
    parser.add_argument(
        "--grid-exit",
        default=None,
        help=(
            "battle 17 diagnostic: exit-rule grid over ONE base variant "
            "(requires --variants <base>); spec 'tp=0.4,0.6,none;trail=0.15,0.2;"
            "abs=0.1,0.15;time=30,50' — data loads once, all cells simulate in-process"
        ),
    )
    parser.add_argument("--weight-mode", default=None, choices=("equal", "score", "inv_atr"),
                        help="battle 18 stage 3: override variant weight_mode")
    parser.add_argument("--score-grid", default=None,
                        help='battle 18 round 2: JSON list of score configs, e.g. '
                             '[{"name":"nofilt","ablate":"filters"},{"name":"role20","role":20}] '
                             '— in-process 交叉(每 config 重算 scored 後跑全部 variants)')
    parser.add_argument("--sweep", action="store_true",
                        help="sweep 模式:不寫 per-cell daily/trades/png,summary 內建 boot_p5(固定 seed)")
    parser.add_argument("--regime-exit", action="store_true",
                        help="battle 18: regime-adaptive exits (bull: no TP/wide trail; bear: TP60/tight)")
    parser.add_argument("--theme-dead-exit", action="store_true",
                        help="battle 18: force-exit holdings whose themes all hit active_until")
    parser.add_argument("--role-bonus", type=float, default=0.0,
                        help="battle 18: score bonus for chokepoint_owner (0 = off)")
    parser.add_argument("--fresh-bonus", type=float, default=0.0,
                        help="battle 18: theme-freshness score bonus (0 = off)")
    parser.add_argument("--fresh-months", type=int, default=6,
                        help="battle 18: freshness window in months")
    args = parser.parse_args()

    if args.ablate:
        set_ablate(args.ablate.split(","))
    set_pe_pen_mode(args.pe_pen_mode)
    set_role_bonus(args.role_bonus)
    set_fresh(args.fresh_bonus, args.fresh_months)

    suffix = "" if args.activation_lag_days == 0 else f"_lag{args.activation_lag_days}"
    mode_tag = "" if args.mode == "registry" else "_mech"
    out_prefix = args.label or f"{OUT_PREFIX}{mode_tag}{suffix}"

    con = connect(read_only=True, register_raw_quarterly=False)  # 引擎不用該 view,省開機 ~7s
    try:
        cutoff = con.sql("select max(date) from daily_quote").fetchone()[0]
        if args.end:
            cutoff = min(cutoff, date.fromisoformat(args.end))
        start = date.fromisoformat(args.start)
        load_start = start - timedelta(days=420)

        if args.mode == "registry":
            registry = load_registry(Path(args.registry) if args.registry else REGISTRY)
            if args.activation_lag_days:
                registry["active_from"] = registry["active_from"].map(
                    lambda value: value + timedelta(days=args.activation_lag_days)
                )
            universe_codes = registry["company_code"].tolist()
        else:
            registry = None

        if args.mode == "registry":
            universe = load_universe(con, universe_codes)
        else:
            universe = load_universe_history(con)
        taxonomy = load_taxonomy(con, universe["company_code"].tolist())
        # 效能(2026-07-17):調整面板磁碟快取——back-adjustment(除權息/減資掃描)
        # 是單進程載入的最大殘餘項;同池同窗的格點掃描全部命中。key 含 cache.duckdb
        # mtime,資料世代一變即失效(不會吃到舊世代面板)。
        import hashlib
        import os as _os
        panel_cache = RESULTS / ".panel_cache"
        panel_cache.mkdir(parents=True, exist_ok=True)
        db_mtime = int(_os.path.getmtime(REPO_ROOT / "research" / "cache.duckdb"))
        _key = hashlib.sha1(
            ("|".join(sorted(universe["company_code"])) + f"|{load_start}|{cutoff}|{db_mtime}").encode()
        ).hexdigest()[:16]
        pf_path = panel_cache / f"pf_{_key}.parquet"
        dr_path = panel_cache / f"dr_{_key}.parquet"
        if pf_path.exists() and dr_path.exists():
            price_features = pd.read_parquet(pf_path)
            daily_returns = pd.read_parquet(dr_path)
        else:
            price_features, daily_returns = load_price_features(con, universe, load_start, cutoff)
            price_features.to_parquet(pf_path, index=False)
            daily_returns.to_parquet(dr_path, index=False)
        revenue = load_revenue_features(
            con, codes=universe["company_code"].tolist() if args.mode == "registry" else None
        )
        per = load_point_in_time_table(con, "stock_per_pbr", ["price_to_earning_ratio", "price_book_ratio"],
                                       codes=universe["company_code"].tolist())
        # 效能修(2026-07-17):法人流按池過濾 + rolling 留在 polars——原版全市場
        # ~590 萬行進 pandas 做 lambda rolling,是每進程載入時間的最大單一項。
        uni_sql_flows = ",".join(f"'{c}'" for c in sorted(set(universe["company_code"])))
        flows = (
            con.sql(
                f"SELECT date, company_code, total_difference AS inst_diff "
                f"FROM daily_trading_details WHERE company_code IN ({uni_sql_flows})"
            )
            .pl()
            .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
            .sort(["company_code", "date"])
            .with_columns(
                pl.col("inst_diff").rolling_sum(20, min_samples=5)
                .over("company_code").alias("inst_20d")
            )
            .to_pandas()
        )
        buybacks = load_buyback_events(con)

        # Battle 4 (chips 2.0) features, universe-restricted for size.
        uni_sql = ",".join(f"'{c}'" for c in sorted(set(universe["company_code"])))
        sbl = (
            con.sql(
                f"SELECT date, company_code, daily_balance FROM sbl_borrowing WHERE company_code IN ({uni_sql})"
            )
            .pl()
            .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
            .to_pandas()
            .sort_values(["company_code", "date"])
        )
        prev20 = sbl.groupby("company_code")["daily_balance"].transform(lambda s: s.shift(20))
        sbl["sbl_chg_20d"] = np.where(prev20 > 0, sbl["daily_balance"] / prev20 - 1.0, np.nan)
        fhr = (
            con.sql(
                f"SELECT date, company_code, foreign_held_ratio FROM foreign_holding_ratio WHERE company_code IN ({uni_sql})"
            )
            .pl()
            .with_columns(pl.col("company_code").cast(pl.Utf8).str.zfill(4))
            .to_pandas()
            .sort_values(["company_code", "date"])
        )
        fhr["fhr_chg_60d"] = fhr.groupby("company_code")["foreign_held_ratio"].transform(
            lambda s: s - s.shift(60)
        )

        if args.live_revenue:
            # Presence == published: clamp future report_dates down to the
            # cutoff so the freshest month scores today (backtests never set
            # this flag — their validated deadline convention is unchanged).
            revenue = revenue.copy()
            revenue.loc[revenue["report_date"] > cutoff, "report_date"] = cutoff

        px = price_features.copy()
        px["_date"] = pd.to_datetime(px["date"]).dt.date

        # Full-market buyback channel (trials ledger battle 3): extend the price
        # panel with announce-window codes outside the thesis universe so the
        # counter-cyclical bb channel can trade the whole market.
        bb_codes = set(universe["company_code"])
        bb_window = buybacks[
            (buybacks["announce_date"] >= start) & (buybacks["announce_date"] <= cutoff)
        ]
        # 效能(2026-07-17):全市場 buyback extra 面板只在有 variant 用 buyback 通道
        # 時才載(戰役三已否決該通道,champion 不用——原本每進程白算 4 年全市場面板)
        _needs_bb = any(v.use_buyback for v in VARIANTS if not args.variants
                        or v.name in {n.strip() for n in (args.variants or "").split(",")})
        extra_codes = sorted(set(bb_window["company_code"]) - bb_codes) if _needs_bb else []
        if extra_codes:
            extra_uni = load_universe(con, extra_codes)
            if not extra_uni.empty:
                extra_px, _ = load_price_features(con, extra_uni, load_start, cutoff)
                extra_px["_date"] = pd.to_datetime(extra_px["date"]).dt.date
                px = pd.concat([px, extra_px], ignore_index=True)
                bb_codes |= set(extra_uni["company_code"])

        trading_days = sorted(d for d in px["_date"].unique() if start <= d <= cutoff)
        refresh_days = build_refresh_days(trading_days, start, cutoff)
        if args.live_revenue and trading_days and trading_days[-1] not in refresh_days:
            refresh_days.append(trading_days[-1])  # rolling refresh at cutoff
        day_index = {d: i for i, d in enumerate(trading_days)}

        window = px[px["_date"].isin(set(trading_days))]
        close_by_day: dict[date, dict[str, float]] = {
            d: g.set_index("company_code")["close"].to_dict() for d, g in window.groupby("_date")
        }
        ret_by_day: dict[date, dict[str, float]] = {
            d: g.set_index("company_code")["ret_1d"].fillna(0.0).to_dict() for d, g in window.groupby("_date")
        }
        adv_by_day: dict[date, dict[str, float]] = {
            d: g.set_index("company_code")["adv20"].to_dict() for d, g in window.groupby("_date")
        }

        bench = total_return_series(
            con, "0050", (start - timedelta(days=250)).isoformat(), cutoff.isoformat(), market="twse"
        ).to_pandas()
        bench = bench.sort_values("date").reset_index(drop=True)
        bench["ma120"] = bench["adj_close"].rolling(120).mean()
        market_risk_off = set(
            pd.to_datetime(bench.loc[bench["adj_close"] < bench["ma120"], "date"]).dt.date
        )

        # Scored candidate lists + thesis health on each refresh day (PIT as-of joins).
        scored_by_refresh: dict[date, pd.DataFrame] = {}
        scored_by_refresh_chips: dict[date, pd.DataFrame] = {}
        thesis_ok_by_refresh: dict[date, dict[str, bool]] = {}
        thesis_metrics_by_refresh: dict[date, dict[str, dict]] = {}
        valuation_by_refresh: dict[date, dict[str, tuple[float, float]]] = {}
        joined_by_refresh: dict[date, pd.DataFrame] = {}
        pick_rows: list[pd.DataFrame] = []
        for day in refresh_days:
            tax_day = row_latest_before(taxonomy, day, "effective_date")
            tax_day = tax_day[(tax_day["is_financial"] == False) & (tax_day["is_special_category"] == False)]
            rev_day = row_latest_before(revenue, day, "report_date")
            if args.mode == "registry":
                active = active_registry_for_day(registry, day)
            else:
                active = mechanical_registry_for_day(day, tax_day, rev_day)
            if active.empty:
                continue
            px_day = px[px["_date"] == day]
            per_day = row_latest_before(per, day, "date")
            flow_day = row_latest_before(flows[["date", "company_code", "inst_20d"]], day, "date")
            joined = (
                active.merge(tax_day, on="company_code", how="inner")
                .merge(px_day, on="company_code", how="inner")
                .merge(rev_day, on="company_code", how="left", suffixes=("", "_rev"))
                .merge(per_day, on="company_code", how="left")
                .merge(flow_day, on="company_code", how="left")
            )
            # battle 18: theme age (days since first admit) for the freshness axis.
            joined["theme_age_days"] = joined["first_active_from"].map(
                lambda v: (day - v).days if pd.notna(v) else 99_999
            )
            scored = score_candidates(joined)
            if scored.empty:
                continue
            joined_by_refresh[day] = joined  # battle 18 round 2: score-grid 重算原料
            scored_by_refresh[day] = scored.head(40).reset_index(drop=True)

            # chips 2.0 scoring (battle 4): pre-committed fixed weights, no grid.
            sbl_day = row_latest_before(sbl[["date", "company_code", "sbl_chg_20d"]], day, "date")
            fhr_day = row_latest_before(fhr[["date", "company_code", "fhr_chg_60d"]], day, "date")
            joined_c = joined.merge(
                sbl_day.drop(columns=["date"]), on="company_code", how="left"
            ).merge(fhr_day.drop(columns=["date"]), on="company_code", how="left")
            scored_c = score_candidates(joined_c)
            if not scored_c.empty:
                adj = (
                    pd.to_numeric(scored_c.get("sbl_chg_20d"), errors="coerce").clip(0, 1).fillna(0) * 3.0
                    + pd.to_numeric(scored_c.get("fhr_chg_60d"), errors="coerce").clip(-3, 3).fillna(0) * 0.8
                )
                scored_c = scored_c.assign(score=scored_c["score"] + adj).sort_values(
                    "score", ascending=False
                )
                scored_by_refresh_chips[day] = scored_c.head(40).reset_index(drop=True)
            thesis_ok_by_refresh[day] = {
                str(row.company_code): not (pd.notna(row.yoy_3m) and float(row.yoy_3m) < 0.0)
                for row in rev_day.itertuples(index=False)
            }
            met_map = rev_day[["company_code", "yoy_3m", "monthly_revenue_yoy"]].merge(
                flow_day[["company_code", "inst_20d"]], on="company_code", how="outer"
            )
            thesis_metrics_by_refresh[day] = {
                str(r.company_code): {
                    "yoy_3m": float(r.yoy_3m) if pd.notna(r.yoy_3m) else None,
                    "yoy_1m": float(r.monthly_revenue_yoy) if pd.notna(r.monthly_revenue_yoy) else None,
                    "inst_20d": float(r.inst_20d) if pd.notna(r.inst_20d) else None,
                }
                for r in met_map.itertuples(index=False)
            }
            val_map = rev_day[["company_code", "yoy_3m"]].merge(
                per_day[["company_code", "price_to_earning_ratio"]], on="company_code", how="outer"
            )
            valuation_by_refresh[day] = {
                str(r.company_code): (
                    float(r.price_to_earning_ratio) if pd.notna(r.price_to_earning_ratio) else None,
                    float(r.yoy_3m) if pd.notna(r.yoy_3m) else None,
                )
                for r in val_map.itertuples(index=False)
            }
            pick_rows.append(scored.head(20).assign(signal_date=day))

        # Buyback support events -> candidate codes on announce day (traded T+1 by engine).
        buyback_by_day: dict[date, list[str]] = {}
        uni_codes = bb_codes
        px_lookup = px.set_index(["_date", "company_code"])
        for row in buybacks.itertuples(index=False):
            d = row.announce_date
            if d is None or d not in day_index or row.company_code not in uni_codes:
                continue
            try:
                feat = px_lookup.loc[(d, row.company_code)]
            except KeyError:
                continue
            ret60 = float(feat.get("ret_60d", np.nan))
            raw_close = float(feat.get("raw_close", np.nan))
            adv20 = float(feat.get("adv20", np.nan))
            price_high = float(row.price_high) if pd.notna(row.price_high) else np.nan
            if (
                np.isfinite(ret60)
                and ret60 <= -0.15
                and np.isfinite(price_high)
                and np.isfinite(raw_close)
                and price_high >= raw_close
                and np.isfinite(adv20)
                and adv20 >= 50_000_000
            ):
                buyback_by_day.setdefault(d, []).append(row.company_code)

        variants = tuple(replace(v, max_positions=args.max_positions) for v in VARIANTS)
        if args.variants:
            wanted = {name.strip() for name in args.variants.split(",") if name.strip()}
            unknown = wanted - {v.name for v in variants}
            if unknown:
                raise ValueError(f"unknown variant names: {sorted(unknown)}")
            variants = tuple(v for v in variants if v.name in wanted)
        if args.grid_exit:
            if len(variants) != 1:
                raise ValueError("--grid-exit requires exactly one base variant via --variants")
            base = variants[0]
            axes: dict[str, list] = {}
            for part in args.grid_exit.split(";"):
                key, raw = part.split("=")
                axes[key.strip()] = [
                    None if v.strip().lower() in ("none", "inf") else float(v)
                    for v in raw.split(",")
                ]
            fmt = lambda v: "none" if v is None else f"{v:g}"
            variants = tuple(
                replace(
                    base,
                    name=f"g_tp{fmt(tp)}_tr{fmt(tr)}_ab{fmt(ab)}_td{fmt(td)}",
                    rules=replace(
                        base.rules,
                        take_profit=tp,
                        trail=tr,
                        abs_stop=ab,
                        time_days=None if td is None else int(td),
                    ),
                )
                for tp in axes.get("tp", [base.rules.take_profit])
                for tr in axes.get("trail", [base.rules.trail])
                for ab in axes.get("abs", [base.rules.abs_stop])
                for td in axes.get("time", [base.rules.time_days])
            )
            print(f"grid-exit: {len(variants)} cells over base {base.name}")
        if args.weight_mode:
            variants = tuple(replace(v, name=f"{v.name}_w{args.weight_mode}", weight_mode=args.weight_mode)
                             for v in variants)
        if args.regime_exit:
            variants = tuple(replace(v, name=f"{v.name}_rgx", regime_exit=True) for v in variants)
        if args.theme_dead_exit:
            variants = tuple(replace(v, name=f"{v.name}_tdx", theme_dead_exit=True) for v in variants)
        theme_dead_by_code: dict[str, date] = {}
        if args.mode == "registry" and registry is not None:
            for code, g in registry.groupby("company_code"):
                if g["active_until"].notna().all():
                    theme_dead_by_code[str(code)] = max(g["active_until"])
        # battle 18 round 2: score-config 外層迴圈(in-process 全交叉)。
        # None = 沿用 CLI 全域配置(生產路徑,行為不變)。
        score_cfgs: list[dict | None] = [None]
        if args.score_grid:
            import json as _json
            score_cfgs = _json.loads(args.score_grid)
            print(f"score-grid: {len(score_cfgs)} configs × {len(variants)} exit variants "
                  f"= {len(score_cfgs) * len(variants)} cells (in-process)")

        def _boot_p5(daily_df: pd.DataFrame) -> float:
            nav = daily_df.set_index("date")["nav"]
            m = nav.groupby(pd.Index(nav.index.astype(str)).str.slice(0, 7)).last() \
                   .pct_change().dropna().to_numpy()
            n = len(m)
            if n < 8:
                return float("nan")
            rng = np.random.default_rng(20260716)
            out = []
            for _ in range(1000):
                idx = (int(rng.integers(n)) + np.arange(n + 6)) % n
                samp = m[idx[:n]]
                out.append(float(np.prod(1 + samp) ** (12 / n) - 1))
            return float(np.percentile(out, 5))

        summaries: list[dict[str, object]] = []
        daily_paths: dict[str, Path] = {}
        for cfg in score_cfgs:
            cfg_tag = ""
            active_scored = scored_by_refresh
            if cfg is not None:
                set_ablate((cfg.get("ablate") or "").split(",") if cfg.get("ablate") else [])
                set_role_bonus(cfg.get("role", 0.0))
                set_fresh(cfg.get("fresh", 0.0), cfg.get("fresh_m", 6))
                set_conv_weight(cfg.get("conv_w", 8.0))
                cfg_tag = f"{cfg['name']}|"
                active_scored = {}
                for d, j in joined_by_refresh.items():
                    s = score_candidates(j)
                    if not s.empty:
                        active_scored[d] = s.head(40).reset_index(drop=True)
            for variant in variants:
                daily, trades, turnover = _run_variant(
                    variant, args, out_prefix, trading_days, close_by_day, ret_by_day,
                    active_scored, scored_by_refresh_chips, thesis_ok_by_refresh,
                    buyback_by_day, adv_by_day, market_risk_off,
                    valuation_by_refresh, thesis_metrics_by_refresh, theme_dead_by_code,
                )
                cell_name = f"{cfg_tag}{variant.name}"
                if args.sweep:
                    row = summarize_nav(cell_name, daily, turnover, len(active_scored))
                    row["boot_p5"] = _boot_p5(daily)
                    row["n_trades"] = len(trades)
                    summaries.append(row)
                    continue
                path = RESULTS / f"{out_prefix}_{variant.name}_daily.csv"
                daily.to_csv(path, index=False)
                trades.to_csv(RESULTS / f"{out_prefix}_{variant.name}_trades.csv", index=False)
                daily_paths[variant.name] = path
                row = summarize_nav(cell_name, daily, turnover, len(active_scored))
                extra = nav_metrics(pl.from_pandas(daily[["date", "nav"]]))
                row.update({k: v for k, v in extra.items() if k in ("ulcer_index", "upi", "k_ratio", "tail_ratio")})
                if not trades.empty:
                    row.update(trade_distribution_metrics(trades["ret"].tolist()))
                    row["n_trades"] = len(trades)
                    row["exit_mix"] = trades["reason"].value_counts().to_dict()
                summaries.append(row)

        for code, market, name in (("0050", "twse", "hold_0050"), ("2330", "twse", "hold_2330")):
            daily = benchmark_nav(con, code, market, trading_days[0], trading_days[-1], name)
            path = RESULTS / f"{out_prefix}_{name}_daily.csv"
            daily.to_csv(path, index=False)
            daily_paths[name] = path
            summaries.append(summarize_nav(name, daily, 0.0, 0))

        summary = pd.DataFrame(summaries).sort_values("cagr", ascending=False)
        summary.to_csv(RESULTS / f"{out_prefix}_summary.csv", index=False)
        if pick_rows:
            pd.concat(pick_rows).to_csv(RESULTS / f"{out_prefix}_picks.csv", index=False)

        if not args.sweep:
            fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
            for name in summary.head(6)["name"]:
                if name not in daily_paths:
                    continue
                daily = pd.read_csv(daily_paths[name], parse_dates=["date"])
                axes[0].plot(daily["date"], daily["nav"] / daily["nav"].iloc[0], label=name)
                axes[1].plot(daily["date"], daily["nav"] / daily["nav"].cummax() - 1.0, label=name)
            axes[0].set_title(f"Serenity event engine v1 ({args.mode}{suffix}): NAV")
            axes[0].grid(True, alpha=0.3)
            axes[0].legend(fontsize=8)
            axes[1].set_title("Drawdown")
            axes[1].grid(True, alpha=0.3)
            fig.tight_layout()
            chart = RESULTS / f"{out_prefix}_overview.png"
            fig.savefig(chart, dpi=160)
            plt.close(fig)

        print(f"data_cutoff={cutoff} mode={args.mode} lag={args.activation_lag_days}")
        print(f"window={trading_days[0]}~{trading_days[-1]} refreshes={len(scored_by_refresh)}")
        print(f"buyback_event_days={len(buyback_by_day)}")
        cols = [
            "name",
            "cagr",
            "recent_1y_cagr",
            "sharpe",
            "sortino",
            "mdd",
            "calmar",
            "total_turnover",
            "avg_active",
            "n_trades",
        ]
        show = summary.copy()
        for c in cols:
            if c not in show:
                show[c] = np.nan
        print(
            show[cols].to_string(
                index=False,
                formatters={
                    "cagr": "{:.2%}".format,
                    "recent_1y_cagr": "{:.2%}".format,
                    "sharpe": "{:.3f}".format,
                    "sortino": "{:.3f}".format,
                    "mdd": "{:.2%}".format,
                    "calmar": "{:.2f}".format,
                    "total_turnover": "{:.2f}x".format,
                    "avg_active": "{:.1f}".format,
                },
            )
        )
        exit_mix = {row["name"]: row.get("exit_mix") for row in summaries if row.get("exit_mix")}
        for name, mix in exit_mix.items():
            print(f"exit_mix[{name}] = {mix}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
