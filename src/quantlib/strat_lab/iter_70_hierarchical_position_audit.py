"""iter_70 - hierarchical position audit for the Iter67 NAV lineage.

Iter69 showed that the flat target-book rebuild does not reconcile to the
source NAV lineage. This audit answers only the position-count question with a
hierarchical book reconstruction:

  - rebuild the breakout_risk event lifecycle positions instead of using a
    simplified daily target list;
  - preserve annual sleeve rebalancing for Iter42 / Iter44;
  - propagate the same meta-switch states used by the NAV-lineage scripts;
  - count the final Iter67 union without pretending this is an order-level
    performance backtest.
"""
from __future__ import annotations

import math
import os
import sys
from datetime import date
from pathlib import Path

import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.dirname(__file__))

from iter_40_research_campaign import CampaignConfig, EventStore, build_event_candidates, load_panel, risk_multipliers  # noqa: E402
from iter_44_idle_fallback import load_market_0050  # noqa: E402
from iter_52_ownership_flow_alpha import FlowConfig, add_flow_scores, build_targets as build_flow_targets, fetch_extra_features  # noqa: E402
from iter_57_cost_aware_switch import SwitchSpec, simulate_switch  # noqa: E402
from iter_68_position_level_bridge import (  # noqa: E402
    Book,
    BookByDate,
    build_iter64_targets,
    expand_targets,
    load_pick_targets,
    merge_books,
    state_by_date,
)
from iter_69_production_audit_and_ablation import (  # noqa: E402
    build_core63_sharpe_targets,
    build_iter56_targets,
    build_iter61_targets,
    build_iter62_targets,
    build_iter67_state,
)


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_70_hierarchical_position_audit"
CAPITAL = 1_000_000.0
COMMISSION = 0.000285
SELL_TAX = 0.003


def nav_returns(path: Path) -> pl.DataFrame:
    return (
        pl.read_csv(path, try_parse_dates=True)
        .sort("date")
        .select(["date", "nav"])
        .with_columns(pl.col("nav").pct_change().fill_null(0.0).alias("ret"))
        .select(["date", "ret"])
    )


def event_lifecycle_books(
    days: list[date],
    panel: pl.DataFrame,
    market: dict[str, dict[date, float]],
    cfg: CampaignConfig,
) -> BookByDate:
    store = EventStore(panel)
    candidates = build_event_candidates(panel, cfg)
    risk = risk_multipliers(days, market, cfg.risk_mode)
    cash = CAPITAL
    positions: dict[str, dict[str, float | date]] = {}
    pending_entries: list[str] = []
    pending_exits: set[str] = set()
    books: BookByDate = {}

    def value_at(d: date, code: str, px_col: str) -> float:
        pos = positions[code]
        row = store.row(d, code)
        px = row.get(px_col) if row else None
        if px is None or px <= 0 or not math.isfinite(px):
            px = float(pos.get("last_close", 0.0))
        return float(pos["shares"]) * px

    for d in days:
        gross_mult = risk.get(d, 1.0)
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
            if px > 0:
                cash += float(pos["shares"]) * px * (1 - SELL_TAX - COMMISSION)
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
                atr_pct = row.get("atr_pct", math.nan)
                trail_pct = min(max((atr_pct if math.isfinite(atr_pct) else 0.04) * 3.0, 0.10), 0.25)
                positions[code] = {
                    "shares": buy_value / px,
                    "entry_px": px,
                    "high_water": px,
                    "trail_pct": trail_pct,
                    "last_close": px,
                    "entry_date": d,
                }
        pending_entries = []

        nav_close = cash
        close_values: dict[str, float] = {}
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
            value = float(pos["shares"]) * close
            close_values[code] = value
            nav_close += value
            ma200 = row.get("ma200", math.nan) if row else math.nan
            yoy = row.get("latest_yoy", math.nan) if row else math.nan
            trail = close / float(pos["high_water"]) - 1 <= -float(pos["trail_pct"])
            trend_fail = math.isfinite(ma200) and close < ma200
            rev_fail = math.isfinite(yoy) and yoy < (0.0 if cfg.family != "spike_precondition" else -20.0)
            if trail or trend_fail or rev_fail:
                pending_exits.add(code)

        books[d] = {code: value / nav_close for code, value in close_values.items() if nav_close > 0 and value > 0}

        if gross_mult > 0 and len(positions) < cfg.topn:
            for code in candidates.get(d, []):
                if code not in positions and code not in pending_entries:
                    pending_entries.append(code)
                if len(positions) + len(pending_entries) >= cfg.topn:
                    break
    return books


def annual_blend_books(
    days: list[date],
    books_a: BookByDate,
    ret_a: pl.DataFrame,
    books_b: BookByDate,
    ret_b: pl.DataFrame,
    weight_a: float,
) -> BookByDate:
    returns_a = dict(zip(ret_a["date"].to_list(), ret_a["ret"].to_list(), strict=True))
    returns_b = dict(zip(ret_b["date"].to_list(), ret_b["ret"].to_list(), strict=True))
    out: BookByDate = {}
    nav = CAPITAL
    current_year: int | None = None
    cap_a = 0.0
    cap_b = 0.0
    for d in days:
        if current_year != d.year:
            current_year = d.year
            cap_a = nav * weight_a
            cap_b = nav * (1.0 - weight_a)
        cap_a *= 1.0 + returns_a.get(d, 0.0)
        cap_b *= 1.0 + returns_b.get(d, 0.0)
        nav = cap_a + cap_b
        out[d] = merge_books(
            [
                (cap_a / nav if nav > 0 else 0.0, books_a.get(d, {})),
                (cap_b / nav if nav > 0 else 0.0, books_b.get(d, {})),
            ]
        )
    return out


def iter44_q3_trend_books(days: list[date], q3: BookByDate, ret_q3: pl.DataFrame, event: BookByDate, ret_event: pl.DataFrame) -> BookByDate:
    market = load_market_0050()
    mkt_up = dict(zip(market["date"].to_list(), market["mkt_up"].to_list(), strict=True))
    returns_q3 = dict(zip(ret_q3["date"].to_list(), ret_q3["ret"].to_list(), strict=True))
    returns_event = dict(zip(ret_event["date"].to_list(), ret_event["ret"].to_list(), strict=True))
    out: BookByDate = {}
    nav = CAPITAL
    current_year: int | None = None
    cap_q = 0.0
    cap_s = 0.0
    previous_event_active = False
    for d in days:
        if current_year != d.year:
            current_year = d.year
            cap_q = nav * 0.74
            cap_s = nav * 0.26
        event_active = len(event.get(d, {})) > 0
        event_invested = event_active or previous_event_active
        if event_invested:
            ret_s = returns_event.get(d, 0.0)
            sat_book = event.get(d, {})
        elif mkt_up.get(d, True):
            ret_s = returns_q3.get(d, 0.0)
            sat_book = q3.get(d, {})
        else:
            ret_s = 0.0
            sat_book = {}
        cap_q *= 1.0 + returns_q3.get(d, 0.0)
        cap_s *= 1.0 + ret_s
        nav = cap_q + cap_s
        out[d] = merge_books(
            [
                (cap_q / nav if nav > 0 else 0.0, q3.get(d, {})),
                (cap_s / nav if nav > 0 else 0.0, sat_book),
            ]
        )
        previous_event_active = event_active
    return out


def build_squeeze_books(days: list[date], panel: pl.DataFrame) -> BookByDate:
    extra = fetch_extra_features()
    flow_panel = (
        panel.join(extra, on=["date", "company_code"], how="left")
        .with_columns(
            [
                pl.col("outstanding_shares").fill_null(0),
                pl.col("foreign_held_ratio").fill_null(0.0),
                pl.col("foreign_chg20").fill_null(0.0),
                pl.col("foreign_chg60").fill_null(0.0),
                pl.col("margin_balance").fill_null(0),
                pl.col("short_balance").fill_null(0),
                pl.col("sbl_balance").fill_null(0),
                pl.col("margin_ratio").fill_null(0.0),
                pl.col("short_ratio").fill_null(0.0),
                pl.col("sbl_ratio").fill_null(0.0),
                pl.col("pbr").fill_null(999.0),
                pl.col("dividend_yield").fill_null(0.0),
                pl.col("pe").fill_null(999.0),
                pl.col("buyback_pct").fill_null(0.0),
                pl.col("buyback_executed_shares").fill_null(0),
            ]
        )
        .pipe(add_flow_scores)
        .rechunk()
    )
    raw = build_flow_targets(
        flow_panel,
        days,
        FlowConfig(
            name="squeeze_top5_monthly",
            score_kind="squeeze_score",
            topn=5,
            rebalance="monthly",
            min_adv=80_000_000.0,
            min_roa=0.00,
            min_gm=0.05,
            min_fscore=2,
            require_short_pressure=True,
            require_trend=True,
        ),
    )
    return expand_targets(days, raw, persist=True)


def build_iter57_books(days: list[date], iter44: BookByDate, squeeze: BookByDate) -> BookByDate:
    from iter_54_cross_family_switch import load_switch_base  # noqa: WPS433

    switch_base = load_switch_base({"iter44_w74_q3_trend", "iter52_squeeze_top5"}).with_columns(
        (pl.col("gate_mkt_mom63") & pl.col("gate_q3_ma50") & pl.col("gate_iter52_squeeze_top5_ma50")).alias(
            "gate_mkt_mom63_q3_ma50_sq_ma50"
        )
    )
    state = state_by_date(
        simulate_switch(
            switch_base,
            SwitchSpec(
                name="iter70_iter57_position_state",
                defense="iter44_w74_q3_trend",
                attack="iter52_squeeze_top5",
                entry_gate="gate_mkt_mom63_q3_ma50_sq_ma50",
                exit_gate="gate_mkt_mom63_q3_ma50_sq_ma50",
                schedule="monthly",
                min_hold_days=20,
                confirm_days=3,
            ),
        )
    )
    return {d: squeeze.get(d, {}) if state.get(d) == "iter52_squeeze_top5" else iter44.get(d, {}) for d in days}


def summarize(name: str, books: BookByDate) -> dict[str, object]:
    counts = {d: len(book) for d, book in books.items()}
    max_active = max(counts.values())
    example_dates = [d.isoformat() for d, count in counts.items() if count == max_active][:5]
    return {
        "name": name,
        "days": len(counts),
        "max_active_positions": max_active,
        "days_over_5": sum(count > 5 for count in counts.values()),
        "days_over_6": sum(count > 6 for count in counts.values()),
        "days_over_10": sum(count > 10 for count in counts.values()),
        "example_max_dates": ",".join(example_dates),
    }


def write_iter67_daily_counts(iter67: BookByDate, state: dict[date, str]) -> None:
    rows = []
    for d, book in iter67.items():
        rows.append(
            {
                "date": d,
                "selected": "attack64" if state.get(d) == "attack" else "core63",
                "active_positions": len(book),
                "total_weight": float(sum(book.values())),
            }
        )
    pl.DataFrame(rows).write_csv(RESULTS / f"{OUT_PREFIX}_iter67_daily_counts.csv")


def build_hierarchical_books() -> tuple[list[date], pl.DataFrame, dict[str, BookByDate], dict[date, str]]:
    panel, days, market = load_panel()
    q3 = expand_targets(days, load_pick_targets(RESULTS / "iter_13_iter50_q3_mcap_monthly_picks.csv"), persist=True)
    ret_q3 = nav_returns(RESULTS / "latest_q3_daily.csv")
    event_cfg = CampaignConfig(
        name="breakout_risk_ma200_cash_top3",
        family="breakout_risk",
        score_kind="rev_accel",
        topn=3,
        risk_mode="ma200_cash",
        min_yoy=30.0,
        breakout_lkb=90,
        breakout_ratio=0.98,
        vol_mult=1.5,
        min_roa=0.02,
        min_gm=0.10,
        min_fscore=3,
        max_atr=0.10,
    )
    event = event_lifecycle_books(days, panel, market, event_cfg)
    ret_event = nav_returns(RESULTS / "iter_40_breakout_risk_ma200_cash_top3_daily.csv")
    squeeze = build_squeeze_books(days, panel)

    iter42 = annual_blend_books(days, q3, ret_q3, event, ret_event, 0.59)
    iter44 = iter44_q3_trend_books(days, q3, ret_q3, event, ret_event)
    iter57 = build_iter57_books(days, iter44, squeeze)
    iter56, _iter56_state = build_iter56_targets(days, iter44, squeeze)
    iter61, _iter61_state = build_iter61_targets(days, iter42, iter57)
    iter62, _iter62_state = build_iter62_targets(days, iter61, iter56)
    core63 = build_core63_sharpe_targets(days, iter62)
    attack64 = build_iter64_targets(days, iter42, iter57)
    iter67_state = build_iter67_state()
    iter67 = {d: attack64.get(d, {}) if iter67_state.get(d) == "attack" else core63.get(d, {}) for d in days}

    return days, panel, {
        "q3": q3,
        "event_breakout_risk": event,
        "iter42_hierarchical": iter42,
        "iter44_hierarchical": iter44,
        "squeeze": squeeze,
        "iter57_hierarchical": iter57,
        "iter56_hierarchical": iter56,
        "iter61_hierarchical": iter61,
        "iter62_hierarchical": iter62,
        "core63_hierarchical": core63,
        "attack64_hierarchical": attack64,
        "iter67_hierarchical": iter67,
    }, iter67_state


def main() -> None:
    print("[iter70] building hierarchical position audit", flush=True)
    _days, _panel, books_by_name, iter67_state = build_hierarchical_books()
    summary = pl.DataFrame([summarize(name, books) for name, books in books_by_name.items()])
    summary.write_csv(RESULTS / f"{OUT_PREFIX}_summary.csv")
    write_iter67_daily_counts(books_by_name["iter67_hierarchical"], iter67_state)
    print(summary.to_pandas().to_string(index=False), flush=True)
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_summary.csv'}", flush=True)
    print(f"Saved: {RESULTS / f'{OUT_PREFIX}_iter67_daily_counts.csv'}", flush=True)


if __name__ == "__main__":
    main()
