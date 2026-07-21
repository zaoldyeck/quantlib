"""iter_58 - unified holdings-level PM allocator.

iter55/57 showed that switching complete sleeves can create attractive paper
CAGR, but the idea is fragile after charging real whole-sleeve switch friction.
This iteration restarts from the portfolio mandate instead:

  - one live book, max 10 single-name holdings;
  - point-in-time total-return OHLCV and local feature cache;
  - signals known after close, entries/exits at the next open;
  - deterministic multi-factor candidate ranking, no fitted coefficients;
  - stock-level exits/replacements instead of whole-strategy rotation.

The only loop is the live portfolio state loop; candidate generation stays in
Polars and all expensive features are served from local cache / DuckDB.
"""
from __future__ import annotations

import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import (  # noqa: E402
    CAPITAL,
    COMMISSION,
    END,
    SELL_TAX,
    START,
    fetch_market_calendar,
    load_panel,
    validate_daily,
)
from iter_52_ownership_flow_alpha import add_flow_scores, fetch_extra_features  # noqa: E402


RESULTS = Path("research/strat_lab/results")


@dataclass(frozen=True)
class AllocatorConfig:
    name: str
    score: str
    max_positions: int
    schedule: str
    min_adv: float
    min_roa: float
    min_gm: float
    min_fscore: int
    min_score: float
    exit_score: float
    trend_gate: str
    market_gate: str
    min_yoy: float | None
    min_yoy_delta: float | None
    max_atr: float
    replace_gap: float
    min_hold_days: int
    trail_mult: float
    trail_min: float
    trail_max: float
    exit_ma: str
    exit_yoy: float


def z_expr(col: str, lo: float, hi: float) -> pl.Expr:
    return ((pl.col(col) - lo) / (hi - lo) * 2 - 1).clip(-1.0, 1.0)


def add_unified_scores(panel: pl.DataFrame) -> pl.DataFrame:
    """Add monotonic scores designed from PM intuition, not fitted weights."""
    return (
        panel.with_columns(
            [
                z_expr("latest_yoy", -10.0, 90.0).fill_null(0.0).alias("u_rev"),
                z_expr("yoy_delta", -30.0, 90.0).fill_null(0.0).alias("u_rev_accel"),
                z_expr("ret120", -0.20, 0.85).fill_null(0.0).alias("u_mom120"),
                z_expr("vol_ratio", 0.7, 3.0).fill_null(0.0).alias("u_volume"),
                z_expr("inst_flow20", -0.08, 0.20).fill_null(0.0).alias("u_inst"),
                z_expr("foreign_chg20", -1.5, 2.5).fill_null(0.0).alias("u_foreign"),
                (-z_expr("margin_ratio", 0.02, 0.13)).fill_null(0.0).alias("u_low_margin"),
                z_expr("short_ratio", 0.002, 0.055).fill_null(0.0).alias("u_short"),
                z_expr("sbl_ratio", 0.005, 0.12).fill_null(0.0).alias("u_sbl"),
                z_expr("industry_ret120", -0.20, 0.65).fill_null(0.0).alias("u_ind_mom"),
                z_expr("industry_yoy", -10.0, 55.0).fill_null(0.0).alias("u_ind_rev"),
                (-pl.col("atr_pct") / 0.09).clip(-1.0, 0.0).fill_null(0.0).alias("u_low_atr"),
                z_expr("pbr", 0.8, 7.0).fill_null(0.0).alias("u_expensive"),
            ]
        )
        .with_columns(
            [
                (
                    0.24 * pl.col("quality_score")
                    + 0.15 * pl.col("u_rev")
                    + 0.13 * pl.col("u_rev_accel")
                    + 0.14 * pl.col("u_mom120")
                    + 0.09 * pl.col("u_inst")
                    + 0.08 * pl.col("u_foreign")
                    + 0.07 * pl.col("u_low_margin")
                    + 0.06 * pl.col("u_ind_mom")
                    + 0.04 * pl.col("u_low_atr")
                ).alias("score_balanced_pm"),
                (
                    0.34 * pl.col("quality_score")
                    + 0.18 * pl.col("core_score")
                    + 0.12 * pl.col("u_mom120")
                    + 0.10 * pl.col("u_inst")
                    + 0.08 * pl.col("u_foreign")
                    + 0.08 * pl.col("u_low_margin")
                    + 0.06 * pl.col("u_ind_mom")
                    + 0.04 * pl.col("u_low_atr")
                ).alias("score_quality_flow"),
                (
                    0.22 * pl.col("u_rev")
                    + 0.20 * pl.col("u_rev_accel")
                    + 0.18 * pl.col("u_mom120")
                    + 0.14 * pl.col("u_volume")
                    + 0.10 * pl.col("quality_score")
                    + 0.08 * pl.col("u_inst")
                    + 0.08 * pl.col("u_ind_mom")
                ).alias("score_growth_accel"),
                (
                    0.20 * pl.col("u_mom120")
                    + 0.16 * pl.col("u_short")
                    + 0.14 * pl.col("u_sbl")
                    + 0.14 * pl.col("u_volume")
                    + 0.12 * pl.col("u_rev")
                    + 0.10 * pl.col("quality_score")
                    + 0.08 * pl.col("u_low_margin")
                    + 0.06 * pl.col("u_foreign")
                ).alias("score_squeeze_quality"),
                (
                    0.26 * pl.col("quality_score")
                    + 0.18 * pl.col("u_low_margin")
                    + 0.14 * (-pl.col("u_expensive"))
                    + 0.12 * pl.col("z_dividend").fill_null(0.0)
                    + 0.10 * pl.col("u_foreign")
                    + 0.10 * pl.col("u_inst")
                    + 0.10 * pl.col("u_mom120")
                ).alias("score_value_flow"),
            ]
        )
        .with_columns(
            [
                (pl.col("close") > pl.col("ma50")).fill_null(False).alias("gate_px_ma50"),
                (pl.col("close") > pl.col("ma100")).fill_null(False).alias("gate_px_ma100"),
                (pl.col("close") > pl.col("ma200")).fill_null(False).alias("gate_px_ma200"),
                (pl.col("ma50") > pl.col("ma200") * 0.98).fill_null(False).alias("gate_structural_trend"),
            ]
        )
    )


def add_market_gates(panel: pl.DataFrame, days: list[date], market: dict[str, dict[date, float]]) -> pl.DataFrame:
    close = np.asarray([market["close"].get(d, math.nan) for d in days], dtype=float)
    ma200 = np.asarray([market["ma200"].get(d, math.nan) for d in days], dtype=float)
    mom63 = np.full_like(close, False, dtype=bool)
    mom126 = np.full_like(close, False, dtype=bool)
    for i in range(len(close)):
        if i >= 63 and np.isfinite(close[i]) and np.isfinite(close[i - 63]):
            mom63[i] = close[i] / close[i - 63] - 1 > 0
        if i >= 126 and np.isfinite(close[i]) and np.isfinite(close[i - 126]):
            mom126[i] = close[i] / close[i - 126] - 1 > 0
    gates = pl.DataFrame(
        {
            "date": days,
            "mkt_ma200": np.where(np.isfinite(ma200), close > ma200, True),
            "mkt_mom63": mom63,
            "mkt_mom126": mom126,
        }
    ).with_columns(
        [
            (pl.col("mkt_ma200") | pl.col("mkt_mom63")).alias("mkt_ma200_or_mom63"),
            (pl.col("mkt_ma200") & pl.col("mkt_mom63")).alias("mkt_ma200_and_mom63"),
            (pl.col("mkt_mom63") | pl.col("mkt_mom126")).alias("mkt_mom63_or_126"),
        ]
    )
    return panel.join(gates, on="date", how="left").with_columns(
        [pl.col(c).fill_null(True) for c in gates.columns if c != "date"]
    )


def signal_dates_expr(schedule: str) -> pl.Expr:
    if schedule == "daily":
        return pl.lit(True)
    if schedule == "weekly":
        return pl.col("date").dt.weekday() == 5
    if schedule == "monthly":
        key = pl.col("date").dt.year() * 100 + pl.col("date").dt.month()
        return pl.col("date") == pl.col("date").min().over(key)
    raise ValueError(schedule)


def market_gate_expr(gate: str) -> pl.Expr:
    if gate == "none":
        return pl.lit(True)
    return pl.col(gate).fill_null(True)


def trend_gate_expr(gate: str) -> pl.Expr:
    if gate == "none":
        return pl.lit(True)
    if gate == "ma100":
        return pl.col("gate_px_ma100")
    if gate == "ma200":
        return pl.col("gate_px_ma200")
    if gate == "structural":
        return pl.col("gate_px_ma100") & pl.col("gate_structural_trend")
    raise ValueError(gate)


def candidate_filter(cfg: AllocatorConfig) -> pl.Expr:
    expr = (
        (~pl.col("is_etf"))
        & (~pl.col("is_finance"))
        & (pl.col("listed_days") >= 252)
        & (pl.col("adv60") >= cfg.min_adv)
        & (pl.col("open") > 0)
        & (pl.col("close") > 0)
        & (pl.col("atr_pct").is_between(0.008, cfg.max_atr))
        & (pl.col("roa_ttm").fill_null(-999.0) >= cfg.min_roa)
        & (pl.col("gross_margin_ttm").fill_null(-999.0) >= cfg.min_gm)
        & (pl.col("f_score_raw").fill_null(0) >= cfg.min_fscore)
        & trend_gate_expr(cfg.trend_gate)
        & market_gate_expr(cfg.market_gate)
        & signal_dates_expr(cfg.schedule)
    )
    if cfg.min_yoy is not None:
        expr &= pl.col("latest_yoy").fill_null(-999.0) >= cfg.min_yoy
    if cfg.min_yoy_delta is not None:
        expr &= pl.col("yoy_delta").fill_null(-999.0) >= cfg.min_yoy_delta
    return expr


def build_candidate_groups(panel: pl.DataFrame, cfg: AllocatorConfig) -> dict[date, list[dict]]:
    rank = "__rank"
    score = "__score"
    keep = [
        "date",
        "company_code",
        "open",
        "close",
        "ma50",
        "ma100",
        "ma200",
        "latest_yoy",
        "atr_pct",
        cfg.score,
    ]
    candidates = (
        panel.filter(candidate_filter(cfg))
        .with_columns(pl.col(cfg.score).fill_null(-999.0).alias(score))
        .filter((pl.col(score) >= cfg.min_score) & pl.col(score).is_finite())
        .sort(["date", score], descending=[False, True])
        .with_columns(pl.col(score).rank("ordinal", descending=True).over("date").alias(rank))
        .filter(pl.col(rank) <= max(cfg.max_positions * 4, 30))
        .select(keep + [score])
    )
    return {
        (d[0] if isinstance(d, tuple) else d): sub.to_dicts()
        for d, sub in candidates.group_by("date", maintain_order=True)
    }


def build_row_lookup(panel: pl.DataFrame, codes: set[str], score_cols: list[str]) -> dict[tuple[date, str], dict]:
    cols = [
        "date",
        "company_code",
        "open",
        "close",
        "ma50",
        "ma100",
        "ma200",
        "latest_yoy",
        "atr_pct",
        *score_cols,
    ]
    return {
        (r["date"], r["company_code"]): r
        for r in panel.filter(pl.col("company_code").is_in(sorted(codes))).select(cols).iter_rows(named=True)
    }


def simulate_allocator(
    cfg: AllocatorConfig,
    days: list[date],
    candidates: dict[date, list[dict]],
    rows: dict[tuple[date, str], dict],
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, float]]:
    cash = CAPITAL
    positions: dict[str, dict] = {}
    pending_entries: list[dict] = []
    pending_exits: set[str] = set()
    nav_rows = []
    trade_rows = []
    max_active = 0

    def px(row: dict | None, pos: dict | None, col: str) -> float:
        val = row.get(col) if row else None
        if val is None or not math.isfinite(float(val)) or float(val) <= 0:
            return float(pos.get("last_close", 0.0)) if pos else 0.0
        return float(val)

    def nav_at(d: date, use_open: bool) -> float:
        total = cash
        col = "open" if use_open else "close"
        for code, pos in positions.items():
            total += float(pos["shares"]) * px(rows.get((d, code)), pos, col)
        return total

    def holding_days(d: date, pos: dict) -> int:
        return max((d - pos["entry_date"]).days, 0)

    for d in days:
        for code in list(pending_exits):
            pos = positions.pop(code, None)
            if pos is None:
                continue
            sell_px = px(rows.get((d, code)), pos, "open")
            if sell_px <= 0:
                positions[code] = pos
                continue
            cash += float(pos["shares"]) * sell_px * (1.0 - SELL_TAX - COMMISSION)
            trade_rows.append(
                {
                    "date": d,
                    "code": code,
                    "action": "exit",
                    "price": sell_px,
                    "reason": pos.get("pending_reason", ""),
                    "ret": sell_px / float(pos["entry_px"]) - 1.0,
                }
            )
        pending_exits.clear()

        if pending_entries:
            nav_open = nav_at(d, use_open=True)
            slot_value = nav_open / cfg.max_positions
            for sig in pending_entries:
                if len(positions) >= cfg.max_positions:
                    break
                code = sig["company_code"]
                if code in positions:
                    continue
                row = rows.get((d, code))
                buy_px = px(row, None, "open")
                if buy_px <= 0:
                    continue
                spend = min(slot_value, cash / (1.0 + COMMISSION))
                if spend <= nav_open * 0.01:
                    continue
                shares = spend / buy_px
                cost = spend * (1.0 + COMMISSION)
                if cost > cash + 1e-6:
                    continue
                cash -= cost
                atr = float(row.get("atr_pct") or 0.05)
                positions[code] = {
                    "shares": shares,
                    "entry_px": buy_px,
                    "entry_date": d,
                    "last_close": buy_px,
                    "peak_close": buy_px,
                    "entry_score": float(sig["__score"]),
                    "trail_pct": min(max(atr * cfg.trail_mult, cfg.trail_min), cfg.trail_max),
                }
                trade_rows.append(
                    {
                        "date": d,
                        "code": code,
                        "action": "entry",
                        "price": buy_px,
                        "reason": f"score={float(sig['__score']):.3f}",
                        "ret": None,
                    }
                )
            pending_entries = []

        for code, pos in list(positions.items()):
            row = rows.get((d, code))
            close = px(row, pos, "close")
            if close <= 0:
                continue
            pos["last_close"] = close
            pos["peak_close"] = max(float(pos["peak_close"]), close)

        nav = nav_at(d, use_open=False)
        max_active = max(max_active, len(positions))
        nav_rows.append({"date": d, "nav": nav, "active": len(positions), "cash": cash})

        for code, pos in list(positions.items()):
            row = rows.get((d, code))
            if row is None:
                continue
            close = px(row, pos, "close")
            exit_px = row.get(cfg.exit_ma)
            score_now = row.get(cfg.score)
            latest_yoy = row.get("latest_yoy")
            reason = None
            if close > 0 and close / float(pos["peak_close"]) - 1.0 <= -float(pos["trail_pct"]):
                reason = "atr_trail"
            elif exit_px is not None and math.isfinite(float(exit_px)) and close < float(exit_px):
                reason = f"{cfg.exit_ma}_break"
            elif latest_yoy is not None and math.isfinite(float(latest_yoy)) and float(latest_yoy) < cfg.exit_yoy:
                reason = "revenue_fade"
            elif (
                score_now is not None
                and math.isfinite(float(score_now))
                and float(score_now) < cfg.exit_score
                and holding_days(d, pos) >= cfg.min_hold_days
            ):
                reason = "score_fade"
            if reason:
                pos["pending_reason"] = reason
                pending_exits.add(code)

        desired = candidates.get(d, [])
        held_or_pending = set(positions) | pending_exits
        open_slots = cfg.max_positions - (len(positions) - len(pending_exits))
        pending_entries = []
        for sig in desired:
            if open_slots <= 0:
                break
            code = sig["company_code"]
            if code in held_or_pending:
                continue
            pending_entries.append(sig)
            held_or_pending.add(code)
            open_slots -= 1

        if open_slots <= 0:
            for sig in desired:
                code = sig["company_code"]
                if code in positions or code in pending_exits:
                    continue
                replaceable = []
                for held_code, pos in positions.items():
                    if held_code in pending_exits or holding_days(d, pos) < cfg.min_hold_days:
                        continue
                    row = rows.get((d, held_code))
                    cur_score = row.get(cfg.score) if row else pos.get("entry_score", -999.0)
                    replaceable.append((float(cur_score or -999.0), held_code, pos))
                if not replaceable:
                    break
                worst_score, worst_code, worst_pos = min(replaceable, key=lambda item: item[0])
                if float(sig["__score"]) >= worst_score + cfg.replace_gap:
                    worst_pos["pending_reason"] = "better_candidate"
                    pending_exits.add(worst_code)
                    pending_entries.append(sig)
                    break

    daily = pl.DataFrame(nav_rows)
    trades = (
        pl.DataFrame(trade_rows)
        if trade_rows
        else pl.DataFrame(schema={"date": pl.Date, "code": pl.Utf8, "action": pl.Utf8, "price": pl.Float64})
    )
    trade_days = trades.filter(pl.col("action") == "entry").select("date").n_unique() if trades.height else 0
    stats = {
        "max_active": float(max_active),
        "trade_days": float(trade_days),
        "avg_turnover_trade_day": float(1.0 / cfg.max_positions) if trade_days else 0.0,
        "entries": float((trades["action"] == "entry").sum()) if trades.height else 0.0,
        "exits": float((trades["action"] == "exit").sum()) if trades.height else 0.0,
        "avg_active": float(daily["active"].mean()) if daily.height else 0.0,
        "avg_cash": float((daily["cash"] / daily["nav"]).mean()) if daily.height else 0.0,
    }
    return daily, trades, stats


def configs() -> list[AllocatorConfig]:
    specs = []
    score_profiles = [
        ("score_balanced_pm", 0.35, 0.05, 0.04, 0.12, 3, None, None),
        ("score_quality_flow", 0.42, 0.10, 0.06, 0.16, 4, None, None),
        ("score_growth_accel", 0.45, 0.05, 0.02, 0.08, 3, 10.0, -10.0),
        ("score_squeeze_quality", 0.42, 0.00, 0.00, 0.05, 2, 0.0, -20.0),
        ("score_value_flow", 0.30, 0.00, 0.03, 0.10, 3, None, None),
    ]
    for score, min_score, exit_score, min_roa, min_gm, fscore, min_yoy, min_delta in score_profiles:
        for max_pos in (3, 5, 7, 10):
            for schedule in ("weekly", "monthly"):
                for market_gate in ("none", "mkt_ma200_or_mom63", "mkt_ma200_and_mom63"):
                    for trend_gate in ("ma100", "structural"):
                        # Keep the grid compact and pre-declared; this is a
                        # strategy-family search, not blind curve fitting.
                        name = (
                            f"iter58_{score}_top{max_pos}_{schedule}_{market_gate}_"
                            f"{trend_gate}_hold20_gap20"
                        )
                        specs.append(
                            AllocatorConfig(
                                name=name,
                                score=score,
                                max_positions=max_pos,
                                schedule=schedule,
                                min_adv=50_000_000.0 if max_pos <= 5 else 80_000_000.0,
                                min_roa=min_roa,
                                min_gm=min_gm,
                                min_fscore=fscore,
                                min_score=min_score,
                                exit_score=exit_score,
                                trend_gate=trend_gate,
                                market_gate=market_gate,
                                min_yoy=min_yoy,
                                min_yoy_delta=min_delta,
                                max_atr=0.11 if score != "score_squeeze_quality" else 0.14,
                                replace_gap=0.20,
                                min_hold_days=20,
                                trail_mult=3.5 if score != "score_quality_flow" else 4.5,
                                trail_min=0.10,
                                trail_max=0.30 if score != "score_squeeze_quality" else 0.35,
                                exit_ma="ma100" if score in {"score_growth_accel", "score_squeeze_quality"} else "ma200",
                                exit_yoy=-20.0 if score in {"score_growth_accel", "score_squeeze_quality"} else -40.0,
                            )
                        )
    return specs


def load_allocator_panel() -> tuple[pl.DataFrame, list[date]]:
    panel, days, market = load_panel()
    extra = fetch_extra_features()
    panel = (
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
        .pipe(add_unified_scores)
        .pipe(add_market_gates, days, market)
        .rechunk()
    )
    return panel, days


def main() -> None:
    t0 = time.time()
    panel, days = load_allocator_panel()
    cfgs = configs()
    score_cols = sorted({cfg.score for cfg in cfgs})
    print(f"[iter58] panel rows={panel.height:,} configs={len(cfgs)}", flush=True)

    candidate_groups: dict[str, dict[date, list[dict]]] = {}
    candidate_codes: set[str] = set()
    for cfg in cfgs:
        groups = build_candidate_groups(panel, cfg)
        candidate_groups[cfg.name] = groups
        candidate_codes.update(row["company_code"] for rows in groups.values() for row in rows)
    print(f"[iter58] candidate codes={len(candidate_codes):,}", flush=True)

    row_lookup = build_row_lookup(panel, candidate_codes, score_cols)
    rows = []
    n_trials = len(cfgs)
    for i, cfg in enumerate(cfgs, 1):
        cfg_t0 = time.time()
        daily, trades, stats = simulate_allocator(cfg, days, candidate_groups[cfg.name], row_lookup)
        daily_path = RESULTS / f"{cfg.name}_daily.csv"
        trades_path = RESULTS / f"{cfg.name}_trades.csv"
        daily.select(["date", "nav", "active", "cash"]).write_csv(daily_path)
        trades.write_csv(trades_path)
        row = validate_daily(cfg.name, daily.select(["date", "nav"]), n_trials, stats)
        row["score"] = cfg.score
        row["max_positions_cfg"] = cfg.max_positions
        row["schedule"] = cfg.schedule
        row["market_gate"] = cfg.market_gate
        row["trend_gate"] = cfg.trend_gate
        row["path"] = str(daily_path)
        row["trades_path"] = str(trades_path)
        row["promotable"] = (
            row["dsr"] >= 0.95
            and row["pbo"] < 0.50
            and row["boot_cagr_lb"] > 0.10
            and row["oos_mdd"] > -0.45
            and row["max_active"] <= 10.0
        )
        rows.append(row)
        if i % 25 == 0 or row["promotable"]:
            print(
                f"[iter58] {i:03d}/{len(cfgs)} {cfg.name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%} DSR={row['dsr']:.3f} PBO={row['pbo']:.3f} "
                f"max_active={row['max_active']:.0f} ({time.time()-cfg_t0:.1f}s)",
                flush=True,
            )

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_58_unified_pm_allocator_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "score",
        "max_positions_cfg",
        "schedule",
        "market_gate",
        "trend_gate",
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
        pl.col("entries").cast(pl.Int64),
        pl.col("avg_active").round(2),
        pl.col("avg_cash").mul(100).round(1).alias("avg_cash_pct"),
    ]
    print("=" * 120)
    print("iter_58 unified holdings-level PM allocator")
    print("=" * 120)
    print(summary.select(view_cols).head(35).to_pandas().to_string(index=False))
    print("\nTop promotable by OOS CAGR")
    print(
        summary.filter(pl.col("promotable"))
        .sort(["oos_cagr", "oos_sortino"], descending=[True, True])
        .select(view_cols)
        .head(15)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {out}")
    print(f"[iter58] elapsed={time.time()-t0:.1f}s", flush=True)


if __name__ == "__main__":
    main()
