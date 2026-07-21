"""iter_33 - portfolio-manager first-principles TW stock strategy.

This is intentionally separate from the existing hybrid experiments. The design
starts from the mandate:

* long-only Taiwan common stocks
* max 10 simultaneous holdings
* adjusted total-return OHLCV
* next-open execution
* event-triggered exits/replacements, not calendar rebalancing

Portfolio thesis:

1. Taiwan long-run returns are concentrated, so staying mostly invested in the
   strongest compounders matters more than waiting for rare perfect setups.
2. The largest excess returns still come from acceleration events, so part of
   the book should be able to rotate quickly into revenue/price/volume breakouts.
3. A PM should replace only when deterioration or a materially better candidate
   appears, which avoids fixed rebalance churn while staying adaptive.
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

os.environ.setdefault("POLARS_MAX_THREADS", str(os.cpu_count() or 1))

import numpy as np
import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from iter_32_first_principles import (  # noqa: E402
    CAPITAL,
    COMMISSION,
    RESULTS,
    SELL_TAX,
    _breakout_col,
    _clip_expr,
    benchmark_rows,
    build_feature_panel,
    metrics,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect  # noqa: E402
from prices import total_return_series  # noqa: E402


FEATURE_VERSION = "iter33-pm-v4-prices-split"
CACHE_DIR = Path("research/strat_lab/results/cache")

PM_PANEL_COLUMNS = [
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
    "next_open",
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


def log(msg: str) -> None:
    print(msg, flush=True)


@dataclass(frozen=True)
class PMConfig:
    name: str
    max_positions: int = 10
    core_slots: int = 4
    satellite_slots: int = 6
    min_adv: float = 30_000_000.0
    core_min_score: float = 0.45
    core_exit_score: float = 0.00
    sat_min_score: float = 0.75
    sat_min_yoy: float = 20.0
    sat_min_yoy_delta: float = -15.0
    sat_breakout_lkb: int = 90
    sat_breakout_ratio: float = 0.98
    sat_vol_mult: float = 1.2
    min_roa: float = 0.04
    min_gm: float = 0.12
    min_fscore: int = 3
    min_inst_flow: float = -0.05
    replace_gap: float = 0.20
    min_hold_days: int = 20
    exit_yoy: float = -20.0
    atr_mult: float = 4.0
    trail_min: float = 0.12
    trail_max: float = 0.35
    market_risk_off: bool = False


def add_pm_scores(panel: pl.DataFrame) -> pl.DataFrame:
    """Add simple monotonic PM scores; no fitted model coefficients."""
    quality = (
        0.40 * _clip_expr(pl.col("roa_ttm") / 0.12, -1.0, 2.0)
        + 0.30 * _clip_expr(pl.col("gross_margin_ttm") / 0.35, -1.0, 2.0)
        + 0.30 * _clip_expr((pl.col("f_score_raw") - 4.0) / 3.0, -1.0, 1.0)
    )
    panel = panel.with_columns(
        [
            quality.alias("quality_score"),
            _clip_expr(pl.col("adv60").log10() - 7.5, -1.0, 1.0).alias("liquidity_score"),
        ]
    )
    return panel.with_columns(
        [
            (
                0.24 * pl.col("s_mom120")
                + 0.18 * pl.col("s_mom20")
                + 0.20 * pl.col("s_rev")
                + 0.18 * pl.col("quality_score")
                + 0.10 * pl.col("s_inst")
                + 0.06 * pl.col("s_risk")
                + 0.04 * pl.col("liquidity_score")
            ).alias("core_score"),
            (
                0.24 * pl.col("s_rev")
                + 0.18 * pl.col("s_accel")
                + 0.24 * pl.col("s_mom120")
                + 0.16 * pl.col("s_mom20")
                + 0.12 * pl.col("s_inst")
                + 0.06 * pl.col("s_risk")
            ).alias("sat_score"),
        ]
    ).with_columns(pl.max_horizontal(["core_score", "sat_score"]).alias("rank_score"))


def finance_expr() -> pl.Expr:
    return (
        pl.col("industry").str.contains("金融").fill_null(False)
        | pl.col("industry").str.contains("證券").fill_null(False)
        | pl.col("industry").str.contains("保險").fill_null(False)
    )


def prepare_pm_panel(panel: pl.DataFrame) -> pl.DataFrame:
    panel = add_pm_scores(panel)
    panel = panel.with_columns(finance_expr().alias("is_finance"))
    return panel.select(PM_PANEL_COLUMNS).rechunk()


def cache_fingerprint(start: date, end: date) -> str:
    return hashlib.sha1(f"{FEATURE_VERSION}|{start}|{end}".encode("utf-8")).hexdigest()[:10]


def panel_cache_path(start: date, end: date) -> Path:
    return CACHE_DIR / f"iter33_features_{start}_{end}_{cache_fingerprint(start, end)}.parquet"


def panel_meta_path(start: date, end: date) -> Path:
    return panel_cache_path(start, end).with_suffix(".json")


def data_signature(con, start: date, end: date) -> dict[str, object]:
    daily_max, daily_n = con.sql(
        f"""
        SELECT MAX(date)::VARCHAR, COUNT(*)
        FROM daily_quote
        WHERE date BETWEEN DATE '{start}' - INTERVAL '320 days' AND DATE '{end}'
        """
    ).fetchone()
    flow_max, flow_n = con.sql(
        f"""
        SELECT MAX(date)::VARCHAR, COUNT(*)
        FROM daily_trading_details
        WHERE date BETWEEN DATE '{start}' - INTERVAL '320 days' AND DATE '{end}'
        """
    ).fetchone()
    rev_max, rev_n = con.sql(
        """
        SELECT MAX(year * 100 + month), COUNT(*)
        FROM operating_revenue
        """
    ).fetchone()
    q_max, q_n = con.sql(
        """
        SELECT MAX(year * 10 + quarter), COUNT(*)
        FROM raw_quarterly
        """
    ).fetchone()
    return {
        "feature_version": FEATURE_VERSION,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "daily_quote": [daily_max, daily_n],
        "daily_trading_details": [flow_max, flow_n],
        "operating_revenue": [rev_max, rev_n],
        "raw_quarterly": [q_max, q_n],
    }


def _read_meta(path: Path) -> dict[str, object] | None:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _compatible_legacy_cache(start: date, end: date) -> Path | None:
    pattern = f"iter33_features_{start}_{end}_*.parquet"
    for path in sorted(CACHE_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True):
        meta = _read_meta(path.with_suffix(".json"))
        if meta is None or meta.get("feature_version") != FEATURE_VERSION:
            continue
        try:
            names = set(pl.scan_parquet(path).collect_schema().names())
        except Exception:
            continue
        if set(PM_PANEL_COLUMNS).issubset(names):
            return path
    return None


def trading_days(con, start: date, end: date) -> list[date]:
    return [
        r[0]
        for r in con.sql(
            f"""
            SELECT date FROM daily_quote
            WHERE market='twse' AND company_code='0050'
              AND date BETWEEN DATE '{start}' AND DATE '{end}'
            ORDER BY date
            """
        ).fetchall()
    ]


def load_or_build_panel(con, start: date, end: date, use_cache: bool = True) -> tuple[pl.DataFrame, list[date]]:
    """Cache the expensive adjusted/PIT feature panel by DB mtime and date range."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cpath = panel_cache_path(start, end)
    mpath = panel_meta_path(start, end)
    sig = data_signature(con, start, end)
    days = trading_days(con, start, end)
    if use_cache and cpath.exists() and _read_meta(mpath) == sig:
        log(f"[iter33] load feature cache: {cpath}")
        return pl.scan_parquet(cpath).collect(), days
    if use_cache:
        legacy = _compatible_legacy_cache(start, end)
        if legacy is not None:
            log(f"[iter33] load compatible feature cache: {legacy}")
            panel = pl.scan_parquet(legacy).select(PM_PANEL_COLUMNS).collect()
            panel.write_parquet(cpath, compression="zstd", statistics=True)
            mpath.write_text(json.dumps(sig, sort_keys=True, indent=2))
            return panel, days

    panel, _ = build_feature_panel(con, start, end)
    panel = prepare_pm_panel(panel)
    if use_cache:
        tmp = cpath.with_suffix(".tmp.parquet")
        panel.write_parquet(tmp, compression="zstd", statistics=True)
        tmp.replace(cpath)
        mpath.write_text(json.dumps(sig, sort_keys=True, indent=2))
        log(f"[iter33] saved feature cache: {cpath}")
    return panel, days


def build_candidate_groups(panel: pl.DataFrame, cfg: PMConfig) -> dict[date, list[dict]]:
    t0 = time.time()
    common = (
        (~pl.col("is_etf"))
        & (~pl.col("is_finance"))
        & (pl.col("listed_days") >= 252)
        & (pl.col("adv60") >= cfg.min_adv)
        & (pl.col("next_open").is_not_null())
        & (pl.col("atr_pct").is_between(0.008, 0.11))
        & (pl.col("inst_flow20").fill_null(0) >= cfg.min_inst_flow)
    )
    quality_floor = (
        (pl.col("roa_ttm").fill_null(-999) >= cfg.min_roa)
        & (pl.col("gross_margin_ttm").fill_null(-999) >= cfg.min_gm)
        & (pl.col("f_score_raw").fill_null(0) >= cfg.min_fscore)
    )
    base = panel.filter(common)

    core = (
        base.filter(
            quality_floor
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
        .head(max(cfg.max_positions * 3, 30))
    )

    hi_col = _breakout_col(cfg.sat_breakout_lkb)
    sat = (
        base.filter(
            (pl.col("latest_yoy") >= cfg.sat_min_yoy)
            & (pl.col("yoy_delta").fill_null(0) >= cfg.sat_min_yoy_delta)
            & (pl.col("close") >= pl.col(hi_col) * cfg.sat_breakout_ratio)
            & (pl.col("vol") >= pl.col("vol_avg60") * cfg.sat_vol_mult)
            & (pl.col("close") > pl.col("ma100"))
            & (pl.col("sat_score") >= cfg.sat_min_score)
            & (pl.col("f_score_raw").fill_null(0) >= max(2, cfg.min_fscore - 1))
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
        .head(max(cfg.max_positions * 4, 40))
    )

    groups: dict[date, list[dict]] = {}
    core_groups = {
        key[0] if isinstance(key, tuple) else key: g.to_dicts()
        for key, g in core.group_by("date", maintain_order=True)
    }
    sat_groups = {
        key[0] if isinstance(key, tuple) else key: g.to_dicts()
        for key, g in sat.group_by("date", maintain_order=True)
    }
    for d in sorted(set(core_groups) | set(sat_groups)):
        picked: dict[str, dict] = {}
        for row in core_groups.get(d, [])[: cfg.core_slots]:
            picked[row["company_code"]] = row
        for row in sat_groups.get(d, [])[: cfg.satellite_slots]:
            picked.setdefault(row["company_code"], row)

        if len(picked) < cfg.max_positions:
            extras = sorted(
                core_groups.get(d, [])[cfg.core_slots :] + sat_groups.get(d, [])[cfg.satellite_slots :],
                key=lambda r: r["rank_score"],
                reverse=True,
            )
            for row in extras:
                picked.setdefault(row["company_code"], row)
                if len(picked) >= cfg.max_positions:
                    break
        groups[d] = sorted(picked.values(), key=lambda r: r["rank_score"], reverse=True)
    log(
        f"  candidates {cfg.name}: core_rows={core.height:,} sat_rows={sat.height:,} "
        f"days={len(groups):,} ({time.time() - t0:.1f}s)"
    )
    return groups


def build_pm_lookup(panel: pl.DataFrame, candidate_codes: set[str]) -> dict[tuple[date, str], dict]:
    cols = [
        "date",
        "company_code",
        "open",
        "close",
        "ma100",
        "ma200",
        "latest_yoy",
        "atr_pct",
        "core_score",
        "sat_score",
        "rank_score",
    ]
    lookup_panel = panel
    if candidate_codes:
        lookup_panel = lookup_panel.filter(pl.col("company_code").is_in(sorted(candidate_codes)))
    return {(r["date"], r["company_code"]): r for r in lookup_panel.select(cols).iter_rows(named=True)}


def market_regime(con, start: date, end: date, days: list[date]) -> dict[date, bool]:
    idx = total_return_series(con, "0050", start.isoformat(), end.isoformat(), market="twse").sort("date")
    idx = idx.with_columns(
        [
            pl.col("adj_close").rolling_mean(200).alias("ma200"),
            (pl.col("adj_close") > pl.col("adj_close").rolling_mean(200)).fill_null(True).alias("risk_on"),
        ]
    )
    raw = {r["date"]: bool(r["risk_on"]) for r in idx.iter_rows(named=True)}
    return {d: raw.get(d, True) for d in days}


def run_pm_config(
    cfg: PMConfig,
    candidates: dict[date, list[dict]],
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

    def current_nav(d: date, use_open: bool = False) -> float:
        total = cash
        px_col = "open" if use_open else "close"
        for code, pos in positions.items():
            row = row_lookup.get((d, code))
            px = row.get(px_col) if row else None
            if px is None or px <= 0 or math.isnan(float(px)):
                px = pos["last_close"]
            total += pos["shares"] * px
        return total

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
                    "lane": pos.get("lane"),
                    "price": sell_px,
                    "ret": sell_px / pos["entry_px"] - 1,
                    "reason": pos.get("pending_reason", ""),
                }
            )
        pending_exits.clear()

        if pending_entries:
            nav_open = current_nav(d, use_open=True)
            target_dollar = nav_open / cfg.max_positions
            for sig in pending_entries:
                if len(positions) >= cfg.max_positions:
                    break
                code = sig["company_code"]
                if code in positions:
                    continue
                row = row_lookup.get((d, code))
                buy_px = row.get("open") if row else None
                if buy_px is None or buy_px <= 0:
                    continue
                spend = min(cash, target_dollar)
                if spend <= 0:
                    break
                shares = spend / buy_px / (1 + COMMISSION)
                cost = shares * buy_px * (1 + COMMISSION)
                if shares <= 0 or cost > cash + 1e-6:
                    continue
                cash -= cost
                atr_pct = row.get("atr_pct") or 0.05
                positions[code] = {
                    "shares": shares,
                    "entry_px": buy_px,
                    "entry_date": d,
                    "lane": sig.get("lane", "core"),
                    "trail_pct": max(cfg.trail_min, min(cfg.trail_max, atr_pct * cfg.atr_mult)),
                    "peak_close": buy_px,
                    "last_close": buy_px,
                    "entry_score": sig.get("rank_score", 0.0),
                }
                trades.append(
                    {
                        "date": d,
                        "code": code,
                        "action": "entry",
                        "lane": sig.get("lane", "core"),
                        "price": buy_px,
                        "ret": None,
                        "reason": f"rank_score={sig.get('rank_score', 0):.3f}",
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
        nav_hist.append((d, nav, len(positions), cash))

        pending_exits = set()
        for code, pos in positions.items():
            row = row_lookup.get((d, code))
            if row is None:
                continue
            close = row.get("close")
            ma100 = row.get("ma100")
            ma200 = row.get("ma200")
            latest_yoy = row.get("latest_yoy")
            rank_score = row.get("rank_score")
            reason = None
            if pos["peak_close"] > 0 and close / pos["peak_close"] - 1 <= -pos["trail_pct"]:
                reason = "atr_trailing"
            elif ma200 is not None and close < ma200:
                reason = "ma200_break"
            elif pos.get("lane") == "sat" and ma100 is not None and close < ma100:
                reason = "sat_ma100_break"
            elif latest_yoy is not None and latest_yoy < cfg.exit_yoy:
                reason = "yoy_fade"
            elif (
                rank_score is not None
                and rank_score < cfg.core_exit_score
                and holding_days(d, pos) >= cfg.min_hold_days
            ):
                reason = "score_fade"
            elif cfg.market_risk_off and not risk_on.get(d, True) and close < ma100:
                reason = "risk_off_weak"
            if reason:
                pos["pending_reason"] = reason
                pending_exits.add(code)

        desired = candidates.get(d, [])
        held_or_exiting = set(positions) | pending_exits
        available = cfg.max_positions - (len(positions) - len(pending_exits))
        pending_entries = []
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
            candidate_iter = [s for s in desired if s["company_code"] not in positions and s["company_code"] not in pending_exits]
            for sig in candidate_iter:
                replaceable = []
                for code, pos in positions.items():
                    if code in pending_exits or holding_days(d, pos) < cfg.min_hold_days:
                        continue
                    row = row_lookup.get((d, code))
                    cur_score = row.get("rank_score") if row else pos.get("entry_score", 0.0)
                    replaceable.append((cur_score or -999.0, code, pos))
                if not replaceable:
                    break
                worst_score, worst_code, worst_pos = min(replaceable, key=lambda x: x[0])
                if sig["rank_score"] >= worst_score + cfg.replace_gap:
                    worst_pos["pending_reason"] = "better_candidate"
                    pending_exits.add(worst_code)
                    pending_entries.append(sig)
                if len(pending_entries) >= cfg.max_positions:
                    break

    nav_df = pl.DataFrame(
        {
            "date": [x[0] for x in nav_hist],
            "nav": [x[1] for x in nav_hist],
            "n_active": [x[2] for x in nav_hist],
            "cash": [x[3] for x in nav_hist],
        }
    )
    trades_df = pl.DataFrame(trades) if trades else pl.DataFrame(
        schema={"date": pl.Date, "code": pl.Utf8, "action": pl.Utf8, "price": pl.Float64}
    )
    m = metrics(nav_df["nav"].to_numpy(), nav_df["date"].to_list())
    m.update(
        {
            "name": cfg.name,
            "max_active": max_active,
            "n_entries": int((trades_df["action"] == "entry").sum()) if trades_df.height else 0,
            "n_exits": int((trades_df["action"] == "exit").sum()) if trades_df.height else 0,
            "avg_active": float(nav_df["n_active"].mean()),
            "cash_avg": float((nav_df["cash"] / nav_df["nav"]).mean()),
        }
    )
    return m, nav_df, trades_df


def configs() -> list[PMConfig]:
    return [
        PMConfig("pm_1pos_best_strength", max_positions=1, core_slots=1, satellite_slots=0, core_min_score=0.55, sat_min_score=0.95, min_adv=80_000_000, replace_gap=0.35, min_hold_days=30),
        PMConfig("pm_2pos_best_strength", max_positions=2, core_slots=1, satellite_slots=1, core_min_score=0.50, sat_min_score=0.85, min_adv=60_000_000, replace_gap=0.30, min_hold_days=25),
        PMConfig("pm_3pos_concentrated", max_positions=3, core_slots=1, satellite_slots=2, core_min_score=0.45, sat_min_score=0.75, min_adv=50_000_000, replace_gap=0.25, min_hold_days=20),
        PMConfig("pm_5pos_concentrated", max_positions=5, core_slots=2, satellite_slots=3, core_min_score=0.40, sat_min_score=0.70, min_adv=40_000_000, replace_gap=0.20, min_hold_days=20),
        PMConfig("pm_5pos_accel", max_positions=5, core_slots=1, satellite_slots=4, core_min_score=0.45, sat_min_score=0.65, sat_vol_mult=1.0, min_adv=40_000_000, replace_gap=0.15, min_hold_days=15),
        PMConfig("pm_4core_6sat_balanced"),
        PMConfig("pm_6core_4sat_compounder", core_slots=6, satellite_slots=4, core_min_score=0.40, sat_min_score=0.80),
        PMConfig("pm_3core_7sat_accel", core_slots=3, satellite_slots=7, core_min_score=0.45, sat_min_score=0.70),
        PMConfig("pm_5core_5sat_loose", core_slots=5, satellite_slots=5, core_min_score=0.35, sat_min_score=0.65, min_adv=20_000_000),
        PMConfig("pm_5core_5sat_quality", core_slots=5, satellite_slots=5, core_min_score=0.50, sat_min_score=0.75, min_roa=0.08, min_gm=0.18, min_fscore=4),
        PMConfig("pm_4core_6sat_fast", replace_gap=0.10, min_hold_days=10, sat_min_score=0.65, sat_vol_mult=1.0),
        PMConfig("pm_4core_6sat_slow", replace_gap=0.35, min_hold_days=45, sat_min_score=0.80),
        PMConfig("pm_4core_6sat_riskoff", market_risk_off=True, core_min_score=0.40, sat_min_score=0.70),
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2005-01-03")
    ap.add_argument("--end", default="2026-05-08")
    ap.add_argument("--no-cache", action="store_true", help="rebuild feature panel instead of using parquet cache")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    RESULTS.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    con = connect()
    panel, days = load_or_build_panel(con, start, end, use_cache=not args.no_cache)
    risk_on = market_regime(con, start, end, days)

    log("[iter33] build candidate groups vectorized ...")
    cfgs = configs()
    candidate_groups = {}
    candidate_codes: set[str] = set()
    for cfg in cfgs:
        groups = build_candidate_groups(panel, cfg)
        candidate_groups[cfg.name] = groups
        candidate_codes.update(row["company_code"] for rows in groups.values() for row in rows)

    row_lookup = build_pm_lookup(panel, candidate_codes)
    log(
        f"[iter33] row lookup: {len(row_lookup):,} rows "
        f"({len(candidate_codes):,} candidate codes, filtered from {panel.height:,} panel rows)"
    )

    rows = []
    for cfg in cfgs:
        log(f"\n[iter33] run {cfg.name}")
        t = time.time()
        result, nav_df, trades_df = run_pm_config(cfg, candidate_groups[cfg.name], days, row_lookup, risk_on)
        nav_path = RESULTS / f"iter_33_{cfg.name}_daily.csv"
        trades_path = RESULTS / f"iter_33_{cfg.name}_trades.csv"
        nav_df.write_csv(nav_path)
        trades_df.write_csv(trades_path)
        result.update(asdict(cfg))
        result["nav_path"] = str(nav_path)
        rows.append(result)
        log(
            f"  CAGR {result['cagr'] * 100:+.2f}% Sortino {result['sortino']:.3f} "
            f"Sharpe {result['sharpe']:.3f} MDD {result['mdd'] * 100:.2f}% "
            f"entries {result['n_entries']} max_active {result['max_active']} "
            f"cash_avg {result['cash_avg']:.1%} ({time.time() - t:.1f}s)"
        )

    rows.extend(benchmark_rows(con, start, end))
    out = pl.DataFrame(rows).sort("sortino", descending=True)
    out_path = RESULTS / "iter_33_pm_first_principles_summary.csv"
    out.write_csv(out_path)
    log("\n" + "=" * 96)
    log(f"iter_33 PM first-principles summary ({start} -> {end})")
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
        .to_pandas()
        .to_string(index=False)
    )
    log(f"\nSaved: {out_path}")
    log(f"Total runtime: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
