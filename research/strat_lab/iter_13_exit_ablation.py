"""iter_13 退場機制完整 ablation — 4 種 exit layer 對比.

Background: User push back — Phase A 只測固定 % trailing stop（marginal +0.020），
沒測 ATR-based / event-driven exit。完整 ablation 需要對比：

| 變體 | Exit 條件 |
|---|---|
| baseline | Monthly re-rank only (no intra-month exit) |
| fixed_15 | Phase A best — 固定 -15% trailing stop from peak |
| atr | ATR-based trailing — clip(atr/px × 3, 10%, 25%) |
| qual_fade | Event: 持倉股最新 quarter ROA TTM < 8% → exit |
| rev_neg | Event: 持倉股最新月營收 YoY < 0% → exit |
| atr_qual | Combined: ATR trailing + qual_fade |
| atr_qual_rev | Combined: ATR + qual_fade + rev_neg |

Output: research/strat_lab/results/iter_13_exit_ablation_v8.csv

Run:
    uv run --project research python research/strat_lab/iter_13_exit_ablation.py
"""
from __future__ import annotations

import math
import os
import sys
import time
from datetime import date

import numpy as np
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect
from prices import fetch_adjusted_panel, fetch_daily_returns

RESULTS = "research/strat_lab/results"
TDPY = 252
RF = 0.01
SELL_TAX = 0.003
COMMISSION = 0.000285
CAPITAL = 1_000_000.0


def get_atr_panel(con, codes: list[str], start: date, end: date) -> pl.DataFrame:
    """Pull adjusted high/low/close for ATR(14) calculation."""
    panels = []
    for mkt in ("twse", "tpex"):
        p = fetch_adjusted_panel(con, start.isoformat(), end.isoformat(),
                                   codes=codes, market=mkt,
                                   include_extra_history_days=30)
        if not p.is_empty():
            panels.append(p.select(["date", "company_code", "high", "low", "close"]))
    if not panels:
        return pl.DataFrame()

    px = pl.concat(panels).unique(subset=["date", "company_code"]).sort(["company_code", "date"])
    # True Range
    px = px.with_columns(pl.col("close").shift(1).over("company_code").alias("prev_close"))
    px = px.with_columns(
        pl.max_horizontal([
            pl.col("high") - pl.col("low"),
            (pl.col("high") - pl.col("prev_close")).abs(),
            (pl.col("low") - pl.col("prev_close")).abs(),
        ]).alias("tr")
    )
    px = px.with_columns(
        pl.col("tr").rolling_mean(14).over("company_code").alias("atr14")
    )
    return px.select(["date", "company_code", "close", "atr14"])


def get_quality_quarterly(con, codes: list[str]) -> pl.DataFrame:
    """Get quarterly ROA TTM for held stocks (for qual_fade event)."""
    if not codes:
        return pl.DataFrame()
    codes_sql = ",".join(f"'{c}'" for c in codes)
    df = con.sql(f"""
        SELECT company_code, year, quarter, roa_ttm
        FROM raw_quarterly
        WHERE company_code IN ({codes_sql}) AND roa_ttm IS NOT NULL
        ORDER BY company_code, year, quarter
    """).pl()

    # PIT-safe effective_date: end of (year, quarter+1)'s announcement window (~next q+15)
    # Simplified: Q1 effective = year/5/16, Q2 = year/8/15, Q3 = year/11/15, Q4 = (year+1)/4/8
    rows = []
    for r in df.iter_rows(named=True):
        y, q = r["year"], r["quarter"]
        if q == 1:
            eff = date(y, 5, 16)
        elif q == 2:
            eff = date(y, 8, 15)
        elif q == 3:
            eff = date(y, 11, 15)
        else:  # q == 4
            eff = date(y + 1, 4, 8)
        rows.append({
            "company_code": r["company_code"],
            "effective_date": eff,
            "roa_ttm": r["roa_ttm"],
        })
    return pl.DataFrame(rows).sort(["company_code", "effective_date"])


def get_revenue_monthly(con, codes: list[str]) -> pl.DataFrame:
    """Get monthly revenue YoY (PIT publish date = next month 11th)."""
    if not codes:
        return pl.DataFrame()
    codes_sql = ",".join(f"'{c}'" for c in codes)
    df = con.sql(f"""
        SELECT company_code, year, month, monthly_revenue_yoy AS yoy
        FROM operating_revenue
        WHERE company_code IN ({codes_sql}) AND monthly_revenue_yoy IS NOT NULL
        ORDER BY company_code, year, month
    """).pl()

    # PIT publish date = next month's 11th
    rows = []
    for r in df.iter_rows(named=True):
        y, m = r["year"], r["month"]
        if m == 12:
            eff = date(y + 1, 1, 11)
        else:
            eff = date(y, m + 1, 11)
        rows.append({
            "company_code": r["company_code"],
            "effective_date": eff,
            "yoy": r["yoy"],
        })
    return pl.DataFrame(rows).sort(["company_code", "effective_date"])


def replay_with_exits(picks_csv: str, start: date, end: date,
                       use_atr: bool = False,
                       fixed_stop_pct: float = 0.0,
                       qual_fade_thresh: float = 0.0,
                       rev_neg_thresh: float = -999.0,
                       output_csv: str = None) -> dict:
    """Replay iter_13 monthly NAV with various exit layers.

    Args:
      use_atr: True = ATR-based trailing, False + fixed_stop_pct>0 = fixed % trailing
      fixed_stop_pct: 0 = no fixed trailing (still need use_atr=True to enable trailing)
      qual_fade_thresh: 0 = disabled; else exit if held stock's most recent ROA TTM < this
      rev_neg_thresh: -999 = disabled; else exit if held stock's most recent rev_yoy < this
      output_csv: if None, don't write
    """
    t0 = time.time()
    con = connect()

    picks = pl.read_csv(picks_csv, try_parse_dates=True,
                          schema_overrides={"company_code": pl.Utf8}).sort("rebal_d")
    rebal_ds = sorted(picks["rebal_d"].unique().to_list())
    held_codes = picks["company_code"].unique().to_list()
    if "0050" not in held_codes:
        held_codes.append("0050")

    # Pull all needed data
    rets_df = pl.concat([
        fetch_daily_returns(con, start.isoformat(), end.isoformat(),
                              codes=held_codes, market="twse"),
        fetch_daily_returns(con, start.isoformat(), end.isoformat(),
                              codes=held_codes, market="tpex"),
    ]).unique(subset=["date", "company_code"])
    rets_lookup = {(r[0], r[1]): r[2] for r in rets_df.iter_rows()}

    rets_0050 = {r[0]: r[2] for r in rets_df.filter(pl.col("company_code") == "0050").iter_rows()}

    # ATR data (only if use_atr)
    atr_lookup = {}
    if use_atr:
        atr_df = get_atr_panel(con, held_codes, start, end)
        for r in atr_df.iter_rows(named=True):
            atr_lookup[(r["date"], r["company_code"])] = (r["close"], r["atr14"])

    # Quality fade data (only if qual_fade_thresh > 0)
    qual_lookup = {}   # code -> [(eff_date, roa_ttm), ...]
    if qual_fade_thresh > 0:
        qual_df = get_quality_quarterly(con, [c for c in held_codes if c != "0050"])
        for r in qual_df.iter_rows(named=True):
            qual_lookup.setdefault(r["company_code"], []).append((r["effective_date"], r["roa_ttm"]))

    # Revenue YoY data
    rev_lookup = {}
    if rev_neg_thresh > -999:
        rev_df = get_revenue_monthly(con, [c for c in held_codes if c != "0050"])
        for r in rev_df.iter_rows(named=True):
            rev_lookup.setdefault(r["company_code"], []).append((r["effective_date"], r["yoy"]))

    # Trading days
    days = [r[0] for r in con.sql(f"""
        SELECT date FROM daily_quote WHERE market='twse' AND company_code='0050'
          AND date BETWEEN DATE '{start}' AND DATE '{end}' ORDER BY date
    """).fetchall()]

    # Build (date → period_id) mapping
    days_df = pl.DataFrame({"date": days}).sort("date")
    rebal_df = pl.DataFrame({"period_start": rebal_ds}).with_columns(
        (pl.col("period_start") + pl.duration(days=1)).alias("effective"))
    da = days_df.join_asof(rebal_df.sort("effective"), left_on="date",
                              right_on="effective", strategy="backward")

    # For each (rebal_d, code), get weight + entry-day metrics
    pbd = {}
    for r in picks.iter_rows(named=True):
        pbd.setdefault(r["rebal_d"], {})[r["company_code"]] = r["weight"]

    # Daily simulator: track per-stock peak, exit flags, contributions
    nav = CAPITAL
    nav_hist = []
    triggers = {"trailing": 0, "qual_fade": 0, "rev_neg": 0}
    # State: per (rebal_d, code) → entry_close, entry_atr, peak_close, ever_stopped
    pos_state = {}

    for di, d in enumerate(days):
        d_row = da.row(di, named=True)
        rd = d_row["period_start"]
        if rd is None or rd not in pbd:
            nav_hist.append((d, nav))
            continue

        cs = pbd[rd]
        # First day of period: init state
        for c, w in cs.items():
            key = (rd, c)
            if key not in pos_state:
                # Init on entry day (rd+1's first available)
                close_atr = atr_lookup.get((d, c)) if use_atr else None
                pos_state[key] = {
                    "weight": w,
                    "entry_close": close_atr[0] if close_atr else None,
                    "entry_atr": close_atr[1] if close_atr else None,
                    "peak_close": close_atr[0] if close_atr else None,
                    "ever_stopped": False,
                    "stop_reason": None,
                }

        # Daily: check each held position
        port_ret = 0.0
        for c, w in cs.items():
            key = (rd, c)
            state = pos_state[key]

            if c == "0050":
                # Pure cash buffer
                r = rets_0050.get(d, 0.0) or 0.0
                port_ret += w * r
                continue

            if state["ever_stopped"]:
                # Stopped → use 0050 return
                r0 = rets_0050.get(d, 0.0) or 0.0
                port_ret += w * r0
                continue

            r = rets_lookup.get((d, c), 0.0) or 0.0
            port_ret += w * r

            # Update close for trailing
            if use_atr:
                close_atr = atr_lookup.get((d, c))
                if close_atr:
                    state["peak_close"] = max(state["peak_close"] or 0, close_atr[0])
                    # Check ATR trailing
                    trail_pct = max(0.10, min(0.25, state["entry_atr"] / state["entry_close"] * 3.0)) if state["entry_atr"] and state["entry_close"] else 0.15
                    if close_atr[0] / state["peak_close"] - 1 < -trail_pct:
                        state["ever_stopped"] = True
                        state["stop_reason"] = "trailing_atr"
                        triggers["trailing"] += 1
                        continue
            elif fixed_stop_pct > 0:
                # Fixed % trailing — track close from rets (approximate)
                # Compute cumulative growth from period start
                # 簡化用 cum return via running prod
                cur_growth = state.get("cum_growth", 1.0) * (1 + r)
                state["cum_growth"] = cur_growth
                state["peak_growth"] = max(state.get("peak_growth", 1.0), cur_growth)
                if cur_growth / state["peak_growth"] - 1 < -fixed_stop_pct:
                    state["ever_stopped"] = True
                    state["stop_reason"] = "trailing_fixed"
                    triggers["trailing"] += 1
                    continue

            # Quality fade check
            if qual_fade_thresh > 0 and c in qual_lookup:
                # Find most recent ROA TTM with effective_date ≤ d
                recent_roa = None
                for eff, roa in qual_lookup[c]:
                    if eff <= d:
                        recent_roa = roa
                    else:
                        break
                if recent_roa is not None and recent_roa < qual_fade_thresh:
                    state["ever_stopped"] = True
                    state["stop_reason"] = "qual_fade"
                    triggers["qual_fade"] += 1

            # Revenue YoY check
            if rev_neg_thresh > -999 and c in rev_lookup:
                recent_yoy = None
                for eff, yoy in rev_lookup[c]:
                    if eff <= d:
                        recent_yoy = yoy
                    else:
                        break
                if recent_yoy is not None and recent_yoy < rev_neg_thresh:
                    state["ever_stopped"] = True
                    state["stop_reason"] = "rev_neg"
                    triggers["rev_neg"] += 1

        nav = nav * (1 + port_ret)
        nav_hist.append((d, nav))

        # End of period: clear state for next month
        next_rd = None
        if di + 1 < len(days):
            next_d_row = da.row(di + 1, named=True)
            next_rd = next_d_row["period_start"]
        if next_rd != rd and next_rd is not None:
            # 新月份開始 → 清除舊狀態
            pos_state = {k: v for k, v in pos_state.items() if k[0] == next_rd}

    nav_arr = np.array([n for _, n in nav_hist])
    rets_arr = np.diff(np.concatenate([[CAPITAL], nav_arr])) / np.concatenate([[CAPITAL], nav_arr[:-1]])
    days_list = [d for d, _ in nav_hist]
    years = max((days_list[-1] - days_list[0]).days / 365.25, 1e-9)
    cagr = (nav_arr[-1] / CAPITAL) ** (1 / years) - 1
    vol = float(rets_arr.std(ddof=1) * math.sqrt(TDPY))
    downside = rets_arr[rets_arr < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - RF) / vol if vol > 0 else 0
    sortino = (cagr - RF) / downvol if downvol > 0 else 0
    peak, mdd = CAPITAL, 0.0
    for v in nav_arr:
        peak = max(peak, v); mdd = min(mdd, (v - peak) / peak)

    if output_csv:
        pl.DataFrame({"date": days_list, "nav": nav_arr}).write_csv(output_csv)

    return {
        "cagr": cagr, "sortino": sortino, "sharpe": sharpe, "mdd": mdd,
        "triggers": triggers, "runtime_s": time.time() - t0,
    }


def main():
    picks_csv = f"{RESULTS}/iter_13_monthly_mcap_dual_picks.csv"
    start = date(2005, 1, 3)
    end = date(2026, 4, 25)

    print("=" * 78)
    print("iter_13 Quality 池退場機制 ablation")
    print("=" * 78)

    variants = [
        ("baseline (monthly re-rank only)", dict()),
        ("fixed_15 (Phase A best)", dict(fixed_stop_pct=0.15)),
        ("atr (ATR trailing 3x clip [10%,25%])", dict(use_atr=True)),
        ("qual_fade (ROA TTM < 8%)", dict(qual_fade_thresh=0.08)),
        ("rev_neg (月營收 YoY < 0%)", dict(rev_neg_thresh=0.0)),
        ("atr + qual_fade", dict(use_atr=True, qual_fade_thresh=0.08)),
        ("atr + rev_neg", dict(use_atr=True, rev_neg_thresh=0.0)),
        ("atr + qual_fade + rev_neg", dict(use_atr=True, qual_fade_thresh=0.08, rev_neg_thresh=0.0)),
    ]

    rows = []
    for name, kwargs in variants:
        print(f"\n[{name}] ...", end=" ")
        suffix = name.replace(" ", "_").replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace(",", "_").replace("%", "p").replace("/", "")[:50]
        out = f"{RESULTS}/iter_13_exit_ablation_{suffix}_daily.csv"
        r = replay_with_exits(picks_csv, start, end, output_csv=out, **kwargs)
        print(f"CAGR {r['cagr']*100:+.2f}% Sortino {r['sortino']:.3f} MDD {r['mdd']*100:.1f}% "
              f"triggers={r['triggers']} ({r['runtime_s']:.1f}s)")
        rows.append({"variant": name, "cagr": r["cagr"], "sortino": r["sortino"],
                     "sharpe": r["sharpe"], "mdd": r["mdd"],
                     "trail_triggers": r["triggers"]["trailing"],
                     "qual_fade_triggers": r["triggers"]["qual_fade"],
                     "rev_neg_triggers": r["triggers"]["rev_neg"]})

    df = pl.DataFrame(rows)
    df.write_csv(f"{RESULTS}/iter_13_exit_ablation_v8.csv")

    print(f"\n{'=' * 78}")
    print(f"{'Variant':<42} {'CAGR':>8} {'Sortino':>9} {'MDD':>9}")
    print('-' * 78)
    for r in rows:
        print(f"{r['variant']:<42} {r['cagr']*100:>+7.2f}% {r['sortino']:>9.3f} {r['mdd']*100:>+8.1f}%")


if __name__ == "__main__":
    main()
