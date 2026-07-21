"""iter_37 - professional next-open catalyst backtest.

This is a timing-correct replacement for iter_24 when validating production
quality hybrid candidates.

Execution model:
  1. Today's close generates entry/exit/pyramid signals.
  2. Orders execute at the next available open.
  3. End-of-day NAV is marked at close.
  4. All prices are adjusted total-return OHLCV from research.prices.

It intentionally keeps the same signal family as iter_24 so we can isolate the
effect of execution timing:
  - revenue-confirmed breakout entry
  - ATR/fixed trailing, MA200, revenue fade exits
  - optional pyramid adds
  - 0050 buffer for unused catalyst sleeve capital, matching the existing
    hybrid convention.
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect
from prices import fetch_adjusted_panel


RESULTS = Path("research/strat_lab/results")
CAPITAL = 1_000_000.0
COMMISSION = 0.000285
SELL_TAX = 0.003
TDPY = 252


def metrics(nav_arr: np.ndarray, days: list[date], capital: float = CAPITAL) -> dict[str, float]:
    rets = np.diff(np.concatenate([[capital], nav_arr])) / np.concatenate([[capital], nav_arr[:-1]])
    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (nav_arr[-1] / capital) ** (1 / years) - 1
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY)) if len(rets) > 1 else 0.0
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    peak = capital
    mdd = 0.0
    for v in nav_arr:
        peak = max(peak, float(v))
        mdd = min(mdd, (float(v) - peak) / peak)
    return {
        "cagr": float(cagr),
        "sortino": float((cagr - 0.01) / downvol) if downvol > 0 else 0.0,
        "sharpe": float((cagr - 0.01) / vol) if vol > 0 else 0.0,
        "mdd": float(mdd),
        "vol": vol,
        "downvol": downvol,
        "final_nav": float(nav_arr[-1]),
    }


def build_daily_panel(con, start: date, end: date, breakout_lookback: int) -> pl.DataFrame:
    print("[iter37] fetch adjusted OHLCV (twse+tpex) ...", flush=True)
    panels = []
    for market in ("twse", "tpex"):
        p = fetch_adjusted_panel(
            con,
            start.isoformat(),
            end.isoformat(),
            market=market,
            include_extra_history_days=max(320, breakout_lookback + 220),
        )
        if not p.is_empty():
            panels.append(p)
    if not panels:
        raise RuntimeError("no adjusted price data")
    df = (
        pl.concat(panels)
        .filter(
            (pl.col("open") > 0)
            & (pl.col("high") > 0)
            & (pl.col("low") > 0)
            & (pl.col("close") > 0)
            & (pl.col("volume") > 0)
        )
        .filter(pl.col("company_code").str.contains(r"^[1-9][0-9]{3}$"))
        .sort(["company_code", "date"])
        .rename({"volume": "vol"})
    )
    df = df.with_columns(pl.col("close").shift(1).over("company_code").alias("prev_close"))
    df = df.with_columns(
        pl.max_horizontal(
            [
                pl.col("high") - pl.col("low"),
                (pl.col("high") - pl.col("prev_close")).abs(),
                (pl.col("low") - pl.col("prev_close")).abs(),
            ]
        ).alias("tr")
    )
    df = df.with_columns(
        [
            pl.col("close").shift(1).rolling_max(breakout_lookback).over("company_code").alias("breakout_high"),
            pl.col("vol").shift(1).rolling_mean(60).over("company_code").alias("vol_60d_avg"),
            pl.col("close").rolling_mean(200).over("company_code").alias("ma200"),
            pl.col("trade_value").rolling_mean(60).over("company_code").alias("adv60_ntd"),
            pl.col("tr").rolling_mean(14).over("company_code").alias("atr14"),
        ]
    )

    print("[iter37] join PIT monthly revenue ...", flush=True)
    rev = con.sql(
        """
        SELECT company_code, year, month, monthly_revenue_yoy AS yoy
        FROM operating_revenue
        WHERE monthly_revenue_yoy IS NOT NULL
          AND regexp_matches(company_code, '^[1-9][0-9]{3}$')
        """
    ).pl()
    rev = (
        rev.with_columns(
            pl.date(pl.col("year"), pl.col("month"), 1)
            .dt.offset_by("1mo")
            .dt.offset_by("12d")
            .alias("publish_d")
        )
        .select(["company_code", "publish_d", "yoy"])
        .sort(["company_code", "publish_d"])
    )
    df = (
        df.sort(["company_code", "date"])
        .join_asof(rev, left_on="date", right_on="publish_d", by="company_code", strategy="backward")
        .rename({"yoy": "latest_yoy"})
        .filter((pl.col("date") >= start) & (pl.col("date") <= end))
    )
    print(f"[iter37] panel rows={df.height:,}, codes={df['company_code'].n_unique():,}", flush=True)
    return df


def fetch_static_filters(con) -> tuple[set[str], dict[str, date], dict[str, str]]:
    etf_codes = {r[0] for r in con.sql("SELECT DISTINCT company_code FROM etf").fetchall()}
    listing = {
        r[0]: r[1]
        for r in con.sql(
            """
            SELECT company_code, MIN(date)
            FROM daily_quote
            WHERE closing_price > 0
            GROUP BY company_code
            """
        ).fetchall()
    }
    industry = {
        r[0]: r[1]
        for r in con.sql(
            """
            SELECT DISTINCT ON (company_code) company_code, industry
            FROM operating_revenue
            WHERE industry IS NOT NULL
            ORDER BY company_code, year DESC, month DESC
            """
        ).fetchall()
    }
    return etf_codes, listing, industry


def fetch_0050(con, start: date, end: date) -> pl.DataFrame:
    return (
        fetch_adjusted_panel(con, start.isoformat(), end.isoformat(), codes=["0050"], market="twse", include_extra_history_days=90)
        .filter((pl.col("date") >= start) & (pl.col("date") <= end))
        .sort("date")
        .select(["date", "open", "close"])
    )


def run_backtest(
    start: date,
    end: date,
    max_positions: int,
    breakout_lookback: int,
    vol_multiplier: float,
    yoy_entry: float,
    yoy_exit: float,
    atr_trailing: bool,
    atr_mult: float,
    fixed_trailing: float,
    min_adv: float,
    target_weight_new: float,
    pyramid: bool,
    out_suffix: str,
    capital: float = CAPITAL,
) -> dict[str, float]:
    t0 = time.time()
    con = connect()
    panel = build_daily_panel(con, start, end, breakout_lookback)
    etf_codes, listing, industry = fetch_static_filters(con)
    px0050_df = fetch_0050(con, start, end)
    days = px0050_df["date"].to_list()
    px0050 = {r["date"]: r for r in px0050_df.iter_rows(named=True)}
    panel_by_date = {k[0] if isinstance(k, tuple) else k: g for k, g in panel.sort("date").group_by("date", maintain_order=True)}
    row_lookup = {(r["date"], r["company_code"]): r for r in panel.iter_rows(named=True)}

    cash_0050_units = capital / px0050[days[0]]["open"]
    positions: dict[str, dict] = {}
    pending_exits: dict[str, str] = {}
    pending_entries: list[dict] = []
    pending_adds: list[str] = []
    nav_hist = []
    trades = []
    max_active = 0

    def stock_value(d: date, code: str, pos: dict, px_col: str) -> float:
        row = row_lookup.get((d, code))
        px = row.get(px_col) if row else None
        if px is None or px <= 0:
            px = pos["last_close"]
        return pos["shares"] * px

    def nav_at(d: date, px_col: str = "close") -> float:
        buffer_px = px0050[d][px_col]
        return cash_0050_units * buffer_px + sum(stock_value(d, c, p, px_col) for c, p in positions.items())

    for di, d in enumerate(days):
        buffer_open = px0050[d]["open"]

        # 1) Execute exits from prior close at today's open.
        for code, reason in list(pending_exits.items()):
            pos = positions.pop(code, None)
            if pos is None:
                continue
            row = row_lookup.get((d, code))
            sell_px = row.get("open") if row else None
            if sell_px is None or sell_px <= 0:
                sell_px = pos["last_close"]
            proceeds = pos["shares"] * sell_px * (1 - SELL_TAX - COMMISSION)
            cash_0050_units += proceeds / buffer_open
            trades.append(
                {
                    "date": d,
                    "code": code,
                    "action": f"exit_{reason}",
                    "price": sell_px,
                    "entry_d": pos["entry_d"],
                    "entry_px": pos["entry_px"],
                    "ret": sell_px / pos["entry_px"] - 1,
                }
            )
        pending_exits.clear()

        # 2) Execute pyramid adds from prior close at today's open.
        nav_open = nav_at(d, "open")
        for code in list(pending_adds):
            pos = positions.get(code)
            if pos is None:
                continue
            row = row_lookup.get((d, code))
            buy_px = row.get("open") if row else None
            if buy_px is None or buy_px <= 0:
                continue
            add_dollar = min(nav_open * 0.10, cash_0050_units * buffer_open)
            if add_dollar <= 0:
                continue
            add_shares = add_dollar / buy_px / (1 + COMMISSION)
            cost = add_shares * buy_px * (1 + COMMISSION)
            cash_0050_units -= cost / buffer_open
            pos["shares"] += add_shares
            pos["pyramid_lvl"] += 1
            trades.append({"date": d, "code": code, "action": f"pyramid{pos['pyramid_lvl']}", "price": buy_px, "entry_d": pos["entry_d"], "entry_px": pos["entry_px"], "ret": None})
        pending_adds = []

        # 3) Execute new entries from prior close at today's open.
        nav_open = nav_at(d, "open")
        for sig in pending_entries:
            if len(positions) >= max_positions:
                break
            code = sig["company_code"]
            if code in positions:
                continue
            row = row_lookup.get((d, code))
            buy_px = row.get("open") if row else None
            if buy_px is None or buy_px <= 0:
                continue
            target_dollar = min(nav_open * target_weight_new, cash_0050_units * buffer_open)
            if target_dollar <= 0:
                break
            shares = target_dollar / buy_px / (1 + COMMISSION)
            cost = shares * buy_px * (1 + COMMISSION)
            cash_0050_units -= cost / buffer_open
            positions[code] = {
                "shares": shares,
                "entry_d": d,
                "entry_px": buy_px,
                "peak_close": buy_px,
                "last_close": buy_px,
                "pyramid_lvl": 0,
                "entry_atr": sig.get("atr14"),
            }
            trades.append({"date": d, "code": code, "action": "entry", "price": buy_px, "entry_d": d, "entry_px": buy_px, "ret": None})
        pending_entries = []

        # 4) Mark positions at close.
        for code, pos in list(positions.items()):
            row = row_lookup.get((d, code))
            if row is None:
                continue
            close = row.get("close")
            if close is None or close <= 0:
                continue
            pos["last_close"] = close
            pos["peak_close"] = max(pos["peak_close"], close)

        nav_close = nav_at(d, "close")
        max_active = max(max_active, len(positions))
        nav_hist.append((d, nav_close, len(positions), cash_0050_units * px0050[d]["close"]))

        # 5) Generate exits and pyramid orders for next open.
        pending_exits = {}
        for code, pos in positions.items():
            row = row_lookup.get((d, code))
            if row is None:
                continue
            close = row.get("close")
            ma200 = row.get("ma200")
            yoy = row.get("latest_yoy")
            if atr_trailing:
                entry_atr = pos.get("entry_atr")
                entry_px = pos.get("entry_px")
                if entry_atr is not None and entry_atr > 0 and entry_px and entry_px > 0:
                    trail_pct = max(0.10, min(0.25, entry_atr / entry_px * atr_mult))
                else:
                    trail_pct = fixed_trailing
            else:
                trail_pct = fixed_trailing
            reason = None
            if pos["peak_close"] > 0 and close / pos["peak_close"] - 1 <= -trail_pct:
                reason = "trailing_atr" if atr_trailing else "trailing"
            elif ma200 is not None and close < ma200:
                reason = "below_ma200"
            elif yoy is not None and yoy < yoy_exit:
                reason = "yoy_fade"
            if reason:
                pending_exits[code] = reason
                continue
            if pyramid:
                factor = close / pos["entry_px"] if pos["entry_px"] else 1.0
                if pos.get("pyramid_lvl", 0) == 0 and factor >= 1.30:
                    pending_adds.append(code)
                elif pos.get("pyramid_lvl", 0) == 1 and factor >= 1.60:
                    pending_adds.append(code)

        # 6) Generate entries for next open.
        pending_entries = []
        available = max_positions - (len(positions) - len(pending_exits))
        if available > 0:
            today = panel_by_date.get(d)
            if today is not None and not today.is_empty():
                signals = (
                    today.filter(
                        (pl.col("close") > pl.col("breakout_high"))
                        & (pl.col("vol") > pl.col("vol_60d_avg") * vol_multiplier)
                        & (pl.col("latest_yoy") >= yoy_entry)
                        & (pl.col("adv60_ntd") >= min_adv)
                    )
                    .sort(by=[(pl.col("close") / pl.col("breakout_high") - 1)], descending=True)
                )
                held_after_exits = set(positions) - set(pending_exits)
                for sig in signals.iter_rows(named=True):
                    code = sig["company_code"]
                    if code in held_after_exits or code in pending_exits:
                        continue
                    if code in etf_codes:
                        continue
                    ind = industry.get(code, "")
                    if "金融" in ind or "證券" in ind or "保險" in ind:
                        continue
                    first_day = listing.get(code)
                    if first_day is None or (d - first_day).days < 90:
                        continue
                    pending_entries.append(sig)
                    held_after_exits.add(code)
                    if len(pending_entries) >= available:
                        break

        if di % 1000 == 0:
            print(f"  [iter37] {di:>5}/{len(days)} {d} nav=${nav_close:,.0f} active={len(positions)}", flush=True)

    nav_df = pl.DataFrame(
        {
            "date": [x[0] for x in nav_hist],
            "nav": [x[1] for x in nav_hist],
            "n_active": [x[2] for x in nav_hist],
            "buffer_value": [x[3] for x in nav_hist],
        }
    )
    trades_df = pl.DataFrame(trades) if trades else pl.DataFrame(schema={"date": pl.Date, "code": pl.Utf8, "action": pl.Utf8})
    m = metrics(nav_df["nav"].to_numpy(), nav_df["date"].to_list(), capital)
    m.update({"max_active": max_active, "n_entries": int((trades_df["action"] == "entry").sum()) if trades_df.height else 0})

    RESULTS.mkdir(parents=True, exist_ok=True)
    nav_path = RESULTS / f"iter_37_{out_suffix}_daily.csv"
    trades_path = RESULTS / f"iter_37_{out_suffix}_trades.csv"
    nav_df.write_csv(nav_path)
    trades_df.write_csv(trades_path)
    m["nav_path"] = str(nav_path)
    m["trades_path"] = str(trades_path)
    m["runtime_s"] = time.time() - t0
    return m


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2005-01-03")
    ap.add_argument("--end", default="2026-05-08")
    ap.add_argument("--max-positions", type=int, default=7)
    ap.add_argument("--breakout-lookback", type=int, default=90)
    ap.add_argument("--vol-multiplier", type=float, default=2.0)
    ap.add_argument("--yoy-entry", type=float, default=30.0)
    ap.add_argument("--yoy-exit", type=float, default=0.0)
    ap.add_argument("--atr-trailing", action="store_true")
    ap.add_argument("--atr-mult", type=float, default=3.0)
    ap.add_argument("--trailing-stop", type=float, default=0.15)
    ap.add_argument("--min-adv", type=float, default=50_000_000.0)
    ap.add_argument("--target-weight-new", type=float, default=0.10)
    ap.add_argument("--no-pyramid", action="store_true")
    ap.add_argument("--out-suffix", default=None)
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    suffix = args.out_suffix or (
        f"max{args.max_positions}_lkb{args.breakout_lookback}_v{args.vol_multiplier:g}"
        f"_y{args.yoy_entry:g}_{'atr' if args.atr_trailing else 'fix'}_nextopen"
    )
    print("=" * 96, flush=True)
    print("iter_37 catalyst next-open", flush=True)
    print(f"  window={start}->{end}, max_positions={args.max_positions}, lkb={args.breakout_lookback}, vol={args.vol_multiplier}, yoy={args.yoy_entry}", flush=True)
    print(f"  execution=signal close -> next open, atr={args.atr_trailing}, pyramid={not args.no_pyramid}", flush=True)
    print("=" * 96, flush=True)
    res = run_backtest(
        start=start,
        end=end,
        max_positions=args.max_positions,
        breakout_lookback=args.breakout_lookback,
        vol_multiplier=args.vol_multiplier,
        yoy_entry=args.yoy_entry,
        yoy_exit=args.yoy_exit,
        atr_trailing=args.atr_trailing,
        atr_mult=args.atr_mult,
        fixed_trailing=args.trailing_stop,
        min_adv=args.min_adv,
        target_weight_new=args.target_weight_new,
        pyramid=not args.no_pyramid,
        out_suffix=suffix,
    )
    print(
        f"\nCAGR {res['cagr']*100:+.2f}% Sortino {res['sortino']:.3f} "
        f"Sharpe {res['sharpe']:.3f} MDD {res['mdd']*100:.2f}% "
        f"entries {res['n_entries']} max_active {res['max_active']} "
        f"runtime {res['runtime_s']:.1f}s",
        flush=True,
    )
    print(f"Saved: {res['nav_path']}", flush=True)


if __name__ == "__main__":
    main()
