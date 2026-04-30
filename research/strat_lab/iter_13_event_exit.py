"""iter_13 event-driven exit ablation — adds stop-loss + quality-fade exit layers.

Background:
  Memory feedback_quant_standards.md and CLAUDE.md mandate event-driven over
  calendar-driven. The base iter_13.py uses pure monthly rebal — it picks 5
  stocks at month start and holds them unchanged until next month, even if
  one tanks -50% mid-month. This script adds intra-month event exits.

Exit layers tested:
  1. **Stop-loss** — individual position drops X% from entry-period peak →
     exit immediately, weight → 0050 buffer until next month
  2. **Quality fade (TBD)** — if quarter rolls and stock fails ROA/GM filter
     mid-period → exit. Skip for now (quality pool is monthly re-screened
     so this overlaps with monthly rebal).

Exit semantics:
  - Triggered position's weight → 0050 from trigger-date+1
  - Compounding inside the stopped period uses 0050 daily returns
  - At next month rebal, weight resets to new pick set

Output:
  research/strat_lab/results/iter_13_monthly_mcap_dual_sl{X}_daily.csv
  where X ∈ {0 (no stop), 15, 20, 25, 30}

Compare: NO_STOP baseline vs each stop-loss threshold.
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
from datetime import date

import numpy as np
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect
from prices import fetch_daily_returns

RESULTS = "research/strat_lab/results"
TDPY = 252
RF = 0.01
SELL_TAX = 0.003
COMMISSION = 0.000285


def apply_stop_loss(picks_csv: str, stop_loss_pct: float, start: date, end: date,
                     output_csv: str) -> dict:
    """Replay the iter_13 monthly NAV with intra-month stop-loss event exit.

    Args:
      picks_csv: input 'iter_13_monthly_mcap_dual_picks.csv' (rebal_d, code, weight)
      stop_loss_pct: 0.0 = no stop, 0.25 = -25% from period peak triggers exit
      start, end: backtest window
      output_csv: output NAV CSV path
    """
    t0 = time.time()
    con = connect()

    picks = pl.read_csv(picks_csv, try_parse_dates=True,
                          schema_overrides={"company_code": pl.Utf8}).sort("rebal_d")
    rebal_ds = sorted(picks["rebal_d"].unique().to_list())
    print(f"[event_exit] {len(rebal_ds)} rebal dates ({rebal_ds[0]} → {rebal_ds[-1]}), "
          f"stop_loss = {stop_loss_pct:.0%}")

    # Get all daily returns for held stocks (incl 0050 for buffer)
    held_codes = picks["company_code"].unique().to_list()
    if "0050" not in held_codes:
        held_codes.append("0050")
    twse_rets = fetch_daily_returns(con, start.isoformat(), end.isoformat(),
                                      codes=held_codes, market="twse")
    tpex_rets = fetch_daily_returns(con, start.isoformat(), end.isoformat(),
                                      codes=held_codes, market="tpex")
    rets = pl.concat([twse_rets, tpex_rets]).unique(subset=["date", "company_code"])

    # Daily 0050 returns
    rets_0050 = rets.filter(pl.col("company_code") == "0050").sort("date").select(
        ["date", pl.col("ret").alias("ret_0050")])

    # All trading days
    days_df = pl.DataFrame({
        "date": [r[0] for r in con.sql(f"""
            SELECT date FROM daily_quote WHERE market='twse' AND company_code='0050'
              AND date BETWEEN DATE '{start}' AND DATE '{end}' ORDER BY date
        """).fetchall()]
    })

    # Build (date → period_id) mapping where period_id = rebal_d
    rebal_df = pl.DataFrame({"period_start": rebal_ds}).with_columns(
        (pl.col("period_start") + pl.duration(days=1)).alias("effective"))
    da = days_df.sort("date").join_asof(
        rebal_df.sort("effective"), left_on="date", right_on="effective", strategy="backward"
    ).rename({"period_start": "rebal_d"})

    # Join picks per (rebal_d, code)
    daily_pos = (da.join(picks, on="rebal_d", how="left")
                    .filter(pl.col("company_code").is_not_null())
                    .join(rets, on=["date", "company_code"], how="left")
                    .with_columns(pl.col("ret").fill_null(0.0))
                    .sort(["rebal_d", "company_code", "date"]))

    # Apply stop-loss logic per (rebal_d, code) — vectorized via polars window
    if stop_loss_pct > 0:
        # cum growth from period start
        daily_pos = daily_pos.with_columns(
            (1 + pl.col("ret")).cum_prod().over(["rebal_d", "company_code"]).alias("cum_growth")
        )
        # peak growth (max so far)
        daily_pos = daily_pos.with_columns(
            pl.col("cum_growth").cum_max().over(["rebal_d", "company_code"]).alias("peak_growth")
        )
        # drawdown from peak
        daily_pos = daily_pos.with_columns(
            (pl.col("cum_growth") / pl.col("peak_growth") - 1.0).alias("dd_from_peak")
        )
        # ever-triggered flag (cumulative)
        daily_pos = daily_pos.with_columns(
            (pl.col("dd_from_peak") < -stop_loss_pct).cast(pl.Int8).alias("triggered_now")
        )
        daily_pos = daily_pos.with_columns(
            pl.col("triggered_now").cum_max().over(["rebal_d", "company_code"]).alias("ever_stopped")
        )
    else:
        daily_pos = daily_pos.with_columns([
            pl.lit(0).cast(pl.Int8).alias("ever_stopped"),
            pl.lit(0).cast(pl.Int8).alias("triggered_now"),
        ])

    # Daily contribution: stocks before stop, 0050 after stop
    # Join 0050 daily return
    daily_pos = daily_pos.join(rets_0050, on="date", how="left").with_columns(
        pl.col("ret_0050").fill_null(0.0))

    daily_pos = daily_pos.with_columns(
        pl.when(pl.col("company_code") == "0050")
          .then(pl.col("weight") * pl.col("ret"))   # 0050 buffer position
          .when(pl.col("ever_stopped") == 1)
          .then(pl.col("weight") * pl.col("ret_0050"))   # stopped → 0050 ret
          .otherwise(pl.col("weight") * pl.col("ret"))   # normal stock ret
          .alias("contrib")
    )

    # Aggregate to portfolio daily return
    port = (daily_pos.group_by("date").agg(pl.col("contrib").sum().alias("r"))
                     .sort("date").with_columns(pl.col("r").fill_null(0.0)))

    # Turnover cost — only on rebal days (entry / exit pairs)
    # Add stop-loss trigger turnover (exit a stock mid-period = sell + buy 0050)
    # For simplicity: count stop-loss trigger as 1 sale × stopped_weight
    triggered = daily_pos.filter(
        (pl.col("triggered_now") == 1) & (pl.col("ever_stopped") == 1) &
        (pl.col("company_code") != "0050")
    ).group_by("date").agg(pl.col("weight").sum().alias("triggered_weight"))
    port = port.join(triggered, on="date", how="left").with_columns(
        pl.col("triggered_weight").fill_null(0.0))
    # Cost: stop-loss trigger pays sell_tax + 2 × commission on triggered weight
    port = port.with_columns(
        (pl.col("triggered_weight") * (SELL_TAX + 2 * COMMISSION)).alias("trigger_cost")
    )

    # Monthly rebal turnover (existing logic)
    pbd = {}
    for row in picks.iter_rows(named=True):
        pbd.setdefault(row["rebal_d"], {})[row["company_code"]] = row["weight"]
    cmap, prev = {}, None
    for rd in sorted(rebal_ds):
        cs = pbd.get(rd, {})
        if prev is None:
            sf = 1.0 if cs else 0.0
        else:
            all_codes = set(cs.keys()) | set(prev.keys())
            sf = sum(abs(cs.get(c, 0) - prev.get(c, 0)) for c in all_codes) / 2
        cmap[rd] = sf * (SELL_TAX + 2 * COMMISSION)
        prev = cs
    cdf = pl.DataFrame({"date": list(cmap.keys()), "monthly_cost": list(cmap.values())})
    port = port.join(cdf, on="date", how="left").with_columns(
        pl.col("monthly_cost").fill_null(0.0))
    port = port.with_columns(
        (pl.col("r") - pl.col("trigger_cost") - pl.col("monthly_cost")).alias("net")
    )

    rets_arr = port["net"].to_numpy()
    capital = 1_000_000.0
    navs = capital * np.cumprod(1 + rets_arr)
    days_list = port["date"].to_list()

    years = max((days_list[-1] - days_list[0]).days / 365.25, 1e-9)
    cagr = (navs[-1] / capital) ** (1 / years) - 1
    vol = float(rets_arr.std(ddof=1) * math.sqrt(TDPY))
    downside = rets_arr[rets_arr < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - RF) / vol if vol > 0 else 0.0
    sortino = (cagr - RF) / downvol if downvol > 0 else 0.0
    peak_n, mdd = capital, 0.0
    for v in navs:
        peak_n = max(peak_n, v); mdd = min(mdd, (v - peak_n) / peak_n)

    # Count stop triggers
    n_triggers = int((daily_pos.filter(
        (pl.col("triggered_now") == 1) &
        (pl.col("company_code") != "0050")
    )).height)

    pl.DataFrame({"date": days_list, "nav": navs}).write_csv(output_csv)
    print(f"[event_exit] CAGR {cagr*100:+.2f}% Sortino {sortino:.3f} MDD {mdd*100:.1f}% "
          f"({n_triggers} stop triggers, {time.time()-t0:.1f}s)")
    return {"cagr": cagr, "sortino": sortino, "sharpe": sharpe, "mdd": mdd,
            "n_triggers": n_triggers, "out": output_csv}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2005-01-03")
    ap.add_argument("--end",   default="2026-04-25")
    ap.add_argument("--picks-csv", default=f"{RESULTS}/iter_13_monthly_mcap_dual_picks.csv")
    args = ap.parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)

    print("=" * 78)
    print("iter_13 event-driven exit ablation")
    print(f"  base: {args.picks_csv}")
    print(f"  testing stop-loss thresholds: 0% (baseline), 15%, 20%, 25%, 30%")
    print("=" * 78)

    results = []
    for sl in [0.0, 0.15, 0.20, 0.25, 0.30]:
        sl_str = f"sl{int(sl*100)}" if sl > 0 else "no_stop"
        out = f"{RESULTS}/iter_13_monthly_mcap_dual_{sl_str}_daily.csv"
        r = apply_stop_loss(args.picks_csv, sl, start, end, out)
        r["stop_loss_pct"] = sl
        results.append(r)

    # Summary
    print(f"\n{'=' * 78}")
    print(f"{'Stop-loss':>10} {'CAGR':>10} {'Sortino':>10} {'Sharpe':>10} {'MDD':>10} {'Triggers':>10}")
    print(f"{'─' * 70}")
    for r in results:
        sl_lbl = f"{r['stop_loss_pct']:.0%}" if r['stop_loss_pct'] > 0 else "none"
        print(f"{sl_lbl:>10} {r['cagr']*100:>9.2f}% {r['sortino']:>10.3f} "
              f"{r['sharpe']:>10.3f} {r['mdd']*100:>9.2f}% {r['n_triggers']:>10}")

    pl.DataFrame(results).write_csv(f"{RESULTS}/iter_13_event_exit_ablation.csv")
    print(f"\nSaved: {RESULTS}/iter_13_event_exit_ablation.csv")


if __name__ == "__main__":
    main()
