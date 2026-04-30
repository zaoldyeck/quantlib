"""Robustness & OOS checks on the chase-v4 ensemble.

Questions to resolve before calling this a real alpha:
  1. Year-by-year performance breakdown — is the alpha one crazy year?
  2. Chase alone on 2015-2017 (earlier sample) — does it survive bear regimes?
  3. Parameter sensitivity — does ±10% threshold / trail change outcomes?
  4. Turnover / cost realism — how often does chase trade? cost drag?
  5. Largest winners concentration — is CAGR driven by 3 stocks or diversified?
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import polars as pl
import vectorbt as vbt
import empyrical as ep

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", "strat_lab"))  # v4 lives here since 2026-04-30
from db import connect  # noqa: E402
import v4 as v4_mod  # noqa: E402

sys.path.insert(0, HERE)
from chase_trailing_stop import (  # noqa: E402
    load_price_panel, build_entries_return_breakout, run_chase,
    compute_split_safe_drip_returns, metrics, extract_returns,
    INIT_CAPITAL,
)

START_FULL = "2015-01-02"
END = "2026-04-17"


def year_breakdown(rets: pd.Series, label: str) -> pd.DataFrame:
    """Yearly CAGR / Sharpe / MDD."""
    df = rets.to_frame("ret").assign(year=rets.index.year)
    rows = []
    for yr, grp in df.groupby("year"):
        r = grp["ret"]
        yr_cum = (1 + r).prod() - 1
        yr_sharpe = ep.sharpe_ratio(r) if len(r) > 10 else 0
        peak = (1 + r).cumprod().cummax()
        dd = ((1 + r).cumprod() - peak) / peak
        yr_mdd = dd.min() if len(dd) else 0
        rows.append({"year": yr, "ret": yr_cum, "sharpe": yr_sharpe, "mdd": yr_mdd, "n": len(r)})
    out = pd.DataFrame(rows)
    out["label"] = label
    return out


def main():
    con = connect()

    # --- Full-history chase (2015-2026) to see bear-regime survival ---
    print("[1] running chase 2015-2026 (full history) ...")
    prices_full, tv_full = load_price_panel(con, START_FULL, END)
    entries_full = build_entries_return_breakout(
        prices_full, tv_full, lookback=60, ret_thresh=0.80
    )
    pf_full = run_chase(prices_full, entries_full, trail_stop=0.15, slot_dollars=200_000)
    chase_full = extract_returns(pf_full)
    chase_full.name = "chase_full"
    m = metrics(chase_full, "chase 2015-2026")
    print(f"    CAGR {m['CAGR']*100:+.2f}% / Sharpe {m['Sharpe']:.2f} / MDD {m['MDD']*100:+.2f}%")

    # Year breakdown
    print("\n[2] Year-by-year breakdown ...")
    yb = year_breakdown(chase_full, "chase")
    print(yb.to_string(index=False))

    # --- v4 over same full window for comparison ---
    print("\n[3] running v4 2015-2026 ...")
    v4_result = v4_mod.backtest(START_FULL, END, min_day=1,
                                 capital=INIT_CAPITAL, use_regime=True)
    v4_rets = pd.Series(v4_result["net_returns"],
                         index=pd.to_datetime(v4_result["dates"]), name="v4")
    m_v4 = metrics(v4_rets, "v4 2015-2026")
    print(f"    v4 2015-2026: CAGR {m_v4['CAGR']*100:+.2f}% / Sharpe {m_v4['Sharpe']:.2f} / MDD {m_v4['MDD']*100:+.2f}%")

    yb_v4 = year_breakdown(v4_rets, "v4")
    print(yb_v4.to_string(index=False))

    # --- Blend over full history ---
    print("\n[4] blend 50/50 over full 2015-2026 ...")
    aligned = pd.DataFrame({"v4": v4_rets, "chase": chase_full}).dropna(how="any")
    corr = aligned["v4"].corr(aligned["chase"])
    print(f"    correlation full period: {corr:.3f}")
    blend = 0.5 * aligned["v4"] + 0.5 * aligned["chase"]
    m_blend = metrics(blend, "blend 50/50 full")
    print(f"    blend CAGR {m_blend['CAGR']*100:+.2f}% / Sharpe {m_blend['Sharpe']:.2f} / MDD {m_blend['MDD']*100:+.2f}%")

    yb_blend = year_breakdown(blend, "blend")
    print(yb_blend.to_string(index=False))

    # --- Parameter sensitivity (small grid around best chase) ---
    print("\n[5] chase parameter sensitivity (2018-2026)...")
    prices_18, tv_18 = load_price_panel(con, "2018-01-02", END)
    bench = compute_split_safe_drip_returns(con, "0050", "2018-01-02", END)

    grid = []
    for lb in [45, 60, 75]:
        for th in [0.60, 0.80, 1.00]:
            for trail in [0.10, 0.15, 0.20]:
                entries = build_entries_return_breakout(prices_18, tv_18, lookback=lb, ret_thresh=th)
                pf = run_chase(prices_18, entries, trail_stop=trail, slot_dollars=200_000)
                r = extract_returns(pf)
                m_g = metrics(r, f"lb={lb} th={th} trail={trail}")
                grid.append({
                    "lookback": lb, "threshold": th, "trail": trail,
                    "CAGR": m_g["CAGR"], "Sharpe": m_g["Sharpe"], "MDD": m_g["MDD"],
                })
    grid_df = pd.DataFrame(grid)
    print(grid_df.to_string(index=False))

    best_cagr = grid_df.loc[grid_df["CAGR"].idxmax()]
    best_sharpe = grid_df.loc[grid_df["Sharpe"].idxmax()]
    print(f"\n  best CAGR:   lookback={best_cagr['lookback']:.0f}, threshold={best_cagr['threshold']:.2f}, trail={best_cagr['trail']:.2f} → CAGR {best_cagr['CAGR']*100:+.2f}%")
    print(f"  best Sharpe: lookback={best_sharpe['lookback']:.0f}, threshold={best_sharpe['threshold']:.2f}, trail={best_sharpe['trail']:.2f} → Sharpe {best_sharpe['Sharpe']:.2f}")

    # CAGR spread: if best vs worst in the grid is >5pp, strategy may be overfit
    cagr_spread = grid_df["CAGR"].max() - grid_df["CAGR"].min()
    print(f"  CAGR spread across 27 configs: {cagr_spread*100:.1f}pp")
    if cagr_spread > 0.08:
        print("  ⚠️  >8pp spread suggests parameter sensitivity / potential overfit")
    else:
        print("  ✅  ≤8pp spread — parameters are robust-ish")

    # --- Trade count & winner concentration ---
    print("\n[6] chase trade statistics ...")
    trades = pf_full.trades.records_readable
    if len(trades) > 0:
        trades["pnl_pct"] = trades["Return"]
        n_trades = len(trades)
        n_winners = (trades["pnl_pct"] > 0).sum()
        n_losers = (trades["pnl_pct"] < 0).sum()
        avg_win = trades.loc[trades["pnl_pct"] > 0, "pnl_pct"].mean()
        avg_loss = trades.loc[trades["pnl_pct"] < 0, "pnl_pct"].mean()
        print(f"    total trades: {n_trades}, win rate: {n_winners/n_trades:.1%}")
        print(f"    avg winner: {avg_win*100:+.2f}%, avg loser: {avg_loss*100:+.2f}%")
        top5 = trades.nlargest(5, "PnL")
        print(f"    top 5 trades' PnL contribution: {top5['PnL'].sum() / trades['PnL'].sum():.1%}")
        top10 = trades.nlargest(10, "PnL")
        print(f"    top 10 trades' PnL contribution: {top10['PnL'].sum() / trades['PnL'].sum():.1%}")
        print(f"    top 5 winners:")
        print(top5[["Column", "Entry Timestamp", "Exit Timestamp", "PnL", "Return"]].to_string(index=False))


if __name__ == "__main__":
    main()
