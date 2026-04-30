"""iter_21 — Hybrid 50% iter_13 mcap + 50% iter_20 v8 breakout，年度 50/50 rebal。

User 確認：兩策略各自獨立 max 10 names，總 portfolio max 20 names。

設計：
  Year 0:  50% to iter_13 mcap、50% to iter_20 breakout
  Daily:   兩 strategy NAVs 各自獨立累積
  Year-end: rebalance 回 50/50 split

  期望：iter_13 (Sortino 1.352) 提供 downside 控制，iter_20 (CAGR 22%) 提供 momentum
  → 預期 Sortino 1.15-1.30、CAGR 22-23%、MDD 改善

實作：直接讀兩 daily NAV 檔案、合成。沒重跑 backtest。

評估窗口（依鐵則）：永遠 2005-01-03 → 2026-04-25 完整 21 年。
"""
from __future__ import annotations

import argparse
import math
import os
from datetime import date

import numpy as np
import polars as pl


TDPY = 252


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2005-01-03")
    ap.add_argument("--end",   default="2026-04-25")
    ap.add_argument("--capital", type=float, default=1_000_000)
    ap.add_argument("--w-iter13", type=float, default=0.5)
    ap.add_argument("--w-iter20", type=float, default=0.5)
    args = ap.parse_args()

    # 讀兩 daily NAV
    n13 = pl.read_csv("research/strat_lab/results/iter_13_mcap_daily.csv",
                      try_parse_dates=True).sort("date")
    n20 = pl.read_csv("research/strat_lab/results/iter_20_daily.csv",
                      try_parse_dates=True).sort("date")
    print(f"iter_13 mcap NAV rows: {len(n13)}")
    print(f"iter_20 v8  NAV rows: {len(n20)}")

    # iter_13 NAV 是「daily return」+「nav」 — 取 nav 列
    # iter_20 直接是 nav
    n13 = n13.select(["date", pl.col("nav").alias("nav_13")])
    n20 = n20.select(["date", pl.col("nav").alias("nav_20")])
    df = n13.join(n20, on="date", how="inner").sort("date")
    print(f"joined rows: {len(df)}")

    # 計算 daily return for each
    df = df.with_columns([
        (pl.col("nav_13").pct_change()).fill_null(0.0).alias("ret_13"),
        (pl.col("nav_20").pct_change()).fill_null(0.0).alias("ret_20"),
        pl.col("date").dt.year().alias("year"),
    ])

    # 模擬 hybrid: 年度 rebal 回 w_13 / w_20
    capital = args.capital
    w_13, w_20 = args.w_iter13, args.w_iter20
    print(f"weights: iter_13 = {w_13:.0%}, iter_20 = {w_20:.0%}")

    # iterate years
    nav = capital
    nav_hist = []
    for yr, sub in df.group_by("year", maintain_order=True):
        # year start: rebal nav into w_13 × nav and w_20 × nav
        cap_13 = nav * w_13
        cap_20 = nav * w_20
        for r13, r20, d in zip(sub["ret_13"].to_list(), sub["ret_20"].to_list(),
                                  sub["date"].to_list()):
            cap_13 *= (1 + r13)
            cap_20 *= (1 + r20)
            nav = cap_13 + cap_20
            nav_hist.append((d, nav))

    nav_arr = np.array([n for _, n in nav_hist])
    rets = np.diff(np.concatenate([[capital], nav_arr])) / np.concatenate([[capital], nav_arr[:-1]])
    days_list = [d for d, _ in nav_hist]
    years = max((days_list[-1] - days_list[0]).days / 365.25, 1e-9)
    cagr = (nav_arr[-1] / capital) ** (1 / years) - 1
    vol_ann = rets.std(ddof=1) * math.sqrt(TDPY)
    downside = rets[rets < 0]
    downvol_ann = (downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    sharpe = (cagr - 0.01) / vol_ann if vol_ann > 0 else 0
    sortino = (cagr - 0.01) / downvol_ann if downvol_ann > 0 else 0
    peak, mdd = capital, 0.0
    for v in nav_arr:
        peak = max(peak, v); mdd = min(mdd, (v - peak) / peak)

    # 寫 result
    pl.DataFrame({"date": days_list, "nav": nav_arr}).write_csv(
        "research/strat_lab/results/iter_21_daily.csv")

    print("\n--- iter_21 hybrid 結果 ---")
    print(f"  CAGR:    {cagr:+.2%}")
    print(f"  Sharpe:  {sharpe:.3f}")
    print(f"  Sortino: {sortino:.3f}  ★")
    print(f"  MDD:     {mdd:.2%}")
    print(f"  finalNAV: ${nav_arr[-1]:,.0f}")

    print("\n--- 對照 ---")
    print(f"  hold_2330: CAGR +24.23% Sortino 1.333 MDD -45.86%")
    print(f"  iter_13 mcap: CAGR +22.76% Sortino 1.352 MDD -44.00%")
    print(f"  iter_20 v8:   CAGR +20.27% Sortino 0.960 MDD -62.48%")

    print("\n--- 是否破 2330？ ---")
    print(f"  CAGR    > 2330: {'✓' if cagr > 0.2423 else '✗'} ({cagr:+.2%})")
    print(f"  Sortino > 2330: {'✓' if sortino > 1.333 else '✗'} ({sortino:.3f})")
    print(f"  MDD     > 2330: {'✓' if mdd > -0.4586 else '✗'} ({mdd:.2%})")


if __name__ == "__main__":
    main()
