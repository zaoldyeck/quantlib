"""畫多策略對戰圖 vs 0050 / 2330"""
import os
import sys
from datetime import date

import numpy as np
import plotly.graph_objects as go
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect
from prices import fetch_daily_returns

con = connect()
START, END = date(2005, 1, 3), date(2026, 4, 25)
CAPITAL = 1_000_000

# 取 benchmarks（dividend-adjusted via prices.py）
print("loading benchmarks...")
b_2330 = fetch_daily_returns(con, START.isoformat(), END.isoformat(), codes=["2330"], market="twse").sort("date")
b_2330_nav = (CAPITAL * np.cumprod(1 + b_2330["ret"].fill_null(0).to_numpy())).tolist()
b_0050 = fetch_daily_returns(con, START.isoformat(), END.isoformat(), codes=["0050"], market="twse").sort("date")
b_0050_nav = (CAPITAL * np.cumprod(1 + b_0050["ret"].fill_null(0).to_numpy())).tolist()
b_dates = b_2330["date"].to_list()

# 策略 NAVs
strats = {
    "iter_21 hybrid (BEST OOS)":  ("research/strat_lab/results/iter_21_daily.csv", "#d62728"),
    "iter_13 mcap (best single)": ("research/strat_lab/results/iter_13_mcap_daily.csv", "#1f77b4"),
    "iter_25 hybrid 70/30":       ("research/strat_lab/results/iter_25_daily.csv", "#2ca02c"),
    "iter_24 pyramid":            ("research/strat_lab/results/iter_24_daily.csv", "#9467bd"),
    "iter_20 v8 breakout":        ("research/strat_lab/results/iter_20_daily.csv", "#ff7f0e"),
}

fig = go.Figure()

# 策略線
for label, (path, color) in strats.items():
    if not os.path.exists(path):
        print(f"skip {label}"); continue
    df = pl.read_csv(path, try_parse_dates=True).sort("date")
    df = df.filter((pl.col("date") >= START) & (pl.col("date") <= END))
    fig.add_trace(go.Scatter(
        x=df["date"].to_list(), y=df["nav"].to_list(),
        mode="lines", name=label, line=dict(color=color, width=2),
    ))

# benchmark 線（粗、灰）
fig.add_trace(go.Scatter(
    x=b_dates, y=b_2330_nav, mode="lines",
    name="2330 hold (benchmark)",
    line=dict(color="black", width=3, dash="solid"),
))
fig.add_trace(go.Scatter(
    x=b_dates, y=b_0050_nav, mode="lines",
    name="0050 hold (TWSE beta)",
    line=dict(color="gray", width=3, dash="dash"),
))

fig.update_layout(
    title="<b>21 年完整窗口策略對戰 vs 2330 / 0050</b><br>"
          "<sub>$1M → final NAV (log scale)，2005-01-03 → 2026-04-25</sub>",
    xaxis_title="Date",
    yaxis_title="NAV (NTD, log scale)",
    yaxis_type="log",
    hovermode="x unified",
    template="plotly_white",
    legend=dict(orientation="v", yanchor="top", y=0.98, xanchor="left", x=0.02,
                bgcolor="rgba(255,255,255,0.8)"),
    height=750, width=1300,
)

# 加上 final NAV annotations
final_data = []
for label, (path, color) in strats.items():
    if os.path.exists(path):
        df = pl.read_csv(path, try_parse_dates=True).sort("date")
        df = df.filter((pl.col("date") >= START) & (pl.col("date") <= END))
        if df.height: final_data.append((label, df["nav"].to_list()[-1], color))
final_data.append(("2330 hold", b_2330_nav[-1], "black"))
final_data.append(("0050 hold", b_0050_nav[-1], "gray"))
final_data.sort(key=lambda x: x[1], reverse=True)

print(f"\n{'='*60}")
print(f"Final NAV ranking (從 $1M 開始):")
print(f"{'='*60}")
for label, nav, _ in final_data:
    print(f"  {label:<40}  ${nav/1e6:>6.1f}M  ({nav/1e6:.0f}x)")

out_path = "research/strat_lab/results/strategy_comparison.html"
fig.write_html(out_path)
print(f"\n圖表已寫入: {out_path}")

# 也輸出 PNG (如果有 kaleido)
try:
    fig.write_image(out_path.replace(".html", ".png"), width=1300, height=750, scale=2)
    print(f"PNG 也輸出: {out_path.replace('.html', '.png')}")
except Exception as e:
    print(f"(PNG export 跳過: {e})")
