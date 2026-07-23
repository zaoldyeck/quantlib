"""Visual comparison for current strategy candidates versus 0050.

Inputs:
  - Iter67 full-switch NAV-lineage research benchmark daily NAV
  - Iter69 hard-cap 5 target-book audit daily NAV
  - Iter72 source-reconciled cap-5 attribution daily NAV
  - 0050 dividend-adjusted total return rebuilt through prices.py

Outputs:
  - interactive Plotly dashboard
  - static PNG overview
  - metrics CSV
  - aligned daily NAV/return/drawdown CSV
"""
from __future__ import annotations

import html
import math
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots
from quantlib import paths

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from quantlib.db import connect  # noqa: E402
from quantlib.prices import total_return_series  # noqa: E402


CAPITAL = 1_000_000.0
RF = 0.01
TDPY = 252
START = date(2005, 1, 3)
END = date(2026, 5, 8)
RECENT_1Y_START = date(2025, 5, 8)
RESULTS = Path(f"{paths.OUT_STRAT_LAB}")


@dataclass(frozen=True)
class SeriesSpec:
    key: str
    label: str
    source: str
    color: str
    dash: str = "solid"


SERIES = [
    SeriesSpec(
        key="iter67_full_switch",
        label="Iter67 NAV-Lineage Research Benchmark",
        source=str(
            RESULTS
            / (
                "iter67_core63_sharpe_iter64_no_overlay_gate_tech_rs_mom21_abs21_monthly_lb42_m-5"
                "_hold60_confirm2_w100_daily.csv"
            )
        ),
        color="#C2410C",
    ),
    SeriesSpec(
        key="iter69_hard_cap5_target_book",
        label="Hard-Cap 5 Diagnostic Target-Book",
        source=str(RESULTS / "iter_69_production_audit_hard_cap5_target_book_daily.csv"),
        color="#2563EB",
    ),
    SeriesSpec(
        key="iter72_cap5_source_reconciled",
        label="Cap 5 Cash Source-Reconciled Attribution",
        source=str(RESULTS / "iter_72_source_reconciled_cap_attribution_cap5_daily.csv"),
        color="#16A34A",
    ),
    SeriesSpec(
        key="0050",
        label="0050 Total Return",
        source="prices.py total_return_series adjusted benchmark",
        color="#6B7280",
        dash="dash",
    ),
]
STRATEGY_SERIES = [spec for spec in SERIES if spec.key != "0050"]


def load_strategy_nav(path: str, col: str) -> pl.DataFrame:
    if not Path(path).exists():
        raise FileNotFoundError(path)
    return (
        pl.read_csv(path, try_parse_dates=True)
        .sort("date")
        .filter((pl.col("date") >= START) & (pl.col("date") <= END))
        .select(["date", pl.col("nav").cast(pl.Float64).alias(col)])
    )


def load_0050_nav() -> pl.DataFrame:
    con = connect(read_only=True)
    try:
        px = total_return_series(
            con,
            "0050",
            START.isoformat(),
            END.isoformat(),
            market="twse",
        ).sort("date")
    finally:
        con.close()

    if px.height == 0:
        raise RuntimeError("0050 benchmark returned no rows")
    nav = CAPITAL * (px["adj_close"].to_numpy() / px["adj_close"][0])
    return pl.DataFrame({"date": px["date"].to_list(), "0050": nav})


def load_aligned_nav() -> pl.DataFrame:
    wide = load_strategy_nav(SERIES[0].source, SERIES[0].key)
    for spec in STRATEGY_SERIES[1:]:
        wide = wide.join(load_strategy_nav(spec.source, spec.key), on="date", how="inner")
    wide = wide.join(load_0050_nav(), on="date", how="inner").sort("date")
    if wide.height < 2:
        raise RuntimeError("not enough overlapping rows for comparison")

    for spec in SERIES:
        wide = wide.with_columns((pl.col(spec.key) / pl.col(spec.key).first() * CAPITAL).alias(spec.key))
    return wide


def daily_returns(nav: np.ndarray) -> np.ndarray:
    out = np.zeros_like(nav, dtype=float)
    out[1:] = nav[1:] / nav[:-1] - 1.0
    return out


def drawdown(nav: np.ndarray) -> np.ndarray:
    peak = np.maximum.accumulate(nav)
    return nav / peak - 1.0


def rolling_return(nav: np.ndarray, window: int = TDPY) -> np.ndarray:
    out = np.full_like(nav, np.nan, dtype=float)
    out[window:] = nav[window:] / nav[:-window] - 1.0
    return out


def metrics(nav: np.ndarray, dates: list[date], bench_ret: np.ndarray | None = None) -> dict[str, float | str]:
    rets = daily_returns(nav)
    years = max((dates[-1] - dates[0]).days / 365.25, len(rets) / TDPY, 1e-9)
    total = nav[-1] / nav[0] - 1.0
    cagr = (nav[-1] / nav[0]) ** (1.0 / years) - 1.0
    vol = float(np.std(rets[1:], ddof=1) * math.sqrt(TDPY)) if len(rets) > 2 else 0.0
    downside = rets[rets < 0]
    downvol = float(np.std(downside, ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 0.0
    dd = drawdown(nav)
    mdd = float(np.min(dd))
    best_day = float(np.max(rets[1:])) if len(rets) > 1 else 0.0
    worst_day = float(np.min(rets[1:])) if len(rets) > 1 else 0.0
    pos_day = float(np.mean(rets[1:] > 0)) if len(rets) > 1 else 0.0

    beta = np.nan
    alpha = np.nan
    ir = np.nan
    if bench_ret is not None:
        aligned_rets = rets[1:]
        aligned_bench = bench_ret[1:]
        var_bench = float(np.var(aligned_bench, ddof=1))
        if var_bench > 0:
            beta = float(np.cov(aligned_rets, aligned_bench, ddof=1)[0, 1] / var_bench)
            alpha = float(np.mean(aligned_rets - beta * aligned_bench) * TDPY)
        excess = aligned_rets - aligned_bench
        excess_std = float(np.std(excess, ddof=1))
        if excess_std > 0:
            ir = float(np.mean(excess) / excess_std * math.sqrt(TDPY))

    return {
        "start": dates[0].isoformat(),
        "end": dates[-1].isoformat(),
        "trading_days": float(len(dates)),
        "final_nav": float(nav[-1]),
        "final_multiple": float(nav[-1] / nav[0]),
        "total_return": float(total),
        "cagr": float(cagr),
        "sortino": float((cagr - RF) / downvol) if downvol > 0 else np.nan,
        "sharpe": float((cagr - RF) / vol) if vol > 0 else np.nan,
        "volatility": vol,
        "mdd": mdd,
        "calmar": float(cagr / abs(mdd)) if mdd < 0 else np.nan,
        "best_day": best_day,
        "worst_day": worst_day,
        "positive_day_rate": pos_day,
        "mdd_date": dates[int(np.argmin(dd))].isoformat(),
        "beta_vs_0050": beta,
        "annual_alpha_vs_0050": alpha,
        "ir_vs_0050": ir,
    }


def window_metrics(df: pl.DataFrame, start_year: int, end_year: int) -> dict[str, dict[str, float | str]]:
    sub = df.filter((pl.col("date").dt.year() >= start_year) & (pl.col("date").dt.year() <= end_year))
    dates = sub["date"].to_list()
    bench_ret = daily_returns(sub["0050"].to_numpy())
    return {spec.key: metrics(sub[spec.key].to_numpy(), dates, bench_ret) for spec in SERIES}


def date_window_metrics(df: pl.DataFrame, start: date, end: date) -> dict[str, dict[str, float | str]]:
    sub = df.filter((pl.col("date") >= start) & (pl.col("date") <= end))
    dates = sub["date"].to_list()
    if len(dates) < 2:
        raise RuntimeError(f"not enough rows for window {start} to {end}")
    bench_ret = daily_returns(sub["0050"].to_numpy())
    return {spec.key: metrics(sub[spec.key].to_numpy(), dates, bench_ret) for spec in SERIES}


def build_metrics(df: pl.DataFrame) -> pl.DataFrame:
    dates = df["date"].to_list()
    bench_ret = daily_returns(df["0050"].to_numpy())
    full = {spec.key: metrics(df[spec.key].to_numpy(), dates, bench_ret) for spec in SERIES}
    oos = window_metrics(df, 2010, 2025)
    recent_1y = date_window_metrics(df, RECENT_1Y_START, END)

    rows = []
    for spec in SERIES:
        row = {
            "name": spec.label,
            "source": spec.source,
            **{f"full_{k}": v for k, v in full[spec.key].items()},
            **{f"oos_{k}": v for k, v in oos[spec.key].items()},
            **{f"recent_1y_{k}": v for k, v in recent_1y[spec.key].items()},
        }
        rows.append(row)
    return pl.DataFrame(rows)


def build_timeseries(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    for spec in SERIES:
        nav = out[spec.key].to_numpy()
        out = out.with_columns(
            [
                pl.Series(f"{spec.key}_ret", daily_returns(nav)),
                pl.Series(f"{spec.key}_drawdown", drawdown(nav)),
                pl.Series(f"{spec.key}_rolling_1y_return", rolling_return(nav)),
            ]
        )
    out = out.with_columns(
        [
            (pl.col(f"{spec.key}_rolling_1y_return") - pl.col("0050_rolling_1y_return")).alias(
                f"{spec.key}_rolling_1y_excess_vs_0050"
            )
            for spec in STRATEGY_SERIES
        ]
    )
    return out


def annual_returns(df: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for year in sorted(set(df["date"].dt.year().to_list())):
        sub = df.filter(pl.col("date").dt.year() == year)
        if sub.height < 2:
            continue
        row: dict[str, float | int] = {"year": int(year)}
        for spec in SERIES:
            row[spec.key] = float(sub[spec.key][-1] / sub[spec.key][0] - 1.0)
        rows.append(row)
    return pl.DataFrame(rows)


def fmt_pct(x: float | None, digits: int = 1) -> str:
    if x is None or not np.isfinite(float(x)):
        return "-"
    return f"{float(x) * 100:.{digits}f}%"


def fmt_float(x: float | None, digits: int = 2) -> str:
    if x is None or not np.isfinite(float(x)):
        return "-"
    return f"{float(x):.{digits}f}"


def fmt_money(x: float) -> str:
    return f"{x / 1_000_000:.1f}M"


def metric_cards(metrics_df: pl.DataFrame) -> str:
    cards = []
    for row in metrics_df.iter_rows(named=True):
        cards.append(
            f"""
            <div class="metric-card">
              <div class="metric-name">{html.escape(str(row["name"]))}</div>
              <div class="metric-main">{fmt_money(float(row["full_final_nav"]))}</div>
              <div class="metric-sub">Full CAGR {fmt_pct(float(row["full_cagr"]))} · Sortino {fmt_float(float(row["full_sortino"]))} · MDD {fmt_pct(float(row["full_mdd"]))}</div>
              <div class="metric-sub">OOS CAGR {fmt_pct(float(row["oos_cagr"]))} · OOS Sortino {fmt_float(float(row["oos_sortino"]))} · OOS MDD {fmt_pct(float(row["oos_mdd"]))}</div>
              <div class="metric-sub">Recent 1Y CAGR {fmt_pct(float(row["recent_1y_cagr"]))} · {html.escape(str(row["recent_1y_start"]))} to {html.escape(str(row["recent_1y_end"]))}</div>
            </div>
            """
        )
    return "\n".join(cards)


def metrics_table(metrics_df: pl.DataFrame) -> go.Figure:
    cols = [
        ("name", "Asset / Strategy"),
        ("full_final_nav", "Final NAV"),
        ("full_cagr", "Full CAGR"),
        ("full_sortino", "Full Sortino"),
        ("full_mdd", "Full MDD"),
        ("oos_cagr", "OOS CAGR"),
        ("recent_1y_cagr", "Recent 1Y CAGR"),
        ("oos_sortino", "OOS Sortino"),
        ("oos_mdd", "OOS MDD"),
        ("full_annual_alpha_vs_0050", "Alpha vs 0050"),
        ("full_ir_vs_0050", "IR vs 0050"),
    ]
    values = []
    for col, _ in cols:
        col_values = []
        for row in metrics_df.iter_rows(named=True):
            value = row[col]
            if col == "full_final_nav":
                col_values.append(fmt_money(float(value)))
            elif col in {"full_cagr", "full_mdd", "oos_cagr", "recent_1y_cagr", "oos_mdd", "full_annual_alpha_vs_0050"}:
                col_values.append(fmt_pct(float(value)))
            elif isinstance(value, float):
                col_values.append(fmt_float(value))
            else:
                col_values.append(str(value))
        values.append(col_values)

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=[label for _, label in cols],
                    fill_color="#102A43",
                    font=dict(color="white", size=13),
                    align="left",
                    height=30,
                ),
                cells=dict(
                    values=values,
                    fill_color=[["#F7FAFC", "#FFFFFF", "#F7FAFC"]],
                    font=dict(color="#102A43", size=12),
                    align="left",
                    height=28,
                ),
            )
        ]
    )
    fig.update_layout(height=230, margin=dict(l=0, r=0, t=5, b=5))
    return fig


def nav_figure(df: pl.DataFrame, metrics_df: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    dates = df["date"].to_list()
    for spec in SERIES:
        nav = df[spec.key].to_numpy()
        final = metrics_df.filter(pl.col("name") == spec.label)["full_final_nav"][0]
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=nav,
                mode="lines",
                name=f"{spec.label} ({fmt_money(final)})",
                line=dict(color=spec.color, width=3, dash=spec.dash),
                hovertemplate="%{x|%Y-%m-%d}<br>NAV=%{y:,.0f}<extra></extra>",
            )
        )
    fig.update_layout(
        title="Cumulative Total-Return NAV (rebased to NT$1M)",
        template="plotly_white",
        hovermode="x unified",
        height=560,
        yaxis=dict(type="log", title="NAV, log scale"),
        xaxis=dict(title="Date"),
        legend=dict(orientation="h", y=1.08, x=0),
        margin=dict(l=55, r=25, t=80, b=50),
    )
    return fig


def drawdown_figure(ts: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    dates = ts["date"].to_list()
    for spec in SERIES:
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=ts[f"{spec.key}_drawdown"].to_numpy() * 100,
                mode="lines",
                name=spec.label,
                line=dict(color=spec.color, width=2.5, dash=spec.dash),
                hovertemplate="%{x|%Y-%m-%d}<br>Drawdown=%{y:.1f}%<extra></extra>",
            )
        )
    fig.update_layout(
        title="Underwater / Drawdown",
        template="plotly_white",
        hovermode="x unified",
        height=430,
        yaxis=dict(title="Drawdown (%)", ticksuffix="%"),
        xaxis=dict(title="Date"),
        legend=dict(orientation="h", y=1.12, x=0),
        margin=dict(l=55, r=25, t=70, b=50),
    )
    return fig


def rolling_figure(ts: pl.DataFrame) -> go.Figure:
    dates = ts["date"].to_list()
    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("Rolling 1Y Return", "Rolling 1Y Excess Return vs 0050"),
        horizontal_spacing=0.08,
    )
    for spec in SERIES:
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=ts[f"{spec.key}_rolling_1y_return"].to_numpy() * 100,
                mode="lines",
                name=spec.label,
                line=dict(color=spec.color, width=2.2, dash=spec.dash),
                hovertemplate="%{x|%Y-%m-%d}<br>Rolling 1Y=%{y:.1f}%<extra></extra>",
            ),
            row=1,
            col=1,
        )
    for spec in STRATEGY_SERIES:
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=ts[f"{spec.key}_rolling_1y_excess_vs_0050"].to_numpy() * 100,
                mode="lines",
                name=f"{spec.label} excess",
                line=dict(color=spec.color, width=2.2),
                hovertemplate="%{x|%Y-%m-%d}<br>Excess=%{y:.1f}%<extra></extra>",
                showlegend=False,
            ),
            row=1,
            col=2,
        )
    fig.add_hline(y=0, line_width=1, line_dash="dot", line_color="#6B7280", row=1, col=2)
    fig.update_layout(
        template="plotly_white",
        hovermode="x unified",
        height=440,
        legend=dict(orientation="h", y=1.18, x=0),
        margin=dict(l=55, r=25, t=85, b=50),
    )
    fig.update_yaxes(title_text="Return (%)", ticksuffix="%", row=1, col=1)
    fig.update_yaxes(title_text="Excess (%)", ticksuffix="%", row=1, col=2)
    fig.update_xaxes(title_text="Date")
    return fig


def yearly_figure(annual: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    years = annual["year"].to_list()
    for spec in SERIES:
        fig.add_trace(
            go.Bar(
                x=years,
                y=annual[spec.key].to_numpy() * 100,
                name=spec.label,
                marker_color=spec.color,
                hovertemplate="Year=%{x}<br>Return=%{y:.1f}%<extra></extra>",
            )
        )
    fig.update_layout(
        title="Calendar-Year Returns",
        template="plotly_white",
        height=500,
        barmode="group",
        yaxis=dict(title="Return (%)", ticksuffix="%"),
        xaxis=dict(title="Year", dtick=1),
        legend=dict(orientation="h", y=1.12, x=0),
        margin=dict(l=55, r=25, t=80, b=70),
    )
    return fig


def scatter_figure(metrics_df: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    for spec in SERIES:
        row = metrics_df.filter(pl.col("name") == spec.label).to_dicts()[0]
        fig.add_trace(
            go.Scatter(
                x=[float(row["full_mdd"]) * 100],
                y=[float(row["full_cagr"]) * 100],
                mode="markers+text",
                text=[spec.label],
                textposition="top center",
                marker=dict(
                    color=spec.color,
                    size=max(18, min(54, float(row["full_final_multiple"]) * 0.7)),
                    line=dict(color="white", width=2),
                ),
                name=spec.label,
                hovertemplate=(
                    "%{text}<br>CAGR=%{y:.1f}%<br>MDD=%{x:.1f}%"
                    f"<br>Sortino={fmt_float(float(row['full_sortino']))}<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        title="Risk / Return Map",
        template="plotly_white",
        height=420,
        xaxis=dict(title="Max Drawdown (%)"),
        yaxis=dict(title="CAGR (%)"),
        showlegend=False,
        margin=dict(l=55, r=25, t=70, b=50),
    )
    return fig


def write_dashboard(df: pl.DataFrame, ts: pl.DataFrame, metrics_df: pl.DataFrame, annual: pl.DataFrame) -> Path:
    out = RESULTS / "strategy_candidates_vs_0050_dashboard.html"
    figs = [
        nav_figure(df, metrics_df),
        metrics_table(metrics_df),
        drawdown_figure(ts),
        rolling_figure(ts),
        yearly_figure(annual),
        scatter_figure(metrics_df),
    ]
    fragments = []
    for i, fig in enumerate(figs):
        fragments.append(fig.to_html(full_html=False, include_plotlyjs=True if i == 0 else False))

    html_doc = f"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Strategy Candidates vs 0050</title>
  <style>
    :root {{
      --ink: #102A43;
      --muted: #627D98;
      --line: #D9E2EC;
      --panel: #FFFFFF;
      --bg: #F5F7FA;
    }}
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans TC", "PingFang TC", sans-serif;
      color: var(--ink);
      background: var(--bg);
    }}
    .wrap {{
      max-width: 1380px;
      margin: 0 auto;
      padding: 32px 28px 48px;
    }}
    .hero {{
      margin-bottom: 22px;
    }}
    .eyebrow {{
      color: var(--muted);
      font-size: 13px;
      letter-spacing: 0;
      text-transform: uppercase;
      font-weight: 700;
    }}
    h1 {{
      margin: 8px 0 10px;
      font-size: 34px;
      line-height: 1.15;
    }}
    .note {{
      color: var(--muted);
      font-size: 15px;
      max-width: 1120px;
      line-height: 1.55;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 14px;
      margin: 22px 0;
    }}
    .metric-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px 18px;
      box-shadow: 0 1px 2px rgba(16, 42, 67, 0.05);
    }}
    .metric-name {{
      font-size: 14px;
      font-weight: 700;
      color: var(--muted);
      margin-bottom: 8px;
    }}
    .metric-main {{
      font-size: 30px;
      line-height: 1.1;
      font-weight: 800;
      margin-bottom: 8px;
    }}
    .metric-sub {{
      font-size: 13px;
      color: var(--muted);
      line-height: 1.45;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px 16px 4px;
      margin-bottom: 18px;
      box-shadow: 0 1px 2px rgba(16, 42, 67, 0.05);
    }}
    .footnote {{
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
      margin-top: 18px;
    }}
    @media (max-width: 900px) {{
      .cards {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 26px; }}
      .wrap {{ padding: 22px 14px 36px; }}
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <div class="eyebrow">Quantlib strategy visualization</div>
      <h1>Strategy candidates vs 0050 total-return benchmark</h1>
      <div class="note">
        資料區間：{df["date"][0]} 至 {df["date"][-1]}。所有曲線都重設為 NT$1,000,000 起始 NAV。
        0050 由 <code>src/quantlib/prices.py::total_return_series()</code> 重建還原報酬，避免使用只到 2026-04-24 且與目前 canonical 口徑不一致的舊 benchmark CSV。
        目前沒有策略通過 hard-cap target-book production gate；本圖保留 Iter67 NAV-lineage 研究基準、hard-cap 5 diagnostic target-book、以及 cap 5 cash source-reconciled attribution，方便直接檢查不同口徑的績效落差。
      </div>
    </section>
    <section class="cards">{metric_cards(metrics_df)}</section>
    {"".join(f'<section class="panel">{fragment}</section>' for fragment in fragments)}
    <div class="footnote">
      計算口徑：CAGR 使用日曆年化；Sortino/Sharpe 使用 RF=1%、252 交易日年化波動；
      Alpha vs 0050 使用每日 beta-adjusted alpha 線性年化；IR 使用每日超額報酬年化。
      本圖為研究與風控比較，不代表未來報酬保證。
    </div>
  </main>
</body>
</html>
"""
    out.write_text(html_doc, encoding="utf-8")
    return out


def write_static_png(df: pl.DataFrame, ts: pl.DataFrame, metrics_df: pl.DataFrame, annual: pl.DataFrame) -> Path:
    out = RESULTS / "strategy_candidates_vs_0050_overview.png"
    plt.style.use("seaborn-v0_8-whitegrid")
    fig = plt.figure(figsize=(18, 13.5))
    grid = fig.add_gridspec(3, 2, height_ratios=[1.0, 1.0, 0.36], hspace=0.32, wspace=0.08)
    axs = np.array(
        [
            [fig.add_subplot(grid[0, 0]), fig.add_subplot(grid[0, 1])],
            [fig.add_subplot(grid[1, 0]), fig.add_subplot(grid[1, 1])],
        ]
    )
    summary_ax = fig.add_subplot(grid[2, :])
    summary_ax.axis("off")
    dates = df["date"].to_list()

    ax = axs[0, 0]
    for spec in SERIES:
        ax.plot(dates, df[spec.key].to_numpy(), color=spec.color, linewidth=2.2, linestyle="--" if spec.dash == "dash" else "-", label=spec.label)
    ax.set_title("Cumulative NAV, rebased to NT$1M")
    ax.set_ylabel("NAV (log scale)")
    ax.set_yscale("log")
    ax.legend(loc="upper left", fontsize=9)

    ax = axs[0, 1]
    for spec in SERIES:
        ax.plot(dates, ts[f"{spec.key}_drawdown"].to_numpy() * 100, color=spec.color, linewidth=2.0, linestyle="--" if spec.dash == "dash" else "-", label=spec.label)
    ax.set_title("Drawdown")
    ax.set_ylabel("Drawdown (%)")
    ax.legend(loc="lower left", fontsize=9)

    ax = axs[1, 0]
    for spec in SERIES:
        ax.plot(dates, ts[f"{spec.key}_rolling_1y_return"].to_numpy() * 100, color=spec.color, linewidth=2.0, linestyle="--" if spec.dash == "dash" else "-", label=spec.label)
    ax.axhline(0, color="#6B7280", linestyle=":", linewidth=1)
    ax.set_title("Rolling 1Y Return")
    ax.set_ylabel("Return (%)")
    ax.legend(loc="upper left", fontsize=9)

    ax = axs[1, 1]
    years = np.array(annual["year"].to_list())
    width = min(0.36, 0.8 / len(SERIES))
    offsets = (np.arange(len(SERIES)) - (len(SERIES) - 1) / 2) * width
    for spec, offset in zip(SERIES, offsets, strict=True):
        ax.bar(years + offset, annual[spec.key].to_numpy() * 100, width=width, color=spec.color, label=spec.label)
    ax.axhline(0, color="#6B7280", linewidth=1)
    ax.set_title("Calendar-Year Returns")
    ax.set_ylabel("Return (%)")
    ax.set_xticks(years)
    ax.tick_params(axis="x", labelrotation=45)
    ax.legend(loc="upper left", fontsize=9)

    summary_lines = []
    for row in metrics_df.iter_rows(named=True):
        summary_lines.append(
            f"{row['name']}: Final {fmt_money(float(row['full_final_nav']))}, "
            f"CAGR {fmt_pct(float(row['full_cagr']))}, "
            f"Sortino {fmt_float(float(row['full_sortino']))}, "
            f"MDD {fmt_pct(float(row['full_mdd']))}"
        )
    fig.suptitle("Strategy Candidates vs 0050 Total Return (2005-01-03 to 2026-05-08)", fontsize=18, fontweight="bold", y=0.985)
    summary_ax.text(0.0, 0.96, "\n".join(summary_lines), fontsize=10.5, color="#102A43", va="top")
    summary_ax.text(
        0.0,
        0.08,
        "0050 is split-adjusted through prices.py; all series are rebased to NT$1M. "
        "No strategy is currently promoted as hard-cap target-book production champion.",
        fontsize=10,
        color="#627D98",
        va="bottom",
    )
    fig.savefig(out, dpi=180)
    plt.close(fig)
    return out


def main() -> None:
    df = load_aligned_nav()
    ts = build_timeseries(df)
    metrics_df = build_metrics(df)
    annual = annual_returns(df)

    dashboard_path = write_dashboard(df, ts, metrics_df, annual)
    png_path = write_static_png(df, ts, metrics_df, annual)
    metrics_path = RESULTS / "strategy_candidates_vs_0050_metrics.csv"
    ts_path = RESULTS / "strategy_candidates_vs_0050_timeseries.csv"
    metrics_df.write_csv(metrics_path)
    ts.write_csv(ts_path)
    legacy_paths = [
        (dashboard_path, RESULTS / "usable_strategy_vs_0050_dashboard.html"),
        (png_path, RESULTS / "usable_strategy_vs_0050_overview.png"),
        (metrics_path, RESULTS / "usable_strategy_vs_0050_metrics.csv"),
        (ts_path, RESULTS / "usable_strategy_vs_0050_timeseries.csv"),
        (dashboard_path, RESULTS / "codex_strategies_vs_0050_dashboard.html"),
        (png_path, RESULTS / "codex_strategies_vs_0050_overview.png"),
        (metrics_path, RESULTS / "codex_strategies_vs_0050_metrics.csv"),
        (ts_path, RESULTS / "codex_strategies_vs_0050_timeseries.csv"),
    ]
    for src, dst in legacy_paths:
        shutil.copyfile(src, dst)

    print("=" * 110)
    print("Strategy candidate comparison vs 0050")
    print("=" * 110)
    print(
        metrics_df.select(
            [
                "name",
                pl.col("full_final_nav").truediv(1_000_000).round(2).alias("final_nav_m"),
                pl.col("full_cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("full_sortino").round(3).alias("full_sortino"),
                pl.col("full_mdd").mul(100).round(2).alias("full_mdd_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("oos_sortino").round(3).alias("oos_sortino"),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("full_annual_alpha_vs_0050").mul(100).round(2).alias("alpha_vs_0050_pct"),
                pl.col("full_ir_vs_0050").round(3).alias("ir_vs_0050"),
            ]
        )
    )
    print(f"\nHTML: {dashboard_path}")
    print(f"PNG:  {png_path}")
    print(f"CSV:  {metrics_path}")
    print(f"TS:   {ts_path}")
    print("Legacy aliases refreshed: usable_strategy_vs_0050_* and codex_strategies_vs_0050_*")


if __name__ == "__main__":
    main()
