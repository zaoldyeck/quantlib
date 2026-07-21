"""Generate HTML review report for Iter100 overlay bounded sweep."""

from __future__ import annotations

import html
import sys
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from constants import CAPITAL  # noqa: E402
from iter_96_robust_alpha_research import load_benchmark_nav  # noqa: E402


RESULTS = REPO_ROOT / "research/strat_lab/results"
OUT_DIR = REPO_ROOT / "docs/strategy_research"
OUT_HTML = OUT_DIR / "iter100_ict_smc_tpo_overlay.html"
PREFIX = "iter_100_ict_smc_tpo_overlay"


def pct(value: object, digits: int = 2) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "-"
    if not np.isfinite(v):
        return "-"
    return f"{v * 100:.{digits}f}%"


def num(value: object, digits: int = 3) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "-"
    if not np.isfinite(v):
        return "-"
    return f"{v:.{digits}f}"


def drawdown(daily: pl.DataFrame) -> pl.DataFrame:
    ordered = daily.select(["date", "nav"]).sort("date")
    nav = ordered["nav"].to_numpy().astype(float)
    peak = np.maximum.accumulate(nav)
    dd = nav / np.maximum(peak, 1e-12) - 1.0
    return ordered.with_columns(pl.Series("drawdown", dd))


def load_daily(name: str) -> pl.DataFrame:
    return pl.read_csv(RESULTS / f"{PREFIX}_{name}_daily.csv", try_parse_dates=True).select(["date", "nav"]).sort("date")


def normalize(daily: pl.DataFrame) -> pl.DataFrame:
    base = float(daily["nav"][0])
    return daily.with_columns((pl.col("nav") / base * CAPITAL).alias("nav"))


def table_html(rows: list[dict[str, object]]) -> str:
    cols = [
        ("name", "策略"),
        ("cagr", "Full CAGR"),
        ("oos_cagr", "OOS CAGR"),
        ("recent_1y_cagr", "最近一年 CAGR"),
        ("mdd", "MDD"),
        ("oos_mdd", "OOS MDD"),
        ("dsr", "DSR"),
        ("pbo", "PBO"),
        ("trade_below_cost_mae_p95", "Trade below-cost MAE P95"),
        ("portfolio_below_cost_ulcer", "Portfolio below-cost ulcer"),
        ("iter100_cost_below_objective", "Iter100 objective"),
    ]
    head = "".join(f"<th>{label}</th>" for _key, label in cols)
    body = []
    for row in rows:
        cells = []
        for key, _label in cols:
            value = row.get(key, "")
            if key in {"cagr", "oos_cagr", "recent_1y_cagr", "mdd", "oos_mdd", "trade_below_cost_mae_p95", "portfolio_below_cost_ulcer"}:
                cells.append(f"<td>{pct(value)}</td>")
            elif key in {"dsr", "pbo", "iter100_cost_below_objective"}:
                cells.append(f"<td>{num(value)}</td>")
            else:
                cells.append(f"<td>{html.escape(str(value))}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = pl.read_csv(RESULTS / f"{PREFIX}_summary.csv").sort("iter100_cost_below_objective", descending=True)
    top_names = ["baseline_iter95_targets", "exit_structure_break", "entry_mss_fvg_or_sweep_score_1p5"]
    nav_series = {name: load_daily(name) for name in top_names}
    start = max(frame["date"][0] for frame in nav_series.values())
    end = min(frame["date"][-1] for frame in nav_series.values())
    b0050 = normalize(load_benchmark_nav("0050", start, end, "0050 TR").select(["date", "nav"]).sort("date"))
    b2330 = normalize(load_benchmark_nav("2330", start, end, "2330 TR").select(["date", "nav"]).sort("date"))
    nav_series["0050 TR"] = b0050
    nav_series["2330 TR"] = b2330

    nav_fig = go.Figure()
    for name, frame in nav_series.items():
        f = normalize(frame.filter((pl.col("date") >= start) & (pl.col("date") <= end)))
        nav_fig.add_trace(go.Scatter(x=f["date"], y=f["nav"], mode="lines", name=name))
    nav_fig.update_layout(title="NAV / P&L Curve", yaxis_title="NAV normalized to 1,000,000", template="plotly_white")

    dd_fig = go.Figure()
    for name, frame in nav_series.items():
        f = normalize(frame.filter((pl.col("date") >= start) & (pl.col("date") <= end)))
        dd = drawdown(f)
        dd_fig.add_trace(go.Scatter(x=dd["date"], y=dd["drawdown"], mode="lines", name=name))
    dd_fig.update_layout(title="Drawdown", yaxis_tickformat=".0%", template="plotly_white")

    metric_rows = summary.filter(pl.col("name").is_in(top_names)).sort("oos_cagr", descending=True)
    bar = make_subplots(rows=1, cols=2, subplot_titles=("Growth Metrics", "Below-Cost Risk"))
    bar.add_trace(go.Bar(x=metric_rows["name"], y=metric_rows["oos_cagr"], name="OOS CAGR"), row=1, col=1)
    bar.add_trace(go.Bar(x=metric_rows["name"], y=metric_rows["recent_1y_cagr"], name="Recent 1Y CAGR"), row=1, col=1)
    bar.add_trace(go.Bar(x=metric_rows["name"], y=metric_rows["trade_below_cost_mae_p95"], name="Trade MAE P95"), row=1, col=2)
    bar.add_trace(go.Bar(x=metric_rows["name"], y=metric_rows["portfolio_below_cost_ulcer"], name="Portfolio below-cost ulcer"), row=1, col=2)
    bar.update_layout(template="plotly_white", barmode="group")
    bar.update_yaxes(tickformat=".0%")

    rows = summary.head(10).to_dicts()
    generated = pl.datetime_range(start, end, interval="1d", eager=True).len()
    html_out = f"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <title>Iter100 ICT/SMC/TPO Overlay Research</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Noto Sans TC", sans-serif; margin: 32px; color: #1f2328; }}
    h1, h2 {{ margin-bottom: 8px; }}
    .note {{ color: #57606a; line-height: 1.6; max-width: 1080px; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-top: 12px; }}
    th, td {{ border-bottom: 1px solid #d8dee4; padding: 8px 10px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #f6f8fa; }}
  </style>
</head>
<body>
  <h1>Iter100 ICT/SMC/TPO Overlay Research</h1>
  <p class="note">
    本報告使用 Iter95 champion target book 作為核心，測試 ICT2022 / SMC / TPO daily proxy / 型態 overlay。
    資料窗口為 {start} 至 {end}；本輪結果顯示沒有 overlay 同時提升 Iter95 的 full/OOS/最近一年 CAGR。
    `exit_structure_break` 在 cost-below objective 上較高，但犧牲太多 CAGR，因此不可升級為更強策略。
    圖表資料列數約 {generated:,} 個 calendar days 對齊窗口。
  </p>
  <h2>NAV 比較</h2>
  {nav_fig.to_html(include_plotlyjs="cdn", full_html=False)}
  <h2>Drawdown 比較</h2>
  {dd_fig.to_html(include_plotlyjs=False, full_html=False)}
  <h2>成長與成本線以下風險</h2>
  {bar.to_html(include_plotlyjs=False, full_html=False)}
  <h2>排名表</h2>
  {table_html(rows)}
</body>
</html>
"""
    OUT_HTML.write_text(html_out)
    print(OUT_HTML)


if __name__ == "__main__":
    main()
