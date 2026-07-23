"""Generate charted robust-alpha campaign report for Iter95/96/97."""

from __future__ import annotations

import html
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import polars as pl
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_ROOT = REPO_ROOT / "src" / "quantlib"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from quantlib.constants import CAPITAL  # noqa: E402
from quantlib.db import connect  # noqa: E402
from evaluation import drawdown_series, nav_metrics  # noqa: E402
from iter_96_robust_alpha_research import load_benchmark_nav, relative_metrics  # noqa: E402
from quantlib.prices import fetch_adjusted_panel  # noqa: E402
from validator import recent_one_year_metrics, validate_daily_nav  # noqa: E402


RESULTS = REPO_ROOT / f"{paths.OUT_STRAT_LAB}"
OUT_DIR = REPO_ROOT / "docs/strategy_research/robust_alpha_campaign"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_daily(path: Path, label: str) -> pl.DataFrame:
    return pl.read_csv(path, try_parse_dates=True).select(["date", "nav"]).sort("date").with_columns(
        pl.lit(label).alias("series")
    )


def best_row(summary_path: Path) -> dict[str, object]:
    return pl.read_csv(summary_path, try_parse_dates=True, infer_schema_length=10000).sort(
        "robust_alpha_objective", descending=True
    ).row(0, named=True)


def strategy_daily_path(prefix: str, row: dict[str, object]) -> Path:
    key = f"{row['name']}__{row['exit_config']}".replace("/", "_")
    return RESULTS / f"{prefix}_{key}_daily.csv"


def benchmark_nav(code: str, start, end, label: str) -> pl.DataFrame:
    frame = load_benchmark_nav(code, start, end, label)
    return frame.select(["date", "nav"]).with_columns(pl.lit(label).alias("series"))


def single_stock_nav(code: str, start, end, label: str) -> pl.DataFrame:
    con = connect(read_only=True)
    try:
        panel = fetch_adjusted_panel(
            con,
            start.isoformat(),
            end.isoformat(),
            codes=[code],
            market="twse",
            include_extra_history_days=20,
        ).sort("date")
    finally:
        con.close()
    base = float(panel["close"][0])
    return panel.select(["date", (pl.col("close") / base * CAPITAL).alias("nav")]).with_columns(
        pl.lit(label).alias("series")
    )


def top_single_stocks(cache_path: Path, end) -> pl.DataFrame:
    frame = (
        pl.scan_parquet(cache_path)
        .select(["date", "company_code", "close", "is_etf", "adv60"])
        .filter(~pl.col("is_etf"))
        .sort(["company_code", "date"])
        .group_by("company_code")
        .agg(
            [
                pl.col("date").first().alias("start"),
                pl.col("close").first().alias("start_close"),
                pl.col("date").last().alias("end"),
                pl.col("close").last().alias("end_close"),
                pl.col("adv60").last().alias("adv60"),
                pl.len().alias("rows"),
            ]
        )
        .filter(
            (pl.col("start") <= pl.date(2005, 12, 31))
            & (pl.col("end") == pl.lit(end))
            & (pl.col("start_close") > 0)
            & (pl.col("end_close") > 0)
        )
        .with_columns(
            (
                (pl.col("end_close") / pl.col("start_close")).pow(
                    1.0 / ((pl.col("end") - pl.col("start")).dt.total_days() / 365.25)
                )
                - 1.0
            ).alias("cagr")
        )
        .sort("cagr", descending=True)
        .head(25)
        .collect()
    )
    return frame


def metric_row(label: str, daily: pl.DataFrame, n_trials: int | None = None) -> dict[str, object]:
    ordered = daily.select(["date", "nav"]).sort("date")
    if n_trials is None:
        base = nav_metrics(ordered, capital=CAPITAL)
        row = {
            "name": label,
            **base,
            **recent_one_year_metrics(ordered, capital=CAPITAL),
            "oos_cagr": None,
            "oos_sortino": None,
            "oos_mdd": None,
            "dsr": None,
            "pbo": None,
            "fill_ratio": None,
        }
    else:
        row = validate_daily_nav(label, ordered, n_trials=n_trials)
    row["name"] = label
    row["start"] = ordered["date"].min()
    row["end"] = ordered["date"].max()
    return row


def drawdown_frame(daily: pl.DataFrame) -> pd.DataFrame:
    ordered = daily.sort("date")
    dd = drawdown_series(ordered["nav"].to_numpy().astype(float), CAPITAL)
    return pd.DataFrame({"date": ordered["date"].to_list(), "drawdown": dd})


def normalize_frame(daily: pl.DataFrame) -> pd.DataFrame:
    ordered = daily.sort("date")
    nav = ordered["nav"].to_numpy().astype(float)
    return pd.DataFrame({"date": ordered["date"].to_list(), "nav": nav / nav[0]})


def rolling_cagr_frame(daily: pl.DataFrame, window: int) -> pd.DataFrame:
    ordered = daily.sort("date")
    pdf = pd.DataFrame({"date": ordered["date"].to_list(), "nav": ordered["nav"].to_numpy().astype(float)})
    pdf[f"rolling_{window}"] = (pdf["nav"] / pdf["nav"].shift(window)) ** (252.0 / window) - 1.0
    return pdf[["date", f"rolling_{window}"]].dropna()


def relative_frame(strategy: pl.DataFrame, benchmark: pl.DataFrame) -> pd.DataFrame:
    joined = (
        strategy.select(["date", pl.col("nav").alias("s")])
        .join(benchmark.select(["date", pl.col("nav").alias("b")]), on="date", how="inner")
        .sort("date")
    )
    s = joined["s"].to_numpy().astype(float)
    b = joined["b"].to_numpy().astype(float)
    return pd.DataFrame({"date": joined["date"].to_list(), "relative": (s / s[0]) / (b / b[0])})


def pct(value: object) -> str:
    if value is None:
        return ""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return ""
    if not np.isfinite(f):
        return ""
    return f"{f * 100:.2f}%"


def make_table(rows: list[dict[str, object]]) -> str:
    columns = [
        ("name", "策略 / Benchmark"),
        ("cagr", "Full CAGR"),
        ("oos_cagr", "OOS CAGR"),
        ("recent_1y_cagr", "最近 1Y CAGR"),
        ("oos_sortino", "OOS Sortino"),
        ("oos_mdd", "OOS MDD"),
        ("dsr", "DSR"),
        ("pbo", "PBO"),
        ("fill_ratio", "Fill Ratio"),
        ("b0050_final_relative_nav", "相對 0050 終值"),
        ("b2330_final_relative_nav", "相對 2330 終值"),
        ("b2330_rolling_3y_win_rate", "vs 2330 rolling 3Y 勝率"),
    ]
    out = ["<table><thead><tr>"]
    out += [f"<th>{html.escape(title)}</th>" for _key, title in columns]
    out.append("</tr></thead><tbody>")
    for row in rows:
        out.append("<tr>")
        for key, _title in columns:
            value = row.get(key)
            if key in {"cagr", "oos_cagr", "recent_1y_cagr", "oos_mdd", "fill_ratio", "b2330_rolling_3y_win_rate"}:
                text = pct(value)
            elif isinstance(value, float):
                text = f"{value:.3f}"
            else:
                text = "" if value is None else str(value)
            out.append(f"<td>{html.escape(text)}</td>")
        out.append("</tr>")
    out.append("</tbody></table>")
    return "\n".join(out)


def plot_nav(series: dict[str, pl.DataFrame]) -> str:
    fig = go.Figure()
    for label, daily in series.items():
        pdf = normalize_frame(daily)
        fig.add_trace(go.Scatter(x=pdf["date"], y=pdf["nav"], mode="lines", name=label))
    fig.update_layout(title="NAV / P&L Curve (normalized)", yaxis_type="log", height=560, template="plotly_white")
    return fig.to_html(full_html=False, include_plotlyjs=True)


def plot_drawdown(series: dict[str, pl.DataFrame]) -> str:
    fig = go.Figure()
    for label, daily in series.items():
        pdf = drawdown_frame(daily)
        fig.add_trace(go.Scatter(x=pdf["date"], y=pdf["drawdown"], mode="lines", name=label))
    fig.update_layout(title="Drawdown", yaxis_tickformat=".0%", height=460, template="plotly_white")
    return fig.to_html(full_html=False, include_plotlyjs=False)


def plot_relative(series: dict[str, pl.DataFrame], b0050: pl.DataFrame, b2330: pl.DataFrame) -> str:
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Relative NAV vs 0050", "Relative NAV vs 2330"))
    for label, daily in series.items():
        if label in {"0050 TR", "2330 TR"}:
            continue
        pdf50 = relative_frame(daily, b0050)
        pdf2330 = relative_frame(daily, b2330)
        fig.add_trace(go.Scatter(x=pdf50["date"], y=pdf50["relative"], mode="lines", name=label), row=1, col=1)
        fig.add_trace(go.Scatter(x=pdf2330["date"], y=pdf2330["relative"], mode="lines", name=label, showlegend=False), row=1, col=2)
    fig.update_layout(title="Benchmark-relative NAV", height=500, template="plotly_white")
    return fig.to_html(full_html=False, include_plotlyjs=False)


def plot_rolling(series: dict[str, pl.DataFrame]) -> str:
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Rolling 1Y CAGR", "Rolling 3Y CAGR"))
    for label, daily in series.items():
        r1 = rolling_cagr_frame(daily, 252)
        r3 = rolling_cagr_frame(daily, 756)
        fig.add_trace(go.Scatter(x=r1["date"], y=r1["rolling_252"], mode="lines", name=label), row=1, col=1)
        fig.add_trace(go.Scatter(x=r3["date"], y=r3["rolling_756"], mode="lines", name=label, showlegend=False), row=1, col=2)
    fig.update_layout(title="Rolling CAGR", yaxis_tickformat=".0%", yaxis2_tickformat=".0%", height=500, template="plotly_white")
    return fig.to_html(full_html=False, include_plotlyjs=False)


def main() -> None:
    iter95_path = RESULTS / "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_daily.csv"
    iter96_row = best_row(RESULTS / "iter_96_robust_alpha_research_summary.csv")
    iter97_row = best_row(RESULTS / "iter_97_regime_risk_overlay_research_summary.csv")
    iter96_path = strategy_daily_path("iter_96_robust_alpha_research", iter96_row)
    iter97_path = strategy_daily_path("iter_97_regime_risk_overlay_research", iter97_row)

    iter95 = load_daily(iter95_path, "Iter95 execution-ready champion")
    iter96 = load_daily(iter96_path, "Iter96 best first-principles")
    iter97 = load_daily(iter97_path, "Iter97 best regime overlay")
    start = iter95["date"].min()
    end = iter95["date"].max()
    b0050 = benchmark_nav("0050", start, end, "0050 TR")
    b2330 = benchmark_nav("2330", start, end, "2330 TR")
    b2383 = single_stock_nav("2383", start, end, "2383 TR hindsight top")

    series = {
        "Iter95 execution-ready champion": iter95,
        "Iter96 best first-principles": iter96,
        "Iter97 best regime overlay": iter97,
        "0050 TR": b0050,
        "2330 TR": b2330,
        "2383 TR hindsight top": b2383,
    }

    metric_rows = [
        metric_row("Iter95 execution-ready champion", iter95, n_trials=42_569),
        {**metric_row("Iter96 best first-principles", iter96, n_trials=42_569), **{k: iter96_row.get(k) for k in iter96_row}},
        {**metric_row("Iter97 best regime overlay", iter97, n_trials=42_569), **{k: iter97_row.get(k) for k in iter97_row}},
        metric_row("0050 TR", b0050),
        metric_row("2330 TR", b2330),
        metric_row("2383 TR hindsight top", b2383),
    ]
    for row, daily in [(metric_rows[0], iter95), (metric_rows[1], iter96), (metric_rows[2], iter97)]:
        for prefix, bench in [("b0050", b0050), ("b2330", b2330)]:
            row.update(relative_metrics(daily.select(["date", "nav"]), bench.select(["date", "nav"]), prefix))

    metrics = pl.DataFrame(metric_rows)
    metrics_path = OUT_DIR / "campaign_comparison_metrics.csv"
    metrics.write_csv(metrics_path)

    daily_nav = pl.concat(
        [
            frame.select(["date", "series", "nav"])
            for frame in series.values()
        ],
        how="vertical",
    )
    daily_path = OUT_DIR / "campaign_daily_nav.csv"
    daily_nav.write_csv(daily_path)

    cache_path = max((RESULTS / "cache").glob("iter33_features_2005-01-03_2026-05-22_*.parquet"), key=lambda p: p.stat().st_mtime)
    singles = top_single_stocks(cache_path, end)
    singles_path = OUT_DIR / "top_single_stock_hindsight.csv"
    singles.write_csv(singles_path)

    table = make_table(metric_rows)
    top_single_html = singles.with_columns((pl.col("cagr") * 100).round(2).alias("cagr_pct")).select(
        ["company_code", "start", "end", "cagr_pct", "start_close", "end_close", "adv60"]
    ).head(12).to_pandas().to_html(index=False, escape=True)

    html_text = f"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <title>Robust Alpha Campaign Report</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Noto Sans TC", sans-serif; margin: 28px; color: #172026; }}
    h1, h2 {{ margin-bottom: 8px; }}
    p, li {{ line-height: 1.65; }}
    table {{ border-collapse: collapse; width: 100%; margin: 14px 0 28px; font-size: 13px; }}
    th, td {{ border: 1px solid #d7dee8; padding: 7px 9px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #f3f6fa; }}
    .note {{ background: #f8fafc; border-left: 4px solid #516173; padding: 12px 16px; }}
    .bad {{ color: #9f1239; font-weight: 600; }}
    .good {{ color: #166534; font-weight: 600; }}
  </style>
</head>
<body>
  <h1>台股 Robust Alpha 策略研發報告</h1>
  <p>資料截止：{end}。本報告比較目前 execution-ready champion Iter95、第一性原理重新研發的 Iter96、加入市場 regime 風控的 Iter97，以及 0050、2330、事後最佳長壽單股 2383 的 total-return benchmark。</p>
  <div class="note">
    <p><strong>結論：</strong>Iter96/Iter97 沒有產生比 Iter95 更值得上線的策略。Iter95 仍是目前可用策略中最強；新的 leader-selection 系列雖然最近一年很強，但 MDD 過深、DSR 過低，而且相對 2330 沒有永久超車，因此不能升級為 production candidate。</p>
  </div>

  <h2>核心比較</h2>
  {table}

  <h2>NAV / P&L</h2>
  {plot_nav(series)}

  <h2>Drawdown</h2>
  {plot_drawdown(series)}

  <h2>相對 Benchmark</h2>
  {plot_relative({"Iter95 execution-ready champion": iter95, "Iter96 best first-principles": iter96, "Iter97 best regime overlay": iter97, "2383 TR hindsight top": b2383}, b0050, b2330)}

  <h2>Rolling CAGR</h2>
  {plot_rolling(series)}

  <h2>事後最佳長壽單股</h2>
  <p>這張表不是可投資的前瞻策略，只是壓力測試「是否勝過任一單股長持」這個極端要求。2005 起完整存活到 {end} 的股票中，2383 的 full-window CAGR 約 {pct(float(singles['cagr'][0]))}，高於 Iter95 full CAGR。這代表目前策略沒有達成「勝過事後最佳單股」這個最嚴格版本。</p>
  {top_single_html}

  <h2>研究判斷</h2>
  <ul>
    <li>Iter96 的失敗點不是沒有近期 upside，而是 realistic execution 後 OOS MDD 約 -70%，DSR 接近 0，風險調整後不可上線。</li>
    <li>Iter97 修正並測試 MA200 half/cash overlay 後，MDD 有改善但仍約 -60%，CAGR 與相對 2330 指標不足，沒有形成更好的 risk/return tradeoff。</li>
    <li>Iter95 的 OOS CAGR、DSR/PBO、MDD、fill ratio 仍全面勝過 Iter96/97，因此 registry 不應更新。</li>
  </ul>

  <h2>Artifacts</h2>
  <ul>
    <li>{html.escape(str(metrics_path.relative_to(REPO_ROOT)))}</li>
    <li>{html.escape(str(daily_path.relative_to(REPO_ROOT)))}</li>
    <li>{html.escape(str(singles_path.relative_to(REPO_ROOT)))}</li>
  </ul>
</body>
</html>
"""
    html_path = OUT_DIR / "robust_alpha_campaign_report.html"
    html_path.write_text(html_text)
    print(f"report={html_path}")
    print(f"metrics={metrics_path}")
    print(f"daily={daily_path}")
    print(f"single_stocks={singles_path}")


if __name__ == "__main__":
    main()
