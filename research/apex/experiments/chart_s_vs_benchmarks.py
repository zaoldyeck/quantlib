"""apex_revcycle_S vs 0050 vs 00631L(正2)PnL 比較圖。

需要 cache_tables.py 為最新(讀 research/cache.duckdb)。
重跑 S 於兩窗口(正2 全史同窗 2014-10-31 起、現代 era 2019-01-02 起),
與含息基準同窗歸一化,輸出互動 HTML 至 research/apex/reports/。

窗口紀律披露:S 的參數最佳化窗 = dev 2019-01-02 → 2025-06-30;
圖中 2014-2018 與 2025-07 之後為最佳化窗外(evaluation-only)。

    uv run --project research python -m research.apex.experiments.chart_s_vs_benchmarks
"""
from __future__ import annotations

import os
import time

import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
import polars as pl
from plotly.subplots import make_subplots

from research.apex import data
from research.apex.strategy_s import DS, prep, run_s  # 官方 S 引擎唯一真源

MODERN = "2019-01-02"      # chart 對比用「現代 era」窗(非 S 規格)
COLORS = {"S": "#d62728", "正2": "#ff9f40", "0050": "#6b8cae"}
NAMES = {"S": "apex_revcycle_S", "正2": "00631L 正2", "0050": "0050(含息)"}
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "reports")


def stats(df: pl.DataFrame) -> dict:
    v = df["nav"].to_numpy()
    d = df["date"].to_numpy()
    yrs = (d[-1] - d[0]).astype("timedelta64[D]").astype(float) / 365.25
    r = v[1:] / v[:-1] - 1
    dd = v / np.maximum.accumulate(v) - 1
    return {"mult": v[-1], "cagr": v[-1] ** (1 / yrs) - 1, "mdd": dd.min(),
            "sharpe": r.mean() / r.std() * np.sqrt(252)}


def nav_dd_fig(series: dict[str, pl.DataFrame], title: str) -> go.Figure:
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.72, 0.28],
                        vertical_spacing=0.04,
                        subplot_titles=("累積淨值(log)", "回撤 %"))
    for k in ["S", "正2", "0050"]:
        df = series[k]
        fig.add_trace(go.Scatter(
            x=df["date"].to_list(), y=df["nav"].to_list(), name=NAMES[k],
            line=dict(color=COLORS[k], width=2.2 if k == "S" else 1.6),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.1f}x<extra>" + NAMES[k] + "</extra>"),
            row=1, col=1)
        v = df["nav"].to_numpy()
        fig.add_trace(go.Scatter(
            x=df["date"].to_list(), y=(v / np.maximum.accumulate(v) - 1) * 100,
            name=NAMES[k], showlegend=False,
            line=dict(color=COLORS[k], width=1.2),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.1f}%<extra>" + NAMES[k] + "</extra>"),
            row=2, col=1)
    fig.update_yaxes(type="log", row=1, col=1, tickformat=",.0f", ticksuffix="x")
    fig.update_layout(title=title, height=680, template="plotly_white",
                      legend=dict(orientation="h", y=1.06, x=0),
                      margin=dict(l=60, r=30, t=90, b=40), hovermode="x unified")
    return fig


def yearly_ret(series: dict[str, pl.DataFrame]) -> dict[int, dict[str, float]]:
    """逐年報酬(基期 = 前年末收,首年 = 窗口起點)。"""
    rows: dict[int, dict[str, float]] = {}
    for k, df in series.items():
        v = df["nav"].to_numpy()
        years = df["date"].dt.year().to_numpy()
        for y in np.unique(years):
            idx = np.where(years == y)[0]
            prev = v[idx[0] - 1] if idx[0] > 0 else v[idx[0]]
            rows.setdefault(int(y), {})[k] = v[idx[-1]] / prev - 1
    return rows


def main() -> None:
    t0 = time.time()
    con = data.connect()
    de = data.latest_date(con).isoformat()  # cache 最新日,動態
    panel, feat, elig = prep(con, de)
    full = {"S": run_s(panel, feat, elig, DS)}
    modern = {"S": run_s(panel, feat, elig, MODERN)}
    for key, code in [("0050", "0050"), ("正2", "00631L")]:
        for tag, start in [("full", DS), ("modern", MODERN)]:
            b = data.benchmark_nav(con, start, de, code=code).sort("date")
            b = b.with_columns(pl.col("nav") / pl.col("nav").first())
            (full if tag == "full" else modern)[key] = b

    f1 = nav_dd_fig(full, f"正2 全史同窗:{DS} → {de}(起點 = 1,log 刻度)")
    f2 = nav_dd_fig(modern, f"現代 era:{MODERN} → {de}(起點 = 1,log 刻度)")

    yr = yearly_ret(full)

    def cell(x: float | None) -> str:
        if x is None:
            return "<td>—</td>"
        style = ' style="color:#c0392b"' if x < 0 else ""
        return f"<td{style}>{x:+.1%}</td>"

    trs = "".join(
        f"<tr><td><b>{y}{'*' if y in (2014, 2026) else ''}</b></td>"
        + cell(yr[y].get("S")) + cell(yr[y].get("正2")) + cell(yr[y].get("0050"))
        + "</tr>"
        for y in sorted(yr))

    def srow(label: str, d: dict) -> str:
        return (f"<tr><td><b>{label}</b></td>"
                + "".join(f"<td>{d[k]['mult']:,.0f}x / {d[k]['cagr']:+.1%}"
                          f" / {d[k]['mdd']:.0%} / {d[k]['sharpe']:.2f}</td>"
                          for k in ["S", "正2", "0050"]) + "</tr>")

    st_f = {k: stats(v) for k, v in full.items()}
    st_m = {k: stats(v) for k, v in modern.items()}
    html = f"""<meta charset="utf-8"><title>apex_revcycle_S vs 0050 vs 正2</title>
<style>
body{{font-family:-apple-system,'PingFang TC',sans-serif;max-width:1080px;margin:24px auto;padding:0 16px;color:#222}}
table{{border-collapse:collapse;margin:12px 0;font-size:14px;width:100%}}
td,th{{border:1px solid #ddd;padding:5px 10px;text-align:right}}
th{{background:#f5f5f5}} td:first-child,th:first-child{{text-align:left}}
h1{{font-size:22px}} h2{{font-size:17px;margin-top:28px}} .note{{color:#777;font-size:13px}}
</style>
<h1>apex_revcycle_S vs 0050 vs 00631L(正2)PnL 比較</h1>
<p class="note">三條線皆為同窗歸一化淨值(起點 = 1)、含息含成本:策略淨值已扣手續費
0.0285%×2、賣稅 0.3%、滑價 0.1%×2,基準為調整後總報酬。資料至 {de}。
<b>窗口紀律</b>:S 參數最佳化窗 = 2019-01 → 2025-06(dev);圖中 2014-2018 與
2025-07 之後為最佳化窗外(evaluation-only)。</p>
{pio.to_html(f1, full_html=False, include_plotlyjs=True)}
{pio.to_html(f2, full_html=False, include_plotlyjs=False)}
<h2>摘要(倍數 / CAGR / MDD / Sharpe)</h2>
<table><tr><th>窗口</th><th>apex_revcycle_S</th><th>00631L 正2</th><th>0050</th></tr>
{srow("正2 全史同窗 2014-10 起", st_f)}
{srow("現代 era 2019 起", st_m)}</table>
<h2>逐年報酬(正2 全史同窗)</h2>
<table><tr><th>年</th><th>apex_revcycle_S</th><th>00631L 正2</th><th>0050</th></tr>{trs}</table>
<p class="note">* 2014 僅 11–12 月;2026 至 7/9。策略容量為 NT$300 萬級(池底 ADV 500 萬),
數字不可外推至大資本;完整規格與極限見 research/apex/STRATEGY.md。</p>
"""
    os.makedirs(OUT_DIR, exist_ok=True)
    out = os.path.join(OUT_DIR, "s_vs_benchmarks.html")
    with open(out, "w") as f:
        f.write(html)
    for tag, d in [("同窗", st_f), ("現代", st_m)]:
        print(tag, {k: f"{v['mult']:,.0f}x/{v['cagr']:+.1%}/MDD {v['mdd']:.0%}"
                    for k, v in d.items()})
    print(f"→ {os.path.relpath(out)}  ({time.time()-t0:.0f}s)")


if __name__ == "__main__":
    main()
