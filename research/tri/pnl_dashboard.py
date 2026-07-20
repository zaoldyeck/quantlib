"""三策略 PnL 永續追蹤儀表板(2026-07-17;使用者規格;2026-07-19 加安聯台灣科技基金).

七線至 cache 最新,顯示/歸一共同窗自 2023-07-11(= Evergreen walk-forward 首個 OOS):
apex_revcycle_S、Evergreen(live-refit)、Serenity(ev_v3_wf)、0050、00685L、2330、
安聯台灣科技基金(見 `research/tri/allianz_fund.py`)。

**誠實基準(2026-07-20 重構)**:三策略線一律呼叫各自的官方引擎(Evergreen=
`research.evergreen.engine`、S=`chart_s_vs_benchmarks.run_s`、Serenity=`serenity/engine.py`),
**禁止在本檔重寫回測**(根治「dashboard 手寫 membership 偏離官方、與 live_config 差 96pp」
事故;parity 由 `evergreen/tests/test_engine_parity` 鎖死)。Evergreen 走 walk-forward
(逐年 refit-on-past 拼 OOS,非全樣本內 replay);各線 in-sample/OOS 邊界不同,附註標明。
上圖 NAV(log)/下圖 DD、KPI 卡、逐年表。由 `research.tri.daily` 鏈尾自動重生
(--no-dashboard 跳過)。

Run: uv run --project research python -m research.tri.pnl_dashboard
依賴 cache: 是(需最新)。色盤:dataviz reference palette(七槽已過 CVD 驗證,見
2026-07-19 驗證紀錄 —— validate_palette.js light/dark 皆 ALL CHECKS PASS)。
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "research"))

START = date(2022, 7, 11)            # 顯示/計算起點(起始標記池;暖機另加)
ALL_OOS_START = date(2025, 7, 11)    # 三線皆真前瞻起點(Serenity 出場參數凍結最晚)
OOS_STARTS = {  # 各策略線真前瞻起點;此前為 in-sample(繪成虛線)
    "Evergreen(live-refit)": date(2023, 7, 11),   # walk-forward 首個 OOS
    "apex_revcycle_S": date(2024, 1, 1),          # dev 2012-23 凍結後
    "Serenity(ev_v3_wf)": date(2025, 7, 11),      # 出場參數 train 2022-25 凍結後(選股仍 PIT)
}
OUT_HTML = REPO_ROOT / "research" / "tri" / "reports" / "pnl_dashboard.html"
RESULTS = REPO_ROOT / "research" / "strat_lab" / "results"
# dataviz reference palette slots 1-7(validate_palette.js 七槽 PASS,2026-07-19)
COLORS = {"Serenity(ev_v3_wf)": "#2a78d6", "Evergreen(live-refit)": "#008300", "apex_revcycle_S": "#e87ba4",
          "0050": "#eda100", "00685L 正2": "#1baf7a", "2330": "#eb6834", "安聯台灣科技基金": "#4a3aa7"}


def _cache_latest() -> date:
    """cache 最新交易日——單一真相來源(apex data.latest_date),不自帶查詢副本。"""
    from research.apex import data as apex_data
    con = apex_data.connect()
    try:
        return apex_data.latest_date(con)
    finally:
        con.close()


def serenity_nav(end: date) -> pd.Series:
    """現役 ev_v3_wf + 最新縫合冊全窗重放(冊由月度策展自動長大)。"""
    subprocess.run([sys.executable, "-m", "research.serenity.wf.build_registry"],
                   cwd=REPO_ROOT, check=True, capture_output=True, text=True)
    cmd = [sys.executable, str(REPO_ROOT / "research" / "serenity" / "engine.py"),
           "--start", START.isoformat(), "--end", end.isoformat(),
           "--registry", str(REPO_ROOT / "research/serenity/wf/registry_wf.csv"),
           "--variants", "ev_v3_wf", "--label", "pnl_dash_serenity",
           "--ablate", "filters", "--fresh-bonus", "10", "--fresh-months", "12"]
    subprocess.run(cmd, cwd=REPO_ROOT, check=True, capture_output=True, text=True)
    daily = pd.read_csv(RESULTS / "pnl_dash_serenity_ev_v3_wf_daily.csv", parse_dates=["date"])
    return daily.set_index("date")["nav"]


def evergreen_nav(end: date) -> pd.Series:
    """Evergreen 誠實前瞻線 = 官方引擎的 walk-forward(逐年 refit-on-past → 拼 OOS)。

    **零重寫**:池籍/計分/simulate 一律呼叫 research.evergreen.engine(唯一真源;
    2026-07-20 根治「dashboard 手寫 membership 偏離官方 midmonth_membership + 漏
    gate → live-refit 線與 live_config 差 96pp」事故)。walk-forward = 每點只用
    過去參數,真 OOS 起於 2023-07(之前為初訓期,無可交易軌跡)。引擎與
    live_config 逐位一致由 evergreen/tests/test_engine_parity 鎖死。"""
    from research.apex import data as apex_data
    from research.evergreen.engine import walkforward_nav_cached

    con = apex_data.connect()
    try:
        nav = walkforward_nav_cached(con, end).to_pandas()
    finally:
        con.close()
    nav["date"] = pd.to_datetime(nav["date"])
    return nav.set_index("date")["nav"]


def s_nav(end: date) -> pd.Series:
    """apex_revcycle_S 現役規格全窗重放到 cache 最新日(end 與其他線同源,
    見 main;prep 的資料截止由 end 決定,不再吃 chart 腳本的寫死字面值)。"""
    from research.apex import data as apex_data
    from research.apex.strategy_s import prep, run_s  # 官方 S 引擎唯一真源
    con = apex_data.connect()
    try:
        panel, feat, elig = prep(con, end.isoformat())
    finally:
        con.close()
    nav = run_s(panel, feat, elig, start=START.isoformat()).to_pandas()
    nav["date"] = pd.to_datetime(nav["date"])
    return nav.set_index("date")["nav"]


def bench_navs(end: date) -> dict[str, pd.Series]:
    from db import connect
    from prices import fetch_adjusted_panel, total_return_series
    out: dict[str, pd.Series] = {}
    con = connect(register_raw_quarterly=False)
    try:
        for code, name in (("0050", "0050"), ("2330", "2330")):
            s = total_return_series(con, code, START.isoformat(), end.isoformat(),
                                    market="twse").to_pandas()
            s["date"] = pd.to_datetime(s["date"])
            out[name] = s.set_index("date")["adj_close"]
        lev = fetch_adjusted_panel(con, START.isoformat(), end.isoformat(),
                                   codes=["00685L"], market="twse",
                                   include_extra_history_days=0).to_pandas()
        lev["date"] = pd.to_datetime(lev["date"])
        out["00685L 正2"] = lev.set_index("date")["close"]
    finally:
        con.close()
    return out


def boot_p5(mrets: pd.Series, seed: int = 20260716) -> float:
    m, n = mrets.to_numpy(), len(mrets)
    if n < 8:
        return float("nan")
    rng = np.random.default_rng(seed)
    out = []
    for _ in range(2000):
        idx: list[int] = []
        while len(idx) < n:
            pos = int(rng.integers(n))
            idx.extend(range(pos, pos + 6))
        samp = m[np.array(idx[:n]) % n]
        out.append(float(np.prod(1 + samp) ** (12 / n) - 1))
    return float(np.percentile(out, 5))


def kpis(nav: pd.Series) -> dict:
    nav = nav / nav.iloc[0]
    r = nav.pct_change().dropna()
    yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    dd = nav / nav.cummax() - 1
    downside = r[r < 0].std()
    mrets = nav.groupby(nav.index.to_period("M")).last().pct_change().dropna()
    ytd = nav.iloc[-1] / nav[nav.index.year < nav.index[-1].year].iloc[-1] - 1 \
        if (nav.index.year < nav.index[-1].year).any() else nav.iloc[-1] - 1
    return {"total_x": float(nav.iloc[-1]), "cagr": float(nav.iloc[-1]) ** (1 / yrs) - 1,
            "mdd": float(dd.min()),
            "sortino": float(r.mean() / downside * np.sqrt(252)) if downside > 0 else np.nan,
            "p5": boot_p5(mrets), "ytd": float(ytd)}


def yearly_table(navs: dict[str, pd.Series]) -> pd.DataFrame:
    rows = {}
    for name, nav in navs.items():
        nav = nav / nav.iloc[0]
        ye = nav.groupby(nav.index.year).last()
        prev = 1.0
        yr_ret = {}
        for y, v in ye.items():
            yr_ret[str(y)] = v / prev - 1
            prev = v
        rows[name] = yr_ret
    return pd.DataFrame(rows).T


def build_html(navs: dict[str, pd.Series], data_date: date) -> str:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.68, 0.32],
                        vertical_spacing=0.04, subplot_titles=("累積淨值(起點=1,log 尺度)", "回撤"))
    ends = []
    for name, nav in navs.items():
        norm = nav / nav.iloc[0]
        dd = norm / norm.cummax() - 1
        c = COLORS[name]
        oos = OOS_STARTS.get(name)
        if oos is not None and bool((norm.index < pd.Timestamp(oos)).any()):
            b = pd.Timestamp(oos)  # in-sample(虛)+ OOS(實);邊界點兩段共用以接續
            ins, oosn = norm[norm.index <= b], norm[norm.index >= b]
            fig.add_trace(go.Scatter(x=ins.index, y=ins, name=name, mode="lines",
                                     line=dict(color=c, width=2, dash="dot"), showlegend=False,
                                     hovertemplate=f"{name}(in-sample): %{{y:.2f}}x<extra></extra>"), 1, 1)
            fig.add_trace(go.Scatter(x=oosn.index, y=oosn, name=name, mode="lines",
                                     line=dict(color=c, width=2),
                                     hovertemplate=f"{name}: %{{y:.2f}}x<extra></extra>"), 1, 1)
        else:
            fig.add_trace(go.Scatter(x=norm.index, y=norm, name=name, mode="lines",
                                     line=dict(color=c, width=2),
                                     hovertemplate=f"{name}: %{{y:.2f}}x<extra></extra>"), 1, 1)
        fig.add_trace(go.Scatter(x=dd.index, y=dd, name=name, mode="lines", showlegend=False,
                                 line=dict(color=c, width=1.5),
                                 hovertemplate=f"{name}: %{{y:.1%}}<extra></extra>"), 2, 1)
        ends.append([name, norm.index[-1], float(np.log10(norm.iloc[-1])), float(norm.iloc[-1]), c])
    # 線尾直標防碰撞:log10 空間由下而上推開最小間隔
    ends.sort(key=lambda e: e[2])
    MIN_GAP = 0.07
    for i in range(1, len(ends)):
        if ends[i][2] - ends[i - 1][2] < MIN_GAP:
            ends[i][2] = ends[i - 1][2] + MIN_GAP
    for name, x_last, ylog, mult, c in ends:
        fig.add_annotation(x=x_last, y=ylog, xref="x", yref="y",
                           text=f" {name} {mult:.1f}x", showarrow=False,
                           font=dict(color=c, size=11), xanchor="left")
    for rowi in (1, 2):
        fig.add_shape(type="line", x0=ALL_OOS_START.isoformat(), x1=ALL_OOS_START.isoformat(),
                      y0=0, y1=1, yref=f"y{'' if rowi == 1 else '2'} domain",
                      xref="x", line=dict(dash="dot", color="#52514e", width=1))
    fig.add_annotation(x=ALL_OOS_START.isoformat(), y=1.02, xref="x", yref="y domain",
                       text="此後三線皆真前瞻(各線 in-sample 邊界見附註)",
                       showarrow=False, font=dict(size=10, color="#52514e"))
    fig.update_yaxes(type="log", row=1, col=1, gridcolor="#eceae6")
    fig.update_yaxes(tickformat=".0%", row=2, col=1, gridcolor="#eceae6")
    fig.update_xaxes(gridcolor="#eceae6")
    fig.update_layout(height=760, template="plotly_white", hovermode="x unified",
                      legend=dict(orientation="h", y=1.06),
                      margin=dict(l=60, r=140, t=60, b=40),
                      paper_bgcolor="#fcfcfb", plot_bgcolor="#fcfcfb")

    cards = []
    for name, nav in navs.items():
        k = kpis(nav)
        cards.append(
            f"<div class='card' style='border-top:3px solid {COLORS[name]}'>"
            f"<div class='cn'>{name}</div>"
            f"<div class='big'>{k['total_x']:.1f}x</div>"
            f"<div class='kv'>CAGR <b>{k['cagr']:.1%}</b> · MDD <b>{k['mdd']:.1%}</b></div>"
            f"<div class='kv'>Sortino <b>{k['sortino']:.2f}</b> · 保守P5 <b>{k['p5']:.0%}</b></div>"
            f"<div class='kv'>YTD <b>{k['ytd']:.1%}</b></div></div>")

    yt = yearly_table(navs)
    def cell(v):
        if pd.isna(v):
            return "<td>—</td>"
        bg = "#e8f2e8" if v > 0 else "#f7e6e6"
        return f"<td style='background:{bg}'>{v:.1%}</td>"
    yr_html = ("<table class='yr'><tr><th>策略 \\ 年</th>"
               + "".join(f"<th>{c}</th>" for c in yt.columns) + "</tr>"
               + "".join("<tr><td style='text-align:left'><b>"
                         f"<span style='color:{COLORS[n]}'>●</span> {n}</b></td>"
                         + "".join(cell(v) for v in row) + "</tr>"
                         for n, row in yt.iterrows())
               + "</table>")

    note = (f"資料日 <b>{data_date}</b> · 生成 {date.today()} · 三策略均自 <b>{START}</b>"
            f"(起始標記池)起 · 基準含息調整 · 由 <code>research.tri.daily</code> 自動更新"
            f"(策略線一律呼叫官方引擎,禁重寫)。<br>"
            f"<b>虛線 = in-sample(參數看過該段)、實線 = 真前瞻(OOS)</b>,各線邊界不同:"
            f"<b>Evergreen</b> 逐年 walk-forward refit,OOS 自 <b>2023-07</b>(前為初訓期);"
            f"<b>apex_revcycle_S</b> 固定規則(dev 2012-23),OOS 自 <b>2024-01</b>;"
            f"<b>Serenity</b> 選股全程 PIT,惟出場參數 train 2022-25 凍結,OOS 自 <b>2025-07</b>。"
            f"點線(2025-07)後三線皆真前瞻。<b>三策略均為微型股集中回測,絕對數字含容量"
            f"膨脹,不可外推至大資本</b>。")
    return f"""<meta charset='utf-8'><title>三策略 PnL 追蹤</title>
<style>body{{font-family:-apple-system,'PingFang TC',sans-serif;background:#fcfcfb;color:#0b0b0b;margin:24px auto;max-width:1180px}}
h1{{font-size:22px}} .cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:14px 0}}
.card{{background:#fff;border:1px solid #eceae6;border-radius:8px;padding:10px 12px}}
.cn{{font-size:12px;color:#52514e}} .big{{font-size:22px;font-weight:700;margin:2px 0}}
.kv{{font-size:11.5px;color:#52514e}} .kv b{{color:#0b0b0b}}
.yr{{border-collapse:collapse;margin:10px 0;font-size:13px}} .yr th,.yr td{{border:1px solid #eceae6;padding:5px 10px;text-align:right}}
.note{{font-size:12px;color:#52514e;margin-top:14px}}</style>
<h1>三策略 PnL 永續追蹤 <span style='font-size:13px;color:#52514e'>(三策略現役參數 vs 0050 / 00685L / 2330 / 安聯台灣科技基金)</span></h1>
<div class='cards'>{''.join(cards)}</div>
{fig.to_html(full_html=False, include_plotlyjs=True)}
<h2 style='font-size:16px'>逐年績效</h2>{yr_html}
<div class='note'>{note}</div>"""


#: 策略線 = cache 全窗重放,末日必須貼齊 end;基準/基金容許自然落後(只報告)
_STRATEGY_LINES = ("Serenity(ev_v3_wf)", "Evergreen(live-refit)", "apex_revcycle_S")
_STALE_TOLERANCE_DAYS = 4  # 容長週末/假期;策略線正常應恰好貼齊 cache 最新日


def _assert_current(navs: dict[str, pd.Series], end: date) -> None:
    """防凍結守護(2026-07-20 apex_revcycle_S 凍結事故的復發防線)。

    策略線的 NAV 由 cache 全窗重放,末日必須貼齊 cache 最新日;任一條落後過多 =
    某段管線把「最新」寫死了(如 chart 腳本的 DE 字面值)。寧可大聲炸掉、留住上
    一張正確的圖,也不要靜靜覆蓋成過時儀表板。基準/基金容許自然落後,只報告。
    """
    end_ts = pd.Timestamp(end)
    stale = []
    for name, series in navs.items():
        last = series.index.max()
        behind = (end_ts - last).days
        kind = "策略" if name in _STRATEGY_LINES else "基準/基金"
        print(f"  [{kind}] {name}: 末日 {last.date()}(距 cache 最新 {behind} 日)")
        if name in _STRATEGY_LINES and behind > _STALE_TOLERANCE_DAYS:
            stale.append(f"{name}(末日 {last.date()}、落後 {behind} 日)")
    if stale:
        raise RuntimeError(
            f"策略線未更新到 cache 最新日 {end}:" + "、".join(stale)
            + "。某段管線把資料截止寫死了——一律改用 data.latest_date 動態讀;"
              "修好再出圖,不覆蓋過時儀表板。")


def main() -> None:
    from research.tri.allianz_fund import load_nav as allianz_nav

    end = _cache_latest()
    navs: dict[str, pd.Series] = {}
    navs["Serenity(ev_v3_wf)"] = serenity_nav(end)
    navs["Evergreen(live-refit)"] = evergreen_nav(end)
    navs["apex_revcycle_S"] = s_nav(end)
    navs.update(bench_navs(end))
    navs["安聯台灣科技基金"] = allianz_nav(end)
    navs = {k: v[(v.index.date >= START)] for k, v in navs.items()}
    _assert_current(navs, end)
    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUT_HTML.write_text(build_html(navs, end), encoding="utf-8")
    print(f"dashboard -> {OUT_HTML}")


if __name__ == "__main__":
    main()
