"""三策略 PnL 永續追蹤儀表板(2026-07-17;使用者規格;2026-07-19 加安聯台灣科技基金).

七線同窗(2022-07-11 起)至 cache 最新:apex_revcycle_S(apex_revcycle_S 現役)、Evergreen
最強(live_config 現役)、Serenity(ev_v3_wf)(ev_v3_wf 現役+最新縫合冊)、0050、00685L、
2330、安聯台灣科技基金(主動式共同基金,見 `research/tri/allianz_fund.py`)。上圖
NAV(log)/下圖 DD、KPI 卡、逐年績效表;三策略線標「參數世代線」(2026-07-17,右側
為真前瞻)。輸出 self-contained HTML 至固定路徑,由 `research.tri.daily` 鏈尾自動
重生(--no-dashboard 跳過)——瀏覽器書籤即「打開就最新」。

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

START = date(2022, 7, 11)
PARAM_EPOCH = date(2026, 7, 17)  # 三策略現役參數世代日(右側=真前瞻)
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
    """live_config(EV43 refit 現役參數)全窗重放。資料構建複製自 ev43 LabL,
    僅把 hardcode 窗參數化(不動戰役檔案)。"""
    import json
    from research.apex import data as apex_data
    from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
    from research.evergreen.ev36_walkforward import load_registry as ev_load_registry
    from research.evergreen.ev30_baseline import midmonth_membership  # noqa: F401(路徑一致性)
    from research.evergreen.harvest import C

    cfg = json.loads((REPO_ROOT / "research/evergreen/data/live_config.json").read_text())["config"]
    reg = ev_load_registry()
    con = apex_data.connect()
    panel_full = apex_data.common_stocks(
        apex_data.load_panel(con, "2021-06-01", end.isoformat(), warmup_days=300))
    codes = reg["code"].unique().to_list()
    panel = panel_full.filter(pl.col(C).is_in(codes)).sort([C, "date"])
    feats = (panel.with_columns([
        (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
        (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
    ]).select(["date", C, "h120", "h52"]))
    # 池籍:最近 pool_months 個標記月聯集(月中生效,同 tri advisor 語義)
    months = sorted(reg["month"].unique().to_list())
    dates_all = panel_full.select("date").unique().sort("date")["date"].to_list()
    memb_rows = []
    for i, m in enumerate(months):
        y, mo = int(m[:4]), int(m[5:7])
        eff = next((d for d in dates_all if d >= date(y, mo, 11)), None)
        if eff is None:
            continue
        nxt_months = months[i + 1:i + 2]
        if nxt_months:
            ny, nm = int(nxt_months[0][:4]), int(nxt_months[0][5:7])
            eff_next = next((d for d in dates_all if d >= date(ny, nm, 11)), end)
        else:
            eff_next = end
        active = months[max(0, i - cfg["pool_months"] + 1):i + 1]
        pool = reg.filter(pl.col("month").is_in(active))["code"].unique().to_list()
        for d in [x for x in dates_all if eff <= x < eff_next]:
            memb_rows.extend({"date": d, C: c} for c in pool)
    memb = pl.DataFrame(memb_rows).with_columns(pl.col(C).cast(pl.Utf8))
    pool_flag = (panel.select(["date", C]).join(memb.with_columns(pl.lit(True).alias("in_pool")),
                                                on=["date", C], how="left")
                 .filter(pl.col("in_pool").is_null()).select(["date", C]))

    def rank(c):
        return (pl.col(c).rank() / pl.len()).over("date")

    adv = (panel.sort([C, "date"])
           .with_columns(pl.col("trade_value").cast(pl.Float64)
                         .rolling_median(20).over(C).alias("adv20"))
           .select(["date", C, "adv20"]))
    sc = (memb.join(feats, on=["date", C], how="left")
          .join(adv, on=["date", C], how="left")
          .with_columns(pl.col("adv20").fill_null(1e12))
          .filter(pl.col("h120").fill_null(0) > cfg["h120"]))
    base = rank("h52") * rank("h120")
    expr = base if cfg["score"] == "base" else base * (1.0 - rank("adv20"))
    sc = (sc.with_columns(expr.alias("score"))
          .with_columns(pl.lit(1.0 / cfg["n_slots"]).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    res = simulate(panel, sc, exit_flags=pool_flag, exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=cfg["n_slots"], max_new_per_day=cfg["max_new"]),
                   exit_spec=ExitSpec(trailing_stop=cfg["trail"], loser_time_stop=cfg["lts"]),
                   start=START)
    nav = res.nav.sort("date").filter(pl.col("date") >= START).to_pandas()
    nav["date"] = pd.to_datetime(nav["date"])
    return nav.set_index("date")["nav"]


def s_nav(end: date) -> pd.Series:
    """apex_revcycle_S 現役規格全窗重放到 cache 最新日(end 與其他線同源,
    見 main;prep 的資料截止由 end 決定,不再吃 chart 腳本的寫死字面值)。"""
    from research.apex import data as apex_data
    from research.apex.experiments.chart_s_vs_benchmarks import prep, run_s
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
        fig.add_shape(type="line", x0=PARAM_EPOCH.isoformat(), x1=PARAM_EPOCH.isoformat(),
                      y0=0, y1=1, yref=f"y{'' if rowi == 1 else '2'} domain",
                      xref="x", line=dict(dash="dot", color="#52514e", width=1))
    fig.add_annotation(x=PARAM_EPOCH.isoformat(), y=1.02, xref="x", yref="y domain",
                       text="參數世代線(右側=真前瞻)", showarrow=False,
                       font=dict(size=10, color="#52514e"))
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

    note = (f"資料日 <b>{data_date}</b> · 生成 {date.today()} · 同窗起點 {START} · "
            f"三條策略線 = 各自<b>現役上場參數</b>的全窗重放(參數世代線左側含 in-sample,"
            f"右側為真前瞻,證據力隨時間累積);基準含息調整。"
            f"由 <code>research.tri.daily</code> 自動更新。")
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
