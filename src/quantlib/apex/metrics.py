"""apex 績效與交易統計 — 純函式,輸入 SimResult/DataFrame、輸出 dict/DataFrame。"""
from __future__ import annotations

import numpy as np
import polars as pl

TRADING_DAYS = 252


def perf_stats(nav: pl.DataFrame) -> dict:
    """NAV 序列 → 核心績效指標。

    CAGR 用實際日曆跨度年化;Sharpe/Sortino 用日報酬 ×√252(rf = 0);
    Sortino 下行以 min(r, 0) 的 RMS 定義;exposure = mean(invested / nav)。
    """
    v = nav["nav"].to_numpy()
    d = nav["date"].to_numpy()
    if len(v) < 2:
        raise ValueError("nav series too short")
    years = (d[-1] - d[0]).astype("timedelta64[D]").astype(float) / 365.25
    rets = v[1:] / v[:-1] - 1.0
    mean, std = float(np.mean(rets)), float(np.std(rets, ddof=1))
    downside = float(np.sqrt(np.mean(np.minimum(rets, 0.0) ** 2)))
    runmax = np.maximum.accumulate(v)
    dd = v / runmax - 1.0
    mdd = float(dd.min())
    cagr = float((v[-1] / v[0]) ** (1.0 / years) - 1.0) if years > 0 else 0.0
    return {
        "years": round(float(years), 2),
        "cagr": cagr,
        "ann_vol": std * np.sqrt(TRADING_DAYS),
        "sharpe": (mean / std * np.sqrt(TRADING_DAYS)) if std > 0 else 0.0,
        "sortino": (mean / downside * np.sqrt(TRADING_DAYS)) if downside > 0 else 0.0,
        "mdd": mdd,
        "calmar": (cagr / abs(mdd)) if mdd < 0 else float("inf"),
        "exposure": float((nav["invested"] / nav["nav"]).mean()) if "invested" in nav.columns else None,
        "final_nav_ratio": float(v[-1] / v[0]),
    }


def yearly_table(nav: pl.DataFrame) -> pl.DataFrame:
    """逐年報酬與年內 MDD:(year, ret, mdd)。首年以窗口首日 NAV 為基期。"""
    df = nav.sort("date").with_columns(pl.col("date").dt.year().alias("year"))
    year_end = df.group_by("year").agg(pl.col("nav").last()).sort("year")
    intra = (
        df.with_columns(
            (pl.col("nav") / pl.col("nav").cum_max().over("year") - 1).alias("dd")
        )
        .group_by("year")
        .agg(pl.col("dd").min().alias("mdd"))
    )
    return (
        year_end.with_columns(
            pl.col("nav").shift(1, fill_value=float(df["nav"][0])).alias("prev")
        )
        .with_columns((pl.col("nav") / pl.col("prev") - 1).alias("ret"))
        .join(intra, on="year")
        .select(["year", "ret", "mdd"])
        .sort("year")
    )


def trade_stats(trades: pl.DataFrame) -> dict:
    """已平倉交易統計(exit_reason="open" 的期末假想出場另計 n_open)。"""
    closed = trades.filter(pl.col("exit_reason") != "open")
    n_open = trades.height - closed.height
    if closed.height == 0:
        return {"n_trades": 0, "n_open": n_open}
    r = closed["ret_net"].to_numpy()
    wins, losses = r[r > 0], r[r <= 0]
    reasons = dict(
        closed.group_by("exit_reason").len().sort("len", descending=True).iter_rows()
    )
    return {
        "n_trades": int(closed.height),
        "n_open": n_open,
        "win_rate": float(len(wins) / len(r)),
        "avg_win": float(wins.mean()) if len(wins) else 0.0,
        "avg_loss": float(losses.mean()) if len(losses) else 0.0,
        "profit_factor": float(wins.sum() / -losses.sum()) if losses.sum() < 0 else float("inf"),
        "med_days_held": float(closed["days_held"].median()),
        "exit_reasons": reasons,
    }


def turnover_ann(trades: pl.DataFrame, nav: pl.DataFrame) -> float:
    """年化單邊換手率 = Σ買進成本(含手續費)/ 平均 NAV / 年數(精確值)。"""
    d = nav["date"].to_numpy()
    years = (d[-1] - d[0]).astype("timedelta64[D]").astype(float) / 365.25
    if years <= 0 or trades.height == 0:
        return 0.0
    return float(trades["cost"].sum()) / float(nav["nav"].mean()) / years


def summarize(nav: pl.DataFrame, trades: pl.DataFrame, benchmark: pl.DataFrame | None = None) -> dict:
    """單一 trial 的 ledger-ready 扁平指標 dict。"""
    out = perf_stats(nav) | trade_stats(trades)
    out["turnover_ann"] = turnover_ann(trades, nav)
    if benchmark is not None and benchmark.height >= 2:
        b = benchmark.sort("date")
        bd = b["date"].to_numpy()
        bv = b["nav"].to_numpy()
        yrs = (bd[-1] - bd[0]).astype("timedelta64[D]").astype(float) / 365.25
        bench_cagr = float((bv[-1] / bv[0]) ** (1.0 / yrs) - 1.0)
        out["bench_cagr"] = bench_cagr
        out["excess_cagr"] = out["cagr"] - bench_cagr
    return out


def fmt_report(name: str, nav: pl.DataFrame, trades: pl.DataFrame,
               benchmark: pl.DataFrame | None = None) -> str:
    """人類可讀單頁報告(終端輸出用)。"""
    s = summarize(nav, trades, benchmark)
    lines = [
        f"── {name} ──",
        f"window: {nav['date'][0]} → {nav['date'][-1]}  ({s['years']}y)",
        f"CAGR {s['cagr']*100:8.2f}%   Sharpe {s['sharpe']:5.2f}   Sortino {s['sortino']:5.2f}",
        f"MDD  {s['mdd']*100:8.2f}%   Calmar {s['calmar']:5.2f}   vol {s['ann_vol']*100:6.2f}%",
        f"exposure {s['exposure']*100:5.1f}%   turnover(ann,單邊) {s['turnover_ann']:.1f}x",
    ]
    if "bench_cagr" in s:
        lines.append(f"bench CAGR {s['bench_cagr']*100:6.2f}%   excess {s['excess_cagr']*100:+6.2f}pp")
    if s.get("n_trades", 0):
        lines.append(
            f"trades {s['n_trades']} (open {s['n_open']})   win {s['win_rate']*100:.1f}%   "
            f"avgW {s['avg_win']*100:+.2f}% avgL {s['avg_loss']*100:+.2f}%   "
            f"PF {s['profit_factor']:.2f}   medHold {s['med_days_held']:.0f}d"
        )
        lines.append(f"exits: {s['exit_reasons']}")
    yt = yearly_table(nav)
    lines.append("year  " + "  ".join(f"{y}" for y in yt["year"]))
    lines.append("ret%  " + "  ".join(f"{r*100:+.1f}" for r in yt["ret"]))
    lines.append("mdd%  " + "  ".join(f"{m*100:.1f}" for m in yt["mdd"]))
    return "\n".join(lines)
