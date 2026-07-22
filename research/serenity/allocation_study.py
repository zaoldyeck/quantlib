"""Serenity engine × Iter95 (pure-quant champion) allocation study.

Overlap window is short (~17 months), so the deliverable is a weight BAND
chosen by month-block-bootstrap lower-bound Sortino, not a point estimate.
Monthly-rebalanced NAV-level blend of the two realistic-execution series.

Usage:
  uv run --project research python research/serenity/allocation_study.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "research"))
sys.path.insert(0, str(REPO_ROOT / "research" / "strat_lab"))

from validate_hybrid import TDPY, metrics  # noqa: E402

RESULTS = paths.OUT_STRAT_LAB
DOCS = REPO_ROOT / "docs" / "serenity"

SERENITY = RESULTS / "serenity_event_engine_v1_ev_full_tp60_v2_exec_daily.csv"
ITER95 = RESULTS / "iter_95_global_exit_aware_search_iter92_unconstrained_meta_switch__time50_r-1_daily.csv"


def read_nav(path: Path, name: str) -> pl.DataFrame:
    df = pl.read_csv(path, try_parse_dates=True).select(["date", "nav"]).sort("date")
    if df["date"].dtype != pl.Date:
        df = df.with_columns(pl.col("date").cast(pl.Date))
    return df.rename({"nav": name})


def month_block_bootstrap_sortino(rets: np.ndarray, months: np.ndarray, n: int = 2000) -> tuple[float, float]:
    keys = np.unique(months)
    blocks = [rets[months == k] for k in keys]
    rng = np.random.default_rng(42)
    sortinos, cagrs = [], []
    for _ in range(n):
        idx = rng.integers(0, len(blocks), size=len(blocks))
        sample = np.concatenate([blocks[i] for i in idx])
        m = metrics(sample, years=len(sample) / TDPY)
        sortinos.append(m["sortino"])
        cagrs.append(m["cagr"])
    return float(np.quantile(sortinos, 0.05)), float(np.quantile(cagrs, 0.05))


def main() -> None:
    a = read_nav(SERENITY, "nav_a")
    b = read_nav(ITER95, "nav_b")
    j = a.join(b, on="date", how="inner").sort("date")
    dates = j["date"].to_list()
    na = j["nav_a"].to_numpy()
    nb = j["nav_b"].to_numpy()
    ra = na[1:] / na[:-1] - 1.0
    rb = nb[1:] / nb[:-1] - 1.0
    months = np.array([d.year * 100 + d.month for d in dates[1:]])
    yrs = (dates[-1] - dates[0]).days / 365.25
    corr = float(np.corrcoef(ra, rb)[0, 1])
    print(f"overlap {dates[0]}~{dates[-1]} ({yrs:.2f}y), corr={corr:.3f}")

    rows = []
    for w in np.arange(0.0, 1.01, 0.1):
        blend = w * ra + (1 - w) * rb  # monthly-rebalanced approximation at daily level
        m = metrics(blend, years=yrs)
        nav = np.cumprod(1 + blend)
        mdd = float((nav / np.maximum.accumulate(nav) - 1).min())
        s_lb, c_lb = month_block_bootstrap_sortino(blend, months)
        rows.append(
            {
                "w_serenity": round(float(w), 1),
                "cagr": m["cagr"],
                "sortino": m["sortino"],
                "mdd": mdd,
                "boot_sortino_lb95": s_lb,
                "boot_cagr_lb95": c_lb,
            }
        )
    frame = pl.DataFrame(rows)
    print(frame.with_columns(pl.col(c).round(3) for c in frame.columns))
    best = frame.sort("boot_sortino_lb95", descending=True).head(3)
    print("top-3 by bootstrap-LB Sortino:")
    print(best.with_columns(pl.col(c).round(3) for c in best.columns))

    out = DOCS / "serenity_quant_allocation_2026-07.md"
    lines = [
        "# Serenity × Iter95 資金分配研究(2026-07-07)",
        "",
        f"- 重疊窗:{dates[0]} ~ {dates[-1]}({yrs:.2f} 年)— **樣本短,結論為權重帶而非精確值**",
        f"- 兩序列皆為 realistic execution NAV;日報酬相關性 {corr:.3f}",
        "- 選擇準則:月 block bootstrap 的 Sortino 5% 下界(保守),非點估計",
        "",
        frame.with_columns(pl.col(c).round(3) for c in frame.columns).to_pandas().to_markdown(index=False),
        "",
        "## 判讀",
        "",
        "- 依 bootstrap-LB Sortino 的最佳權重帶見 top-3;鄰近權重差異小於樣本噪音,",
        "  故採「帶」而非「點」。",
        "- 重疊窗僅覆蓋 2025-26 單一 regime;**live 累積 6 個月後必須重跑本研究**。",
        "- Iter95 NAV 截止 2026-05-22(其 book 尚未再生);正式合併執行前先更新其 target book。",
    ]
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"report -> {out}")


if __name__ == "__main__":
    main()
