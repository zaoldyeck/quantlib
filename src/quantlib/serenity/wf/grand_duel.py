"""戰役十八四方對決:同 OOS 窗(2025-07-11 ~ 2026-07-03)、同語義(全窗一跑、
OOS 段內歸一)、同尺(統一 harness 重算月報酬 block bootstrap P5 + CAGR/Sortino/MDD)。

四方:S(apex_revcycle_S,chart_s_vs_benchmarks.run_s 全窗)、Evergreen(EV36 top-1,
ev36_top1_nav.parquet oos 段)、Serenity 現役(b18_full_incumbent)、Serenity 極限
(Stage 3 top-1,b18_full_newtop1)。另附 0050 同窗。

Run: uv run --project . python -m quantlib.serenity.wf.grand_duel
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from quantlib import paths

REPO_ROOT = paths.REPO
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src" / "quantlib"))
RESULTS = paths.OUT_STRAT_LAB
OOS0, OOS1 = date(2025, 7, 11), date(2026, 7, 3)

from quantlib.serenity.backfill.pool_quality_duel import boot_cagr_lb  # noqa: E402


def oos_slice(nav: pd.Series) -> pd.Series:
    seg = nav[(nav.index.date >= OOS0) & (nav.index.date <= OOS1)]
    return seg / seg.iloc[0]


def metrics_row(name: str, nav: pd.Series, rng) -> dict:
    r_d = nav.pct_change().dropna()
    yrs = (nav.index[-1] - nav.index[0]).days / 365.25
    cagr = float(nav.iloc[-1]) ** (1 / yrs) - 1
    mdd = float((nav / nav.cummax() - 1).min())
    downside = r_d[r_d < 0].std()
    sortino = float(r_d.mean() / downside * np.sqrt(252)) if downside and downside > 0 else np.nan
    mrets = nav.groupby(nav.index.astype(str).str.slice(0, 7)).last().pct_change().dropna()
    p5, p50, p95 = boot_cagr_lb(mrets, rng)
    return {"arm": name, "oos_cagr": round(cagr, 3), "sortino": round(sortino, 2),
            "mdd": round(mdd, 3), "boot_p5": round(p5, 3), "boot_p50": round(p50, 3),
            "months": len(mrets)}


def serenity_nav(label: str) -> pd.Series:
    daily = pd.read_csv(RESULTS / f"{label}_ev_v2_thesis_inst_daily.csv", parse_dates=["date"])
    return daily.set_index("date")["nav"]


def serenity_grid_nav(label: str) -> pd.Series:
    cand = sorted(RESULTS.glob(f"{label}_g_*_daily.csv"))
    daily = pd.read_csv(cand[0], parse_dates=["date"])
    return daily.set_index("date")["nav"]


def evergreen_nav() -> pd.Series:
    nav = pl.read_parquet(REPO_ROOT / "src/quantlib/evergreen/data/ev36_top1_nav.parquet").to_pandas()
    nav["date"] = pd.to_datetime(nav["date"])
    return nav.set_index("date")["nav"]


def s_strategy_nav() -> pd.Series:
    from quantlib.apex.experiments.chart_s_vs_benchmarks import prep, run_s  # type: ignore
    from quantlib.apex import data as apex_data  # type: ignore
    con = apex_data.connect()
    try:
        panel, feat, elig = prep(con)
    finally:
        con.close()
    nav = run_s(panel, feat, elig, start="2022-07-11").to_pandas()
    nav["date"] = pd.to_datetime(nav["date"])
    return nav.set_index("date")["nav"]


def bench_0050() -> pd.Series:
    from db import connect
    from prices import total_return_series
    con = connect()
    try:
        nav = total_return_series(con, "0050", "2025-07-01", "2026-07-03", market="twse").to_pandas()
    finally:
        con.close()
    nav["date"] = pd.to_datetime(nav["date"])
    return nav.set_index("date")["adj_close"]


def main() -> None:
    rng = np.random.default_rng(20260716)
    arms: dict[str, pd.Series] = {}
    arms["Evergreen(EV36 top-1)"] = evergreen_nav()
    arms["Serenity 現役(champion)"] = serenity_nav("b18_full_incumbent")
    top1 = RESULTS / "b18_full_newtop1"
    if list(RESULTS.glob("b18_full_newtop1_*daily.csv")):
        arms["Serenity 極限(b18 top-1)"] = serenity_grid_nav("b18_full_newtop1")
    try:
        arms["S(apex_revcycle_S)"] = s_strategy_nav()
    except Exception as exc:
        print(f"⚠ S 策略 NAV 取得失敗:{exc}")
    arms["0050(對照)"] = bench_0050()

    rows = [metrics_row(name, oos_slice(nav), rng) for name, nav in arms.items()]
    df = pd.DataFrame(rows).sort_values("boot_p5", ascending=False)
    out = Path(__file__).parent / "grand_duel_report.md"
    lines = ["# 四方對決 — 同 OOS 窗 2025-07-11~2026-07-03(全窗一跑、OOS 段內歸一、同尺)",
             "", df.to_markdown(index=False), ""]
    out.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"report -> {out}")


if __name__ == "__main__":
    main()
