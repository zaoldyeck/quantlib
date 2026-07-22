"""Iter102 - backtest spike-factor signals as monthly target books."""

from __future__ import annotations

import math
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
STRAT_LAB = RESEARCH_ROOT / "strat_lab"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(STRAT_LAB))

from research.constants import CAPITAL  # noqa: E402
from research.db import connect  # noqa: E402
from evaluation import nav_metrics  # noqa: E402
from experiments.spike_factor_analysis import build_labeled_monthly, load_panel  # noqa: E402
from iter_32_first_principles import COMMISSION, SELL_TAX  # noqa: E402
from research.prices import total_return_series  # noqa: E402
from validator import recent_one_year_metrics  # noqa: E402


START = date(2012, 1, 3)
RESULTS = REPO_ROOT / f"{paths.OUT_STRAT_LAB}"
OUT_PREFIX = "iter_102_spike_factor_signal"


def latest_0050_day() -> date:
    con = connect(read_only=True)
    try:
        return con.sql("SELECT MAX(date) FROM daily_quote WHERE market='twse' AND company_code='0050'").fetchone()[0]
    finally:
        con.close()


def signal_score_expr() -> pl.Expr:
    return (
        0.28 * pl.col("near_52w_high_pct").fill_null(0.0)
        + 0.24 * pl.col("ret60_pct").fill_null(0.0)
        + 0.18 * pl.col("volume_surge_60d_pct").fill_null(0.0)
        + 0.14 * pl.col("latest_yoy_pct").fill_null(0.0)
        + 0.10 * pl.col("inst_flow20_pct").fill_null(0.0)
        + 0.06 * pl.col("roa_ttm_pct").fill_null(0.0)
    )


def condition(name: str) -> pl.Expr:
    if name == "revenue_momentum":
        return (
            (pl.col("latest_yoy") >= 30)
            & (pl.col("yoy_delta").fill_null(-999) > 0)
            & (pl.col("ret60_pct") >= 0.80)
            & (pl.col("near_52w_high") >= 0.80)
        )
    if name == "momentum_new_high":
        return (pl.col("near_52w_high") >= 0.90) & (pl.col("ret60_pct") >= 0.80) & (pl.col("rsv_60d") >= 0.80)
    if name == "volume_breakout":
        return (pl.col("volume_surge_60d") >= 1.50) & (pl.col("ret20") >= 0.10) & (pl.col("near_52w_high") >= 0.90)
    if name == "quality_growth_breakout":
        return (pl.col("latest_yoy") >= 20) & (pl.col("roa_ttm") >= 0.08) & (pl.col("f_score_raw") >= 4) & (pl.col("ret120_pct") >= 0.80)
    if name == "spike_setup_blend":
        return (
            ((pl.col("latest_yoy") >= 20) | (pl.col("latest_yoy_pct") >= 0.80))
            & (pl.col("ret60_pct") >= 0.80)
            & (pl.col("near_52w_high") >= 0.85)
            & (pl.col("volume_surge_60d_pct") >= 0.65)
        )
    raise ValueError(name)


def build_targets(labeled: pl.DataFrame, name: str, topn: int) -> dict[date, list[str]]:
    picks = (
        labeled.with_columns(signal_score_expr().alias("signal_score"))
        .filter(condition(name))
        .sort(["date", "signal_score"], descending=[False, True])
        .group_by("date", maintain_order=True)
        .head(topn)
        .select(["date", "company_code", "signal_score"])
    )
    return {
        key[0] if isinstance(key, tuple) else key: g["company_code"].to_list()
        for key, g in picks.group_by("date", maintain_order=True)
    }


def simulate(panel: pl.DataFrame, days: list[date], targets: dict[date, list[str]], name: str) -> tuple[dict[str, object], pl.DataFrame]:
    daily_rets = (
        panel.sort(["company_code", "date"])
        .with_columns((pl.col("close") / pl.col("close").shift(1).over("company_code") - 1.0).fill_null(0.0).alias("ret"))
        .select(["date", "company_code", "ret"])
    )
    ret_lookup = {
        (r["date"], r["company_code"]): float(r["ret"] or 0.0)
        for r in daily_rets.iter_rows(named=True)
    }
    nav = CAPITAL
    weights: dict[str, float] = {}
    rows: list[dict[str, object]] = []
    active_counts: list[int] = []
    turnover_sum = 0.0
    rebalance_count = 0
    for day in days:
        nav *= 1.0 + sum(w * ret_lookup.get((day, code), 0.0) for code, w in weights.items())
        turnover = 0.0
        cost = 0.0
        if day in targets:
            codes = targets[day]
            next_weights = {code: 1.0 / len(codes) for code in codes} if codes else {}
            all_codes = set(weights) | set(next_weights)
            buy_turnover = sum(max(next_weights.get(code, 0.0) - weights.get(code, 0.0), 0.0) for code in all_codes)
            sell_turnover = sum(max(weights.get(code, 0.0) - next_weights.get(code, 0.0), 0.0) for code in all_codes)
            turnover = buy_turnover + sell_turnover
            cost = nav * (buy_turnover * COMMISSION + sell_turnover * (COMMISSION + SELL_TAX))
            nav -= cost
            weights = next_weights
            turnover_sum += turnover
            rebalance_count += 1
        active_counts.append(len(weights))
        rows.append({"date": day, "nav": nav, "position_count": len(weights), "turnover": turnover, "cost": cost})
    daily = pl.DataFrame(rows)
    row = nav_metrics(daily.select(["date", "nav"]))
    row.update(recent_one_year_metrics(daily.select(["date", "nav"]), capital=CAPITAL))
    row.update({
        "name": name,
        "rebalance_count": rebalance_count,
        "avg_positions": float(np.mean(active_counts)) if active_counts else 0.0,
        "avg_monthly_turnover": turnover_sum / max(rebalance_count, 1),
        "final_nav": float(nav),
    })
    return row, daily


def benchmark(code: str, label: str, start: date, end: date) -> tuple[dict[str, object], pl.DataFrame]:
    con = connect(read_only=True)
    try:
        s = total_return_series(con, code, start.isoformat(), end.isoformat(), market="twse").sort("date")
    finally:
        con.close()
    daily = s.select(["date", "adj_close"]).with_columns(
        (CAPITAL * pl.col("adj_close") / pl.col("adj_close").first()).alias("nav")
    ).select(["date", "nav"])
    row = nav_metrics(daily)
    row.update(recent_one_year_metrics(daily, capital=CAPITAL))
    row.update({"name": label, "final_nav": float(daily["nav"][-1])})
    return row, daily


def main() -> None:
    t0 = time.time()
    end = latest_0050_day()
    panel, days = load_panel(START, end)
    labeled = build_labeled_monthly(panel, days)
    # Use only dates with complete forward-60 labels for honest signal calibration.
    sim_end = labeled["date"].max()
    sim_days = [d for d in days if START <= d <= sim_end]
    panel = panel.filter(pl.col("date") <= sim_end)
    print(f"[iter102] simulate {START} -> {sim_end}, monthly rows={labeled.height:,}", flush=True)

    rows: list[dict[str, object]] = []
    for name in ["revenue_momentum", "momentum_new_high", "volume_breakout", "quality_growth_breakout", "spike_setup_blend"]:
        for topn in [5, 10, 20]:
            strategy = f"{name}_top{topn}"
            targets = build_targets(labeled, name, topn)
            row, daily = simulate(panel, sim_days, targets, strategy)
            path = RESULTS / f"{OUT_PREFIX}_{strategy}_daily.csv"
            daily.write_csv(path)
            row["daily_path"] = str(path)
            rows.append(row)
            print(
                f"  {strategy}: CAGR={float(row['cagr']):+.2%} recent1Y={float(row['recent_1y_cagr']):+.2%} "
                f"MDD={float(row['mdd']):+.2%} avg_pos={float(row['avg_positions']):.1f}",
                flush=True,
            )

    for code, label in [("0050", "0050 TR"), ("2330", "2330 TR")]:
        row, daily = benchmark(code, label, START, sim_end)
        path = RESULTS / f"{OUT_PREFIX}_{code}_benchmark_daily.csv"
        daily.write_csv(path)
        row["daily_path"] = str(path)
        rows.append(row)

    summary = pl.DataFrame(rows).sort("cagr", descending=True)
    out = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.write_csv(out)
    print("\n" + "=" * 100)
    print(f"Iter102 spike-factor signal backtest ({START} -> {sim_end})")
    print("=" * 100)
    with pl.Config(tbl_rows=25, tbl_width_chars=160):
        print(summary.select([
            "name",
            pl.col("cagr").mul(100).round(2).alias("cagr_pct"),
            pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
            pl.col("sortino").round(3),
            pl.col("sharpe").round(3),
            pl.col("mdd").mul(100).round(2).alias("mdd_pct"),
            pl.col("avg_positions").round(2),
            pl.col("avg_monthly_turnover").round(2),
        ]))
    print(f"\n[iter102] wrote {out}")
    print(f"[iter102] runtime {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
