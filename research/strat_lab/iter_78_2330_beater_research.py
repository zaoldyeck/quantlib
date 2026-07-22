"""iter_78 - concentrated leadership search against 2330 buy-and-hold.

The benchmark is 2330 total-return buy-and-hold over the same adjusted-price
window.  A candidate is not considered useful unless it can beat 2330 on OOS
CAGR, not just on short recent bursts.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path

import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))
from iter_75_dynamic_industry_leadership import (  # noqa: E402
    CAPITAL,
    START,
    DynamicIndustryConfig,
    build_candidates,
    build_row_store,
    load_or_build_panel,
    latest_trading_day,
    market_risk_flags,
    run_strategy,
)
from validator import validate_daily_nav  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from research.db import connect  # noqa: E402
from research.prices import fetch_adjusted_panel  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")


def build_2330_benchmark(con, start: date, end: date) -> tuple[pl.DataFrame, dict[str, object]]:
    px = (
        fetch_adjusted_panel(
            con,
            start.isoformat(),
            end.isoformat(),
            codes=["2330"],
            market="twse",
            include_extra_history_days=320,
        )
        .filter(pl.col("date") >= start)
        .sort("date")
    )
    if px.is_empty():
        raise RuntimeError("no adjusted 2330 data")
    base = float(px["close"][0])
    daily = px.select(["date", (CAPITAL * pl.col("close") / base).alias("nav")])
    metrics = validate_daily_nav("2330_buyhold_total_return", daily, n_trials=1)
    return daily, metrics


def build_configs(preset: str = "full") -> list[DynamicIndustryConfig]:
    configs: list[DynamicIndustryConfig] = []
    if preset not in {"full", "focused"}:
        raise ValueError(f"unknown preset: {preset}")
    score_kinds = ("price", "quality", "revenue", "balanced") if preset == "full" else ("price", "quality", "revenue")
    max_position_grid = (1, 2, 3, 4, 5) if preset == "full" else (2, 3, 4, 5)
    threshold_count = 3 if preset == "full" else 2
    score_thresholds = {
        "price": (0.30, 0.45, 0.60),
        "quality": (0.24, 0.36, 0.50),
        "revenue": (0.30, 0.45, 0.60),
        "balanced": (0.30, 0.45, 0.60),
    }
    for score_kind in score_kinds:
        thresholds = score_thresholds[score_kind][:threshold_count]
        for max_positions in max_position_grid:
            industry_caps = sorted({1, min(2, max_positions), max_positions})
            for industry_topn in (3, 5):
                for industry_cap in industry_caps:
                    for threshold in thresholds:
                        for risk_mode in ("none", "market_ma200_cash"):
                            name = (
                                f"iter78_2330beat_{score_kind}_pos{max_positions}"
                                f"_ind{industry_topn}_cap{industry_cap}_th{threshold:g}_{risk_mode}"
                            )
                            configs.append(
                                DynamicIndustryConfig(
                                    name=name,
                                    score_kind=score_kind,
                                    max_positions=max_positions,
                                    industry_topn=industry_topn,
                                    industry_cap=industry_cap,
                                    rebalance="weekly",
                                    min_stock_score=threshold,
                                    min_industry_score=0.05,
                                    min_adv=50_000_000.0,
                                    min_hold_days=10,
                                    exit_score=-0.05,
                                    exit_industry_rank=8,
                                    trail_mult=3.0,
                                    trail_min=0.08,
                                    trail_max=0.24,
                                    risk_mode=risk_mode,
                                )
                            )
    return configs


def run(max_configs: int | None, validate_top_n: int, preset: str) -> None:
    t0 = time.time()
    con = connect(read_only=True)
    try:
        end = latest_trading_day(con)
        panel, days = load_or_build_panel(con, START, end, use_cache=True)
        risk_off = market_risk_flags(con, end)
        benchmark_daily, benchmark = build_2330_benchmark(con, START, end)
    finally:
        con.close()

    configs = build_configs(preset)
    if max_configs is not None:
        configs = configs[:max_configs]
    n_trials = len(configs)
    store = build_row_store(panel)
    rows = []
    dailies: list[tuple[DynamicIndustryConfig, pl.DataFrame, pl.DataFrame, dict[str, object]]] = []
    print(
        f"[iter78] end={end} configs={len(configs)} "
        f"2330_oos={benchmark['oos_cagr']:+.2%} 2330_1y={benchmark['recent_1y_cagr']:+.2%}",
        flush=True,
    )
    for i, cfg in enumerate(configs, 1):
        candidates = build_candidates(panel, cfg)
        if not candidates:
            continue
        daily, holdings, stats = run_strategy(days, store, candidates, risk_off, cfg)
        row = validate_daily_nav(cfg.name, daily.select(["date", "nav"]), n_trials=n_trials, extra=stats)
        row.update(
            {
                "score_kind": cfg.score_kind,
                "max_positions_cfg": float(cfg.max_positions),
                "industry_topn": float(cfg.industry_topn),
                "industry_cap": float(cfg.industry_cap),
                "min_stock_score": float(cfg.min_stock_score),
                "risk_mode": cfg.risk_mode,
                "candidate_signal_days": float(len(candidates)),
                "bench_2330_oos_cagr": benchmark["oos_cagr"],
                "bench_2330_recent_1y_cagr": benchmark["recent_1y_cagr"],
                "excess_oos_cagr_vs_2330": float(row["oos_cagr"]) - float(benchmark["oos_cagr"]),
                "excess_recent_1y_cagr_vs_2330": float(row["recent_1y_cagr"])
                - float(benchmark["recent_1y_cagr"]),
                "beats_2330_oos_cagr": float(row["oos_cagr"]) > float(benchmark["oos_cagr"]),
            }
        )
        rows.append(row)
        dailies.append((cfg, daily, holdings, row))
        if i % 25 == 0 or bool(row["beats_2330_oos_cagr"]):
            print(
                f"[iter78] {i:03d}/{len(configs)} {cfg.name}: "
                f"OOS={row['oos_cagr']:+.2%} vs2330={row['excess_oos_cagr_vs_2330']:+.2%} "
                f"MDD={row['oos_mdd']:.2%} 1Y={row['recent_1y_cagr']:+.2%}",
                flush=True,
            )

    if not rows:
        raise RuntimeError("no iter78 candidates produced trades")

    summary = pl.DataFrame(rows).sort(
        ["beats_2330_oos_cagr", "excess_oos_cagr_vs_2330", "robust_growth_score"],
        descending=[True, True, True],
    )
    selected = set(summary.head(validate_top_n)["name"].to_list())
    RESULTS.mkdir(parents=True, exist_ok=True)
    benchmark_daily.write_csv(RESULTS / "benchmark_2330_buyhold_daily.csv")
    for cfg, daily, holdings, _ in dailies:
        if cfg.name in selected:
            daily.write_csv(RESULTS / f"{cfg.name}_daily.csv")
            holdings.write_csv(RESULTS / f"{cfg.name}_holdings.csv")

    out = RESULTS / "iter_78_2330_beater_summary.csv"
    summary.write_csv(out)
    cols = [
        "name",
        "score_kind",
        "max_positions_cfg",
        "oos_cagr",
        "bench_2330_oos_cagr",
        "excess_oos_cagr_vs_2330",
        "recent_1y_cagr",
        "oos_sortino",
        "oos_mdd",
        "oos_cdar_95",
        "boot_cagr_lb",
        "dsr",
        "pbo",
        "robust_growth_score",
        "max_active",
    ]
    print("\niter_78 2330 beater search")
    print(summary.select([c for c in cols if c in summary.columns]).head(25).to_pandas().to_string(index=False))
    print(f"\nSaved: {out}")
    print(f"[iter78] elapsed={time.time() - t0:.1f}s")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-configs", type=int, default=None)
    ap.add_argument("--validate-top-n", type=int, default=20)
    ap.add_argument("--preset", choices=["full", "focused"], default="full")
    args = ap.parse_args()
    run(args.max_configs, args.validate_top_n, args.preset)


if __name__ == "__main__":
    main()
