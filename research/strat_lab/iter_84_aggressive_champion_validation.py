"""iter_84 - validate and refine the aggressive Iter83 champion.

The best Iter83 paper candidate by OOS CAGR is not the conservative all-ETF
winner. This script validates that aggressive candidate directly, then performs
a focused refinement sweep around its parameter neighborhood.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))

from iter_40_research_campaign import CAPITAL, metrics_from_rets, simulate, validate_daily  # noqa: E402
from iter_64_active_etf_beater_confirm import load_active_etfs, window_metrics  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_82_oos_recent_pm_allocator import (  # noqa: E402
    RECENT_TARGETS_PATH,
    build_hierarchical_books,
    build_price_lookup,
    expand_book_targets,
    load_execution_targets,
)
from iter_83_blended_oos_recent_allocator import (  # noqa: E402
    INNER_SPECS,
    ITER82_TRIALS,
    MAX_POSITIONS,
    BlendSpec,
    build_oos_sleeves,
    build_specs as build_iter83_specs,
    combine_targets,
    weight_path,
)
from iter_79_lagged_hierarchical_executor import benchmark_2330  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_84_aggressive_champion_validation"

AGGRESSIVE = BlendSpec(
    inner_idx=0,
    mode="dynamic",
    lookback=21,
    margin=0.00,
    schedule="weekly",
    min_hold_days=40,
    confirm_days=1,
    low_recent_weight=0.0,
    high_recent_weight=1.0,
    default_recent_weight=0.75,
)

CONSERVATIVE = BlendSpec(
    inner_idx=0,
    mode="dynamic",
    lookback=21,
    margin=0.02,
    schedule="monthly",
    min_hold_days=5,
    confirm_days=1,
    low_recent_weight=0.0,
    high_recent_weight=1.0,
    default_recent_weight=0.75,
)


def build_refinement_specs() -> list[BlendSpec]:
    specs = []
    seen = set()
    inner_idxs = (0, 1, 5)
    schedules = ("weekly", "monthly")
    lookbacks = (10, 15, 21, 31, 42)
    margins = (-0.02, -0.01, 0.00, 0.01, 0.02)
    holds = (20, 30, 40, 60)
    confirms = (1, 2)
    weight_sets = (
        (0.0, 1.0, 0.75),
        (0.0, 0.90, 0.75),
        (0.0, 0.75, 0.75),
        (0.25, 1.0, 0.75),
        (0.25, 0.90, 0.75),
    )
    for inner_idx in inner_idxs:
        for schedule in schedules:
            for lookback in lookbacks:
                for margin in margins:
                    for hold in holds:
                        for confirm in confirms:
                            for low_w, high_w, default_w in weight_sets:
                                spec = BlendSpec(
                                    inner_idx=inner_idx,
                                    mode="dynamic",
                                    lookback=lookback,
                                    margin=margin,
                                    schedule=schedule,
                                    min_hold_days=hold,
                                    confirm_days=confirm,
                                    low_recent_weight=low_w,
                                    high_recent_weight=high_w,
                                    default_recent_weight=default_w,
                                )
                                if spec.name not in seen:
                                    seen.add(spec.name)
                                    specs.append(spec)
    return specs


def build_inputs() -> tuple[
    list[date],
    dict[tuple[date, str], tuple[float, float]],
    list[tuple[dict[date, dict[str, float]], dict[date, dict[str, float]], pl.DataFrame]],
    dict[date, dict[str, float]],
    pl.DataFrame,
    dict[str, object],
    dict[str, pl.DataFrame],
]:
    days, panel, books_by_name, _state = build_hierarchical_books()
    source_books = books_by_name["iter67_hierarchical"]
    recent_targets = load_execution_targets(RECENT_TARGETS_PATH)
    codes = {code for book in source_books.values() for code in book} | {code for book in recent_targets.values() for code in book}
    price_lookup = build_price_lookup(panel, codes)
    oos_sleeves = build_oos_sleeves(days, source_books, price_lookup)
    recent_books = expand_book_targets(days, recent_targets)
    recent_daily, _recent_stats = simulate(days, price_lookup, recent_targets, {d: 1.0 for d in days}, persist=True)
    _bench_daily, benchmark = benchmark_2330(days)
    etfs = load_active_etfs(days[0], days[-1])
    return days, price_lookup, oos_sleeves, recent_books, recent_daily, benchmark, etfs


def run_spec(
    days: list[date],
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    oos_sleeves: list[tuple[dict[date, dict[str, float]], dict[date, dict[str, float]], pl.DataFrame]],
    recent_books: dict[date, dict[str, float]],
    recent_daily: pl.DataFrame,
    spec: BlendSpec,
) -> tuple[pl.DataFrame, dict[str, float], dict[date, dict[str, float]], dict[str, object]]:
    _oos_targets, oos_books, oos_daily = oos_sleeves[spec.inner_idx]
    base = (
        oos_daily.select(["date", pl.col("nav").alias("nav_oos")])
        .join(recent_daily.select(["date", pl.col("nav").alias("nav_recent")]), on="date", how="inner")
        .sort("date")
    )
    weights, switches, avg_recent_weight = weight_path(base, spec)
    targets = combine_targets(days, oos_books, recent_books, weights)
    daily, stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
    meta = {
        "avg_recent_weight": avg_recent_weight,
        "allocator_switches": switches,
        "target_rebalance_days": len(targets),
    }
    return daily, stats, targets, meta


def period_metrics(daily: pl.DataFrame, periods: list[tuple[str, date, date]]) -> pl.DataFrame:
    rows = []
    daily = daily.sort("date")
    for name, start, end in periods:
        sub = daily.filter((pl.col("date") >= start) & (pl.col("date") <= end))
        if sub.height < 2:
            continue
        nav = sub["nav"].to_numpy()
        dates = sub["date"].to_list()
        rets = np.diff(np.concatenate([[nav[0]], nav])) / np.concatenate([[nav[0]], nav[:-1]])
        m = metrics_from_rets(rets, dates)
        rows.append(
            {
                "period": name,
                "start": dates[0].isoformat(),
                "end": dates[-1].isoformat(),
                "days": sub.height,
                "cagr": m["cagr"],
                "sortino": m["sortino"],
                "mdd": m["mdd"],
                "total_return": float(nav[-1] / nav[0] - 1.0),
            }
        )
    return pl.DataFrame(rows)


def calendar_year_metrics(daily: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for year, sub in daily.sort("date").with_columns(pl.col("date").dt.year().alias("year")).group_by("year", maintain_order=True):
        year_value = int(year[0] if isinstance(year, tuple) else year)
        if sub.height < 2:
            continue
        nav = sub["nav"].to_numpy()
        ret = float(nav[-1] / nav[0] - 1.0)
        peak = np.maximum.accumulate(nav)
        mdd = float((nav / peak - 1.0).min())
        rows.append({"year": year_value, "return": ret, "mdd": mdd, "days": sub.height})
    return pl.DataFrame(rows).sort("year")


def rolling_metrics(daily: pl.DataFrame, windows: tuple[int, ...] = (252, 756)) -> pl.DataFrame:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    rows = []
    for window in windows:
        vals = []
        for i in range(window, len(nav)):
            years = max((dates[i] - dates[i - window]).days / 365.25, 1e-9)
            vals.append((nav[i] / nav[i - window]) ** (1.0 / years) - 1.0)
        arr = np.asarray(vals, dtype=float)
        rows.append(
            {
                "window_trading_days": window,
                "count": len(arr),
                "min_cagr": float(arr.min()) if len(arr) else 0.0,
                "p05_cagr": float(np.quantile(arr, 0.05)) if len(arr) else 0.0,
                "median_cagr": float(np.median(arr)) if len(arr) else 0.0,
                "p95_cagr": float(np.quantile(arr, 0.95)) if len(arr) else 0.0,
            }
        )
    return pl.DataFrame(rows)


def stressed_daily(daily: pl.DataFrame, extra_cost_per_turnover: float) -> pl.DataFrame:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    turnover = daily["turnover"].to_numpy()
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    stressed_rets = rets - turnover * extra_cost_per_turnover
    stressed_nav = CAPITAL * np.cumprod(1.0 + stressed_rets)
    return pl.DataFrame({"date": dates, "nav": stressed_nav, "turnover": turnover})


def stress_table(daily: pl.DataFrame, n_trials: int, stats: dict[str, float]) -> pl.DataFrame:
    rows = []
    for bps in (0, 5, 10, 25, 50, 100):
        extra = bps / 10_000.0
        stressed = stressed_daily(daily, extra)
        m = validate_daily(f"extra_cost_{bps}bps", stressed, n_trials, stats)
        recent = window_metrics(stressed, 365)
        rows.append(
            {
                "extra_cost_bps_per_turnover": bps,
                "full_cagr": m["cagr"],
                "oos_cagr": m["oos_cagr"],
                "oos_sortino": m["oos_sortino"],
                "oos_mdd": m["oos_mdd"],
                "recent_1y_cagr": recent["recent_1y_cagr"],
            }
        )
    return pl.DataFrame(rows)


def evaluate_detailed_local(
    row: dict[str, object],
    daily: pl.DataFrame,
    stats: dict[str, float],
    n_trials: int,
    etfs: dict[str, pl.DataFrame],
    benchmark: dict[str, object],
) -> tuple[dict[str, object], pl.DataFrame]:
    focused = validate_daily(str(row["name"]), daily, n_trials, stats)
    recent = window_metrics(daily, 365)
    active_summary, active_rows = compare_active_etfs_local(str(row["name"]), daily, etfs)
    out = {
        **row,
        **focused,
        "cumulative_dsr": focused["dsr"],
        **recent,
        **active_summary,
        "excess_oos_cagr_vs_2330": float(focused["oos_cagr"]) - float(benchmark["oos_cagr"]),
        "excess_recent_1y_cagr_vs_2330": float(recent["recent_1y_cagr"]) - float(benchmark["recent_1y_cagr"]),
    }
    out["strict_promotable"] = (
        float(out["cumulative_dsr"]) >= 0.95
        and float(out["pbo"]) < 0.50
        and float(out["boot_cagr_lb"]) > 0.10
        and float(out["oos_mdd"]) > -0.45
        and float(out["max_active"]) <= MAX_POSITIONS
        and float(out["active_etf_wins"]) == float(out["active_etf_count"])
        and float(out["excess_oos_cagr_vs_2330"]) > 0.0
    )
    daily_path = RESULTS / f"{OUT_PREFIX}_{out['name']}_daily.csv"
    if out["strict_promotable"] or float(out["excess_oos_cagr_vs_2330"]) > 0.05:
        daily.write_csv(daily_path)
        out["path"] = str(daily_path)
    else:
        out["path"] = ""
    return out, pl.DataFrame(active_rows)


def compare_active_etfs_local(name: str, daily: pl.DataFrame, etfs: dict[str, pl.DataFrame]) -> tuple[dict[str, float | str], list[dict[str, float | str]]]:
    rows = []
    for code, etf in etfs.items():
        joined = (
            daily.sort("date")
            .select(["date", "nav"])
            .join(etf, on="date", how="inner")
            .sort("date")
        )
        if joined.height < 2:
            continue
        strat_total = float(joined["nav"][-1] / joined["nav"][0] - 1.0)
        etf_total = float(joined["adj_close"][-1] / joined["adj_close"][0] - 1.0)
        gap = strat_total - etf_total
        rows.append(
            {
                "strategy": name,
                "etf": code,
                "start": joined["date"][0].isoformat(),
                "end": joined["date"][-1].isoformat(),
                "days": float(joined.height),
                "strategy_total_return": strat_total,
                "etf_total_return": etf_total,
                "gap": gap,
                "win": bool(gap > 0),
            }
        )
    if not rows:
        return {
            "active_etf_count": 0.0,
            "active_etf_wins": 0.0,
            "active_etf_avg_gap": 0.0,
            "active_etf_min_gap": 0.0,
            "active_etf_losses": "",
        }, rows
    gaps = np.array([float(r["gap"]) for r in rows])
    losses = [str(r["etf"]) for r in rows if not bool(r["win"])]
    return {
        "active_etf_count": float(len(rows)),
        "active_etf_wins": float(sum(bool(r["win"]) for r in rows)),
        "active_etf_avg_gap": float(gaps.mean()),
        "active_etf_min_gap": float(gaps.min()),
        "active_etf_losses": ",".join(losses),
    }, rows


def target_diagnostics(targets: dict[date, dict[str, float]]) -> pl.DataFrame:
    rows = []
    for d, book in sorted(targets.items()):
        rows.append(
            {
                "date": d.isoformat(),
                "names": len(book),
                "gross": sum(book.values()),
                "top_weight": max(book.values()) if book else 0.0,
                "top_code": max(book.items(), key=lambda kv: kv[1])[0] if book else "",
                "codes": ",".join(book.keys()),
            }
        )
    return pl.DataFrame(rows)


def screen_row(daily: pl.DataFrame, stats: dict[str, float], meta: dict[str, object], spec: BlendSpec, benchmark: dict[str, object]) -> dict[str, object]:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    full = metrics_from_rets(rets, dates)
    frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = frame.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_metrics = metrics_from_rets(oos["ret"].to_numpy(), oos["date"].to_list())
    recent = window_metrics(daily, 365)
    return {
        "name": spec.name,
        "inner_idx": spec.inner_idx,
        "schedule": spec.schedule,
        "lookback": spec.lookback,
        "margin": spec.margin,
        "min_hold_days": spec.min_hold_days,
        "confirm_days": spec.confirm_days,
        "low_recent_weight": spec.low_recent_weight,
        "high_recent_weight": spec.high_recent_weight,
        "avg_recent_weight": meta["avg_recent_weight"],
        "allocator_switches": meta["allocator_switches"],
        "target_rebalance_days": meta["target_rebalance_days"],
        **full,
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_sharpe": oos_metrics["sharpe"],
        "oos_mdd": oos_metrics["mdd"],
        **stats,
        **recent,
        "excess_oos_cagr_vs_2330": float(oos_metrics["cagr"]) - float(benchmark["oos_cagr"]),
    }


def main() -> None:
    days, price_lookup, oos_sleeves, recent_books, recent_daily, benchmark, etfs = build_inputs()
    n_trials = ITER67_CUMULATIVE_TRIALS + ITER82_TRIALS + len(build_iter83_specs()) + len(build_refinement_specs())
    print(f"[iter84] n_trials={n_trials} data_end={days[-1]}", flush=True)

    validation_rows = []
    active_frames = []
    diagnostics = {}
    for label, spec in [("aggressive_4021", AGGRESSIVE), ("conservative_3715", CONSERVATIVE)]:
        daily, stats, targets, meta = run_spec(days, price_lookup, oos_sleeves, recent_books, recent_daily, spec)
        base_row = {
            "name": spec.name,
            "label": label,
            "inner_idx": spec.inner_idx,
            "schedule": spec.schedule,
            "lookback": spec.lookback,
            "margin": spec.margin,
            "min_hold_days": spec.min_hold_days,
            "confirm_days": spec.confirm_days,
            "low_recent_weight": spec.low_recent_weight,
            "high_recent_weight": spec.high_recent_weight,
            **meta,
        }
        detailed, active = evaluate_detailed_local(base_row, daily, stats, n_trials, etfs, benchmark)
        validation_rows.append(detailed)
        active_frames.append(active)
        daily.write_csv(RESULTS / f"{OUT_PREFIX}_{label}_daily.csv")
        period_metrics(
            daily,
            [
                ("full", days[0], days[-1]),
                ("oos_2010_2025", date(2010, 1, 1), date(2025, 12, 31)),
                ("pre_covid_2010_2019", date(2010, 1, 1), date(2019, 12, 31)),
                ("covid_post_2020_2023", date(2020, 1, 1), date(2023, 12, 31)),
                ("recent_2024_latest", date(2024, 1, 1), days[-1]),
            ],
        ).write_csv(RESULTS / f"{OUT_PREFIX}_{label}_periods.csv")
        calendar_year_metrics(daily).write_csv(RESULTS / f"{OUT_PREFIX}_{label}_calendar_year.csv")
        rolling_metrics(daily).write_csv(RESULTS / f"{OUT_PREFIX}_{label}_rolling.csv")
        stress_table(daily, n_trials, stats).write_csv(RESULTS / f"{OUT_PREFIX}_{label}_cost_stress.csv")
        target_diagnostics(targets).write_csv(RESULTS / f"{OUT_PREFIX}_{label}_target_diagnostics.csv")
        diagnostics[label] = (daily, stats, targets)
        print(
            f"[iter84 validate] {label} OOS={detailed['oos_cagr']:+.2%} "
            f"1Y={detailed['recent_1y_cagr']:+.2%} DSR={detailed['cumulative_dsr']:.3f} "
            f"PBO={detailed['pbo']:.3f} wins={detailed['active_etf_wins']:.0f}/{detailed['active_etf_count']:.0f}",
            flush=True,
        )

    validation = pl.DataFrame(validation_rows).sort(["oos_cagr", "oos_sortino"], descending=[True, True])
    validation_path = RESULTS / f"{OUT_PREFIX}_champion_validation_summary.csv"
    validation.write_csv(validation_path)
    pl.concat(active_frames, how="vertical").write_csv(RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv")

    specs = build_refinement_specs()
    print(f"[iter84 sweep] specs={len(specs)}", flush=True)
    screen_rows = []
    saved: dict[str, tuple[BlendSpec, pl.DataFrame, dict[str, float], dict[str, object]]] = {}
    for i, spec in enumerate(specs, 1):
        daily, stats, _targets, meta = run_spec(days, price_lookup, oos_sleeves, recent_books, recent_daily, spec)
        row = screen_row(daily, stats, meta, spec, benchmark)
        screen_rows.append(row)
        if row["oos_cagr"] >= 0.37 or row["recent_1y_cagr"] >= 2.5:
            saved[spec.name] = (spec, daily, stats, row)
        if i % 500 == 0 or i == len(specs):
            best = max(float(r["oos_cagr"]) for r in screen_rows)
            print(f"[iter84 sweep] {i:04d}/{len(specs)} best_oos={best:+.2%}", flush=True)

    fast = pl.DataFrame(screen_rows)
    fast_path = RESULTS / f"{OUT_PREFIX}_refinement_fast_screen.csv"
    fast.sort(["oos_cagr", "oos_sortino", "recent_1y_cagr"], descending=[True, True, True]).write_csv(fast_path)

    detailed_names = set()
    for cols, descending, n in [
        (["oos_cagr", "oos_sortino"], [True, True], 120),
        (["recent_1y_cagr", "oos_cagr"], [True, True], 80),
        (["oos_sortino", "oos_cagr"], [True, True], 80),
    ]:
        detailed_names.update(fast.sort(cols, descending=descending).head(n)["name"].to_list())
    detail_rows = []
    detail_active = []
    for i, name in enumerate(sorted(detailed_names), 1):
        spec = next(s for s in specs if s.name == name)
        if name in saved:
            _spec, daily, stats, fast_row = saved[name]
        else:
            daily, stats, _targets, meta = run_spec(days, price_lookup, oos_sleeves, recent_books, recent_daily, spec)
            fast_row = screen_row(daily, stats, meta, spec, benchmark)
        detailed, active = evaluate_detailed_local(fast_row, daily, stats, n_trials, etfs, benchmark)
        detailed["beats_aggressive_oos"] = float(detailed["oos_cagr"]) > float(validation["oos_cagr"][0])
        if detailed["strict_promotable"] or detailed["beats_aggressive_oos"]:
            daily.write_csv(RESULTS / f"{OUT_PREFIX}_{name}_daily.csv")
        detail_rows.append(detailed)
        detail_active.append(active)
        print(
            f"[iter84 detail] {i:03d}/{len(detailed_names)} OOS={detailed['oos_cagr']:+.2%} "
            f"1Y={detailed['recent_1y_cagr']:+.2%} DSR={detailed['cumulative_dsr']:.3f} "
            f"PBO={detailed['pbo']:.3f} wins={detailed['active_etf_wins']:.0f}/{detailed['active_etf_count']:.0f}",
            flush=True,
        )

    detail = pl.DataFrame(detail_rows).sort(
        ["oos_cagr", "cumulative_dsr", "oos_sortino", "recent_1y_cagr"],
        descending=[True, True, True, True],
    )
    detail_path = RESULTS / f"{OUT_PREFIX}_refinement_summary.csv"
    detail.write_csv(detail_path)
    if detail_active:
        pl.concat(detail_active, how="vertical").write_csv(RESULTS / f"{OUT_PREFIX}_refinement_active_etf_comparison.csv")

    print("=" * 150)
    print("iter_84 aggressive champion validation and refinement")
    print("=" * 150)
    print(
        validation.select(
            [
                "label",
                "name",
                "strict_promotable",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("cumulative_dsr").round(3),
                pl.col("pbo").round(3),
                "max_active",
                "active_etf_wins",
                "active_etf_count",
            ]
        ).to_pandas().to_string(index=False)
    )
    print("\nTop refinement:")
    print(
        detail.head(20).select(
            [
                "name",
                "strict_promotable",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("cumulative_dsr").round(3),
                pl.col("pbo").round(3),
                "max_active",
                "active_etf_wins",
                "active_etf_count",
            ]
        ).to_pandas().to_string(index=False)
    )
    print(f"2330 benchmark OOS={benchmark['oos_cagr']:+.2%} recent1Y={benchmark['recent_1y_cagr']:+.2%}")
    print(f"Saved: {validation_path}")
    print(f"Saved: {fast_path}")
    print(f"Saved: {detail_path}")


if __name__ == "__main__":
    main()
