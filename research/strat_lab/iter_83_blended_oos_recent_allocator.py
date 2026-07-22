"""iter_83 - blended PM allocator over executable OOS/recent sleeves.

Iter82 promoted a hard switch between two executable target books. This pass
tests whether a PM allocator should allocate partially between the books instead
of switching all-or-nothing. It keeps the same implementation constraints:

* signals are prior NAV momentum only;
* target books are executed by the shared next-open simulator;
* the final book is long-only, no leverage, and capped at 10 names;
* detailed validation is run only after a fast screen to keep the sweep cheap.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))

from iter_40_research_campaign import CAPITAL, metrics_from_rets, simulate, validate_daily  # noqa: E402
from iter_57_cost_aware_switch import confirmed, scheduled_dates  # noqa: E402
from iter_64_active_etf_beater_confirm import compare_active_etfs, load_active_etfs, strict_dsr, window_metrics  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_68_position_level_bridge import Book, BookByDate  # noqa: E402
from iter_79_lagged_hierarchical_executor import benchmark_2330  # noqa: E402
from iter_81_hierarchical_pm_allocator import (  # noqa: E402
    ATTACK,
    BALANCED,
    AllocatorSpec as InnerAllocatorSpec,
    build_sleeve,
    merge_allocator_targets,
    state_path as inner_state_path,
)
from iter_82_oos_recent_pm_allocator import (  # noqa: E402
    RECENT_TARGETS_PATH,
    build_hierarchical_books,
    build_price_lookup,
    expand_book_targets,
    load_execution_targets,
)


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_83_blended_oos_recent_allocator"
MAX_POSITIONS = 10
ITER82_TRIALS = 480


INNER_SPECS = (
    InnerAllocatorSpec(lookback=42, margin=0.00, schedule="quarterly", min_hold_days=20, confirm_days=2),
    InnerAllocatorSpec(lookback=42, margin=0.00, schedule="quarterly", min_hold_days=40, confirm_days=2),
    InnerAllocatorSpec(lookback=63, margin=0.02, schedule="monthly", min_hold_days=60, confirm_days=1),
    InnerAllocatorSpec(lookback=63, margin=0.02, schedule="monthly", min_hold_days=20, confirm_days=1),
    InnerAllocatorSpec(lookback=21, margin=-0.02, schedule="monthly", min_hold_days=20, confirm_days=1),
    InnerAllocatorSpec(lookback=42, margin=-0.02, schedule="monthly", min_hold_days=40, confirm_days=1),
)


@dataclass(frozen=True)
class BlendSpec:
    inner_idx: int
    mode: str
    lookback: int
    margin: float
    schedule: str
    min_hold_days: int
    confirm_days: int
    low_recent_weight: float
    high_recent_weight: float
    default_recent_weight: float

    @property
    def name(self) -> str:
        if self.mode == "fixed":
            w = int(round(self.default_recent_weight * 100))
            return f"iter83_inner{self.inner_idx}_fixed_recent{w}"
        low = int(round(self.low_recent_weight * 100))
        high = int(round(self.high_recent_weight * 100))
        default = int(round(self.default_recent_weight * 100))
        margin = int(round(self.margin * 100))
        return (
            f"iter83_inner{self.inner_idx}_blend_{self.schedule}_lb{self.lookback}"
            f"_m{margin}_hold{self.min_hold_days}_c{self.confirm_days}"
            f"_w{low}_{high}_d{default}"
        )


def build_specs() -> list[BlendSpec]:
    specs: list[BlendSpec] = []
    fixed_weights = (0.0, 0.25, 0.50, 0.75, 1.0)
    dynamic_pairs = (
        (0.0, 1.0, 0.75),
        (0.0, 0.75, 0.75),
        (0.25, 1.0, 0.75),
        (0.25, 0.75, 0.75),
        (0.50, 1.0, 0.75),
    )
    for inner_idx in range(len(INNER_SPECS)):
        for w in fixed_weights:
            specs.append(
                BlendSpec(
                    inner_idx=inner_idx,
                    mode="fixed",
                    lookback=0,
                    margin=0.0,
                    schedule="fixed",
                    min_hold_days=0,
                    confirm_days=1,
                    low_recent_weight=w,
                    high_recent_weight=w,
                    default_recent_weight=w,
                )
            )
        for lookback in (21, 42, 63):
            for margin in (0.0, 0.02, 0.05):
                for schedule in ("weekly", "monthly"):
                    for hold in (5, 20, 40):
                        for confirm in (1, 2):
                            for low_w, high_w, default_w in dynamic_pairs:
                                specs.append(
                                    BlendSpec(
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
                                )
    return specs


def mix_book(oos: Book, recent: Book, recent_weight: float) -> Book:
    out: dict[str, float] = {}
    oos_weight = 1.0 - recent_weight
    for code, weight in oos.items():
        out[code] = out.get(code, 0.0) + oos_weight * weight
    for code, weight in recent.items():
        out[code] = out.get(code, 0.0) + recent_weight * weight
    out = {code: weight for code, weight in out.items() if weight > 1e-10}
    if len(out) <= MAX_POSITIONS:
        return out
    return dict(sorted(out.items(), key=lambda kv: (-kv[1], kv[0]))[:MAX_POSITIONS])


def weight_path(base: pl.DataFrame, spec: BlendSpec) -> tuple[dict[date, float], int, float]:
    dates = base["date"].to_list()
    if spec.mode == "fixed":
        return {d: spec.default_recent_weight for d in dates}, 0, spec.default_recent_weight

    work = base.with_columns(
        [
            pl.col("nav_recent").pct_change(spec.lookback).shift(1).fill_null(0.0).alias("mom_recent"),
            pl.col("nav_oos").pct_change(spec.lookback).shift(1).fill_null(0.0).alias("mom_oos"),
        ]
    )
    rel = (work["mom_recent"] - work["mom_oos"]).to_numpy().astype(float)
    recent_enter = confirmed(rel >= spec.margin, spec.confirm_days)
    oos_enter = confirmed(rel <= -spec.margin, spec.confirm_days)
    sched = scheduled_dates(work, spec.schedule)

    current = spec.default_recent_weight
    held = 10_000
    switches = 0
    weights: dict[date, float] = {}
    for i, d in enumerate(dates):
        new_weight = current
        if sched[i] and held >= spec.min_hold_days:
            if recent_enter[i]:
                new_weight = spec.high_recent_weight
            elif oos_enter[i]:
                new_weight = spec.low_recent_weight
        if abs(new_weight - current) > 1e-12:
            switches += 1
            held = 0
            current = new_weight
        else:
            held += 1
        weights[d] = current
    avg_recent_weight = float(np.mean(list(weights.values()))) if weights else 0.0
    return weights, switches, avg_recent_weight


def combine_targets(days: list[date], oos_books: dict[date, Book], recent_books: dict[date, Book], weights: dict[date, float]) -> BookByDate:
    out: BookByDate = {}
    last: Book | None = None
    for d in days:
        target = mix_book(oos_books[d], recent_books[d], weights[d])
        if last is None or target != last:
            out[d] = target
            last = target
    return out


def fast_metrics(daily: pl.DataFrame, stats: dict[str, float]) -> dict[str, float]:
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
        "cagr": full["cagr"],
        "sortino": full["sortino"],
        "mdd": full["mdd"],
        "final_nav": full["final_nav"],
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_mdd": oos_metrics["mdd"],
        "recent_1y_cagr": float(recent["recent_1y_cagr"]),
        "recent_1y_start": str(recent["recent_1y_start"]),
        "recent_1y_end": str(recent["recent_1y_end"]),
        "max_active": float(stats["max_active"]),
        "trade_days": float(stats["trade_days"]),
        "avg_turnover_trade_day": float(stats["avg_turnover_trade_day"]),
    }


def build_oos_sleeves(
    days: list[date],
    source_books: BookByDate,
    price_lookup: dict[tuple[date, str], tuple[float, float]],
) -> list[tuple[BookByDate, dict[date, Book], pl.DataFrame]]:
    attack_targets, attack_books, attack_daily, _attack_stats = build_sleeve(days, source_books, price_lookup, ATTACK)
    balanced_targets, balanced_books, balanced_daily, _balanced_stats = build_sleeve(days, source_books, price_lookup, BALANCED)
    base = (
        attack_daily.select(["date", pl.col("nav").alias("nav_attack")])
        .join(balanced_daily.select(["date", pl.col("nav").alias("nav_balanced")]), on="date", how="inner")
        .sort("date")
    )
    out = []
    for spec in INNER_SPECS:
        state, _switches = inner_state_path(base, spec)
        targets = merge_allocator_targets(days, attack_books, balanced_books, state)
        books = expand_book_targets(days, targets)
        daily, _stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
        out.append((targets, books, daily))
    return out


def evaluate_fast(
    days: list[date],
    price_lookup: dict[tuple[date, str], tuple[float, float]],
    oos_books: dict[date, Book],
    recent_books: dict[date, Book],
    base: pl.DataFrame,
    spec: BlendSpec,
    benchmark: dict[str, object],
) -> tuple[dict[str, object], pl.DataFrame, BookByDate]:
    weights, switches, avg_recent_weight = weight_path(base, spec)
    targets = combine_targets(days, oos_books, recent_books, weights)
    daily, stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
    row = {
        "name": spec.name,
        "inner_idx": spec.inner_idx,
        "inner_spec": INNER_SPECS[spec.inner_idx].name,
        "mode": spec.mode,
        "lookback": spec.lookback,
        "margin": spec.margin,
        "schedule": spec.schedule,
        "min_hold_days": spec.min_hold_days,
        "confirm_days": spec.confirm_days,
        "low_recent_weight": spec.low_recent_weight,
        "high_recent_weight": spec.high_recent_weight,
        "default_recent_weight": spec.default_recent_weight,
        "avg_recent_weight": avg_recent_weight,
        "allocator_switches": switches,
        "target_rebalance_days": len(targets),
        **fast_metrics(daily, stats),
    }
    row["excess_oos_cagr_vs_2330"] = float(row["oos_cagr"]) - float(benchmark["oos_cagr"])
    row["excess_recent_1y_cagr_vs_2330"] = float(row["recent_1y_cagr"]) - float(benchmark["recent_1y_cagr"])
    return row, daily, targets


def evaluate_detailed(
    row: dict[str, object],
    daily: pl.DataFrame,
    stats: dict[str, float],
    n_trials: int,
    etfs: dict[str, pl.DataFrame],
    benchmark: dict[str, object],
) -> tuple[dict[str, object], pl.DataFrame]:
    focused = validate_daily(str(row["name"]), daily, n_trials, stats)
    recent = window_metrics(daily, 365)
    active_summary, active_rows = compare_active_etfs(str(row["name"]), daily, etfs)
    out = {
        **row,
        **focused,
        "cumulative_dsr": strict_dsr(daily, n_trials),
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


def main() -> None:
    specs = build_specs()
    print(f"[iter83] specs={len(specs)} blended allocator over {len(INNER_SPECS)} OOS sleeves and Iter69 recent sleeve", flush=True)
    days, panel, books_by_name, _iter67_state = build_hierarchical_books()
    source_books = books_by_name["iter67_hierarchical"]
    recent_targets = load_execution_targets(RECENT_TARGETS_PATH)
    codes = {code for book in source_books.values() for code in book} | {code for book in recent_targets.values() for code in book}
    price_lookup = build_price_lookup(panel, codes)
    oos_sleeves = build_oos_sleeves(days, source_books, price_lookup)
    recent_books = expand_book_targets(days, recent_targets)
    recent_daily, _recent_stats = simulate(days, price_lookup, recent_targets, {d: 1.0 for d in days}, persist=True)
    _bench_daily, benchmark = benchmark_2330(days)

    fast_rows: list[dict[str, object]] = []
    detailed_inputs: dict[str, tuple[BlendSpec, pl.DataFrame, BookByDate, dict[str, float]]] = {}
    for i, spec in enumerate(specs, 1):
        _oos_targets, oos_books, oos_daily = oos_sleeves[spec.inner_idx]
        base = (
            oos_daily.select(["date", pl.col("nav").alias("nav_oos")])
            .join(recent_daily.select(["date", pl.col("nav").alias("nav_recent")]), on="date", how="inner")
            .sort("date")
        )
        row, daily, targets = evaluate_fast(days, price_lookup, oos_books, recent_books, base, spec, benchmark)
        fast_rows.append(row)
        stats = {
            "avg_turnover_trade_day": float(row["avg_turnover_trade_day"]),
            "trade_days": float(row["trade_days"]),
            "max_active": float(row["max_active"]),
        }
        detailed_inputs[spec.name] = (spec, daily, targets, stats)
        if i % 250 == 0 or i == len(specs):
            print(
                f"[iter83] fast {i:04d}/{len(specs)} best_oos="
                f"{max(float(r['oos_cagr']) for r in fast_rows):+.2%}",
                flush=True,
            )

    fast = pl.DataFrame(fast_rows)
    fast_path = RESULTS / f"{OUT_PREFIX}_fast_screen.csv"
    fast.sort(["excess_oos_cagr_vs_2330", "recent_1y_cagr", "oos_sortino"], descending=[True, True, True]).write_csv(fast_path)

    ranked_names = set()
    for sort_cols, descending, n in [
        (["oos_cagr", "recent_1y_cagr"], [True, True], 120),
        (["excess_oos_cagr_vs_2330", "oos_sortino"], [True, True], 120),
        (["recent_1y_cagr", "oos_cagr"], [True, True], 80),
        (["oos_sortino", "oos_cagr"], [True, True], 80),
    ]:
        for name in fast.sort(sort_cols, descending=descending).head(n)["name"].to_list():
            ranked_names.add(name)
    candidates = list(ranked_names)
    print(f"[iter83] detailed candidates={len(candidates)}", flush=True)

    etfs = load_active_etfs(days[0], days[-1])
    n_trials = ITER67_CUMULATIVE_TRIALS + ITER82_TRIALS + len(specs)
    detail_rows = []
    active_frames = []
    for i, name in enumerate(candidates, 1):
        spec, daily, _targets, stats = detailed_inputs[name]
        fast_row = next(r for r in fast_rows if r["name"] == name)
        row, active = evaluate_detailed(fast_row, daily, stats, n_trials, etfs, benchmark)
        detail_rows.append(row)
        active_frames.append(active)
        print(
            f"[iter83] detail {i:03d}/{len(candidates)} inner={spec.inner_idx} {spec.mode} "
            f"OOS={row['oos_cagr']:+.2%} excess2330={row['excess_oos_cagr_vs_2330']:+.2%} "
            f"1Y={row['recent_1y_cagr']:+.2%} MDD={row['oos_mdd']:.2%} "
            f"DSR={row['cumulative_dsr']:.3f} PBO={row['pbo']:.3f} "
            f"wins={row['active_etf_wins']:.0f}/{row['active_etf_count']:.0f} "
            f"max={row['max_active']:.0f}",
            flush=True,
        )

    summary = pl.DataFrame(detail_rows).sort(
        ["strict_promotable", "active_etf_wins", "excess_oos_cagr_vs_2330", "oos_sortino", "recent_1y_cagr"],
        descending=[True, True, True, True, True],
    )
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    active_path = RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv"
    summary.write_csv(summary_path)
    if active_frames:
        pl.concat(active_frames, how="vertical").write_csv(active_path)

    print("=" * 150)
    print("iter_83 blended OOS/recent allocator")
    print("=" * 150)
    print(
        summary.head(25).select(
            [
                "name",
                "strict_promotable",
                "inner_idx",
                "mode",
                "schedule",
                "lookback",
                "margin",
                "min_hold_days",
                "confirm_days",
                "low_recent_weight",
                "high_recent_weight",
                pl.col("avg_recent_weight").mul(100).round(1).alias("avg_recent_weight_pct"),
                "allocator_switches",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("excess_oos_cagr_vs_2330").mul(100).round(2).alias("excess2330_pct"),
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
    print(f"\n2330 benchmark OOS={benchmark['oos_cagr']:+.2%} recent1Y={benchmark['recent_1y_cagr']:+.2%}")
    print(f"Saved: {fast_path}")
    print(f"Saved: {summary_path}")
    print(f"Saved: {active_path}")


if __name__ == "__main__":
    main()
