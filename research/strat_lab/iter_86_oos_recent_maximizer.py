"""iter_86 - maximize OOS CAGR and latest 1Y CAGR.

This pass treats the best Iter84 OOS and recent-strength candidates as sleeves,
then searches a target-book level PM allocator between them.  The allocator uses
only lagged sleeve NAV momentum, executes through the shared next-open simulator,
and remains long-only with a hard 10-name cap.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, os.path.dirname(__file__))

from iter_40_research_campaign import CAPITAL, metrics_from_rets, simulate, validate_daily  # noqa: E402
from iter_57_cost_aware_switch import confirmed, scheduled_dates  # noqa: E402
from iter_64_active_etf_beater_confirm import load_active_etfs, window_metrics  # noqa: E402
from iter_67_partial_bridge import CUMULATIVE_TRIALS as ITER67_CUMULATIVE_TRIALS  # noqa: E402
from iter_82_oos_recent_pm_allocator import expand_book_targets  # noqa: E402
from iter_83_blended_oos_recent_allocator import BlendSpec, build_specs as build_iter83_specs  # noqa: E402
from iter_84_aggressive_champion_validation import (  # noqa: E402
    build_inputs,
    compare_active_etfs_local,
    run_spec,
)
from iter_83_blended_oos_recent_allocator import MAX_POSITIONS  # noqa: E402


RESULTS = Path("research/strat_lab/results")
OUT_PREFIX = "iter_86_oos_recent_maximizer"
ITER84_FAST = RESULTS / "iter_84_aggressive_champion_validation_refinement_fast_screen.csv"
ITER84_DETAIL = RESULTS / "iter_84_aggressive_champion_validation_refinement_summary.csv"
BASE_LIMIT = 24


@dataclass(frozen=True)
class BaseSleeve:
    base_id: str
    source_name: str
    spec: BlendSpec
    role: str


@dataclass(frozen=True)
class MetaSpec:
    left_id: str
    right_id: str
    mode: str
    lookback: int
    margin: float
    schedule: str
    min_hold_days: int
    confirm_days: int
    low_right_weight: float
    high_right_weight: float
    default_right_weight: float

    @property
    def strategy_id(self) -> str:
        if self.mode == "fixed":
            w = int(round(self.default_right_weight * 100))
            return f"iter86_{self.left_id}_{self.right_id}_fixed_r{w}"
        low = int(round(self.low_right_weight * 100))
        high = int(round(self.high_right_weight * 100))
        default = int(round(self.default_right_weight * 100))
        margin = int(round(self.margin * 100))
        return (
            f"iter86_{self.left_id}_{self.right_id}_{self.schedule}"
            f"_lb{self.lookback}_m{margin}_hold{self.min_hold_days}_c{self.confirm_days}"
            f"_rw{low}_{high}_d{default}"
        )


@dataclass
class SleeveRun:
    sleeve: BaseSleeve
    daily: pl.DataFrame
    stats: dict[str, float]
    targets: dict[date, dict[str, float]]
    books: dict[date, dict[str, float]]
    screen: dict[str, object]


def parse_blend_spec(row: dict[str, object]) -> BlendSpec:
    name = str(row["name"])
    default = 0.75
    if "_d" in name:
        tail = name.rsplit("_d", 1)[-1]
        digits = "".join(ch for ch in tail if ch.isdigit())
        if digits:
            default = int(digits) / 100.0
    return BlendSpec(
        inner_idx=int(row["inner_idx"]),
        mode="dynamic",
        lookback=int(row["lookback"]),
        margin=float(row["margin"]),
        schedule=str(row["schedule"]),
        min_hold_days=int(row["min_hold_days"]),
        confirm_days=int(row["confirm_days"]),
        low_recent_weight=float(row["low_recent_weight"]),
        high_recent_weight=float(row["high_recent_weight"]),
        default_recent_weight=default,
    )


def add_unique(rows: list[dict[str, object]], selected: dict[str, dict[str, object]], role: str) -> None:
    for row in rows:
        name = str(row["name"])
        if name not in selected:
            out = dict(row)
            out["_role"] = role
            selected[name] = out
        else:
            current = str(selected[name].get("_role", ""))
            if role not in current.split("+"):
                selected[name]["_role"] = f"{current}+{role}" if current else role


def load_base_sleeves() -> list[BaseSleeve]:
    if not ITER84_DETAIL.exists() or not ITER84_FAST.exists():
        raise FileNotFoundError("Iter84 result CSVs are required before running Iter86.")

    detail = pl.read_csv(ITER84_DETAIL, infer_schema_length=10000)
    fast = pl.read_csv(ITER84_FAST, infer_schema_length=10000)
    cols = [
        "name",
        "inner_idx",
        "schedule",
        "lookback",
        "margin",
        "min_hold_days",
        "confirm_days",
        "low_recent_weight",
        "high_recent_weight",
        "oos_cagr",
        "recent_1y_cagr",
        "oos_sortino",
        "oos_mdd",
        "max_active",
    ]
    frame = pl.concat(
        [
            detail.select([c for c in cols if c in detail.columns]),
            fast.select([c for c in cols if c in fast.columns]),
        ],
        how="diagonal",
    ).unique(subset=["name"], keep="first")
    frame = frame.filter((pl.col("max_active") <= MAX_POSITIONS) & (pl.col("oos_mdd") > -0.45))

    best_oos = float(frame["oos_cagr"].max())
    best_recent = float(frame["recent_1y_cagr"].max())
    frame = frame.with_columns(
        [
            pl.min_horizontal(
                pl.col("oos_cagr") / max(best_oos, 1e-9),
                pl.col("recent_1y_cagr") / max(best_recent, 1e-9),
            ).alias("dual_min_ratio"),
            (pl.col("oos_cagr") * pl.col("recent_1y_cagr")).alias("dual_product"),
        ]
    )

    selected: dict[str, dict[str, object]] = {}
    add_unique(frame.sort("oos_cagr", descending=True).head(8).to_dicts(), selected, "oos")
    add_unique(frame.sort("recent_1y_cagr", descending=True).head(8).to_dicts(), selected, "recent")
    add_unique(
        frame.sort(["dual_min_ratio", "dual_product"], descending=[True, True]).head(8).to_dicts(),
        selected,
        "dual",
    )
    if "strict_promotable" in detail.columns:
        strict = detail.filter(pl.col("strict_promotable") == True)  # noqa: E712
        strict = strict.sort(["oos_cagr", "recent_1y_cagr"], descending=[True, True]).head(8)
        add_unique(strict.to_dicts(), selected, "strict")

    ordered = sorted(
        selected.values(),
        key=lambda row: (
            float(row.get("dual_min_ratio") or 0.0),
            float(row.get("oos_cagr") or 0.0),
            float(row.get("recent_1y_cagr") or 0.0),
        ),
        reverse=True,
    )[:BASE_LIMIT]
    return [
        BaseSleeve(
            base_id=f"b{i:02d}",
            source_name=str(row["name"]),
            spec=parse_blend_spec(row),
            role=str(row.get("_role") or ""),
        )
        for i, row in enumerate(ordered)
    ]


def daily_rets(daily: pl.DataFrame) -> np.ndarray:
    nav = daily.sort("date")["nav"].to_numpy()
    return np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])


def screen_daily(
    name: str,
    daily: pl.DataFrame,
    stats: dict[str, float],
    extra: dict[str, object],
    benchmark: dict[str, object],
) -> dict[str, object]:
    daily = daily.sort("date")
    dates = daily["date"].to_list()
    rets = daily_rets(daily)
    full = metrics_from_rets(rets, dates)
    frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = frame.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_metrics = metrics_from_rets(oos["ret"].to_numpy(), oos["date"].to_list())
    recent = window_metrics(daily, 365)
    return {
        "strategy_id": name,
        **extra,
        **full,
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_sharpe": oos_metrics["sharpe"],
        "oos_mdd": oos_metrics["mdd"],
        **stats,
        **recent,
        "excess_oos_cagr_vs_2330": float(oos_metrics["cagr"]) - float(benchmark["oos_cagr"]),
        "excess_recent_1y_cagr_vs_2330": float(recent["recent_1y_cagr"]) - float(benchmark["recent_1y_cagr"]),
    }


def mix_books(left: dict[str, float], right: dict[str, float], right_weight: float) -> dict[str, float]:
    out: dict[str, float] = {}
    left_weight = 1.0 - right_weight
    for code, weight in left.items():
        out[code] = out.get(code, 0.0) + left_weight * weight
    for code, weight in right.items():
        out[code] = out.get(code, 0.0) + right_weight * weight
    out = {code: weight for code, weight in out.items() if weight > 1e-10}
    if len(out) <= MAX_POSITIONS:
        return out
    return dict(sorted(out.items(), key=lambda kv: (-kv[1], kv[0]))[:MAX_POSITIONS])


def meta_weight_path(base: pl.DataFrame, spec: MetaSpec) -> tuple[dict[date, float], int, float]:
    dates = base["date"].to_list()
    if spec.mode == "fixed":
        return {d: spec.default_right_weight for d in dates}, 0, spec.default_right_weight

    work = base.with_columns(
        [
            pl.col("nav_left").pct_change(spec.lookback).shift(1).fill_null(0.0).alias("mom_left"),
            pl.col("nav_right").pct_change(spec.lookback).shift(1).fill_null(0.0).alias("mom_right"),
        ]
    )
    rel = (work["mom_right"] - work["mom_left"]).to_numpy().astype(float)
    right_enter = confirmed(rel >= spec.margin, spec.confirm_days)
    left_enter = confirmed(rel <= -spec.margin, spec.confirm_days)
    sched = scheduled_dates(work, spec.schedule)

    current = spec.default_right_weight
    held = 10_000
    switches = 0
    weights: dict[date, float] = {}
    for i, d in enumerate(dates):
        new_weight = current
        if sched[i] and held >= spec.min_hold_days:
            if right_enter[i]:
                new_weight = spec.high_right_weight
            elif left_enter[i]:
                new_weight = spec.low_right_weight
        if abs(new_weight - current) > 1e-12:
            switches += 1
            held = 0
            current = new_weight
        else:
            held += 1
        weights[d] = current
    return weights, switches, float(np.mean(list(weights.values()))) if weights else 0.0


def combine_meta_targets(
    days: list[date],
    left_books: dict[date, dict[str, float]],
    right_books: dict[date, dict[str, float]],
    weights: dict[date, float],
) -> dict[date, dict[str, float]]:
    out: dict[date, dict[str, float]] = {}
    last: dict[str, float] | None = None
    for d in days:
        target = mix_books(left_books[d], right_books[d], weights[d])
        if last is None or target != last:
            out[d] = target
            last = target
    return out


def build_meta_specs(bases: list[BaseSleeve], base_screens: dict[str, dict[str, object]]) -> list[MetaSpec]:
    ranking = pl.DataFrame(list(base_screens.values()))
    oos_ids = ranking.sort("oos_cagr", descending=True).head(5)["strategy_id"].to_list()
    recent_ids = ranking.sort("recent_1y_cagr", descending=True).head(5)["strategy_id"].to_list()
    dual_ids = (
        ranking.with_columns(
            pl.min_horizontal(
                pl.col("oos_cagr") / max(float(ranking["oos_cagr"].max()), 1e-9),
                pl.col("recent_1y_cagr") / max(float(ranking["recent_1y_cagr"].max()), 1e-9),
            ).alias("dual_min_ratio")
        )
        .sort("dual_min_ratio", descending=True)
        .head(5)["strategy_id"]
        .to_list()
    )

    pairs: list[tuple[str, str]] = []
    for left in oos_ids:
        for right in recent_ids:
            if left != right:
                pairs.append((left, right))
    for dual in dual_ids:
        if oos_ids and dual != oos_ids[0]:
            pairs.append((oos_ids[0], dual))
        if recent_ids and dual != recent_ids[0]:
            pairs.append((dual, recent_ids[0]))
    pairs = list(dict.fromkeys(pairs))

    specs: list[MetaSpec] = []
    seen: set[str] = set()
    fixed_weights = (0.25, 0.50, 0.75)
    dynamic_weights = (
        (0.0, 1.0, 0.75),
        (0.25, 1.0, 0.75),
    )
    for left, right in pairs:
        for weight in fixed_weights:
            spec = MetaSpec(left, right, "fixed", 0, 0.0, "fixed", 0, 1, weight, weight, weight)
            if spec.strategy_id not in seen:
                seen.add(spec.strategy_id)
                specs.append(spec)
        for schedule in ("weekly", "monthly"):
            for lookback in (5, 10, 15, 21, 31):
                for margin in (-0.02, 0.0, 0.02):
                    for hold in (10, 20, 40):
                        for confirm in (1, 2):
                            for low_w, high_w, default_w in dynamic_weights:
                                spec = MetaSpec(
                                    left,
                                    right,
                                    "dynamic",
                                    lookback,
                                    margin,
                                    schedule,
                                    hold,
                                    confirm,
                                    low_w,
                                    high_w,
                                    default_w,
                                )
                                if spec.strategy_id not in seen:
                                    seen.add(spec.strategy_id)
                                    specs.append(spec)
    return specs


def evaluate_detailed(
    row: dict[str, object],
    daily: pl.DataFrame,
    stats: dict[str, float],
    n_trials: int,
    etfs: dict[str, pl.DataFrame],
    benchmark: dict[str, object],
) -> tuple[dict[str, object], pl.DataFrame]:
    focused = validate_daily(str(row["strategy_id"]), daily, n_trials, stats)
    recent = window_metrics(daily, 365)
    active_summary, active_rows = compare_active_etfs_local(str(row["strategy_id"]), daily, etfs)
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
    return out, pl.DataFrame(active_rows)


def main() -> None:
    days, price_lookup, oos_sleeves, recent_books, recent_daily, benchmark, etfs = build_inputs()
    base_sleeves = load_base_sleeves()
    print(f"[iter86] data_end={days[-1]} base_sleeves={len(base_sleeves)}", flush=True)

    sleeve_runs: dict[str, SleeveRun] = {}
    base_rows = []
    for sleeve in base_sleeves:
        daily, stats, targets, meta = run_spec(days, price_lookup, oos_sleeves, recent_books, recent_daily, sleeve.spec)
        row = screen_daily(
            sleeve.base_id,
            daily,
            stats,
            {
                "source_name": sleeve.source_name,
                "kind": "base",
                "role": sleeve.role,
                "left_id": "",
                "right_id": "",
                "mode": "base",
                "lookback": sleeve.spec.lookback,
                "margin": sleeve.spec.margin,
                "schedule": sleeve.spec.schedule,
                "min_hold_days": sleeve.spec.min_hold_days,
                "confirm_days": sleeve.spec.confirm_days,
                "avg_right_weight": 0.0,
                "allocator_switches": 0,
                **meta,
            },
            benchmark,
        )
        books = expand_book_targets(days, targets)
        sleeve_runs[sleeve.base_id] = SleeveRun(sleeve, daily, stats, targets, books, row)
        base_rows.append(row)
        print(
            f"[iter86 base] {sleeve.base_id} OOS={row['oos_cagr']:+.2%} "
            f"1Y={row['recent_1y_cagr']:+.2%} role={sleeve.role}",
            flush=True,
        )

    base_summary = pl.DataFrame(base_rows)
    base_summary.write_csv(RESULTS / f"{OUT_PREFIX}_base_sleeves.csv")

    meta_specs = build_meta_specs(base_sleeves, {row["strategy_id"]: row for row in base_rows})
    prior_iter84 = pl.read_csv(ITER84_FAST).height if ITER84_FAST.exists() else 0
    n_trials = ITER67_CUMULATIVE_TRIALS + 480 + len(build_iter83_specs()) + prior_iter84 + len(meta_specs)
    print(f"[iter86] meta_specs={len(meta_specs)} n_trials={n_trials}", flush=True)

    fast_rows: list[dict[str, object]] = []
    saved: dict[str, tuple[pl.DataFrame, dict[str, float], dict[str, object]]] = {}
    best_oos = -999.0
    best_recent = -999.0
    for i, spec in enumerate(meta_specs, 1):
        left = sleeve_runs[spec.left_id]
        right = sleeve_runs[spec.right_id]
        base = (
            left.daily.select(["date", pl.col("nav").alias("nav_left")])
            .join(right.daily.select(["date", pl.col("nav").alias("nav_right")]), on="date", how="inner")
            .sort("date")
        )
        weights, switches, avg_right_weight = meta_weight_path(base, spec)
        targets = combine_meta_targets(days, left.books, right.books, weights)
        daily, stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
        row = screen_daily(
            spec.strategy_id,
            daily,
            stats,
            {
                "source_name": "",
                "kind": "meta_pair",
                "role": "",
                "left_id": spec.left_id,
                "right_id": spec.right_id,
                "left_source_name": left.sleeve.source_name,
                "right_source_name": right.sleeve.source_name,
                "mode": spec.mode,
                "lookback": spec.lookback,
                "margin": spec.margin,
                "schedule": spec.schedule,
                "min_hold_days": spec.min_hold_days,
                "confirm_days": spec.confirm_days,
                "low_right_weight": spec.low_right_weight,
                "high_right_weight": spec.high_right_weight,
                "default_right_weight": spec.default_right_weight,
                "avg_right_weight": avg_right_weight,
                "allocator_switches": switches,
                "target_rebalance_days": len(targets),
            },
            benchmark,
        )
        fast_rows.append(row)
        if float(row["oos_cagr"]) >= 0.39 or float(row["recent_1y_cagr"]) >= 2.75:
            saved[spec.strategy_id] = (daily, stats, row)
        best_oos = max(best_oos, float(row["oos_cagr"]))
        best_recent = max(best_recent, float(row["recent_1y_cagr"]))
        if i % 500 == 0 or i == len(meta_specs):
            print(
                f"[iter86 screen] {i:05d}/{len(meta_specs)} "
                f"best_oos={best_oos:+.2%} best_1y={best_recent:+.2%}",
                flush=True,
            )

    fast = pl.concat([base_summary, pl.DataFrame(fast_rows)], how="diagonal")
    best_oos = float(fast["oos_cagr"].max())
    best_recent = float(fast["recent_1y_cagr"].max())
    fast = fast.with_columns(
        [
            pl.min_horizontal(
                pl.col("oos_cagr") / max(best_oos, 1e-9),
                pl.col("recent_1y_cagr") / max(best_recent, 1e-9),
            ).alias("dual_min_ratio"),
            (pl.col("oos_cagr") * pl.col("recent_1y_cagr")).alias("dual_product"),
        ]
    )
    fast_path = RESULTS / f"{OUT_PREFIX}_fast_screen.csv"
    fast.sort(["dual_min_ratio", "oos_cagr", "recent_1y_cagr"], descending=[True, True, True]).write_csv(fast_path)

    detail_ids = set()
    for cols, descending, n in [
        (["oos_cagr", "recent_1y_cagr"], [True, True], 80),
        (["recent_1y_cagr", "oos_cagr"], [True, True], 80),
        (["dual_min_ratio", "dual_product"], [True, True], 100),
        (["oos_sortino", "oos_cagr"], [True, True], 60),
    ]:
        detail_ids.update(fast.sort(cols, descending=descending).head(n)["strategy_id"].to_list())

    meta_by_id = {spec.strategy_id: spec for spec in meta_specs}
    detail_rows = []
    active_frames = []
    for i, strategy_id in enumerate(sorted(detail_ids), 1):
        if strategy_id in sleeve_runs:
            run = sleeve_runs[strategy_id]
            daily, stats, fast_row = run.daily, run.stats, run.screen
        elif strategy_id in saved:
            daily, stats, fast_row = saved[strategy_id]
        else:
            spec = meta_by_id[strategy_id]
            left = sleeve_runs[spec.left_id]
            right = sleeve_runs[spec.right_id]
            base = (
                left.daily.select(["date", pl.col("nav").alias("nav_left")])
                .join(right.daily.select(["date", pl.col("nav").alias("nav_right")]), on="date", how="inner")
                .sort("date")
            )
            weights, switches, avg_right_weight = meta_weight_path(base, spec)
            targets = combine_meta_targets(days, left.books, right.books, weights)
            daily, stats = simulate(days, price_lookup, targets, {d: 1.0 for d in days}, persist=True)
            fast_row = screen_daily(
                strategy_id,
                daily,
                stats,
                {
                    "source_name": "",
                    "kind": "meta_pair",
                    "role": "",
                    "left_id": spec.left_id,
                    "right_id": spec.right_id,
                    "left_source_name": left.sleeve.source_name,
                    "right_source_name": right.sleeve.source_name,
                    "mode": spec.mode,
                    "lookback": spec.lookback,
                    "margin": spec.margin,
                    "schedule": spec.schedule,
                    "min_hold_days": spec.min_hold_days,
                    "confirm_days": spec.confirm_days,
                    "low_right_weight": spec.low_right_weight,
                    "high_right_weight": spec.high_right_weight,
                    "default_right_weight": spec.default_right_weight,
                    "avg_right_weight": avg_right_weight,
                    "allocator_switches": switches,
                    "target_rebalance_days": len(targets),
                },
                benchmark,
            )
        detailed, active = evaluate_detailed(fast_row, daily, stats, n_trials, etfs, benchmark)
        if detailed["strict_promotable"] or strategy_id in {
            str(fast.sort("oos_cagr", descending=True)["strategy_id"][0]),
            str(fast.sort("recent_1y_cagr", descending=True)["strategy_id"][0]),
            str(fast.sort("dual_min_ratio", descending=True)["strategy_id"][0]),
        }:
            path = RESULTS / f"{OUT_PREFIX}_{strategy_id}_daily.csv"
            daily.write_csv(path)
            detailed["path"] = str(path)
        else:
            detailed["path"] = ""
        detail_rows.append(detailed)
        active_frames.append(active)
        print(
            f"[iter86 detail] {i:03d}/{len(detail_ids)} {strategy_id} "
            f"OOS={detailed['oos_cagr']:+.2%} 1Y={detailed['recent_1y_cagr']:+.2%} "
            f"DSR={detailed['cumulative_dsr']:.3f} PBO={detailed['pbo']:.3f} "
            f"wins={detailed['active_etf_wins']:.0f}/{detailed['active_etf_count']:.0f}",
            flush=True,
        )

    detail = pl.DataFrame(detail_rows).with_columns(
        [
            pl.min_horizontal(
                pl.col("oos_cagr") / max(float(pl.DataFrame(detail_rows)["oos_cagr"].max()), 1e-9),
                pl.col("recent_1y_cagr") / max(float(pl.DataFrame(detail_rows)["recent_1y_cagr"].max()), 1e-9),
            ).alias("validated_dual_min_ratio"),
            (pl.col("oos_cagr") * pl.col("recent_1y_cagr")).alias("validated_dual_product"),
        ]
    )
    detail_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    detail.sort(["validated_dual_min_ratio", "oos_cagr", "recent_1y_cagr"], descending=[True, True, True]).write_csv(detail_path)
    if active_frames:
        pl.concat(active_frames, how="vertical").write_csv(RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv")

    print("=" * 150)
    print("iter_86 OOS/recent maximizer")
    print("=" * 150)
    view_cols = [
        "strategy_id",
        "kind",
        "left_id",
        "right_id",
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
    for title, frame in [
        ("Top OOS CAGR", detail.sort("oos_cagr", descending=True).head(10)),
        ("Top recent 1Y CAGR", detail.sort("recent_1y_cagr", descending=True).head(10)),
        ("Top dual minimum ratio", detail.sort("validated_dual_min_ratio", descending=True).head(10)),
    ]:
        print(f"\n{title}:")
        print(frame.select(view_cols).to_pandas().to_string(index=False))
    print(f"\n2330 benchmark OOS={benchmark['oos_cagr']:+.2%} recent1Y={benchmark['recent_1y_cagr']:+.2%}")
    print(f"Saved: {fast_path}")
    print(f"Saved: {detail_path}")


if __name__ == "__main__":
    main()
