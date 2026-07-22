"""iter_88 - execution-aware target-book refinement for Iter86 lineage.

This pass starts from the validated Iter86 target book, then searches only
execution-layer transformations: fewer names, minimum target weights, minimum
rebalance distance, turnover bands, and ADV gates. The objective is realistic
Fubon executable performance, not paper NAV.
"""

from __future__ import annotations

import os
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import polars as pl
from research import paths

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, str(RESEARCH_ROOT))

from research.constants import CAPITAL  # noqa: E402
from research.db import connect  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_88_execution_aware_refinement"
SOURCE_DAILY = RESULTS / (
    "iter_86_oos_recent_maximizer_iter86_b15_b08_weekly_lb5_m0_hold20_c1_rw0_100_d75_daily.csv"
)
SOURCE_TARGETS = RESULTS / "iter_87_iter86_execution_validation_iter86_dual_target_weights.csv"
ITER87_SUMMARY = RESULTS / "iter_87_iter86_execution_validation_summary.csv"
N_TRIALS = 40_024


@dataclass(frozen=True)
class TargetPolicy:
    name: str
    max_positions: int
    min_weight: float
    min_rebalance_gap: int
    turnover_band: float
    min_adv_ntd: float
    exposure_mode: str


def load_days() -> list[date]:
    return pl.read_csv(SOURCE_DAILY, try_parse_dates=True).sort("date")["date"].to_list()


def load_source_targets() -> dict[date, dict[str, float]]:
    frame = pl.read_csv(SOURCE_TARGETS, try_parse_dates=True).sort(["date", "company_code"])
    targets: dict[date, dict[str, float]] = {}
    for row in frame.iter_rows(named=True):
        targets.setdefault(row["date"], {})[str(row["company_code"])] = float(row["target_weight"])
    return targets


def adv_lookup(bars: pl.DataFrame) -> dict[tuple[date, str], float]:
    out: dict[tuple[date, str], float] = {}
    for row in bars.select(["date", "company_code", "adv60", "trade_value"]).iter_rows(named=True):
        adv = row["adv60"]
        if adv is None or adv <= 0:
            adv = row["trade_value"]
        out[(row["date"], str(row["company_code"]))] = float(adv or 0.0)
    return out


def turnover(prev: dict[str, float], new: dict[str, float]) -> float:
    return sum(abs(new.get(code, 0.0) - prev.get(code, 0.0)) for code in set(prev) | set(new))


def normalize_book(book: dict[str, float], target_exposure: float) -> dict[str, float]:
    gross = sum(book.values())
    if gross <= 0:
        return {}
    scale = min(max(target_exposure, 0.0), 1.0) / gross
    return {code: weight * scale for code, weight in book.items() if weight * scale > 1e-12}


def transform_targets(
    source: dict[date, dict[str, float]],
    dates: list[date],
    adv: dict[tuple[date, str], float],
    policy: TargetPolicy,
) -> tuple[dict[date, dict[str, float]], dict[str, float]]:
    date_index = {d: i for i, d in enumerate(dates)}
    current: dict[str, float] = {}
    out: dict[date, dict[str, float]] = {}
    last_rebalance_idx = -10**9
    raw_names = 0.0
    raw_exposure = 0.0
    accepted = 0

    for day in sorted(source):
        idx = date_index.get(day)
        if idx is None:
            continue
        if idx - last_rebalance_idx < policy.min_rebalance_gap:
            continue
        raw = source[day]
        source_exposure = sum(raw.values())
        candidates = {
            code: weight
            for code, weight in raw.items()
            if weight >= policy.min_weight
            and (policy.min_adv_ntd <= 0 or adv.get((day, code), 0.0) >= policy.min_adv_ntd)
        }
        candidates = dict(
            sorted(candidates.items(), key=lambda item: (-item[1], item[0]))[: policy.max_positions]
        )
        if policy.exposure_mode == "renorm_source":
            candidates = normalize_book(candidates, source_exposure)
        elif policy.exposure_mode != "keep":
            raise ValueError(f"unknown exposure_mode: {policy.exposure_mode}")
        if not candidates:
            continue
        if current and turnover(current, candidates) < policy.turnover_band:
            continue

        out[day] = candidates
        current = candidates
        last_rebalance_idx = idx
        raw_names += len(candidates)
        raw_exposure += sum(candidates.values())
        accepted += 1

    return out, {
        "target_rebalance_days": float(accepted),
        "target_avg_names": raw_names / accepted if accepted else 0.0,
        "target_avg_exposure": raw_exposure / accepted if accepted else 0.0,
    }


def policies() -> list[TargetPolicy]:
    specs: list[TargetPolicy] = []
    for max_positions in (3, 4, 5, 6):
        specs.append(TargetPolicy(f"top{max_positions}_baseline", max_positions, 0.0, 0, 0.0, 0.0, "keep"))
        specs.append(TargetPolicy(f"top{max_positions}_gap5", max_positions, 0.0, 5, 0.0, 0.0, "keep"))
        specs.append(TargetPolicy(f"top{max_positions}_gap10", max_positions, 0.0, 10, 0.0, 0.0, "keep"))
        specs.append(TargetPolicy(f"top{max_positions}_band15", max_positions, 0.0, 0, 0.15, 0.0, "keep"))
    for max_positions in (4, 5, 6):
        specs.append(TargetPolicy(f"top{max_positions}_minw3", max_positions, 0.03, 0, 0.0, 0.0, "renorm_source"))
        specs.append(TargetPolicy(f"top{max_positions}_minw5", max_positions, 0.05, 0, 0.0, 0.0, "renorm_source"))
        specs.append(TargetPolicy(f"top{max_positions}_adv20m", max_positions, 0.0, 0, 0.0, 20_000_000.0, "renorm_source"))
        specs.append(TargetPolicy(f"top{max_positions}_adv50m", max_positions, 0.0, 0, 0.0, 50_000_000.0, "renorm_source"))
    specs.extend(
        [
            TargetPolicy("top5_gap5_band15", 5, 0.0, 5, 0.15, 0.0, "keep"),
            TargetPolicy("top5_gap10_band15", 5, 0.0, 10, 0.15, 0.0, "keep"),
            TargetPolicy("top6_gap5_adv20m", 6, 0.0, 5, 0.0, 20_000_000.0, "renorm_source"),
            TargetPolicy("top6_gap10_adv20m", 6, 0.0, 10, 0.0, 20_000_000.0, "renorm_source"),
        ]
    )
    return specs


def summarize(name: str, daily: pl.DataFrame, stats: dict[str, float], extra: dict[str, object]) -> dict[str, object]:
    return validate_daily_nav(
        name,
        daily.select(["date", "nav"]),
        n_trials=N_TRIALS,
        extra={**stats, **extra},
    )


def main() -> None:
    days = load_days()
    source_targets = load_source_targets()
    codes = sorted({code for book in source_targets.values() for code in book})
    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, codes, days[0], days[-1])
    finally:
        con.close()

    adv = adv_lookup(bars)
    config = ExecutionConfig(
        name="fubon_odd_lot_5pct_vol_slip5bp",
        capital=CAPITAL,
        lot_size=1,
        max_participation_rate=0.05,
        fixed_slippage_bps=5.0,
        impact_bps_per_1pct_volume=1.0,
        fee_schedule=FubonFeeSchedule(minimum_commission=20.0),
    )
    simulator = RealisticExecutionSimulator(bars, config)

    rows: list[dict[str, object]] = []
    for policy in policies():
        targets, target_stats = transform_targets(source_targets, days, adv, policy)
        if not targets:
            continue
        print(f"[iter88] running {policy.name} target_days={len(targets)}", flush=True)
        result = simulator.simulate(days, targets)
        daily_path = RESULTS / f"{OUT_PREFIX}_{policy.name}_daily.csv"
        fills_path = RESULTS / f"{OUT_PREFIX}_{policy.name}_fills.csv"
        result.daily.write_csv(daily_path)
        result.fills.write_csv(fills_path)
        rows.append(
            {
                **summarize(
                    f"iter88_{policy.name}",
                    result.daily,
                    result.stats,
                    {
                        **target_stats,
                        **asdict(policy),
                        "daily_path": str(daily_path),
                        "fills_path": str(fills_path),
                    },
                )
            }
        )

    summary = pl.DataFrame(rows)
    if ITER87_SUMMARY.exists():
        baseline = (
            pl.read_csv(ITER87_SUMMARY, try_parse_dates=True)
            .filter(pl.col("name") == "fubon_odd_lot_5pct_vol_slip5bp")
            .with_columns(
                [
                    pl.lit("iter87_baseline").alias("name"),
                    pl.lit(None).cast(pl.Utf8).alias("max_positions"),
                    pl.lit(None).cast(pl.Float64).alias("min_weight"),
                    pl.lit(None).cast(pl.Int64).alias("min_rebalance_gap"),
                    pl.lit(None).cast(pl.Float64).alias("turnover_band"),
                    pl.lit(None).cast(pl.Float64).alias("min_adv_ntd"),
                    pl.lit(None).cast(pl.Utf8).alias("exposure_mode"),
                    pl.lit(None).cast(pl.Utf8).alias("daily_path"),
                    pl.lit(None).cast(pl.Utf8).alias("fills_path"),
                ]
            )
        )
        summary = pl.concat([baseline, summary], how="diagonal_relaxed")

    summary = summary.with_columns(
        [
            (
                pl.col("oos_cagr")
                * (1.0 + pl.min_horizontal(pl.col("recent_1y_cagr"), pl.lit(3.0)))
                * pl.when(pl.col("oos_mdd") > -0.35).then(1.0).otherwise(0.85)
                * pl.when(pl.col("fill_ratio") >= 0.80).then(1.0).otherwise(0.80)
            ).alias("execution_objective"),
        ]
    )
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.sort("execution_objective", descending=True).write_csv(summary_path)

    print("=" * 150)
    print("iter_88 execution-aware target-book refinement")
    print("=" * 150)
    print(
        summary.sort("execution_objective", descending=True)
        .select(
            [
                "name",
                "max_positions",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("fill_ratio").mul(100).round(2).alias("fill_ratio_pct"),
                "blocked_orders",
                "partial_orders",
                pl.col("target_rebalance_days").round(0),
                pl.col("target_avg_names").round(2),
                pl.col("target_avg_exposure").round(3),
                pl.col("execution_objective").round(4),
            ]
        )
        .head(12)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"Saved: {summary_path}")


if __name__ == "__main__":
    main()
