"""iter_91 - refine active-ETF-beating realistic execution challengers.

Iter90 found candidates that beat all active ETFs on same live windows, but
their validation quality was weaker than the Iter89 champion.  This pass keeps
the alpha source fixed and searches only execution-layer target transformations
around those challengers: fewer names, minimum weights, rebalance spacing,
turnover bands, liquidity gates, and modest exposure scaling.
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

from active_etf_validator import compare_to_active_etfs, load_active_etf_series  # noqa: E402
from research.constants import CAPITAL  # noqa: E402
from research.db import connect  # noqa: E402
from execution import (  # noqa: E402
    ExecutionConfig,
    FubonFeeSchedule,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from iter_89_execution_champion_search import execution_objective  # noqa: E402
from iter_90_active_etf_aware_search import active_execution_score  # noqa: E402
from validator import validate_daily_nav  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_91_active_etf_challenger_refinement"
ITER90_PREFIX = "iter_90_active_etf_aware_search"
ITER90_SUMMARY = RESULTS / f"{ITER90_PREFIX}_summary.csv"
N_TRIALS = 40_160 + 360


@dataclass(frozen=True)
class RefinementPolicy:
    name: str
    max_positions: int
    min_weight: float
    min_rebalance_gap: int
    turnover_band: float
    min_adv_ntd: float
    exposure_mode: str
    exposure_scale: float


def load_targets(path: Path) -> dict[date, dict[str, float]]:
    frame = pl.read_csv(path, try_parse_dates=True).sort(["date", "company_code"])
    targets: dict[date, dict[str, float]] = {}
    for row in frame.iter_rows(named=True):
        targets.setdefault(row["date"], {})[str(row["company_code"])] = float(row["target_weight"])
    return targets


def load_source_ids() -> list[str]:
    summary = pl.read_csv(ITER90_SUMMARY, infer_schema_length=10000)
    selected = []
    for frame in [
        summary.filter((pl.col("active_etf_all_win") == True) & (pl.col("max_active") <= 10)),  # noqa: E712
        summary.filter((pl.col("active_etf_wins") >= 16) & (pl.col("max_active") <= 10)).sort(
            "execution_objective", descending=True
        ),
    ]:
        for sid in frame.head(4)["strategy_id"].to_list():
            target_path = RESULTS / f"{ITER90_PREFIX}_{sid}_target_weights.csv"
            if target_path.exists() and sid not in selected:
                selected.append(str(sid))
    return selected


def adv_lookup(bars: pl.DataFrame) -> dict[tuple[date, str], float]:
    out: dict[tuple[date, str], float] = {}
    for row in bars.select(["date", "company_code", "adv60", "trade_value"]).iter_rows(named=True):
        adv = row["adv60"]
        if adv is None or adv <= 0:
            adv = row["trade_value"]
        out[(row["date"], str(row["company_code"]))] = float(adv or 0.0)
    return out


def book_turnover(prev: dict[str, float], new: dict[str, float]) -> float:
    return sum(abs(new.get(code, 0.0) - prev.get(code, 0.0)) for code in set(prev) | set(new))


def normalize_book(book: dict[str, float], exposure: float) -> dict[str, float]:
    gross = sum(book.values())
    if gross <= 0.0:
        return {}
    scale = min(max(exposure, 0.0), 1.0) / gross
    return {code: weight * scale for code, weight in book.items() if weight * scale > 1e-12}


def transform_targets(
    source: dict[date, dict[str, float]],
    dates: list[date],
    adv: dict[tuple[date, str], float],
    policy: RefinementPolicy,
) -> tuple[dict[date, dict[str, float]], dict[str, float]]:
    date_index = {d: i for i, d in enumerate(dates)}
    current: dict[str, float] = {}
    out: dict[date, dict[str, float]] = {}
    last_rebalance_idx = -10**9
    accepted = 0
    names_total = 0.0
    exposure_total = 0.0

    for day in sorted(source):
        idx = date_index.get(day)
        if idx is None or idx - last_rebalance_idx < policy.min_rebalance_gap:
            continue
        raw = source[day]
        source_exposure = sum(raw.values()) * policy.exposure_scale
        candidates = {
            code: weight * policy.exposure_scale
            for code, weight in raw.items()
            if weight >= policy.min_weight
            and (policy.min_adv_ntd <= 0.0 or adv.get((day, code), 0.0) >= policy.min_adv_ntd)
        }
        candidates = dict(
            sorted(candidates.items(), key=lambda item: (-item[1], item[0]))[: policy.max_positions]
        )
        if policy.exposure_mode == "renorm_source":
            candidates = normalize_book(candidates, source_exposure)
        elif policy.exposure_mode != "keep":
            raise ValueError(f"unknown exposure mode: {policy.exposure_mode}")
        candidates = {code: weight for code, weight in candidates.items() if weight > 1e-12}
        if not candidates:
            continue
        if current and book_turnover(current, candidates) < policy.turnover_band:
            continue

        out[day] = candidates
        current = candidates
        last_rebalance_idx = idx
        accepted += 1
        names_total += len(candidates)
        exposure_total += sum(candidates.values())

    return out, {
        "target_rebalance_days": float(accepted),
        "target_avg_names": names_total / accepted if accepted else 0.0,
        "target_avg_exposure": exposure_total / accepted if accepted else 0.0,
    }


def policies() -> list[RefinementPolicy]:
    specs: list[RefinementPolicy] = []
    seen: set[str] = set()

    def add(
        name: str,
        max_positions: int,
        min_weight: float,
        min_rebalance_gap: int,
        turnover_band: float,
        min_adv_ntd: float,
        exposure_mode: str,
        exposure_scale: float,
    ) -> None:
        spec = RefinementPolicy(
            name,
            max_positions,
            min_weight,
            min_rebalance_gap,
            turnover_band,
            min_adv_ntd,
            exposure_mode,
            exposure_scale,
        )
        if spec.name not in seen:
            seen.add(spec.name)
            specs.append(spec)

    for max_positions in (6, 8, 9, 10):
        add(f"top{max_positions}_base", max_positions, 0.0, 0, 0.0, 0.0, "keep", 1.0)
        for gap in (5, 10, 20):
            add(f"top{max_positions}_gap{gap}", max_positions, 0.0, gap, 0.0, 0.0, "keep", 1.0)
        for band in (0.10, 0.15, 0.25):
            add(f"top{max_positions}_band{int(band * 100)}", max_positions, 0.0, 0, band, 0.0, "keep", 1.0)
        for min_weight in (0.03, 0.05):
            add(f"top{max_positions}_minw{int(min_weight * 100)}", max_positions, min_weight, 0, 0.0, 0.0, "renorm_source", 1.0)
        for adv in (20_000_000.0, 50_000_000.0):
            add(f"top{max_positions}_adv{int(adv / 1_000_000)}m", max_positions, 0.0, 0, 0.0, adv, "renorm_source", 1.0)
        for exposure in (0.90, 0.80):
            add(f"top{max_positions}_expo{int(exposure * 100)}", max_positions, 0.0, 0, 0.0, 0.0, "keep", exposure)
            add(
                f"top{max_positions}_gap10_expo{int(exposure * 100)}",
                max_positions,
                0.0,
                10,
                0.0,
                0.0,
                "keep",
                exposure,
            )

    return specs


def load_active_etfs(end: date) -> dict[str, pl.DataFrame]:
    con = connect(read_only=True)
    try:
        return load_active_etf_series(con, end=end.isoformat())
    finally:
        con.close()


def main() -> None:
    source_ids = load_source_ids()
    if not source_ids:
        raise RuntimeError("No Iter90 source target files found for refinement.")
    print(f"[iter91] sources={source_ids}", flush=True)

    source_targets = {
        sid: load_targets(RESULTS / f"{ITER90_PREFIX}_{sid}_target_weights.csv")
        for sid in source_ids
    }
    all_dates = sorted({day for targets in source_targets.values() for day in targets})
    all_codes = sorted({code for targets in source_targets.values() for book in targets.values() for code in book})
    con = connect(read_only=True)
    try:
        bars = load_adjusted_execution_bars(con, all_codes, all_dates[0], all_dates[-1])
    finally:
        con.close()
    adv = adv_lookup(bars)
    active_etfs = load_active_etfs(all_dates[-1])

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
    policy_list = policies()
    rows: list[dict[str, object]] = []
    active_frames: list[pl.DataFrame] = []
    top_artifacts: list[tuple[float, str, pl.DataFrame, pl.DataFrame, dict[date, dict[str, float]]]] = []
    n_trials = N_TRIALS + len(source_ids) * len(policy_list)

    total = len(source_ids) * len(policy_list)
    done = 0
    for source_id, targets in source_targets.items():
        for policy in policy_list:
            done += 1
            refined, target_stats = transform_targets(targets, all_dates, adv, policy)
            if not refined:
                continue
            name = f"{source_id}__{policy.name}"
            if done % 25 == 1 or done == total:
                print(f"[iter91] run {done:03d}/{total} {name}", flush=True)
            result = simulator.simulate(all_dates, refined)
            row = validate_daily_nav(
                name,
                result.daily.select(["date", "nav"]),
                n_trials=n_trials,
                extra={**result.stats, **target_stats, **asdict(policy), "source_strategy_id": source_id},
            )
            active_summary, active_detail = compare_to_active_etfs(name, result.daily.select(["date", "nav"]), active_etfs)
            row["strategy_id"] = name
            row.update(active_summary.as_dict())
            row["execution_objective"] = execution_objective(row)
            row["active_execution_score"] = active_execution_score(row)
            row["eligible_for_user_limit"] = float(row.get("max_active") or 0.0) <= 10.0
            row["strict_backtest_validated"] = (
                bool(row["eligible_for_user_limit"])
                and float(row.get("dsr") or 0.0) >= 0.95
                and float(row.get("pbo") or 1.0) < 0.20
                and float(row.get("oos_mdd") or -1.0) > -0.35
                and float(row.get("oos_cagr") or 0.0) >= 0.35
                and int(row.get("active_etf_wins") or 0) == int(row.get("active_etf_count") or -1)
            )
            row["config"] = str(asdict(config))
            rows.append(row)
            active_frames.append(active_detail)
            score = float(row["active_execution_score"])
            top_artifacts.append((score, name, result.daily, result.fills, refined))
            top_artifacts = sorted(top_artifacts, key=lambda item: item[0], reverse=True)[:16]

    summary = pl.DataFrame(rows).sort(
        [
            "strict_backtest_validated",
            "eligible_for_user_limit",
            "active_etf_all_win",
            "active_etf_wins",
            "dsr",
            "execution_objective",
        ],
        descending=[True, True, True, True, True, True],
    )
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    summary.write_csv(summary_path)
    if active_frames:
        pl.concat(active_frames, how="vertical").write_csv(RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv")

    saved_ids = set(summary.head(8)["strategy_id"].to_list())
    for _score, name, daily, fills, targets in top_artifacts:
        if name not in saved_ids:
            continue
        daily.write_csv(RESULTS / f"{OUT_PREFIX}_{name}_daily.csv")
        fills.write_csv(RESULTS / f"{OUT_PREFIX}_{name}_fills.csv")
        target_rows = [
            {"date": d, "company_code": code, "target_weight": weight}
            for d, book in sorted(targets.items())
            for code, weight in sorted(book.items())
        ]
        pl.DataFrame(target_rows).write_csv(RESULTS / f"{OUT_PREFIX}_{name}_target_weights.csv")

    print("=" * 150)
    print("iter_91 active-ETF challenger refinement")
    print("=" * 150)
    print(
        summary.select(
            [
                "strategy_id",
                "source_strategy_id",
                "strict_backtest_validated",
                "active_etf_wins",
                "active_etf_count",
                "active_etf_loss_list",
                pl.col("active_etf_worst_total_return_alpha").mul(100).round(2).alias("worst_total_alpha_pp"),
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("dsr").round(3),
                pl.col("pbo").round(3),
                "max_active",
                pl.col("fill_ratio").mul(100).round(2).alias("fill_ratio_pct"),
                pl.col("execution_objective").round(4),
                pl.col("active_execution_score").round(4),
            ]
        )
        .head(20)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"Saved: {summary_path}")


if __name__ == "__main__":
    main()
