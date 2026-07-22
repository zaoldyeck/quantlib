"""iter_59 - costed portfolio exposure overlay.

After iter58 rejected a fresh unified stock allocator, this pass separates the
problem into alpha source and risk throttle. It applies a prior-day, long-only
cash throttle to already validated strategy daily streams and charges an
additional exposure-change cost whenever the throttle sells or buys back book
exposure.

This is deliberately not a free return rescaling: base strategy NAV already
contains its own stock-level costs, and this overlay adds incremental cost for
the portfolio-level exposure change.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, COMMISSION, SELL_TAX, metrics_from_rets, validate_daily  # noqa: E402
from iter_45_fallback_gate_sweep import load_base  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")


@dataclass(frozen=True)
class BaseSpec:
    name: str
    path: Path
    max_active: float


BASES = [
    BaseSpec("iter42_w59", RESULTS / "iter42_q3_risk_breakout_top3_w59_daily.csv", 6.0),
    BaseSpec("iter42_w58", RESULTS / "iter42_q3_risk_breakout_top3_w58_daily.csv", 6.0),
    BaseSpec("iter44_w72_q3_trend", RESULTS / "iter44_q3_risk_breakout_top3_w72_fallback_q3_trend_daily.csv", 6.0),
    BaseSpec("iter44_w74_q3_trend", RESULTS / "iter44_q3_risk_breakout_top3_w74_fallback_q3_trend_daily.csv", 6.0),
    BaseSpec(
        "iter57_mkt63_monthly_hold20",
        RESULTS
        / "iter57_iter44_w74_q3_trend_iter52_squeeze_top5_gate_mkt_mom63_q3_ma50_sq_ma50_exit_gate_mkt_mom63_q3_ma50_sq_ma50_monthly_hold20_confirm3_daily.csv",
        6.0,
    ),
    BaseSpec(
        "iter57_mkt63_monthly_hold20_exit_q3sq",
        RESULTS
        / "iter57_iter44_w74_q3_trend_iter52_squeeze_top5_gate_mkt_mom63_q3_ma50_sq_ma50_exit_gate_q3_ma50_and_sq_ma50_monthly_hold20_confirm3_daily.csv",
        6.0,
    ),
]

GATES = [
    "gate_mkt_ma50",
    "gate_mkt_ma100",
    "gate_mkt_ma150",
    "gate_mkt_ma200",
    "gate_mkt_mom21",
    "gate_mkt_mom63",
    "gate_mkt_mom126",
    "gate_mkt_dd10",
    "gate_mkt_dd15",
    "gate_mkt_ma150_or_mom63",
    "gate_mkt_ma200_or_mom63",
    "gate_mkt_ma100_and_mom21",
    "gate_q3_ma50",
    "gate_q3_ma100",
    "gate_q3_mom63",
    "gate_q3_ma100_or_mom63",
    "gate_q3_ma50_and_mkt_ma50",
]
OFF_MULTS = [0.0, 0.25, 0.5, 0.75]
CONFIRMS = [1, 2, 3]
MIN_HOLDS = [0, 10, 20, 40]
VALIDATE_TOP_N = 320


def load_strategy(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return (
        pl.read_csv(path, try_parse_dates=True)
        .sort("date")
        .select(["date", "nav"])
        .with_columns(pl.col("nav").pct_change().fill_null(0.0).alias("base_ret"))
        .select(["date", "base_ret"])
    )


def confirmed(arr: np.ndarray, days: int) -> np.ndarray:
    if days <= 1:
        return arr.astype(bool)
    out = np.zeros_like(arr, dtype=bool)
    count = 0
    for i, v in enumerate(arr.astype(bool)):
        count = count + 1 if v else 0
        out[i] = count >= days
    return out


def exposure_path(gate: np.ndarray, off_mult: float, confirm_days: int, min_hold: int) -> np.ndarray:
    target_on = confirmed(gate, confirm_days)
    target_off = confirmed(~gate, confirm_days)
    exposure = np.ones(len(gate), dtype=float)
    state = 1.0
    held = 10_000
    for i in range(len(gate)):
        new_state = state
        if held >= min_hold:
            if state >= 0.999 and target_off[i]:
                new_state = off_mult
            elif state < 0.999 and target_on[i]:
                new_state = 1.0
        held = 0 if abs(new_state - state) > 1e-12 else held + 1
        state = new_state
        exposure[i] = state
    return exposure


def apply_overlay(strategy: pl.DataFrame, gates: pl.DataFrame, gate: str, off_mult: float, confirm: int, min_hold: int) -> tuple[pl.DataFrame, dict[str, float]]:
    df = strategy.join(gates, on="date", how="inner").sort("date")
    base_ret = df["base_ret"].to_numpy().astype(float)
    gate_arr = df[gate].to_numpy().astype(bool)
    expo = exposure_path(gate_arr, off_mult, confirm, min_hold)
    delta = np.diff(np.concatenate([[1.0], expo]))
    costs = np.where(delta < 0, -delta * (SELL_TAX + COMMISSION), delta * COMMISSION)
    ret = base_ret * expo - costs
    nav = CAPITAL * np.cumprod(1.0 + ret)
    stats = {
        "overlay_switches": float(np.count_nonzero(np.abs(delta) > 1e-12)),
        "avg_exposure": float(expo.mean()) if len(expo) else 0.0,
        "overlay_cost_total": float(costs.sum()),
    }
    return pl.DataFrame({"date": df["date"].to_list(), "nav": nav}), stats


def fast_screen_row(name: str, daily: pl.DataFrame, extra: dict[str, float]) -> dict[str, float | str]:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    full = metrics_from_rets(rets, dates)
    frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = frame.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_metrics = metrics_from_rets(oos["ret"].to_numpy(), oos["date"].to_list())
    return {
        "name": name,
        **full,
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_sharpe": oos_metrics["sharpe"],
        "oos_mdd": oos_metrics["mdd"],
        **extra,
    }


def load_gates() -> pl.DataFrame:
    base = load_base()
    return base.with_columns(
        [
            (pl.col("gate_mkt_ma200") | pl.col("gate_mkt_mom63")).alias("gate_mkt_ma200_or_mom63"),
            (pl.col("gate_q3_ma50") & pl.col("gate_mkt_ma50")).alias("gate_q3_ma50_and_mkt_ma50"),
        ]
    ).select(["date", *GATES])


def main() -> None:
    gates = load_gates()
    specs = [
        (base, gate, off, confirm, hold)
        for base in BASES
        for gate in GATES
        for off in OFF_MULTS
        for confirm in CONFIRMS
        for hold in MIN_HOLDS
    ]
    n_trials = len(specs)
    print(f"[iter59] bases={len(BASES)} specs={n_trials} validate_top={VALIDATE_TOP_N}", flush=True)
    screen_rows = []
    strategy_cache = {base.name: load_strategy(base.path) for base in BASES}
    for i, (base, gate, off, confirm, hold) in enumerate(specs, 1):
        off_tag = int(round(off * 100))
        name = f"iter59_{base.name}_{gate}_off{off_tag}_confirm{confirm}_hold{hold}"
        daily, overlay_stats = apply_overlay(strategy_cache[base.name], gates, gate, off, confirm, hold)
        row = fast_screen_row(
            name,
            daily,
            {
                "max_active": base.max_active,
                "trade_days": overlay_stats["overlay_switches"],
                "avg_turnover_trade_day": overlay_stats["overlay_cost_total"] / max(overlay_stats["overlay_switches"], 1.0),
            },
        )
        row["base"] = base.name
        row["gate"] = gate
        row["off_mult"] = off
        row["confirm_days"] = confirm
        row["min_hold_days"] = hold
        row["overlay_switches"] = overlay_stats["overlay_switches"]
        row["avg_exposure"] = overlay_stats["avg_exposure"]
        row["overlay_cost_total"] = overlay_stats["overlay_cost_total"]
        row["screen_pass"] = row["oos_cagr"] > 0.12 and row["oos_sortino"] > 1.0 and row["oos_mdd"] > -0.50
        screen_rows.append(row)
        if i % 500 == 0:
            print(
                f"[iter59 screen] {i:04d}/{n_trials} {name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%}",
                flush=True,
            )

    screen = pl.DataFrame(screen_rows)
    screen_out = RESULTS / "iter_59_costed_exposure_overlay_screen.csv"
    screen.sort(["screen_pass", "oos_sortino", "oos_cagr"], descending=[True, True, True]).write_csv(screen_out)
    ranked = screen.sort(["screen_pass", "oos_sortino", "oos_cagr"], descending=[True, True, True]).head(VALIDATE_TOP_N)
    selected = set(ranked["name"].to_list())
    print(f"[iter59] full-validation candidates={len(selected)} screen_saved={screen_out}", flush=True)

    rows = []
    for i, (base, gate, off, confirm, hold) in enumerate(specs, 1):
        off_tag = int(round(off * 100))
        name = f"iter59_{base.name}_{gate}_off{off_tag}_confirm{confirm}_hold{hold}"
        if name not in selected:
            continue
        daily, overlay_stats = apply_overlay(strategy_cache[base.name], gates, gate, off, confirm, hold)
        out_path = RESULTS / f"{name}_daily.csv"
        daily.write_csv(out_path)
        row = validate_daily(
            name,
            daily,
            n_trials,
            {
                "max_active": base.max_active,
                "trade_days": overlay_stats["overlay_switches"],
                "avg_turnover_trade_day": overlay_stats["overlay_cost_total"] / max(overlay_stats["overlay_switches"], 1.0),
            },
        )
        row["base"] = base.name
        row["gate"] = gate
        row["off_mult"] = off
        row["confirm_days"] = confirm
        row["min_hold_days"] = hold
        row["overlay_switches"] = overlay_stats["overlay_switches"]
        row["avg_exposure"] = overlay_stats["avg_exposure"]
        row["overlay_cost_total"] = overlay_stats["overlay_cost_total"]
        row["path"] = str(out_path)
        row["promotable"] = (
            row["dsr"] >= 0.95
            and row["pbo"] < 0.50
            and row["boot_cagr_lb"] > 0.10
            and row["oos_mdd"] > -0.45
            and row["max_active"] <= 10.0
        )
        rows.append(row)
        if len(rows) % 50 == 0 or row["promotable"]:
            print(
                f"[iter59 validate] {len(rows):03d}/{len(selected)} {name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%} DSR={row['dsr']:.3f} PBO={row['pbo']:.3f}",
                flush=True,
            )

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_59_costed_exposure_overlay_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "base",
        "gate",
        pl.col("off_mult").mul(100).round(0).cast(pl.Int64).alias("off_pct"),
        "confirm_days",
        "min_hold_days",
        pl.col("avg_exposure").mul(100).round(1).alias("avg_exposure_pct"),
        pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
        pl.col("sortino").round(3).alias("full_sortino"),
        pl.col("mdd").mul(100).round(2).alias("full_mdd_pct"),
        pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
        pl.col("oos_sortino").round(3),
        pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
        pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
        pl.col("dsr").round(3),
        pl.col("pbo").round(3),
        pl.col("overlay_switches").cast(pl.Int64),
    ]
    print("=" * 120)
    print("iter_59 costed exposure overlay")
    print("=" * 120)
    print(summary.select(view_cols).head(35).to_pandas().to_string(index=False))
    print("\nTop promotable by OOS CAGR")
    print(
        summary.filter(pl.col("promotable"))
        .sort(["oos_cagr", "oos_sortino"], descending=[True, True])
        .select(view_cols)
        .head(15)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
