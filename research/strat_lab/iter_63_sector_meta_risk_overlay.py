"""iter_63 - risk overlay on sector-leadership meta candidates.

iter62 solved the "too conservative during active-ETF momentum" problem but
still failed the production gate because DSR stayed below 0.95. The weak spots
were classic risk-off years, especially 2022.

This pass does not search for a new alpha source. It applies a costed,
prior-day exposure throttle to a small deterministic shortlist from iter62:

  - top two by OOS Sortino;
  - top two by active-window return among candidates with strong OOS Sortino.

The overlay is long-only: exposure is either full or partially in cash, and
every exposure change pays incremental transaction friction.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, COMMISSION, SELL_TAX, metrics_from_rets, validate_daily  # noqa: E402
from iter_59_costed_exposure_overlay import GATES, confirmed, load_gates  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
ITER62_SUMMARY = RESULTS / "iter_62_sector_leadership_meta_summary.csv"
VALIDATE_TOP_N = 220
OFF_MULTS = [0.0, 0.25, 0.5, 0.75]
CONFIRMS = [1, 2, 3]
MIN_HOLDS = [0, 10, 20, 40]


def select_bases() -> pl.DataFrame:
    if not ITER62_SUMMARY.exists():
        raise FileNotFoundError(ITER62_SUMMARY)
    summary = pl.read_csv(ITER62_SUMMARY)
    required = {"name", "path", "oos_sortino", "oos_mdd", "active_window_total_return"}
    if not required.issubset(summary.columns):
        raise ValueError(f"{ITER62_SUMMARY} missing required columns: {required - set(summary.columns)}")

    eligible = summary.filter((pl.col("oos_sortino") > 1.70) & (pl.col("oos_mdd") > -0.43))
    top_oos = eligible.sort(["oos_sortino", "oos_cagr"], descending=[True, True]).head(2)
    top_active = eligible.sort(["active_window_total_return", "oos_sortino"], descending=[True, True]).head(2)
    return pl.concat([top_oos, top_active]).unique(subset=["name"], keep="first")


def load_strategy(path: str) -> pl.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return (
        pl.read_csv(p, try_parse_dates=True)
        .sort("date")
        .select(["date", "nav"])
        .with_columns(pl.col("nav").pct_change().fill_null(0.0).alias("base_ret"))
        .select(["date", "base_ret"])
    )


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


def apply_overlay(
    strategy: pl.DataFrame,
    gates: pl.DataFrame,
    gate: str,
    off_mult: float,
    confirm: int,
    min_hold: int,
) -> tuple[pl.DataFrame, dict[str, float]]:
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


def fast_screen_row(name: str, daily: pl.DataFrame, extra: dict[str, float | str]) -> dict[str, float | str]:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    full = metrics_from_rets(rets, dates)
    frame = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = frame.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    recent = frame.filter(pl.col("date") >= frame["date"][-1] - timedelta(days=365 * 3))
    active_window = frame.filter(pl.col("date") >= pl.date(2026, 1, 22))
    oos_metrics = metrics_from_rets(oos["ret"].to_numpy(), oos["date"].to_list())
    recent_metrics = metrics_from_rets(recent["ret"].to_numpy(), recent["date"].to_list())
    active_metrics = metrics_from_rets(active_window["ret"].to_numpy(), active_window["date"].to_list())
    return {
        "name": name,
        **full,
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_sharpe": oos_metrics["sharpe"],
        "oos_mdd": oos_metrics["mdd"],
        "recent_3y_cagr": recent_metrics["cagr"],
        "recent_3y_sortino": recent_metrics["sortino"],
        "active_window_total_return": active_metrics["final_nav"] / CAPITAL - 1.0,
        "active_window_cagr": active_metrics["cagr"],
        "active_window_mdd": active_metrics["mdd"],
        **extra,
    }


def main() -> None:
    bases = select_bases()
    gates = load_gates()
    strategy_cache = {row["name"]: load_strategy(row["path"]) for row in bases.iter_rows(named=True)}
    specs = [
        (row, gate, off, confirm, hold)
        for row in bases.iter_rows(named=True)
        for gate in GATES
        for off in OFF_MULTS
        for confirm in CONFIRMS
        for hold in MIN_HOLDS
    ]
    n_trials = len(specs)
    print(f"[iter63] bases={bases.height} specs={n_trials} validate_top={VALIDATE_TOP_N}", flush=True)
    print(bases.select(["name", "oos_sortino", "oos_mdd", "active_window_total_return"]).to_pandas().to_string(index=False))

    screen_rows = []
    daily_cache: dict[str, pl.DataFrame] = {}
    for i, (base, gate, off, confirm, hold) in enumerate(specs, 1):
        off_tag = int(round(off * 100))
        name = f"iter63_{base['name']}_{gate}_off{off_tag}_confirm{confirm}_hold{hold}"
        daily, overlay_stats = apply_overlay(strategy_cache[base["name"]], gates, gate, off, confirm, hold)
        row = fast_screen_row(
            name,
            daily,
            {
                "base": base["name"],
                "gate": gate,
                "off_mult": off,
                "confirm_days": float(confirm),
                "min_hold_days": float(hold),
                "max_active": 6.0,
                "trade_days": overlay_stats["overlay_switches"],
                "avg_turnover_trade_day": overlay_stats["overlay_cost_total"]
                / max(overlay_stats["overlay_switches"], 1.0),
                "overlay_switches": overlay_stats["overlay_switches"],
                "avg_exposure": overlay_stats["avg_exposure"],
                "overlay_cost_total": overlay_stats["overlay_cost_total"],
            },
        )
        row["screen_pass"] = (
            row["oos_sortino"] > 1.75
            and row["oos_mdd"] > -0.35
            and row["active_window_total_return"] > 0.25
            and row["recent_3y_cagr"] > 0.25
        )
        screen_rows.append(row)
        if row["screen_pass"] or (row["oos_sortino"] > 1.85 and row["active_window_total_return"] > 0.20):
            daily_cache[name] = daily
        if i % 500 == 0:
            print(
                f"[iter63 screen] {i:04d}/{n_trials} {name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%} ActiveWin={row['active_window_total_return']:+.2%}",
                flush=True,
            )

    screen = pl.DataFrame(screen_rows).sort(
        ["screen_pass", "oos_sortino", "active_window_total_return", "oos_cagr"],
        descending=[True, True, True, True],
    )
    screen_path = RESULTS / "iter_63_sector_meta_risk_overlay_screen.csv"
    screen.write_csv(screen_path)
    selected_names = set(screen.head(VALIDATE_TOP_N)["name"].to_list())
    print(f"[iter63] full-validation candidates={len(selected_names)}", flush=True)

    rows = []
    for name in selected_names:
        row0 = screen.filter(pl.col("name") == name).row(0, named=True)
        daily = daily_cache.get(name)
        if daily is None:
            base = bases.filter(pl.col("name") == row0["base"]).row(0, named=True)
            daily, _ = apply_overlay(
                strategy_cache[row0["base"]],
                gates,
                row0["gate"],
                float(row0["off_mult"]),
                int(row0["confirm_days"]),
                int(row0["min_hold_days"]),
            )
        out_path = RESULTS / f"{name}_daily.csv"
        daily.write_csv(out_path)
        row = validate_daily(
            name,
            daily,
            n_trials,
            {
                "max_active": 6.0,
                "trade_days": float(row0["trade_days"]),
                "avg_turnover_trade_day": float(row0["avg_turnover_trade_day"]),
            },
        )
        for key in [
            "base",
            "gate",
            "off_mult",
            "confirm_days",
            "min_hold_days",
            "overlay_switches",
            "avg_exposure",
            "overlay_cost_total",
            "active_window_total_return",
            "active_window_cagr",
            "active_window_mdd",
        ]:
            row[key] = row0[key]
        row["path"] = str(out_path)
        row["promotable"] = (
            row["dsr"] >= 0.95
            and row["pbo"] < 0.50
            and row["boot_cagr_lb"] > 0.10
            and row["oos_mdd"] > -0.40
            and row["max_active"] <= 10.0
        )
        rows.append(row)
        if len(rows) % 25 == 0 or row["promotable"]:
            print(
                f"[iter63 validate] {len(rows):03d}/{len(selected_names)} {name}: "
                f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"MDD={row['oos_mdd']:.2%} DSR={row['dsr']:.3f} PBO={row['pbo']:.3f} "
                f"ActiveWin={row['active_window_total_return']:+.2%}",
                flush=True,
            )

    summary = pl.DataFrame(rows).sort(
        ["promotable", "oos_sortino", "active_window_total_return", "oos_cagr"],
        descending=[True, True, True, True],
    )
    summary_path = RESULTS / "iter_63_sector_meta_risk_overlay_summary.csv"
    summary.write_csv(summary_path)
    view_cols = [
        "name",
        "promotable",
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
        pl.col("active_window_total_return").mul(100).round(2).alias("active_window_total_pct"),
    ]
    print("=" * 140)
    print("iter_63 sector-meta risk overlay")
    print("=" * 140)
    print(summary.select(view_cols).head(40).to_pandas().to_string(index=False))
    print("\nTop promotable by active-window return")
    print(
        summary.filter(pl.col("promotable"))
        .sort(["active_window_total_return", "oos_sortino"], descending=[True, True])
        .select(view_cols)
        .head(20)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {screen_path}")
    print(f"Saved: {summary_path}")


if __name__ == "__main__":
    main()
