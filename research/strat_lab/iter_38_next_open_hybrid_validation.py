"""iter_38 - next-open hybrid validation.

Combines quality sleeve NAVs with iter_37 next-open catalyst NAVs, then runs a
validation suite comparable to validate_hybrid.py.

This isolates the production execution assumption:
  signal at close -> trade next open.
"""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import polars as pl

from validate_hybrid import (
    N_TRIALS_DSR,
    TDPY,
    bootstrap_ci,
    deflated_sharpe,
    lo_2002_sharpe_test,
    pbo_cscv,
    walk_forward_folds,
)
from research import paths


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
CAPITAL = 1_000_000.0
RF = 0.01


def metrics_from_nav(nav: np.ndarray, dates: list) -> dict[str, float]:
    nav = np.asarray(nav, dtype=float)
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    years = max((dates[-1] - dates[0]).days / 365.25, 1e-9)
    cagr = (nav[-1] / CAPITAL) ** (1 / years) - 1
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY)) if len(rets) > 1 else 0.0
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    peak = CAPITAL
    mdd = 0.0
    for v in nav:
        peak = max(peak, float(v))
        mdd = min(mdd, (float(v) - peak) / peak)
    return {
        "cagr": float(cagr),
        "sortino": float((cagr - RF) / downvol) if downvol > 0 else 0.0,
        "sharpe": float((cagr - RF) / vol) if vol > 0 else 0.0,
        "mdd": float(mdd),
        "final_nav": float(nav[-1]),
    }


def annual_rebalanced_blend(path_a: Path, path_b: Path, w_a: float) -> pl.DataFrame:
    a = pl.read_csv(path_a, try_parse_dates=True).sort("date").select(["date", pl.col("nav").alias("nav_a")])
    b = pl.read_csv(path_b, try_parse_dates=True).sort("date").select(["date", pl.col("nav").alias("nav_b")])
    df = a.join(b, on="date", how="inner").sort("date")
    df = df.with_columns(
        [
            pl.col("nav_a").pct_change().fill_null(0.0).alias("ret_a"),
            pl.col("nav_b").pct_change().fill_null(0.0).alias("ret_b"),
            pl.col("date").dt.year().alias("year"),
        ]
    )
    w_b = 1.0 - w_a
    nav = CAPITAL
    rows = []
    for _, sub in df.group_by("year", maintain_order=True):
        cap_a = nav * w_a
        cap_b = nav * w_b
        for d, ra, rb in zip(sub["date"].to_list(), sub["ret_a"].to_list(), sub["ret_b"].to_list(), strict=True):
            cap_a *= 1 + ra
            cap_b *= 1 + rb
            nav = cap_a + cap_b
            rows.append({"date": d, "nav": nav})
    return pl.DataFrame(rows)


def validate_daily(name: str, daily: pl.DataFrame) -> dict[str, float | str]:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    full = metrics_from_nav(nav, dates)
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])

    folds = walk_forward_folds(rets, dates)
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = df.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_rets = oos["ret"].to_numpy()
    oos_nav = CAPITAL * np.cumprod(1 + oos_rets)
    oos_metrics = metrics_from_nav(oos_nav, oos["date"].to_list())
    lo = lo_2002_sharpe_test(oos_rets)
    boot = bootstrap_ci(oos_rets, oos["date"].to_list())
    dsr = deflated_sharpe(oos_metrics["sharpe"], N_TRIALS_DSR, oos_rets)
    pbo = pbo_cscv(folds)
    return {
        "name": name,
        **full,
        "oos_cagr": oos_metrics["cagr"],
        "oos_sortino": oos_metrics["sortino"],
        "oos_sharpe": oos_metrics["sharpe"],
        "oos_mdd": oos_metrics["mdd"],
        "lo_p": lo["p_value"],
        "boot_cagr_lb": boot["cagr_lb"],
        "boot_sortino_lb": boot["sortino_lb"],
        "dsr": dsr,
        "pbo": pbo,
    }


def load_benchmark(name: str, path: Path) -> dict[str, float | str]:
    df = pl.read_csv(path, try_parse_dates=True).sort("date")
    return validate_daily(name, df.select(["date", "nav"]))


def main() -> None:
    configs = [
        ("nextopen_3q_7c_w60", RESULTS / "latest_q3_daily.csv", RESULTS / "iter_37_max7_lkb90_v2_y30_atr3_nextopen_daily.csv", 0.60),
        ("nextopen_3q_7c_w70", RESULTS / "latest_q3_daily.csv", RESULTS / "iter_37_max7_lkb90_v2_y30_atr3_nextopen_daily.csv", 0.70),
        ("nextopen_5q_5c_w80", RESULTS / "latest_q5_daily.csv", RESULTS / "iter_37_max5_lkb90_v2_y30_atr3_nextopen_daily.csv", 0.80),
        ("nextopen_5q_5c_w85", RESULTS / "latest_q5_daily.csv", RESULTS / "iter_37_max5_lkb90_v2_y30_atr3_nextopen_daily.csv", 0.85),
    ]
    rows = []
    for name, path_a, path_b, w_a in configs:
        daily = annual_rebalanced_blend(path_a, path_b, w_a)
        out_path = RESULTS / f"iter_38_{name}_daily.csv"
        daily.write_csv(out_path)
        row = validate_daily(name, daily)
        row["path"] = str(out_path)
        rows.append(row)

    for name, path in [
        ("closefill_3q_7c_w60", RESULTS / "latest_true_3q_7c_best_w60_daily.csv"),
        ("closefill_3q_7c_w70", RESULTS / "latest_true_3q_7c_best_w70_daily.csv"),
        ("closefill_5q_5c_w85", RESULTS / "latest_true_5q_5c_best_w85_daily.csv"),
        ("hold_2330", RESULTS / "latest_hold_2330_daily.csv"),
    ]:
        if path.exists():
            row = load_benchmark(name, path)
            row["path"] = str(path)
            rows.append(row)

    summary = pl.DataFrame(rows).sort(["oos_sortino", "oos_cagr"], descending=[True, True])
    out = RESULTS / "iter_38_next_open_hybrid_validation.csv"
    summary.write_csv(out)
    print("=" * 120)
    print("iter_38 next-open hybrid validation")
    print("=" * 120)
    print(
        summary.select(
            [
                "name",
                pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
                pl.col("sortino").round(3).alias("full_sortino"),
                pl.col("mdd").mul(100).round(2).alias("full_mdd_pct"),
                pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
                pl.col("oos_sortino").round(3),
                pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
                pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
                pl.col("dsr").round(3),
                pl.col("pbo").round(3),
            ]
        )
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
