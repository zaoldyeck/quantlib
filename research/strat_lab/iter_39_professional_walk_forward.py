"""iter_39 - production-sane walk-forward selector for next-open candidates.

This is the validation layer that should sit after iter_37/iter_38:
  - all catalyst fills are signal close -> next open;
  - every candidate respects the <=10 concurrent holding limit;
  - weights are selected with trailing data only, not full-window hindsight.
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


RESULTS = Path("research/strat_lab/results")
CAPITAL = 1_000_000.0
RF = 0.01


def metrics_from_rets(rets: np.ndarray, dates: list) -> dict[str, float]:
    rets = np.asarray(rets, dtype=float)
    if len(rets) < 2:
        return {"cagr": 0.0, "sortino": 0.0, "sharpe": 0.0, "mdd": 0.0, "final_nav": CAPITAL}
    nav = CAPITAL * np.cumprod(1 + rets)
    years = max((dates[-1] - dates[0]).days / 365.25, len(rets) / TDPY, 1e-9)
    cagr = (nav[-1] / CAPITAL) ** (1 / years) - 1
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY))
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


def nav_to_rets(path: Path, name: str) -> pl.DataFrame:
    df = pl.read_csv(path, try_parse_dates=True).sort("date").select(["date", "nav"])
    return df.with_columns(pl.col("nav").pct_change().fill_null(0.0).alias(name)).select(["date", name])


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
        prev = nav
        for d, ra, rb in zip(sub["date"].to_list(), sub["ret_a"].to_list(), sub["ret_b"].to_list(), strict=True):
            cap_a *= 1 + ra
            cap_b *= 1 + rb
            nav = cap_a + cap_b
            rows.append({"date": d, "nav": nav, "ret": (nav - prev) / prev if prev > 0 else 0.0})
            prev = nav
    return pl.DataFrame(rows)


def build_candidates() -> dict[str, pl.DataFrame]:
    q3 = RESULTS / "latest_q3_daily.csv"
    q5 = RESULTS / "latest_q5_daily.csv"
    c5 = RESULTS / "iter_37_max5_lkb90_v2_y30_atr3_nextopen_daily.csv"
    c7 = RESULTS / "iter_37_max7_lkb90_v2_y30_atr3_nextopen_daily.csv"
    c10 = RESULTS / "iter_37_max10_lkb90_v2_y30_atr3_nextopen_daily.csv"
    required = [q3, q5, c5, c7, c10]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise FileNotFoundError("missing required daily NAV files: " + ", ".join(missing))

    out: dict[str, pl.DataFrame] = {
        "quality3_only": nav_to_rets(q3, "quality3_only"),
        "quality5_only": nav_to_rets(q5, "quality5_only"),
        "catalyst5_nextopen_only": nav_to_rets(c5, "catalyst5_nextopen_only"),
        "catalyst7_nextopen_only": nav_to_rets(c7, "catalyst7_nextopen_only"),
        "catalyst10_nextopen_only": nav_to_rets(c10, "catalyst10_nextopen_only"),
    }

    for pct in range(50, 81, 5):
        name = f"nextopen_3q_7c_w{pct}"
        blend = annual_rebalanced_blend(q3, c7, pct / 100)
        blend.write_csv(RESULTS / f"iter_39_{name}_daily.csv")
        out[name] = blend.select(["date", pl.col("ret").alias(name)])

    for pct in range(70, 91, 5):
        name = f"nextopen_5q_5c_w{pct}"
        blend = annual_rebalanced_blend(q5, c5, pct / 100)
        blend.write_csv(RESULTS / f"iter_39_{name}_daily.csv")
        out[name] = blend.select(["date", pl.col("ret").alias(name)])

    return out


def aligned_returns(candidates: dict[str, pl.DataFrame]) -> pl.DataFrame:
    names = list(candidates)
    df = candidates[names[0]]
    for name in names[1:]:
        df = df.join(candidates[name], on="date", how="inner")
    return df.sort("date").with_columns(pl.col("date").dt.year().alias("year"))


def validate_returns(name: str, rets: np.ndarray, dates: list, n_trials: int) -> dict[str, float | str]:
    full = metrics_from_rets(rets, dates)
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = df.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_rets = oos["ret"].to_numpy()
    oos_dates = oos["date"].to_list()
    oos_metrics = metrics_from_rets(oos_rets, oos_dates)
    lo = lo_2002_sharpe_test(oos_rets)
    boot = bootstrap_ci(oos_rets, oos_dates)
    dsr = deflated_sharpe(oos_metrics["sharpe"], max(N_TRIALS_DSR, n_trials), oos_rets)
    pbo = pbo_cscv(walk_forward_folds(rets, dates))
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


def walk_forward_select(frame: pl.DataFrame, names: list[str], train_years: int = 5) -> tuple[pl.DataFrame, pl.DataFrame]:
    rows = []
    selections = []
    for test_year in range(2010, 2027):
        train = frame.filter((pl.col("year") >= test_year - train_years) & (pl.col("year") < test_year))
        test = frame.filter(pl.col("year") == test_year)
        if train.height < 500 or test.height < 30:
            continue

        scored = []
        train_dates = train["date"].to_list()
        for name in names:
            m = metrics_from_rets(train[name].to_numpy(), train_dates)
            score = m["sortino"] + min(m["mdd"] + 0.45, 0.0)
            scored.append({"name": name, "score": score, **{f"train_{k}": v for k, v in m.items()}})
        best = sorted(scored, key=lambda r: (r["score"], r["train_cagr"]), reverse=True)[0]
        selections.append({"year": test_year, **best})
        for d, r in zip(test["date"].to_list(), test[best["name"]].to_list(), strict=True):
            rows.append({"date": d, "ret": r, "selected": best["name"]})
    return pl.DataFrame(rows), pl.DataFrame(selections)


def returns_to_daily_nav(df: pl.DataFrame) -> pl.DataFrame:
    nav = CAPITAL * np.cumprod(1 + df["ret"].to_numpy())
    return df.with_columns(pl.Series("nav", nav))


def main() -> None:
    candidates = build_candidates()
    frame = aligned_returns(candidates)
    names = list(candidates)

    static_rows = []
    dates = frame["date"].to_list()
    for name in names:
        static_rows.append(validate_returns(name, frame[name].to_numpy(), dates, len(names)))

    wf, selections = walk_forward_select(frame, names)
    wf_daily = returns_to_daily_nav(wf)
    wf_daily.write_csv(RESULTS / "iter_39_walk_forward_daily.csv")
    selections.write_csv(RESULTS / "iter_39_walk_forward_selections.csv")
    static_rows.append(
        validate_returns(
            "walk_forward_5y_selector",
            wf["ret"].to_numpy(),
            wf["date"].to_list(),
            len(names),
        )
    )

    summary = pl.DataFrame(static_rows).sort(["oos_sortino", "oos_cagr"], descending=[True, True])
    out = RESULTS / "iter_39_professional_walk_forward_summary.csv"
    summary.write_csv(out)

    print("=" * 120)
    print("iter_39 professional walk-forward validation")
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
        .head(15)
        .to_pandas()
        .to_string(index=False)
    )
    print("\nWalk-forward yearly selections:")
    print(
        selections.select(
            [
                "year",
                "name",
                pl.col("score").round(3),
                pl.col("train_cagr").mul(100).round(2).alias("train_cagr_pct"),
                pl.col("train_sortino").round(3),
                pl.col("train_mdd").mul(100).round(2).alias("train_mdd_pct"),
            ]
        )
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
