"""iter_36 - walk-forward selector over dynamic, non-hard-coded strategies.

This script does not include fixed 2330 as a selectable strategy. 2330 and the
existing hybrid are benchmarks only. The selector chooses among iter_35 dynamic
strategies using trailing historical data, then applies the chosen strategy to
the next calendar year.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
RF = 0.01
TDPY = 252


def nav_metrics(nav: np.ndarray, dates: list) -> dict[str, float]:
    nav = np.asarray(nav, dtype=float)
    rets = np.diff(nav) / nav[:-1]
    years = max((dates[-1] - dates[0]).days / 365.25, 1e-9)
    cagr = (nav[-1] / nav[0]) ** (1 / years) - 1
    vol = float(rets.std(ddof=1) * math.sqrt(TDPY)) if len(rets) > 1 else 0.0
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 1e-9
    peak = nav[0]
    mdd = 0.0
    for v in nav:
        peak = max(peak, v)
        mdd = min(mdd, (v - peak) / peak)
    return {
        "cagr": float(cagr),
        "sortino": float((cagr - RF) / downvol) if downvol > 0 else 0.0,
        "sharpe": float((cagr - RF) / vol) if vol > 0 else 0.0,
        "mdd": float(mdd),
        "final_nav": float(nav[-1]),
    }


def strategy_files() -> dict[str, Path]:
    return {
        p.name.removeprefix("iter_35_").removesuffix("_daily.csv"): p
        for p in RESULTS.glob("iter_35_*_daily.csv")
        if "daily" in p.name
    }


def load_returns(paths: dict[str, Path]) -> pl.DataFrame:
    frames = []
    for name, path in sorted(paths.items()):
        df = (
            pl.read_csv(path, try_parse_dates=True)
            .select(["date", "nav"])
            .sort("date")
            .with_columns((pl.col("nav") / pl.col("nav").shift(1) - 1).alias(name))
            .select(["date", name])
        )
        frames.append(df)
    out = frames[0]
    for frame in frames[1:]:
        out = out.join(frame, on="date", how="inner")
    return out.drop_nulls().sort("date")


def train_score(ret: pl.Series) -> tuple[float, float, float]:
    r = ret.to_numpy()
    if len(r) < 252:
        return (-999.0, -999.0, -1.0)
    nav = np.cumprod(1 + r)
    cagr = nav[-1] ** (252 / len(r)) - 1
    down = r[r < 0]
    downvol = float(down.std(ddof=1) * math.sqrt(TDPY)) if len(down) > 1 else 1e-9
    sortino = (cagr - RF) / downvol if downvol > 0 else -999.0
    peak = np.maximum.accumulate(nav)
    mdd = float(((nav - peak) / peak).min())
    # Penalize strategies that only win by accepting severe training drawdown.
    score = sortino + min(0.0, mdd + 0.45) * 2.0
    return (float(score), float(sortino), mdd)


def walk_forward(ret_df: pl.DataFrame, lookback_years: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    strategies = [c for c in ret_df.columns if c != "date"]
    years = sorted(ret_df["date"].dt.year().unique().to_list())
    nav = 1_000_000.0
    daily = []
    decisions = []
    for year in years:
        train_start = year - lookback_years
        train = ret_df.filter((pl.col("date").dt.year() >= train_start) & (pl.col("date").dt.year() < year))
        test = ret_df.filter(pl.col("date").dt.year() == year)
        min_train_days = min(lookback_years, 3) * 220
        if train.height < min_train_days or test.height == 0:
            continue
        scored = []
        for s in strategies:
            score, sortino, mdd = train_score(train[s])
            scored.append((score, sortino, mdd, s))
        score, sortino, mdd, chosen = max(scored, key=lambda x: x[0])
        decisions.append({"year": year, "chosen": chosen, "train_score": score, "train_sortino": sortino, "train_mdd": mdd})
        for d, r in zip(test["date"].to_list(), test[chosen].to_list(), strict=True):
            nav *= 1 + float(r)
            daily.append({"date": d, "nav": nav, "chosen": chosen})
    return pl.DataFrame(daily), pl.DataFrame(decisions)


def benchmark_metrics(name: str, path: Path, start_date) -> dict[str, object] | None:
    if not path.exists():
        return None
    df = pl.read_csv(path, try_parse_dates=True).filter(pl.col("date") >= start_date).sort("date")
    if df.height < 3:
        return None
    return {"strategy": name, **nav_metrics(df["nav"].to_numpy(), df["date"].to_list())}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-years", type=int, default=5)
    args = ap.parse_args()

    local_paths = strategy_files()
    if not local_paths:
        raise FileNotFoundError("no iter_35 daily files found")
    ret_df = load_returns(local_paths)
    daily, decisions = walk_forward(ret_df, args.lookback_years)
    if daily.is_empty():
        raise RuntimeError("walk-forward produced no OOS days")

    daily_path = RESULTS / "iter_36_walk_forward_dynamic_daily.csv"
    decisions_path = RESULTS / "iter_36_walk_forward_dynamic_decisions.csv"
    summary_path = RESULTS / "iter_36_walk_forward_dynamic_summary.csv"
    daily.write_csv(daily_path)
    decisions.write_csv(decisions_path)

    rows = [{"strategy": f"walk_forward_dynamic_{args.lookback_years}y", **nav_metrics(daily["nav"].to_numpy(), daily["date"].to_list())}]
    start_date = daily["date"][0]
    for name, path in [
        ("hybrid_3q7_w60", RESULTS / "latest_true_3q_7c_best_w60_daily.csv"),
        ("hold_2330", RESULTS / "latest_hold_2330_daily.csv"),
    ]:
        row = benchmark_metrics(name, path, start_date)
        if row:
            rows.append(row)
    summary = pl.DataFrame(rows).sort("sortino", descending=True)
    summary.write_csv(summary_path)

    print("=" * 96)
    print(f"iter_36 walk-forward dynamic ({args.lookback_years}y lookback)")
    print("=" * 96)
    print(
        summary.select(
            [
                "strategy",
                pl.col("cagr").mul(100).round(2).alias("cagr_pct"),
                pl.col("sortino").round(3),
                pl.col("sharpe").round(3),
                pl.col("mdd").mul(100).round(2).alias("mdd_pct"),
                pl.col("final_nav").round(0),
            ]
        ).to_pandas().to_string(index=False)
    )
    print(f"\nSaved: {daily_path}")
    print(f"Saved: {decisions_path}")
    print(f"Saved: {summary_path}")


if __name__ == "__main__":
    main()
