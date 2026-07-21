"""iter_43 - attribution for the current iter42 champion.

The goal is to identify the next optimization target before sweeping blindly:
annual returns, drawdowns, and component contribution versus the Quality3 and
risk-breakout sleeves.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl


RESULTS = Path("research/strat_lab/results")
CAPITAL = 1_000_000.0


def load_ret(path: Path, name: str) -> pl.DataFrame:
    return (
        pl.read_csv(path, try_parse_dates=True)
        .sort("date")
        .select(["date", "nav"])
        .with_columns(pl.col("nav").pct_change().fill_null(0.0).alias(name))
        .select(["date", name])
    )


def annual_return_expr(col: str) -> pl.Expr:
    return ((1 + pl.col(col)).product() - 1).alias(col)


def main() -> None:
    champion = load_ret(RESULTS / "iter42_q3_risk_breakout_top3_w72_daily.csv", "iter42")
    q3 = load_ret(RESULTS / "latest_q3_daily.csv", "q3")
    event = load_ret(RESULTS / "iter_40_breakout_risk_ma200_cash_top3_daily.csv", "event")
    df = champion.join(q3, on="date", how="inner").join(event, on="date", how="inner")
    df = df.with_columns(pl.col("date").dt.year().alias("year"))

    annual = (
        df.group_by("year", maintain_order=True)
        .agg([annual_return_expr(c) for c in ["iter42", "q3", "event"]])
        .with_columns(
            [
                (pl.col("iter42") - pl.col("q3")).alias("iter42_minus_q3"),
                (pl.col("event") - pl.col("q3")).alias("event_minus_q3"),
            ]
        )
        .sort("year")
    )
    annual.write_csv(RESULTS / "iter_43_iter42_annual_attribution.csv")

    # Rolling drawdown by year for the champion.
    rows = []
    for y, sub in df.group_by("year", maintain_order=True):
        year = y[0] if isinstance(y, tuple) else y
        nav = CAPITAL * (1 + sub["iter42"].to_numpy()).cumprod()
        peak = CAPITAL
        mdd = 0.0
        for v in nav:
            peak = max(peak, float(v))
            mdd = min(mdd, (float(v) - peak) / peak)
        rows.append({"year": year, "iter42_year_mdd": mdd})
    dd = pl.DataFrame(rows)
    report = annual.join(dd, on="year", how="left")
    report.write_csv(RESULTS / "iter_43_iter42_attribution.csv")

    print("=" * 120)
    print("iter_43 iter42 attribution")
    print("=" * 120)
    print(
        report.select(
            [
                "year",
                pl.col("iter42").mul(100).round(2).alias("iter42_pct"),
                pl.col("q3").mul(100).round(2).alias("q3_pct"),
                pl.col("event").mul(100).round(2).alias("event_pct"),
                pl.col("iter42_minus_q3").mul(100).round(2).alias("iter42_minus_q3_pp"),
                pl.col("event_minus_q3").mul(100).round(2).alias("event_minus_q3_pp"),
                pl.col("iter42_year_mdd").mul(100).round(2).alias("iter42_year_mdd_pct"),
            ]
        )
        .to_pandas()
        .to_string(index=False)
    )
    print("\nWorst iter42 years:")
    print(
        report.sort("iter42").select(
            [
                "year",
                pl.col("iter42").mul(100).round(2).alias("iter42_pct"),
                pl.col("q3").mul(100).round(2).alias("q3_pct"),
                pl.col("event").mul(100).round(2).alias("event_pct"),
                pl.col("iter42_year_mdd").mul(100).round(2).alias("iter42_year_mdd_pct"),
            ]
        )
        .head(8)
        .to_pandas()
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
