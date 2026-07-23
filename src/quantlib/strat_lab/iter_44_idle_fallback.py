"""iter_44 - idle-satellite fallback refinement.

iter_43 attribution showed the iter42 event sleeve creates large cash drag when
there are no breakout positions or when the MA200 gate is off. This iteration
tests a simple production rule:

  If the event sleeve has no active positions, route that sleeve to a fallback
  asset instead of cash.

Fallback variants:
  - q3: use the same Quality3 sleeve returns.
  - q3_trend: use Quality3 only when 0050 is above MA200, else cash.
  - 0050: use 0050 total-return returns.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from quantlib.db import connect
sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import CAPITAL, validate_daily
from quantlib.prices import fetch_adjusted_panel


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")


def nav_to_ret(path: Path, col: str) -> pl.DataFrame:
    return (
        pl.read_csv(path, try_parse_dates=True)
        .sort("date")
        .select(["date", "nav"])
        .with_columns(pl.col("nav").pct_change().fill_null(0.0).alias(col))
        .select(["date", col])
    )


def load_market_0050() -> pl.DataFrame:
    con = connect()
    try:
        px = (
            fetch_adjusted_panel(
                con,
                "2005-01-03",
                "2026-05-08",
                codes=["0050"],
                market="twse",
                include_extra_history_days=320,
            )
            .sort("date")
            .with_columns(
                [
                    pl.col("close").pct_change().fill_null(0.0).alias("ret_0050"),
                    pl.col("close").rolling_mean(200).alias("ma200"),
                ]
            )
            .with_columns((pl.col("close") >= pl.col("ma200")).fill_null(True).alias("mkt_up_raw"))
            .with_columns(pl.col("mkt_up_raw").shift(1).fill_null(True).alias("mkt_up"))
            .filter((pl.col("date") >= pl.date(2005, 1, 3)) & (pl.col("date") <= pl.date(2026, 5, 8)))
            .select(["date", "ret_0050", "mkt_up"])
        )
    finally:
        con.close()
    return px


def load_base_returns() -> pl.DataFrame:
    q3 = nav_to_ret(RESULTS / "latest_q3_daily.csv", "ret_q3")
    event = (
        pl.read_csv(RESULTS / "iter_40_breakout_risk_ma200_cash_top3_daily.csv", try_parse_dates=True)
        .sort("date")
        .select(["date", "nav", "active"])
        .with_columns(pl.col("nav").pct_change().fill_null(0.0).alias("ret_event"))
        .select(["date", "ret_event", "active"])
    )
    df = q3.join(event, on="date", how="inner")
    market = load_market_0050()
    if market.height:
        df = df.join(market, on="date", how="left").with_columns(
            [
                pl.col("ret_0050").fill_null(0.0),
                pl.col("mkt_up").fill_null(True),
            ]
        )
    else:
        df = df.with_columns([pl.lit(0.0).alias("ret_0050"), pl.lit(True).alias("mkt_up")])
    return (
        df.with_columns((pl.col("active").shift(1).fill_null(0) > 0).alias("had_position_prev"))
        .with_columns(((pl.col("active") > 0) | pl.col("had_position_prev")).alias("event_invested"))
        .with_columns(
            [
                pl.when(pl.col("event_invested")).then(pl.col("ret_event")).otherwise(0.0).alias("ret_sat_cash"),
                pl.when(pl.col("event_invested")).then(pl.col("ret_event")).otherwise(pl.col("ret_q3")).alias("ret_sat_q3"),
                pl.when(pl.col("event_invested"))
                .then(pl.col("ret_event"))
                .otherwise(pl.when(pl.col("mkt_up")).then(pl.col("ret_q3")).otherwise(0.0))
                .alias("ret_sat_q3_trend"),
                pl.when(pl.col("event_invested")).then(pl.col("ret_event")).otherwise(pl.col("ret_0050")).alias("ret_sat_0050"),
                pl.col("date").dt.year().alias("year"),
            ]
        )
        .sort("date")
        .select(
            [
                "date",
                "year",
                "ret_q3",
                "ret_sat_cash",
                "ret_sat_q3",
                "ret_sat_q3_trend",
                "ret_sat_0050",
            ]
        )
    )


def annual_blend_with_fallback(q_weight: float, fallback: str, base: pl.DataFrame) -> pl.DataFrame:
    sat_col = f"ret_sat_{fallback}"
    if sat_col not in base.columns:
        raise ValueError(f"unknown fallback {fallback}")

    nav = CAPITAL
    rows = []
    sat_weight = 1.0 - q_weight
    for _, sub in base.group_by("year", maintain_order=True):
        cap_q = nav * q_weight
        cap_s = nav * sat_weight
        for d, rq, rs in zip(sub["date"].to_list(), sub["ret_q3"].to_list(), sub[sat_col].to_list(), strict=True):
            cap_q *= 1 + rq
            cap_s *= 1 + rs
            nav = cap_q + cap_s
            rows.append({"date": d, "nav": nav})
    return pl.DataFrame(rows)


def main() -> None:
    rows = []
    fallbacks = ["cash", "q3", "q3_trend", "0050"]
    n_trials = len(fallbacks) * 41
    base = load_base_returns()
    print(f"[iter44] loaded base rows={base.height} candidates={n_trials}", flush=True)
    for fallback in fallbacks:
        print(f"[iter44] fallback={fallback}", flush=True)
        for pct in range(45, 86):
            name = f"iter44_q3_risk_breakout_top3_w{pct}_fallback_{fallback}"
            daily = annual_blend_with_fallback(pct / 100, fallback, base)
            out_path = RESULTS / f"{name}_daily.csv"
            daily.write_csv(out_path)
            row = validate_daily(name, daily, n_trials, {"max_active": 6.0, "trade_days": 0.0, "avg_turnover_trade_day": 0.0})
            row["fallback"] = fallback
            row["q_weight"] = pct / 100
            row["promotable"] = (
                row["dsr"] >= 0.95
                and row["pbo"] < 0.50
                and row["boot_cagr_lb"] > 0.10
                and row["oos_mdd"] > -0.45
            )
            rows.append(row)

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_44_idle_fallback_summary.csv"
    summary.write_csv(out)
    print("=" * 120)
    print("iter_44 idle fallback refinement")
    print("=" * 120)
    print(
        summary.select(
            [
                "name",
                "promotable",
                "fallback",
                pl.col("q_weight").mul(100).round(0).cast(pl.Int64).alias("q_weight_pct"),
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
        .head(30)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
