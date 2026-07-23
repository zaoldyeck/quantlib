"""Alpha attribution for the current strongest registered strategy.

This is a diagnostic attribution, not an executable target-book backtest. It
uses the Iter70 hierarchical holdings reconstruction to answer where Iter67's
research alpha appears to come from: stock concentration, holding age, trend
phase, and observable factor conditions.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import sys
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.dirname(__file__))

from iter_69_production_audit_and_ablation import ITER67_NAV_PATH  # noqa: E402
from iter_70_hierarchical_position_audit import build_hierarchical_books  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_74_iter67_alpha_attribution"


@dataclass
class ActivePosition:
    entry_date: date
    age: int = 0


def age_bucket(age: int) -> str:
    if age <= 20:
        return "0-20d"
    if age <= 60:
        return "21-60d"
    if age <= 120:
        return "61-120d"
    return "121d+"


def source_nav_metrics() -> dict[str, object]:
    daily = pl.read_csv(ITER67_NAV_PATH, try_parse_dates=True).sort("date")
    dates = daily["date"].to_list()
    navs = daily["nav"].to_numpy().astype(float)
    start = dates[0]
    end = dates[-1]
    years = (end - start).days / 365.25
    full_cagr = (navs[-1] / navs[0]) ** (1 / years) - 1
    one_year_start = max(d for d in dates if d <= date(end.year - 1, end.month, end.day))
    lookup = dict(zip(dates, navs, strict=True))
    one_year_cagr = lookup[end] / lookup[one_year_start] - 1.0
    return {
        "source_start": start,
        "source_end": end,
        "source_full_cagr": full_cagr,
        "source_recent_1y_start": one_year_start,
        "source_recent_1y_cagr": one_year_cagr,
    }


def contribution_frame() -> tuple[pl.DataFrame, dict[str, object]]:
    days, panel, books_by_name, iter67_state = build_hierarchical_books()
    books = books_by_name["iter67_hierarchical"]
    active: dict[str, ActivePosition] = {}
    rows = []

    for idx in range(1, len(days)):
        prev_date = days[idx - 1]
        cur_date = days[idx]
        prev_book = books.get(prev_date, {})
        cur_codes = set(books.get(cur_date, {}))
        prev_codes = set(prev_book)

        for code in prev_codes:
            active.setdefault(code, ActivePosition(entry_date=prev_date))
        for code in list(active):
            if code not in prev_codes and code not in cur_codes:
                active.pop(code, None)

        for code, weight in prev_book.items():
            pos = active.setdefault(code, ActivePosition(entry_date=prev_date))
            rows.append(
                {
                    "date": cur_date,
                    "signal_date": prev_date,
                    "selected": "attack64" if iter67_state.get(prev_date) == "attack" else "core63",
                    "company_code": code,
                    "weight": weight,
                    "holding_age_td": pos.age,
                    "age_bucket": age_bucket(pos.age),
                }
            )

        for code in prev_codes & cur_codes:
            active[code].age += 1
        for code in cur_codes - prev_codes:
            active[code] = ActivePosition(entry_date=cur_date)

    positions = pl.DataFrame(rows)
    feature_cols = [
        "date",
        "company_code",
        "close",
        "ret120",
        "br120",
        "trend50",
        "trend200",
        "latest_yoy",
        "yoy_delta",
        "inst_flow20",
        "roa_ttm",
        "gross_margin_ttm",
        "f_score_raw",
        "industry",
        "industry_ret120",
        "vol_ratio",
        "atr_pct",
    ]
    prev_features = (
        panel.select([c for c in feature_cols if c in panel.columns])
        .rename({"date": "signal_date", "close": "prev_close"})
    )
    cur_prices = panel.select(["date", "company_code", "close"]).rename({"close": "cur_close"})
    finite_ret120 = pl.col("ret120").is_finite()
    finite_br120 = pl.col("br120").is_finite()
    finite_trend50 = pl.col("trend50").is_finite()
    finite_trend200 = pl.col("trend200").is_finite()
    finite_yoy = pl.col("latest_yoy").is_finite()
    finite_yoy_delta = pl.col("yoy_delta").is_finite()
    finite_inst = pl.col("inst_flow20").is_finite()
    finite_roa = pl.col("roa_ttm").is_finite()
    finite_gm = pl.col("gross_margin_ttm").is_finite()
    finite_f = pl.col("f_score_raw").is_finite()
    finite_industry = pl.col("industry_ret120").is_finite()
    frame = (
        positions.join(prev_features, on=["signal_date", "company_code"], how="left")
        .join(cur_prices, on=["date", "company_code"], how="left")
        .filter((pl.col("prev_close") > 0) & (pl.col("cur_close") > 0))
        .with_columns(((pl.col("cur_close") / pl.col("prev_close")) - 1.0).alias("stock_return"))
        .with_columns((pl.col("weight") * pl.col("stock_return")).alias("contribution"))
        .with_columns(
            pl.when(~finite_ret120)
            .then(pl.lit("unknown"))
            .when((pl.col("ret120") < 0) | (finite_trend200 & (pl.col("trend200") < 0)))
            .then(pl.lit("recovery_or_downtrend"))
            .when(pl.col("ret120") < 0.30)
            .then(pl.lit("early_rise"))
            .when((pl.col("ret120") <= 1.00) & ((~finite_br120) | (pl.col("br120") >= 0.80)))
            .then(pl.lit("main_rise"))
            .when((pl.col("ret120") > 1.00) | (finite_trend50 & (pl.col("trend50") > 0.50)))
            .then(pl.lit("late_or_extended"))
            .otherwise(pl.lit("main_rise"))
            .alias("phase")
        )
        .with_columns(
            [
                (finite_br120 & (pl.col("br120") >= 0.90)).alias("near_120d_high"),
                (finite_ret120 & (pl.col("ret120") > 0.0)).alias("positive_120d_momentum"),
                (finite_ret120 & (pl.col("ret120") >= 0.30)).alias("strong_120d_momentum"),
                (finite_ret120 & (pl.col("ret120") >= 1.00)).alias("extended_120d_momentum"),
                (finite_yoy & (pl.col("latest_yoy") >= 30.0)).alias("revenue_growth_30"),
                (finite_yoy_delta & (pl.col("yoy_delta") > 0.0)).alias("revenue_accelerating"),
                (finite_inst & (pl.col("inst_flow20") > 0.0)).alias("positive_inst_flow"),
                (
                    finite_roa
                    & (pl.col("roa_ttm") >= 0.02)
                    & finite_gm
                    & (pl.col("gross_margin_ttm") >= 0.10)
                    & finite_f
                    & (pl.col("f_score_raw") >= 3)
                ).alias("quality_ok"),
                (finite_industry & (pl.col("industry_ret120") > 0.0)).alias("industry_tailwind"),
            ]
        )
    )
    daily = frame.group_by("date").agg(pl.col("contribution").sum().alias("attribution_return")).sort("date")
    approx_nav = float(np.prod(1.0 + daily["attribution_return"].to_numpy())) if daily.height else 1.0
    meta = {
        "attribution_start": daily["date"][0] if daily.height else None,
        "attribution_end": daily["date"][-1] if daily.height else None,
        "attribution_rows": frame.height,
        "attribution_cumulative_return": approx_nav - 1.0,
        **source_nav_metrics(),
    }
    return frame, meta


def summary_tables(frame: pl.DataFrame, meta: dict[str, object]) -> dict[str, pl.DataFrame]:
    total_contribution = float(frame["contribution"].sum())
    positive_contribution = float(frame.filter(pl.col("contribution") > 0)["contribution"].sum())

    def add_share(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            [
                (pl.col("total_contribution") / total_contribution).alias("share_of_net_contribution"),
                (pl.col("positive_contribution") / positive_contribution).alias("share_of_positive_contribution"),
            ]
        )

    by_code = add_share(
        frame.group_by("company_code")
        .agg(
            [
                pl.col("contribution").sum().alias("total_contribution"),
                pl.when(pl.col("contribution") > 0).then(pl.col("contribution")).otherwise(0).sum().alias("positive_contribution"),
                pl.col("contribution").mean().alias("avg_daily_contribution"),
                pl.col("stock_return").mean().alias("avg_stock_return_when_held"),
                (pl.col("stock_return") > 0).mean().alias("win_rate"),
                pl.col("weight").mean().alias("avg_weight"),
                pl.len().alias("held_days"),
            ]
        )
        .sort("total_contribution", descending=True)
    )

    by_phase = add_share(
        frame.group_by("phase")
        .agg(
            [
                pl.col("contribution").sum().alias("total_contribution"),
                pl.when(pl.col("contribution") > 0).then(pl.col("contribution")).otherwise(0).sum().alias("positive_contribution"),
                (pl.col("stock_return") > 0).mean().alias("win_rate"),
                pl.col("weight").mean().alias("avg_weight"),
                pl.len().alias("row_count"),
            ]
        )
        .sort("total_contribution", descending=True)
    )

    by_age = add_share(
        frame.group_by("age_bucket")
        .agg(
            [
                pl.col("contribution").sum().alias("total_contribution"),
                pl.when(pl.col("contribution") > 0).then(pl.col("contribution")).otherwise(0).sum().alias("positive_contribution"),
                (pl.col("stock_return") > 0).mean().alias("win_rate"),
                pl.col("weight").mean().alias("avg_weight"),
                pl.len().alias("row_count"),
            ]
        )
        .sort("age_bucket")
    )

    factor_rows = []
    for factor in [
        "near_120d_high",
        "positive_120d_momentum",
        "strong_120d_momentum",
        "extended_120d_momentum",
        "revenue_growth_30",
        "revenue_accelerating",
        "positive_inst_flow",
        "quality_ok",
        "industry_tailwind",
    ]:
        sub = frame.filter(pl.col(factor))
        factor_rows.append(
            {
                "factor": factor,
                "row_count": sub.height,
                "row_pct": sub.height / frame.height if frame.height else 0.0,
                "total_contribution": float(sub["contribution"].sum()) if sub.height else 0.0,
                "positive_contribution": float(sub.filter(pl.col("contribution") > 0)["contribution"].sum()) if sub.height else 0.0,
                "win_rate": float((sub["stock_return"] > 0).mean()) if sub.height else 0.0,
                "avg_weight": float(sub["weight"].mean()) if sub.height else 0.0,
            }
        )
    factors = add_share(pl.DataFrame(factor_rows).sort("total_contribution", descending=True))

    by_sleeve = add_share(
        frame.group_by("selected")
        .agg(
            [
                pl.col("contribution").sum().alias("total_contribution"),
                pl.when(pl.col("contribution") > 0).then(pl.col("contribution")).otherwise(0).sum().alias("positive_contribution"),
                (pl.col("stock_return") > 0).mean().alias("win_rate"),
                pl.col("weight").mean().alias("avg_weight"),
                pl.len().alias("row_count"),
            ]
        )
        .sort("total_contribution", descending=True)
    )

    meta_df = pl.DataFrame([meta | {"net_contribution_sum": total_contribution, "positive_contribution_sum": positive_contribution}])
    return {
        "meta": meta_df,
        "by_code": by_code,
        "by_phase": by_phase,
        "by_age": by_age,
        "by_factor": factors,
        "by_sleeve": by_sleeve,
    }


def main() -> None:
    frame, meta = contribution_frame()
    tables = summary_tables(frame, meta)
    RESULTS.mkdir(parents=True, exist_ok=True)
    frame.write_csv(RESULTS / f"{OUT_PREFIX}_daily_contributions.csv")
    for name, table in tables.items():
        table.write_csv(RESULTS / f"{OUT_PREFIX}_{name}.csv")

    print("iter_74 Iter67 alpha attribution")
    print(tables["meta"].to_pandas().to_string(index=False))
    for name in ["by_sleeve", "by_phase", "by_age", "by_factor", "by_code"]:
        print(f"\n{name}")
        print(tables[name].head(20).to_pandas().to_string(index=False))
    print(f"\nSaved prefix: {RESULTS / OUT_PREFIX}")


if __name__ == "__main__":
    main()
