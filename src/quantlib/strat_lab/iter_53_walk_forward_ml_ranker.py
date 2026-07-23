"""iter_53 - walk-forward ML ranker over all available point-in-time features.

This is a different research path from iter42/44 weight tuning:

  - train only on data available before each test year;
  - target is next-open to future close return, so signal-day features do not
    use same-day execution;
  - model predictions choose top-N holdings at weekly/monthly signal dates;
  - trade simulation reuses iter40's next-open, cost-aware dollar simulator.

The script is intentionally compact: a few economically plausible ML
configurations, then the same DSR/PBO promotion gate as the current champion.
"""
from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import lightgbm as lgb
import numpy as np
import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import (  # noqa: E402
    END,
    START,
    build_price_lookup,
    fetch_market_calendar,
    load_panel,
    risk_multipliers,
    signal_dates_expr,
    simulate,
    validate_daily,
)
from iter_52_ownership_flow_alpha import add_flow_scores, fetch_extra_features  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
CAPITAL = 1_000_000.0
TEST_START_YEAR = 2010


FEATURES = [
    "adv60",
    "vol_ratio",
    "atr_pct",
    "ret120",
    "trend50",
    "trend200",
    "latest_yoy",
    "yoy_delta",
    "inst_flow20",
    "roa_ttm",
    "gross_margin_ttm",
    "f_score_raw",
    "core_score",
    "sat_score",
    "rank_score",
    "quality_score",
    "defensive_quality_score",
    "spike_score",
    "rev_accel_score",
    "industry_catalyst_score",
    "industry_quality_score",
    "foreign_held_ratio",
    "foreign_chg20",
    "foreign_chg60",
    "margin_ratio",
    "short_ratio",
    "sbl_ratio",
    "pbr",
    "dividend_yield",
    "buyback_pct",
    "ownership_quality_score",
    "flow_momentum_score",
    "value_quality_score",
    "buyback_quality_score",
    "squeeze_score",
]


@dataclass(frozen=True)
class MLSpec:
    name: str
    horizon: int
    train_years: int
    rebalance: str
    objective: str = "regression_l1"
    min_adv: float = 50_000_000.0
    require_quality_floor: bool = False


def log(msg: str) -> None:
    print(msg, flush=True)


def prepare_panel() -> tuple[pl.DataFrame, list[date], dict[str, dict[date, float]]]:
    panel, days, market = load_panel()
    extra = fetch_extra_features()
    panel = (
        panel.join(extra, on=["date", "company_code"], how="left")
        .sort(["company_code", "date"])
        .with_columns(pl.col("open").shift(-1).over("company_code").alias("next_open"))
        .with_columns(
            [
                pl.col("outstanding_shares").fill_null(0),
                pl.col("foreign_held_ratio").fill_null(0.0),
                pl.col("foreign_chg20").fill_null(0.0),
                pl.col("foreign_chg60").fill_null(0.0),
                pl.col("margin_balance").fill_null(0),
                pl.col("short_balance").fill_null(0),
                pl.col("sbl_balance").fill_null(0),
                pl.col("margin_ratio").fill_null(0.0),
                pl.col("short_ratio").fill_null(0.0),
                pl.col("sbl_ratio").fill_null(0.0),
                pl.col("pbr").fill_null(999.0),
                pl.col("dividend_yield").fill_null(0.0),
                pl.col("pe").fill_null(999.0),
                pl.col("buyback_pct").fill_null(0.0),
                pl.col("buyback_executed_shares").fill_null(0),
            ]
        )
        .pipe(add_flow_scores)
        .with_columns([pl.col(c).fill_nan(None).fill_null(0.0).cast(pl.Float32) for c in FEATURES])
        .with_columns(pl.col("date").dt.year().alias("year"))
        .rechunk()
    )
    return panel, days, market


def common_filter(spec: MLSpec) -> pl.Expr:
    expr = (
        (~pl.col("is_etf"))
        & (~pl.col("is_finance"))
        & (pl.col("listed_days") >= 252)
        & (pl.col("adv60") >= spec.min_adv)
        & (pl.col("open") > 0)
        & (pl.col("close") > 0)
        & (pl.col("next_open") > 0)
        & (pl.col("atr_pct").is_between(0.005, 0.18))
        & signal_dates_expr(spec.rebalance)
    )
    if spec.require_quality_floor:
        expr &= (
            (pl.col("roa_ttm") >= 0.02)
            & (pl.col("gross_margin_ttm") >= 0.08)
            & (pl.col("f_score_raw") >= 3)
        )
    return expr


def add_labels(panel: pl.DataFrame, horizon: int) -> pl.DataFrame:
    return (
        panel.sort(["company_code", "date"])
        .with_columns(
            [
                pl.col("close").shift(-horizon).over("company_code").alias("fwd_close"),
                pl.col("date").shift(-horizon).over("company_code").alias("fwd_date"),
            ]
        )
        .with_columns(((pl.col("fwd_close") / pl.col("next_open")) - 1.0).clip(-0.60, 1.80).alias("target"))
    )


def fit_predict_year(frame: pl.DataFrame, spec: MLSpec, test_year: int) -> pl.DataFrame:
    train_start = test_year - spec.train_years
    train_end_cutoff = date(test_year, 1, 1)
    train = frame.filter(
        (pl.col("year") >= train_start)
        & (pl.col("year") < test_year)
        & (pl.col("fwd_date") < train_end_cutoff)
        & pl.col("target").is_finite()
    )
    test = frame.filter(pl.col("year") == test_year)
    if train.height < 3_000 or test.height == 0:
        return pl.DataFrame({"date": [], "company_code": [], "score": []}, schema={"date": pl.Date, "company_code": pl.String, "score": pl.Float64})

    x_train = train.select(FEATURES).to_numpy().astype(np.float32, copy=False)
    y_train = train["target"].to_numpy().astype(np.float32, copy=False)
    x_test = test.select(FEATURES).to_numpy().astype(np.float32, copy=False)

    model = lgb.LGBMRegressor(
        objective=spec.objective,
        n_estimators=160,
        learning_rate=0.045,
        num_leaves=31,
        min_child_samples=120,
        subsample=0.75,
        subsample_freq=1,
        colsample_bytree=0.80,
        reg_alpha=0.05,
        reg_lambda=0.25,
        n_jobs=max(1, (os.cpu_count() or 2) - 1),
        random_state=10_000 + test_year + spec.horizon + spec.train_years,
        verbosity=-1,
    )
    model.fit(x_train, y_train)
    pred = model.predict(x_test)
    return test.select(["date", "company_code"]).with_columns(pl.Series("score", pred))


def build_scores(panel: pl.DataFrame, spec: MLSpec) -> pl.DataFrame:
    t0 = time.time()
    frame = add_labels(panel, spec.horizon).filter(common_filter(spec))
    log(
        f"[iter53] {spec.name}: signal_rows={frame.height:,} "
        f"horizon={spec.horizon} train_years={spec.train_years} rebalance={spec.rebalance}"
    )
    parts = []
    for test_year in range(TEST_START_YEAR, END.year + 1):
        yt0 = time.time()
        pred = fit_predict_year(frame, spec, test_year)
        if pred.height:
            parts.append(pred)
        log(f"[iter53] {spec.name} year={test_year} pred_rows={pred.height:,} ({time.time()-yt0:.1f}s)")
    if not parts:
        raise RuntimeError(f"no predictions for {spec.name}")
    scores = pl.concat(parts).sort(["date", "score"], descending=[False, True])
    log(f"[iter53] {spec.name}: total_pred_rows={scores.height:,} elapsed={time.time()-t0:.1f}s")
    return scores


def targets_from_scores(scores: pl.DataFrame, days: list[date], topn: int) -> dict[date, dict[str, float]]:
    ranked = (
        scores.with_columns(pl.col("score").rank("ordinal", descending=True).over("date").alias("rank"))
        .filter(pl.col("rank") <= topn)
        .select(["date", "company_code", "score"])
        .sort(["date", "score"], descending=[False, True])
    )
    day_to_next = {days[i]: days[i + 1] for i in range(len(days) - 1)}
    targets: dict[date, dict[str, float]] = {}
    for d, sub in ranked.group_by("date", maintain_order=True):
        signal_d = d[0] if isinstance(d, tuple) else d
        exec_d = day_to_next.get(signal_d)
        if exec_d is None or sub.is_empty():
            continue
        codes = sub["company_code"].to_list()
        targets[exec_d] = {c: 1.0 / len(codes) for c in codes}
    return targets


def run_variant(
    panel: pl.DataFrame,
    days: list[date],
    market: dict[str, dict[date, float]],
    scores: pl.DataFrame,
    spec: MLSpec,
    topn: int,
    risk_mode: str,
    n_trials: int,
) -> dict[str, object]:
    sim_days = [d for d in days if d >= date(TEST_START_YEAR, 1, 1)]
    targets = targets_from_scores(scores, days, topn)
    codes = {c for target in targets.values() for c in target}
    name = f"iter53_{spec.name}_top{topn}_{risk_mode}"
    daily, stats = simulate(
        sim_days,
        build_price_lookup(panel, codes),
        targets,
        risk_multipliers(sim_days, market, risk_mode),
        persist=True,
    )
    out_path = RESULTS / f"{name}_daily.csv"
    daily.write_csv(out_path)
    row = validate_daily(name, daily, n_trials, stats)
    row["spec"] = spec.name
    row["horizon"] = spec.horizon
    row["train_years"] = spec.train_years
    row["rebalance"] = spec.rebalance
    row["topn"] = topn
    row["risk_mode"] = risk_mode
    row["path"] = str(out_path)
    row["promotable"] = (
        row["dsr"] >= 0.95
        and row["pbo"] < 0.50
        and row["boot_cagr_lb"] > 0.10
        and row["oos_mdd"] > -0.45
        and row["max_active"] <= 10.0
    )
    return row


def specs() -> list[MLSpec]:
    return [
        MLSpec("lgbm_h21_w5_monthly", horizon=21, train_years=5, rebalance="monthly"),
        MLSpec("lgbm_h63_w5_monthly", horizon=63, train_years=5, rebalance="monthly"),
        MLSpec("lgbm_h63_w8_monthly", horizon=63, train_years=8, rebalance="monthly"),
        MLSpec("lgbm_h63_w5_weekly", horizon=63, train_years=5, rebalance="weekly", require_quality_floor=True),
    ]


def main() -> None:
    t0 = time.time()
    panel, days, market = prepare_panel()
    all_specs = specs()
    topns = [3, 5, 7, 10]
    risk_modes = ["none", "ma200_half"]
    n_trials = len(all_specs) * len(topns) * len(risk_modes)
    log(f"[iter53] panel rows={panel.height:,} specs={len(all_specs)} variants={n_trials}")

    rows = []
    for spec in all_specs:
        scores = build_scores(panel, spec)
        scores.write_csv(RESULTS / f"iter53_{spec.name}_scores.csv")
        for topn in topns:
            for risk_mode in risk_modes:
                vt0 = time.time()
                row = run_variant(panel, days, market, scores, spec, topn, risk_mode, n_trials)
                rows.append(row)
                log(
                    f"[iter53] {row['name']}: OOS CAGR={row['oos_cagr']:+.2%} "
                    f"Sortino={row['oos_sortino']:.3f} MDD={row['oos_mdd']:.2%} "
                    f"DSR={row['dsr']:.3f} PBO={row['pbo']:.3f} max_active={row['max_active']:.0f} "
                    f"({time.time()-vt0:.1f}s)"
                )

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_53_walk_forward_ml_ranker_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "spec",
        "horizon",
        "train_years",
        "rebalance",
        "topn",
        "risk_mode",
        pl.col("cagr").mul(100).round(2).alias("full_cagr_pct"),
        pl.col("sortino").round(3).alias("full_sortino"),
        pl.col("mdd").mul(100).round(2).alias("full_mdd_pct"),
        pl.col("oos_cagr").mul(100).round(2).alias("oos_cagr_pct"),
        pl.col("oos_sortino").round(3),
        pl.col("oos_mdd").mul(100).round(2).alias("oos_mdd_pct"),
        pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
        pl.col("dsr").round(3),
        pl.col("pbo").round(3),
        pl.col("max_active").cast(pl.Int64),
    ]
    print("=" * 120)
    print("iter_53 walk-forward ML ranker")
    print("=" * 120)
    print(summary.select(view_cols).head(32).to_pandas().to_string(index=False))
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
    print(f"Elapsed: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
