"""iter_52 - ownership, flow, value, and buyback alpha families.

This generation intentionally avoids another weight/gate tweak around iter42.
It tests new point-in-time signal families that were not part of iter40:

  - foreign ownership accumulation
  - margin-crowding avoidance
  - valuation plus quality
  - buyback support
  - short/sbl squeeze pressure

All signals are observed at signal-day close and executed at the next open.
The simulator is reused from iter40, so transaction costs, max-holding checks,
and DSR/PBO validation stay on the same basis as the current champion.
"""
from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from quantlib.db import connect  # noqa: E402

sys.path.insert(0, os.path.dirname(__file__))
from iter_40_research_campaign import (  # noqa: E402
    END,
    START,
    CampaignConfig,
    build_price_lookup,
    fetch_market_calendar,
    load_panel,
    risk_multipliers,
    signal_dates_expr,
    simulate,
    validate_daily,
)


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")


@dataclass(frozen=True)
class FlowConfig:
    name: str
    score_kind: str
    topn: int
    rebalance: str = "weekly"
    persist: bool = True
    risk_mode: str = "none"
    min_adv: float = 80_000_000.0
    min_roa: float | None = 0.04
    min_gm: float | None = 0.12
    min_fscore: int | None = 3
    require_trend: bool = False
    min_foreign_chg20: float | None = None
    max_margin_ratio: float | None = None
    require_buyback: bool = False
    require_short_pressure: bool = False


def log(msg: str) -> None:
    print(msg, flush=True)


def z_expr(col: str, lo: float, hi: float) -> pl.Expr:
    return ((pl.col(col) - lo) / (hi - lo) * 2 - 1).clip(-1.0, 1.0)


def fetch_extra_features() -> pl.DataFrame:
    """Fetch deterministic point-in-time features from the local DuckDB cache."""
    con = connect()
    try:
        foreign = con.sql(
            f"""
            SELECT
                date,
                company_code,
                outstanding_shares,
                foreign_held_ratio
            FROM foreign_holding_ratio
            WHERE date BETWEEN DATE '{START}' AND DATE '{END}'
            """
        ).pl()
        margin = con.sql(
            f"""
            SELECT
                date,
                company_code,
                margin_balance,
                short_balance
            FROM margin_transactions
            WHERE date BETWEEN DATE '{START}' AND DATE '{END}'
            """
        ).pl()
        sbl = con.sql(
            f"""
            SELECT
                date,
                company_code,
                daily_balance AS sbl_balance
            FROM sbl_borrowing
            WHERE date BETWEEN DATE '{START}' AND DATE '{END}'
            """
        ).pl()
        value = con.sql(
            f"""
            SELECT
                date,
                company_code,
                price_book_ratio AS pbr,
                dividend_yield,
                price_to_earning_ratio AS pe
            FROM stock_per_pbr
            WHERE date BETWEEN DATE '{START}' AND DATE '{END}'
            """
        ).pl()
        buyback = con.sql(
            f"""
            WITH days AS (
                SELECT DISTINCT date
                FROM daily_quote
                WHERE date BETWEEN DATE '{START}' AND DATE '{END}'
            )
            SELECT
                d.date,
                b.company_code,
                MAX(COALESCE(b.pct_of_capital, 0.0)) AS buyback_pct,
                MAX(COALESCE(b.executed_shares, 0)) AS buyback_executed_shares
            FROM days d
            JOIN treasury_stock_buyback b
              ON d.date BETWEEN b.announce_date AND b.period_end
            GROUP BY d.date, b.company_code
            """
        ).pl()
    finally:
        con.close()

    foreign = (
        foreign.sort(["company_code", "date"])
        .with_columns(
            [
                pl.col("foreign_held_ratio").diff(20).over("company_code").alias("foreign_chg20"),
                pl.col("foreign_held_ratio").diff(60).over("company_code").alias("foreign_chg60"),
            ]
        )
        .select(["date", "company_code", "outstanding_shares", "foreign_held_ratio", "foreign_chg20", "foreign_chg60"])
    )
    margin = margin.select(["date", "company_code", "margin_balance", "short_balance"])
    sbl = sbl.select(["date", "company_code", "sbl_balance"])
    value = value.select(["date", "company_code", "pbr", "dividend_yield", "pe"])
    buyback = buyback.select(["date", "company_code", "buyback_pct", "buyback_executed_shares"])

    return (
        foreign.join(margin, on=["date", "company_code"], how="left")
        .join(sbl, on=["date", "company_code"], how="left")
        .join(value, on=["date", "company_code"], how="left")
        .join(buyback, on=["date", "company_code"], how="left")
        .with_columns(
            [
                (pl.col("margin_balance") / pl.col("outstanding_shares")).alias("margin_ratio"),
                (pl.col("short_balance") / pl.col("outstanding_shares")).alias("short_ratio"),
                (pl.col("sbl_balance") / pl.col("outstanding_shares")).alias("sbl_ratio"),
                pl.col("buyback_pct").fill_null(0.0),
                pl.col("buyback_executed_shares").fill_null(0),
            ]
        )
    )


def add_flow_scores(panel: pl.DataFrame) -> pl.DataFrame:
    panel = panel.with_columns(
        [
            z_expr("foreign_held_ratio", 5.0, 45.0).fill_null(0.0).alias("z_foreign_level"),
            z_expr("foreign_chg20", -2.0, 2.0).fill_null(0.0).alias("z_foreign_chg20"),
            z_expr("foreign_chg60", -4.0, 4.0).fill_null(0.0).alias("z_foreign_chg60"),
            z_expr("inst_flow20", -0.10, 0.20).fill_null(0.0).alias("z_inst_flow"),
            (-z_expr("margin_ratio", 0.01, 0.12)).fill_null(0.0).alias("z_low_margin"),
            z_expr("short_ratio", 0.002, 0.06).fill_null(0.0).alias("z_short_pressure"),
            z_expr("sbl_ratio", 0.005, 0.12).fill_null(0.0).alias("z_sbl_pressure"),
            (-z_expr("pbr", 1.0, 6.0)).fill_null(0.0).alias("z_value_pbr"),
            z_expr("dividend_yield", 0.0, 8.0).fill_null(0.0).alias("z_dividend"),
            z_expr("buyback_pct", 0.0, 8.0).fill_null(0.0).alias("z_buyback"),
            (pl.col("close") / pl.col("ma50") - 1).alias("trend50"),
            (pl.col("close") / pl.col("ma200") - 1).alias("trend200"),
            (pl.col("vol") / pl.col("vol_avg60")).alias("vol_ratio"),
        ]
    )
    return panel.with_columns(
        [
            (
                0.28 * pl.col("quality_score")
                + 0.22 * pl.col("z_foreign_chg20")
                + 0.16 * pl.col("z_inst_flow")
                + 0.16 * pl.col("z_low_margin")
                + 0.12 * pl.col("z_foreign_level")
                + 0.06 * z_expr("ret120", -0.20, 0.70).fill_null(0.0)
            ).alias("ownership_quality_score"),
            (
                0.24 * pl.col("quality_score")
                + 0.22 * z_expr("ret120", -0.20, 0.70).fill_null(0.0)
                + 0.18 * pl.col("z_foreign_chg20")
                + 0.14 * pl.col("z_inst_flow")
                + 0.12 * pl.col("z_low_margin")
                + 0.10 * z_expr("latest_yoy", -10.0, 80.0).fill_null(0.0)
            ).alias("flow_momentum_score"),
            (
                0.32 * pl.col("quality_score")
                + 0.20 * pl.col("z_value_pbr")
                + 0.14 * pl.col("z_dividend")
                + 0.14 * pl.col("z_low_margin")
                + 0.12 * z_expr("ret120", -0.20, 0.50).fill_null(0.0)
                + 0.08 * pl.col("z_foreign_chg60")
            ).alias("value_quality_score"),
            (
                0.34 * pl.col("z_buyback")
                + 0.20 * pl.col("quality_score")
                + 0.18 * z_expr("ret120", -0.20, 0.70).fill_null(0.0)
                + 0.16 * pl.col("z_low_margin")
                + 0.12 * pl.col("z_foreign_chg20")
            ).alias("buyback_quality_score"),
            (
                0.26 * z_expr("ret120", -0.10, 0.80).fill_null(0.0)
                + 0.20 * pl.col("z_short_pressure")
                + 0.18 * pl.col("z_sbl_pressure")
                + 0.16 * z_expr("vol_ratio", 0.8, 3.0).fill_null(0.0)
                + 0.12 * z_expr("latest_yoy", 0.0, 80.0).fill_null(0.0)
                + 0.08 * pl.col("quality_score")
            ).alias("squeeze_score"),
        ]
    )


def score_expr(kind: str) -> pl.Expr:
    return pl.col(kind)


def candidate_filter(cfg: FlowConfig) -> pl.Expr:
    expr = (
        (~pl.col("is_etf"))
        & (~pl.col("is_finance"))
        & (pl.col("listed_days") >= 250)
        & (pl.col("adv60") >= cfg.min_adv)
        & (pl.col("open") > 0)
        & (pl.col("close") > 0)
    )
    if cfg.min_roa is not None:
        expr &= pl.col("roa_ttm") >= cfg.min_roa
    if cfg.min_gm is not None:
        expr &= pl.col("gross_margin_ttm") >= cfg.min_gm
    if cfg.min_fscore is not None:
        expr &= pl.col("f_score_raw") >= cfg.min_fscore
    if cfg.require_trend:
        expr &= (pl.col("trend50") > 0) & (pl.col("trend200") > -0.10)
    if cfg.min_foreign_chg20 is not None:
        expr &= pl.col("foreign_chg20") >= cfg.min_foreign_chg20
    if cfg.max_margin_ratio is not None:
        expr &= pl.col("margin_ratio") <= cfg.max_margin_ratio
    if cfg.require_buyback:
        expr &= pl.col("buyback_pct") > 0
    if cfg.require_short_pressure:
        expr &= (pl.col("short_ratio") >= 0.005) | (pl.col("sbl_ratio") >= 0.01)
    return expr & signal_dates_expr(cfg.rebalance)


def build_targets(panel: pl.DataFrame, days: list[date], cfg: FlowConfig) -> dict[date, dict[str, float]]:
    score = "__score"
    rank = "__rank"
    candidates = (
        panel.filter(candidate_filter(cfg))
        .with_columns(score_expr(cfg.score_kind).fill_null(-999.0).alias(score))
        .filter(pl.col(score).is_finite())
        .sort(["date", score], descending=[False, True])
        .with_columns(pl.col(score).rank("ordinal", descending=True).over("date").alias(rank))
        .filter(pl.col(rank) <= cfg.topn)
        .select(["date", "company_code"])
    )
    day_to_next = {days[i]: days[i + 1] for i in range(len(days) - 1)}
    targets: dict[date, dict[str, float]] = {}
    for d, sub in candidates.group_by("date", maintain_order=True):
        signal_d = d[0] if isinstance(d, tuple) else d
        exec_d = day_to_next.get(signal_d)
        if exec_d is None or sub.is_empty():
            continue
        codes = sub["company_code"].to_list()
        targets[exec_d] = {c: 1.0 / len(codes) for c in codes}
    return targets


def build_configs() -> list[FlowConfig]:
    configs: list[FlowConfig] = []
    for score in ("ownership_quality_score", "flow_momentum_score"):
        for topn in (3, 5, 7, 10):
            for rebalance in ("weekly", "monthly"):
                for risk_mode in ("none", "ma200_half"):
                    configs.append(
                        FlowConfig(
                            name=f"{score}_top{topn}_{rebalance}_{risk_mode}",
                            score_kind=score,
                            topn=topn,
                            rebalance=rebalance,
                            risk_mode=risk_mode,
                            min_foreign_chg20=-1.0,
                            max_margin_ratio=0.12,
                            require_trend=True,
                        )
                    )

    for topn in (3, 5, 7, 10):
        for rebalance in ("monthly", "weekly"):
            configs.append(
                FlowConfig(
                    name=f"value_quality_top{topn}_{rebalance}",
                    score_kind="value_quality_score",
                    topn=topn,
                    rebalance=rebalance,
                    risk_mode="none",
                    min_adv=50_000_000.0,
                    min_roa=0.03,
                    min_gm=0.10,
                    max_margin_ratio=0.10,
                    require_trend=False,
                )
            )
            configs.append(
                FlowConfig(
                    name=f"value_quality_top{topn}_{rebalance}_riskhalf",
                    score_kind="value_quality_score",
                    topn=topn,
                    rebalance=rebalance,
                    risk_mode="ma200_half",
                    min_adv=50_000_000.0,
                    min_roa=0.03,
                    min_gm=0.10,
                    max_margin_ratio=0.10,
                    require_trend=False,
                )
            )

    for topn in (3, 5, 7):
        for rebalance in ("weekly", "monthly"):
            configs.append(
                FlowConfig(
                    name=f"buyback_quality_top{topn}_{rebalance}",
                    score_kind="buyback_quality_score",
                    topn=topn,
                    rebalance=rebalance,
                    min_adv=30_000_000.0,
                    min_roa=0.00,
                    min_gm=0.05,
                    min_fscore=2,
                    require_buyback=True,
                    require_trend=False,
                )
            )

    for topn in (3, 5, 7):
        for rebalance in ("weekly", "monthly"):
            configs.append(
                FlowConfig(
                    name=f"squeeze_top{topn}_{rebalance}",
                    score_kind="squeeze_score",
                    topn=topn,
                    rebalance=rebalance,
                    min_adv=80_000_000.0,
                    min_roa=0.00,
                    min_gm=0.05,
                    min_fscore=2,
                    require_short_pressure=True,
                    require_trend=True,
                )
            )
    return configs


def main() -> None:
    t0 = time.time()
    panel, days, market = load_panel()
    extra = fetch_extra_features()
    log(f"[iter52] join extra features rows={extra.height:,}")
    panel = (
        panel.join(extra, on=["date", "company_code"], how="left")
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
        .rechunk()
    )
    configs = build_configs()
    log(f"[iter52] panel rows={panel.height:,} configs={len(configs)} load_elapsed={time.time()-t0:.1f}s")

    rows = []
    n_trials = len(configs)
    for i, cfg in enumerate(configs, 1):
        cfg_t0 = time.time()
        targets = build_targets(panel, days, cfg)
        codes = {c for target in targets.values() for c in target}
        if not codes:
            log(f"[iter52] {i:03d}/{len(configs)} {cfg.name}: no candidates")
            continue
        daily, stats = simulate(
            days,
            build_price_lookup(panel, codes),
            targets,
            risk_multipliers(days, market, cfg.risk_mode),
            cfg.persist,
        )
        out_path = RESULTS / f"iter52_{cfg.name}_daily.csv"
        daily.write_csv(out_path)
        row = validate_daily(cfg.name, daily, n_trials, stats)
        row["score_kind"] = cfg.score_kind
        row["topn"] = cfg.topn
        row["rebalance"] = cfg.rebalance
        row["risk_mode"] = cfg.risk_mode
        row["path"] = str(out_path)
        row["promotable"] = (
            row["dsr"] >= 0.95
            and row["pbo"] < 0.50
            and row["boot_cagr_lb"] > 0.10
            and row["oos_mdd"] > -0.45
            and row["max_active"] <= 10.0
        )
        rows.append(row)
        log(
            f"[iter52] {i:03d}/{len(configs)} {cfg.name}: "
            f"OOS CAGR={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
            f"MDD={row['oos_mdd']:.2%} DSR={row['dsr']:.3f} PBO={row['pbo']:.3f} "
            f"max_active={row['max_active']:.0f} ({time.time()-cfg_t0:.1f}s)"
        )

    if not rows:
        raise RuntimeError("no iter52 results")

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_52_ownership_flow_alpha_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "score_kind",
        "topn",
        "rebalance",
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
    print("iter_52 ownership/flow alpha")
    print("=" * 120)
    print(summary.select(view_cols).head(30).to_pandas().to_string(index=False))
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
