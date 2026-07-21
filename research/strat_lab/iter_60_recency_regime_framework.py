"""iter_60 - recency-weighted strategy promotion framework.

This is not another alpha sweep. It codifies the updated research process:

  - recent 1/3/5-year performance decides whether an alpha is alive;
  - long-history OOS and stress slices act as vetoes;
  - cost realism is mandatory for production;
  - strategies are classified as Production / Paper-Watchlist / Research-only
    instead of a single pass/fail bucket.

The output is a reusable promotion report for strategy library governance.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl

RESULTS = Path("research/strat_lab/results")
OUT_PREFIX = "iter_60_recency_regime_framework"
CAPITAL = 1_000_000.0
RF = 0.01
TDPY = 252


@dataclass(frozen=True)
class Candidate:
    key: str
    label: str
    path: Path
    validation_file: Path
    validation_name: str
    cost_status: str
    notes: str


CANDIDATES = [
    Candidate(
        key="iter42",
        label="Corrected Iter42 w59",
        path=RESULTS / "iter42_q3_risk_breakout_top3_w59_daily.csv",
        validation_file=RESULTS / "iter_42_precision_refinement_summary.csv",
        validation_name="iter42_q3_risk_breakout_top3_w59",
        cost_status="costed",
        notes="Production fallback core; superseded by Iter63 sector meta risk overlay in the current ranking.",
    ),
    Candidate(
        key="iter55_raw",
        label="Iter55 mkt63/q3/squeeze raw",
        path=RESULTS / "iter55_iter44_w74_q3_trend_iter52_squeeze_top5_gate_mkt_mom63_q3_ma50_sq_ma50_off100_daily.csv",
        validation_file=RESULTS / "iter_55_squeeze_switch_refinement_summary.csv",
        validation_name="iter55_iter44_w74_q3_trend_iter52_squeeze_top5_gate_mkt_mom63_q3_ma50_sq_ma50_off100",
        cost_status="missing_whole_sleeve_switch_cost",
        notes="High-CAGR research breakthrough, but whole-sleeve switching was free.",
    ),
    Candidate(
        key="iter55_costed36",
        label="Iter55 mkt63/q3/squeeze costed 36bp",
        path=RESULTS / "iter56_iter55_mkt_mom63_q3_ma50_sq_ma50_tax_commission_36bp_daily.csv",
        validation_file=RESULTS / "iter_56_iter55_deploy_validation_summary.csv",
        validation_name="iter55_mkt_mom63_q3_ma50_sq_ma50_tax_commission_36bp",
        cost_status="costed",
        notes="Same iter55 idea after charging whole-sleeve switch friction.",
    ),
    Candidate(
        key="iter57",
        label="Iter57 cost-aware monthly switch",
        path=RESULTS
        / "iter57_iter44_w74_q3_trend_iter52_squeeze_top5_gate_mkt_mom63_q3_ma50_sq_ma50_exit_gate_mkt_mom63_q3_ma50_sq_ma50_monthly_hold20_confirm3_daily.csv",
        validation_file=RESULTS / "iter_57_cost_aware_switch_summary.csv",
        validation_name=(
            "iter57_iter44_w74_q3_trend_iter52_squeeze_top5_gate_mkt_mom63_q3_ma50_sq_ma50"
            "_exit_gate_mkt_mom63_q3_ma50_sq_ma50_monthly_hold20_confirm3"
        ),
        cost_status="costed",
        notes="Best cost-aware low-turnover squeeze switch; not promotable alone after full-history validation.",
    ),
    Candidate(
        key="iter61",
        label="Iter61 recency meta switch",
        path=RESULTS / "iter61_meta_gate_mkt_ma50_q3_ma50_sq_ma50_monthly_lb63_m0_hold20_confirm2_daily.csv",
        validation_file=RESULTS / "iter_61_recency_meta_switch_summary.csv",
        validation_name="iter61_meta_gate_mkt_ma50_q3_ma50_sq_ma50_monthly_lb63_m0_hold20_confirm2",
        cost_status="costed",
        notes="Production core iter42 with gated, recent-evidence promotion into the best iter57 candidate.",
    ),
    Candidate(
        key="iter63_active",
        label="Iter63 sector meta risk overlay",
        path=RESULTS
        / (
            "iter63_iter62_iter42_iter57_gate_tech_rs_mom21_abs21_monthly_lb21_m-5_hold40_confirm2"
            "_gate_mkt_mom21_off75_confirm2_hold10_daily.csv"
        ),
        validation_file=RESULTS / "iter_63_sector_meta_risk_overlay_summary.csv",
        validation_name=(
            "iter63_iter62_iter42_iter57_gate_tech_rs_mom21_abs21_monthly_lb21_m-5_hold40_confirm2"
            "_gate_mkt_mom21_off75_confirm2_hold10"
        ),
        cost_status="costed",
        notes="Active-ETF-aware production candidate: sector leadership meta switch plus market-momentum risk throttle.",
    ),
    Candidate(
        key="iter63_risk",
        label="Iter63 risk-first overlay",
        path=RESULTS
        / (
            "iter63_iter62_iter61_iter56_mkt36_gate_0052_mom21_monthly_lb252_m5_hold40_confirm1"
            "_gate_mkt_mom21_off75_confirm2_hold40_daily.csv"
        ),
        validation_file=RESULTS / "iter_63_sector_meta_risk_overlay_summary.csv",
        validation_name=(
            "iter63_iter62_iter61_iter56_mkt36_gate_0052_mom21_monthly_lb252_m5_hold40_confirm1"
            "_gate_mkt_mom21_off75_confirm2_hold40"
        ),
        cost_status="costed",
        notes="Risk-first production candidate with the highest OOS Sortino among iter63 promotable variants.",
    ),
    Candidate(
        key="iter67",
        label="Iter67 full-switch all-ETF-beater",
        path=RESULTS
        / (
            "iter67_core63_sharpe_iter64_no_overlay_gate_tech_rs_mom21_abs21_monthly_lb42_m-5"
            "_hold60_confirm2_w100_daily.csv"
        ),
        validation_file=RESULTS / "iter_67_partial_bridge_summary.csv",
        validation_name=(
            "iter67_core63_sharpe_iter64_no_overlay_gate_tech_rs_mom21_abs21_monthly_lb42_m-5"
            "_hold60_confirm2_w100"
        ),
        cost_status="costed",
        notes=(
            "Current production champion: stronger Iter63 core plus a 100% full-sleeve attack switch. "
            "It wins 17/17 active ETF same-window tests and does not create a partial-blend union-cap issue."
        ),
    ),
]

STRESS_WINDOWS = [
    ("gfc_2008", date(2008, 1, 2), date(2009, 3, 31)),
    ("euro_2011", date(2011, 7, 1), date(2011, 12, 30)),
    ("china_2015", date(2015, 6, 1), date(2016, 2, 29)),
    ("covid_2020", date(2020, 2, 1), date(2020, 4, 30)),
    ("bear_2022", date(2022, 1, 1), date(2022, 12, 31)),
    ("recent_2025_26", date(2025, 1, 1), date(2026, 5, 8)),
]


def daily_returns(nav: np.ndarray) -> np.ndarray:
    rets = np.zeros_like(nav, dtype=float)
    rets[1:] = nav[1:] / nav[:-1] - 1.0
    return rets


def drawdown(nav: np.ndarray) -> np.ndarray:
    return nav / np.maximum.accumulate(nav) - 1.0


def metrics(nav: np.ndarray, dates: list[date]) -> dict[str, float]:
    if len(nav) < 2:
        return {"cagr": 0.0, "sortino": 0.0, "sharpe": 0.0, "mdd": 0.0, "total_return": 0.0}
    rets = daily_returns(nav)
    years = max((dates[-1] - dates[0]).days / 365.25, len(rets) / TDPY, 1e-9)
    cagr = (nav[-1] / nav[0]) ** (1.0 / years) - 1.0
    vol = float(rets[1:].std(ddof=1) * math.sqrt(TDPY)) if len(rets) > 2 else 0.0
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 0.0
    mdd = float(drawdown(nav).min())
    return {
        "total_return": float(nav[-1] / nav[0] - 1.0),
        "cagr": float(cagr),
        "sortino": float((cagr - RF) / downvol) if downvol > 0 else 0.0,
        "sharpe": float((cagr - RF) / vol) if vol > 0 else 0.0,
        "mdd": mdd,
        "calmar": float(cagr / abs(mdd)) if mdd < 0 else 0.0,
        "final_multiple": float(nav[-1] / nav[0]),
    }


def load_nav(candidate: Candidate) -> pl.DataFrame:
    if not candidate.path.exists():
        raise FileNotFoundError(candidate.path)
    return pl.read_csv(candidate.path, try_parse_dates=True).sort("date").select(["date", "nav"])


def validation_row(candidate: Candidate) -> dict[str, float]:
    if not candidate.validation_file.exists():
        return {}
    df = pl.read_csv(candidate.validation_file)
    if "name" not in df.columns:
        return {}
    hit = df.filter(pl.col("name") == candidate.validation_name)
    if hit.is_empty():
        return {}
    row = hit.row(0, named=True)
    dsr = row.get("cumulative_dsr", row.get("dsr", float("nan")))
    return {
        "validation_cagr": float(row.get("cagr", float("nan"))),
        "validation_sortino": float(row.get("sortino", float("nan"))),
        "validation_mdd": float(row.get("mdd", float("nan"))),
        "validation_oos_cagr": float(row.get("oos_cagr", float("nan"))),
        "validation_oos_sortino": float(row.get("oos_sortino", float("nan"))),
        "validation_oos_mdd": float(row.get("oos_mdd", float("nan"))),
        "boot_cagr_lb": float(row.get("boot_cagr_lb", float("nan"))),
        "dsr": float(dsr),
        "focused_dsr": float(row.get("focused_dsr", row.get("dsr", float("nan")))),
        "cumulative_dsr": float(row.get("cumulative_dsr", float("nan"))),
        "pbo": float(row.get("pbo", float("nan"))),
        "max_active": float(row.get("max_active", float("nan"))),
    }


def slice_df(df: pl.DataFrame, start: date, end: date) -> pl.DataFrame:
    return df.filter((pl.col("date") >= start) & (pl.col("date") <= end))


def tier(row: dict[str, float | str]) -> str:
    costed = row["cost_status"] == "costed"
    dsr = float(row.get("dsr", 0.0))
    pbo = float(row.get("pbo", 1.0))
    boot = float(row.get("boot_cagr_lb", -1.0))
    full_mdd = float(row.get("full_mdd", -1.0))
    y1 = float(row.get("recent_1y_cagr", -1.0))
    y3 = float(row.get("recent_3y_cagr", -1.0))
    stress_mdd = float(row.get("worst_stress_mdd", -1.0))
    if costed and dsr >= 0.95 and pbo < 0.50 and boot > 0.10 and full_mdd > -0.45 and y1 > 0 and y3 > 0:
        return "Production"
    if costed and y1 > 0.50 and y3 > 0.20 and stress_mdd > -0.50 and dsr >= 0.75 and pbo < 0.50:
        return "Paper-Watchlist"
    if not costed:
        return "Research-only"
    return "Rejected"


def main() -> None:
    rows = []
    window_rows = []
    stress_rows = []
    for candidate in CANDIDATES:
        df = load_nav(candidate)
        end = df["date"][-1]
        val = validation_row(candidate)
        row: dict[str, float | str] = {
            "key": candidate.key,
            "label": candidate.label,
            "path": str(candidate.path),
            "cost_status": candidate.cost_status,
            "notes": candidate.notes,
            **val,
        }
        full = metrics(df["nav"].to_numpy(), df["date"].to_list())
        row.update({f"full_{k}": v for k, v in full.items()})

        for label, days in [("recent_1y", 365), ("recent_3y", 365 * 3), ("recent_5y", 365 * 5)]:
            sub = slice_df(df, end - timedelta(days=days), end)
            m = metrics(sub["nav"].to_numpy(), sub["date"].to_list())
            row.update({f"{label}_{k}": v for k, v in m.items()})
            window_rows.append(
                {
                    "key": candidate.key,
                    "label": candidate.label,
                    "window": label,
                    "start": sub["date"][0].isoformat(),
                    "end": sub["date"][-1].isoformat(),
                    "days": sub.height,
                    **m,
                }
            )

        worst_stress = 0.0
        for stress_name, start, stop in STRESS_WINDOWS:
            sub = slice_df(df, start, stop)
            if sub.height < 2:
                continue
            m = metrics(sub["nav"].to_numpy(), sub["date"].to_list())
            worst_stress = min(worst_stress, m["mdd"])
            stress_rows.append(
                {
                    "key": candidate.key,
                    "label": candidate.label,
                    "stress": stress_name,
                    "start": sub["date"][0].isoformat(),
                    "end": sub["date"][-1].isoformat(),
                    "days": sub.height,
                    **m,
                }
            )
        row["worst_stress_mdd"] = worst_stress
        row["tier"] = tier(row)
        rows.append(row)

    summary = (
        pl.DataFrame(rows)
        .with_columns(
            pl.when(pl.col("tier") == "Production")
            .then(0)
            .when(pl.col("tier") == "Paper-Watchlist")
            .then(1)
            .when(pl.col("tier") == "Research-only")
            .then(2)
            .otherwise(3)
            .alias("tier_rank")
        )
        .sort(["tier_rank", "recent_3y_sortino", "recent_1y_cagr"], descending=[False, True, True])
        .drop("tier_rank")
    )
    window = pl.DataFrame(window_rows)
    stress = pl.DataFrame(stress_rows)

    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    window_path = RESULTS / f"{OUT_PREFIX}_window_metrics.csv"
    stress_path = RESULTS / f"{OUT_PREFIX}_stress_metrics.csv"
    report_path = RESULTS / f"{OUT_PREFIX}_report.md"
    summary.write_csv(summary_path)
    window.write_csv(window_path)
    stress.write_csv(stress_path)

    view = summary.select(
        [
            "tier",
            "label",
            "cost_status",
            pl.col("recent_1y_cagr").mul(100).round(2).alias("recent_1y_cagr_pct"),
            pl.col("recent_3y_cagr").mul(100).round(2).alias("recent_3y_cagr_pct"),
            pl.col("recent_5y_cagr").mul(100).round(2).alias("recent_5y_cagr_pct"),
            pl.col("full_cagr").mul(100).round(2).alias("full_cagr_pct"),
            pl.col("full_mdd").mul(100).round(2).alias("full_mdd_pct"),
            pl.col("worst_stress_mdd").mul(100).round(2).alias("worst_stress_mdd_pct"),
            pl.col("boot_cagr_lb").mul(100).round(2).alias("boot_cagr_lb_pct"),
            pl.col("dsr").round(3),
            pl.col("pbo").round(3),
            "notes",
        ]
    )

    report_lines = [
        "# Iter60 Recency / Regime Promotion Report",
        "",
        "Rule: recent windows decide whether alpha is alive; long history and stress slices veto production risk.",
        "",
        view.to_pandas().to_markdown(index=False),
        "",
        f"CSV summary: `{summary_path}`",
        f"Window metrics: `{window_path}`",
        f"Stress metrics: `{stress_path}`",
    ]
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    print("=" * 120)
    print("iter_60 recency/regime promotion framework")
    print("=" * 120)
    print(view.to_pandas().to_string(index=False))
    print(f"\nSaved: {summary_path}")
    print(f"Saved: {window_path}")
    print(f"Saved: {stress_path}")
    print(f"Saved: {report_path}")


if __name__ == "__main__":
    main()
