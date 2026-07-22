"""iter_85 - professional investor strategy survey.

This script compares the current Taiwan-equity strategy candidates from an
investment-committee perspective.  It intentionally does not search new
parameters; it validates and ranks already promoted candidates on common
windows, recent regimes, rolling robustness, benchmark excess return, active ETF
comparison, and extra cost stress.
"""
from __future__ import annotations

import math
import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from research.db import connect  # noqa: E402
from research.prices import total_return_series  # noqa: E402


CAPITAL = 1_000_000.0
RF = 0.01
TDPY = 252
RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
OUT_PREFIX = "iter_85_professional_strategy_survey"


@dataclass(frozen=True)
class Candidate:
    key: str
    label: str
    path: Path
    family: str
    stage: str
    note: str


CANDIDATES = [
    Candidate(
        key="iter84_aggressive_4021",
        label="Iter84 Aggressive 40.21",
        path=RESULTS
        / "iter_84_aggressive_champion_validation_iter83_inner1_blend_weekly_lb21_m0_hold40_c1_w0_100_d75_daily.csv",
        family="Iter84 / blended PM allocator",
        stage="backtest_validated_candidate",
        note="Highest OOS CAGR; loses two short-window active ETFs.",
    ),
    Candidate(
        key="iter84_strict_3882",
        label="Iter84 Strict 38.82",
        path=RESULTS
        / "iter_84_aggressive_champion_validation_iter83_inner1_blend_monthly_lb10_m1_hold60_c2_w0_100_d75_daily.csv",
        family="Iter84 / blended PM allocator",
        stage="backtest_validated_candidate",
        note="Best all-active-ETF winner by OOS CAGR.",
    ),
    Candidate(
        key="iter84_conservative_3720",
        label="Iter84 Conservative 37.20",
        path=RESULTS
        / "iter_84_aggressive_champion_validation_iter83_inner0_blend_monthly_lb21_m2_hold5_c1_w0_100_d75_daily.csv",
        family="Iter84 / blended PM allocator",
        stage="backtest_validated_candidate",
        note="Lower PBO and strongest recent 1Y among the main strict candidates.",
    ),
    Candidate(
        key="iter84_recent_chaser_3622",
        label="Iter84 Recent-Fit 36.22",
        path=RESULTS
        / "iter_84_aggressive_champion_validation_iter83_inner5_blend_weekly_lb10_m-1_hold40_c1_w0_100_d75_daily.csv",
        family="Iter84 / blended PM allocator",
        stage="backtest_validated_candidate",
        note="Best recent 1Y strict challenger; included to test recent-performance preference.",
    ),
    Candidate(
        key="iter67_cap6_source_reconciled",
        label="Iter67 / Iter72 cap6 source-reconciled",
        path=RESULTS / "iter_72_source_reconciled_cap_attribution_cap6_daily.csv",
        family="Iter67 lineage",
        stage="backtest_validated_reference",
        note="Older source-reconciled reference; superseded by Iter84 candidates.",
    ),
]


META_FILES = [
    RESULTS / "iter_84_aggressive_champion_validation_champion_validation_summary.csv",
    RESULTS / "iter_84_aggressive_champion_validation_refinement_summary.csv",
    RESULTS / "iter_72_source_reconciled_cap_attribution_summary.csv",
]


def daily_returns(nav: np.ndarray) -> np.ndarray:
    out = np.zeros(len(nav), dtype=float)
    out[1:] = nav[1:] / nav[:-1] - 1.0
    return out


def drawdown(nav: np.ndarray) -> np.ndarray:
    peaks = np.maximum.accumulate(nav)
    return nav / peaks - 1.0


def cagr_from_nav(nav: np.ndarray, dates: list[date]) -> float:
    if len(nav) < 2:
        return float("nan")
    years = max((dates[-1] - dates[0]).days / 365.25, 1e-9)
    return float((nav[-1] / nav[0]) ** (1.0 / years) - 1.0)


def metric_block(daily: pl.DataFrame) -> dict[str, float | str]:
    daily = daily.sort("date")
    dates = daily["date"].to_list()
    nav = daily["nav"].cast(pl.Float64).to_numpy()
    rets = daily_returns(nav)
    cagr = cagr_from_nav(nav, dates)
    vol = float(np.std(rets[1:], ddof=1) * math.sqrt(TDPY)) if len(rets) > 2 else 0.0
    downside = rets[rets < 0]
    downvol = float(np.std(downside, ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 0.0
    dd = drawdown(nav)
    mdd = float(np.min(dd)) if len(dd) else float("nan")
    return {
        "start": dates[0].isoformat(),
        "end": dates[-1].isoformat(),
        "days": float(len(dates)),
        "cagr": cagr,
        "sortino": float((cagr - RF) / downvol) if downvol > 0 else float("nan"),
        "sharpe": float((cagr - RF) / vol) if vol > 0 else float("nan"),
        "vol": vol,
        "mdd": mdd,
        "calmar": float(cagr / abs(mdd)) if mdd < 0 else float("nan"),
        "mdd_date": dates[int(np.argmin(dd))].isoformat() if len(dd) else "",
    }


def slice_window(daily: pl.DataFrame, start: date, end: date) -> pl.DataFrame:
    return daily.filter((pl.col("date") >= start) & (pl.col("date") <= end)).sort("date")


def safe_window_metric(daily: pl.DataFrame, start: date, end: date) -> dict[str, float | str]:
    sub = slice_window(daily, start, end)
    if sub.height < 2:
        return {"start": start.isoformat(), "end": end.isoformat(), "days": float(sub.height), "cagr": float("nan")}
    return metric_block(sub)


def rolling_cagr_stats(daily: pl.DataFrame, window: int) -> dict[str, float]:
    daily = daily.sort("date")
    nav = daily["nav"].cast(pl.Float64).to_numpy()
    if len(nav) <= window:
        return {f"rolling_{window}_min_cagr": float("nan"), f"rolling_{window}_p05_cagr": float("nan")}
    vals = (nav[window:] / nav[:-window]) ** (TDPY / window) - 1.0
    return {
        f"rolling_{window}_min_cagr": float(np.min(vals)),
        f"rolling_{window}_p05_cagr": float(np.quantile(vals, 0.05)),
        f"rolling_{window}_median_cagr": float(np.median(vals)),
    }


def stressed_daily(daily: pl.DataFrame, extra_bps: float) -> pl.DataFrame:
    daily = daily.sort("date")
    nav = daily["nav"].cast(pl.Float64).to_numpy()
    dates = daily["date"].to_list()
    turnover = daily["turnover"].fill_null(0.0).cast(pl.Float64).to_numpy() if "turnover" in daily.columns else np.zeros(len(nav))
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    stressed_rets = rets - turnover * (extra_bps / 10_000.0)
    stressed_nav = CAPITAL * np.cumprod(1.0 + stressed_rets)
    return pl.DataFrame({"date": dates, "nav": stressed_nav, "turnover": turnover})


def load_meta() -> dict[str, dict[str, object]]:
    frames: list[pl.DataFrame] = []
    for path in META_FILES:
        if path.exists():
            frames.append(pl.read_csv(path, infer_schema_length=10000).with_columns(pl.lit(path.name).alias("_meta_file")))
    if not frames:
        return {}
    meta = pl.concat(frames, how="diagonal")
    out: dict[str, dict[str, object]] = {}
    for row in meta.iter_rows(named=True):
        for key in (str(row.get("path") or ""), str(row.get("name") or "")):
            if key:
                current = out.get(key)
                if current is None or float(row.get("oos_cagr") or -999.0) >= float(current.get("oos_cagr") or -999.0):
                    out[key] = row
    return out


def load_benchmark(code: str, start: date, end: date) -> pl.DataFrame:
    con = connect(read_only=True)
    try:
        px = total_return_series(con, code, start.isoformat(), end.isoformat(), market="twse").sort("date")
    finally:
        con.close()
    nav = CAPITAL * (px["adj_close"].cast(pl.Float64).to_numpy() / float(px["adj_close"][0]))
    return pl.DataFrame({"date": px["date"].to_list(), "nav": nav})


def rank_pct(values: list[float], *, higher_is_better: bool = True) -> list[float]:
    arr = np.asarray(values, dtype=float)
    finite = np.isfinite(arr)
    out = np.full(len(arr), 0.5, dtype=float)
    if finite.sum() <= 1:
        return out.tolist()
    order = np.argsort(arr[finite])
    ranks = np.empty(finite.sum(), dtype=float)
    ranks[order] = np.linspace(0.0, 1.0, finite.sum())
    if not higher_is_better:
        ranks = 1.0 - ranks
    out[finite] = ranks
    return out.tolist()


def score_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    components = [
        ("oos_cagr", 0.20, True),
        ("oos_sortino", 0.15, True),
        ("recent_3y_cagr", 0.12, True),
        ("recent_1y_cagr", 0.06, True),
        ("ai_2024_cagr", 0.08, True),
        ("rolling_756_p05_cagr", 0.12, True),
        ("cost25_oos_cagr", 0.10, True),
        ("oos_mdd_abs", 0.06, False),
        ("active_etf_win_ratio", 0.06, True),
        ("dsr", 0.03, True),
        ("pbo", 0.02, False),
    ]
    scores = np.zeros(len(rows), dtype=float)
    for field, weight, high in components:
        vals = [float(row.get(field) or float("nan")) for row in rows]
        scores += weight * np.asarray(rank_pct(vals, higher_is_better=high))
    for row, score in zip(rows, scores):
        row["professional_score"] = float(score)
    return rows


def main() -> None:
    missing = [str(c.path) for c in CANDIDATES if not c.path.exists()]
    if missing:
        raise FileNotFoundError("missing candidate daily files:\n" + "\n".join(missing))

    first_daily = pl.read_csv(CANDIDATES[0].path, try_parse_dates=True).sort("date")
    start = first_daily["date"][0]
    cutoff = first_daily["date"][-1]
    recent_1y_start = cutoff.replace(year=cutoff.year - 1)
    recent_3y_start = cutoff.replace(year=cutoff.year - 3)

    bench_0050 = load_benchmark("0050", start, cutoff)
    bench_2330 = load_benchmark("2330", start, cutoff)
    bench_oos_0050 = metric_block(slice_window(bench_0050, date(2010, 1, 1), date(2025, 12, 31)))
    bench_oos_2330 = metric_block(slice_window(bench_2330, date(2010, 1, 1), date(2025, 12, 31)))
    bench_recent_2330 = metric_block(slice_window(bench_2330, recent_1y_start, cutoff))

    meta = load_meta()
    rows: list[dict[str, object]] = []
    for candidate in CANDIDATES:
        daily = pl.read_csv(candidate.path, try_parse_dates=True).sort("date")
        meta_row = meta.get(str(candidate.path)) or {}
        full = metric_block(daily)
        oos = safe_window_metric(daily, date(2010, 1, 1), date(2025, 12, 31))
        recent_1y = safe_window_metric(daily, recent_1y_start, cutoff)
        recent_3y = safe_window_metric(daily, recent_3y_start, cutoff)
        ai_2024 = safe_window_metric(daily, date(2024, 1, 1), cutoff)
        bear_2022 = safe_window_metric(daily, date(2022, 1, 1), date(2022, 12, 31))
        covid_2020 = safe_window_metric(daily, date(2020, 2, 1), date(2020, 12, 31))
        gfc_2008 = safe_window_metric(daily, date(2008, 1, 1), date(2008, 12, 31))
        cost25 = safe_window_metric(stressed_daily(daily, 25), date(2010, 1, 1), date(2025, 12, 31))
        cost50 = safe_window_metric(stressed_daily(daily, 50), date(2010, 1, 1), date(2025, 12, 31))
        rolling = {**rolling_cagr_stats(daily, 252), **rolling_cagr_stats(daily, 756)}

        active_wins = float(meta_row.get("active_etf_wins") or 0.0)
        active_count = float(meta_row.get("active_etf_count") or 0.0)
        cutoff_is_current = full["end"] == cutoff.isoformat()
        row = {
            "key": candidate.key,
            "label": candidate.label,
            "family": candidate.family,
            "stage": candidate.stage,
            "note": candidate.note,
            "source_path": str(candidate.path),
            "data_start": full["start"],
            "data_cutoff": full["end"],
            "cutoff_is_current": cutoff_is_current,
            "full_cagr": full["cagr"],
            "full_sortino": full["sortino"],
            "full_mdd": full["mdd"],
            "oos_cagr": oos["cagr"],
            "oos_sortino": oos["sortino"],
            "oos_mdd": oos["mdd"],
            "oos_mdd_abs": abs(float(oos["mdd"])),
            "recent_1y_start": recent_1y["start"],
            "recent_1y_end": recent_1y["end"],
            "recent_1y_cagr": recent_1y["cagr"],
            "recent_1y_mdd": recent_1y["mdd"],
            "recent_3y_cagr": recent_3y["cagr"],
            "recent_3y_mdd": recent_3y["mdd"],
            "ai_2024_cagr": ai_2024["cagr"],
            "ai_2024_mdd": ai_2024["mdd"],
            "bear_2022_cagr": bear_2022["cagr"],
            "bear_2022_mdd": bear_2022["mdd"],
            "covid_2020_cagr": covid_2020["cagr"],
            "gfc_2008_cagr": gfc_2008["cagr"],
            "cost25_oos_cagr": cost25["cagr"],
            "cost50_oos_cagr": cost50["cagr"],
            **rolling,
            "dsr": float(meta_row.get("dsr") or float("nan")),
            "pbo": float(meta_row.get("pbo") or float("nan")),
            "max_active": float(meta_row.get("max_active") or float("nan")),
            "avg_turnover_trade_day": float(meta_row.get("avg_turnover_trade_day") or float("nan")),
            "active_etf_wins": active_wins,
            "active_etf_count": active_count,
            "active_etf_win_ratio": active_wins / active_count if active_count else float("nan"),
            "active_etf_losses": str(meta_row.get("active_etf_losses") or ""),
            "strict_promotable": (bool(meta_row.get("strict_promotable")) and cutoff_is_current) if meta_row else False,
            "excess_oos_vs_0050": float(oos["cagr"]) - float(bench_oos_0050["cagr"]),
            "excess_oos_vs_2330": float(oos["cagr"]) - float(bench_oos_2330["cagr"]),
            "excess_recent_1y_vs_2330": float(recent_1y["cagr"]) - float(bench_recent_2330["cagr"]),
        }
        rows.append(row)

    rows = score_rows(rows)
    ranked = pl.DataFrame(rows).sort(
        ["cutoff_is_current", "professional_score", "strict_promotable", "oos_cagr"],
        descending=[True, True, True, True],
    ).with_row_index("professional_rank", offset=1)

    ranked.write_csv(RESULTS / f"{OUT_PREFIX}_ranking.csv")
    pl.DataFrame(
        [
            {
                "benchmark": "0050_total_return",
                "oos_cagr": bench_oos_0050["cagr"],
                "oos_sortino": bench_oos_0050["sortino"],
                "oos_mdd": bench_oos_0050["mdd"],
            },
            {
                "benchmark": "2330_total_return",
                "oos_cagr": bench_oos_2330["cagr"],
                "oos_sortino": bench_oos_2330["sortino"],
                "oos_mdd": bench_oos_2330["mdd"],
                "recent_1y_cagr": bench_recent_2330["cagr"],
            },
        ]
    ).write_csv(RESULTS / f"{OUT_PREFIX}_benchmarks.csv")

    show = ranked.select(
        [
            "professional_rank",
            "label",
            "data_cutoff",
            "cutoff_is_current",
            "professional_score",
            "full_cagr",
            "oos_cagr",
            "recent_3y_cagr",
            "recent_1y_cagr",
            "ai_2024_cagr",
            "oos_sortino",
            "oos_mdd",
            "rolling_756_p05_cagr",
            "cost25_oos_cagr",
            "dsr",
            "pbo",
            "active_etf_wins",
            "active_etf_count",
            "active_etf_losses",
            "max_active",
        ]
    )
    print(f"Data cutoff: {cutoff.isoformat()} / recent 1Y: {recent_1y_start.isoformat()} to {cutoff.isoformat()}")
    print(f"0050 OOS CAGR={bench_oos_0050['cagr']:+.2%}; 2330 OOS CAGR={bench_oos_2330['cagr']:+.2%}")
    print(show.write_csv())
    print(f"Wrote {RESULTS / (OUT_PREFIX + '_ranking.csv')}")
    print(f"Wrote {RESULTS / (OUT_PREFIX + '_benchmarks.csv')}")


if __name__ == "__main__":
    main()
