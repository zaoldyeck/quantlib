"""iter_64 - confirm active-ETF-beater candidates.

The broad iter62/iter63 sweeps found a small family that beats every listed
Taiwan active ETF on each ETF's own live window, but those variants did not pass
the full exploratory DSR gate. This pass is deliberately narrow: it treats that
family as the hypothesis and applies a small, thesis-driven set of costed market
risk throttles. The output reports both:

  - focused_dsr: penalty for this confirmatory candidate set only;
  - cumulative_dsr: penalty including iter62 + iter63 + this pass.

That separation prevents a post-hoc candidate from being promoted as if it had
been designed before the exploratory searches.
"""
from __future__ import annotations

import math
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
from research import paths

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from research.db import connect  # noqa: E402
from research.prices import total_return_series  # noqa: E402
from iter_40_research_campaign import (  # noqa: E402
    CAPITAL,
    N_TRIALS_DSR,
    TDPY,
    deflated_sharpe,
    metrics_from_rets,
    validate_daily,
)
from iter_59_costed_exposure_overlay import load_gates  # noqa: E402
from iter_63_sector_meta_risk_overlay import apply_overlay, load_strategy  # noqa: E402


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
ITER62_SUMMARY = RESULTS / "iter_62_sector_leadership_meta_summary.csv"
OUT_PREFIX = "iter_64_active_etf_beater_confirm"

ACTIVE_ETFS = [
    "00400A",
    "00401A",
    "00980A",
    "00981A",
    "00982A",
    "00984A",
    "00985A",
    "00986A",
    "00987A",
    "00988A",
    "00990A",
    "00991A",
    "00992A",
    "00993A",
    "00994A",
    "00995A",
    "00996A",
]

BASE_NAMES = [
    "iter62_iter42_iter57_gate_tech_rs_mom21_abs21_monthly_lb63_m-5_hold40_confirm2",
    "iter62_iter42_iter57_gate_tech_rs_mom21_abs21_monthly_lb63_m-5_hold40_confirm1",
    "iter62_iter42_iter57_gate_tech_rs_mom21_abs21_monthly_lb126_m0_hold40_confirm1",
]

# Narrow, thesis-driven overlays. These are not a new alpha sweep; they are
# defensive throttles around the already discovered active-ETF-beater family.
OVERLAYS = [
    ("none", 1.00, 0, 0),
    ("gate_mkt_mom21", 0.75, 2, 10),
    ("gate_mkt_mom21", 0.75, 2, 20),
    ("gate_mkt_mom21", 0.90, 2, 10),
    ("gate_mkt_mom21", 0.90, 2, 20),
    ("gate_mkt_dd10", 0.75, 2, 10),
    ("gate_mkt_dd10", 0.75, 2, 20),
    ("gate_mkt_dd15", 0.75, 2, 10),
    ("gate_mkt_ma100_and_mom21", 0.75, 2, 10),
    ("gate_mkt_ma150_or_mom63", 0.75, 2, 20),
]

ITER62_TRIALS = 10_240
ITER63_TRIALS = 3_264
FOCUSED_TRIALS = len(BASE_NAMES) * len(OVERLAYS)
CUMULATIVE_TRIALS = ITER62_TRIALS + ITER63_TRIALS + FOCUSED_TRIALS


def daily_returns(nav: np.ndarray) -> np.ndarray:
    rets = np.zeros_like(nav, dtype=float)
    if len(nav) > 1:
        rets[1:] = nav[1:] / nav[:-1] - 1.0
    return rets


def nav_metrics(df: pl.DataFrame) -> dict[str, float]:
    df = df.sort("date")
    nav = df["nav"].to_numpy()
    dates = df["date"].to_list()
    if len(nav) < 2:
        return {"cagr": 0.0, "sortino": 0.0, "sharpe": 0.0, "mdd": 0.0, "final_nav": float(nav[-1]) if len(nav) else 0.0}
    rets = daily_returns(nav)
    years = max((dates[-1] - dates[0]).days / 365.25, len(rets) / TDPY, 1e-9)
    cagr = (nav[-1] / nav[0]) ** (1.0 / years) - 1.0
    vol = float(rets[1:].std(ddof=1) * math.sqrt(TDPY)) if len(rets) > 2 else 0.0
    downside = rets[rets < 0]
    downvol = float(downside.std(ddof=1) * math.sqrt(TDPY)) if len(downside) > 1 else 0.0
    peak = np.maximum.accumulate(nav)
    mdd = float((nav / peak - 1.0).min())
    return {
        "cagr": float(cagr),
        "sortino": float((cagr - 0.01) / downvol) if downvol > 0 else 0.0,
        "sharpe": float((cagr - 0.01) / vol) if vol > 0 else 0.0,
        "mdd": mdd,
        "final_nav": float(nav[-1]),
    }


def window_metrics(df: pl.DataFrame, days: int) -> dict[str, float | str]:
    df = df.sort("date")
    end = df["date"][-1]
    sub = df.filter(pl.col("date") >= end - timedelta(days=days))
    m = nav_metrics(sub)
    return {
        "recent_1y_start": sub["date"][0].isoformat(),
        "recent_1y_end": sub["date"][-1].isoformat(),
        "recent_1y_days": float(sub.height),
        "recent_1y_cagr": m["cagr"],
        "recent_1y_mdd": m["mdd"],
        "recent_1y_sortino": m["sortino"],
    }


def strict_dsr(daily: pl.DataFrame, n_trials: int) -> float:
    daily = daily.sort("date")
    nav = daily["nav"].to_numpy()
    dates = daily["date"].to_list()
    rets = np.diff(np.concatenate([[CAPITAL], nav])) / np.concatenate([[CAPITAL], nav[:-1]])
    df = pl.DataFrame({"date": dates, "ret": rets}).with_columns(pl.col("date").dt.year().alias("year"))
    oos = df.filter((pl.col("year") >= 2010) & (pl.col("year") <= 2025))
    oos_rets = oos["ret"].to_numpy()
    oos_metrics = metrics_from_rets(oos_rets, oos["date"].to_list())
    return deflated_sharpe(oos_metrics["sharpe"], max(N_TRIALS_DSR, n_trials), oos_rets)


def load_bases() -> pl.DataFrame:
    if not ITER62_SUMMARY.exists():
        raise FileNotFoundError(ITER62_SUMMARY)
    summary = pl.read_csv(ITER62_SUMMARY)
    missing = set(BASE_NAMES) - set(summary["name"].to_list())
    if missing:
        raise ValueError(f"missing iter62 base rows: {sorted(missing)}")
    return summary.filter(pl.col("name").is_in(BASE_NAMES))


def load_active_etfs(start: date, end: date) -> dict[str, pl.DataFrame]:
    con = connect(read_only=True)
    try:
        out = {}
        for code in ACTIVE_ETFS:
            px = total_return_series(con, code, start.isoformat(), end.isoformat(), market="twse")
            if px.height >= 2:
                out[code] = px.sort("date").select(["date", "adj_close"])
        return out
    finally:
        con.close()


def compare_active_etfs(name: str, daily: pl.DataFrame, etfs: dict[str, pl.DataFrame]) -> tuple[dict[str, float | str], list[dict[str, float | str]]]:
    rows = []
    for code, etf in etfs.items():
        joined = (
            daily.sort("date")
            .select(["date", "nav"])
            .join(etf, on="date", how="inner")
            .sort("date")
        )
        if joined.height < 2:
            continue
        strat_total = float(joined["nav"][-1] / joined["nav"][0] - 1.0)
        etf_total = float(joined["adj_close"][-1] / joined["adj_close"][0] - 1.0)
        gap = strat_total - etf_total
        rows.append(
            {
                "strategy": name,
                "etf": code,
                "start": joined["date"][0].isoformat(),
                "end": joined["date"][-1].isoformat(),
                "days": float(joined.height),
                "strategy_total_return": strat_total,
                "etf_total_return": etf_total,
                "gap": gap,
                "win": bool(gap > 0),
            }
        )
    if not rows:
        return {
            "active_etf_count": 0.0,
            "active_etf_wins": 0.0,
            "active_etf_avg_gap": 0.0,
            "active_etf_min_gap": 0.0,
            "active_etf_losses": "",
        }, rows
    gaps = np.array([float(r["gap"]) for r in rows])
    losses = [str(r["etf"]) for r in rows if not bool(r["win"])]
    return {
        "active_etf_count": float(len(rows)),
        "active_etf_wins": float(sum(bool(r["win"]) for r in rows)),
        "active_etf_avg_gap": float(gaps.mean()),
        "active_etf_min_gap": float(gaps.min()),
        "active_etf_losses": ",".join(losses),
    }, rows


def make_candidate(
    base_row: dict[str, float | str],
    base_daily: pl.DataFrame,
    gates: pl.DataFrame,
    gate: str,
    off: float,
    confirm: int,
    hold: int,
) -> tuple[str, pl.DataFrame, dict[str, float | str]]:
    if gate == "none":
        nav = CAPITAL * np.cumprod(1.0 + base_daily["base_ret"].to_numpy())
        daily = pl.DataFrame({"date": base_daily["date"].to_list(), "nav": nav})
        stats = {"overlay_switches": 0.0, "avg_exposure": 1.0, "overlay_cost_total": 0.0}
        name = f"iter64_{base_row['name']}_no_overlay"
    else:
        daily, overlay_stats = apply_overlay(base_daily, gates, gate, off, confirm, hold)
        stats = overlay_stats
        off_tag = int(round(off * 100))
        name = f"iter64_{base_row['name']}_{gate}_off{off_tag}_confirm{confirm}_hold{hold}"
    extra: dict[str, float | str] = {
        "base": str(base_row["name"]),
        "gate": gate,
        "off_mult": float(off),
        "confirm_days": float(confirm),
        "min_hold_days": float(hold),
        "overlay_switches": float(stats["overlay_switches"]),
        "avg_exposure": float(stats["avg_exposure"]),
        "overlay_cost_total": float(stats["overlay_cost_total"]),
    }
    return name, daily, extra


def main() -> None:
    bases = load_bases()
    gates = load_gates()
    strategy_cache = {row["name"]: load_strategy(str(row["path"])) for row in bases.iter_rows(named=True)}
    start = min(df["date"][0] for df in strategy_cache.values())
    end = max(df["date"][-1] for df in strategy_cache.values())
    etfs = load_active_etfs(start, end)
    print(
        f"[iter64] bases={bases.height} overlays={len(OVERLAYS)} "
        f"focused_trials={FOCUSED_TRIALS} cumulative_trials={CUMULATIVE_TRIALS} etfs={len(etfs)}",
        flush=True,
    )

    summary_rows = []
    compare_rows = []
    for base_row in bases.iter_rows(named=True):
        base_daily = strategy_cache[base_row["name"]]
        for gate, off, confirm, hold in OVERLAYS:
            name, daily, extra = make_candidate(base_row, base_daily, gates, gate, off, confirm, hold)
            out_path = RESULTS / f"{name}_daily.csv"
            daily.write_csv(out_path)
            active_summary, active_rows = compare_active_etfs(name, daily, etfs)
            focused = validate_daily(
                name,
                daily,
                FOCUSED_TRIALS,
                {
                    "max_active": float(base_row.get("max_active", 6.0)),
                    "trade_days": float(extra["overlay_switches"]),
                    "avg_turnover_trade_day": float(extra["overlay_cost_total"]) / max(float(extra["overlay_switches"]), 1.0),
                },
            )
            row = {
                **focused,
                "path": str(out_path),
                **extra,
                **window_metrics(daily, 365),
                **active_summary,
                "focused_dsr": float(focused["dsr"]),
                "cumulative_dsr": strict_dsr(daily, CUMULATIVE_TRIALS),
                "focused_trials": float(FOCUSED_TRIALS),
                "cumulative_trials": float(CUMULATIVE_TRIALS),
            }
            row["beats_all_active_etfs"] = row["active_etf_wins"] == row["active_etf_count"]
            row["focused_promotable"] = (
                row["focused_dsr"] >= 0.95
                and row["pbo"] < 0.50
                and row["boot_cagr_lb"] > 0.10
                and row["oos_mdd"] > -0.45
            )
            row["strict_promotable"] = (
                row["cumulative_dsr"] >= 0.95
                and row["pbo"] < 0.50
                and row["boot_cagr_lb"] > 0.10
                and row["oos_mdd"] > -0.45
            )
            if row["strict_promotable"] and row["beats_all_active_etfs"]:
                row["classification"] = "Production-All-ETF-Beater"
            elif row["focused_promotable"] and row["beats_all_active_etfs"]:
                row["classification"] = "Focused-Pass / Cumulative-Watchlist"
            elif row["beats_all_active_etfs"]:
                row["classification"] = "All-ETF-Beater / Research"
            else:
                row["classification"] = "Rejected"
            summary_rows.append(row)
            compare_rows.extend(active_rows)
            print(
                f"[iter64] {name}: wins={row['active_etf_wins']:.0f}/{row['active_etf_count']:.0f} "
                f"OOS={row['oos_cagr']:+.2%} Sortino={row['oos_sortino']:.3f} "
                f"1Y={row['recent_1y_cagr']:+.2%} focused_DSR={row['focused_dsr']:.3f} "
                f"cum_DSR={row['cumulative_dsr']:.3f}",
                flush=True,
            )

    sort_cols = [
        "strict_promotable",
        "focused_promotable",
        "beats_all_active_etfs",
        "active_etf_wins",
        "active_etf_min_gap",
        "oos_sortino",
    ]
    summary = pl.DataFrame(summary_rows).sort(sort_cols, descending=[True, True, True, True, True, True])
    summary_path = RESULTS / f"{OUT_PREFIX}_summary.csv"
    compare_path = RESULTS / f"{OUT_PREFIX}_active_etf_comparison.csv"
    summary.write_csv(summary_path)
    pl.DataFrame(compare_rows).write_csv(compare_path)
    print(f"[iter64] wrote {summary_path}", flush=True)
    print(f"[iter64] wrote {compare_path}", flush=True)
    print(summary.head(12).select([
        "classification",
        "name",
        "active_etf_wins",
        "active_etf_min_gap",
        "oos_cagr",
        "oos_sortino",
        "oos_mdd",
        "recent_1y_cagr",
        "focused_dsr",
        "cumulative_dsr",
        "pbo",
    ]).to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
