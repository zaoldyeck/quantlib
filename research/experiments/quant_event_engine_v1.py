"""Pure-quant event engine v1 (campaign C1).

Selector layer = the strongest cross-sectional combos from the spike-factor
study (revenue_momentum / momentum_new_high / blend), fully mechanical, no
thesis registry. Discipline layer = the serenity event engine's validated v2
stack (tp60 recycle, trail/abs/time/thesis stops, dual regime guards,
throttles), reused via import.

Pre-registration and acceptance criteria: docs/strategy_research/quant_campaign_ledger.md.

Run:
  uv run --project research python research/experiments/quant_event_engine_v1.py --start 2018-01-02
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import matplotlib
from research import paths

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "research"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT))
sys.path.insert(0, str(RESEARCH_ROOT / "serenity"))
sys.path.insert(0, str(RESEARCH_ROOT / "strat_lab"))

from research.constants import CAPITAL  # noqa: E402
from research.db import connect  # noqa: E402
from research.prices import total_return_series  # noqa: E402

from replay_2025 import (  # noqa: E402
    benchmark_nav,
    load_point_in_time_table,
    load_price_features,
    load_revenue_features,
    row_latest_before,
    summarize_nav,
)
import engine as eng  # noqa: E402
from evaluation import nav_metrics, trade_distribution_metrics  # noqa: E402

RESULTS = paths.OUT_STRAT_LAB
DOCS = REPO_ROOT / "docs" / "strategy_research"
OUT_PREFIX = "quant_event_engine_v1"

ADV_MIN = 40_000_000
PRICE_MIN = 10.0

FULL_V2 = dict(
    rules=replace(eng.FULL_RULES, take_profit=0.60),
    regime_guard=True,
    max_new_per_day=3,
    adv_cap=0.20,
)

VARIANTS = tuple(
    [
        eng.EngineVariant(f"{sel}_rot", rules=eng.ExitRules(), refresh_rotation=True)
        for sel in ("revmom", "newhigh", "blend")
    ]
    + [eng.EngineVariant(f"{sel}_v2", **FULL_V2) for sel in ("revmom", "newhigh", "blend")]
    + [
        # C2-A: rotation + regime guards only (no price exits)
        eng.EngineVariant(
            "newhigh_rotg", rules=eng.ExitRules(), refresh_rotation=True, regime_guard=True
        ),
        # C2-B: rotation + PIT quality gate (ROA>8% or F-score>=4)
        eng.EngineVariant("newhighq_rot", rules=eng.ExitRules(), refresh_rotation=True),
        # C3: Iter95's discipline recipe — time50(r=-1) as the ONLY exit
        eng.EngineVariant(
            "newhigh_rot_t50", rules=eng.ExitRules(time_days=50), refresh_rotation=True
        ),
        eng.EngineVariant(
            "newhigh_rot_t50g",
            rules=eng.ExitRules(time_days=50),
            refresh_rotation=True,
            regime_guard=True,
        ),
    ]
)

QUARTER_DEADLINE = {1: (5, 22), 2: (8, 21), 3: (11, 21), 4: (4, 7)}


def load_quality_pit() -> pd.DataFrame:
    """First-principles quarterly quality panel with PIT availability dates."""
    q = pl.read_parquet(REPO_ROOT / "research" / "raw_quarterly.parquet").select(
        ["company_code", "year", "quarter", "roa_ttm", "f_score_raw"]
    ).to_pandas()
    q["company_code"] = q["company_code"].astype(str).str.zfill(4)

    def avail(row) -> date:
        m, d = QUARTER_DEADLINE[int(row.quarter)]
        y = int(row.year) + (1 if int(row.quarter) == 4 else 0)
        return date(y, m, d)

    q["available_from"] = [avail(r) for r in q.itertuples(index=False)]
    return q.sort_values(["company_code", "available_from"])


def pct_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True)


def build_selector_frames(day_px: pd.DataFrame, rev_day: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Return per-selector scored frames (company_code, score, drawdown_252, ...)."""
    base = day_px.merge(rev_day, on="company_code", how="left", suffixes=("", "_rev"))
    for col in ("monthly_revenue_yoy", "yoy_3m", "yoy_accel"):
        if col not in base:
            base[col] = np.nan
        base[col] = pd.to_numeric(base[col], errors="coerce")
    base = base[
        (pd.to_numeric(base["adv20"], errors="coerce") >= ADV_MIN)
        & (pd.to_numeric(base["raw_close"], errors="coerce") >= PRICE_MIN)
    ].copy()
    if base.empty:
        return {}
    base["p60"] = pct_rank(base["ret_60d"])
    base["p120"] = pct_rank(base["ret_120d"])
    base["pyoy"] = pct_rank(base["yoy_3m"])
    out: dict[str, pd.DataFrame] = {}

    s1 = base[
        (base["monthly_revenue_yoy"] > 30)
        & (base["yoy_accel"] > 0)
        & (base["p60"] >= 0.80)
        & (base["drawdown_252"] >= -0.10)
    ].copy()
    s1["score"] = pd.to_numeric(s1["ret_120d"], errors="coerce")
    out["revmom"] = s1.sort_values("score", ascending=False)

    s2 = base[(base["drawdown_252"] >= -0.05) & (base["p60"] >= 0.80)].copy()
    s2["score"] = pd.to_numeric(s2["ret_60d"], errors="coerce")
    out["newhigh"] = s2.sort_values("score", ascending=False)

    union = pd.concat([s1, s2]).drop_duplicates("company_code").copy()
    union["score"] = (union["p120"] + union["p60"] + union["pyoy"].fillna(0.5)) / 3.0
    out["blend"] = union.sort_values("score", ascending=False)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2018-01-02")
    parser.add_argument("--end", default=None)
    parser.add_argument("--label", default=None)
    parser.add_argument("--emit-book", default="", help="comma-separated variant names to emit books for")
    args = parser.parse_args()
    emit_names = {s.strip() for s in args.emit_book.split(",") if s.strip()}
    out_prefix = args.label or OUT_PREFIX

    con = connect(read_only=True)
    try:
        cutoff = con.sql("select max(date) from daily_quote").fetchone()[0]
        if args.end:
            cutoff = min(cutoff, date.fromisoformat(args.end))
        start = date.fromisoformat(args.start)
        load_start = start - timedelta(days=420)

        universe = eng.load_universe_history(con, min_avg_trade_value=30_000_000)
        price_features, _ = load_price_features(con, universe, load_start, cutoff)
        revenue = load_revenue_features(con)

        px = price_features.copy()
        px["_date"] = pd.to_datetime(px["date"]).dt.date
        trading_days = sorted(d for d in px["_date"].unique() if start <= d <= cutoff)
        refresh_days = eng.build_refresh_days(trading_days, start, cutoff)

        window = px[px["_date"].isin(set(trading_days))]
        close_by_day = {d: g.set_index("company_code")["close"].to_dict() for d, g in window.groupby("_date")}
        ret_by_day = {
            d: g.set_index("company_code")["ret_1d"].fillna(0.0).to_dict() for d, g in window.groupby("_date")
        }
        adv_by_day = {d: g.set_index("company_code")["adv20"].to_dict() for d, g in window.groupby("_date")}

        bench = total_return_series(
            con, "0050", (start - timedelta(days=250)).isoformat(), cutoff.isoformat(), market="twse"
        ).to_pandas()
        bench = bench.sort_values("date").reset_index(drop=True)
        bench["ma120"] = bench["adj_close"].rolling(120).mean()
        market_risk_off = set(
            pd.to_datetime(bench.loc[bench["adj_close"] < bench["ma120"], "date"]).dt.date
        )

        quality = load_quality_pit()
        scored_by_sel: dict[str, dict[date, pd.DataFrame]] = {
            s: {} for s in ("revmom", "newhigh", "blend", "newhighq")
        }
        thesis_ok_by_refresh: dict[date, dict[str, bool]] = {}
        for day in refresh_days:
            day_px = px[px["_date"] == day]
            rev_day = row_latest_before(revenue, day, "report_date")
            frames = build_selector_frames(day_px, rev_day)
            qual_day = row_latest_before(quality, day, "available_from")
            qual_ok = set(
                qual_day.loc[
                    (pd.to_numeric(qual_day["roa_ttm"], errors="coerce") > 0.08)
                    | (pd.to_numeric(qual_day["f_score_raw"], errors="coerce") >= 4),
                    "company_code",
                ]
            )
            if "newhigh" in frames and not frames["newhigh"].empty:
                frames["newhighq"] = frames["newhigh"][
                    frames["newhigh"]["company_code"].isin(qual_ok)
                ].copy()
            for sel, frame in frames.items():
                if not frame.empty:
                    scored_by_sel[sel][day] = frame.head(40).reset_index(drop=True)
            thesis_ok_by_refresh[day] = {
                str(r.company_code): not (pd.notna(r.yoy_3m) and float(r.yoy_3m) < 0.0)
                for r in rev_day.itertuples(index=False)
            }

        summaries: list[dict[str, object]] = []
        daily_paths: dict[str, Path] = {}
        for variant in VARIANTS:
            sel = variant.name.split("_")[0]
            if sel not in scored_by_sel:
                sel = "newhigh"
            book_sink = {} if variant.name in emit_names else None
            daily, trades, turnover = eng.simulate_event_variant(
                variant,
                trading_days,
                close_by_day,
                ret_by_day,
                scored_by_sel[sel],
                thesis_ok_by_refresh,
                {},
                adv_by_day=adv_by_day,
                market_risk_off=market_risk_off,
                book_sink=book_sink,
            )
            if book_sink:
                book_rows = [
                    {"date": d, "company_code": c, "weight": w}
                    for d, (book, traded) in sorted(book_sink.items())
                    if traded
                    for c, w in sorted(book.items())
                ]
                pd.DataFrame(book_rows).to_csv(
                    RESULTS / f"{out_prefix}_{variant.name}_book.csv", index=False
                )
                print(f"book -> {out_prefix}_{variant.name}_book.csv ({len(book_rows)} rows)")
            path = RESULTS / f"{out_prefix}_{variant.name}_daily.csv"
            daily.to_csv(path, index=False)
            trades.to_csv(RESULTS / f"{out_prefix}_{variant.name}_trades.csv", index=False)
            daily_paths[variant.name] = path
            row = summarize_nav(variant.name, daily, turnover, len(scored_by_sel[sel]))
            if not trades.empty:
                row.update(trade_distribution_metrics(trades["ret"].tolist()))
                row["n_trades"] = len(trades)
            summaries.append(row)

        for code, market, name in (("0050", "twse", "hold_0050"), ("2330", "twse", "hold_2330")):
            daily = benchmark_nav(con, code, market, trading_days[0], trading_days[-1], name)
            path = RESULTS / f"{out_prefix}_{name}_daily.csv"
            daily.to_csv(path, index=False)
            daily_paths[name] = path
            summaries.append(summarize_nav(name, daily, 0.0, 0))

        summary = pd.DataFrame(summaries).sort_values("cagr", ascending=False)
        summary.to_csv(RESULTS / f"{out_prefix}_summary.csv", index=False)

        fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        for name in summary.head(6)["name"]:
            daily = pd.read_csv(daily_paths[name], parse_dates=["date"])
            axes[0].plot(daily["date"], daily["nav"] / daily["nav"].iloc[0], label=name)
            axes[1].plot(daily["date"], daily["nav"] / daily["nav"].cummax() - 1.0, label=name)
        axes[0].set_yscale("log")
        axes[0].set_title(f"Quant event engine v1: NAV ({trading_days[0]}~{trading_days[-1]})")
        axes[0].grid(True, alpha=0.3)
        axes[0].legend(fontsize=8)
        axes[1].set_title("Drawdown")
        axes[1].grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(RESULTS / f"{out_prefix}_overview.png", dpi=160)
        plt.close(fig)

        print(f"data_cutoff={cutoff} window={trading_days[0]}~{trading_days[-1]} refreshes={len(refresh_days)}")
        cols = ["name", "cagr", "recent_1y_cagr", "sharpe", "sortino", "mdd", "calmar", "total_turnover", "n_trades"]
        show = summary.copy()
        for c in cols:
            if c not in show:
                show[c] = np.nan
        print(
            show[cols].to_string(
                index=False,
                formatters={
                    "cagr": "{:.2%}".format,
                    "recent_1y_cagr": "{:.2%}".format,
                    "sharpe": "{:.3f}".format,
                    "sortino": "{:.3f}".format,
                    "mdd": "{:.2%}".format,
                    "calmar": "{:.2f}".format,
                    "total_turnover": "{:.2f}x".format,
                },
            )
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
