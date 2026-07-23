"""Realistic-execution road test for the Serenity event engine champion.

Feeds the engine's emitted target book (paper decisions) through the project's
RealisticExecutionSimulator (Fubon tiered fees, 5% participation cap, limit-up/
down blocking, slippage + impact, lot rounding). Book dates are shifted to the
NEXT trading day so simulator open-fills happen after the paper close-decision
(conservative).

Usage:
  uv run --project . python src/quantlib/serenity/engine.py \
      --start 2025-01-01 --emit-book ev_full_tp60_v2
  uv run --project . python src/quantlib/serenity/execution_test.py \
      --variant ev_full_tp60_v2
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import polars as pl
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src" / "quantlib"))
sys.path.insert(0, str(REPO_ROOT / "src" / "quantlib" / "strat_lab"))

from quantlib.db import connect  # noqa: E402
from quantlib.execsim import (  # noqa: E402
    ExecutionConfig,
    RealisticExecutionSimulator,
    load_adjusted_execution_bars,
)
from evaluation import nav_metrics  # noqa: E402

RESULTS = paths.OUT_STRAT_LAB
DOCS = REPO_ROOT / "docs" / "serenity"
PREFIX = "serenity_event_engine_v1"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", default="ev_full_tp60_v2")
    parser.add_argument("--label", default=None)
    args = parser.parse_args()
    tag = args.label or PREFIX

    book_path = RESULTS / f"{tag}_{args.variant}_book.csv"
    paper_path = RESULTS / f"{tag}_{args.variant}_daily.csv"
    book = pd.read_csv(book_path, dtype={"company_code": str}, parse_dates=["date"])
    book["date"] = book["date"].dt.date
    codes = sorted(book["company_code"].unique())

    con = connect(read_only=True)
    try:
        start = min(book["date"])
        end = con.sql("select max(date) from daily_quote").fetchone()[0]
        cal = [
            r[0]
            for r in con.sql(
                f"SELECT DISTINCT date FROM daily_quote WHERE date >= '{start}' AND date <= '{end}' ORDER BY date"
            ).fetchall()
        ]
        next_day = {d: cal[i + 1] for i, d in enumerate(cal[:-1])}
        targets: dict[date, dict[str, float]] = {}
        for d, g in book.groupby("date"):
            shifted = next_day.get(d)
            if shifted is None:
                continue
            targets[shifted] = dict(zip(g["company_code"], g["weight"]))

        bars = load_adjusted_execution_bars(con, codes, cal[0], end)
    finally:
        con.close()

    # Missing-bar guard: the simulator marks held positions at 0 when a bar is
    # absent (halt / data gap), faking a crash. Forward-fill each code's prices
    # over the full calendar with volume 0 (untradable that day, priced at last
    # close) from its first bar onward.
    grid = pl.DataFrame({"date": cal}).join(pl.DataFrame({"company_code": codes}), how="cross")
    bars = (
        grid.join(bars, on=["date", "company_code"], how="left")
        .sort(["company_code", "date"])
        .with_columns(pl.col("close").fill_null(strategy="forward").over("company_code"))
        .with_columns(
            [
                pl.col("open").fill_null(pl.col("close")),
                pl.col("high").fill_null(pl.col("close")),
                pl.col("low").fill_null(pl.col("close")),
                pl.col("volume").fill_null(0.0),
                pl.col("trade_value").fill_null(0.0),
                pl.col("prev_close").fill_null(strategy="forward").over("company_code"),
                pl.col("adv60").fill_null(strategy="forward").over("company_code"),
            ]
        )
        .filter(pl.col("close").is_not_null())
    )

    sim = RealisticExecutionSimulator(bars, ExecutionConfig())
    result = sim.simulate(cal, targets)

    daily = result.daily
    out_daily = RESULTS / f"{tag}_{args.variant}_exec_daily.csv"
    daily.write_csv(out_daily)
    result.fills.write_csv(RESULTS / f"{tag}_{args.variant}_exec_fills.csv")

    exec_m = nav_metrics(daily.select(["date", "nav"]))
    paper = pl.read_csv(paper_path, try_parse_dates=True).select(["date", "nav"])
    paper_m = nav_metrics(paper)

    rows = [
        {"series": "paper (5bps model)", **paper_m},
        {"series": "realistic execution", **exec_m},
    ]
    table = pd.DataFrame(rows)[["series", "cagr", "sharpe", "sortino", "mdd", "calmar"]]
    stats = {k: round(v, 4) if isinstance(v, float) else v for k, v in result.stats.items()}

    print(table.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("execution stats:", stats)

    out_md = DOCS / f"{tag}_execution_{args.variant}.md"
    out_md.write_text(
        "\n".join(
            [
                f"# Realistic execution road test — {args.variant}",
                "",
                f"- book: `{book_path.name}`, {len(targets)} trade days, {len(codes)} codes",
                "- fills at next-day open after paper close-decision (conservative shift)",
                "- config: fubon_odd_lot (tiered fees, 5% participation cap, limit blocks, 5bps + impact slippage)",
                "",
                table.to_markdown(index=False, floatfmt=".4f"),
                "",
                f"- execution stats: `{stats}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(f"report -> {out_md}")


if __name__ == "__main__":
    main()
