"""iter_41 - hybrid refinement using iter_40 winners.

Generation 1 found that standalone event alpha is not strong enough, but the
best breakout variants may still be useful as satellites. This script combines
them with existing Quality3/Quality5 sleeves while respecting the 10-stock cap:
  - Quality3 + event top7/top5/top3
  - Quality5 + event top5/top3

All blends are annual-rebalanced NAV sleeves and then validated with the same
OOS/DSR/PBO suite.
"""
from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import polars as pl

from iter_38_next_open_hybrid_validation import annual_rebalanced_blend
from iter_40_research_campaign import CAPITAL, validate_daily


RESULTS = Path("research/strat_lab/results")


def parse_slots(name: str) -> int:
    m = re.search(r"top(\d+)", name)
    if not m:
        raise ValueError(f"cannot parse slot count from {name}")
    return int(m.group(1))


def event_candidates(limit: int = 12) -> list[tuple[str, Path, int]]:
    summary = pl.read_csv(RESULTS / "iter_40_research_campaign_summary.csv")
    rows = (
        summary.filter(pl.col("family").str.contains("breakout"))
        .sort(["oos_sortino", "oos_cagr"], descending=[True, True])
        .head(limit)
        .iter_rows(named=True)
    )
    out = []
    for row in rows:
        name = row["name"]
        path = RESULTS / f"iter_40_{name}_daily.csv"
        if path.exists():
            out.append((name, path, parse_slots(name)))
    return out


def validate_blend(name: str, daily: pl.DataFrame, n_trials: int, path: Path) -> dict[str, float | str]:
    row = validate_daily(name, daily, n_trials, {"max_active": 0.0, "trade_days": 0.0, "avg_turnover_trade_day": 0.0})
    row["path"] = str(path)
    return row


def main() -> None:
    quality = [
        ("q3", RESULTS / "latest_q3_daily.csv", 3),
        ("q5", RESULTS / "latest_q5_daily.csv", 5),
    ]
    events = event_candidates()
    rows = []
    n_trials = max(1, len(quality) * len(events) * 10)

    for qname, qpath, qslots in quality:
        for ename, epath, eslots in events:
            if qslots + eslots > 10:
                continue
            for q_weight_pct in range(50, 96, 5):
                q_weight = q_weight_pct / 100
                name = f"iter41_{qname}_{ename}_w{q_weight_pct}"
                daily = annual_rebalanced_blend(qpath, epath, q_weight)
                out_path = RESULTS / f"{name}_daily.csv"
                daily.write_csv(out_path)
                row = validate_blend(name, daily, n_trials, out_path)
                row["q_sleeve"] = qname
                row["event"] = ename
                row["q_weight"] = q_weight
                row["slots"] = qslots + eslots
                rows.append(row)

    if not rows:
        raise RuntimeError("no valid iter41 hybrid rows")

    summary = pl.DataFrame(rows).sort(["oos_sortino", "oos_cagr"], descending=[True, True])
    out = RESULTS / "iter_41_hybrid_refinement_summary.csv"
    summary.write_csv(out)
    print("=" * 120)
    print("iter_41 hybrid refinement")
    print("=" * 120)
    print(
        summary.select(
            [
                "name",
                "q_sleeve",
                "event",
                "slots",
                pl.col("q_weight").mul(100).cast(pl.Int64).alias("q_weight_pct"),
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
        .head(25)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
