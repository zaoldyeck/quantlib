"""iter_50 - Quality sleeve slot-count sweep.

Q3 is the current core sleeve, but iter42 only compared Q3 and Q5. This sweep
tests whether a different number of PIT-safe monthly quality holdings improves
the hybrid with the risk-gated breakout top3 satellite while respecting the
10-stock cap.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import polars as pl
from quantlib import paths

sys.path.insert(0, os.path.dirname(__file__))
from iter_13 import run_backtest
from iter_38_next_open_hybrid_validation import annual_rebalanced_blend
from iter_40_research_campaign import validate_daily


RESULTS = Path(f"{paths.OUT_STRAT_LAB}")
EVENT = RESULTS / "iter_40_breakout_risk_ma200_cash_top3_daily.csv"
START = date(2005, 1, 3)
END = date(2026, 5, 8)


def ensure_quality_sleeves() -> list[tuple[str, Path, int]]:
    sleeves = []
    for topn in range(2, 8):
        suffix = f"iter50_q{topn}_mcap_monthly"
        path = RESULTS / f"iter_13_{suffix}_daily.csv"
        if not path.exists():
            print(f"[iter50] building quality sleeve topn={topn}", flush=True)
            run_backtest(
                START,
                END,
                1_000_000.0,
                weight_mode="mcap",
                ranker="mcap",
                freq="monthly",
                universe="twse_tpex",
                topn=topn,
                out_suffix=suffix,
            )
        else:
            print(f"[iter50] using cached quality sleeve topn={topn}", flush=True)
        sleeves.append((f"q{topn}", path, topn))
    return sleeves


def main() -> None:
    sleeves = ensure_quality_sleeves()
    weights = list(range(50, 86))
    n_trials = len(sleeves) * len(weights)
    print(f"[iter50] sleeves={len(sleeves)} candidates={n_trials}", flush=True)
    rows = []
    for qname, qpath, qslots in sleeves:
        print(f"[iter50] quality={qname}", flush=True)
        for pct in weights:
            name = f"iter50_{qname}_risk_breakout_top3_w{pct}"
            daily = annual_rebalanced_blend(qpath, EVENT, pct / 100)
            out_path = RESULTS / f"{name}_daily.csv"
            daily.write_csv(out_path)
            row = validate_daily(
                name,
                daily,
                n_trials,
                {"max_active": float(qslots + 3), "trade_days": 0.0, "avg_turnover_trade_day": 0.0},
            )
            row["q_sleeve"] = qname
            row["q_slots"] = qslots
            row["q_weight"] = pct / 100
            row["promotable"] = (
                row["dsr"] >= 0.95
                and row["pbo"] < 0.50
                and row["boot_cagr_lb"] > 0.10
                and row["oos_mdd"] > -0.45
            )
            rows.append(row)

    summary = pl.DataFrame(rows).sort(["promotable", "oos_sortino", "oos_cagr"], descending=[True, True, True])
    out = RESULTS / "iter_50_quality_slot_sweep_summary.csv"
    summary.write_csv(out)
    view_cols = [
        "name",
        "promotable",
        "q_sleeve",
        "q_slots",
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
    print("=" * 120)
    print("iter_50 quality slot sweep")
    print("=" * 120)
    print(summary.select(view_cols).head(30).to_pandas().to_string(index=False))
    print("\nBest by quality sleeve")
    print(
        summary.sort(["q_slots", "promotable", "oos_sortino", "oos_cagr"], descending=[False, True, True, True])
        .group_by("q_slots", maintain_order=True)
        .head(3)
        .select(view_cols)
        .to_pandas()
        .to_string(index=False)
    )
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
