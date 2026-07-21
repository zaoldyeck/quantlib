#!/usr/bin/env python3
"""Print the latest Serenity valuation-aware candidate snapshot.

This helper is read-only. It does not refresh data, run a backtest, or place
orders. It exists so the Serenity recommendation skill can quickly inspect the
latest generated research artifact before writing an investor-facing ranking.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


DEFAULT_CANDIDATES = (
    "research/strat_lab/results/"
    "serenity_valuation_methods_replay_2025_scored_candidates.csv"
)

SCORE_COLUMNS = {
    "dcf_peg_blend": "score_dcf_peg_blend",
    "peg": "score_peg",
    "reverse_dcf_gap": "score_reverse_dcf_gap",
    "alpha": "score_alpha",
    "valuation_combo": "score_valuation_combo",
}


def _float_value(row: dict[str, str], key: str) -> float:
    try:
        return float(row.get(key, "") or "-inf")
    except ValueError:
        return float("-inf")


def _latest_rows(rows: Iterable[dict[str, str]]) -> tuple[str, list[dict[str, str]]]:
    all_rows = list(rows)
    if not all_rows:
        raise SystemExit("No rows found in scored candidate artifact.")
    latest_signal = max(row["signal_date"] for row in all_rows if row.get("signal_date"))
    return latest_signal, [row for row in all_rows if row.get("signal_date") == latest_signal]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=".", help="Repository root path.")
    parser.add_argument("--top", type=int, default=10, help="Number of rows to print.")
    parser.add_argument(
        "--method",
        choices=sorted(SCORE_COLUMNS),
        default="dcf_peg_blend",
        help="Score column to sort by.",
    )
    parser.add_argument(
        "--csv",
        default=DEFAULT_CANDIDATES,
        help="Scored candidate artifact, relative to repo unless absolute.",
    )
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = repo / csv_path
    if not csv_path.exists():
        raise SystemExit(f"Missing artifact: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        latest_signal, rows = _latest_rows(reader)

    score_col = SCORE_COLUMNS[args.method]
    ranked = sorted(rows, key=lambda row: _float_value(row, score_col), reverse=True)

    fields = [
        "rank",
        "company_code",
        "company_name",
        "theme_id",
        "theme_name",
        "raw_close",
        "close",
        score_col,
        "dcf_upside",
        "supported_growth",
        "reverse_dcf_gap",
        "peg",
        "monthly_revenue_yoy",
        "yoy_3m",
        "ret_60d",
        "ret_252d",
        "price_to_earning_ratio",
        "price_book_ratio",
        "execution_date",
    ]
    print(",".join(fields))
    for index, row in enumerate(ranked[: args.top], start=1):
        output = {"rank": str(index)}
        output.update(row)
        print(",".join(str(output.get(field, "")) for field in fields))

    print(f"# latest_signal_date={latest_signal}")
    print(f"# method={args.method}")
    print(f"# artifact={csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
