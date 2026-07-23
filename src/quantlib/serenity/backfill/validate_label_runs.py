"""Validate Serenity backfill label_runs against seeds.

Checks, per month:
  1. JSON validity.
  2. v4 record-completeness clause: every seed cluster (each industry in
     momentum_clusters counted as kind=momentum, each in revenue_accel_clusters
     as kind=revenue) has >=1 label cluster whose seed_type matches the kind and
     whose `industry` field carries the seed's original industry name.
  3. Fence sanity: label `fence` == month-end of label_month.
  4. Conviction sanity: convictions are ints in 0..5; every non-book cluster has a
     verdict in {admit,reject,carry_over}.

Run (no cache dependency; pure JSON):
  uv run --project . python src/quantlib/serenity/backfill/validate_label_runs.py 2024-07 2024-08 2024-09 2024-10 2024-11 2024-12
  (no args -> validates every label_runs/*.json that has a matching seed file)
"""
from __future__ import annotations
import calendar
import datetime as dt
import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
SEEDS = BASE / "seeds"
RUNS = BASE / "label_runs"


def month_end(label_month: str) -> str:
    y, m = (int(x) for x in label_month.split("-"))
    return f"{label_month}-{calendar.monthrange(y, m)[1]:02d}"


def industry_matches(seed_industry: str, cluster_industry: str) -> bool:
    """Cluster industry carries the seed's original name (may be annotated).

    Exact, or seed name followed by an annotation opener, so that a short seed
    like "其他" does NOT falsely match "其他電子業".
    """
    if cluster_industry == seed_industry:
        return True
    for sep in ("(", "（", "/", " "):
        if cluster_industry.startswith(seed_industry + sep):
            return True
    return False


def validate_month(label_month: str) -> list[str]:
    errs: list[str] = []
    seed_path = SEEDS / f"{label_month}.json"
    run_path = RUNS / f"{label_month}.json"
    if not seed_path.exists():
        return [f"{label_month}: seed file missing"]
    if not run_path.exists():
        return [f"{label_month}: label_run missing"]
    try:
        seed = json.loads(seed_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return [f"{label_month}: seed JSON invalid: {e}"]
    try:
        run = json.loads(run_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return [f"{label_month}: label_run JSON invalid: {e}"]

    # fence sanity
    exp_fence = month_end(label_month)
    if run.get("fence") != exp_fence:
        errs.append(f"{label_month}: fence {run.get('fence')} != month-end {exp_fence}")

    clusters = run.get("clusters", [])

    # index label clusters by (seed_type, industry)
    seed_kinds = [("momentum_clusters", "momentum"), ("revenue_accel_clusters", "revenue")]
    for seed_key, kind in seed_kinds:
        for sc in seed.get(seed_key, []):
            ind = sc["industry"]
            hit = any(
                c.get("seed_type") == kind and industry_matches(ind, c.get("industry", ""))
                for c in clusters
            )
            if not hit:
                errs.append(f"{label_month}: MISSING record for seed cluster ({kind}) industry={ind!r}")

    # conviction + verdict sanity
    for c in clusters:
        v = c.get("verdict")
        if c.get("seed_type") not in ("book_theme",) and v not in ("admit", "reject", "carry_over"):
            errs.append(f"{label_month}: cluster industry={c.get('industry')!r} bad verdict={v!r}")
        for m in c.get("members", []) or []:
            cv = m.get("conviction")
            if not isinstance(cv, int) or not (0 <= cv <= 5):
                errs.append(f"{label_month}: {c.get('theme_id')} member {m.get('code')} bad conviction={cv!r}")
    return errs


def summarize_month(label_month: str) -> str:
    run = json.loads((RUNS / f"{label_month}.json").read_text(encoding="utf-8"))
    clusters = run.get("clusters", [])
    verdicts: dict[str, int] = {}
    for c in clusters:
        verdicts[c.get("verdict", "?")] = verdicts.get(c.get("verdict", "?"), 0) + 1
    updates = [u for c in clusters for u in (c.get("conviction_updates") or [])]
    return (
        f"{label_month} [{run.get('mode')}] clusters={len(clusters)} "
        f"admit={verdicts.get('admit',0)} reject={verdicts.get('reject',0)} "
        f"carry_over={verdicts.get('carry_over',0)} "
        f"discarded={run.get('sources_discarded_count')} conv_updates={len(updates)}"
    )


def main(argv: list[str]) -> int:
    months = argv or sorted(
        p.stem for p in RUNS.glob("*.json") if (SEEDS / p.name).exists()
    )
    all_errs: list[str] = []
    for m in months:
        errs = validate_month(m)
        all_errs.extend(errs)
        print(summarize_month(m) + ("  OK" if not errs else f"  {len(errs)} ERR"))
    print()
    if all_errs:
        print(f"FAIL: {len(all_errs)} problem(s):")
        for e in all_errs:
            print("  -", e)
        return 1
    print(f"PASS: all {len(months)} month(s) complete + valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
