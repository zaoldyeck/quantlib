"""回溯標記彙整器:label_runs JSON → backcast registry CSV.

從各月 label_runs 收 admit 主題與成員(role != beneficiary),active_from 取
「該主題首次 admit 月的最早關鍵證據日與標記日孰晚」的保守值(此處用標記月首日,
evidence_date 另記),conviction 取各月演化後的最終值。輸出 registry schema
+ role 欄(walk-forward 的 role gate 需要)。

Run: uv run --project research python -m research.serenity.backfill.consolidate --months 2023-01 ... --out backcast_2023H1.csv
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
REGISTRY_DIR = HERE.parents[0] / "registry"
COLS = ["theme_id", "theme_name", "bottleneck_layer", "active_from", "active_until",
        "company_code", "conviction", "source_note", "evidence_date", "evidence_url",
        "invalidation_criteria", "review_by", "role"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    rows: dict[tuple[str, str], dict] = {}
    for ym in args.months:
        run = json.load(open(HERE / "label_runs" / f"{ym}.json"))
        for cl in run["clusters"]:
            if cl.get("verdict") not in ("admit", "carry_over") and not cl.get("conviction_updates"):
                continue
            theme = cl.get("theme_id") or ""
            if cl.get("verdict") == "admit":
                evs = [e for e in (cl.get("evidence") or []) if e.get("date")]
                ev_date = min((str(e["date"])[:10] for e in evs), default=f"{ym}-01")
                ev_urls = "; ".join(
                    (e.get("source") or "")[:120] for e in evs[:2]
                ) or f"repo:backfill/label_runs/{ym}.json"
                for m in cl.get("members") or []:
                    if m.get("role") == "beneficiary":
                        continue
                    key = (theme, str(m["code"]).zfill(4))
                    rows[key] = {
                        "theme_id": theme, "theme_name": cl.get("theme_name", ""),
                        "bottleneck_layer": cl.get("narrative", "")[:80],
                        "active_from": f"{ym}-01", "active_until": "",
                        "company_code": key[1], "conviction": m.get("conviction", 2),
                        "source_note": m.get("rationale", "")[:300],
                        "evidence_date": ev_date,
                        "evidence_url": f"repo:backfill/label_runs/{ym}.json; {ev_urls}"[:300],
                        "invalidation_criteria": cl.get("invalidation_criteria", ""),
                        "review_by": "", "role": m.get("role", ""),
                    }
            elif cl.get("verdict") == "carry_over":
                # 增量月快照:新成員以該月入冊,既有成員取快照 conviction(演化)
                for m in cl.get("members") or []:
                    if m.get("role") == "beneficiary":
                        continue
                    key = (theme, str(m["code"]).zfill(4))
                    if key in rows:
                        rows[key]["conviction"] = m.get("conviction", rows[key]["conviction"])
                    else:
                        rows[key] = {
                            "theme_id": theme, "theme_name": cl.get("theme_name", ""),
                            "bottleneck_layer": cl.get("narrative", "")[:80],
                            "active_from": f"{ym}-01", "active_until": "",
                            "company_code": key[1], "conviction": m.get("conviction", 2),
                            "source_note": (m.get("rationale") or "carry_over 月新增成員")[:300],
                            "evidence_date": f"{ym}-01",
                            "evidence_url": f"repo:backfill/label_runs/{ym}.json",
                            "invalidation_criteria": cl.get("invalidation_criteria", ""),
                            "review_by": "", "role": m.get("role", ""),
                        }
            for up in cl.get("conviction_updates") or []:
                key = (up.get("theme_id") or theme, str(up.get("code", "")).zfill(4))
                if key in rows:
                    rows[key]["conviction"] = up.get("to", rows[key]["conviction"])

    df = pd.DataFrame(list(rows.values()), columns=COLS)
    out = REGISTRY_DIR / args.out
    df.to_csv(out, index=False)
    print(df[["theme_id", "company_code", "role", "conviction", "active_from"]].to_string(index=False))
    print(f"-> {out} ({len(df)} rows)")


if __name__ == "__main__":
    main()
