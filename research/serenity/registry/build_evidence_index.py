"""Registry 證據材料庫建置(零 token 整理步).

掃 docs/serenity/ 各版推薦報告(7-06/7-07/7-07verified/7-09),對每個個股段落
抽取 markdown 連結與來源名,建 `registry/evidence/registry_evidence.jsonl`
(Evergreen 材料庫規格的 Serenity 對應物),並回填 registry `evidence_url` 欄
(有真 URL 者)。輸出「仍缺出處」清單供補搜 agent。

Run: uv run --project research python -m research.serenity.registry.build_evidence_index
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
DOCS = REPO_ROOT / "docs" / "serenity"
REG = Path(__file__).parent / "thesis_registry_2025.csv"
OUT_DIR = Path(__file__).parent / "evidence"
REPORTS = [
    "serenity_recommendation_2026-07-06.md",
    "serenity_recommendation_2026-07-07.md",
    "serenity_recommendation_verified_2026-07-07.md",
    "serenity_recommendation_2026-07-09.md",
]
MD_LINK = re.compile(r"\[([^\]]{2,60})\]\((https?://[^)\s]+)\)")
# 「WebSearch(鉅亨/工商時報…)證實」形態的來源名列舉
NAMED = re.compile(r"WebSearch[^((]*[((]([^))]{4,120})[))]")


def stock_sections(text: str) -> list[tuple[str, str]]:
    """切出個股段落 → (code, body)。標題形如「### 1. 南亞科 2408(上市)…」。"""
    out = []
    heads = [
        (m.start(), m.group(1))
        for m in re.finditer(r"^#{2,4} [^\n]*?(\d{4})\D", text, re.M)
    ]
    for i, (pos, code) in enumerate(heads):
        end = heads[i + 1][0] if i + 1 < len(heads) else len(text)
        out.append((code, text[pos:end]))
    return out


def main() -> None:
    reg = pd.read_csv(REG, dtype=str).fillna("")
    active = reg[reg.active_until == ""]
    active_codes = set(active.company_code)

    evidence: dict[str, list[dict]] = {}
    for name in REPORTS:
        path = DOCS / name
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for code, body in stock_sections(text):
            if code not in active_codes:
                continue
            links = [{"name": t.strip(), "url": u} for t, u in MD_LINK.findall(body)]
            named = [{"name": m.strip(), "url": ""} for m in NAMED.findall(body)]
            if links or named:
                evidence.setdefault(code, []).append(
                    {"report": name, "links": links, "named_sources": named}
                )

    OUT_DIR.mkdir(exist_ok=True)
    out_path = OUT_DIR / "registry_evidence.jsonl"
    with out_path.open("w", encoding="utf-8") as fh:
        for code in sorted(evidence):
            fh.write(json.dumps({"company_code": code, "records": evidence[code]},
                                ensure_ascii=False) + "\n")

    covered_with_url, covered_named_only, updated = [], [], 0
    for code, recs in evidence.items():
        urls = [l["url"] for r in recs for l in r["links"]]
        if urls:
            covered_with_url.append(code)
            ref = f"repo:registry/evidence/registry_evidence.jsonl#{code}; " + "; ".join(urls[:2])
            mask = (reg.company_code == code) & (reg.active_until == "") & (
                reg.evidence_url.str.startswith(("legacy:", "internal:"))
            )
            reg.loc[mask, "evidence_url"] = ref
            updated += int(mask.sum())
        else:
            covered_named_only.append(code)
    reg.to_csv(REG, index=False)

    # 第二步:補搜 agent 的 supplement 材料回填(有 sources 者)
    supp_path = OUT_DIR / "supplement_2026-07.jsonl"
    if supp_path.exists():
        for line in supp_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            code, urls = rec["company_code"], [s["url"] for s in rec.get("sources", []) if s.get("url")]
            if not urls:
                print(f"⚠ 補搜查無實質瓶頸證據:{code} — {rec.get('note', '')}(策展複審)")
                continue
            ref = f"repo:registry/evidence/supplement_2026-07.jsonl#{code}; " + "; ".join(urls[:2])
            mask = (reg.company_code == code) & (reg.active_until == "") & (
                reg.evidence_url.str.startswith(("legacy:", "internal:"))
            )
            reg.loc[mask, "evidence_url"] = ref
        reg.to_csv(REG, index=False)

    missing = sorted(
        active_codes
        - set(covered_with_url)
        - {c for c in active_codes
           if not reg.loc[(reg.company_code == c) & (reg.active_until == ""),
                          "evidence_url"].str.startswith(("legacy:", "internal:")).any()}
    )
    print(f"active members: {len(active_codes)}")
    print(f"evidence jsonl -> {out_path} ({len(evidence)} codes)")
    print(f"registry evidence_url updated rows: {updated}")
    print(f"covered with real URLs: {sorted(covered_with_url)}")
    print(f"named-sources only (no URL): {sorted(covered_named_only)}")
    print(f"STILL MISSING (補搜清單): {missing}")


if __name__ == "__main__":
    main()
