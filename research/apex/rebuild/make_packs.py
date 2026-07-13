"""apex 實驗代碼考古重建 — 任務包生成器。

背景:apex campaign 中後期(r03 之後)實驗以 heredoc 執行未落檔,違反
「研發代碼永久留存」鐵律(2026-07-10 立法,apex 收官在前夜)。本工程
從 trials.jsonl(結果完整)+ ledger/batches.md + REPORT.md 反向重建
每個 batch 的正式實驗檔,並以當年 metrics 為 golden 驗證。

本腳本:掃描 trials.jsonl 中無對應代碼的 trial,按 batch 分組,
附上 golden metrics 與文件敘述段落,產出 rebuild/packs.json 供
workflow 平行重建。

Run: uv run --project research python -m research.apex.rebuild.make_packs
依賴 cache: 否(純 ledger/文件解析)
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

APEX = Path("research/apex")
OUT = APEX / "rebuild" / "packs.json"

# batch key 正規化:r18a_n5 → r18;f07b_x → f07b;ev5b_h2 → ev5b;
# b11ef_x → b11;中文 summary runs 歸 "summary";v01/v02 歸自己
def batch_key(name: str) -> str:
    if name.startswith(("全跨度", "正2", "現代era")):
        return "summary_windows"
    if name.startswith(("fullspan_", "holdout_", "matchwin_", "modern_", "oos_")):
        return "summary_windows"
    m = re.match(r"([a-z]+\d+[a-z]*?)(?=_|$)", name)
    if not m:
        return name
    k = m.group(1)
    # r18a → r18(單字母 config 尾碼併回 batch);f07b / ev5b 保留(獨立 batch)
    m2 = re.match(r"^([bnrv]\d{2})[a-z]$", k)
    if m2:
        return m2.group(1)
    return k


def main() -> None:
    bodies = "\n".join(
        p.read_text(errors="ignore") for p in APEX.rglob("*.py"))
    trials, seen = [], set()
    for l in open(APEX / "ledger" / "trials.jsonl"):
        r = json.loads(l)
        n = r.get("name")
        if n and n not in seen:
            seen.add(n)
            trials.append(r)

    missing = [r for r in trials
               if not re.search(rf'["\']({re.escape(r["name"])})["\']', bodies)
               and r["name"] not in bodies]

    groups: dict[str, list] = defaultdict(list)
    for r in missing:
        groups[batch_key(r["name"])].append({
            "name": r["name"], "hypothesis": r.get("hypothesis"),
            "config": r.get("config"), "window": r.get("window"),
            "metrics": r.get("metrics"), "family": r.get("family"),
        })

    batches_md = (APEX / "ledger" / "batches.md").read_text()
    report_md = (APEX / "REPORT.md").read_text()

    def doc_context(key: str, names: list[str]) -> dict:
        pats = {key} | {n.split("_")[0] for n in names}
        ctx = {}
        for label, doc in (("batches.md", batches_md), ("REPORT.md", report_md)):
            hits = []
            for para in doc.split("\n\n"):
                if any(re.search(rf"\b{re.escape(p)}\b", para, re.I) for p in pats):
                    hits.append(para.strip())
            if hits:
                ctx[label] = "\n\n".join(hits)[:6000]
        return ctx

    packs = []
    for key in sorted(groups):
        names = [t["name"] for t in groups[key]]
        packs.append({
            "batch": key,
            "n_trials": len(names),
            "trials": groups[key],
            "docs": doc_context(key, names),
        })

    OUT.write_text(json.dumps(packs, ensure_ascii=False, indent=1))
    print(f"缺檔 trial {len(missing)}/{len(trials)};batch {len(packs)} 個 → {OUT}")
    for p in packs:
        print(f"  {p['batch']:16s} {p['n_trials']:3d} trials  "
              f"docs:{','.join(p['docs']) or '無'}")


if __name__ == "__main__":
    main()
