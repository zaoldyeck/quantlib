"""EV29 registry_v3 標記 workflow 組裝:2024-10~2026-06(扣 2025-04)。

配方凍結:哲學 v4 + EV28 prompt 逐字;站位 = 每月 10 日後首交易日。
Run: uv run --project . python -m quantlib.evergreen.ev29_build
"""
from __future__ import annotations

import json

import duckdb
from quantlib import paths

SKIP = {"2025-04"}  # pilot 已跑


def main() -> None:
    raw = duckdb.connect(f"{paths.CACHE_DB}", read_only=True)
    dates = [r[0] for r in raw.execute(
        "SELECT DISTINCT date FROM daily_quote WHERE date >= '2024-10-01' ORDER BY date").fetchall()]
    months = []
    for y in (2024, 2025, 2026):
        for m in range(1, 13):
            ym = f"{y}-{m:02d}"
            if ym < "2024-10" or ym > "2026-06" or ym in SKIP:
                continue
            from datetime import date as Date
            stance = min(d for d in dates if d.year == y and d.month == m and d.day > 10)
            months.append({"ym": ym, "date": stance.isoformat()})
    print(f"{len(months)} 個月:", [m["ym"] for m in months])

    md = open("src/quantlib/evergreen/PROMPT_ev28_labeling.md").read()
    seg = md.split("## 標記提示詞", 1)[1]
    body = seg.split("\n---\n", 1)[0].split("\n\n", 1)[1].strip()
    phil = open("src/quantlib/evergreen/data/ev27_phil_inline.txt").read()
    template = body.replace("{哲學全文}", phil)

    SCHEMA = {"type": "object", "properties": {
        "month": {"type": "string"},
        "labels": {"type": "array", "minItems": 0, "maxItems": 15,
                   "items": {"type": "object", "properties": {
                       "code": {"type": "string", "pattern": "^\\d{4}$"},
                       "name": {"type": "string", "maxLength": 20},
                       "theme": {"type": "string", "maxLength": 30},
                       "signal_type": {"type": "string", "maxLength": 40},
                       "event": {"type": "string", "maxLength": 200},
                       "evidence": {"type": "string", "maxLength": 200},
                       "invalidation": {"type": "string", "maxLength": 100},
                       "conviction": {"type": "integer", "minimum": 1, "maximum": 5}},
                       "required": ["code", "name", "theme", "signal_type",
                                    "event", "evidence", "invalidation", "conviction"],
                       "additionalProperties": False}}},
        "required": ["month", "labels"], "additionalProperties": False}

    jobs = []
    for m in months:
        pr = template.replace("{date}", m["date"]).replace("{month}", m["ym"])
        assert "{date}" not in pr and "{month}" not in pr and "{哲學全文}" not in pr
        jobs.append({"label": f"v3:{m['ym']}", "prompt": pr, "month": m["ym"]})

    script = (
        "export const meta = {\n"
        "  name: 'ev29-v3-labeling',\n"
        "  description: 'EV29 registry_v3 標記:純質化搜尋 × 20 個月(Serenity 同窗)',\n"
        "  phases: [{ title: 'Label', detail: '2024-10~2026-06' }],\n"
        "}\n"
        f"const SCHEMA = {json.dumps(SCHEMA, ensure_ascii=False)}\n"
        f"const JOBS = {json.dumps(jobs, ensure_ascii=False)}\n"
        "phase('Label')\n"
        "const out = await parallel(JOBS.map(j => () => agent(j.prompt, {\n"
        "  label: j.label, schema: SCHEMA, model: 'opus', effort: 'max',\n"
        "}).then(v => v && { ...v, month: j.month, arm: 'v3' })))\n"
        "log(`完成 ${out.filter(Boolean).length}/20`)\n"
        "return out.filter(Boolean)\n")
    open("src/quantlib/evergreen/workflows/ev29_labeling.js", "w").write(script)
    print("ev29_labeling.js 就緒")


if __name__ == "__main__":
    main()
