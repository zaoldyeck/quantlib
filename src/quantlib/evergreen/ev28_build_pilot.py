"""EV28 標記 pilot workflow 組裝(prompt 逐字取自 PROMPT_ev28_labeling.md v2)。

- 站位:每月 10 日後首交易日(4 個 pilot 月)
- {哲學全文} 代入 ev27_phil_inline.txt;{date}/{month} 代入
- 產出 workflows/ev28_pilot.js

Run: uv run --project . python -m quantlib.evergreen.ev28_build_pilot
"""
from __future__ import annotations

import json

PILOT_DATES = {"2023-02": "2023-02-13", "2023-08": "2023-08-11",
               "2024-03": "2024-03-11", "2025-04": "2025-04-11"}


def main() -> None:
    md = open("src/quantlib/evergreen/PROMPT_ev28_labeling.md").read()
    seg = md.split("## 標記提示詞", 1)[1]
    body = seg.split("\n---\n", 1)[0].split("\n\n", 1)[1].strip()
    phil = open("src/quantlib/evergreen/data/ev27_phil_inline.txt").read()
    assert "{哲學全文}" in body
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
    for ym, d in PILOT_DATES.items():
        pr = template.replace("{date}", d).replace("{month}", ym)
        assert "{date}" not in pr and "{month}" not in pr
        jobs.append({"label": f"label:{ym}", "prompt": pr, "month": ym})

    script = (
        "export const meta = {\n"
        "  name: 'ev28-labeling-pilot',\n"
        "  description: 'EV28 純質化標記 pilot × 4 個月(月中站位,真搜尋)',\n"
        "  phases: [{ title: 'Label' }],\n"
        "}\n"
        f"const SCHEMA = {json.dumps(SCHEMA, ensure_ascii=False)}\n"
        f"const JOBS = {json.dumps(jobs, ensure_ascii=False)}\n"
        "phase('Label')\n"
        "const out = await parallel(JOBS.map(j => () => agent(j.prompt, {\n"
        "  label: j.label, schema: SCHEMA, model: 'opus', effort: 'max',\n"
        "}).then(v => v && { ...v, month: j.month, arm: 'ev28' })))\n"
        "return out.filter(Boolean)\n")
    open("src/quantlib/evergreen/workflows/ev28_pilot.js", "w").write(script)
    print(f"ev28_pilot.js 就緒:{len(jobs)} jobs;單 prompt {len(jobs[0]['prompt'])} 字")


if __name__ == "__main__":
    main()
