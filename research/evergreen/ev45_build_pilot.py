"""EV45 pilot 組裝:Fable 哲學 × ev28 操作框架(逐字)→ 2 月標記提示詞。

與原版唯二差異:{哲學全文} 換 Fable 蒸餾版;材料落檔路徑 ev28_news →
ev45_news(實驗隔離,防覆蓋原版材料)。其餘操作規格逐字同構。
月份:2023-08(站位 08-11)、2025-04(站位 04-11)= EV28 pilot 同款子集。

Run: uv run --project research python -m research.evergreen.ev45_build_pilot
依賴 cache: 否
"""
from __future__ import annotations

import json
from pathlib import Path

EG = Path("research/evergreen")
PILOT = {"2023-08": "2023-08-11", "2025-04": "2025-04-11"}


def main() -> None:
    md = (EG / "PROMPT_ev28_labeling.md").read_text()
    seg = md.split("## 標記提示詞", 1)[1]
    body = seg.split("\n---\n", 1)[0].split("\n\n", 1)[1].strip()
    assert "{哲學全文}" in body
    phil = (EG / "data" / "ev45_phil_inline_fable.txt").read_text()
    template = (body.replace("{哲學全文}", phil)
                    .replace("ev28_news/{month}/materials", "ev45_news/{month}/materials"))

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
    for ym, d in PILOT.items():
        pr = template.replace("{date}", d).replace("{month}", ym)
        assert "{date}" not in pr and "{month}" not in pr
        jobs.append({"label": f"ev45:{ym}", "prompt": pr, "month": ym})
        (EG / "data" / "prompts").mkdir(parents=True, exist_ok=True)
        (EG / "data" / "prompts" / f"ev45_{ym}.txt").write_text(pr)

    script = (
        "export const meta = {\n"
        "  name: 'ev45-labeling-pilot',\n"
        "  description: 'EV45 pilot:Fable 哲學 × Opus MAX 標記 × 2 月(對照原版同月)',\n"
        "  phases: [{ title: 'Label' }],\n"
        "}\n"
        f"const SCHEMA = {json.dumps(SCHEMA, ensure_ascii=False)}\n"
        f"const JOBS = {json.dumps(jobs, ensure_ascii=False)}\n"
        "phase('Label')\n"
        "const out = await parallel(JOBS.map(j => () => agent(j.prompt, {\n"
        "  label: j.label, schema: SCHEMA, model: 'opus', effort: 'max',\n"
        "}).then(v => v && { ...v, month: j.month })))\n"
        "return out.filter(Boolean)\n")
    (EG / "workflows" / "ev45_pilot.js").write_text(script)
    print(f"ev45_pilot.js 就緒:{len(jobs)} jobs;單 prompt {len(jobs[0]['prompt']):,} 字"
          f"(原版 ev28 為 9,621)")


if __name__ == "__main__":
    main()
