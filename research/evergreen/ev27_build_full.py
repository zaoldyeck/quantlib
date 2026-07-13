"""EV27 全量歸因 workflow 組裝:26 批(208 檔;16 檔小樣本已完成不重做)。

- 樣本 = EV1 全集 224 − 小樣本 16;分批清單落 ev27_batches.json(審計/續跑)
- prompt 逐字取自 PROMPT_ev27_attribution.md v6(使用者 review 版)
- 產出 workflows/ev27_full.js

Run: uv run --project research python -m research.evergreen.ev27_build_full
"""
from __future__ import annotations

import glob
import json
import re

DATA = "research/evergreen/data"


def main() -> None:
    smoke = json.load(open(f"{DATA}/ev27_smoketest_samples.json"))
    done = {(r["code"], r["t0"]) for r in smoke["surge"] + smoke["control"]}
    surge, ctrl = [], []
    for f in sorted(glob.glob(f"{DATA}/ev18_packs/surge_*.json")):
        surge += json.load(open(f))
    for f in sorted(glob.glob(f"{DATA}/ev18_packs/control_*.json")):
        ctrl += json.load(open(f))

    def keep(x, kind):
        base = {k: x.get(k) for k in ("code", "name", "industry", "t0")}
        if kind == "s":
            base["gain_60d"] = x.get("gain_60d")
        else:
            base["prior_120d_gain"] = x.get("prior_120d_gain")
            base["fwd60_max"] = x.get("fwd60_max")
        return base

    def dedup(rows):
        seen, out = set(), []
        for x in rows:
            k = (x["code"], x["t0"])
            if k not in seen:
                seen.add(k)
                out.append(x)
        return out

    s_rest = dedup([keep(x, "s") for x in surge if (x["code"], x["t0"]) not in done])
    c_rest = dedup([keep(x, "c") for x in ctrl if (x["code"], x["t0"]) not in done])
    s_batches = [s_rest[i:i + 8] for i in range(0, len(s_rest), 8)]
    c_batches = [c_rest[i:i + 8] for i in range(0, len(c_rest), 8)]
    json.dump({"surge_batches": s_batches, "control_batches": c_batches},
              open(f"{DATA}/ev27_batches.json", "w"), ensure_ascii=False, indent=1)
    print(f"暴漲 {len(s_rest)} 檔/{len(s_batches)} 批;偽形 {len(c_rest)} 檔/{len(c_batches)} 批")

    md = open("research/evergreen/PROMPT_ev27_attribution.md").read()

    def extract(marker):
        seg = md.split(marker, 1)[1]
        return seg.split("\n---\n", 1)[0].split("\n\n", 1)[1].strip()

    p_surge, p_ctrl = extract("## 提示詞一"), extract("## 提示詞二")

    def fs(rows):
        return "\n".join(
            f"- {r['code']} {r['name']}({r['industry']}),t0 = {r['t0']},"
            f"60 日漲幅 +{r['gain_60d'] * 100:.0f}%" for r in rows)

    def fc(rows):
        return "\n".join(
            f"- {r['code']} {r['name']}({r['industry']}),t0 = {r['t0']},"
            f"前期漲幅 +{r['prior_120d_gain'] * 100:.0f}%,"
            f"後續 60 日最高僅 +{(r['fwd60_max'] or 0) * 100:.0f}%" for r in rows)

    NEWS_ITEM = {"type": "object", "properties": {
        "date": {"type": "string"}, "source": {"type": "string"},
        "content": {"type": "string", "maxLength": 200},
        "pricing_state": {"type": "string", "maxLength": 30}},
        "required": ["date", "source", "content"], "additionalProperties": False}
    S_SCHEMA = {"type": "object", "properties": {
        "findings": {"type": "array", "items": {"type": "object", "properties": {
            "code": {"type": "string"},
            "driver_narrative": {"type": "string", "maxLength": 500},
            "pre_t0_news": {"type": "array", "items": NEWS_ITEM},
            "post_t0_news": {"type": "array", "items": NEWS_ITEM},
            "catalyst_verdict": {"type": "string", "maxLength": 60}},
            "required": ["code", "driver_narrative", "pre_t0_news",
                         "post_t0_news", "catalyst_verdict"],
            "additionalProperties": False}},
        "patterns": {"type": "string", "maxLength": 3000},
        "news_report": {"type": "string", "maxLength": 1800}},
        "required": ["findings", "patterns", "news_report"],
        "additionalProperties": False}
    C_SCHEMA = {"type": "object", "properties": {
        "findings": {"type": "array", "items": {"type": "object", "properties": {
            "code": {"type": "string"},
            "why_failed_narrative": {"type": "string", "maxLength": 500},
            "pre_t0_news": {"type": "array", "items": NEWS_ITEM},
            "news_diagnosis": {"type": "string", "maxLength": 400}},
            "required": ["code", "why_failed_narrative", "pre_t0_news",
                         "news_diagnosis"],
            "additionalProperties": False}},
        "patterns": {"type": "string", "maxLength": 3000},
        "news_report": {"type": "string", "maxLength": 1800}},
        "required": ["findings", "patterns", "news_report"],
        "additionalProperties": False}

    job_defs = []
    for i, b in enumerate(s_batches):
        pr = re.sub(r"\{樣本清單[^}]*\}", lambda m: fs(b), p_surge)
        pr = pr.replace("以下 8 檔股票", f"以下 {len(b)} 檔股票")
        job_defs.append({"label": f"s{i:02d}", "prompt": pr, "schema": "S"})
    for i, b in enumerate(c_batches):
        pr = re.sub(r"\{樣本清單[^}]*\}", lambda m: fc(b), p_ctrl)
        pr = pr.replace("以下 8 檔股票", f"以下 {len(b)} 檔股票")
        job_defs.append({"label": f"c{i:02d}", "prompt": pr, "schema": "C"})
    assert all("{樣本清單" not in j["prompt"] for j in job_defs)

    script = (
        "export const meta = {\n"
        "  name: 'ev27-full-attribution',\n"
        "  description: 'EV27 全量歸因:純質化消息面 × 26 批(208 檔;16 檔小樣本已完成)',\n"
        "  phases: [{ title: 'Attribution', detail: '20 surge + 6 control' }],\n"
        "}\n"
        f"const S_SCHEMA = {json.dumps(S_SCHEMA, ensure_ascii=False)}\n"
        f"const C_SCHEMA = {json.dumps(C_SCHEMA, ensure_ascii=False)}\n"
        f"const JOBS = {json.dumps(job_defs, ensure_ascii=False)}\n"
        "phase('Attribution')\n"
        "const out = await parallel(JOBS.map(j => () => agent(j.prompt, {\n"
        "  label: j.label, schema: j.schema === 'S' ? S_SCHEMA : C_SCHEMA,\n"
        "  model: 'opus', effort: 'max',\n"
        "}).then(v => v && { ...v, batch: j.label })))\n"
        "log(`完成 ${out.filter(Boolean).length}/26`)\n"
        "return out.filter(Boolean)\n")
    open("research/evergreen/workflows/ev27_full.js", "w").write(script)
    print(f"ev27_full.js 就緒:{len(job_defs)} jobs")


if __name__ == "__main__":
    main()
